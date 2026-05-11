"""SSH config discovery — surface ``~/.ssh/config`` as molq cluster candidates.

molq's :class:`~molq.transport.SshTransport` already inherits ``~/.ssh/config``
implicitly (it shells out to OpenSSH).  This module makes that config
*visible* — listing the named hosts the user has already configured so the
CLI and API can show them as cluster candidates without forcing a
``~/.molq/config.toml`` entry.

Two operations:

* :func:`list_ssh_hosts` — enumerate concrete ``Host`` aliases (skipping
  wildcard patterns and ``Match`` blocks) by reading the config files
  directly.  Includes ``Include`` directives.
* :func:`resolve_ssh_host` — return the *effective* configuration for a
  single host (hostname, user, port, identityfile) by shelling out to
  ``ssh -G <name>``.  This is the canonical resolver — it understands
  globs, ``Match`` blocks, ``ProxyJump``, system-wide config — so we never
  need to re-implement OpenSSH's config language.

Zero new Python deps.  Falls back gracefully when ``~/.ssh/config`` is
absent or ``ssh`` is unavailable.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_USER_CONFIG = Path("~/.ssh/config")
DEFAULT_SYSTEM_CONFIG = Path("/etc/ssh/ssh_config")


@dataclass(frozen=True)
class SshHost:
    """A named entry from ``~/.ssh/config`` resolved for molq.

    ``alias`` is what the user typed under ``Host``; ``hostname`` is the
    effective remote hostname (``Hostname`` directive or alias if absent).
    All effective fields come from ``ssh -G`` so we honor includes, globs,
    and ``Match`` blocks without re-parsing.
    """

    alias: str
    hostname: str | None = None
    user: str | None = None
    port: int | None = None
    identity_file: str | None = None
    proxy_jump: str | None = None
    forward_agent: bool = False
    extra: dict[str, str] = field(default_factory=dict)

    @property
    def target(self) -> str:
        """``user@hostname[:port]`` shorthand for display."""
        host = self.hostname or self.alias
        prefix = f"{self.user}@" if self.user else ""
        suffix = f":{self.port}" if self.port and self.port != 22 else ""
        return f"{prefix}{host}{suffix}"


def _expand(path: Path) -> Path:
    return Path(os.path.expanduser(str(path)))


def _iter_config_files(config_path: Path) -> list[Path]:
    """Walk a config file plus any ``Include`` directives it pulls in.

    Returned in include order so later overrides win, matching ``ssh``'s
    own evaluation order.
    """
    seen: set[Path] = set()
    out: list[Path] = []

    def visit(path: Path) -> None:
        path = _expand(path)
        if not path.exists() or path in seen:
            return
        seen.add(path)
        out.append(path)
        try:
            text = path.read_text()
        except OSError:
            return
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("include "):
                pattern = line.split(None, 1)[1].strip().strip('"')
                # Include is relative to ~/.ssh by convention when not absolute.
                pat_path = Path(os.path.expanduser(pattern))
                if not pat_path.is_absolute():
                    pat_path = _expand(Path("~/.ssh")) / pat_path
                # Glob expansion — ssh supports wildcards in Include.
                for match in sorted(pat_path.parent.glob(pat_path.name)):
                    visit(match)

    visit(config_path)
    return out


def list_ssh_hosts(
    config_path: str | Path | None = None,
) -> list[SshHost]:
    """Return concrete ``Host`` aliases discovered in ``~/.ssh/config``.

    Wildcard patterns (``Host *``) and ``Match`` blocks are skipped — they
    are templates, not nameable destinations.  Each surviving alias is
    resolved through :func:`resolve_ssh_host` so the returned objects carry
    the *effective* hostname/user/port/identityfile, including anything
    pulled in via ``Include`` directives.

    Args:
        config_path: Override the default ``~/.ssh/config`` location.

    Returns:
        Hosts in declaration order (deduplicated).  Empty list when the
        config file does not exist.
    """
    cfg = _expand(Path(config_path) if config_path else DEFAULT_USER_CONFIG)
    if not cfg.exists():
        return []

    aliases: list[str] = []
    seen: set[str] = set()
    for file_path in _iter_config_files(cfg):
        try:
            text = file_path.read_text()
        except OSError:
            continue
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if not line.lower().startswith("host "):
                continue
            for token in line.split()[1:]:
                if any(ch in token for ch in "*?!"):
                    continue
                if token in seen:
                    continue
                seen.add(token)
                aliases.append(token)

    out: list[SshHost] = []
    for alias in aliases:
        try:
            out.append(resolve_ssh_host(alias, config_path=cfg))
        except OSError:
            # ssh -G missing — fall back to a bare alias entry so the host is
            # still discoverable; effective fields stay None.
            out.append(SshHost(alias=alias))
    return out


def resolve_ssh_host(
    alias: str,
    *,
    config_path: str | Path | None = None,
    ssh_bin: str = "ssh",
) -> SshHost:
    """Return the effective ssh config for *alias*.

    Shells out to ``ssh -G <alias>`` — the canonical way to ask OpenSSH
    "what would happen if I ran ``ssh <alias>``" without actually
    connecting.  Output is ``key value`` pairs, one per line.

    Args:
        alias: ``Host`` alias to resolve.
        config_path: Optional explicit config file (``ssh -F <path>``).
            Default reads ``~/.ssh/config`` plus system config like ``ssh``
            itself does.
        ssh_bin: Override the ``ssh`` binary (mostly for tests).

    Raises:
        OSError: When the ``ssh`` binary is not on PATH.
    """
    if shutil.which(ssh_bin) is None:
        raise OSError(f"{ssh_bin} not found on PATH")
    argv = [ssh_bin, "-G"]
    if config_path is not None:
        argv += ["-F", os.path.expanduser(str(config_path))]
    argv.append(alias)
    proc = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        check=False,
    )
    # ssh -G prints config even on unknown aliases; non-zero only on bad flags.
    parsed: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        if not line or " " not in line:
            continue
        key, _, value = line.partition(" ")
        parsed.setdefault(key.lower(), value.strip())

    port_raw = parsed.get("port")
    port: int | None
    try:
        port = int(port_raw) if port_raw else None
    except ValueError:
        port = None

    identity = parsed.get("identityfile")
    identity = os.path.expanduser(identity) if identity else None

    forward = parsed.get("forwardagent", "no").lower() in {"yes", "true"}

    # Drop fields we already extract; keep the rest as a passthrough so
    # ``molq clusters show`` can surface ProxyCommand, ServerAliveInterval, etc.
    extracted = {
        "host",
        "hostname",
        "user",
        "port",
        "identityfile",
        "proxyjump",
        "forwardagent",
    }
    extra = {k: v for k, v in parsed.items() if k not in extracted and v}

    return SshHost(
        alias=alias,
        hostname=parsed.get("hostname"),
        user=parsed.get("user"),
        port=port,
        identity_file=identity,
        proxy_jump=parsed.get("proxyjump") or None,
        forward_agent=forward,
        extra=extra,
    )


def to_ssh_target(host: SshHost) -> str:
    """Produce the ``user@host`` string molq's :class:`SshTransport` expects.

    Falls back to bare alias when no user is set — OpenSSH will use the
    ``User`` directive from config at connect time.
    """
    if host.user and host.hostname:
        return f"{host.user}@{host.hostname}"
    if host.hostname:
        return host.hostname
    return host.alias


__all__ = [
    "SshHost",
    "list_ssh_hosts",
    "resolve_ssh_host",
    "to_ssh_target",
]


def _format_command(argv: list[str]) -> str:
    """For debug logs — quote a command line consistent with shells."""
    return " ".join(shlex.quote(a) for a in argv)
