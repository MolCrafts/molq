"""Tests for molq.ssh_config — SSH config discovery."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from molq.ssh_config import (
    SshHost,
    list_ssh_hosts,
    resolve_ssh_host,
    to_ssh_target,
)


@pytest.fixture
def fake_ssh_config(tmp_path: Path) -> Path:
    cfg = tmp_path / "config"
    cfg.write_text(
        """
# Top-level comment
Host dardel
    Hostname dardel.example.org
    User alice
    IdentityFile ~/.ssh/id-dardel
    ForwardAgent yes

Host alvis
    Hostname alvis.example.org
    User bob
    Port 2222

Host *.internal
    User svc

Host *
    ServerAliveInterval 60
""".strip()
    )
    return cfg


def test_list_skips_wildcards_and_returns_concrete_aliases(
    fake_ssh_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if shutil.which("ssh") is None:
        pytest.skip("ssh binary required for resolve_ssh_host")
    hosts = list_ssh_hosts(fake_ssh_config)
    aliases = [h.alias for h in hosts]
    assert aliases == ["dardel", "alvis"]


def test_list_returns_empty_when_config_missing(tmp_path: Path) -> None:
    assert list_ssh_hosts(tmp_path / "nope") == []


def test_list_handles_include_directive(tmp_path: Path) -> None:
    extra = tmp_path / "extra.conf"
    extra.write_text("Host included\n    Hostname inc.example.org\n")
    cfg = tmp_path / "config"
    cfg.write_text(f"Include {extra}\nHost main\n    Hostname main.example.org\n")
    if shutil.which("ssh") is None:
        pytest.skip("ssh binary required for resolve_ssh_host")
    aliases = [h.alias for h in list_ssh_hosts(cfg)]
    assert set(aliases) == {"included", "main"}


def test_resolve_returns_effective_fields(fake_ssh_config: Path) -> None:
    if shutil.which("ssh") is None:
        pytest.skip("ssh binary required for resolve_ssh_host")
    host = resolve_ssh_host("dardel", config_path=fake_ssh_config)
    assert host.alias == "dardel"
    assert host.hostname == "dardel.example.org"
    assert host.user == "alice"
    assert host.port == 22  # default when not overridden
    # IdentityFile is expanded; just sanity-check the trailing component.
    assert host.identity_file is not None and host.identity_file.endswith("id-dardel")
    assert host.forward_agent is True


def test_resolve_picks_up_port_override(fake_ssh_config: Path) -> None:
    if shutil.which("ssh") is None:
        pytest.skip("ssh binary required for resolve_ssh_host")
    host = resolve_ssh_host("alvis", config_path=fake_ssh_config)
    assert host.user == "bob"
    assert host.port == 2222


def test_resolve_raises_when_ssh_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("molq.ssh_config.shutil.which", lambda _: None)
    with pytest.raises(OSError):
        resolve_ssh_host("anywhere")


def test_to_ssh_target_prefers_user_at_hostname() -> None:
    host = SshHost(alias="x", hostname="h.example.org", user="u")
    assert to_ssh_target(host) == "u@h.example.org"


def test_to_ssh_target_falls_back_to_alias() -> None:
    host = SshHost(alias="bare")
    assert to_ssh_target(host) == "bare"


def test_to_ssh_target_omits_user_when_missing() -> None:
    host = SshHost(alias="x", hostname="h.example.org")
    assert to_ssh_target(host) == "h.example.org"
