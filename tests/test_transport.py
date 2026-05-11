"""Tests for ``molq.transport``.

``LocalTransport`` is exercised against a real subprocess + tmp_path filesystem
because it has no external dependencies.  ``SshTransport`` is exercised at the
*argv-construction* level by injecting fake binaries — we never want CI to
require an SSH daemon.  End-to-end SSH coverage against a real host belongs
in a CI-provisioned integration suite, not in this unit-test module.
"""

from __future__ import annotations

import base64
import os
import stat
import sys
from pathlib import Path

import pytest

from molq.options import SshTransportOptions
from molq.transport import (
    CommandResult,
    LocalTransport,
    SshTransport,
    Transport,
    TransportError,
)

# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_local_transport_is_a_transport() -> None:
    assert isinstance(LocalTransport(), Transport)


def test_ssh_transport_is_a_transport() -> None:
    t = SshTransport(options=SshTransportOptions(host="user@example"))
    assert isinstance(t, Transport)


# ---------------------------------------------------------------------------
# LocalTransport — run()
# ---------------------------------------------------------------------------


def test_local_run_captures_stdout() -> None:
    t = LocalTransport()
    result = t.run([sys.executable, "-c", "print('hello')"])
    assert isinstance(result, CommandResult)
    assert result.returncode == 0
    assert result.stdout.strip() == "hello"
    assert result.stderr == ""


def test_local_run_returns_nonzero_without_raising() -> None:
    t = LocalTransport()
    result = t.run([sys.executable, "-c", "import sys; sys.exit(2)"])
    assert result.returncode == 2


def test_local_run_check_returncode_raises() -> None:
    import subprocess as sp

    result = LocalTransport().run([sys.executable, "-c", "import sys; sys.exit(7)"])
    with pytest.raises(sp.CalledProcessError) as exc_info:
        result.check_returncode()
    assert exc_info.value.returncode == 7


def test_local_run_passes_cwd(tmp_path: Path) -> None:
    t = LocalTransport()
    result = t.run(
        [sys.executable, "-c", "import os; print(os.getcwd())"], cwd=str(tmp_path)
    )
    assert result.returncode == 0
    assert result.stdout.strip() == str(tmp_path)


def test_local_run_passes_env() -> None:
    t = LocalTransport()
    result = t.run(
        [sys.executable, "-c", "import os; print(os.environ['MOLQ_TEST'])"],
        env={**os.environ, "MOLQ_TEST": "42"},
    )
    assert result.returncode == 0
    assert result.stdout.strip() == "42"


def test_local_run_passes_input() -> None:
    t = LocalTransport()
    result = t.run(
        [sys.executable, "-c", "import sys; print(sys.stdin.read().upper())"],
        input="hi",
    )
    assert result.returncode == 0
    assert result.stdout.strip() == "HI"


def test_local_run_timeout_raises_transport_error() -> None:
    t = LocalTransport()
    with pytest.raises(TransportError):
        t.run([sys.executable, "-c", "import time; time.sleep(5)"], timeout=0.2)


def test_local_run_unknown_binary_raises_transport_error() -> None:
    t = LocalTransport()
    with pytest.raises(TransportError):
        t.run(["/nonexistent/binary-asdfqwer"])


# ---------------------------------------------------------------------------
# LocalTransport — file ops
# ---------------------------------------------------------------------------


def test_local_write_then_read_text(tmp_path: Path) -> None:
    t = LocalTransport()
    target = tmp_path / "subdir" / "file.txt"
    t.write_text(str(target), "hello world")
    assert target.exists()
    assert t.read_text(str(target)) == "hello world"


def test_local_write_text_uses_atomic_rename(tmp_path: Path) -> None:
    """Failure mid-write must not leave a partial file at the target path."""
    t = LocalTransport()
    target = tmp_path / "atomic.txt"
    t.write_text(str(target), "v1")
    # Crash inside fsync/replace wouldn't leave .tmp around for another writer to see;
    # we just confirm no extra files are visible at the target location after a normal write.
    siblings = list(tmp_path.iterdir())
    assert siblings == [target]


def test_local_write_text_sets_mode(tmp_path: Path) -> None:
    t = LocalTransport()
    target = tmp_path / "mode.txt"
    t.write_text(str(target), "x", mode=0o600)
    mode = stat.S_IMODE(target.stat().st_mode)
    assert mode == 0o600


def test_local_write_bytes_round_trips_binary(tmp_path: Path) -> None:
    t = LocalTransport()
    target = tmp_path / "bin"
    payload = bytes(range(256))
    t.write_bytes(str(target), payload)
    assert t.read_bytes(str(target)) == payload


def test_local_exists_and_mkdir(tmp_path: Path) -> None:
    t = LocalTransport()
    nested = tmp_path / "a" / "b" / "c"
    assert not t.exists(str(nested))
    t.mkdir(str(nested))
    assert t.exists(str(nested))
    # Idempotent
    t.mkdir(str(nested))


def test_local_chmod(tmp_path: Path) -> None:
    t = LocalTransport()
    target = tmp_path / "f"
    target.write_text("x")
    t.chmod(str(target), 0o700)
    assert stat.S_IMODE(target.stat().st_mode) == 0o700


def test_local_remove_file(tmp_path: Path) -> None:
    t = LocalTransport()
    target = tmp_path / "f"
    target.write_text("x")
    t.remove(str(target))
    assert not target.exists()
    # Removing missing file is a no-op
    t.remove(str(target))


def test_local_remove_recursive(tmp_path: Path) -> None:
    t = LocalTransport()
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    (nested / "f").write_text("x")
    t.remove(str(tmp_path / "a"), recursive=True)
    assert not (tmp_path / "a").exists()


# ---------------------------------------------------------------------------
# LocalTransport — upload/download (local copy)
# ---------------------------------------------------------------------------


def test_local_upload_file(tmp_path: Path) -> None:
    t = LocalTransport()
    src = tmp_path / "src.txt"
    src.write_text("data")
    dst = tmp_path / "out" / "dst.txt"
    t.upload(str(src), str(dst))
    assert dst.read_text() == "data"


def test_local_upload_directory_recursive(tmp_path: Path) -> None:
    t = LocalTransport()
    src = tmp_path / "src"
    (src / "a").mkdir(parents=True)
    (src / "a" / "x").write_text("X")
    (src / "y").write_text("Y")
    dst = tmp_path / "dst"
    t.upload(str(src), str(dst), recursive=True)
    assert (dst / "a" / "x").read_text() == "X"
    assert (dst / "y").read_text() == "Y"


def test_local_upload_directory_merges_into_existing(tmp_path: Path) -> None:
    t = LocalTransport()
    src = tmp_path / "src"
    src.mkdir()
    (src / "new").write_text("new")
    dst = tmp_path / "dst"
    dst.mkdir()
    (dst / "old").write_text("old")
    t.upload(str(src), str(dst), recursive=True)
    assert (dst / "old").read_text() == "old"
    assert (dst / "new").read_text() == "new"


def test_local_upload_directory_excludes(tmp_path: Path) -> None:
    t = LocalTransport()
    src = tmp_path / "src"
    src.mkdir()
    (src / "keep.py").write_text("ok")
    (src / "__pycache__").mkdir()
    (src / "__pycache__" / "skip.pyc").write_text("nope")
    dst = tmp_path / "dst"
    t.upload(str(src), str(dst), recursive=True, exclude=["__pycache__"])
    assert (dst / "keep.py").exists()
    assert not (dst / "__pycache__").exists()


def test_local_upload_same_path_is_noop(tmp_path: Path) -> None:
    t = LocalTransport()
    target = tmp_path / "f"
    target.write_text("x")
    t.upload(str(target), str(target))  # would otherwise fail
    assert target.read_text() == "x"


def test_local_upload_directory_without_recursive_raises(tmp_path: Path) -> None:
    t = LocalTransport()
    src = tmp_path / "d"
    src.mkdir()
    with pytest.raises(IsADirectoryError):
        t.upload(str(src), str(tmp_path / "out"))


def test_local_download_is_symmetric_with_upload(tmp_path: Path) -> None:
    t = LocalTransport()
    src = tmp_path / "src.txt"
    src.write_text("data")
    dst = tmp_path / "out" / "dst.txt"
    t.download(str(src), str(dst))
    assert dst.read_text() == "data"


# ---------------------------------------------------------------------------
# SshTransport — argv construction (no real ssh)
# ---------------------------------------------------------------------------


def test_ssh_argv_basic() -> None:
    t = SshTransport(options=SshTransportOptions(host="me@host"))
    argv = t._ssh_argv()
    # Always non-interactive
    assert "BatchMode=yes" in argv
    # Host is the last entry (so subsequent items become the remote command)
    assert argv[-1] == "me@host"


def test_ssh_argv_includes_port_and_identity() -> None:
    t = SshTransport(
        options=SshTransportOptions(
            host="h",
            port=2222,
            identity_file="/home/me/.ssh/id",
            ssh_opts=("-o", "ServerAliveInterval=30"),
        )
    )
    argv = t._ssh_argv()
    assert "-p" in argv and "2222" in argv
    assert "-i" in argv and "/home/me/.ssh/id" in argv
    assert "ServerAliveInterval=30" in argv


def test_ssh_e_arg_quotes_options() -> None:
    t = SshTransport(
        options=SshTransportOptions(host="h", port=2222, identity_file="/k id")
    )
    e_arg = t._ssh_e_arg()
    # Spaces in identity file path must survive shlex quoting
    assert "'/k id'" in e_arg
    assert "BatchMode=yes" in e_arg


def test_ssh_run_builds_quoted_remote_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """The remote argv must be a single shell-quoted string passed after `--`."""
    captured: dict = {}

    def fake_run(argv, **kwargs):
        captured["argv"] = argv
        captured["kwargs"] = kwargs

        class P:
            returncode = 0
            stdout = ""
            stderr = ""

        return P()

    monkeypatch.setattr("molq.transport.subprocess.run", fake_run)

    t = SshTransport(options=SshTransportOptions(host="h"))
    t.run(["echo", "hello world"], cwd="/tmp/spaced dir", env={"K": "v"})

    argv = captured["argv"]
    assert argv[:2] == ["ssh", "-o"]
    assert argv[-2] == "--"
    remote = argv[-1]
    # cwd, env, and the user argv all appear shell-quoted in the remote command
    assert "K=v" in remote
    assert "cd '/tmp/spaced dir'" in remote
    assert "echo 'hello world'" in remote


def test_ssh_exists_returns_true_on_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "molq.transport.subprocess.run",
        lambda *a, **kw: type("P", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
    )
    t = SshTransport(options=SshTransportOptions(host="h"))
    assert t.exists("/x") is True


def test_ssh_exists_returns_false_on_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "molq.transport.subprocess.run",
        lambda *a, **kw: type("P", (), {"returncode": 1, "stdout": "", "stderr": ""})(),
    )
    t = SshTransport(options=SshTransportOptions(host="h"))
    assert t.exists("/x") is False


def test_ssh_exists_other_returncode_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "molq.transport.subprocess.run",
        lambda *a, **kw: type(
            "P", (), {"returncode": 255, "stdout": "", "stderr": "boom"}
        )(),
    )
    t = SshTransport(options=SshTransportOptions(host="h"))
    with pytest.raises(TransportError):
        t.exists("/x")


def test_ssh_read_text_decodes_base64(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = "héllo\nworld"
    encoded = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    monkeypatch.setattr(
        "molq.transport.subprocess.run",
        lambda *a, **kw: type(
            "P", (), {"returncode": 0, "stdout": encoded, "stderr": ""}
        )(),
    )
    t = SshTransport(options=SshTransportOptions(host="h"))
    assert t.read_text("/x") == payload


def test_ssh_read_text_missing_raises_filenotfound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "molq.transport.subprocess.run",
        lambda *a, **kw: type(
            "P",
            (),
            {
                "returncode": 1,
                "stdout": "",
                "stderr": "base64: /x: No such file or directory\n",
            },
        )(),
    )
    t = SshTransport(options=SshTransportOptions(host="h"))
    with pytest.raises(FileNotFoundError):
        t.read_text("/x")


def test_ssh_write_text_pipes_base64(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    def fake_run(argv, **kwargs):
        captured["argv"] = argv
        captured["input"] = kwargs.get("input")

        class P:
            returncode = 0
            stdout = ""
            stderr = ""

        return P()

    monkeypatch.setattr("molq.transport.subprocess.run", fake_run)

    t = SshTransport(options=SshTransportOptions(host="h"))
    t.write_text("/path with space/x", "héllo")

    remote = captured["argv"][-1]
    assert "base64 -d" in remote
    assert "'/path with space/x'" in remote
    assert "chmod 600" in remote
    assert captured["input"] == base64.b64encode("héllo".encode()).decode("ascii")


def test_ssh_mkdir_uses_p_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    def fake_run(argv, **kwargs):
        captured["argv"] = argv

        class P:
            returncode = 0
            stdout = ""
            stderr = ""

        return P()

    monkeypatch.setattr("molq.transport.subprocess.run", fake_run)
    t = SshTransport(options=SshTransportOptions(host="h"))
    t.mkdir("/some path/x", parents=True)
    # shlex.quote only quotes when shell-special chars are present; the space forces it.
    assert "mkdir -p '/some path/x'" in captured["argv"][-1]
    # Without spaces it's still a -p mkdir, just unquoted.
    captured.clear()
    t.mkdir("/plain/path", parents=True)
    assert "mkdir -p /plain/path" in captured["argv"][-1]


def test_ssh_upload_invokes_rsync(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict = {}

    def fake_run(argv, **kwargs):
        captured["argv"] = argv

        class P:
            returncode = 0
            stdout = ""
            stderr = ""

        return P()

    monkeypatch.setattr("molq.transport.subprocess.run", fake_run)
    t = SshTransport(options=SshTransportOptions(host="cluster", port=2222))
    t.upload(str(tmp_path), "/scratch/work", recursive=True, exclude=["__pycache__"])

    argv = captured["argv"]
    assert argv[0] == "rsync"
    # rsync_opts default is ("-a", "--partial", "--inplace")
    assert "-a" in argv and "--partial" in argv
    assert "--exclude" in argv and "__pycache__" in argv
    assert "-e" in argv
    e_idx = argv.index("-e")
    assert "ssh" in argv[e_idx + 1] and "BatchMode=yes" in argv[e_idx + 1]
    # destination is host:remote
    assert argv[-1] == "cluster:/scratch/work"
    # source has trailing slash for directory recursion
    assert argv[-2].endswith("/")
