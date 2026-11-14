"""Workspace binds and narrows existing Path access without owning files."""

import sys

import pytest

from molq import Molq, Path, Workspace


def test_workspace_requires_cluster_root_and_rejects_upward_paths():
    with pytest.raises(ValueError, match="absolute cluster path"):
        Workspace(Path("/tmp/work"))
    workspace = Workspace(Path("unused:/shared/run"))
    assert str(workspace.path()) == "unused:/shared/run"
    assert str(workspace.path("inputs/data")) == "unused:/shared/run/inputs/data"
    for path in ("../other", "a/../../other", "/other", "other:/shared", ""):
        with pytest.raises(ValueError, match="relative workspace path"):
            workspace.path(path)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX target file operations")
def test_workspace_reopen_and_preserve_existing_scope(tmp_path):
    registry = tmp_path / "registry.db"
    base = tmp_path / "base"
    base.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "payload").write_text("outside")
    (base / "escape").symlink_to(outside, target_is_directory=True)
    with Molq(registry=registry) as mq:
        mq.clusters.register(name="box", scheduler="shell")
        root = Path(f"box:{base}/run", molq=mq, root=str(base))
        workspace = Workspace(root)
        assert workspace.ensure().is_dir()
        workspace.ensure()
        workspace.path("input").write_text("shared")
        escaped = Workspace(Path(f"box:{base}/escape", molq=mq, root=str(base)))
        with pytest.raises(PermissionError):
            escaped.path()
        with pytest.raises(PermissionError):
            escaped.ensure()

    with Molq(registry=registry) as mq:
        reopened = Workspace(Path(str(root), molq=mq))
        assert reopened.path("input").read_text() == "shared"
        with pytest.raises(PermissionError):
            (reopened.path() / ".." / "escape" / "payload").read_text()
        assert (outside / "payload").read_text() == "outside"
