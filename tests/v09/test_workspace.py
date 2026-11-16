"""Workspace binds and narrows existing Path access without owning files."""

import sys

import pytest

from molq import ArtifactManifest, Catalog, Molq, Path, Workspace


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


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX target file operations")
def test_workspace_upload_and_download_a_named_output(tmp_path):
    registry = tmp_path / "registry.db"
    local = tmp_path / "upload"
    local.mkdir()
    (local / "data.txt").write_text("payload")
    with Molq(registry=registry) as mq:
        mq.clusters.register(name="box", scheduler="shell")
        root = f"box:{tmp_path}/run"
        workspace = Workspace(Path(root, molq=mq))
        workspace.ensure()
        with pytest.raises(ValueError, match="same Molq client"):
            workspace.upload(Path(local, molq=Molq()), "results", "-a")
        assert workspace.upload(Path(local), "results", "-a")["outcome"] == "applied"
        workspace.path("results/catalog.json").write_text(
            '{"out":{"chosen":"data.txt","other":"keep.txt"}}'
        )
        workspace.path("results/keep.txt").write_text("keep on the target")
        with pytest.raises(ValueError, match="relative workspace path"):
            workspace.upload(Path(local), "../outside", "-a")

    with Molq(registry=registry) as mq:
        workspace = Workspace(Path(root, molq=mq))
        target = tmp_path / "download"
        target.mkdir()
        delivery = workspace.download(
            ArtifactManifest({"results": Catalog("catalog.json", "out")}),
            Path(target),
            relative="results",
            select=("results/chosen",),
        )
        assert delivery.paths == ("data.txt",)
        assert (target / "data.txt").read_text() == "payload"
        assert sorted(p.name for p in target.iterdir()) == ["data.txt"]
        assert workspace.path("results/keep.txt").read_text() == "keep on the target"
