"""Resolve and download named outputs without a submission."""

import json
import shutil
import sys

import pytest

from molq import ArtifactManifest, Catalog, Glob, Molq, OutputPath, Path


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX target file operations")
def test_manifest_selects_files_and_reuses_resolved_paths(tmp_path):
    if not shutil.which("rsync"):
        pytest.skip("rsync unavailable")
    root = tmp_path / "outputs"
    root.mkdir()
    (root / "frames").mkdir()
    (root / "frames" / "b.xyz").write_text("b")
    (root / "frames" / "a.xyz").write_text("a")
    (root / "frames" / "directory.xyz").mkdir()
    (root / "summary*.json").write_text("literal")
    (root / "artifacts.json").write_text(
        json.dumps({"checkpoints": {"latest": "frames/b.xyz", "best": "frames/a.xyz"}})
    )
    manifest = ArtifactManifest(
        {
            "summary": OutputPath("summary*.json"),
            "frames": Glob("frames/*.xyz"),
            "checkpoints": Catalog("artifacts.json", "checkpoints"),
            "missing": Glob("logs/*.log", required=False),
            "unselected": Catalog("absent.json", "unused"),
        }
    )
    with Molq(registry=tmp_path / "registry.db") as client:
        client.clusters.register(name="box", scheduler="shell")
        source = Path(f"box:{root}", molq=client)
        selected = manifest.resolve(
            source, select=("summary", "checkpoints/latest", "frames", "missing")
        )
        assert selected.paths == ("summary*.json", "frames/b.xyz", "frames/a.xyz")
        assert selected.names["checkpoints/latest"] == ("frames/b.xyz",)
        assert selected.names["missing"] == ()
        (root / "frames" / "c.xyz").write_text("new")
        (root / "artifacts.json").unlink()
        dest = Path(tmp_path / "dest", molq=client)
        with pytest.raises(ValueError, match="rsync option"):
            selected.download(dest, options=("-av", "--remove-source-files"))
        assert not dest.exists()
        delivery = selected.download(dest, options=("-a",))
        assert delivery.paths == selected.paths
        assert (dest / "summary*.json").read_text() == "literal"
        assert (dest / "frames" / "a.xyz").read_text() == "a"
        assert not (dest / "frames" / "c.xyz").exists()
        assert not (dest / "frames" / "directory.xyz").exists()
        assert not (dest / "artifacts.json").exists()


def test_manifest_rejects_untrusted_paths_and_escaping_links(tmp_path):
    root = tmp_path / "outputs"
    root.mkdir()
    secret = tmp_path / "outside"
    secret.write_text("outside")
    (root / "link").symlink_to(secret)
    manifest = ArtifactManifest(
        {"link": OutputPath("link"), "task": Catalog("artifacts.json", "task")}
    )
    with pytest.raises(PermissionError):
        manifest.resolve(Path(root), select=("link",))
    for name in ("../outside", str(secret)):
        (root / "artifacts.json").write_text(json.dumps({"task": {"file": name}}))
        with pytest.raises(ValueError, match="relative artifact path"):
            manifest.resolve(Path(root), select=("task/file",))
    with pytest.raises(ValueError, match="relative artifact path"):
        ArtifactManifest({"escape": Glob("../*")})
    with pytest.raises(ValueError, match="Select"):
        manifest.resolve(Path(root), select=())
