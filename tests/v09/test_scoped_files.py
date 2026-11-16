"""Destination I/O contracts used by applications, without workspace policy."""

import base64

import pytest

from molq import Molq, MolqError, Path


def test_scoped_pagination_batches_and_mutations(tmp_path):
    root = tmp_path / "scope"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("secret")
    (root / "escape").symlink_to(outside)
    for name in ["-dash", ".hidden", "a", "b"]:
        (root / name).write_text(name)
    with Molq(registry=tmp_path / "registry.db") as client:
        cluster = client.clusters.register(name="files", scheduler="shell")
        path = Path(f"{cluster.name}:{root}", molq=client, root=str(root))
        first = path.list_page(with_stat=True, limit=2)
        second = path.list_page(with_stat=True, limit=3, after=first["next_after"])
        assert [e["name"] for e in first["entries"] + second["entries"]] == [
            "-dash",
            ".hidden",
            "a",
            "b",
            "escape",
        ]
        assert first["entries"][1]["size"] == 7
        assert (
            path.read_many(
                [str(root / "a"), str(root / "missing"), str(root / "escape")]
            )["items"][0]["data"]
            == base64.b64encode(b"a").decode()
        )
        for bad in ["../outside", str(root / "escape"), str(outside)]:
            with pytest.raises(PermissionError):
                path.with_segments(bad).read_bytes()
        for field in path.read_many([str(root / "missing"), str(root / "escape")])[
            "items"
        ]:
            assert "error" in field and "data" not in field
        sub = path / "sub"
        sub.mkdir()
        (sub / "data").write_bytes(b"012345")
        assert (sub / "data").read_range(2, 3) == b"234"
        (sub / "data").chmod(0o600)
        sub.remove(recursive=True)
        assert not sub.exists()
        assert path.resolve().as_posix() == str(root.resolve())


def test_batch_limits_are_explicit(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    (root / "large").write_bytes(b"x" * 20)
    with Molq(registry=tmp_path / "registry.db") as client:
        cluster = client.clusters.register(name="files", scheduler="shell")
        path = Path(f"{cluster.name}:{root}", molq=client, root=str(root))
        item = path.read_many([str(root / "large")], max_bytes=10)["items"][0]
        assert item["error"]["context"]["reason"] == "limit" and "data" not in item
        with pytest.raises(MolqError):
            path.list_page(limit=513)


def test_scoped_unlink_preserves_the_symlink_target(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    (root / "original").write_text("preserve me")
    (root / "link").symlink_to(root / "original")
    with Molq(registry=tmp_path / "registry.db") as client:
        cluster = client.clusters.register(name="files", scheduler="shell")
        link = Path(f"{cluster.name}:{root / 'link'}", molq=client, root=str(root))
        relative = link.with_name("relative")
        relative.symlink_to("original")
        assert relative.read_text() == "preserve me"
        assert (root / "relative").readlink().as_posix() == "original"
        assert link.is_symlink()
        link.unlink()
        assert (root / "original").read_text() == "preserve me"
        assert not (root / "link").exists()


def test_read_many_executes_one_target_command(tmp_path, monkeypatch):
    import os

    binary = tmp_path / "ssh"
    calls = tmp_path / "calls"
    # `ssh -G` only evaluates local config; count commands that reach the target.
    binary.write_text(
        f'#!/bin/sh\n[ "$1" = -G ] && exit 0\nprintf x >> \'{calls}\'\nfor arg do last=$arg; done\nexec sh -c "$last"\n'.replace(
            '\\"', '"'
        )
    )
    binary.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    root = tmp_path / "root"
    root.mkdir()
    for name in ("a", "b", "c"):
        (root / name).write_text(name)
    with Molq(registry=tmp_path / "registry.db") as client:
        cluster = client.clusters.register(
            name="ssh-files",
            scheduler="slurm",
            transport={"kind": "ssh", "alias": "fixture"},
        )
        calls.write_text("")
        path = Path(f"{cluster.name}:{root}", molq=client, root=str(root))
        result = path.read_many([str(root / name) for name in ("a", "b", "c")])
        assert len(result["items"]) == 3 and all(
            "data" in item for item in result["items"]
        )
        assert calls.read_text() == "x"


def test_scoped_transfer_rejects_nested_escaping_symlinks(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("private")
    source = root / "source"
    source.mkdir()
    (source / "escape").symlink_to(outside)
    with Molq(registry=tmp_path / "registry.db") as client:
        cluster = client.clusters.register(name="files", scheduler="shell")
        path = Path(f"{cluster.name}:{source}", molq=client, root=str(root))
        with pytest.raises(MolqError) as error:
            path.transfer(Path(tmp_path / "copy", molq=client), "-aL")
        assert error.value.context["reason"] == "outside_root"
        assert not (tmp_path / "copy").exists()
