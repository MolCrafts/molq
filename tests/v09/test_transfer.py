"""Destination transfer uses the same handle style as submit."""

import os
import shutil

import pytest

from molq import Molq, MolqError


@pytest.mark.skipif(os.name == "nt", reason="rsync transfer is POSIX")
def test_cluster_transfer_passes_rsync_options(tmp_path):
    if shutil.which("rsync") is None:
        pytest.skip("rsync is not installed")
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "note.txt").write_text("copied")
    with Molq(registry=tmp_path / "registry.db") as mq:
        origin = mq.clusters.register(name="from", scheduler="shell")
        target = mq.clusters.register(name="to", scheduler="shell")
        with pytest.raises(MolqError, match="Cluster"):
            origin.transfer(
                str(source / "note.txt"), "to", str(destination / "note.txt")
            )
        result = origin.transfer(
            str(source / "note.txt"), target, str(destination / "note.txt"), "-a"
        )
        with pytest.raises(MolqError, match="Transport owns"):
            origin.transfer(str(source), target, str(destination), "-essh")
    assert result["outcome"] == "applied"
    assert (destination / "note.txt").read_text() == "copied"
