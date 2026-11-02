"""mq.Path names a cluster the way an rsync operand names a host."""

import os
import pickle
import shutil
import stat

import pytest

import molq as mq
from molq import Molq, MolqError

posix_only = pytest.mark.skipif(
    os.name == "nt", reason="target file scripts are POSIX sh"
)


def test_cluster_prefix_survives_path_arithmetic_and_pickle():
    run = mq.Path("arrhenius:/scratch/you/run_01")
    out = run / "out.log"
    assert (out.cluster, out.as_posix()) == ("arrhenius", "/scratch/you/run_01/out.log")
    assert str(out) == "arrhenius:/scratch/you/run_01/out.log"
    assert mq.Path(str(out)) == out
    assert out.parent.cluster == out.with_suffix(".xyz").cluster == "arrhenius"
    assert out != mq.Path("/scratch/you/run_01/out.log")
    restored = pickle.loads(pickle.dumps(out))
    assert restored == out and restored.cluster == "arrhenius"
    assert (
        mq.Path("hpc:").as_posix() == "." and mq.Path("hpc:runs").as_posix() == "runs"
    )
    assert mq.Path("./a:b").cluster is None
    with pytest.raises(TypeError, match="arrhenius"):
        os.fspath(out)


def test_local_path_uses_this_filesystem(tmp_path):
    root = mq.Path(tmp_path) / "tree"
    root.mkdir()
    (root / "a.txt").write_text("alpha")
    assert os.fspath(root / "a.txt") == str(tmp_path / "tree" / "a.txt")
    assert [p.name for p in root.iterdir()] == ["a.txt"]
    assert (root / "a.txt").read_text() == "alpha"


@posix_only
def test_cluster_path_io_goes_through_files_rpc(tmp_path):
    with Molq(registry=tmp_path / "registry.db") as client:
        client.clusters.register(name="box", scheduler="shell")
        run = mq.Path(f"box:{tmp_path}/run", molq=client)
        assert not run.exists()
        (run / "sub").mkdir(parents=True)
        with pytest.raises(FileExistsError):
            run.mkdir()
        log = run / "out.log"
        assert log.write_text("E = -1.5\n") == 9
        assert log.read_text() == "E = -1.5\n"
        assert log.is_file() and run.is_dir() and not log.is_symlink()
        info = log.stat()
        assert stat.S_ISREG(info.st_mode) and info.st_size == 9 and info.st_mtime > 0
        big = os.urandom(600_000)
        (run / "traj.bin").write_bytes(big)
        assert (run / "traj.bin").read_bytes() == big
        assert sorted(p.name for p in run.iterdir()) == ["out.log", "sub", "traj.bin"]
        (run / "sub" / "deep.log").touch()
        assert sorted(str(p.relative_to(run)) for p in run.rglob("*.log")) == [
            "box:out.log",
            "box:sub/deep.log",
        ]
        moved = log.rename(run / "moved.log")
        assert moved.cluster == "box" and moved.read_text().startswith("E")
        with pytest.raises(FileNotFoundError):
            log.read_bytes()
        with pytest.raises(OSError):
            (run / "sub").rmdir()
        (run / "sub" / "deep.log").unlink()
        (run / "sub").rmdir()
        moved.unlink()
        moved.unlink(missing_ok=True)
        with pytest.raises(FileNotFoundError):
            moved.unlink()


@posix_only
def test_path_transfer_makes_destination_a_copy(tmp_path):
    if shutil.which("rsync") is None:
        pytest.skip("rsync is not installed")
    (tmp_path / "scratch").mkdir()
    source = tmp_path / "project"
    (source / "build").mkdir(parents=True)
    (source / "input.xyz").write_text("2\n")
    (source / "build" / "obj").write_text("skip")
    with Molq(registry=tmp_path / "registry.db") as client:
        client.clusters.register(name="box", scheduler="shell")
        remote = mq.Path(f"box:{tmp_path}/scratch/project", molq=client)
        result = mq.Path(source, molq=client).transfer(remote, "-a", "--exclude=build/")
        assert result["outcome"] == "applied"
        assert (remote / "input.xyz").read_text() == "2\n"
        assert not (remote / "build").exists()
        with pytest.raises(MolqError, match="Transport owns"):
            remote.transfer(mq.Path(tmp_path / "back"), "--rsync-path=sudo rsync")
