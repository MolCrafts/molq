"""The Python SDK composes commands and explicit delivery without a job."""

import shutil
import sys

import pytest

from molq import (
    CondaEnvironment,
    CopyArtifact,
    DockerEnvironment,
    DockerMount,
    JobSpec,
    Molq,
    MolqError,
    Path,
    SharedPathArtifact,
    UvEnvironment,
)


def test_environments_keep_literal_arguments_and_prepared_resources():
    command = ["python", "script with spaces.py", "$literal; no shell", ""]
    conda = CondaEnvironment(prefix="/shared/prepared env")
    uv = UvEnvironment("/shared/project with spaces")
    assert conda.wrap(command) == [
        "conda",
        "run",
        "--no-capture-output",
        "--prefix=/shared/prepared env",
        "--",
        *command,
    ]
    assert uv.wrap(command) == [
        "uv",
        "run",
        "--no-sync",
        "--no-python-downloads",
        "--offline",
        "--project",
        "/shared/project with spaces",
        "--",
        *command,
    ]
    docker = DockerEnvironment(
        "local/image:1",
        mounts=(DockerMount("/shared/path with spaces", "/data", read_only=True),),
        workdir="/data",
        env=(("PARAM", "$literal; value"),),
    )
    wrapped = docker.wrap(command)
    assert wrapped == [
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "--mount",
        "type=bind,source=/shared/path with spaces,target=/data,readonly",
        "--workdir",
        "/data",
        "--env",
        "PARAM=$literal; value",
        "--entrypoint",
        "python",
        "local/image:1",
        *command[1:],
    ]
    assert (
        JobSpec.argv(wrapped).to_wire()["execution"]["units"][0]["command"]["argv"]
        == wrapped
    )


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file transfer")
def test_artifact_delivery_uses_existing_transfer_and_cluster_stat(tmp_path):
    if shutil.which("rsync") is None:
        pytest.skip("rsync is not installed")
    inputs = tmp_path / "input"
    inputs.mkdir()
    (inputs / "one.txt").write_text("payload")
    with Molq(registry=tmp_path / "registry.db") as client:
        client.clusters.register(name="box", scheduler="shell")
        remote = Path(f"box:{tmp_path}/staged", molq=client)
        delivered = CopyArtifact(Path(inputs, molq=client), remote, ("-a",)).deliver()
        assert delivered.location == remote
        assert delivered.transfer_result["outcome"] == "applied"
        assert (remote / "one.txt").read_text() == "payload"

        reference = SharedPathArtifact(remote / "one.txt").deliver()
        assert reference.location == remote / "one.txt"
        assert reference.transfer_result is None
        with pytest.raises(FileNotFoundError):
            SharedPathArtifact(remote / "missing").deliver()

        local = Path(tmp_path / "result.txt", molq=client)
        with pytest.raises(MolqError, match="Transport owns"):
            CopyArtifact(
                reference.location, local, ("--rsync-path=alternate",)
            ).deliver()
        assert not local.exists()
        assert CopyArtifact(reference.location, local).deliver().location == local
        assert local.read_text() == "payload"


def test_copy_artifact_propagates_uncertain_outcome_once(mocker):
    failure = MolqError("OUTCOME_UNKNOWN", "connection interrupted", outcome="unknown")
    transfer = mocker.patch.object(Path, "transfer", side_effect=failure)
    artifact = CopyArtifact(Path("hpc:/shared/out"), Path("/tmp/result"), ("-a",))
    with pytest.raises(MolqError) as caught:
        artifact.deliver()
    assert caught.value is failure
    transfer.assert_called_once_with(
        artifact.destination, "-a", timeout=90, directory=None
    )


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX local execution")
def test_stage_once_run_twice_and_deliver_separately(tmp_path):
    if shutil.which("rsync") is None:
        pytest.skip("rsync is not installed")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = tmp_path / "inputs"
    source.mkdir()
    (source / "run.py").write_text(
        "import pathlib, sys\npathlib.Path(sys.argv[1]).write_text(sys.argv[2])\n"
    )
    launcher = tmp_path / "prepared conda"
    launcher.write_text('#!/bin/sh\nshift 4\nexec "$@"\n')
    launcher.chmod(0o700)
    env = CondaEnvironment(name="prepared", executable=str(launcher))
    with Molq(registry=tmp_path / "registry.db") as client:
        box = client.clusters.register(name="box", scheduler="shell")
        shared = Path(f"box:{workspace}", molq=client)
        inputs = shared / "inputs"
        CopyArtifact(Path(source, molq=client), inputs, ("-a",)).deliver()
        jobs = []
        for sample in ("a", "b"):
            output = shared / f"{sample}.txt"
            argv = env.wrap(
                [
                    sys.executable,
                    (inputs / "run.py").as_posix(),
                    output.as_posix(),
                    sample,
                ]
            )
            jobs.append((box.submit(JobSpec.argv(argv)), output, sample))
        for job, output, sample in jobs:
            assert job.wait(timeout=15, interval=0.05)["completion"]["successful"]
            destination = Path(tmp_path / "downloads" / output.name, molq=client)
            delivery = CopyArtifact(output, destination)
            if sample == "a":
                with pytest.raises(MolqError):
                    delivery.deliver()
                (tmp_path / "downloads").mkdir()
            assert delivery.deliver().location.read_text() == sample


def test_copy_of_serve_host_directory_skips_client_filesystem_probe(mocker):
    client = mocker.Mock()
    stat = mocker.patch.object(
        Path, "is_dir", side_effect=AssertionError("client stat")
    )
    source = Path("/serve-only/input", molq=client)
    target = Path("hpc:/shared/input", molq=client)
    CopyArtifact(source, target, ("-a",), directory=True).deliver()
    stat.assert_not_called()
    client.rpc.assert_called_once_with(
        "files.transfer",
        sources=[{"path": "/serve-only/input/"}],
        destination={"cluster": "hpc", "path": "/shared/input/"},
        options=["-a"],
        timeout_seconds=90,
    )
