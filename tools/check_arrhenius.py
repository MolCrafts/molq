"""Exercise MolQ using an explicitly selected Arrhenius CPU allocation."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

from molq import (
    ApptainerEnvironment,
    ApptainerMount,
    ArtifactManifest,
    Catalog,
    JobSpec,
    Molq,
    MolqError,
    Resources,
    Scheduling,
    Workspace,
)
from molq import (
    Path as ClusterPath,
)

PAYLOAD = "molq: Arrhenius input"
LOG_LINE = "molq: Apptainer ran the staged task"
SCRIPT = r"""set -eu
test -n "${APPTAINER_CONTAINER:-}"
test "$(pwd)" = /work
test "$1" = 'argument with spaces & symbols'
cp inputs/input.txt outputs/selected.txt
printf '%s\n' 'keep on the target' > outputs/other.txt
printf '%s\n' '{"outputs":{"selected":"selected.txt","other":"other.txt"}}' > outputs/catalog.json
printf '%s\n' 'molq: Apptainer ran the staged task'
"""


def run(args: argparse.Namespace, evidence: Path) -> dict:
    registry = evidence / "registry.db"
    config = evidence / "config.json"
    config.write_text("{}\n")
    key = f"molq-arrhenius-{uuid4().hex}"
    root = args.project_root / key
    settings = {
        "registry": registry,
        "config": config,
        "timeout": max(130, args.transfer_deadline + 15),
    }

    def cli(*words: str) -> dict:
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "molq",
                "--registry",
                str(registry),
                "--config",
                str(config),
                *words,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if not process.stdout:
            raise RuntimeError(
                process.stderr.strip() or f"CLI exited {process.returncode}"
            )
        envelope = json.loads(process.stdout)
        if process.returncode:
            raise MolqError.from_wire(envelope["error"]["data"])
        return envelope["result"]

    with Molq(**settings) as mq:
        mq.clusters.register(
            name=args.cluster,
            scheduler="slurm",
            transport={"kind": "ssh", "alias": args.ssh_alias},
        )
        project = ClusterPath(f"{args.cluster}:{root}", molq=mq)
        image = ClusterPath(f"{args.cluster}:{args.sif}", molq=mq)
        if not image.is_file():
            raise FileNotFoundError(str(image))
        if not project.parent.is_dir():
            raise FileNotFoundError(str(project.parent))
        project.mkdir()
        workspace = Workspace(project)
        workspace.path("inputs").mkdir()
        workspace.path("outputs").mkdir()
        payload = evidence / "payload"
        payload.mkdir()
        (payload / "run.sh").write_text(SCRIPT)
        workspace.upload(
            ClusterPath(payload), "inputs", "-a", timeout=args.transfer_deadline
        )
        remote_input = str(workspace.path("inputs/input.txt"))
        environment = ApptainerEnvironment(
            image=image.as_posix(),
            mounts=(ApptainerMount(project.as_posix(), "/work"),),
            workdir="/work",
        )
        command = ["/bin/sh", "inputs/run.sh", "argument with spaces & symbols"]
        spec = JobSpec.argv(
            command,
            environment=environment,
            scheduling=Scheduling(
                name=key, account=args.account, partition=args.partition
            ),
            resources=Resources(tasks=1, cpus_per_task=1, time_limit_seconds=120),
        )
        expected = mq.cluster(args.cluster, project_root=workspace.path()).preview(spec)

    local_input = evidence / "input.txt"
    local_input.write_text(PAYLOAD + "\n")
    cli(
        "transfer",
        "--transfer-deadline",
        str(args.transfer_deadline),
        "-a",
        str(local_input),
        remote_input,
    )
    job_args = [
        args.cluster,
        "--project-root",
        str(root),
        "--name",
        key,
        "--account",
        args.account,
        "--partition",
        args.partition,
        "--tasks",
        "1",
        "--cpus-per-task",
        "1",
        "--time-limit-seconds",
        "120",
    ]
    argv = environment.wrap(command)
    preview = cli("job", "preview", *job_args, "--", *argv)
    (evidence / "preview.sh").write_text(preview["script"])
    if preview["spec"] != expected["spec"]:
        raise RuntimeError("SDK and CLI normalized job specs disagree")
    submit_args = ["job", "submit", *job_args, "--request-key", key, "--", *argv]
    (evidence / "submit-args.json").write_text(json.dumps(submit_args, indent=2) + "\n")
    submitted = cli(*submit_args)
    (evidence / "ref.json").write_text(json.dumps(submitted["ref"]) + "\n")

    with Molq(**settings) as mq:
        job = mq.job(submitted["ref"])
        status = job.wait(timeout=args.wait_timeout, interval=5)
        (evidence / "status.json").write_text(json.dumps(status, indent=2) + "\n")
        logs = job.logs()["text"]
        (evidence / "stdout.log").write_text(logs)
        (evidence / "stderr.log").write_text(job.logs(stream="stderr")["text"])
        if not status["completion"]["successful"] or LOG_LINE not in logs:
            raise RuntimeError("The Slurm job or its container check failed")

        workspace = Workspace(ClusterPath(f"{args.cluster}:{root}", molq=mq))
        destination = evidence / "selected"
        destination.mkdir()
        delivery = workspace.download(
            ArtifactManifest({"results": Catalog("catalog.json", "outputs")}),
            ClusterPath(destination),
            relative="outputs",
            select=("results/selected",),
            timeout=args.transfer_deadline,
        )
        if (
            delivery.paths != ("selected.txt",)
            or sorted(p.name for p in destination.iterdir()) != ["selected.txt"]
            or (destination / "selected.txt").read_bytes() != local_input.read_bytes()
            or not workspace.path("outputs/other.txt").is_file()
        ):
            raise RuntimeError(
                "Named download did not preserve the selected bytes and remote output"
            )
        remote_selected = str(workspace.path("outputs/selected.txt"))
    cli_copy = evidence / "cli-selected.txt"
    cli(
        "transfer",
        "--transfer-deadline",
        str(args.transfer_deadline),
        "-a",
        remote_selected,
        str(cli_copy),
    )
    if cli_copy.read_bytes() != local_input.read_bytes():
        raise RuntimeError("CLI download changed the selected bytes")
    return {
        "ref": submitted["ref"],
        "remote_workspace": str(root),
        "request_key": key,
        "selected": list(delivery.paths),
        "local_evidence": str(evidence),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("ssh-alias", "cluster", "partition", "account"):
        parser.add_argument("--" + option, required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--sif", type=Path, required=True)
    parser.add_argument("--wait-timeout", type=int, default=600)
    parser.add_argument("--transfer-deadline", type=int, default=90)
    args = parser.parse_args()
    if not args.project_root.is_absolute() or not args.sif.is_absolute():
        parser.error("--project-root and --sif must be absolute target paths")
    if args.wait_timeout <= 0 or not 1 <= args.transfer_deadline <= 3600:
        parser.error(
            "Expected a positive wait timeout and a transfer deadline of 1–3600 seconds"
        )
    evidence = Path(tempfile.mkdtemp(prefix="molq-arrhenius-"))
    print(f"Local registry and evidence: {evidence}", file=sys.stderr, flush=True)
    try:
        print(json.dumps({"result": run(args, evidence)}))
    except (MolqError, OSError, ValueError, RuntimeError) as error:
        detail = (
            error.to_wire() if isinstance(error, MolqError) else {"message": str(error)}
        )
        print(json.dumps({"error": detail, "local_evidence": str(evidence)}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
