"""End-to-end demo: drive Dardel (KTH) over SSH from the local host.

Steps the script performs:

    1. Resolve the ``dardel.pdc.kth.se`` alias from ``~/.ssh/config``.
    2. Build a Slurm-backed Cluster wired through SshTransport.
    3. Create a Workspace under ``$HOME/molq-e2e`` on Dardel.
    4. Upload a small artifact (``payload.txt``).
    5. Submit a SLURM job that reads the artifact and emits an output file.
    6. Poll status until terminal.
    7. Tail logs over ssh, fetch them locally, and mirror the workspace.

The submission needs a SLURM ``--account`` (project allocation) on Dardel.
Pass it as ``MOLQ_DARDEL_ACCOUNT`` (and optionally ``MOLQ_DARDEL_PARTITION``,
``MOLQ_DARDEL_RESERVATION``).  The script prints what it skipped if the
required env vars are missing.

Run with::

    MOLQ_DARDEL_ACCOUNT=naissXXX-Y-ZZ python examples/dardel_e2e.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import molq
from molq import (
    Cluster,
    Duration,
    JobExecution,
    JobResources,
    JobScheduling,
    JobState,
    Memory,
    Submitor,
)

ALIAS = os.environ.get("MOLQ_DARDEL_ALIAS", "dardel.pdc.kth.se")
ACCOUNT = os.environ.get("MOLQ_DARDEL_ACCOUNT")
PARTITION = os.environ.get("MOLQ_DARDEL_PARTITION", "shared")
RESERVATION = os.environ.get("MOLQ_DARDEL_RESERVATION") or None


def main() -> int:
    if not ACCOUNT:
        print(
            "MOLQ_DARDEL_ACCOUNT is not set — Dardel requires a SLURM "
            "project allocation; aborting before submission.",
            file=sys.stderr,
        )
        return 2

    print(f"[1/8] resolving SSH alias {ALIAS!r} from ~/.ssh/config…")
    host = molq.resolve_ssh_host(ALIAS)
    print(f"      hostname={host.hostname}  user={host.user}  key={host.identity_file}")

    print("[2/8] building Cluster (slurm + ssh transport)…")
    cluster = Cluster.from_ssh_alias(ALIAS, scheduler="slurm", name="dardel")
    print(f"      {cluster!r}")

    print("[3/8] preparing remote workspace $HOME/molq-e2e…")
    home = cluster.transport.run(["sh", "-c", "echo $HOME"]).stdout.strip()
    workspace = cluster.get_workspace("e2e", path=f"{home}/molq-e2e")
    workspace.ensure()
    print(f"      remote path: {workspace.path}")

    with TemporaryDirectory() as tmp:
        local = Path(tmp)
        artifact = local / "payload.txt"
        artifact.write_text("hello dardel from molq\n")
        print(f"[4/8] uploading {artifact.name} → {workspace.path}/payload.txt")
        workspace.upload(str(artifact))

        print("[5/8] submitting a tiny SLURM job…")
        submitor = Submitor(target=cluster)
        try:
            scheduling = JobScheduling(
                partition=PARTITION,
                account=ACCOUNT,
                reservation=RESERVATION,
            )
            handle = submitor.submit_job(
                argv=[
                    "bash",
                    "-c",
                    "echo '== node =='; hostname; "
                    "echo '== payload =='; cat payload.txt; "
                    "echo '== result =='; date > result.txt; cat result.txt",
                ],
                resources=JobResources(
                    cpu_count=1,
                    memory=Memory.parse("1G"),
                    time_limit=Duration.parse("00:02:00"),
                ),
                scheduling=scheduling,
                execution=JobExecution(cwd=workspace.path, job_name="molq-e2e"),
            )
            print(
                f"      job_id={handle.job_id}  scheduler_job_id={handle.scheduler_job_id}"
            )

            print("[6/8] polling status until terminal…")
            for _ in range(60):  # up to ~5 min
                submitor.refresh_jobs()
                rec = submitor.get_job(handle.job_id)
                print(
                    f"      state={rec.state.value}  scheduler_id={rec.scheduler_job_id}"
                )
                if rec.state.is_terminal:
                    break
                time.sleep(5)

            print("[7/8] tailing remote stdout/stderr (last 40 lines each)…")
            stdout_path = rec.metadata.get("molq.stdout_path")
            stderr_path = rec.metadata.get("molq.stderr_path")
            for label, path in (("stdout", stdout_path), ("stderr", stderr_path)):
                if not path:
                    continue
                rel = path.removeprefix(workspace.path).lstrip("/")
                print(f"\n--- {label}: {path} ---")
                print(workspace.tail(rel, lines=40) or "(empty)")

            print("\n[8/8] fetching logs and mirroring the workspace locally…")
            log_dest = local / "logs"
            logs = submitor.fetch_logs(handle.job_id, dest_dir=log_dest)
            for stream, p in logs.items():
                print(f"      {stream}: {p} ({p.stat().st_size}B)")

            mirror_dest = Path("./dardel-e2e-mirror").resolve()
            workspace.mirror(mirror_dest)
            print(f"      mirror at: {mirror_dest}")
            for f in sorted(mirror_dest.rglob("*")):
                if f.is_file():
                    print(
                        f"        - {f.relative_to(mirror_dest)} ({f.stat().st_size}B)"
                    )

            return 0 if rec.state == JobState.SUCCEEDED else 1
        finally:
            submitor.close()


if __name__ == "__main__":
    raise SystemExit(main())
