"""05_execution.py — Execution environment settings.

Demonstrates:
  - JobExecution: env vars, cwd, job_name, output_file, error_file
"""

import tempfile
from pathlib import Path

from molq.testing import make_submitor
from molq.types import JobExecution

with tempfile.TemporaryDirectory() as tmpdir:
    workdir = Path(tmpdir) / "workdir"
    logdir = Path(tmpdir) / "logs"
    workdir.mkdir()
    logdir.mkdir()

    with make_submitor("demo", job_duration=0) as s:
        handle = s.submit_job(
            argv=["python", "train.py", "--epochs", "100"],
            execution=JobExecution(
                env={
                    "CUDA_VISIBLE_DEVICES": "0,1",
                    "OMP_NUM_THREADS": "8",
                    "WANDB_PROJECT": "my-experiment",
                },
                cwd=workdir,
                job_name="train-run-001",
                output_file=str(logdir / "train.out"),
                error_file=str(logdir / "train.err"),
            ),
        )

        record = handle.wait()
        print(f"job_name : {record.command_display}")
        print(f"state    : {record.state}")
        print(f"cwd      : {record.cwd}")
