"""06_defaults.py — Cluster-wide defaults with per-job overrides.

Demonstrates:
  - SubmitorDefaults: apply resources/scheduling/execution to every submit()
  - Per-job values override the defaults (deep merge, not replace)
"""

from molq.models import SubmitorDefaults
from molq.testing import make_submitor
from molq.types import Duration, JobExecution, JobResources, JobScheduling, Memory

cluster_defaults = SubmitorDefaults(
    resources=JobResources(
        cpu_count=4,
        memory=Memory.gb(16),
        time_limit=Duration.hours(2),
    ),
    scheduling=JobScheduling(
        partition="normal",
        account="lab-budget",
    ),
    execution=JobExecution(
        env={"PYTHONUNBUFFERED": "1"},
    ),
)

with make_submitor("hpc", job_duration=0) as s:
    s._defaults = cluster_defaults  # inject defaults for this demo

    # Uses all defaults
    h1 = s.submit_job(argv=["python", "eval.py"])
    r1 = h1.wait()
    print(f"default job  → {r1.state}")

    # Override: request a GPU queue and more memory; other defaults still apply
    h2 = s.submit_job(
        argv=["python", "train.py"],
        resources=JobResources(
            cpu_count=8,
            memory=Memory.gb(64),
            gpu_count=1,
            time_limit=Duration.hours(12),
        ),
        scheduling=JobScheduling(partition="gpu"),
    )
    r2 = h2.wait()
    print(f"override job → {r2.state}")
