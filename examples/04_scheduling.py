"""04_scheduling.py — Scheduler-specific parameters.

Demonstrates:
  - JobScheduling: queue, account, priority, node_count, exclusive_node
  - array_spec for array jobs
  - dependency between jobs
"""

from molq.testing import make_submitor
from molq.types import JobScheduling

with make_submitor("hpc", job_duration=0) as s:
    # Basic queue / account
    h = s.submit(
        argv=["python", "preprocess.py"],
        scheduling=JobScheduling(
            queue="normal",
            account="project-123",
        ),
    )
    print(f"basic       → {h.wait().state}")

    # Exclusive node, custom QoS
    h = s.submit(
        argv=["mpirun", "-np", "128", "simulation"],
        scheduling=JobScheduling(
            queue="mpi",
            node_count=4,
            exclusive_node=True,
            qos="high",
        ),
    )
    print(f"exclusive   → {h.wait().state}")

    # Dependency: run after a previous job
    first = s.submit(argv=["python", "stage1.py"])
    first_record = first.wait()

    second = s.submit(
        argv=["python", "stage2.py"],
        scheduling=JobScheduling(
            dependency=f"afterok:{first_record.scheduler_job_id}",
        ),
    )
    print(f"dependency  → {second.wait().state}")

    # Array job (SLURM: --array=0-9)
    h = s.submit(
        argv=["python", "task.py"],
        scheduling=JobScheduling(array_spec="0-9"),
    )
    print(f"array       → {h.wait().state}")
