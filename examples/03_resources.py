"""03_resources.py — Specifying hardware resources.

Demonstrates:
  - Memory factory methods and scheduler-format conversions
  - Duration factory methods and scheduler-format conversions
  - JobResources: cpu_count, memory, gpu_count, gpu_type, time_limit
"""

from molq.testing import make_submitor
from molq.types import Duration, JobResources, Memory

# ── Memory ────────────────────────────────────────────────────────────────────
mem = Memory.gb(32)
print(f"Memory : {mem.bytes} bytes → SLURM: {mem.to_slurm()} | PBS: {mem.to_pbs()}")

mem2 = Memory.parse("512MB")
print(f"Parsed : {mem2.to_slurm()}")

# ── Duration ──────────────────────────────────────────────────────────────────
dur = Duration.hours(4)
print(f"Duration: {dur.seconds}s → SLURM: {dur.to_slurm()} | PBS: {dur.to_pbs()}")

dur2 = Duration.parse("2h30m")
print(f"Parsed  : {dur2.to_slurm()}")

# ── Submit with full resource spec ────────────────────────────────────────────
with make_submitor("hpc", job_duration=0) as s:
    handle = s.submit_job(
        argv=["python", "train.py"],
        resources=JobResources(
            cpu_count=16,
            memory=Memory.gb(64),
            gpu_count=2,
            gpu_type="a100",
            time_limit=Duration.hours(8),
        ),
    )
    record = handle.wait()
    print(f"\nJob {record.job_id[:8]}… → {record.state}")
