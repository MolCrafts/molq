"""08_error_handling.py — Handling failures, timeouts, and cancellation.

Demonstrates:
  - A job that fails: inspect record.state and record.failure_reason
  - MolqTimeoutError when wait() times out
  - JobNotFoundError when looking up a non-existent job
  - Cancelling a running job
  - CommandError for invalid submit() calls
"""

from molq.errors import CommandError, JobNotFoundError, MolqTimeoutError
from molq.status import JobState
from molq.testing import make_submitor

# ── 1. Failed job ─────────────────────────────────────────────────────────────
print("=== 1. Failed job ===")
with make_submitor("demo", outcomes="failed", job_duration=0) as s:
    record = s.submit(argv=["python", "broken.py"]).wait()
    print(f"state   : {record.state}")              # JobState.FAILED
    assert record.state == JobState.FAILED

# ── 2. Timeout ────────────────────────────────────────────────────────────────
print("\n=== 2. Timeout ===")
with make_submitor("demo", job_duration=60) as s:   # job takes 60 s
    handle = s.submit(argv=["python", "slow.py"])
    try:
        handle.wait(timeout=0.05)                   # give up after 50 ms
    except MolqTimeoutError as exc:
        print(f"timed out as expected: {exc}")

# ── 3. JobNotFoundError ───────────────────────────────────────────────────────
print("\n=== 3. JobNotFoundError ===")
with make_submitor("demo") as s:
    try:
        s.get("00000000-0000-0000-0000-000000000000")
    except JobNotFoundError as exc:
        print(f"not found: {exc.job_id[:8]}…")

# ── 4. Cancellation ───────────────────────────────────────────────────────────
print("\n=== 4. Cancellation ===")
with make_submitor("demo", job_duration=60) as s:
    handle = s.submit(argv=["python", "long_job.py"])
    s.cancel(handle.job_id)
    record = s.get(handle.job_id)
    print(f"state after cancel: {record.state}")    # CANCELLED

# ── 5. CommandError ───────────────────────────────────────────────────────────
print("\n=== 5. CommandError ===")
with make_submitor("demo") as s:
    try:
        s.submit(argv=["echo"], command="echo")     # two commands at once
    except CommandError as exc:
        print(f"CommandError: {exc}")

    try:
        s.submit(command="echo hello\necho world")  # newline in command
    except CommandError as exc:
        print(f"CommandError: {exc}")
