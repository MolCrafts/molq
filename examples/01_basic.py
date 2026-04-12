"""01_basic.py — Submit a single job and wait for it to finish.

Demonstrates:
  - Context-manager usage of Submitor
  - submit(argv=...)
  - handle.wait() blocking until terminal state
  - Inspecting JobRecord fields
"""

from molq.testing import make_submitor

with make_submitor("demo", job_duration=0) as s:
    handle = s.submit(argv=["echo", "hello"])

    print(f"job_id          : {handle.job_id}")
    print(f"scheduler_job_id: {handle.scheduler_job_id}")
    print(f"initial state   : {handle.status()}")

    record = handle.wait()

    print(f"final state     : {record.state}")
    print(f"exit_code       : {record.exit_code}")
    print(f"elapsed         : {record.finished_at - record.submitted_at:.3f}s")
