"""07_batch.py — Submit many jobs and wait for all of them.

Demonstrates:
  - Submitting a list of jobs in a loop
  - submitor.watch_jobs([job_ids]) to block until all are terminal
  - Iterating over final JobRecords
"""

from molq.testing import make_submitor

PARAMS = [
    {"lr": 0.001, "epochs": 10},
    {"lr": 0.01, "epochs": 20},
    {"lr": 0.1, "epochs": 30},
    {"lr": 0.0001, "epochs": 50},
]

with make_submitor("hpc", job_duration=0.02) as s:
    handles = [
        s.submit_job(
            argv=["python", "train.py", f"--lr={p['lr']}", f"--epochs={p['epochs']}"],
            metadata={"lr": str(p["lr"]), "epochs": str(p["epochs"])},
        )
        for p in PARAMS
    ]

    print(f"Submitted {len(handles)} jobs, waiting…")

    records = s.watch_jobs([h.job_id for h in handles])

    for rec in records:
        lr = rec.metadata.get("lr", "?")
        print(f"  lr={lr:<8} → {rec.state}  ({rec.job_id[:8]}…)")

    succeeded = sum(1 for r in records if r.state.value == "succeeded")
    print(f"\n{succeeded}/{len(records)} jobs succeeded")
