"""09_cluster.py — Cluster + Submitor: orthogonal destination & lifecycle.

Demonstrates the two-axis model:
  - Cluster owns the destination (transport, scheduler kind, options)
  - Submitor owns the lifecycle (store, monitor, reconciler, defaults)
  - cluster.get_queue() returns the scheduler's current queue snapshot
  - cluster.get_workspace() / .get_project() are remote-dir handles

For a remote (SSH) cluster you'd write:

    cluster = mq.Cluster(
        "hpc",
        scheduler="slurm",
        host="user@hpc.example.com",
    )
"""

import tempfile

import molq as mq

with tempfile.TemporaryDirectory() as tmp:
    # 1) Define the destination — local in this demo.
    cluster = mq.Cluster("dev", "local")

    # 2) Bind a Submitor to it.  All lifecycle ops go through the submitor.
    with mq.Submitor(target=cluster) as submitor:
        # 3) Snapshot the cluster queue (empty for local).
        print(f"queue snapshot   : {cluster.get_queue()}")

        # 4) Submit a job and inspect the record.
        handle = submitor.submit_job(argv=["echo", "hello from cluster"])
        record = handle.wait()
        print(f"final state      : {record.state}")
        print(f"exit_code        : {record.exit_code}")

        # 5) Workspace + Project: remote-style directory handles.
        ws = cluster.get_workspace("scratch", path=tmp)
        proj = ws.get_project("alpha")
        proj.ensure()  # mkdir -p tmp/alpha

        # Project.submit_job is sugar — overrides cwd to proj.path.
        h2 = proj.submit_job(submitor, argv=["pwd"])
        rec = h2.wait()
        print(f"project cwd      : {rec.cwd}")
