# molq

Unified job queue for Python services, research scripts, and internal tooling that need one submission interface across local execution and HPC schedulers — local subprocess or remote over SSH, without changing your code.

[Get started](getting-started.md){ .md-button .md-button--primary }
[Concepts](concepts.md){ .md-button }
[API reference](api.md){ .md-button }

## What it gives you

- A two-axis model: **`Cluster`** (where) × **`Submitor`** (lifecycle) — orthogonal, composable
- Schedulers for `local`, `slurm`, `pbs`, `lsf`, `shell` — pick any with a string
- Local **or** SSH execution for any scheduler, via the `Transport` axis (no new deps; uses your `~/.ssh/config`)
- Typed resources and execution models instead of scheduler-specific string munging
- SQLite persistence with WAL mode and durable job history
- Reconciliation, blocking waits, and pluggable polling strategies
- Live queue introspection (`cluster.get_queue()` parses `squeue --me` / `qstat -u $USER` / `bjobs`)
- Rich terminal workflows for logs, status, watch, and full-screen monitoring

## Quick example

```python
import molq as mq

# Cluster = destination (where to run).  Submitor = lifecycle (how jobs are tracked).
cluster  = mq.Cluster("hpc", "slurm", host="user@hpc.example.com")
submitor = mq.Submitor(target=cluster)

handle = submitor.submit_job(
    argv=["python", "train.py"],
    resources=mq.JobResources(
        cpu_count=8,
        memory=mq.Memory.gb(32),
        time_limit=mq.Duration.hours(4),
    ),
    scheduling=mq.JobScheduling(partition="gpu"),
)

record = handle.wait()
assert record.state.value in {"succeeded", "failed", "cancelled"}

# Live scheduler view — independent of molq's own records.
print(cluster.get_queue())
```

## Documentation map

- [Concepts](concepts.md) — Cluster, Submitor, Scheduler, Transport, Workspace, Project — what each owns and how they fit together
- [Getting Started](getting-started.md) — installation, first job, and CLI basics
- [Schedulers](schedulers.md) — scheduler matrix and scheduler option classes
- [Monitoring](monitoring.md) — lifecycle, reconciliation, polling, dashboards, and logs
- [CLI Reference](cli.md) — command reference for `molq`
- [API Reference](api.md) — exported classes, enums, options, and errors
- [Release Notes](release-notes.md) — current release series summary
