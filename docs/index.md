# molq

Unified job queue for Python services, research scripts, and internal tooling that need one submission interface across local execution and HPC schedulers.

[Get started](getting-started.md){ .md-button .md-button--primary }
[API reference](api.md){ .md-button }

## What it gives you

- One `Submitor` API for `local`, `slurm`, `pbs`, and `lsf`
- Typed resources and execution models instead of backend-specific string munging
- SQLite persistence with WAL mode and durable job history
- Reconciliation, blocking waits, and pluggable polling strategies
- Rich terminal workflows for logs, status, watch, and full-screen monitoring

## Quick example

```python
from molq import Duration, JobResources, Memory, Submitor

cluster = Submitor("hpc", "slurm")

job = cluster.submit(
    argv=["python", "train.py"],
    resources=JobResources(
        cpu_count=8,
        memory=Memory.gb(32),
        time_limit=Duration.hours(4),
    ),
)

record = job.wait()
assert record.state.value in {"succeeded", "failed", "cancelled"}
```

## Documentation map

- [Getting Started](getting-started.md) — installation, first job, and CLI basics
- [Schedulers](schedulers.md) — backend matrix and scheduler option classes
- [Monitoring](monitoring.md) — lifecycle, reconciliation, polling, dashboards, and logs
- [CLI Reference](cli.md) — command reference for `molq`
- [API Reference](api.md) — exported classes, enums, options, and errors
- [Release Notes](release-notes.md) — current release series summary
