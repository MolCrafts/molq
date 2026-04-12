# molq

[![CI](https://github.com/MolCrafts/molq/actions/workflows/ci.yml/badge.svg)](https://github.com/MolCrafts/molq/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/molcrafts-molq.svg)](https://pypi.org/project/molcrafts-molq/)
[![Python](https://img.shields.io/badge/python-3.10%2B-3776AB.svg?logo=python&logoColor=white)](./pyproject.toml)
[![License](https://img.shields.io/badge/license-MIT-16A34A.svg)](./LICENSE)

Unified job queue for Python workloads that need the same submission API on a laptop, workstation, or HPC cluster.

## Quick Start

```bash
pip install molcrafts-molq
```

```python
from molq import Duration, JobResources, Memory, Submitor

local = Submitor("devbox", "local")

job = local.submit(
    argv=["python", "train.py"],
    resources=JobResources(
        cpu_count=4,
        memory=Memory.gb(8),
        time_limit=Duration.hours(2),
    ),
)

record = job.wait()
assert record.state.value == "succeeded"
```

## Features

- One `Submitor` API for `local`, `slurm`, `pbs`, and `lsf`
- Typed submission inputs with `Memory`, `Duration`, `Script`, `JobResources`, `JobScheduling`, and `JobExecution`
- SQLite-backed persistence with WAL mode and UUID job identities
- Reconciliation and blocking waits through `JobReconciler` and `JobMonitor`
- Rich CLI for `submit`, `list`, `status`, `watch`, `logs`, `monitor`, and `cancel`
- Default stdout/stderr capture for every submitted job

## Documentation

- [Getting Started](docs/getting-started.md) for installation and first-job examples
- [Scheduler Guide](docs/schedulers.md) for backend capabilities and scheduler options
- [Monitoring Guide](docs/monitoring.md) for lifecycle, polling, and dashboards
- [API Reference](docs/api.md) for the exported classes and functions
- [CLI Reference](docs/cli.md) for command-line usage

---

Built by [MolCrafts](https://github.com/MolCrafts)
