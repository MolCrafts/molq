# molq

[![CI](https://github.com/MolCrafts/molq/actions/workflows/ci.yml/badge.svg)](https://github.com/MolCrafts/molq/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/molcrafts-molq.svg)](https://pypi.org/project/molcrafts-molq/)
[![Python](https://img.shields.io/badge/python-3.12%2B-3776AB.svg?logo=python&logoColor=white)](./pyproject.toml)
[![License](https://img.shields.io/badge/license-MIT-16A34A.svg)](./LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)

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
- First-class retry lineage with persisted attempt history
- Molq job-id dependencies and inspectable dependency metadata
- Profiles from `~/.molq/config.toml` plus reusable defaults
- Cleanup and lightweight daemon workflows for retention and reconciliation
- Event hooks through `EventBus`
- Rich CLI for `submit`, `list`, `status`, `watch`, `logs`, `history`, `inspect`, `cleanup`, `daemon`, `monitor`, and `cancel`
- Default stdout/stderr capture for every submitted job

## Retry and Dependency Example

```python
from molq import RetryBackoff, RetryPolicy, Submitor

slurm = Submitor("hpc", "slurm")

train = slurm.submit(
    argv=["python", "train.py"],
    retry=RetryPolicy(
        max_attempts=3,
        backoff=RetryBackoff(initial_seconds=10, maximum_seconds=60),
    ),
)

eval_job = slurm.submit(
    argv=["python", "eval.py"],
    after_success=[train.job_id],
)
```

## Profile Example

`~/.molq/config.toml`

```toml
[profiles.gpu]
scheduler = "slurm"
cluster_name = "hpc"

[profiles.gpu.defaults.resources]
cpu_count = 8
memory = "34359738368"

[profiles.gpu.defaults.scheduling]
queue = "gpu"

[profiles.gpu.retry]
max_attempts = 3
```

CLI usage:

```bash
molq submit slurm --profile gpu python train.py
molq daemon slurm --profile gpu --once
molq cleanup slurm --profile gpu --dry-run
```

## Documentation

- [Getting Started](docs/getting-started.md) for installation and first-job examples
- [Scheduler Guide](docs/schedulers.md) for backend capabilities and scheduler options
- [Monitoring Guide](docs/monitoring.md) for lifecycle, polling, and dashboards
- [API Reference](docs/api.md) for the exported classes and functions
- [CLI Reference](docs/cli.md) for command-line usage

---

Built by [MolCrafts](https://github.com/MolCrafts) with love.
