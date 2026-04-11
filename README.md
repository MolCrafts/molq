# Molq: Unified Job Queue for Local and HPC Execution

[![Tests](https://github.com/molcrafts/molq/workflows/Tests/badge.svg)](https://github.com/molcrafts/molq/actions)
[![PyPI version](https://badge.fury.io/py/molcrafts-molq.svg)](https://badge.fury.io/py/molcrafts-molq)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Molq** provides a single Python API for submitting, monitoring, and managing computational jobs across local execution and HPC cluster schedulers (SLURM, PBS, LSF).

## Key Features

- **Unified `Submitor` interface** -- one API for local, SLURM, PBS, and LSF
- **Immutable, typed specs** -- `JobResources`, `Memory`, `Duration`, `Script` as frozen dataclasses
- **SQLite persistence** -- WAL-mode job store with UUID identity and schema versioning
- **Pluggable monitoring** -- exponential backoff, fixed, and adaptive polling strategies
- **Rich CLI** -- Typer + Rich for `submit`, `list`, `watch`, `status`, `cancel`

## Quick Start

### Installation

```bash
pip install molcrafts-molq
```

### Python API

```python
from molq import Submitor, JobResources, Memory, Duration

# Create a submitor for local execution
local = Submitor("devbox", "local")

# Submit a job
job = local.submit(
    argv=["python", "train.py"],
    resources=JobResources(
        cpu_count=8,
        memory=Memory.gb(32),
        time_limit=Duration.hours(4),
    ),
)

# Check status (cached, no I/O)
print(job.status())

# Block until completion
record = job.wait()
print(record.state)       # JobState.SUCCEEDED
print(record.exit_code)   # 0
```

### Cluster Submission

```python
from molq import Submitor, JobResources, JobScheduling, Memory, Duration

cluster = Submitor("hpc", "slurm")

job = cluster.submit(
    argv=["python", "train.py"],
    resources=JobResources(
        cpu_count=16,
        memory=Memory.gb(64),
        gpu_count=2,
        time_limit=Duration.hours(8),
    ),
    scheduling=JobScheduling(queue="gpu", account="project123"),
)

record = job.wait(timeout=3600)
```

### Script-Based Submission

```python
from molq import Submitor, Script

local = Submitor("devbox", "local")

# Inline script
job = local.submit(script=Script.inline("""
cd /workspace
python preprocess.py
python train.py --epochs 100
"""))

# External script file
job = local.submit(script=Script.path("run_experiment.sh"))
```

### CLI

```bash
# Submit a local job
molq submit local echo "Hello World"

# Submit to SLURM with resources
molq submit slurm --cpus 8 --mem 32G --time 4h python train.py

# List active jobs
molq list local

# Watch a job until completion
molq watch <job-id> local

# Cancel a job
molq cancel <job-id> local
```

## Supported Backends

| Backend | Description | Status |
|---------|-------------|--------|
| `local` | Local subprocess execution | Stable |
| `slurm` | SLURM workload manager | Stable |
| `pbs` | PBS/Torque scheduler | Stable |
| `lsf` | IBM Spectrum LSF | Stable |

## Architecture

```
Submitor  ──>  Scheduler (Protocol)  ──>  Local | SLURM | PBS | LSF
    │
    ├── JobStore (SQLite + WAL)
    ├── JobReconciler (scheduler <-> DB sync)
    └── JobMonitor (polling + strategies)
```

## Documentation

- **[Getting Started](https://molcrafts.github.io/molq/tutorial/getting-started/)** -- Installation and first job
- **[Core Concepts](https://molcrafts.github.io/molq/tutorial/core-concepts/)** -- Architecture and design
- **[API Reference](https://molcrafts.github.io/molq/api/)** -- Complete API documentation
- **[Recipes](https://molcrafts.github.io/molq/recipes/machine-learning/)** -- Real-world examples

## License

MIT License. See [LICENSE](LICENSE) for details.
