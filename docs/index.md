# Molq: Unified Job Queue

Molq provides a single Python API for submitting, monitoring, and managing computational jobs across local execution and HPC cluster schedulers.

## Why Molq?

Working with compute clusters typically involves writing shell scripts, juggling scheduler-specific flags, and building ad-hoc monitoring tools. Molq replaces this with a typed Python interface:

- Write once, run anywhere -- local, SLURM, PBS, or LSF
- Typed resource specs prevent misconfigured submissions
- SQLite-backed persistence tracks job lifecycle automatically
- Pluggable monitoring with exponential backoff

## Quick Example

```python
from molq import Submitor, JobResources, Memory, Duration

# Create a submitor targeting a SLURM cluster
cluster = Submitor("hpc", "slurm")

# Submit a job with typed resource specs
job = cluster.submit(
    argv=["python", "train.py"],
    resources=JobResources(
        cpu_count=8,
        memory=Memory.gb(32),
        time_limit=Duration.hours(4),
    ),
)

# Block until completion
record = job.wait()
print(record.state)  # JobState.SUCCEEDED
```

## Getting Started

- **[Installation & First Job](tutorial/getting-started.md)** -- Up and running in 5 minutes
- **[Core Concepts](tutorial/core-concepts.md)** -- Architecture and design patterns
- **[API Reference](api/index.md)** -- Complete type and class documentation
- **[Recipes](recipes/machine-learning.md)** -- Real-world examples
