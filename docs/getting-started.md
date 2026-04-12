# Getting Started

## Installation

```bash
pip install molcrafts-molq
```

The PyPI distribution name is `molcrafts-molq`; the import name is `molq`.

For local development:

```bash
pip install -e ".[dev]"
pip install -e ".[docs]"
```

## First Submitor

`Submitor` is the single entry point for job submission and job management.
Start with the local backend so you can validate your workflow without a cluster.

```python
from molq import Submitor

local = Submitor("devbox", "local")
```

The first argument is a cluster namespace. It scopes listings and reconciliation.

## Submit a Job

Every job uses exactly one command form: `argv`, `command`, or `script`.

```python
job = local.submit(argv=["echo", "hello from molq"])

print(job.job_id)
print(job.status())  # cached state, no scheduler I/O
```

## Wait for Completion

```python
record = job.wait()

print(record.state.value)
print(record.exit_code)
```

`JobHandle.wait()` blocks until the job reaches a terminal state and returns an immutable `JobRecord`.

## Add Typed Resources

```python
from molq import Duration, JobResources, Memory, Submitor

local = Submitor("devbox", "local")

job = local.submit(
    argv=["python", "experiment.py"],
    resources=JobResources(
        cpu_count=4,
        memory=Memory.gb(8),
        time_limit=Duration.hours(2),
    ),
)
```

Resource values are typed objects, not raw scheduler strings:

```python
Memory.parse("32G")
Duration.parse("04:00:00")
Duration.parse("2h30m")
```

## Use a Script

Inline scripts are useful for multi-step workflows:

```python
from molq import Script

job = local.submit(
    script=Script.inline("""
cd /workspace
python preprocess.py
python train.py
""")
)
```

You can also submit an existing file:

```python
job = local.submit(script=Script.path("./run_experiment.sh"))
```

## CLI Basics

```bash
molq submit local echo "hello"
molq list local
molq status <job-id> local
molq watch <job-id> local
molq logs <job-id> local --stream stdout
```

The `monitor` command opens a full-screen dashboard across all clusters:

```bash
molq monitor
```

## Next Steps

- [Schedulers](schedulers.md) for backend-specific details
- [Monitoring](monitoring.md) for lifecycle and dashboard behavior
- [API Reference](api.md) for the exported surface
- [CLI Reference](cli.md) for command syntax
