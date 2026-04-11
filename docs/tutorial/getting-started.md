# Getting Started

This guide walks you through installing Molq and running your first job.

## 1. Installation

```bash
pip install molcrafts-molq
```

## 2. Your First Submitor

A `Submitor` is the entry point for job submission. It connects to a specific scheduler backend. Start with a **local** submitor -- it runs jobs as subprocesses on your machine.

```python
from molq import Submitor

# "devbox" is a name for this cluster namespace
# "local" is the scheduler backend
local = Submitor("devbox", "local")
```

## 3. Submitting a Job

Call `submit()` with one of three command forms: `argv`, `command`, or `script`.

```python
# Submit a simple command
job = local.submit(argv=["echo", "Hello from Molq!"])

print(f"Job ID:  {job.job_id}")
print(f"Status:  {job.status()}")
```

`submit()` returns a `JobHandle` -- a lightweight object for tracking the submitted job.

## 4. Waiting for Completion

Call `wait()` on the handle to block until the job finishes:

```python
record = job.wait()

print(f"State:     {record.state}")       # JobState.SUCCEEDED
print(f"Exit code: {record.exit_code}")   # 0
```

The returned `JobRecord` is an immutable snapshot of the job's full lifecycle.

## 5. Adding Resources

For real workloads, specify resources with typed objects:

```python
from molq import Submitor, JobResources, Memory, Duration

local = Submitor("devbox", "local")

job = local.submit(
    argv=["python", "experiment.py"],
    resources=JobResources(
        cpu_count=4,
        memory=Memory.gb(8),
        time_limit=Duration.hours(2),
    ),
)

record = job.wait()
print(record.state)
```

Resource types like `Memory` and `Duration` are immutable and parse human-readable strings:

```python
Memory.parse("8GB")       # Memory(bytes=8589934592)
Duration.parse("2h30m")   # Duration(seconds=9000)
```

## 6. Using Scripts

For multi-line workflows, use `Script`:

```python
from molq import Script

job = local.submit(script=Script.inline("""
cd /workspace
python preprocess.py
python train.py --epochs 100
"""))

record = job.wait()
```

Or reference an existing script file:

```python
job = local.submit(script=Script.path("run_experiment.sh"))
```

## 7. Listing and Managing Jobs

```python
# List active jobs
for r in local.list():
    print(f"{r.job_id[:8]}  {r.state.value}")

# Get a specific job
record = local.get(job.job_id)

# Cancel a job
local.cancel(job.job_id)
```

## 8. CLI

Molq also provides a command-line interface:

```bash
# Submit
molq submit local echo "Hello World"

# List
molq list local

# Watch
molq watch <job-id> local

# Cancel
molq cancel <job-id> local
```

## Next Steps

- **[Core Concepts](core-concepts.md)** -- How the architecture works
- **[API Reference](../api/index.md)** -- Complete type and class documentation
- **[Recipes](../recipes/machine-learning.md)** -- Real-world examples
