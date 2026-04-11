# API Reference

Molq's public API is organized into layers:

| Module | Purpose |
|--------|---------|
| [`submitor`](submit.md) | `Submitor` (entry point), `JobHandle` (per-job operations) |
| [`types`](resources.md) | `Memory`, `Duration`, `Script`, `JobResources`, `JobScheduling`, `JobExecution` |
| [`models`](#models) | `JobRecord` (lifecycle snapshot), `SubmitorDefaults` |
| [`status`](#jobstate) | `JobState` enum with terminal detection |
| [`errors`](#errors) | Structured exception hierarchy |
| [CLI](cli.md) | Command-line interface |

## Quick Start

```python
from molq import Submitor, JobResources, Memory, Duration, JobScheduling

# Create a submitor for a SLURM cluster
cluster = Submitor("hpc", "slurm")

# Submit a job
job = cluster.submit(
    argv=["python", "train.py"],
    resources=JobResources(
        cpu_count=8,
        memory=Memory.gb(32),
        time_limit=Duration.hours(4),
    ),
    scheduling=JobScheduling(queue="gpu"),
)

# Query and wait
print(job.status())        # JobState.SUBMITTED (cached)
job.refresh()              # Reconcile with scheduler
record = job.wait()        # Block until terminal
print(record.state)        # JobState.SUCCEEDED
print(record.exit_code)    # 0
```

## Architecture

```
Submitor  ──>  Scheduler (Protocol)  ──>  Local | SLURM | PBS | LSF
    │                │
    │           submit / poll_many / cancel / resolve_terminal
    │
    ├── JobStore (SQLite + WAL)
    │       └── jobs, status_transitions, molq_meta tables
    │
    ├── JobReconciler (scheduler <-> DB sync)
    │       └── batch poll, diff, apply transitions
    │
    └── JobMonitor (blocking wait)
            └── pluggable PollingStrategy
```

## Submitor

The single entry point for job submission and management.

```python
from molq import Submitor, SubmitorDefaults, JobResources, Memory

# With defaults applied to all submissions
s = Submitor(
    "hpc", "slurm",
    defaults=SubmitorDefaults(
        resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
    ),
)

job = s.submit(argv=["python", "script.py"])  # Uses defaults
record = s.get(job.job_id)                     # Lookup by ID
records = s.list(include_terminal=True)        # All jobs
s.cancel(job.job_id)                           # Cancel
s.refresh()                                    # Reconcile all active
```

See [Submitor API](submit.md) for full details.

## Command Model

Three mutually exclusive ways to specify what to run:

| Parameter | Type | Description |
|-----------|------|-------------|
| `argv` | `list[str]` | Structured args, never shell-interpreted |
| `command` | `str` | Single-line shell command (no newlines) |
| `script` | `Script` | `Script.inline(text)` or `Script.path(path)` |

```python
# argv -- safe, no shell interpretation
job = s.submit(argv=["python", "train.py", "--lr", "0.001"])

# command -- single-line shell command
job = s.submit(command="python train.py && python eval.py")

# script -- multi-line or file-based
job = s.submit(script=Script.inline("""
cd /workspace
python train.py
python eval.py
"""))
```

## Models

### JobRecord

Immutable snapshot of a job's full lifecycle state. Returned by `Submitor.get()`, `JobHandle.wait()`, etc.

```python
record = submitor.get(job_id)

record.job_id            # str (UUID)
record.cluster_name      # str
record.scheduler         # str
record.state             # JobState
record.scheduler_job_id  # str | None
record.submitted_at      # float | None
record.started_at        # float | None
record.finished_at       # float | None
record.exit_code         # int | None
record.failure_reason    # str | None
record.cwd               # str
record.command_type      # str ("argv", "command", "script")
record.command_display   # str
record.metadata          # dict[str, str]
```

### SubmitorDefaults

Default parameters applied to every submission from a `Submitor` instance:

```python
from molq import SubmitorDefaults, JobResources, JobScheduling, Memory

defaults = SubmitorDefaults(
    resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
    scheduling=JobScheduling(queue="compute", account="myproject"),
)

s = Submitor("hpc", "slurm", defaults=defaults)
```

## JobState

Terminal-aware job state enum:

```python
from molq import JobState

# Non-terminal states
JobState.CREATED      # Initial state
JobState.SUBMITTED    # Sent to scheduler
JobState.QUEUED       # In scheduler queue
JobState.RUNNING      # Executing

# Terminal states
JobState.SUCCEEDED    # Completed successfully
JobState.FAILED       # Error or non-zero exit
JobState.CANCELLED    # User cancelled
JobState.TIMED_OUT    # Time limit exceeded
JobState.LOST         # Disappeared from scheduler

JobState.RUNNING.is_terminal   # False
JobState.SUCCEEDED.is_terminal # True
```

## Errors

All exceptions inherit from `MolqError` and carry typed context:

| Exception | When |
|-----------|------|
| `MolqError` | Base for all molq errors |
| `ConfigError` | Invalid Submitor parameters |
| `SubmitError` | Job submission failed |
| `CommandError` | Invalid command specification |
| `ScriptError` | Script file not found or copy failed |
| `SchedulerError` | Scheduler communication failed |
| `JobNotFoundError` | Job ID does not exist |
| `MolqTimeoutError` | Watch/wait timeout exceeded |
| `StoreError` | Database operation failed |

```python
from molq import MolqError, JobNotFoundError

try:
    record = submitor.get("nonexistent-id")
except JobNotFoundError as e:
    print(e.job_id)          # "nonexistent-id"
    print(e.cluster_name)    # "hpc"
```

## Scheduler Options

Each backend accepts a typed options dataclass:

```python
from molq import Submitor, SlurmSchedulerOptions

s = Submitor(
    "hpc", "slurm",
    scheduler_options=SlurmSchedulerOptions(
        sbatch_path="/opt/slurm/bin/sbatch",
        extra_sbatch_flags=("--clusters=gpu",),
    ),
)
```

| Options class | Backend |
|---------------|---------|
| `LocalSchedulerOptions` | `local` |
| `SlurmSchedulerOptions` | `slurm` |
| `PBSSchedulerOptions` | `pbs` |
| `LSFSchedulerOptions` | `lsf` |
