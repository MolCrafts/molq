# Core Concepts

Molq is built on a three-layer architecture: **Submitor** (public API), **Scheduler** (backend protocol), and **JobStore** (persistence).

## 1. Submitor

The `Submitor` is the single entry point. Each instance is bound to a cluster name and scheduler backend.

```python
from molq import Submitor

local = Submitor("devbox", "local")
cluster = Submitor("hpc", "slurm")
```

The cluster name is a namespace -- it scopes job listings and reconciliation. You can have multiple `Submitor` instances with different names pointing at the same backend.

### Defaults

A `Submitor` can carry defaults applied to every submission:

```python
from molq import Submitor, SubmitorDefaults, JobResources, JobScheduling, Memory

defaults = SubmitorDefaults(
    resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
    scheduling=JobScheduling(queue="compute"),
)

cluster = Submitor("hpc", "slurm", defaults=defaults)

# cpu_count=4 and memory=16GB applied unless overridden
job = cluster.submit(argv=["python", "script.py"])
```

Per-submit parameters override defaults at the field level.

## 2. Command Model

Every job needs exactly one command specification:

| Form | Use case |
|------|----------|
| `argv=["python", "train.py"]` | Structured args, no shell interpretation |
| `command="python train.py && python eval.py"` | Single-line shell command |
| `script=Script.inline("...")` | Multi-line script content |
| `script=Script.path("run.sh")` | Reference to a script file |

`argv` is the safest -- arguments are never shell-interpreted. `command` is a convenience for simple one-liners. `Script` handles multi-line logic or existing shell scripts.

## 3. Typed Resource Specs

Resources are specified with frozen dataclasses, not raw strings or dicts:

```python
from molq import JobResources, Memory, Duration

resources = JobResources(
    cpu_count=8,
    memory=Memory.gb(32),      # Not "32GB" -- typed value
    gpu_count=2,
    time_limit=Duration.hours(4),
)
```

This prevents misconfigured submissions (typos, unit errors) and enables scheduler-specific formatting -- `Memory.gb(32)` becomes `"32G"` for SLURM but `"32gb"` for PBS.

## 4. Scheduler Protocol

Under the hood, each backend implements the `Scheduler` protocol:

```python
class Scheduler(Protocol):
    def submit(self, spec: JobSpec, job_dir: Path) -> str: ...
    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]: ...
    def cancel(self, scheduler_job_id: str) -> None: ...
    def resolve_terminal(self, scheduler_job_id: str) -> JobState | None: ...
```

You never interact with schedulers directly -- `Submitor` handles everything. But this protocol makes adding new backends straightforward: implement four methods.

**Available backends:** `LocalScheduler`, `SlurmScheduler`, `PBSScheduler`, `LSFScheduler`.

## 5. Job Lifecycle

A job moves through these states:

```
CREATED ──> SUBMITTED ──> QUEUED ──> RUNNING ──> SUCCEEDED
                                          └──> FAILED
                                          └──> TIMED_OUT
                                          └──> CANCELLED
                                          └──> LOST
```

States are represented by `JobState`, a string enum with an `is_terminal` property:

```python
from molq import JobState

JobState.RUNNING.is_terminal    # False
JobState.SUCCEEDED.is_terminal  # True
```

## 6. Persistence

All job state is persisted in SQLite (WAL mode) at `~/.molq/jobs.db`. The `JobStore` tracks:

- Job records with UUID-based identity
- Status transitions with timestamps
- Scheduler-assigned IDs

This means jobs survive process restarts. You can query past jobs, inspect transition history, or resume monitoring after a disconnect.

## 7. Reconciliation

The `JobReconciler` syncs scheduler state with the database:

1. Load all active (non-terminal) jobs from the store
2. Batch-query the scheduler for their current states
3. Compute diffs between stored and actual states
4. Apply transitions to the store

This happens automatically during `wait()`, `refresh()`, and `watch()`.

## 8. Monitoring

The `JobMonitor` provides blocking waits with pluggable polling strategies:

- **ExponentialBackoffStrategy** (default): starts at 1s, grows to 60s max. Reduces a 24h job from ~43k polls to ~60.
- **FixedStrategy**: constant interval, simple but wasteful.
- **AdaptiveStrategy**: polls at ~1% of expected duration.

## Architecture Diagram

```
Submitor  ──>  Scheduler (Protocol)  ──>  Local | SLURM | PBS | LSF
    │                │
    │           submit / poll_many / cancel / resolve_terminal
    │
    ├── JobStore (SQLite + WAL)
    │       └── jobs, status_transitions, molq_meta
    │
    ├── JobReconciler
    │       └── batch poll ──> diff ──> apply transitions
    │
    └── JobMonitor
            └── PollingStrategy (exponential, fixed, adaptive)
```
