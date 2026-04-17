# API Reference

## Core API

### `Submitor`

```python
Submitor(
    cluster_name: str | None = None,
    scheduler: str = "local",
    *,
    defaults: SubmitorDefaults | None = None,
    scheduler_options: SchedulerOptions | None = None,
    store: JobStore | None = None,
    jobs_dir: str | Path | None = None,
    default_retry_policy: RetryPolicy | None = None,
    retention_policy: RetentionPolicy | None = None,
    profile_name: str | None = None,
    event_bus: EventBus | None = None,
)
```

Primary entry point for job submission and cluster-scoped job management.

Global state such as `jobs.db` still lives under `~/.molq` by default. When
`jobs_dir` is omitted, per-job artifacts are written under the submission
working directory at `.molq/jobs/<job-id>/`.

Alternative constructor:

- `Submitor.from_profile(profile_name, config_path=None, ...) -> Submitor`

#### Main methods

- `submit(...) -> JobHandle` — submit a job using exactly one of `argv`, `command`, or `script`
- `get(job_id) -> JobRecord` — load a persisted record by id
- `list(include_terminal=False) -> list[JobRecord]` — list jobs for the current cluster
- `get_transitions(job_id) -> list[StatusTransition]` — load lifecycle transitions
- `get_retry_family(job_id) -> list[JobRecord]` — load all attempts for the same root job
- `get_dependencies(job_id) -> list[JobDependency]` — load persisted Molq dependency edges
- `watch(job_ids=None, timeout=None) -> list[JobRecord]` — block until jobs reach terminal states
- `cancel(job_id) -> None` — cancel the latest active attempt for the job family
- `refresh() -> None` — reconcile active jobs against the scheduler
- `cleanup(dry_run=False, retention_policy=None) -> dict[str, list[str]]` — prune old artifacts and records
- `daemon(once=False, interval=5.0, run_cleanup=True) -> None` — run the lightweight reconciliation loop
- `on(event, handler) -> None` — subscribe to lifecycle events
- `off(event, handler) -> None` — unsubscribe a lifecycle handler
- `close() -> None` — close the underlying store connection

#### `submit(...)` signature

```python
submitor.submit(
    *,
    argv: list[str] | None = None,
    command: str | None = None,
    script: Script | None = None,
    resources: JobResources | None = None,
    scheduling: JobScheduling | None = None,
    execution: JobExecution | None = None,
    metadata: dict[str, str] | None = None,
    retry: RetryPolicy | None = None,
    # logical dependency shortcuts (SLURM, PBS, LSF only)
    after_started: list[str] | None = None,
    after: list[str] | None = None,
    after_failure: list[str] | None = None,
    after_success: list[str] | None = None,
    job_dir_name: str | None = None,
) -> JobHandle
```

Exactly one of `argv`, `command`, or `script` must be provided.

Behavior:

- retries create a new attempt row with a fresh `job_id`
- `JobHandle.job_id` remains the root Molq job id for the family
- `watch()` and `JobHandle.wait()` follow the latest attempt when retries are enabled
- default artifacts are created under the resolved submission `cwd` unless `jobs_dir` is set

### `JobHandle`

Lightweight handle returned by `Submitor.submit()`.

Fields:

- `job_id`
- `cluster_name`
- `scheduler`
- `scheduler_job_id`

Methods:

- `status() -> JobState`
- `refresh() -> JobHandle`
- `wait(timeout=None) -> JobRecord`
- `cancel() -> None`

### `JobRecord`

Immutable snapshot of persisted job state.

Key fields:

- `job_id`
- `root_job_id`
- `attempt`
- `previous_attempt_job_id`
- `retry_group_id`
- `profile_name`
- `cluster_name`
- `scheduler`
- `state`
- `scheduler_job_id`
- `submitted_at`
- `started_at`
- `finished_at`
- `exit_code`
- `failure_reason`
- `cwd`
- `command_type`
- `command_display`
- `metadata`
- `cleaned_at`

### `JobDependency`

Persisted dependency edge between Molq jobs.

Fields:

- `job_id` — the job that has the dependency
- `dependency_job_id` — the upstream job being depended on
- `dependency_type` — one of the `DependencyCondition` values
- `scheduler_dependency` — the compiled scheduler-native string (e.g. `afterok:12345`)

### `StatusTransition`

Immutable persisted lifecycle transition.

Fields:

- `job_id`
- `old_state`
- `new_state`
- `timestamp`
- `reason`

### `SubmitorDefaults`

```python
SubmitorDefaults(
    resources: JobResources | None = None,
    scheduling: JobScheduling | None = None,
    execution: JobExecution | None = None,
)
```

Reusable defaults merged into every submission for a given `Submitor`.

## Retry, Retention, and Events

### `RetryBackoff`

```python
RetryBackoff(
    mode: Literal["fixed", "exponential"] = "exponential",
    initial_seconds: float = 5.0,
    maximum_seconds: float = 300.0,
    factor: float = 2.0,
)
```

### `RetryPolicy`

```python
RetryPolicy(
    max_attempts: int = 1,
    retry_on_states: tuple[JobState, ...] = (JobState.FAILED, JobState.TIMED_OUT),
    retry_on_exit_codes: tuple[int, ...] | None = None,
    backoff: RetryBackoff = RetryBackoff(),
)
```

### `RetentionPolicy`

```python
RetentionPolicy(
    keep_job_dirs_for_days: int = 30,
    keep_terminal_records_for_days: int = 90,
    keep_failed_job_dirs: bool = True,
)
```

### `EventType`

Lifecycle events emitted through `EventBus`:

- `STATUS_CHANGE`
- `JOB_STARTED`
- `JOB_COMPLETED`
- `JOB_FAILED`
- `JOB_CANCELLED`
- `JOB_TIMEOUT`
- `JOB_TIMED_OUT`
- `JOB_LOST`
- `ALL_COMPLETED`

### `EventPayload`

Fields:

- `event`
- `job_id`
- `transition`
- `record`
- `data`

### `EventBus`

Methods:

- `on(event, handler) -> None`
- `off(event, handler) -> None`
- `emit(event, data=None) -> None`

## Config and Profiles

### `MolqProfile`

Named profile loaded from `~/.molq/config.toml`.

Key fields:

- `name`
- `scheduler`
- `cluster_name`
- `defaults`
- `scheduler_options`
- `retry`
- `retention`
- `jobs_dir` — optional override for where per-job artifacts are written

### `MolqConfig`

Holds `profiles: dict[str, MolqProfile]`.

### Config helpers

- `load_config(path=None) -> MolqConfig`
- `load_profile(name, path=None) -> MolqProfile`

## Submission Types

### `Memory`

Immutable memory quantity stored as bytes.

- `Memory.kb(n)`
- `Memory.mb(n)`
- `Memory.gb(n)`
- `Memory.tb(n)`
- `Memory.parse("32G")`
- `to_slurm()`
- `to_pbs()`
- `to_lsf_kb()`

### `Duration`

Immutable time quantity stored as seconds.

- `Duration.minutes(n)`
- `Duration.hours(n)`
- `Duration.parse("2h30m")`
- `Duration.parse("04:00:00")`
- `to_slurm()`
- `to_pbs()`
- `to_lsf_minutes()`

### `Script`

Immutable script reference.

- `Script.inline(text)`
- `Script.path(path)`
- `variant`
- `text`
- `file_path`

### `JobResources`

```python
JobResources(
    cpu_count: int | None = None,
    memory: Memory | None = None,
    gpu_count: int | None = None,
    gpu_type: str | None = None,
    time_limit: Duration | None = None,
)
```

### `JobScheduling`

```python
JobScheduling(
    queue: str | None = None,
    account: str | None = None,
    priority: str | None = None,
    dependency: str | None = None,          # raw scheduler string — mutually exclusive with dependencies
    dependencies: tuple[DependencyRef, ...] = (),  # logical refs — mutually exclusive with dependency
    node_count: int | None = None,
    exclusive_node: bool = False,
    array_spec: str | None = None,
    email: str | None = None,
    qos: str | None = None,
    reservation: str | None = None,
)
```

`dependency` and `dependencies` are mutually exclusive. Constructing a `JobScheduling` with both set raises `ValueError` immediately.

### `JobExecution`

```python
JobExecution(
    env: dict[str, str] | None = None,
    cwd: str | Path | None = None,
    job_name: str | None = None,
    output_file: str | None = None,
    error_file: str | None = None,
)
```

## Job Dependencies

Molq supports logical job dependencies for SLURM, PBS, and LSF schedulers.  
Dependencies are **not supported** on the local scheduler.

### Conditions

`DependencyCondition` is a type alias for the four valid condition strings:

| Condition | Meaning | SLURM | PBS | LSF |
|---|---|---|---|---|
| `"after_success"` | Run after upstream **succeeded** | `afterok` | `afterok` | `done()` |
| `"after_failure"` | Run after upstream **failed / cancelled / timed out / lost** | `afternotok` | `afternotok` | `exit()` |
| `"after_started"` | Run after upstream **began executing** | `after` | `after` | `started()` |
| `"after"` | Run after upstream **finished (any result)** | `afterany` | `afterany` | `ended()` |

### `DependencyRef`

```python
DependencyRef(
    job_id: str,
    condition: DependencyCondition = "after_success",
)
```

Describes one upstream dependency using a Molq job ID and a condition.

### Defining dependencies — two equivalent approaches

**Approach A: `submit()` keyword arguments** (recommended for simple cases)

```python
parent = s.submit(argv=["python", "preprocess.py"])

child = s.submit(
    argv=["python", "train.py"],
    after_success=[parent.job_id],
)

# Multiple upstreams and mixed conditions in one call:
s.submit(
    argv=["python", "cleanup.py"],
    after_success=[job_a.job_id, job_b.job_id],
    after_failure=[job_c.job_id],
)
```

**Approach B: `DependencyRef` inside `JobScheduling`** (useful when building scheduling objects separately)

```python
from molq import DependencyRef, JobScheduling

s.submit(
    argv=["python", "eval.py"],
    scheduling=JobScheduling(
        queue="gpu",
        dependencies=(
            DependencyRef(parent.job_id, "after_success"),
            DependencyRef(monitor.job_id, "after_started"),
        ),
    ),
)
```

> The two approaches are merged before submission. You cannot mix `dependency` (raw string) with `dependencies` (logical refs) in a single `JobScheduling`.

### Querying dependencies

```python
# Edges where job is the dependent (upstream jobs this job waits on)
deps = s.get_dependencies(job_id)          # list[JobDependency]

# Edges where job is the upstream (downstream jobs waiting on this job)
dependents = s.get_dependents(job_id)      # list[JobDependency]

# Depth-1 preview with satisfaction state
preview = s.get_dependency_preview(job_id) # DependencyPreview
```

### `DependencyPreview`

```python
DependencyPreview(
    job_id: str,
    upstream_total: int,
    upstream_satisfied: int,
    upstream: tuple[DependencyPreviewItem, ...],
    downstream_total: int,
    downstream: tuple[DependencyPreviewItem, ...],
)
```

### `DependencyPreviewItem`

```python
DependencyPreviewItem(
    job_id: str,
    dependency_type: DependencyCondition,
    relation_state: str,   # "satisfied" | "pending" | "impossible"
    job_state: JobState,
    command_display: str,
    scheduler_dependency: str | None,
)
```

### `dependency_relation_state()`

```python
from molq import dependency_relation_state

relation = dependency_relation_state(
    dependency_type="after_success",
    related_state=JobState.SUCCEEDED,
    related_started_at=1234567890.0,
)
# → "satisfied"
```

Evaluates a single dependency edge to `"satisfied"`, `"pending"`, or `"impossible"`.  
Raises `ValueError` for unrecognised condition names.

## Scheduler Options

`molq` exports one scheduler options dataclass per backend:

- `LocalSchedulerOptions`
- `SlurmSchedulerOptions`
- `PBSSchedulerOptions`
- `LSFSchedulerOptions`

These objects are validated against the selected `scheduler=` value in `Submitor`.

## Monitoring and Dashboard

Public monitoring helpers exported at package level:

- `MolqMonitor`
- `RunDashboard`
- `DashboardState`
- `JobRow`

## Job State

`JobState` is a string enum with terminal awareness:

- `CREATED`
- `SUBMITTED`
- `QUEUED`
- `RUNNING`
- `SUCCEEDED`
- `FAILED`
- `CANCELLED`
- `TIMED_OUT`
- `LOST`

Use `state.is_terminal` to distinguish active from terminal states.

## Errors

All public errors inherit from `MolqError`.

| Exception | Raised when |
|-----------|-------------|
| `ConfigError` | Invalid `Submitor` configuration |
| `SubmitError` | Submission fails |
| `CommandError` | Command specification is invalid |
| `ScriptError` | Script path is invalid or cannot be prepared |
| `SchedulerError` | Scheduler interaction fails |
| `JobNotFoundError` | The requested job id does not exist |
| `MolqTimeoutError` | A wait operation exceeds the timeout |
| `StoreError` | Persistence or schema operations fail |
