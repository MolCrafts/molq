# API Reference

## Core API

### `Submitor`

```python
Submitor(
    cluster_name: str,
    scheduler: str = "local",
    *,
    defaults: SubmitorDefaults | None = None,
    scheduler_options: SchedulerOptions | None = None,
    store: JobStore | None = None,
    jobs_dir: str | Path | None = None,
)
```

Primary entry point for job submission and cluster-scoped job management.

#### Methods

- `submit(...) -> JobHandle` — submit a job using exactly one of `argv`, `command`, or `script`
- `get(job_id) -> JobRecord` — load a persisted record by id
- `list(include_terminal=False) -> list[JobRecord]` — list jobs for the current cluster
- `watch(job_ids=None, timeout=None) -> list[JobRecord]` — block until jobs reach terminal states
- `cancel(job_id) -> None` — cancel a running job
- `refresh() -> None` — reconcile active jobs against the scheduler
- `close() -> None` — close the underlying store connection

### `JobHandle`

Lightweight handle returned by `Submitor.submit()`.

- `job_id`
- `cluster_name`
- `scheduler`
- `scheduler_job_id`
- `status() -> JobState`
- `refresh() -> JobHandle`
- `wait(timeout=None) -> JobRecord`
- `cancel() -> None`

### `JobRecord`

Immutable snapshot of persisted job state.

Key fields:

- `job_id`
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

### `SubmitorDefaults`

```python
SubmitorDefaults(
    resources: JobResources | None = None,
    scheduling: JobScheduling | None = None,
    execution: JobExecution | None = None,
)
```

Reusable defaults merged into every submission for a given `Submitor`.

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
    dependency: str | None = None,
    node_count: int | None = None,
    exclusive_node: bool = False,
    array_spec: str | None = None,
    email: str | None = None,
    qos: str | None = None,
    reservation: str | None = None,
)
```

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

The lower-level monitoring engine also includes:

- `JobMonitor`
- `JobReconciler`
- `FixedStrategy`
- `ExponentialBackoffStrategy`
- `AdaptiveStrategy`

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
