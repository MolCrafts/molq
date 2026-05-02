# API Reference

This page is the exported surface. For an overview of how the pieces fit
together, see [Concepts](concepts.md).

## Two-axis core

### `Cluster`

```python
Cluster(
    name: str,
    scheduler: str = "local",
    *,
    host: str | None = None,
    transport: Transport | None = None,
    scheduler_options: SchedulerOptions | None = None,
)
```

A submission destination — scheduler kind + transport bound together. No
lifecycle state; cheap to construct.

- `name` — namespace used to scope persisted records and CLI listings
- `scheduler` — one of `"local"`, `"slurm"`, `"pbs"`, `"lsf"`. `"local"`
  is the no-batch-system backend; the *transport* decides where it runs.
- `host` — SSH shortcut. Builds `SshTransport(SshTransportOptions(host=host))`. **Mutually exclusive** with `transport`.
- `transport` — explicit Transport (use when you need custom SSH options). **Mutually exclusive** with `host`.
- `scheduler_options` — scheduler-specific options dataclass (see [Schedulers](schedulers.md))

#### Properties

- `name: str`
- `scheduler: str` — kind
- `transport: Transport`
- `scheduler_impl: Scheduler`
- `scheduler_options: SchedulerOptions | None`

#### Methods

- `get_queue(*, user=None) -> list[QueueEntry]` — parsed `squeue --me` /
  `qstat -u $USER` / `bjobs` snapshot. Local schedulers return `[]`.
- `get_workspace(name, *, path) -> Workspace` — handle to a remote
  directory.
- `get_project(name, *, workspace) -> Project` — sub-namespace under a
  workspace.

#### Class methods

- `Cluster.from_profile(profile_name, *, config_path=None) -> Cluster` —
  load destination half of a TOML profile (scheduler, scheduler_options).

### `Submitor`

```python
Submitor(
    target: Cluster,
    *,
    defaults: SubmitorDefaults | None = None,
    store: JobStore | None = None,
    jobs_dir: str | Path | None = None,
    default_retry_policy: RetryPolicy | None = None,
    retention_policy: RetentionPolicy | None = None,
    profile_name: str | None = None,
    event_bus: EventBus | None = None,
)
```

Lifecycle engine. Bound to one `Cluster` as `target`. All lifecycle calls
are scoped implicitly to `target.name`, so multiple Submitors can share a
JobStore without seeing each other's records.

- `target` — the destination Cluster (required)
- `store` — defaults to `JobStore()` (`~/.molq/jobs.db`)
- `jobs_dir` — when omitted, per-job artifacts are written under the
  submission working directory at `.molq/jobs/<job-id>/`

#### Public properties

- `target: Cluster`
- `cluster_name: str` — same as `target.name`

#### Public methods (verb_noun)

| Method | Description |
|---|---|
| `submit_job(...) -> JobHandle` | Submit a job; exactly one of `argv` / `command` / `script` |
| `get_job(job_id) -> JobRecord` | Load a persisted record by id |
| `list_jobs(include_terminal=False) -> list[JobRecord]` | List jobs for the current cluster |
| `get_transitions(job_id) -> list[StatusTransition]` | Lifecycle transitions |
| `get_retry_family(job_id) -> list[JobRecord]` | All attempts for the same root job |
| `get_dependencies(job_id) -> list[JobDependency]` | Persisted dependency edges |
| `get_dependents(job_id) -> list[JobDependency]` | Inverse — jobs depending on this one |
| `watch_jobs(job_ids=None, timeout=None) -> list[JobRecord]` | Block until terminal |
| `cancel_job(job_id) -> None` | Cancel the latest active attempt |
| `refresh_jobs() -> None` | Reconcile active jobs against the scheduler |
| `cleanup_jobs(dry_run=False, retention_policy=None) -> dict[str, list[str]]` | Prune old artifacts and records |
| `run_daemon(once=False, interval=5.0, run_cleanup=True) -> None` | Background reconciliation loop |
| `on_event(event, handler) -> None` | Subscribe to lifecycle events |
| `off_event(event, handler) -> None` | Unsubscribe |
| `close() -> None` | Close the underlying store connection |

#### Class methods

- `Submitor.from_profile(profile_name, *, target=None, config_path=None, store=None)` — load lifecycle parameters from a TOML profile. If `target` is omitted, builds one via `Cluster.from_profile`.

#### `submit_job(...)` signature

```python
submitor.submit_job(
    *,
    argv: list[str] | None = None,
    command: str | None = None,
    script: Script | None = None,
    resources: JobResources | None = None,
    scheduling: JobScheduling | None = None,
    execution: JobExecution | None = None,
    metadata: dict[str, str] | None = None,
    retry: RetryPolicy | None = None,
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
- `JobHandle.job_id` remains the root molq job id for the family
- `watch_jobs()` and `JobHandle.wait()` follow the latest attempt when
  retries are enabled
- default artifacts are created under the resolved submission `cwd`
  unless `jobs_dir` is set

### `JobHandle`

Lightweight handle returned by `Submitor.submit_job()`.

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

Key fields: `job_id`, `root_job_id`, `attempt`, `previous_attempt_job_id`,
`retry_group_id`, `profile_name`, `cluster_name`, `scheduler`, `state`,
`scheduler_job_id`, `submitted_at`, `started_at`, `finished_at`,
`exit_code`, `failure_reason`, `cwd`, `command_type`, `command_display`,
`metadata`, `cleaned_at`.

### `QueueEntry`

```python
QueueEntry(
    scheduler_job_id: str,
    name: str | None = None,
    user: str | None = None,
    state: JobState = JobState.QUEUED,
    raw_state: str = "",
    partition: str | None = None,
    submit_time: float | None = None,
    start_time: float | None = None,
)
```

One row from `cluster.get_queue()`. The scheduler client's view of a job
— **distinct from `JobRecord`** (molq's own persisted view). May include
jobs submitted outside molq.

## Remote directories

### `Workspace`

```python
Workspace(cluster: Cluster, name: str, path: str)
```

A base directory on the cluster's filesystem. `path` is absolute and
interpreted on the cluster's Transport (local FS for `LocalTransport`,
remote FS for `SshTransport`).

Methods:

- `get_project(name) -> Project`
- `upload(local, *, recursive=False) -> None`
- `download(remote_rel, local, *, recursive=False) -> None`
- `exists() -> bool`
- `ensure() -> None` — `mkdir -p`

### `Project`

```python
Project(workspace: Workspace, name: str)
```

A sub-namespace under a `Workspace`. `path` is computed as
`workspace.path / name`.

Methods:

- `path -> str` — computed
- `cluster -> Cluster` — pass-through to `workspace.cluster`
- `upload`, `download`, `exists`, `ensure` — same surface as `Workspace`
- `submit_job(submitor, **kwargs) -> JobHandle` — sugar that overrides
  `JobExecution.cwd` to `self.path` before forwarding to
  `submitor.submit_job(...)`. Validates that
  `submitor.target is self.cluster`.

`Project` and `Workspace` do **not** auto-stage local files referenced in
argv — call `proj.upload(...)` explicitly.

## Transport

### `Transport` (Protocol)

The internal protocol every Transport implements. Methods used by
schedulers and Workspace/Project: `run`, `read_text`, `read_bytes`,
`write_text`, `write_bytes`, `exists`, `mkdir`, `chmod`, `remove`,
`upload`, `download`.

### `LocalTransport`

Runs commands via `subprocess`, file ops via `pathlib`. Default for any
`Cluster(host=None, transport=None)`.

### `SshTransport`

```python
SshTransport(options: SshTransportOptions)
```

Shells out to OpenSSH `ssh` / `rsync` / `scp`. No Python SSH dependency.
Inherits `~/.ssh/config`, agents, ProxyJump, ControlMaster, Kerberos.

### `SshTransportOptions`

```python
SshTransportOptions(
    host: str,                                     # "user@host" or alias from ssh_config
    port: int | None = None,
    identity_file: str | None = None,
    ssh_opts: tuple[str, ...] = (),
    rsync_opts: tuple[str, ...] = ("-a", "--partial", "--inplace"),
)
```

## Scheduler protocol (internal)

> Users normally don't construct schedulers directly — `Cluster` does it
> for you. Documented here for completeness.

### `Scheduler` (Protocol)

```python
class Scheduler(Protocol):
    def capabilities(self) -> SchedulerCapabilities: ...
    def submit(self, spec: JobSpec, job_dir: Path) -> str: ...
    def poll_many(self, scheduler_job_ids: Sequence[str]) -> dict[str, JobState]: ...
    def cancel(self, scheduler_job_id: str) -> None: ...
    def resolve_terminal(self, scheduler_job_id: str) -> TerminalStatus | None: ...
    def list_queue(self, *, user: str | None = None) -> list[QueueEntry]: ...
```

Implementations: `ShellScheduler` (the backend for `scheduler="local"`),
`SlurmScheduler`, `PBSScheduler`, `LSFScheduler`. All four route every
shell call through `self._transport.run(...)`, so any combination of
scheduler kind and Transport works without scheduler-specific glue —
including remote SLURM over SSH or running plain shell jobs on a remote
workstation that has no batch system.

### `SchedulerCapabilities`

Frozen dataclass declaring which fields a scheduler supports. Used for
submit-time validation (`scheduling.partition`, `resources.gpu_count`,
etc.). The local scheduler supports far less than SLURM does — submission
fails fast on unsupported fields with a `ConfigError`.

## Defaults, retry, retention, events

### `SubmitorDefaults`

```python
SubmitorDefaults(
    resources: JobResources | None = None,
    scheduling: JobScheduling | None = None,
    execution: JobExecution | None = None,
)
```

Reusable defaults merged into every submission for a given `Submitor`.

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

`STATUS_CHANGE`, `JOB_STARTED`, `JOB_COMPLETED`, `JOB_FAILED`,
`JOB_CANCELLED`, `JOB_TIMEOUT`, `JOB_TIMED_OUT`, `JOB_LOST`,
`ALL_COMPLETED`.

### `EventPayload`

Fields: `event`, `job_id`, `transition`, `record`, `data`.

### `EventBus`

Methods: `on(event, handler)`, `off(event, handler)`, `emit(event, data=None)`.

## Config and profiles

### `MolqProfile`

Named profile loaded from `~/.molq/config.toml`.

Key fields: `name`, `scheduler`, `cluster_name`, `defaults`,
`scheduler_options`, `retry`, `retention`, `jobs_dir`.

### `MolqConfig`

Holds `profiles: dict[str, MolqProfile]`.

### Helpers

- `load_config(path=None) -> MolqConfig`
- `load_profile(name, path=None) -> MolqProfile`

## Submission types

### `Memory`

Immutable memory quantity stored as bytes.

- `Memory.kb(n)`, `Memory.mb(n)`, `Memory.gb(n)`, `Memory.tb(n)`
- `Memory.parse("32G")`
- `to_slurm()`, `to_pbs()`, `to_lsf_kb()`

### `Duration`

Immutable time quantity stored as seconds.

- `Duration.minutes(n)`, `Duration.hours(n)`
- `Duration.parse("2h30m")`, `Duration.parse("04:00:00")`
- `to_slurm()`, `to_pbs()`, `to_lsf_minutes()`

### `Script`

Immutable script reference.

- `Script.inline(text)`, `Script.path(path)`
- `variant`, `text`, `file_path`

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
    partition: str | None = None,                  # was 'queue' — renamed
    account: str | None = None,
    priority: str | None = None,
    dependency: str | None = None,                  # raw scheduler string — mutually exclusive with dependencies
    dependencies: tuple[DependencyRef, ...] = (),   # logical refs — mutually exclusive with dependency
    node_count: int | None = None,
    exclusive_node: bool = False,
    array_spec: str | None = None,
    email: str | None = None,
    qos: str | None = None,
    reservation: str | None = None,
)
```

`dependency` and `dependencies` are mutually exclusive. Constructing a
`JobScheduling` with both set raises `ValueError` immediately.

> `partition` was previously called `queue`. Profiles and SQLite rows
> using the legacy `queue` key still load (one-release deprecation).

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

## Job dependencies

molq supports logical job dependencies for SLURM, PBS, and LSF schedulers.
Dependencies are **not supported** on the local scheduler.

### Conditions

`DependencyCondition` is a type alias for the four valid condition
strings:

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

Describes one upstream dependency using a molq job ID and a condition.

### Defining dependencies — two equivalent approaches

**Approach A: `submit_job()` keyword arguments** (recommended for simple cases)

```python
parent = submitor.submit_job(argv=["python", "preprocess.py"])

child = submitor.submit_job(
    argv=["python", "train.py"],
    after_success=[parent.job_id],
)

# Multiple upstreams and mixed conditions in one call:
submitor.submit_job(
    argv=["python", "cleanup.py"],
    after_success=[job_a.job_id, job_b.job_id],
    after_failure=[job_c.job_id],
)
```

**Approach B: `DependencyRef` inside `JobScheduling`** (useful when building scheduling objects separately)

```python
from molq import DependencyRef, JobScheduling

submitor.submit_job(
    argv=["python", "eval.py"],
    scheduling=JobScheduling(
        partition="gpu",
        dependencies=(
            DependencyRef(parent.job_id, "after_success"),
            DependencyRef(monitor.job_id, "after_started"),
        ),
    ),
)
```

> The two approaches are merged before submission. You cannot mix
> `dependency` (raw string) with `dependencies` (logical refs) in a
> single `JobScheduling`.

### `JobDependency`

Persisted dependency edge between molq jobs.

Fields:

- `job_id` — the job that has the dependency
- `dependency_job_id` — the upstream job being depended on
- `dependency_type` — one of the `DependencyCondition` values
- `scheduler_dependency` — the compiled scheduler-native string (e.g. `afterok:12345`)

### `StatusTransition`

Immutable persisted lifecycle transition.

Fields: `job_id`, `old_state`, `new_state`, `timestamp`, `reason`.
