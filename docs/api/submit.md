# Submitor API

The `Submitor` class is the single entry point for job submission and management. Each instance represents a connection to a specific cluster.

## Submitor

::: molq.submitor.Submitor

### Constructor

```python
from molq import Submitor, SubmitorDefaults, SlurmSchedulerOptions

s = Submitor(
    cluster_name="hpc",
    scheduler="slurm",
    defaults=SubmitorDefaults(...),                    # Optional
    scheduler_options=SlurmSchedulerOptions(...),       # Optional
    store=JobStore(":memory:"),                         # Optional
)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `cluster_name` | `str` | Identifies this cluster namespace |
| `scheduler` | `str` | One of `"local"`, `"slurm"`, `"pbs"`, `"lsf"` |
| `defaults` | `SubmitorDefaults \| None` | Default resource/scheduling/execution params |
| `scheduler_options` | `SchedulerOptions \| None` | Scheduler-specific configuration |
| `store` | `JobStore \| None` | Custom store (default: `~/.molq/jobs.db`) |

### submit()

Submit a job. Exactly one of `argv`, `command`, or `script` must be provided.

```python
job = s.submit(
    argv=["python", "train.py"],
    resources=JobResources(cpu_count=8, memory=Memory.gb(32)),
    scheduling=JobScheduling(queue="gpu"),
    execution=JobExecution(job_name="training"),
    metadata={"experiment": "v1"},
)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `argv` | `list[str] \| None` | Structured args, never shell-interpreted |
| `command` | `str \| None` | Single-line shell command (no newlines) |
| `script` | `Script \| None` | `Script.inline(text)` or `Script.path(path)` |
| `resources` | `JobResources \| None` | Hardware requirements |
| `scheduling` | `JobScheduling \| None` | Scheduler-level parameters |
| `execution` | `JobExecution \| None` | Execution environment |
| `metadata` | `dict[str, str] \| None` | User-defined key-value metadata |

**Returns:** `JobHandle`

**Raises:** `CommandError` if command validation fails, `SchedulerError` on submission failure.

### get()

Get a job record by ID.

```python
record = s.get("some-uuid")
```

**Returns:** `JobRecord`

**Raises:** `JobNotFoundError` if job does not exist.

### list()

List jobs for this cluster.

```python
active = s.list()                          # Active only
all_jobs = s.list(include_terminal=True)   # Include finished
```

**Returns:** `list[JobRecord]`

### watch()

Block until specified jobs (or all active jobs) reach terminal state.

```python
records = s.watch(["id1", "id2"], timeout=3600)
records = s.watch()  # Wait for all active jobs
```

**Returns:** `list[JobRecord]`

**Raises:** `MolqTimeoutError` if timeout exceeded.

### cancel()

Cancel a job.

```python
s.cancel("some-uuid")
```

**Raises:** `JobNotFoundError` if job does not exist.

### refresh()

Reconcile all active jobs with the scheduler. Updates stored state based on scheduler queries.

```python
s.refresh()
```

---

## JobHandle

::: molq.submitor.JobHandle

Lightweight handle returned by `Submitor.submit()`. Provides single-job operations.

```python
job = s.submit(argv=["echo", "hello"])

job.job_id              # str (UUID)
job.cluster_name        # str
job.scheduler           # str
job.scheduler_job_id    # str | None (PID, SLURM job ID, etc.)
```

### status()

Return cached job state (no I/O).

```python
state = job.status()  # JobState.SUBMITTED
```

### refresh()

Reconcile with scheduler and return updated handle.

```python
job.refresh()
print(job.status())  # JobState.RUNNING
```

### wait()

Block until this job reaches a terminal state.

```python
record = job.wait(timeout=3600)
print(record.state)       # JobState.SUCCEEDED
print(record.exit_code)   # 0
```

**Returns:** `JobRecord`

**Raises:** `MolqTimeoutError` if timeout exceeded.

### cancel()

Cancel this job.

```python
job.cancel()
print(job.status())  # JobState.CANCELLED
```

---

## Examples

### Local Development

```python
from molq import Submitor

local = Submitor("devbox", "local")

job = local.submit(argv=["python", "experiment.py"])
record = job.wait()

if record.state.is_terminal:
    print(f"Finished: {record.state.value}")
```

### SLURM with Defaults

```python
from molq import Submitor, SubmitorDefaults, JobResources, JobScheduling, Memory

defaults = SubmitorDefaults(
    resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
    scheduling=JobScheduling(queue="compute", account="myproject"),
)

cluster = Submitor("hpc", "slurm", defaults=defaults)

# Defaults applied; per-submit overrides take precedence
job = cluster.submit(
    argv=["python", "train.py"],
    resources=JobResources(gpu_count=2),  # Adds GPUs, keeps default CPU/memory
)
```

### Batch Monitoring

```python
from molq import Submitor

s = Submitor("hpc", "slurm")

jobs = [
    s.submit(argv=["python", f"run_{i}.py"])
    for i in range(10)
]

# Wait for all
records = s.watch([j.job_id for j in jobs], timeout=7200)

for r in records:
    print(f"{r.job_id[:8]}  {r.state.value}  exit={r.exit_code}")
```

### PBS Cluster

```python
from molq import Submitor, PBSSchedulerOptions, JobResources, Memory, Duration

s = Submitor(
    "pbs_cluster", "pbs",
    scheduler_options=PBSSchedulerOptions(
        qsub_path="/opt/pbs/bin/qsub",
    ),
)

job = s.submit(
    argv=["python", "simulate.py"],
    resources=JobResources(
        cpu_count=16,
        memory=Memory.gb(64),
        time_limit=Duration.hours(12),
    ),
)
```
