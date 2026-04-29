# Schedulers

`molq` exposes a single public submission API while delegating
scheduler-specific behavior to scheduler implementations behind the
`Scheduler` protocol. The scheduler is selected on the `Cluster`, not the
`Submitor`.

## Supported schedulers

| Scheduler  | Use case                                                    | Option type             |
|----------|-------------------------------------------------------------|-------------------------|
| `local`  | In-process subprocess (legacy local executor)               | `LocalSchedulerOptions` |
| `shell`  | Shell-based local **or** remote execution (Transport-aware) | —                       |
| `slurm`  | SLURM clusters                                              | `SlurmSchedulerOptions` |
| `pbs`    | PBS or Torque clusters                                      | `PBSSchedulerOptions`   |
| `lsf`    | IBM Spectrum LSF clusters                                   | `LSFSchedulerOptions`   |

> **Scheduler × Transport.** Each scheduler works with any `Transport`. To run
> SLURM on a remote cluster, pass `host=` (or a custom `transport=`) —
> molq's SLURM code shells `sbatch` / `squeue` / `scancel` / `sacct`
> through the Transport. Same for PBS and LSF. See [Concepts](concepts.md#transport--physical-where-commands-run).

## Constructing a Cluster

```python
import molq as mq

local  = mq.Cluster("dev", "local")
hpc    = mq.Cluster("hpc",  "slurm", host="user@hpc.example.com")
torque = mq.Cluster("ops",  "pbs",   host="user@ops.example.com")
```

The cluster name is a namespace, not a transport handle. Multiple
`Cluster` instances against the same scheduler are fine — they get their own
scheduler implementation object but share the underlying `JobStore` (via
their `Submitor`s) by default.

## Scheduler option classes

Pass a scheduler-specific options object through `scheduler_options=`.

### Local

```python
mq.Cluster(
    "dev",
    "local",
    scheduler_options=mq.LocalSchedulerOptions(),
)
```

### SLURM

```python
mq.Cluster(
    "hpc",
    "slurm",
    host="user@hpc.example.com",
    scheduler_options=mq.SlurmSchedulerOptions(
        sbatch_path="sbatch",
        squeue_path="squeue",
        scancel_path="scancel",
        sacct_path="sacct",
        extra_sbatch_flags=("--clusters=gpu",),
    ),
)
```

### PBS

```python
mq.Cluster(
    "pbs-cluster",
    "pbs",
    host="user@pbs.example.com",
    scheduler_options=mq.PBSSchedulerOptions(
        qsub_path="qsub",
        qstat_path="qstat",
        qdel_path="qdel",
        tracejob_path="tracejob",
    ),
)
```

### LSF

```python
mq.Cluster(
    "lsf-cluster",
    "lsf",
    host="user@lsf.example.com",
    scheduler_options=mq.LSFSchedulerOptions(
        bsub_path="bsub",
        bjobs_path="bjobs",
        bkill_path="bkill",
        bhist_path="bhist",
    ),
)
```

## Transport options (orthogonal to scheduler)

When `host="user@host"` isn't enough — for ProxyJump, custom identity
files, or non-default rsync flags — construct an `SshTransport` directly:

```python
from molq.options import SshTransportOptions
from molq.transport import SshTransport

ssh = SshTransport(SshTransportOptions(
    host="user@bastion",
    port=2222,
    identity_file="~/.ssh/hpc_key",
    ssh_opts=("-o", "ProxyJump=jump.example.com"),
    rsync_opts=("-a", "--partial", "--inplace", "--bwlimit=20m"),
))

cluster = mq.Cluster("hpc", "slurm", transport=ssh)
```

`host=` and `transport=` are **mutually exclusive**. molq shells out to
the system `ssh` / `rsync` clients — no Python SSH library is required —
and inherits your `~/.ssh/config`.

## Submission model

All schedulers accept the same typed inputs:

- `JobResources` — CPU, memory, GPU, and time requirements
- `JobScheduling` — scheduler-level controls (partition, account, dependency, node count, …)
- `JobExecution` — cwd, job name, environment, output paths

```python
resources = mq.JobResources(
    cpu_count=8,
    memory=mq.Memory.gb(32),
    gpu_count=1,
    time_limit=mq.Duration.hours(4),
)

scheduling = mq.JobScheduling(partition="gpu", account="project123")
execution  = mq.JobExecution(cwd="/scratch/work", job_name="train-v1")
```

> **`partition`, not `queue`.** The `JobScheduling.queue` field was renamed
> to `partition` to match SLURM/PBS/LSF terminology. Profiles and persisted
> records that still spell it `queue` continue to load for one release; the
> CLI accepts `--partition` (canonical) and `--queue` (deprecated alias).

## Command forms

Exactly one command form must be provided per submission:

| Parameter | Meaning                                              |
|-----------|------------------------------------------------------|
| `argv`    | Structured arguments, never shell interpreted        |
| `command` | A single-line shell command                          |
| `script`  | Inline script content or a path to an existing file  |

`argv` is the safest default. Use `script` when you need multi-line shell
logic.

## Capabilities and validation

Each scheduler advertises a `SchedulerCapabilities` matrix
(`supports_partition`, `supports_gpu_count`, `supports_dependency`, …).
Submission fails fast with a `ConfigError` if you set a field the scheduler
doesn't support — for example, passing `JobScheduling(partition="gpu")`
to the local scheduler.

## Defaults

`SubmitorDefaults` lets you set reusable defaults for a Submitor (per
team / per profile). Per-submission values override defaults at the field
level.

```python
defaults = mq.SubmitorDefaults(
    resources=mq.JobResources(
        cpu_count=4,
        memory=mq.Memory.gb(16),
        time_limit=mq.Duration.hours(2),
    ),
    scheduling=mq.JobScheduling(partition="compute"),
)

submitor = mq.Submitor(target=cluster, defaults=defaults)
```

## Live queue snapshot

`cluster.get_queue()` returns a parsed snapshot of the scheduler's current
queue:

| Scheduler  | Underlying command     | Notes                              |
|----------|------------------------|------------------------------------|
| `local`  | —                      | Returns `[]` (no shared queue)     |
| `shell`  | —                      | Returns `[]` (no shared queue)     |
| `slurm`  | `squeue --me ...`      | Override user with `user=...`      |
| `pbs`    | `qstat -u $USER`       |                                    |
| `lsf`    | `bjobs -u $USER ...`   |                                    |

```python
for entry in cluster.get_queue():
    print(entry.scheduler_job_id, entry.state, entry.partition)
```

`QueueEntry` is the scheduler client's view (jobs by other users included
when relevant) — distinct from molq's persisted `JobRecord`. See the [API
reference](api.md#queueentry).
