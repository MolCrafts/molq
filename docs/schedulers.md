# Schedulers

`molq` exposes a single public submission API while delegating backend-specific behavior to scheduler implementations.

## Supported Backends

| Backend | Use case | Option type |
|---------|----------|-------------|
| `local` | Local subprocess execution for development and lightweight automation | `LocalSchedulerOptions` |
| `slurm` | SLURM clusters | `SlurmSchedulerOptions` |
| `pbs` | PBS or Torque clusters | `PBSSchedulerOptions` |
| `lsf` | IBM Spectrum LSF clusters | `LSFSchedulerOptions` |

## Constructing a Submitor

```python
from molq import Submitor

local = Submitor("devbox", "local")
slurm = Submitor("hpc", "slurm")
```

The cluster name is a namespace, not a transport handle. You can use multiple `Submitor` instances against the same backend if you want isolated job views.

## Scheduler Option Classes

Pass a scheduler-specific options object through `scheduler_options=`.

### Local

```python
from molq import LocalSchedulerOptions, Submitor

submitor = Submitor(
    "devbox",
    "local",
    scheduler_options=LocalSchedulerOptions(
        runner_shim="molq-runner",
        max_concurrent=4,
    ),
)
```

### SLURM

```python
from molq import SlurmSchedulerOptions, Submitor

submitor = Submitor(
    "hpc",
    "slurm",
    scheduler_options=SlurmSchedulerOptions(
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
from molq import PBSSchedulerOptions, Submitor

submitor = Submitor(
    "pbs-cluster",
    "pbs",
    scheduler_options=PBSSchedulerOptions(
        qsub_path="qsub",
        qstat_path="qstat",
        qdel_path="qdel",
        tracejob_path="tracejob",
    ),
)
```

### LSF

```python
from molq import LSFSchedulerOptions, Submitor

submitor = Submitor(
    "lsf-cluster",
    "lsf",
    scheduler_options=LSFSchedulerOptions(
        bsub_path="bsub",
        bjobs_path="bjobs",
        bkill_path="bkill",
        bhist_path="bhist",
    ),
)
```

## Submission Model

All backends accept the same typed inputs:

- `JobResources` for CPU, memory, GPU, and time requirements
- `JobScheduling` for queue-level controls such as account, dependency, or node count
- `JobExecution` for cwd, job name, environment, and output paths

```python
from molq import Duration, JobExecution, JobResources, JobScheduling, Memory

resources = JobResources(
    cpu_count=8,
    memory=Memory.gb(32),
    gpu_count=1,
    time_limit=Duration.hours(4),
)

scheduling = JobScheduling(queue="gpu", account="project123")
execution = JobExecution(cwd="/scratch/work", job_name="train-v1")
```

## Command Forms

Exactly one command form must be provided per submission:

| Parameter | Meaning |
|-----------|---------|
| `argv` | Structured arguments, never shell interpreted |
| `command` | A single-line shell command |
| `script` | Inline script content or a path to an existing script |

`argv` is the safest default. Use `script` when you need multi-line shell logic.

## Defaults

`SubmitorDefaults` lets you set reusable defaults for a cluster or team profile.

```python
from molq import Duration, JobResources, JobScheduling, Memory, Submitor, SubmitorDefaults

defaults = SubmitorDefaults(
    resources=JobResources(
        cpu_count=4,
        memory=Memory.gb(16),
        time_limit=Duration.hours(2),
    ),
    scheduling=JobScheduling(queue="compute"),
)

submitor = Submitor("hpc", "slurm", defaults=defaults)
```

Per-submit values override defaults at the field level.
