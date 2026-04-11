# Job Configuration Specification

Complete specification for job configuration types.

## Command Specification

Every `submit()` call requires exactly one of three command forms:

| Parameter | Type | Description |
|-----------|------|-------------|
| `argv` | `list[str]` | Structured args, never shell-interpreted |
| `command` | `str` | Single-line shell command (no newlines) |
| `script` | `Script` | Inline text or file path |

```python
# argv -- safest, no shell expansion
s.submit(argv=["python", "train.py", "--lr", "0.001"])

# command -- single-line shell string
s.submit(command="python train.py && python eval.py")

# script -- multi-line or file reference
s.submit(script=Script.inline("python preprocess.py\npython train.py"))
s.submit(script=Script.path("/path/to/run.sh"))
```

Providing zero or more than one raises `CommandError`. Using `command` with newlines raises `CommandError` -- use `Script.inline()` instead.

## Resource Types

### JobResources

Hardware requirements for job execution.

| Field | Type | Default | SLURM | PBS | LSF |
|-------|------|---------|-------|-----|-----|
| `cpu_count` | `int` | `None` | `--ntasks` | `nodes=N:ppn=X` | `-n` |
| `memory` | `Memory` | `None` | `--mem` | `-l mem=` | `-M` |
| `gpu_count` | `int` | `None` | `--gres=gpu:N` | `-l gpus=N` | `-gpu num=N` |
| `gpu_type` | `str` | `None` | `--gres=gpu:TYPE:N` | -- | `-gpu gmodel=TYPE` |
| `time_limit` | `Duration` | `None` | `--time` | `-l walltime=` | `-W` |

### JobScheduling

Scheduler-level parameters.

| Field | Type | Default | SLURM | PBS | LSF |
|-------|------|---------|-------|-----|-----|
| `queue` | `str` | `None` | `--partition` | `-q` | `-q` |
| `account` | `str` | `None` | `--account` | `-A` | `-P` |
| `priority` | `str` | `None` | -- | -- | -- |
| `dependency` | `str` | `None` | `--dependency` | -- | -- |
| `node_count` | `int` | `None` | -- | `nodes=N` | -- |
| `exclusive_node` | `bool` | `False` | `--exclusive` | -- | -- |
| `array_spec` | `str` | `None` | `--array` | -- | `-J name[spec]` |
| `email` | `str` | `None` | -- | -- | -- |
| `qos` | `str` | `None` | `--qos` | -- | -- |
| `reservation` | `str` | `None` | `--reservation` | -- | -- |

### JobExecution

Execution environment parameters.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `env` | `dict[str, str]` | `None` | Environment variables merged with current env |
| `cwd` | `str \| Path` | `None` | Working directory (default: `os.getcwd()`) |
| `job_name` | `str` | `None` | Human-readable name |
| `output_file` | `str` | `None` | STDOUT redirect path |
| `error_file` | `str` | `None` | STDERR redirect path |

## Value Types

### Memory

Immutable memory quantity. Supports factory methods and parsing:

```python
Memory.gb(8)           # 8 GiB
Memory.parse("512MB")  # 512 MiB
Memory.parse("1024")   # 1024 bytes
```

### Duration

Immutable time duration. Supports factory methods and multiple formats:

```python
Duration.hours(4)           # 4 hours
Duration.parse("2h30m")     # 2h 30m
Duration.parse("01:30:00")  # HH:MM:SS
Duration.parse("5400")      # seconds
```

### Script

Immutable script reference:

```python
Script.inline("echo hello")        # Inline content
Script.path("./run.sh")            # File path (resolved to absolute)
```

## Defaults Merging

When a `Submitor` has defaults set, per-submit parameters override at the field level:

```python
defaults = SubmitorDefaults(
    resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
    scheduling=JobScheduling(queue="compute"),
)

s = Submitor("hpc", "slurm", defaults=defaults)

# Override cpu_count, keep memory from defaults
s.submit(
    argv=["python", "script.py"],
    resources=JobResources(cpu_count=8),  # memory stays 16GB
)
```

## Examples

### Basic Local Job

```python
from molq import Submitor

s = Submitor("devbox", "local")
job = s.submit(argv=["python", "script.py"])
record = job.wait()
```

### SLURM GPU Job

```python
from molq import Submitor, JobResources, JobScheduling, Memory, Duration

s = Submitor("hpc", "slurm")
job = s.submit(
    argv=["python", "train.py"],
    resources=JobResources(
        cpu_count=16,
        memory=Memory.gb(64),
        gpu_count=2,
        gpu_type="a100",
        time_limit=Duration.hours(8),
    ),
    scheduling=JobScheduling(queue="gpu", account="ml_project"),
)
```

### PBS Job with Script

```python
from molq import Submitor, Script, JobResources, Memory, Duration

s = Submitor("pbs_cluster", "pbs")
job = s.submit(
    script=Script.inline("""
module load python/3.10
cd $PBS_O_WORKDIR
python simulate.py
"""),
    resources=JobResources(
        cpu_count=32,
        memory=Memory.gb(64),
        time_limit=Duration.hours(24),
    ),
)
```
