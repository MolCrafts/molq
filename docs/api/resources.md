# Types

The `molq.types` module provides immutable value types for resource specification. All types are frozen dataclasses.

## Memory

Immutable memory quantity stored as bytes.

```python
from molq import Memory

# Factory methods
mem = Memory.kb(512)
mem = Memory.mb(256)
mem = Memory.gb(8)
mem = Memory.tb(1)

# Parse human-readable strings
mem = Memory.parse("8GB")
mem = Memory.parse("512M")
mem = Memory.parse("1024")  # bytes

# Access raw value
mem.bytes  # int

# Scheduler-specific formatting
mem.to_slurm()    # "8G"
mem.to_pbs()      # "8gb"
mem.to_lsf_kb()   # 8388608
```

### Supported Units

| Unit | Aliases | Bytes |
|------|---------|-------|
| Bytes | `b`, `B`, (empty) | 1 |
| Kilobytes | `kb`, `KB` | 1,024 |
| Megabytes | `mb`, `MB` | 1,048,576 |
| Gigabytes | `gb`, `GB` | 1,073,741,824 |
| Terabytes | `tb`, `TB` | 1,099,511,627,776 |

## Duration

Immutable time duration stored as seconds.

```python
from molq import Duration

# Factory methods
d = Duration.minutes(30)
d = Duration.hours(4)

# Parse various formats
d = Duration.parse("2h30m")     # 2 hours 30 minutes
d = Duration.parse("01:30:00")  # HH:MM:SS
d = Duration.parse("90m")       # 90 minutes
d = Duration.parse("3600")      # plain seconds

# Access raw value
d.seconds  # int

# Scheduler-specific formatting
d.to_slurm()        # "02:30:00" or "1-00:00:00" for > 24h
d.to_pbs()           # "02:30:00"
d.to_lsf_minutes()   # 150
```

## Script

Immutable script reference -- either inline text or a file path.

```python
from molq import Script

# Inline script content
s = Script.inline("""
#!/bin/bash
cd /workspace
python train.py --epochs 100
python eval.py
""")

# Reference to an existing file
s = Script.path("/path/to/run.sh")
s = Script.path("./relative/script.sh")  # Resolved to absolute

# Properties
s.variant    # "inline" or "path"
s.text       # str | None (inline content)
s.file_path  # Path | None (resolved path)
```

## JobResources

Hardware requirements for job execution.

```python
from molq import JobResources, Memory, Duration

resources = JobResources(
    cpu_count=8,
    memory=Memory.gb(32),
    gpu_count=2,
    gpu_type="a100",
    time_limit=Duration.hours(4),
)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cpu_count` | `int \| None` | `None` | CPU cores |
| `memory` | `Memory \| None` | `None` | Memory requirement |
| `gpu_count` | `int \| None` | `None` | GPU count |
| `gpu_type` | `str \| None` | `None` | GPU type (e.g. `"a100"`, `"v100"`) |
| `time_limit` | `Duration \| None` | `None` | Time limit |

## JobScheduling

Scheduler-level parameters.

```python
from molq import JobScheduling

scheduling = JobScheduling(
    queue="gpu",
    account="project123",
    priority="high",
    dependency="afterok:12345",
    node_count=2,
    exclusive_node=True,
    array_spec="1-100",
    qos="normal",
)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `queue` | `str \| None` | `None` | Partition/queue name |
| `account` | `str \| None` | `None` | Billing account |
| `priority` | `str \| None` | `None` | Job priority |
| `dependency` | `str \| None` | `None` | Dependency specification |
| `node_count` | `int \| None` | `None` | Number of nodes |
| `exclusive_node` | `bool` | `False` | Exclusive node access |
| `array_spec` | `str \| None` | `None` | Array job specification |
| `email` | `str \| None` | `None` | Email for notifications |
| `qos` | `str \| None` | `None` | Quality of service |
| `reservation` | `str \| None` | `None` | Scheduler reservation |

## JobExecution

Execution environment parameters.

```python
from molq import JobExecution

execution = JobExecution(
    env={"CUDA_VISIBLE_DEVICES": "0,1"},
    cwd="/scratch/project",
    job_name="training_v2",
    output_file="train.out",
    error_file="train.err",
)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `env` | `dict[str, str] \| None` | `None` | Environment variables |
| `cwd` | `str \| Path \| None` | `None` | Working directory |
| `job_name` | `str \| None` | `None` | Human-readable job name |
| `output_file` | `str \| None` | `None` | STDOUT file path |
| `error_file` | `str \| None` | `None` | STDERR file path |
