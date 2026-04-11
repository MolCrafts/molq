# Advanced Usage

Advanced patterns for building sophisticated workflows with Molq.

## Workflow Orchestration

### Sequential Pipeline

Chain jobs where each step depends on the previous:

```python
from molq import Submitor, JobResources, Memory, Duration

cluster = Submitor("hpc", "slurm")

# Step 1: Preprocessing
prep = cluster.submit(
    argv=["python", "preprocess.py", "data.csv"],
    resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
)
prep_record = prep.wait()

# Step 2: Training (only if preprocessing succeeded)
if prep_record.state.value == "succeeded":
    train = cluster.submit(
        argv=["python", "train.py"],
        resources=JobResources(
            cpu_count=16, memory=Memory.gb(64),
            gpu_count=2, time_limit=Duration.hours(8),
        ),
    )
    train_record = train.wait()

    # Step 3: Evaluation
    if train_record.state.value == "succeeded":
        eval_job = cluster.submit(
            argv=["python", "evaluate.py"],
            resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
        )
        eval_job.wait()
```

### Parallel Fan-Out with Collect

Submit independent jobs in parallel, then collect results:

```python
from molq import Submitor, JobResources, JobExecution, Memory

cluster = Submitor("hpc", "slurm")

# Fan out: submit independent preprocessing jobs
prep_jobs = []
for i in range(4):
    job = cluster.submit(
        argv=["python", "preprocess.py", f"--chunk={i}"],
        resources=JobResources(cpu_count=4, memory=Memory.gb(8)),
        execution=JobExecution(job_name=f"prep_chunk_{i}"),
    )
    prep_jobs.append(job)

# Collect: wait for all to complete
records = cluster.watch([j.job_id for j in prep_jobs])

# Check results
failed = [r for r in records if r.state.value != "succeeded"]
if failed:
    for r in failed:
        print(f"Failed: {r.job_id[:8]} ({r.failure_reason})")
    raise SystemExit(1)

# Merge: submit aggregation job
merge = cluster.submit(
    argv=["python", "merge.py"],
    resources=JobResources(cpu_count=16, memory=Memory.gb(32)),
)
merge.wait()
```

### Dynamic Resource Allocation

Scale resources based on problem characteristics:

```python
from molq import Submitor, JobResources, Memory, Duration

cluster = Submitor("hpc", "slurm")


def submit_adaptive(data_size_mb: int):
    # Scale resources with data size
    cpus = min(32, max(4, data_size_mb // 1000))
    memory_gb = min(128, max(16, data_size_mb // 100))
    hours = min(24, max(1, data_size_mb // 5000))

    job = cluster.submit(
        argv=["python", "process.py", f"--size={data_size_mb}"],
        resources=JobResources(
            cpu_count=cpus,
            memory=Memory.gb(memory_gb),
            time_limit=Duration.hours(hours),
        ),
    )
    return job


# Small job: 4 CPUs, 16GB, 1h
small = submit_adaptive(500)

# Large job: 32 CPUs, 128GB, 20h
large = submit_adaptive(100_000)
```

## Error Handling

### Checking Job Results

```python
from molq import Submitor, JobState

cluster = Submitor("hpc", "slurm")

job = cluster.submit(argv=["python", "risky_script.py"])
record = job.wait()

if record.state == JobState.SUCCEEDED:
    print(f"Success (exit={record.exit_code})")
elif record.state == JobState.FAILED:
    print(f"Failed: {record.failure_reason}")
elif record.state == JobState.TIMED_OUT:
    print("Exceeded time limit")
elif record.state == JobState.LOST:
    print("Job disappeared from scheduler")
```

### Retry Pattern

```python
from molq import Submitor, JobResources, Memory
import time

cluster = Submitor("hpc", "slurm")


def submit_with_retry(argv: list[str], max_attempts: int = 3):
    for attempt in range(max_attempts):
        job = cluster.submit(
            argv=argv,
            resources=JobResources(cpu_count=8, memory=Memory.gb(32)),
        )
        record = job.wait()

        if record.state.value == "succeeded":
            return record

        print(f"Attempt {attempt + 1} failed: {record.state.value}")
        if attempt < max_attempts - 1:
            time.sleep(2**attempt)  # Exponential backoff

    raise RuntimeError(f"Failed after {max_attempts} attempts")


result = submit_with_retry(["python", "flaky_script.py"])
```

### Timeout Handling

```python
from molq import Submitor, MolqTimeoutError

cluster = Submitor("hpc", "slurm")

job = cluster.submit(argv=["python", "long_job.py"])

try:
    record = job.wait(timeout=3600)  # 1 hour max
    print(f"Completed: {record.state.value}")
except MolqTimeoutError:
    print("Job still running after 1 hour, cancelling...")
    job.cancel()
```

## Submitor Defaults

### Per-Team Configuration

```python
from molq import Submitor, SubmitorDefaults, JobResources, JobScheduling, Memory

# Team-wide defaults
ml_defaults = SubmitorDefaults(
    resources=JobResources(cpu_count=8, memory=Memory.gb(32)),
    scheduling=JobScheduling(queue="gpu", account="ml_team"),
)

ml_cluster = Submitor("hpc", "slurm", defaults=ml_defaults)

# Every submit uses team defaults unless overridden
job = ml_cluster.submit(argv=["python", "train.py"])  # 8 CPUs, 32GB, gpu queue
```

### Override Individual Fields

Per-submit parameters override defaults at the field level:

```python
# Override GPU count but keep default CPU, memory, and queue
job = ml_cluster.submit(
    argv=["python", "big_train.py"],
    resources=JobResources(gpu_count=4),  # cpu_count=8, memory=32GB still from defaults
)
```

## Scheduler Options

### Custom Binary Paths

```python
from molq import Submitor, SlurmSchedulerOptions

s = Submitor(
    "hpc", "slurm",
    scheduler_options=SlurmSchedulerOptions(
        sbatch_path="/opt/slurm/bin/sbatch",
        squeue_path="/opt/slurm/bin/squeue",
        scancel_path="/opt/slurm/bin/scancel",
        sacct_path="/opt/slurm/bin/sacct",
        extra_sbatch_flags=("--clusters=gpu",),
    ),
)
```

### PBS with Custom Paths

```python
from molq import Submitor, PBSSchedulerOptions

s = Submitor(
    "pbs_cluster", "pbs",
    scheduler_options=PBSSchedulerOptions(
        qsub_path="/opt/pbs/bin/qsub",
        extra_qsub_flags=("-V",),
    ),
)
```

## Script Patterns

### Module Loading

```python
from molq import Submitor, Script

cluster = Submitor("hpc", "slurm")

job = cluster.submit(
    script=Script.inline("""
module load python/3.10
module load cuda/12.0
source /shared/envs/ml/bin/activate
python train.py --config config.yaml
"""),
)
```

### Using Existing Scripts

```python
from molq import Script

# Reference an existing script file
job = cluster.submit(script=Script.path("scripts/run_experiment.sh"))
```

## Monitoring Patterns

### Polling Active Jobs

```python
from molq import Submitor

cluster = Submitor("hpc", "slurm")

# Refresh all active jobs with scheduler
cluster.refresh()

# List current state
for r in cluster.list():
    print(f"{r.job_id[:8]}  {r.state.value}  {r.command_display[:40]}")
```

### Watch All Active Jobs

```python
# Block until ALL active jobs complete
records = cluster.watch()

succeeded = sum(1 for r in records if r.state.value == "succeeded")
failed = sum(1 for r in records if r.state.value == "failed")
print(f"Results: {succeeded} succeeded, {failed} failed")
```

### Job Handle Refresh

```python
job = cluster.submit(argv=["python", "script.py"])

# Later: check current state without blocking
job.refresh()
print(job.status())  # Updated from scheduler
```

## Metadata

Attach user-defined key-value metadata to jobs:

```python
job = cluster.submit(
    argv=["python", "train.py"],
    metadata={
        "experiment": "ablation_v3",
        "dataset": "imagenet",
        "author": "alice",
    },
)

# Metadata is stored and retrievable
record = cluster.get(job.job_id)
print(record.metadata)  # {"experiment": "ablation_v3", ...}
```

## Custom JobStore

Use an in-memory store for testing or a custom path:

```python
from molq import Submitor
from molq.store import JobStore

# In-memory (no persistence, great for tests)
s = Submitor("test", "local", store=JobStore(":memory:"))

# Custom file path
s = Submitor("prod", "slurm", store=JobStore("/data/molq/jobs.db"))
```
