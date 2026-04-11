# Machine Learning Workflows

Machine learning workflows often involve sequential pipelines, GPU resources, and parallel hyperparameter sweeps. Molq handles all of these with typed resource specs.

## Model Training Pipeline

A standard preprocessing -> training pipeline:

```python
from molq import Submitor, JobResources, JobScheduling, Memory, Duration

cluster = Submitor("ml_cluster", "slurm")

# Step 1: Preprocessing
prep = cluster.submit(
    argv=["python", "preprocess.py", "dataset.csv"],
    resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
)
prep_record = prep.wait()

# Step 2: Training (only after preprocessing succeeds)
if prep_record.state.value == "succeeded":
    train = cluster.submit(
        argv=["python", "train.py", "--model", "resnet50"],
        resources=JobResources(
            cpu_count=16,
            memory=Memory.gb(64),
            gpu_count=1,
            time_limit=Duration.hours(8),
        ),
        scheduling=JobScheduling(queue="gpu"),
    )
    train_record = train.wait()
    print(f"Training: {train_record.state.value}")
```

## Hyperparameter Tuning

Submit multiple training jobs in parallel with different parameters:

```python
from molq import Submitor, JobResources, JobScheduling, JobExecution, Memory, Duration

cluster = Submitor("ml_cluster", "slurm")

param_grid = [
    {"lr": "0.01", "batch_size": "32"},
    {"lr": "0.001", "batch_size": "64"},
    {"lr": "0.0001", "batch_size": "128"},
]

jobs = []
for params in param_grid:
    job = cluster.submit(
        argv=[
            "python", "train.py",
            "--lr", params["lr"],
            "--batch-size", params["batch_size"],
        ],
        resources=JobResources(
            cpu_count=8,
            memory=Memory.gb(32),
            gpu_count=1,
            time_limit=Duration.hours(4),
        ),
        scheduling=JobScheduling(queue="gpu"),
        execution=JobExecution(job_name=f"hp_lr{params['lr']}"),
    )
    jobs.append(job)

# Wait for all jobs to finish
records = cluster.watch([j.job_id for j in jobs], timeout=14400)

for r in records:
    print(f"{r.command_display[:40]}  {r.state.value}  exit={r.exit_code}")
```

## Distributed Training

Multi-node training with `torch.distributed`:

```python
from molq import Submitor, JobResources, JobScheduling, Memory, Duration, Script

cluster = Submitor("ml_cluster", "slurm")

job = cluster.submit(
    script=Script.inline("""
module load cuda/12.0
export MASTER_ADDR=$(hostname)
export MASTER_PORT=29500
srun python -m torch.distributed.run \\
    --nproc_per_node=4 \\
    train_distributed.py
"""),
    resources=JobResources(
        cpu_count=16,
        memory=Memory.gb(128),
        gpu_count=4,
        time_limit=Duration.hours(12),
    ),
    scheduling=JobScheduling(
        queue="gpu",
        node_count=2,
        exclusive_node=True,
    ),
)

record = job.wait()
print(f"Training: {record.state.value}")
```

## Evaluation Pipeline

Run evaluation after training completes:

```python
from molq import Submitor, JobResources, Memory, Duration

cluster = Submitor("ml_cluster", "slurm")

# Train
train_job = cluster.submit(
    argv=["python", "train.py", "--output", "model.pt"],
    resources=JobResources(
        cpu_count=16, memory=Memory.gb(64),
        gpu_count=2, time_limit=Duration.hours(8),
    ),
)
train_record = train_job.wait()

# Evaluate on multiple datasets
if train_record.state.value == "succeeded":
    eval_jobs = []
    for dataset in ["test_a", "test_b", "test_c"]:
        job = cluster.submit(
            argv=["python", "evaluate.py", "--model", "model.pt", "--data", dataset],
            resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
        )
        eval_jobs.append(job)

    records = cluster.watch([j.job_id for j in eval_jobs])
    for r in records:
        print(f"{r.command_display}  -> {r.state.value}")
```
