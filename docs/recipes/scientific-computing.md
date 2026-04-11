# Scientific Computing Workflows

Molq handles common HPC patterns: long-running simulations, parameter sweeps, and batch processing.

## Molecular Dynamics

A preparation -> simulation pipeline:

```python
from molq import Submitor, JobResources, JobScheduling, Memory, Duration

cluster = Submitor("hpc", "slurm")

# Step 1: System preparation
prep = cluster.submit(
    argv=["python", "prepare_system.py", "topology.pdb"],
    resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
)
prep.wait()

# Step 2: MD simulation
sim = cluster.submit(
    argv=["gmx", "mdrun", "-s", "system.tpr", "-nsteps", "1000000"],
    resources=JobResources(
        cpu_count=32,
        memory=Memory.gb(64),
        time_limit=Duration.hours(24),
    ),
    scheduling=JobScheduling(queue="compute", exclusive_node=True),
)

record = sim.wait()
print(f"Simulation: {record.state.value}")
```

## Parameter Sweeps

Run the same simulation with different parameters:

```python
from molq import Submitor, JobResources, JobExecution, Memory, Duration

cluster = Submitor("hpc", "slurm")

temperatures = [300, 310, 320, 330, 340, 350]

jobs = []
for temp in temperatures:
    job = cluster.submit(
        argv=["python", "simulate.py", "--temperature", str(temp)],
        resources=JobResources(
            cpu_count=8,
            memory=Memory.gb(16),
            time_limit=Duration.hours(4),
        ),
        execution=JobExecution(job_name=f"sim_T{temp}"),
    )
    jobs.append(job)

# Wait for all sweep jobs
records = cluster.watch([j.job_id for j in jobs])

for r in records:
    print(f"{r.command_display}  {r.state.value}")
```

## Array Jobs

SLURM array jobs for large sweeps:

```python
from molq import Submitor, JobResources, JobScheduling, Memory, Duration, Script

cluster = Submitor("hpc", "slurm")

job = cluster.submit(
    script=Script.inline("""
INPUT_FILE="input_${SLURM_ARRAY_TASK_ID}.dat"
python process.py "$INPUT_FILE" --output "result_${SLURM_ARRAY_TASK_ID}.json"
"""),
    resources=JobResources(
        cpu_count=4,
        memory=Memory.gb(8),
        time_limit=Duration.hours(2),
    ),
    scheduling=JobScheduling(array_spec="1-100"),
)
```

## Batch Processing

Process large file sets efficiently by batching:

```python
from molq import Submitor, JobResources, JobExecution, Memory, Duration
from pathlib import Path

cluster = Submitor("hpc", "slurm")

input_files = sorted(Path("data/").glob("*.xyz"))
batch_size = 10

jobs = []
for i in range(0, len(input_files), batch_size):
    batch = input_files[i : i + batch_size]
    argv = ["python", "batch_process.py"] + [str(f) for f in batch]

    job = cluster.submit(
        argv=argv,
        resources=JobResources(
            cpu_count=16,
            memory=Memory.gb(32),
            time_limit=Duration.hours(8),
        ),
        execution=JobExecution(job_name=f"batch_{i // batch_size}"),
    )
    jobs.append(job)

records = cluster.watch([j.job_id for j in jobs])
succeeded = sum(1 for r in records if r.state.value == "succeeded")
print(f"{succeeded}/{len(records)} batches succeeded")
```

## Multi-Stage Pipeline

A complete analysis pipeline with error handling:

```python
from molq import Submitor, JobResources, Memory, Duration

cluster = Submitor("hpc", "slurm")

# Stage 1: Preprocessing
prep = cluster.submit(
    argv=["python", "preprocess.py", "raw_data/"],
    resources=JobResources(cpu_count=8, memory=Memory.gb(32)),
)
prep_record = prep.wait()

if prep_record.state.value != "succeeded":
    print(f"Preprocessing failed: {prep_record.failure_reason}")
    raise SystemExit(1)

# Stage 2: Analysis
analysis = cluster.submit(
    argv=["python", "analyze.py", "--input", "processed/"],
    resources=JobResources(
        cpu_count=32,
        memory=Memory.gb(128),
        time_limit=Duration.hours(12),
    ),
)
analysis_record = analysis.wait()

# Stage 3: Post-processing
if analysis_record.state.value == "succeeded":
    post = cluster.submit(
        argv=["python", "postprocess.py", "--output", "results/"],
        resources=JobResources(cpu_count=4, memory=Memory.gb(16)),
    )
    post.wait()
    print("Pipeline complete")
```
