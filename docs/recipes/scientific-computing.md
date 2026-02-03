# Scientific Computing Workflows

Scientific computing often involves long-running simulations (like Molecular Dynamics) and extensive parameter sweeps. Molq helps manage these by providing a stable interface to High Performance Computing (HPC) clusters like SLURM.

## Molecular Dynamics

Molecular Dynamics (MD) simulations often require massive parallelism (MPI). Molq allows you to specify MPI requirements easily.

```python
from molq import submit

cluster = submit('hpc_cluster', 'slurm')

@cluster
def run_md_simulation(system_file: str, steps: int):
    # Step 1: Preparation
    prep_job = yield {
        'cmd': ['python', 'prepare.py', system_file],
        'cpus': 4, 'memory': '16GB', 'time': '01:00:00'
    }

    # Step 2: Running the simulation
    # We use 'dependency' to ensure prep finishes first.
    sim_job = yield {
        'cmd': ['gmx', 'mdrun', '-s', 'system.tpr', '-nsteps', str(steps)],
        'dependency': prep_job,
        'cpus': 32, 'memory': '64GB', 'time': '24:00:00'
    }

    return sim_job
```

## Parameter Sweeps

Running the same simulation with different parameters is a common task. In bash, this often involves complex loops and string manipulation. In Molq, it's just a Python loop.

```python
@cluster
def parameter_sweep(param_values: list):
    jobs = []
    for value in param_values:
        # Submit one job per parameter value
        job = yield {
            'cmd': ['python', 'simulate.py', '--param', str(value)],
            'cpus': 8, 'memory': '16GB', 'time': '04:00:00',
            'job_name': f'sim_{value}'
        }
        jobs.append(job)
    return jobs

# Usage is clean and readable
temperatures = [300, 310, 320, 330, 340]
sweep_jobs = parameter_sweep(temperatures)
```

## High-Throughput Batch Processing

When you have thousands of files to process, submitting thousands of jobs can overwhelm the scheduler. It is often better to batch them. With Python, you can easily slice your inputs into chunks.

```python
@cluster
def batch_processing(input_files: list, batch_size: int = 10):
    # Process files in batches to reduce job count
    for i in range(0, len(input_files), batch_size):
        batch = input_files[i:i+batch_size]
        
        # Submit one job for the whole batch
        yield {
            'cmd': ['python', 'batch_process.py'] + batch,
            'cpus': 16, 'memory': '32GB', 'time': '08:00:00',
            'job_name': f'batch_{i}'
        }
```
