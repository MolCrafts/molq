# Machine Learning Workflows

Machine Learning workflows often involve complex dependency chains—such as preprocessing data before training, or evaluating multiple models in parallel. Additionally, they often require specific hardware resources like GPUs.

Molq simplifies these workflows by allowing you to:
1.  **Define Dependencies**: Ensure training doesn't start until preprocessing is complete.
2.  **Request GPUs**: Easily specify `gpus: 1` in your job spec.
3.  **Parallelize**: Submit multiple training jobs at once (e.g., for hyperparameter tuning).

## Model Training Pipeline

This example demonstrates a standard sequence: Preprocessing -> Training. Note how the `train_job` uses the `dependency` key to wait for `prep_job`.

```python
from molq import submit

cluster = submit('ml_cluster', 'slurm')

@cluster
def train_model(dataset: str, model_type: str):
    # Preprocess data
    prep_job = yield {
        'cmd': ['python', 'preprocess.py', dataset],
        'cpus': 4, 'memory': '16GB', 'time': '01:00:00'
    }

    # Train model
    # We pass 'prep_job' (which is a job_id) as a dependency.
    # The scheduler will hold this job until prep_job is successful.
    train_job = yield {
        'cmd': ['python', 'train.py', model_type],
        'dependency': prep_job,
        'cpus': 16, 'memory': '64GB', 'time': '08:00:00',
        'gpus': 1  # Request 1 GPU
    }

    return train_job
```

## Hyperparameter Tuning

Hyperparameter tuning is a classic "embarrassingly parallel" problem. With Molq, you can simply loop over your parameters and `yield` a job for each one. Molq will submit them all to the queue, and the scheduler will run as many as possible in parallel.

```python
@cluster
def hyperparameter_search(param_grid: list):
    jobs = []
    for params in param_grid:
        # Submit a job for this parameter set
        job = yield {
            'cmd': ['python', 'train.py', '--params', str(params)],
            'cpus': 8, 'memory': '32GB', 'time': '04:00:00',
            'job_name': f'train_{params["lr"]}'
        }
        jobs.append(job)
    
    return jobs

# Usage
param_combinations = [
    {'lr': 0.01, 'batch_size': 32},
    {'lr': 0.001, 'batch_size': 64},
]
# This submits 2 jobs instantly
search_jobs = hyperparameter_search(param_combinations)
```

## Distributed Training

For large models, you may need multiple nodes. Molq supports this via the `nodes` and `ntasks_per_node` parameters, which map directly to scheduler options (like `#SBATCH --nodes`).

```python
@cluster
def distributed_training(nodes: int):
    # We yield the configuration to start the job
    job_id = yield {
        'cmd': ['python', '-m', 'torch.distributed.launch',
               '--nproc_per_node=4', 'train_distributed.py'],
        'nodes': nodes,
        'cpus': 16,
        'memory': '64GB',
        'time': '12:00:00',
        'gpus': 4  # 4 GPUs per node
    }
    return job_id
```
