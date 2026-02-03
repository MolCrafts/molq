# Core Concepts

Molq is built on three fundamental concepts that work together to manage your workflows: **Job Specifications**, **Submitters**, and the **Decorator Interface**. Understanding these will render you capable of building complex, scalable pipelines.

## 1. Job Specifications

### What are they?
A Job Specification (or "Job Spec") is a dictionary that describes *what* you want to run. It contains all the necessary details for a scheduler to execute your task, such as the command line, resource requirements, and environment variables.

### Why do we need them?
Compute environments need to know ahead of time what resources a job will consume. A scheduler like SLURM needs to know if you need 1 CPU or 100, and for how long, so it can reserve those resources. By making these explicit in a data structure (a dictionary), we decouple the *what* from the *how*.

### How to use them
In Molq, you define the spec as a standard Python dictionary.

```python
job_spec = {
    # The command to run (Result: execution of this list)
    'cmd': ['python', 'train_model.py', '--epochs', '100'],
    
    # Resources (crucial for clusters)
    'cpus': 4,
    'memory': '16GB',
    'time': '04:00:00',
    
    # Metadata
    'job_name': 'training_run_v1'
}
```

## 2. Submitters

### What are they?
A Submitter is an object that knows how to talk to a specific execution backend. It takes a Job Spec and translates it into the native language of the backend (e.g., generating an `sbatch` script for SLURM or a `subprocess` call for local execution).

### Why do we need them?
Different environments have different rules. A local machine just runs a process. A cluster requires authentication, script generation, queue submission, and status polling. The Submitter abstracts complexity away, allowing your code to remain agnostic to the hardware it runs on. You can switch from local testing to cluster production simply by swapping the Submitter.

### How to use them
You initialize a submitter using the `submit` factory function.

```python
from molq import submit

# A local submitter for testing
# Usage: Runs jobs immediately as subprocesses
dev_backend = submit('dev_cluster', 'local')

# A SLURM submitter for production
# Usage: Generates .sbatch files and runs 'sbatch' command
prod_backend = submit('prod_cluster', 'slurm')
```

## 3. The Decorator & Generator Interface

### What is it?
This is the "magic" that binds your Python functions to the Submitters. By decorating a generator function with a Submitter, you turn a regular Python function into a job definition.

### Why do we need it?
We need a way to integrate job submission into Python's control flow. Simply calling a function `submit_job(...)` works, but it can be clunky for complex workflows involving dependencies.

Generators allow us to **pause** execution. When you `yield` a Job Spec, your function pauses. Molq takes the spec, submits the job, and (optionally) waits for it or returns the ID immediately. This allows you to write linear, readable code that actually orchestrates asynchronous, distributed processes.

### How to use it
Apply the submitter instance as a decorator (`@backend`) to any function that `yields` job specs.

```python
@prod_backend
def complex_workflow(data_file):
    # Step 1: Submit a preprocessing job
    # We yield the spec, and get back a job ID
    prep_id = yield {
        'cmd': ['python', 'preprocess.py', data_file],
        'cpus': 2
    }
    
    # Step 2: Submit a training job that depends on Step 1
    # We use the previous job ID as a dependency
    train_id = yield {
        'cmd': ['python', 'train.py'],
        'dependency': prep_id,  # Wait for prep_id to finish
        'gpus': 1
    }
    
    return train_id
```

In this example, the logic flows naturally from top to bottom, but under the hood, Molq is orchestrating distinct jobs on a remote cluster.
