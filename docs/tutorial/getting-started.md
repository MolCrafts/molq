# Getting Started with Molq

This tutorial will guide you through installing Molq and running your first job. We will start with a local setup so you can test everything immediately without needing access to a cluster.

## 1. Installation

First, we need to install the Molq library. It is available via pip, making it easy to add to your Python environment.

```bash
pip install molq
```

Once installed, you can import it in your Python scripts.

## 2. Your First Submitter

In Molq, a **Submitter** is the agent responsible for taking your job descriptions and sending them to a compute backend. Before you can run any jobs, you need to tell Molq *where* to run them.

We typically start with a **Local Submitter**. This runs jobs directly on your current machine as subprocesses. It is perfect for development, testing, and small-scale workflows.

```python
from molq import submit

# Create a submitter named 'dev' that uses the 'local' backend.
# The first argument is a unique name for this submitter instance.
# The second argument 'local' specifies the backend type.
local_runner = submit('dev', 'local')
```

## 3. Defining a Job

Molq uses a unique pattern to define jobs: **decorated generator functions**.

Why generators? Because a workflow often involves submitting a job, waiting for it, and then doing something with the result (like submitting another job). Python's `yield` keyword provides a perfect mechanism for this "pause and resume" behavior.

To define a job, you write a function that `yields` a configuration dictionary. You then apply your submitter (`@local_runner`) as a decorator.

```python
@local_runner
def run_simulation(steps: int):
    # This dictionary describes the job to the backend
    job_config = {
        'cmd': ['echo', f'Running simulation with {steps} steps...'],
        'job_name': 'sim_test',
        'env': {'SIM_LEVEL': '1'}
    }
    
    # We 'yield' the config to submit the job.
    # The submitter receives this, runs the command, and sends back the job_id.
    job_id = yield job_config
    
    return job_id
```

## 4. Running the Job

Now that we have defined the job, running it is as simple as calling the Python function.

When you call the decorated function, Molq takes over. It steps through your generator, handling every `yield` by submitting a job to the configured backend.

```python
if __name__ == "__main__":
    print("Submitting job...")
    
    # This call submits the job to the local runner
    result_id = run_simulation(100)
    
    print(f"Job submitted! ID: {result_id}")
```

If you run this script, you will see the output of your job (since it's running locally) or a confirmation that it was submitted.

## Next Steps

Now that you have run a simple local job, you are ready to explore the deeper concepts that make Molq powerful.

*   **[Core Concepts](core-concepts.md)**: Learn more about how the Submitter, Decorator, and Job Spec work together.
*   **[Recipes](../recipes/machine-learning.md)**: See real-world examples of Molq in action.
