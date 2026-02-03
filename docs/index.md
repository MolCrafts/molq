# Molq: Modern Job Queue Interface

## What is Molq?

Molq is a seamless bridge between your Python code and computational resources. It allows you to define complex computational workflows—whether they are data processing pipelines, machine learning training loops, or scientific simulations—and execute them on various backends, from your local machine to high-performance SLURM clusters.

At its core, Molq represents a job submission system that decouples **logic definition** from **execution details**. You write your workflow once in Python, and Molq handles the translation to the specific commands required by your compute environment.

## Why do you need plain Python job submission?

If you have ever worked with compute clusters, you are likely familiar with the friction of writing shell scripts (like `.submit` or `sbatch` files) to run your code. This traditional approach has several downsides:

1.  **Context Switching**: You switch between Python for logic and Shell for execution.
2.  **Hardcoded Resources**: Resource requirements are often buried in shell scripts, making them hard to adjust dynamically.
3.  **Complex Dependencies**: Managing dependencies between jobs (e.g., "run job B after job A finishes") requires brittle logic in shell scripts.
4.  **Vendor Lock-in**: Scripts written for SLURM don't work locally or on other schedulers without rewriting.

Molq solves these problems by bringing job submission into your Python code. By treating jobs as Python objects and dependencies as code flow, you gain the full power of the Python language—loops, conditionals, exception handling, and shared configuration—to manage your computational infrastructure.

## How to use Molq

The simplest way to use Molq is through its decorator interface. You create a **Submitter** that targets your desired backend, and then decorate a generator function to define your job.

Here is a simple example that defines a job to greet the world.

```python
from molq import submit

# 1. Create a submitter
# We create a 'local' submitter named 'dev'. This will run jobs on your machine.
# In production, you might change 'local' to 'slurm'.
local = submit('dev', 'local')

# 2. Define your job
# The decorator tells Molq that this function generates jobs.
@local
def hello_world(name: str):
    """A simple job that prints a greeting."""
    
    # 3. Yield the job specification
    # Instead of running code directly, we 'yield' a dictionary describing the job.
    # Molq takes this description and submits it to the backend.
    job_id = yield {
        'cmd': ['echo', f'Hello, {name}!'],
        'job_name': 'greeting',
        'block': True  # Wait for this job to finish before continuing
    }
    
    return f"Job {job_id} completed successfully"

# 4. Execute
# Calling the function submits the job.
result = hello_world("World")
print(result)
```

In this example, the `hello_world` function doesn't execute `echo` directly. Instead, it **yields** a request to run `echo`. Molq intercepts this request, runs the command (locally or on a cluster), and returns the `job_id`. This pattern allows you to compose complex workflows where Python logic orchestrates external processes naturally.
