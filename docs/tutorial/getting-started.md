# Getting Started

## Installation

```bash
pip install molq
```

## Your First Job

```python
from molq import submit

# Create a local submitter (runs on your machine)
local = submit('dev', 'local')

@local
def hello_world():
    job_id = yield {'cmd': ['echo', 'Hello, Molq!']}
    return job_id

# Run it
result = hello_world()
print(f"Job {result} completed")
```

## Basic Concepts

**Three things to remember:**
1. `submit('name', 'type')` creates a submitter
2. `@submitter` decorates functions to submit jobs
3. `yield {'cmd': [...]}` submits one job

## Local vs Cluster

```python
# Local execution (development/testing)
local = submit('dev', 'local')

@local
def dev_job():
    yield {'cmd': ['python', 'test_script.py']}

# Cluster execution (production)
cluster = submit('prod', 'slurm')

@cluster
def prod_job():
    yield {
        'cmd': ['python', 'production_script.py'],
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00'
    }
```

## Multi-Step Workflows

```python
@cluster
def data_pipeline():
    # Step 1: Clean data
    clean_id = yield {
        'cmd': ['python', 'clean.py'],
        'cpus': 4
    }

    # Step 2: Analyze (waits for cleaning to finish)
    analyze_id = yield {
        'cmd': ['python', 'analyze.py'],
        'dependency': clean_id,
        'cpus': 16
    }

    return [clean_id, analyze_id]
```

## Common Job Options

```python
yield {
    'cmd': ['python', 'script.py'],     # Required: command to run
    'job_name': 'my_analysis',          # Optional: human-readable name
    'cpus': 8,                          # SLURM: number of CPU cores
    'memory': '32GB',                   # SLURM: memory requirement
    'time': '02:00:00',                 # SLURM: time limit (HH:MM:SS)
    'env': {'VAR': 'value'}             # Optional: environment variables
}
```

## Error Handling

```python
@cluster
def robust_job():
    try:
        return yield {'cmd': ['python', 'might_fail.py']}
    except Exception:
        # Fallback if job fails
        return yield {'cmd': ['python', 'backup_plan.py']}
```

## Next Steps

- **[Core Concepts](core-concepts.md)** - Understand decorators, generators, and submitters
- **[Recipes](../recipes/machine-learning.md)** - Real-world examples
