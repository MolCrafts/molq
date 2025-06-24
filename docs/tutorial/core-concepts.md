# Core Concepts

Molq has three key concepts you need to understand: **Decorators**, **Generators**, and **Submitters**.

## 1. Decorators Transform Functions

```python
from molq import submit

# Create submitters for different backends
local = submit('dev', 'local')
cluster = submit('hpc', 'slurm')

@local                           # This decorator...
def my_task():                   # ...transforms this function...
    yield {'cmd': ['echo', 'hi']} # ...into a job submitter
```

**What happens:** The decorator intercepts `yield` statements and submits jobs to the backend.

## 2. Generators Control Job Flow

Use `yield` to submit jobs and get results back:

```python
@cluster
def pipeline():
    # Submit first job
    job1 = yield {'cmd': ['prepare_data.py'], 'cpus': 4}

    # Submit second job that depends on first
    job2 = yield {
        'cmd': ['analyze.py'],
        'dependency': job1,
        'cpus': 16
    }

    return [job1, job2]
```

**Key point:** Each `yield` submits one job. Use dependencies to control execution order.

## 3. Submitters Execute Jobs

Different submitters handle different execution environments:

```python
# Local execution (for development/testing)
local = submit('dev', 'local')

# SLURM cluster (for production)
cluster = submit('prod', 'slurm')

# Same cluster name = same submitter instance
cluster2 = submit('prod', 'slurm')  # Returns existing submitter
```

**Submitter types:**
- `'local'` - Runs jobs on your local machine
- `'slurm'` - Submits jobs to SLURM cluster

## Job Configuration

All jobs need a `cmd` (command). Everything else is optional:

```python
# Minimal job
yield {'cmd': ['python', 'script.py']}

# Job with resources (SLURM only)
yield {
    'cmd': ['python', 'script.py'],
    'cpus': 16,
    'memory': '64GB',
    'time': '04:00:00'
}
```

**Common options:**
- `cmd` - Command to run (required)
- `job_name` - Human-readable name
- `cpus` - Number of CPU cores (SLURM)
- `memory` - Memory requirement (SLURM)
- `time` - Time limit in HH:MM:SS (SLURM)
- `dependency` - Wait for other jobs first

## Error Handling

Handle job failures with try/catch:

```python
@cluster
def robust_job():
    try:
        return yield {'cmd': ['risky_script.py']}
    except Exception:
        # Fallback plan
        return yield {'cmd': ['safe_script.py']}
```

## Best Practices

### Keep it Simple
```python
# Good: Clear and simple
@cluster
def train_model(data_file: str):
    yield {
        'cmd': ['python', 'train.py', data_file],
        'cpus': 16,
        'memory': '64GB',
        'time': '08:00:00'
    }

# Avoid: Over-complicated
@cluster
def complex_job():
    # Too much logic in job function
    if datetime.now().hour > 18:
        cpus = calculate_evening_cpus()
        memory = estimate_memory_with_overhead()
        # ... lots of complex logic
```

### Use Configuration Objects
```python
# Good: Reusable configurations
CONFIGS = {
    'small': {'cpus': 4, 'memory': '16GB', 'time': '02:00:00'},
    'large': {'cpus': 32, 'memory': '128GB', 'time': '12:00:00'}
}

@cluster
def scalable_job(size: str):
    config = CONFIGS[size].copy()
    config['cmd'] = ['python', 'process.py']
    yield config
```

That's it! The key is understanding these three concepts: decorators transform functions, generators control flow, and submitters execute jobs.

**Next:** [Advanced Usage](advanced-usage.md) for complex workflows and optimization techniques.
