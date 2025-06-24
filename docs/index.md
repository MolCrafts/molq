# Molq: Molcrafts Queue Interface

Molq is a unified and flexible job queue system designed for both local execution and cluster computing environments. It provides a clean, decorator-based API that makes it easy to submit, monitor, and manage computational tasks across different execution backends.

## Key Features

- **Unified Interface**: Single API for local and cluster execution
- **Decorator-Based**: Simple, Pythonic syntax using decorators
- **Generator Support**: Advanced control flow with generator-based tasks
- **Multiple Backends**: Support for local execution, SLURM clusters, and more
- **Job Monitoring**: Built-in status tracking and error handling
- **Resource Management**: Flexible resource allocation and cleanup

## Quick Start

```python
from molq import submit

# Create a local submitter
local = submit('my_cluster', 'local')

@local
def hello_world(name: str):
    """A simple job that prints a greeting."""
    job_id = yield {
        'cmd': ['echo', f'Hello, {name}!'],
        'job_name': 'greeting',
        'block': True
    }
    return f"Job {job_id} completed"

# Execute the job
result = hello_world("World")
print(result)
```

## Architecture Overview

Molq follows a clean architectural pattern with three main components:

### 1. **Decorators** (`@submit`, `@cmdline`)
High-level interfaces that wrap your functions to enable job submission and execution.

### 2. **Submitters** (`LocalSubmitor`, `SlurmSubmitor`)
Backend-specific implementations that handle the actual job submission and monitoring.

### 3. **Base Classes** (`YieldDecorator`, `BaseSubmitor`)
Abstract foundations that enable extensibility and consistent behavior across implementations.

## Use Cases

### Data Processing
Process large datasets with parallel execution and resource management:

```python
@cluster
def process_dataset(input_file: str, output_file: str):
    job_id = yield {
        'cmd': ['python', 'process.py', input_file, output_file],
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00'
    }
    return job_id
```

### Machine Learning
Train models on compute clusters with proper resource allocation:

```python
@gpu_cluster
def train_model(config_file: str):
    job_id = yield {
        'cmd': ['python', 'train.py', '--config', config_file],
        'gpus': 2,
        'memory': '64GB',
        'time': '24:00:00'
    }
    return job_id
```

### Scientific Computing
Execute complex simulations with dependency management:

```python
@hpc_cluster
def run_simulation(parameters: dict):
    job_id = yield {
        'cmd': ['mpirun', '-n', '64', './simulation', '--params', str(parameters)],
        'nodes': 4,
        'ntasks_per_node': 16,
        'time': '12:00:00'
    }
    return job_id
```

## Why Choose Molq?

### Simple and Intuitive
```python
# Before: Complex job submission scripts
subprocess.run(['sbatch', 'job_script.sh'])
# Manual monitoring and error handling required

# After: Clean, declarative syntax
@cluster
def my_job():
    yield {'cmd': ['python', 'script.py'], 'cpus': 4}
```

### Flexible Resource Management
```python
# Automatically handles resource allocation
@cluster
def resource_intensive_job():
    yield {
        'cmd': ['python', 'heavy_computation.py'],
        'cpus': 16,
        'memory': '128GB',
        'time': '04:00:00',
        'partition': 'high-mem'
    }
```

### Built-in Error Handling
```python
# Automatic retry and error reporting
@cluster
def robust_job():
    try:
        yield {'cmd': ['python', 'might_fail.py']}
    except JobFailedException as e:
        # Handle errors gracefully
        yield {'cmd': ['python', 'cleanup.py']}
```

## Getting Started

Ready to dive in? Here's how to get started:

1. **[Installation & Setup](tutorial/getting-started.md)** - Get Molq installed and configured
2. **[Core Concepts](tutorial/core-concepts.md)** - Understand the key concepts and patterns
3. **[API Reference](api/index.md)** - Detailed documentation of all classes and methods
4. **[Recipes](recipes/machine-learning.md)** - Real-world examples and best practices

## Community and Support

- **GitHub Repository**: [molcrafts/molq](https://github.com/molcrafts/molq)
- **Issue Tracker**: Report bugs and request features
- **Discussions**: Ask questions and share use cases

---

*Molq makes job submission simple, reliable, and scalable. Start building better computational workflows today.*
