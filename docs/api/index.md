# API Reference

Complete API documentation for Molq.

## Core Functions

### `submit(cluster_name, backend)`

Creates a job submission decorator.

**Parameters:**
- `cluster_name` (str): Name identifier for the cluster
- `backend` (str): Backend type (`'local'` or `'slurm'`)

**Returns:** Decorator function

```python
from molq import submit

local = submit('dev', 'local')
cluster = submit('hpc', 'slurm')
```

### `@cmdline`

Decorator for direct command execution with result capture.

```python
from molq import cmdline

@cmdline
def get_files():
    result = yield {'cmd': ['ls', '-la']}
    return result.stdout.decode()
```

## Job Configuration

### Required Fields
- `cmd` (list): Command and arguments to execute

### Optional Fields
- `job_name` (str): Human-readable job name
- `cpus` (int): Number of CPU cores (SLURM only)
- `memory` (str): Memory requirement like '16GB' (SLURM only)
- `time` (str): Time limit in 'HH:MM:SS' format (SLURM only)
- `gpus` (int): Number of GPUs (SLURM only)
- `dependency` (str): Job ID to wait for

## Submitter Classes

### LocalSubmitor
Executes jobs on the local machine using subprocess.

### SlurmSubmitor
Submits jobs to SLURM cluster using `sbatch`.

## Job Status

Jobs return status objects with these attributes:
- `job_id`: Unique job identifier
- `status`: Current state ('pending', 'running', 'completed', 'failed')
- `exit_code`: Process exit code (when available)

For detailed examples, see the [Tutorial](../tutorial/getting-started.md).
{
    'cmd': ['python', 'script.py'],           # Command to execute (required)
    'job_name': 'my_job',                     # Human-readable name
    'cwd': '/path/to/workdir',                # Working directory
    'block': True,                            # Wait for completion
    'env': {'VAR': 'value'},                  # Environment variables
    'timeout': 3600                           # Timeout in seconds
}
```

#### SLURM-Specific Configuration

```python
{
    # Resource allocation
    'cpus': 16,                    # CPU cores
    'memory': '64GB',              # Memory requirement
    'time': '24:00:00',            # Time limit (HH:MM:SS)
    'nodes': 2,                    # Number of nodes
    'ntasks_per_node': 8,          # Tasks per node
    'gpus': 4,                     # GPU count

    # Queue management
    'partition': 'compute',        # SLURM partition
    'qos': 'normal',              # Quality of service
    'account': 'project123',       # Billing account

    # Dependencies and constraints
    'dependency': [job_id1, job_id2],  # Job dependencies
    'constraint': 'haswell',           # Node constraints
    'exclusive': True                  # Exclusive node access
}
```

## Error Handling

Molq provides several exception types for different error conditions:

```python
from molq.exceptions import (
    MolqError,           # Base exception
    JobSubmissionError,  # Job submission failed
    JobExecutionError,   # Job execution failed
    JobTimeoutError,     # Job exceeded time limit
    ResourceError        # Insufficient resources
)

@cluster
def error_handling_example():
    try:
        job_id = yield {'cmd': ['might_fail.py']}
        return job_id
    except JobExecutionError as e:
        print(f"Job execution failed: {e}")
        # Handle error or retry
    except ResourceError as e:
        print(f"Insufficient resources: {e}")
        # Scale down resource requirements
```

## Job Status and Monitoring

### Job Status Enumeration

```python
from molq.submitor.base import JobStatus

# Job status values
JobStatus.Status.PENDING     # Queued, waiting to run
JobStatus.Status.RUNNING     # Currently executing
JobStatus.Status.COMPLETED   # Finished successfully
JobStatus.Status.FAILED      # Terminated with error
JobStatus.Status.FINISHED    # Final state (completed or failed)
```

### Monitoring Jobs

```python
@cluster
def monitor_job():
    job_id = yield {
        'cmd': ['long_running_script.py'],
        'block': False  # Don't wait for completion
    }

    # Get submitter instance to check status
    submitter = cluster.CLUSTERS['cluster_name']

    while True:
        status = submitter.get_job_status(job_id)
        print(f"Job {job_id}: {status.status}")

        if status.is_finish:
            break

        time.sleep(30)  # Check every 30 seconds

    return job_id
```

## Advanced Usage Patterns

### Generator-Based Workflows

```python
@cluster
def complex_workflow():
    # Multi-step workflow with error handling
    try:
        # Step 1: Data preparation
        prep_id = yield {
            'cmd': ['prepare_data.py'],
            'job_name': 'data_prep',
            'cpus': 4,
            'memory': '16GB'
        }

        # Step 2: Analysis (depends on prep)
        analysis_id = yield {
            'cmd': ['analyze.py', '--input', 'prepared_data.csv'],
            'job_name': 'analysis',
            'cpus': 16,
            'memory': '64GB',
            'dependency': prep_id
        }

        return [prep_id, analysis_id]

    except Exception as e:
        # Cleanup on error
        cleanup_id = yield {
            'cmd': ['cleanup.py'],
            'job_name': 'cleanup'
        }
        raise
```

### Resource Scaling

```python
@cluster
def adaptive_job(problem_size: int):
    # Scale resources based on problem size
    if problem_size < 1000:
        resources = {'cpus': 4, 'memory': '16GB', 'time': '01:00:00'}
    elif problem_size < 10000:
        resources = {'cpus': 16, 'memory': '64GB', 'time': '04:00:00'}
    else:
        resources = {'cpus': 32, 'memory': '128GB', 'time': '12:00:00'}

    job_config = {
        'cmd': ['process.py', '--size', str(problem_size)],
        'job_name': f'adaptive_job_{problem_size}',
        **resources
    }

    job_id = yield job_config
    return job_id
```

## Configuration Management

### Environment Variables

```python
# Set environment for all jobs on a submitter
@cluster
def job_with_env():
    yield {
        'cmd': ['python', 'script.py'],
        'env': {
            'PYTHONPATH': '/custom/path',
            'CUDA_VISIBLE_DEVICES': '0,1',
            'OMP_NUM_THREADS': '8'
        }
    }
```

### Working Directory Management

```python
@cluster
def job_with_workdir():
    yield {
        'cmd': ['./run_analysis.sh'],
        'cwd': '/scratch/project/analysis',  # Set working directory
        'job_name': 'analysis_job'
    }
```

## Best Practices

### 1. Resource Estimation

```python
# Good: Conservative resource estimation
@cluster
def well_sized_job():
    yield {
        'cmd': ['python', 'analysis.py'],
        'cpus': 8,           # Match script's parallelism
        'memory': '32GB',    # 2x expected peak usage
        'time': '04:00:00'   # 2x expected runtime
    }
```

### 2. Error Recovery

```python
@cluster
def robust_job():
    max_retries = 3
    for attempt in range(max_retries):
        try:
            job_id = yield {
                'cmd': ['unreliable_process.py'],
                'job_name': f'robust_job_attempt_{attempt + 1}'
            }
            return job_id
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"Attempt {attempt + 1} failed, retrying...")
```

### 3. Job Naming

```python
@cluster
def well_named_job(dataset: str, algorithm: str):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    yield {
        'cmd': ['python', 'train.py', '--data', dataset, '--algo', algorithm],
        'job_name': f'{algorithm}_{dataset}_{timestamp}',  # Descriptive name
        'cpus': 16,
        'memory': '64GB'
    }
```

### 4. Dependency Management

```python
@cluster
def pipeline_with_dependencies():
    # Stage 1: Independent preprocessing jobs
    prep_jobs = []
    for i in range(4):
        job_id = yield {
            'cmd': ['preprocess.py', f'--chunk={i}'],
            'job_name': f'preprocess_chunk_{i}',
            'cpus': 4
        }
        prep_jobs.append(job_id)

    # Stage 2: Merge (depends on all preprocessing)
    merge_id = yield {
        'cmd': ['merge_chunks.py'],
        'job_name': 'merge_chunks',
        'dependency': prep_jobs,  # Wait for all prep jobs
        'cpus': 8
    }

    return merge_id
```

## Migration Guide

### From Shell Scripts

```bash
# Before: Shell script job submission
sbatch --cpus-per-task=16 --mem=64G --time=04:00:00 job_script.sh
```

```python
# After: Molq decorator
@cluster
def migrated_job():
    yield {
        'cmd': ['bash', 'job_script.sh'],
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00'
    }
```

### From Direct subprocess

```python
# Before: Direct subprocess calls
import subprocess
result = subprocess.run(['python', 'analysis.py'], capture_output=True)
```

```python
# After: Molq cmdline decorator
@cmdline
def migrated_subprocess():
    result = yield {'cmd': ['python', 'analysis.py'], 'block': True}
    return result
```

## Troubleshooting

### Common Issues

**1. Command Not Found**
```python
# Problem: Command not in PATH
yield {'cmd': ['my_script.py']}  # Fails if not executable

# Solution: Use full path or python interpreter
yield {'cmd': ['python', '/full/path/to/my_script.py']}
```

**2. Permission Denied**
```python
# Problem: Script not executable
yield {'cmd': ['./script.sh']}  # May fail with permission error

# Solution: Make executable or use interpreter
yield {'cmd': ['bash', 'script.sh']}
```

**3. Import Errors**
```python
# Problem: Python modules not found
yield {'cmd': ['python', 'script.py']}  # Module import fails

# Solution: Set PYTHONPATH
yield {
    'cmd': ['python', 'script.py'],
    'env': {'PYTHONPATH': '/path/to/modules'}
}
```

**4. Resource Allocation Failures**
```python
# Problem: Requesting too many resources
yield {
    'cmd': ['simple_task.py'],
    'cpus': 128,      # Excessive for simple task
    'memory': '1TB'   # More than available
}

# Solution: Right-size resources
yield {
    'cmd': ['simple_task.py'],
    'cpus': 2,        # Appropriate for task
    'memory': '4GB'   # Reasonable requirement
}
```

For detailed examples, see the [Tutorial](../tutorial/getting-started.md).
