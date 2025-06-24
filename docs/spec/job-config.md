# Job Configuration Specification

Complete specification for job configuration options.

## Universal Options

These work with all backends (local, SLURM):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cmd` | list or str | **Yes** | Command to execute |
| `job_name` | str | No | Human-readable name |
| `cwd` | str | No | Working directory |
| `env` | dict | No | Environment variables |
| `dependency` | str/list | No | Wait for job ID(s) |

## SLURM-Specific Options

These only work with SLURM backend:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `cpus` | int | CPU cores | `16` |
| `memory` | str | Memory requirement | `'64GB'` |
| `time` | str | Time limit | `'12:00:00'` |
| `gpus` | int | Number of GPUs | `2` |
| `nodes` | int | Number of nodes | `4` |
| `partition` | str | Queue name | `'gpu'` |
| `account` | str | Billing account | `'project123'` |

## Examples

### Basic Job
```python
yield {
    'cmd': ['python', 'script.py'],
    'job_name': 'my_analysis'
}
```

### SLURM Job with Resources
```python
yield {
    'cmd': ['python', 'big_job.py'],
    'cpus': 32,
    'memory': '128GB',
    'time': '24:00:00',
    'gpus': 4,
    'partition': 'gpu'
}
```

### Job with Dependencies
```python
# First job
job1 = yield {'cmd': ['python', 'step1.py']}

# Second job waits for first
job2 = yield {
    'cmd': ['python', 'step2.py'],
    'dependency': job1
}
```

### Environment Variables
```python
yield {
    'cmd': ['python', 'script.py'],
    'env': {
        'CUDA_VISIBLE_DEVICES': '0,1',
        'OMP_NUM_THREADS': '16'
    }
}
```

## Command Formats

**List format (recommended):**
```python
'cmd': ['python', 'script.py', '--arg', 'value']
```

**String format:**
```python
'cmd': 'python script.py --arg value'
```

Use list format to avoid shell injection issues.
{
    'cmd': ['python', 'gpu_script.py'],
    'env': {
        'CUDA_VISIBLE_DEVICES': '0,1',
        'PYTHONPATH': '/custom/modules',
        'OMP_NUM_THREADS': '8'
    }
}
```

#### Job Dependencies

Jobs can depend on the completion of other jobs:

```python
# Single dependency
{
    'cmd': ['python', 'analysis.py'],
    'dependency': prep_job_id
}

# Multiple dependencies
{
    'cmd': ['python', 'merge.py'],
    'dependency': [job1_id, job2_id, job3_id]
}
```

### Working Directory

The working directory affects where the job executes and how relative paths are resolved:

```python
{
    'cmd': ['./run_analysis.sh'],
    'cwd': '/scratch/project/analysis'  # Job runs in this directory
}
```

## SLURM-Specific Configuration

When using SLURM submitters, additional options are available for resource allocation and job scheduling.

### Resource Allocation

#### CPU Resources

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `cpus` | `int` | Number of CPU cores | `16` |
| `cpus_per_task` | `int` | CPUs per task (for multi-task jobs) | `4` |
| `ntasks` | `int` | Number of tasks | `32` |
| `ntasks_per_node` | `int` | Tasks per node | `16` |

```python
{
    'cmd': ['mpirun', 'parallel_app'],
    'nodes': 4,
    'ntasks_per_node': 16,  # 64 total tasks
    'cpus_per_task': 2      # 128 total CPUs
}
```

#### Memory Resources

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `memory` | `str` | Total memory per node | `'64GB'`, `'128000MB'` |
| `mem_per_cpu` | `str` | Memory per CPU core | `'4GB'` |

```python
# Total memory for the job
{
    'cmd': ['memory_intensive_app'],
    'memory': '256GB'
}

# Memory per CPU core
{
    'cmd': ['parallel_app'],
    'cpus': 32,
    'mem_per_cpu': '2GB'  # 64GB total
}
```

#### Time Limits

Time limits are specified in `HH:MM:SS` format:

```python
{
    'cmd': ['long_simulation'],
    'time': '24:00:00'      # 24 hours
}

{
    'cmd': ['quick_analysis'],
    'time': '00:30:00'      # 30 minutes
}
```

#### GPU Resources

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `gpus` | `int` | Number of GPUs | `2` |
| `gpus_per_node` | `int` | GPUs per node | `4` |
| `gpus_per_task` | `int` | GPUs per task | `1` |
| `gpu_type` | `str` | Specific GPU type | `'V100'`, `'A100'` |

```python
{
    'cmd': ['python', 'gpu_training.py'],
    'gpus': 4,
    'gpu_type': 'A100',
    'cpus': 16,
    'memory': '128GB'
}
```

### Queue Management

#### Partitions and QoS

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `partition` | `str` | SLURM partition name | `'compute'`, `'gpu'`, `'debug'` |
| `qos` | `str` | Quality of Service | `'normal'`, `'high'`, `'low'` |
| `account` | `str` | Billing account | `'project123'` |

```python
{
    'cmd': ['research_app'],
    'partition': 'gpu',
    'qos': 'high',
    'account': 'research_project_2024'
}
```

### Advanced Scheduling

#### Node Constraints

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `constraint` | `str` | Node feature requirements | `'haswell'`, `'infiniband'` |
| `exclude` | `str` | Nodes to exclude | `'node001,node002'` |
| `nodelist` | `str` | Specific nodes to use | `'node[003-006]'` |

```python
{
    'cmd': ['network_intensive_app'],
    'constraint': 'infiniband',  # Require InfiniBand
    'exclude': 'node001'         # Avoid problematic node
}
```

#### Exclusive Access

```python
{
    'cmd': ['benchmark_app'],
    'exclusive': True,  # Get exclusive access to nodes
    'nodes': 2
}
```

#### Job Arrays

For running many similar jobs:

```python
{
    'cmd': ['python', 'process_file.py', '--index', '$SLURM_ARRAY_TASK_ID'],
    'array': '1-100',           # Array indices 1 to 100
    'cpus': 4,
    'memory': '16GB'
}
```

### Notifications

#### Email Notifications

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `mail_type` | `str` | When to send email | `'END'`, `'FAIL'`, `'BEGIN,END,FAIL'` |
| `mail_user` | `str` | Email address | `'user@institution.edu'` |

```python
{
    'cmd': ['long_simulation'],
    'time': '48:00:00',
    'mail_type': 'END,FAIL',
    'mail_user': 'researcher@university.edu'
}
```

### Advanced Features

#### Delayed Start

```python
{
    'cmd': ['scheduled_job'],
    'begin': 'now+2hours'       # Start 2 hours from now
}

{
    'cmd': ['nightly_job'],
    'begin': '2024-01-15T02:00:00'  # Start at specific time
}
```

#### Reservations

```python
{
    'cmd': ['reserved_computation'],
    'reservation': 'workshop_reservation'
}
```

## Local-Specific Configuration

Local submitters support additional options for subprocess execution.

### Execution Control

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `shell` | `bool` | Use shell for execution | `False` |
| `capture_output` | `bool` | Capture stdout/stderr | `True` |
| `check` | `bool` | Raise exception on non-zero exit | `True` |
| `text` | `bool` | Return output as text | `True` |

```python
{
    'cmd': 'ls -la | grep python',
    'shell': True,              # Required for pipes/redirects
    'capture_output': True,
    'text': True
}
```

### Process Management

```python
{
    'cmd': ['long_running_process'],
    'timeout': 3600,            # Kill after 1 hour
    'block': False              # Don't wait for completion
}
```

## Configuration Examples

### Basic Job

```python
{
    'cmd': ['python', 'simple_script.py'],
    'job_name': 'simple_analysis'
}
```

### CPU-Intensive Job

```python
{
    'cmd': ['python', 'cpu_intensive.py'],
    'job_name': 'cpu_analysis',
    'cpus': 32,
    'memory': '64GB',
    'time': '12:00:00',
    'partition': 'compute'
}
```

### GPU Training Job

```python
{
    'cmd': ['python', 'train_model.py', '--epochs', '100'],
    'job_name': 'model_training',
    'cpus': 16,
    'memory': '128GB',
    'gpus': 4,
    'gpu_type': 'A100',
    'time': '24:00:00',
    'partition': 'gpu',
    'env': {
        'CUDA_VISIBLE_DEVICES': '0,1,2,3',
        'NCCL_DEBUG': 'INFO'
    }
}
```

### MPI Parallel Job

```python
{
    'cmd': ['mpirun', '-np', '128', './parallel_simulation'],
    'job_name': 'parallel_simulation',
    'nodes': 8,
    'ntasks_per_node': 16,
    'cpus_per_task': 2,
    'memory': '256GB',
    'time': '48:00:00',
    'constraint': 'infiniband'
}
```

### Job with Dependencies

```python
# First job: data preprocessing
prep_config = {
    'cmd': ['python', 'preprocess.py', 'raw_data.csv'],
    'job_name': 'data_preprocessing',
    'cpus': 8,
    'memory': '32GB',
    'time': '02:00:00'
}

# Second job: analysis (depends on preprocessing)
analysis_config = {
    'cmd': ['python', 'analyze.py', 'preprocessed_data.csv'],
    'job_name': 'data_analysis',
    'cpus': 16,
    'memory': '64GB',
    'time': '08:00:00',
    'dependency': prep_job_id  # Set after submitting prep job
}
```

### Interactive Development Job

```python
{
    'cmd': ['jupyter', 'lab', '--ip=0.0.0.0', '--no-browser'],
    'job_name': 'jupyter_development',
    'cpus': 4,
    'memory': '32GB',
    'time': '08:00:00',
    'partition': 'interactive',
    'env': {
        'JUPYTER_PORT': '8888'
    }
}
```

## Validation Rules

### Required Fields

- `cmd` is the only required field
- All other fields have sensible defaults

### Type Validation

- `cmd` must be a string or list of strings
- `cpus`, `nodes`, `timeout` must be positive integers
- `memory` must be a string with valid units (GB, MB, TB)
- `time` must be in `HH:MM:SS` format
- `block` must be a boolean

### Value Constraints

- CPU count should not exceed node capabilities
- Memory requests should be reasonable for the partition
- Time limits should comply with partition policies
- Dependencies must reference valid job IDs

## Best Practices

### Resource Estimation

```python
# Good: Conservative estimates
{
    'cmd': ['python', 'analysis.py'],
    'cpus': 8,              # Match script parallelism
    'memory': '32GB',       # 1.5-2x expected usage
    'time': '04:00:00'      # 1.5-2x expected runtime
}

# Avoid: Over-allocation
{
    'cmd': ['simple_task.py'],
    'cpus': 128,            # Excessive for simple task
    'memory': '1TB',        # Wasteful
    'time': '72:00:00'      # Much longer than needed
}
```

### Partition Selection

```python
# Match job characteristics to partition
{
    'cmd': ['quick_test.py'],
    'partition': 'debug',    # Short jobs
    'time': '00:15:00'
}

{
    'cmd': ['long_simulation.py'],
    'partition': 'compute',  # Long jobs
    'time': '48:00:00'
}

{
    'cmd': ['gpu_training.py'],
    'partition': 'gpu',      # GPU jobs
    'gpus': 2
}
```

### Environment Management

```python
# Set up clean environment
{
    'cmd': ['conda', 'run', '-n', 'myenv', 'python', 'script.py'],
    'env': {
        'PYTHONUNBUFFERED': '1',    # Immediate output
        'CUDA_CACHE_DISABLE': '1',  # Disable CUDA cache
        'OMP_NUM_THREADS': '8'      # Control OpenMP
    }
}
```

### Error Handling

```python
# Robust job configuration
{
    'cmd': ['python', 'robust_script.py'],
    'job_name': 'robust_analysis_20240115_143022',  # Timestamped name
    'cpus': 16,
    'memory': '64GB',
    'time': '08:00:00',
    'mail_type': 'FAIL',            # Notify on failure
    'mail_user': 'user@email.com'
}
```

This specification provides the foundation for creating effective job configurations in Molq. Always refer to your cluster's documentation for specific partition names, resource limits, and policies.
