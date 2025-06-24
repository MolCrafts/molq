# Submit Decorators

The submit decorator system is the primary interface for job submission in Molq. This module provides the `submit` class and related functionality for creating job submitters.

## submit

::: molq.submit.submit

The `submit` class is a decorator that transforms generator functions into job submitters. It acts as a factory for creating submitter instances bound to specific clusters.

### Basic Usage

```python
from molq import submit

# Create a local submitter
local = submit('my_cluster', 'local')

@local
def my_job():
    job_id = yield {
        'cmd': ['echo', 'Hello World'],
        'job_name': 'greeting'
    }
    return job_id

# Execute the job
result = my_job()
print(f"Job ID: {result}")
```

### Submitter Types

#### Local Submitter

```python
local = submit('development', 'local')

@local
def local_computation():
    yield {
        'cmd': ['python', 'compute.py'],
        'cwd': '/tmp/workspace',
        'timeout': 3600  # 1 hour timeout
    }
```

#### SLURM Submitter

```python
cluster = submit('hpc_cluster', 'slurm')

@cluster
def cluster_job():
    yield {
        'cmd': ['python', 'large_computation.py'],
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00',
        'partition': 'compute'
    }
```

### Advanced Patterns

#### Multi-Step Workflows

```python
@cluster
def complex_workflow():
    # Step 1: Data preprocessing
    prep_id = yield {
        'cmd': ['python', 'preprocess.py'],
        'job_name': 'preprocessing',
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00'
    }

    # Step 2: Main computation (depends on preprocessing)
    compute_id = yield {
        'cmd': ['python', 'compute.py', '--input', 'preprocessed_data.pkl'],
        'job_name': 'computation',
        'cpus': 32,
        'memory': '128GB',
        'time': '12:00:00',
        'dependency': prep_id
    }

    # Step 3: Post-processing
    post_id = yield {
        'cmd': ['python', 'postprocess.py', '--input', 'computation_results.h5'],
        'job_name': 'postprocessing',
        'cpus': 4,
        'memory': '16GB',
        'time': '01:00:00',
        'dependency': compute_id
    }

    return [prep_id, compute_id, post_id]
```

#### Conditional Execution

```python
@cluster
def conditional_job(use_gpu: bool, dataset_size: str):
    if use_gpu:
        job_config = {
            'cmd': ['python', 'gpu_training.py', '--dataset', dataset_size],
            'job_name': 'gpu_training',
            'cpus': 8,
            'memory': '64GB',
            'gpus': 2,
            'time': '08:00:00',
            'partition': 'gpu'
        }
    else:
        # Scale CPU resources for larger datasets
        cpus = 32 if dataset_size == 'large' else 16
        memory = '128GB' if dataset_size == 'large' else '64GB'

        job_config = {
            'cmd': ['python', 'cpu_training.py', '--dataset', dataset_size],
            'job_name': 'cpu_training',
            'cpus': cpus,
            'memory': memory,
            'time': '24:00:00',
            'partition': 'compute'
        }

    job_id = yield job_config
    return job_id
```

#### Error Handling and Retry Logic

```python
@cluster
def robust_job(max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            job_id = yield {
                'cmd': ['python', 'unreliable_process.py'],
                'job_name': f'robust_job_attempt_{attempt + 1}',
                'cpus': 8,
                'memory': '32GB',
                'time': '04:00:00'
            }
            return job_id  # Success on first working attempt

        except Exception as e:
            if attempt == max_retries - 1:
                # Final attempt failed, give up
                raise Exception(f"Job failed after {max_retries} attempts: {e}")

            # Log the failure and continue to next attempt
            print(f"Attempt {attempt + 1} failed: {e}")
            print("Retrying with exponential backoff...")

            # Optional: implement backoff delay
            import time
            time.sleep(2 ** attempt)
```

### Submitter Registry

The `submit` class maintains a global registry of submitters to avoid creating duplicate instances:

```python
# These create the same submitter instance
submitter1 = submit('my_cluster', 'slurm')
submitter2 = submit('my_cluster', 'slurm')

print(submitter1 is submitter2)  # True - same instance

# Access the registry directly
from molq.submit import submit
print(submit.CLUSTERS.keys())  # Shows all registered cluster names
```

## get_submitor

::: molq.submit.get_submitor

Factory function that creates appropriate submitter instances based on cluster type.

### Supported Cluster Types

| Type | Description | Class |
|------|-------------|-------|
| `'local'` | Local machine execution | `LocalSubmitor` |
| `'slurm'` | SLURM workload manager | `SlurmSubmitor` |

### Usage

```python
from molq.submit import get_submitor

# Create submitters directly
local_submitter = get_submitor('dev_cluster', 'local')
slurm_submitter = get_submitor('hpc_cluster', 'slurm')

# Use with custom configuration
custom_submitter = get_submitor('custom_cluster', 'slurm')
```

### Extending with Custom Submitters

```python
from molq.submit import get_submitor
from molq.submitor.base import BaseSubmitor

class CustomSubmitor(BaseSubmitor):
    def submit_job(self, config: dict) -> int:
        # Custom implementation
        pass

    def get_job_status(self, job_id: int):
        # Custom implementation
        pass

# Monkey patch to add support for custom type
original_get_submitor = get_submitor

def extended_get_submitor(cluster_name: str, cluster_type: str):
    if cluster_type == 'custom':
        return CustomSubmitor(cluster_name)
    return original_get_submitor(cluster_name, cluster_type)

# Replace the function
import molq.submit
molq.submit.get_submitor = extended_get_submitor
```

## Job Configuration Reference

### Universal Configuration Options

These options work with all submitter types:

```python
{
    'cmd': ['python', 'script.py'],           # Command to execute (required)
    'job_name': 'my_analysis',               # Human-readable job name
    'cwd': '/path/to/working/directory',     # Working directory
    'block': True,                           # Wait for job completion
    'env': {'VAR': 'value'},                 # Environment variables
    'timeout': 3600,                         # Timeout in seconds
    'dependency': [job_id1, job_id2]         # Job dependencies
}
```

### SLURM-Specific Options

Additional options available when using SLURM submitters:

```python
{
    # Resource allocation
    'cpus': 16,                    # Number of CPU cores
    'memory': '64GB',              # Memory requirement
    'time': '24:00:00',            # Time limit (HH:MM:SS format)
    'nodes': 2,                    # Number of nodes
    'ntasks_per_node': 8,          # Tasks per node
    'gpus': 4,                     # Number of GPUs

    # Queue and partition management
    'partition': 'compute',        # SLURM partition name
    'qos': 'normal',              # Quality of service
    'account': 'project123',       # Billing account

    # Advanced scheduling
    'constraint': 'haswell',       # Node feature constraints
    'exclusive': True,             # Request exclusive node access
    'mail_type': 'END,FAIL',      # Email notification types
    'mail_user': 'user@email.com', # Email address for notifications

    # Advanced options
    'begin': 'now+1hour',         # Delayed start time
    'reservation': 'my_reservation', # Use specific reservation
    'array': '1-10',              # Job array specification
}
```

### Local-Specific Options

Options specific to local execution:

```python
{
    'capture_output': True,        # Capture stdout/stderr
    'shell': False,               # Use shell for command execution
    'check': True,                # Raise exception on non-zero exit
    'text': True,                 # Return output as text (not bytes)
    'encoding': 'utf-8',          # Text encoding for output
    'errors': 'strict'            # Error handling for text decoding
}
```

## Examples

### Data Processing Pipeline

```python
@cluster
def data_processing_pipeline(input_file: str, output_dir: str):
    """Complete data processing pipeline with error handling."""

    # Validate input
    validation_id = yield {
        'cmd': ['python', 'validate_input.py', input_file],
        'job_name': 'input_validation',
        'cpus': 2,
        'memory': '8GB',
        'time': '00:30:00'
    }

    # Data cleaning
    cleaning_id = yield {
        'cmd': ['python', 'clean_data.py', input_file, f'{output_dir}/cleaned.csv'],
        'job_name': 'data_cleaning',
        'cpus': 8,
        'memory': '32GB',
        'time': '02:00:00',
        'dependency': validation_id
    }

    # Feature extraction
    features_id = yield {
        'cmd': ['python', 'extract_features.py',
               f'{output_dir}/cleaned.csv',
               f'{output_dir}/features.pkl'],
        'job_name': 'feature_extraction',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00',
        'dependency': cleaning_id
    }

    # Model training
    training_id = yield {
        'cmd': ['python', 'train_model.py',
               f'{output_dir}/features.pkl',
               f'{output_dir}/model.pkl'],
        'job_name': 'model_training',
        'cpus': 32,
        'memory': '128GB',
        'time': '12:00:00',
        'gpus': 2,
        'partition': 'gpu',
        'dependency': features_id
    }

    # Model evaluation
    evaluation_id = yield {
        'cmd': ['python', 'evaluate_model.py',
               f'{output_dir}/model.pkl',
               f'{output_dir}/evaluation_results.json'],
        'job_name': 'model_evaluation',
        'cpus': 8,
        'memory': '32GB',
        'time': '01:00:00',
        'dependency': training_id
    }

    return {
        'validation': validation_id,
        'cleaning': cleaning_id,
        'features': features_id,
        'training': training_id,
        'evaluation': evaluation_id
    }

# Usage
results = data_processing_pipeline('raw_data.csv', '/scratch/analysis_output')
print(f"Pipeline completed with job IDs: {results}")
```

### Parallel Processing

```python
@cluster
def parallel_analysis(file_list: list, analysis_type: str):
    """Process multiple files in parallel."""

    job_ids = []

    for i, file_path in enumerate(file_list):
        # Estimate resources based on file size
        import os
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

        # Scale resources with file size
        cpus = min(16, max(4, int(file_size_mb / 100)))
        memory_gb = min(64, max(8, int(file_size_mb / 50)))

        job_id = yield {
            'cmd': ['python', 'analyze_file.py',
                   file_path,
                   '--type', analysis_type,
                   '--output', f'results_{i}.json'],
            'job_name': f'analyze_file_{i}',
            'cpus': cpus,
            'memory': f'{memory_gb}GB',
            'time': '04:00:00'
        }

        job_ids.append(job_id)

    # Aggregate results
    aggregate_id = yield {
        'cmd': ['python', 'aggregate_results.py',
               '--input-pattern', 'results_*.json',
               '--output', 'final_results.json'],
        'job_name': 'aggregate_results',
        'cpus': 8,
        'memory': '32GB',
        'time': '01:00:00',
        'dependency': job_ids  # Wait for all analysis jobs
    }

    return {
        'analysis_jobs': job_ids,
        'aggregation': aggregate_id
    }

# Usage
files_to_analyze = ['data1.csv', 'data2.csv', 'data3.csv']
results = parallel_analysis(files_to_analyze, 'statistical')
```

### Adaptive Resource Allocation

```python
@cluster
def adaptive_computation(problem_complexity: float, accuracy_level: str):
    """Dynamically allocate resources based on problem characteristics."""

    # Determine resource requirements
    if accuracy_level == 'high':
        base_cpus = 32
        base_memory = 128
        base_time = 24
    elif accuracy_level == 'medium':
        base_cpus = 16
        base_memory = 64
        base_time = 12
    else:  # low accuracy
        base_cpus = 8
        base_memory = 32
        base_time = 6

    # Scale with problem complexity
    cpus = int(base_cpus * problem_complexity)
    memory = int(base_memory * problem_complexity)
    time_hours = int(base_time * problem_complexity)

    # Ensure reasonable bounds
    cpus = max(4, min(128, cpus))
    memory = max(16, min(512, memory))
    time_hours = max(1, min(72, time_hours))

    job_id = yield {
        'cmd': ['python', 'adaptive_solver.py',
               '--complexity', str(problem_complexity),
               '--accuracy', accuracy_level],
        'job_name': f'adaptive_solve_{accuracy_level}_{problem_complexity}',
        'cpus': cpus,
        'memory': f'{memory}GB',
        'time': f'{time_hours:02d}:00:00'
    }

    return job_id

# Usage
# Simple problem with high accuracy
job1 = adaptive_computation(0.5, 'high')

# Complex problem with medium accuracy
job2 = adaptive_computation(2.0, 'medium')
```

## Best Practices

### 1. Resource Estimation

```python
# Good: Conservative resource estimation
@cluster
def well_estimated_job():
    yield {
        'cmd': ['python', 'analysis.py'],
        'cpus': 8,           # Match script parallelism
        'memory': '32GB',    # 2x expected peak usage
        'time': '04:00:00'   # 1.5-2x expected runtime
    }

# Avoid: Over-requesting resources
@cluster
def resource_waste():
    yield {
        'cmd': ['simple_script.py'],
        'cpus': 128,         # Too many for simple task
        'memory': '1TB',     # Excessive
        'time': '72:00:00'   # Much longer than needed
    }
```

### 2. Job Naming Conventions

```python
@cluster
def well_named_jobs(experiment_id: str, dataset: str):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    yield {
        'cmd': ['python', 'preprocess.py', dataset],
        'job_name': f'{experiment_id}_preprocess_{dataset}_{timestamp}',
        'cpus': 4
    }
```

### 3. Dependency Management

```python
@cluster
def dependency_example():
    # Stage 1: Independent jobs
    prep_jobs = []
    for i in range(4):
        job_id = yield {
            'cmd': ['prepare_chunk.py', str(i)],
            'job_name': f'prep_chunk_{i}',
            'cpus': 4
        }
        prep_jobs.append(job_id)

    # Stage 2: Job that depends on all prep jobs
    merge_id = yield {
        'cmd': ['merge_chunks.py'],
        'job_name': 'merge_all_chunks',
        'dependency': prep_jobs,  # List of dependencies
        'cpus': 8
    }

    return merge_id
```
