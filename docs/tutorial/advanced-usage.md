# Advanced Usage

Advanced patterns for building sophisticated workflows.

## Workflow Orchestration

### Parallel Processing with Dependencies

```python
@cluster
def parallel_pipeline():
    # Stage 1: Independent preprocessing jobs
    prep_jobs = []
    for i in range(4):
        job_id = yield {
            'cmd': ['python', 'preprocess.py', f'--chunk={i}'],
            'cpus': 4
        }
        prep_jobs.append(job_id)

    # Stage 2: Merge results (waits for all preprocessing)
    merge_id = yield {
        'cmd': ['python', 'merge.py'],
        'dependency': prep_jobs,  # List of dependencies
        'cpus': 16
    }

    return merge_id
```

### Dynamic Resource Allocation

```python
@cluster
def adaptive_job(data_size_mb: int):
    # Scale resources based on input size
    cpus = min(32, max(4, data_size_mb // 1000))
    memory_gb = min(128, max(16, data_size_mb // 100))

    yield {
        'cmd': ['python', 'process.py', f'--size={data_size_mb}'],
        'cpus': cpus,
        'memory': f'{memory_gb}GB',
        'time': f'{cpus // 4:02d}:00:00'  # Scale time with CPUs
    }
```

## Error Handling Patterns

### Retry with Exponential Backoff

```python
import time
import random

@cluster
def retry_job(max_attempts: int = 3):
    for attempt in range(max_attempts):
        try:
            return yield {
                'cmd': ['python', 'flaky_service.py'],
                'job_name': f'retry_attempt_{attempt + 1}'
            }
        except Exception as e:
            if attempt == max_attempts - 1:
                raise

            # Exponential backoff with jitter
            delay = (2 ** attempt) + random.uniform(0, 1)
            print(f"Attempt {attempt + 1} failed, retrying in {delay:.1f}s")
            time.sleep(delay)
```

### Circuit Breaker Pattern

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=300):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure = 0
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN

    def call_allowed(self):
        if self.state == 'CLOSED':
            return True
        elif self.state == 'OPEN':
            if time.time() - self.last_failure > self.timeout:
                self.state = 'HALF_OPEN'
                return True
            return False
        return True  # HALF_OPEN

    def on_success(self):
        self.failure_count = 0
        self.state = 'CLOSED'

    def on_failure(self):
        self.failure_count += 1
        self.last_failure = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

breaker = CircuitBreaker()

@cluster
def protected_job():
    if not breaker.call_allowed():
        raise Exception("Circuit breaker is OPEN")

    try:
        job_id = yield {'cmd': ['python', 'unreliable_service.py']}
        breaker.on_success()
        return job_id
    except Exception:
        breaker.on_failure()
        raise
```

## Performance Optimization

### Job Batching

```python
@cluster
def batch_processor(items: list, batch_size: int = 50):
    """Process items in batches to reduce overhead."""

    job_ids = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]

        job_id = yield {
            'cmd': ['python', 'batch_process.py'] + batch,
            'job_name': f'batch_{i // batch_size}',
            'cpus': 8
        }
        job_ids.append(job_id)

    return job_ids
```

### Memory-Efficient Processing

```python
@cluster
def memory_efficient_workflow(large_file: str):
    # First: analyze file to determine optimal processing strategy
    analysis_id = yield {
        'cmd': ['python', 'analyze_file.py', large_file],
        'cpus': 2,
        'memory': '8GB'
    }

    # Then: process in chunks based on analysis
    # (In real implementation, you'd read the analysis results)
    chunk_jobs = []
    for chunk in range(10):  # Determined from analysis
        job_id = yield {
            'cmd': ['python', 'process_chunk.py', large_file, str(chunk)],
            'cpus': 4,
            'memory': '16GB',  # Much smaller than processing whole file
            'dependency': analysis_id
        }
        chunk_jobs.append(job_id)

    # Finally: merge results
    merge_id = yield {
        'cmd': ['python', 'merge_chunks.py'],
        'dependency': chunk_jobs,
        'cpus': 8,
        'memory': '32GB'
    }

    return merge_id
```

## Monitoring and Logging

### Progress Tracking

```python
import json
from datetime import datetime

@cluster
def monitored_workflow(experiment_name: str):
    start_time = datetime.now()

    try:
        # Stage 1
        prep_id = yield {
            'cmd': ['python', 'prepare.py', '--experiment', experiment_name],
            'job_name': f'{experiment_name}_prep'
        }

        # Stage 2
        train_id = yield {
            'cmd': ['python', 'train.py', '--experiment', experiment_name],
            'job_name': f'{experiment_name}_train',
            'dependency': prep_id
        }

        duration = datetime.now() - start_time

        return {
            'experiment': experiment_name,
            'jobs': [prep_id, train_id],
            'duration_seconds': duration.total_seconds(),
            'status': 'success'
        }

    except Exception as e:
        # Log failure and cleanup
        cleanup_id = yield {
            'cmd': ['python', 'cleanup.py', '--experiment', experiment_name],
            'job_name': f'{experiment_name}_cleanup'
        }

        return {
            'experiment': experiment_name,
            'status': 'failed',
            'error': str(e),
            'cleanup_job': cleanup_id
        }
```

## Custom Submitters

### Extending Molq for Custom Backends

```python
from molq.submitor.base import BaseSubmitor, JobStatus
import subprocess

class DockerSubmitor(BaseSubmitor):
    """Custom submitter for Docker containers."""

    def submit_job(self, config: dict) -> int:
        # Extract Docker-specific options
        image = config.get('docker_image', 'python:3.9')
        volumes = config.get('volumes', {})

        # Build Docker command
        docker_cmd = ['docker', 'run', '--rm']

        for host_path, container_path in volumes.items():
            docker_cmd.extend(['-v', f'{host_path}:{container_path}'])

        docker_cmd.append(image)
        docker_cmd.extend(config['cmd'])

        # Execute
        if config.get('block', True):
            result = subprocess.run(docker_cmd)
            job_id = hash(str(docker_cmd))
            status = JobStatus.Status.COMPLETED if result.returncode == 0 else JobStatus.Status.FAILED
            self.GLOBAL_JOB_POOL[job_id] = JobStatus(job_id, status)
            return job_id
        else:
            proc = subprocess.Popen(docker_cmd)
            self.GLOBAL_JOB_POOL[proc.pid] = JobStatus(proc.pid, JobStatus.Status.RUNNING)
            return proc.pid

# Register custom submitter
def custom_get_submitor(cluster_name: str, cluster_type: str):
    if cluster_type == 'docker':
        return DockerSubmitor(cluster_name)
    # Fall back to default
    from molq.submit import get_submitor
    return get_submitor(cluster_name, cluster_type)

# Replace the default function
import molq.submit
molq.submit.get_submitor = custom_get_submitor

# Usage
docker = submit('containers', 'docker')

@docker
def containerized_job():
    yield {
        'cmd': ['python', 'analysis.py'],
        'docker_image': 'tensorflow/tensorflow:latest',
        'volumes': {'/data': '/container/data'}
    }
```

## Testing Workflows

### Mocking for Tests

```python
import pytest
from unittest.mock import Mock, patch

def test_workflow():
    """Test workflow without actually submitting jobs."""

    mock_submitter = Mock()
    mock_submitter.submit_job.return_value = 12345

    with patch('molq.submit.get_submitor', return_value=mock_submitter):
        result = my_workflow('test_input.csv')

        # Verify jobs were submitted correctly
        assert mock_submitter.submit_job.called
        call_args = mock_submitter.submit_job.call_args[0][0]
        assert 'python' in call_args['cmd']
        assert result == 12345
```

## Best Practices

### 1. Configuration Management

```python
import yaml

# config.yaml
workflow_configs = yaml.safe_load("""
small_job:
  cpus: 4
  memory: 16GB
  time: "02:00:00"
large_job:
  cpus: 32
  memory: 128GB
  time: "12:00:00"
""")

@cluster
def configurable_job(job_type: str):
    config = workflow_configs[job_type].copy()
    config['cmd'] = ['python', 'analysis.py']
    yield config
```

### 2. Resource Right-Sizing

```python
@cluster
def right_sized_job():
    # Good: Match resources to actual needs
    yield {
        'cmd': ['python', 'light_analysis.py'],
        'cpus': 4,        # Matches script parallelism
        'memory': '16GB', # 1.5x expected usage
        'time': '02:00:00' # 2x expected runtime
    }
```

### 3. Graceful Error Handling

```python
@cluster
def robust_pipeline():
    try:
        return yield {'cmd': ['python', 'preferred_method.py']}
    except ResourceError:
        # Fallback to less resource-intensive method
        return yield {'cmd': ['python', 'lightweight_method.py']}
    except Exception:
        # Last resort: simple processing
        return yield {'cmd': ['python', 'basic_method.py']}
```

These patterns help you build production-ready workflows that are resilient, efficient, and maintainable.

## Advanced Error Handling

### Circuit Breaker Pattern

```python
class JobCircuitBreaker:
    def __init__(self, failure_threshold: int = 5, timeout: int = 300):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = 0
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN

    def can_execute(self):
        if self.state == 'CLOSED':
            return True
        elif self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.timeout:
                self.state = 'HALF_OPEN'
                return True
            return False
        else:  # HALF_OPEN
            return True

    def on_success(self):
        self.failure_count = 0
        self.state = 'CLOSED'

    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

circuit_breaker = JobCircuitBreaker()

@cluster
def resilient_job(data_file: str):
    """Job with circuit breaker pattern for fault tolerance."""

    if not circuit_breaker.can_execute():
        raise Exception("Circuit breaker is OPEN - too many recent failures")

    try:
        job_id = yield {
            'cmd': ['python', 'unreliable_process.py', data_file],
            'job_name': 'resilient_processing',
            'cpus': 8,
            'memory': '32GB',
            'time': '02:00:00'
        }
        circuit_breaker.on_success()
        return job_id

    except Exception as e:
        circuit_breaker.on_failure()
        raise
```

### Exponential Backoff with Jitter

```python
import random
import time

@cluster
def job_with_backoff(data: str, max_retries: int = 5):
    """Job with exponential backoff retry strategy."""

    for attempt in range(max_retries):
        try:
            job_id = yield {
                'cmd': ['python', 'flaky_service.py', data],
                'job_name': f'backoff_job_attempt_{attempt + 1}',
                'cpus': 4,
                'memory': '16GB',
                'time': '01:00:00'
            }
            return job_id

        except Exception as e:
            if attempt == max_retries - 1:
                raise  # Final attempt failed

            # Exponential backoff with jitter
            base_delay = 2 ** attempt
            jitter = random.uniform(0, 1)
            delay = base_delay + jitter

            print(f"Attempt {attempt + 1} failed: {e}")
            print(f"Retrying in {delay:.2f} seconds...")
            time.sleep(delay)
```

## Performance Optimization

### Job Batching

```python
@cluster
def batch_processor(items: list, batch_size: int = 100):
    """Process items in batches to optimize resource usage."""

    job_ids = []

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batch_file = f'batch_{i // batch_size}.json'

        # Write batch to file
        with open(batch_file, 'w') as f:
            json.dump(batch, f)

        job_id = yield {
            'cmd': ['python', 'batch_processor.py', batch_file],
            'job_name': f'batch_{i // batch_size}',
            'cpus': 8,
            'memory': '32GB',
            'time': '02:00:00'
        }

        job_ids.append(job_id)

    return job_ids
```

### Memory-Efficient Processing

```python
@cluster
def memory_efficient_job(large_dataset: str):
    """Process large datasets with memory optimization."""

    # First pass: analyze data size and characteristics
    analysis_id = yield {
        'cmd': ['python', 'analyze_dataset.py', large_dataset, '--output', 'analysis.json'],
        'job_name': 'dataset_analysis',
        'cpus': 2,
        'memory': '8GB',
        'time': '00:30:00'
    }

    # Read analysis results to determine optimal chunk size
    with open('analysis.json') as f:
        analysis = json.load(f)

    chunk_size = analysis['optimal_chunk_size']
    num_chunks = analysis['num_chunks']

    # Process in chunks
    chunk_jobs = []
    for chunk_id in range(num_chunks):
        job_id = yield {
            'cmd': ['python', 'process_chunk.py',
                   large_dataset,
                   '--chunk-id', str(chunk_id),
                   '--chunk-size', str(chunk_size)],
            'job_name': f'process_chunk_{chunk_id}',
            'cpus': 4,
            'memory': '16GB',  # Much smaller memory requirement
            'time': '01:00:00'
        }
        chunk_jobs.append(job_id)

    # Merge results
    merge_id = yield {
        'cmd': ['python', 'merge_results.py', '--chunks', str(num_chunks)],
        'job_name': 'merge_results',
        'cpus': 8,
        'memory': '32GB',
        'time': '00:30:00',
        'dependency': chunk_jobs
    }

    return merge_id
```

### GPU Job Management

```python
@cluster
def gpu_job_scheduler(gpu_tasks: list):
    """Efficiently schedule GPU jobs based on availability."""

    # Query available GPU resources
    gpu_info = yield {
        'cmd': ['nvidia-smi', '--query-gpu=index,memory.free', '--format=csv,noheader,nounits'],
        'job_name': 'gpu_query',
        'cpus': 1,
        'memory': '2GB',
        'time': '00:05:00'
    }

    # Parse GPU availability (this would be more sophisticated in practice)
    available_gpus = parse_gpu_info(gpu_info)

    job_ids = []
    for i, task in enumerate(gpu_tasks):
        # Select appropriate GPU based on memory requirements
        required_memory = task.get('gpu_memory_gb', 8)
        suitable_gpu = find_suitable_gpu(available_gpus, required_memory)

        if suitable_gpu is None:
            # Queue job to wait for GPU availability
            job_id = yield {
                'cmd': ['python', 'gpu_task.py'] + task['args'],
                'job_name': f'gpu_task_{i}',
                'gpus': 1,
                'memory': '32GB',
                'time': '04:00:00',
                'partition': 'gpu',
                'qos': 'gpu'
            }
        else:
            # Assign specific GPU
            job_id = yield {
                'cmd': ['python', 'gpu_task.py'] + task['args'],
                'job_name': f'gpu_task_{i}',
                'gpus': 1,
                'memory': '32GB',
                'time': '04:00:00',
                'partition': 'gpu',
                'constraint': f'gpu{suitable_gpu}',
                'env': {'CUDA_VISIBLE_DEVICES': str(suitable_gpu)}
            }

        job_ids.append(job_id)

    return job_ids

def parse_gpu_info(gpu_output):
    """Parse GPU information from nvidia-smi output."""
    # Implementation would parse the actual output
    return [{'id': i, 'free_memory': 16000} for i in range(4)]

def find_suitable_gpu(available_gpus, required_memory_gb):
    """Find a GPU with sufficient free memory."""
    required_mb = required_memory_gb * 1024
    for gpu in available_gpus:
        if gpu['free_memory'] >= required_mb:
            return gpu['id']
    return None
```

## Workflow Monitoring and Logging

### Comprehensive Job Monitoring

```python
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@cluster
def monitored_workflow(experiment_name: str):
    """Workflow with comprehensive monitoring and logging."""

    start_time = datetime.now()
    logger.info(f"Starting workflow: {experiment_name}")

    try:
        # Stage 1: Data preparation
        logger.info("Stage 1: Data preparation")
        prep_start = datetime.now()

        prep_id = yield {
            'cmd': ['python', 'prepare_data.py', '--experiment', experiment_name],
            'job_name': f'{experiment_name}_prep',
            'cpus': 4,
            'memory': '16GB',
            'time': '01:00:00'
        }

        prep_duration = datetime.now() - prep_start
        logger.info(f"Data preparation completed in {prep_duration}")

        # Stage 2: Training
        logger.info("Stage 2: Model training")
        train_start = datetime.now()

        train_id = yield {
            'cmd': ['python', 'train_model.py', '--experiment', experiment_name],
            'job_name': f'{experiment_name}_train',
            'cpus': 16,
            'memory': '64GB',
            'gpus': 2,
            'time': '08:00:00'
        }

        train_duration = datetime.now() - train_start
        logger.info(f"Training completed in {train_duration}")

        # Stage 3: Evaluation
        logger.info("Stage 3: Model evaluation")
        eval_start = datetime.now()

        eval_id = yield {
            'cmd': ['python', 'evaluate_model.py', '--experiment', experiment_name],
            'job_name': f'{experiment_name}_eval',
            'cpus': 8,
            'memory': '32GB',
            'time': '02:00:00'
        }

        eval_duration = datetime.now() - eval_start
        logger.info(f"Evaluation completed in {eval_duration}")

        total_duration = datetime.now() - start_time
        logger.info(f"Workflow completed successfully in {total_duration}")

        return {
            'experiment': experiment_name,
            'jobs': [prep_id, train_id, eval_id],
            'duration': total_duration.total_seconds(),
            'status': 'success'
        }

    except Exception as e:
        error_duration = datetime.now() - start_time
        logger.error(f"Workflow failed after {error_duration}: {e}")

        # Cleanup job
        cleanup_id = yield {
            'cmd': ['python', 'cleanup.py', '--experiment', experiment_name],
            'job_name': f'{experiment_name}_cleanup',
            'cpus': 2,
            'memory': '8GB',
            'time': '00:30:00'
        }

        return {
            'experiment': experiment_name,
            'status': 'failed',
            'error': str(e),
            'cleanup_job': cleanup_id,
            'duration': error_duration.total_seconds()
        }
```

### Real-time Progress Tracking

```python
@cluster
def progress_tracked_job(dataset_size: int):
    """Job with real-time progress tracking."""

    # Create progress file
    progress_file = f'progress_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

    job_id = yield {
        'cmd': ['python', 'trackable_process.py',
               '--dataset-size', str(dataset_size),
               '--progress-file', progress_file],
        'job_name': 'progress_tracked',
        'cpus': 8,
        'memory': '32GB',
        'time': '04:00:00',
        'block': False  # Don't block so we can monitor
    }

    # Monitor progress
    submitter = cluster.CLUSTERS['hpc_cluster']

    while True:
        # Check job status
        status = submitter.get_job_status(job_id)

        # Read progress if available
        try:
            with open(progress_file) as f:
                progress = json.load(f)
                logger.info(f"Job {job_id} progress: {progress['percent']:.1f}% "
                           f"({progress['current']}/{progress['total']})")
        except FileNotFoundError:
            logger.info(f"Job {job_id} status: {status.status}")

        if status.is_finish:
            break

        time.sleep(60)  # Check every minute

    return job_id
```

## Custom Submitter Extensions

### Creating a Custom Submitter

```python
from molq.submitor.base import BaseSubmitor, JobStatus
import subprocess

class DockerSubmitor(BaseSubmitor):
    """Custom submitter for Docker container execution."""

    def submit_job(self, config: dict) -> int:
        """Submit a job to run in a Docker container."""

        # Extract Docker-specific configuration
        image = config.get('docker_image', 'python:3.9')
        volumes = config.get('volumes', {})
        ports = config.get('ports', {})

        # Build Docker command
        docker_cmd = ['docker', 'run', '--rm']

        # Add volume mounts
        for host_path, container_path in volumes.items():
            docker_cmd.extend(['-v', f'{host_path}:{container_path}'])

        # Add port mappings
        for host_port, container_port in ports.items():
            docker_cmd.extend(['-p', f'{host_port}:{container_port}'])

        # Add image and command
        docker_cmd.append(image)
        docker_cmd.extend(config['cmd'])

        # Submit job
        if config.get('block', True):
            result = subprocess.run(docker_cmd, capture_output=True)
            job_id = hash(str(docker_cmd))  # Generate pseudo job ID

            status = JobStatus.Status.COMPLETED if result.returncode == 0 else JobStatus.Status.FAILED
            self.GLOBAL_JOB_POOL[job_id] = JobStatus(job_id, status)

            return job_id
        else:
            proc = subprocess.Popen(docker_cmd)
            job_id = proc.pid

            self.GLOBAL_JOB_POOL[job_id] = JobStatus(job_id, JobStatus.Status.RUNNING)
            return job_id

    def get_job_status(self, job_id: int) -> JobStatus:
        """Get status of a Docker job."""
        return self.GLOBAL_JOB_POOL.get(job_id,
                                       JobStatus(job_id, JobStatus.Status.FAILED))

# Register the custom submitter
from molq.submit import submit

def get_docker_submitor(cluster_name: str, cluster_type: str):
    if cluster_type == 'docker':
        return DockerSubmitor(cluster_name)
    # Fallback to default submitters
    from molq.submit import get_submitor
    return get_submitor(cluster_name, cluster_type)

# Monkey patch for custom submitter
import molq.submit
molq.submit.get_submitor = get_docker_submitor

# Usage
docker = submit('docker_env', 'docker')

@docker
def containerized_job():
    yield {
        'cmd': ['python', 'analysis.py'],
        'docker_image': 'tensorflow/tensorflow:latest-gpu',
        'volumes': {'/data': '/container/data'},
        'env': {'CUDA_VISIBLE_DEVICES': '0'}
    }
```

## Best Practices for Advanced Usage

### 1. Workflow Documentation

```python
@cluster
def documented_workflow(data_path: str, model_type: str):
    """
    Execute machine learning training workflow.

    This workflow implements a complete ML pipeline including:
    - Data validation and preprocessing
    - Feature engineering
    - Model training with hyperparameter tuning
    - Model evaluation and validation

    Args:
        data_path: Path to input dataset
        model_type: Type of model ('rf', 'xgb', 'nn')

    Returns:
        Dictionary containing job IDs and workflow metadata

    Resource Requirements:
        - Data preprocessing: 4 CPUs, 16GB RAM, 1 hour
        - Feature engineering: 8 CPUs, 32GB RAM, 2 hours
        - Model training: 16 CPUs, 64GB RAM, 8 hours
        - Evaluation: 4 CPUs, 16GB RAM, 1 hour

    Dependencies:
        - Python 3.9+
        - scikit-learn, xgboost, tensorflow
        - Custom preprocessing modules
    """
    # Implementation here...
```

### 2. Configuration Management

```python
import yaml
from pathlib import Path

# Load configuration from file
def load_workflow_config(config_file: str) -> dict:
    """Load workflow configuration from YAML file."""
    with open(config_file) as f:
        return yaml.safe_load(f)

@cluster
def configurable_workflow(config_file: str):
    """Workflow driven by external configuration."""

    config = load_workflow_config(config_file)

    job_ids = []
    for stage_name, stage_config in config['stages'].items():
        job_id = yield {
            'cmd': stage_config['command'],
            'job_name': f"{config['workflow_name']}_{stage_name}",
            **stage_config['resources']
        }
        job_ids.append(job_id)

    return job_ids
```

### 3. Testing and Validation

```python
import pytest
from unittest.mock import Mock, patch

def test_workflow():
    """Test workflow logic without actual job submission."""

    # Mock the submitter
    mock_submitter = Mock()
    mock_submitter.submit_job.return_value = 12345

    with patch('molq.submit.get_submitor', return_value=mock_submitter):
        # Test your workflow
        result = my_workflow('test_data.csv')

        # Verify the correct commands were submitted
        assert mock_submitter.submit_job.called
        call_args = mock_submitter.submit_job.call_args[0][0]
        assert 'python' in call_args['cmd']
```

### 4. Resource Profiling

```python
@cluster
def profiled_job(profile_mode: bool = False):
    """Job with optional performance profiling."""

    cmd = ['python', 'compute_intensive.py']

    if profile_mode:
        # Add profiling tools
        cmd = ['python', '-m', 'cProfile', '-o', 'profile.stats'] + cmd

    job_id = yield {
        'cmd': cmd,
        'job_name': 'profiled_computation',
        'cpus': 16,
        'memory': '64GB',
        'time': '04:00:00'
    }

    if profile_mode:
        # Analyze profiling results
        analysis_id = yield {
            'cmd': ['python', 'analyze_profile.py', 'profile.stats'],
            'job_name': 'profile_analysis',
            'cpus': 2,
            'memory': '8GB',
            'time': '00:30:00'
        }
        return [job_id, analysis_id]

    return job_id
```

## Troubleshooting Advanced Workflows

### Common Issues and Solutions

**1. Job Dependencies Not Working**
```python
# Problem: Dependencies not properly specified
@cluster
def broken_dependency():
    job1 = yield {'cmd': ['step1.py']}
    job2 = yield {'cmd': ['step2.py']}  # Should depend on job1

# Solution: Explicit dependency specification
@cluster
def fixed_dependency():
    job1 = yield {'cmd': ['step1.py']}
    job2 = yield {
        'cmd': ['step2.py'],
        'dependency': job1  # Explicit dependency
    }
```

**2. Resource Conflicts**
```python
# Problem: Over-requesting resources
@cluster
def resource_hog():
    yield {
        'cmd': ['python', 'simple_script.py'],
        'cpus': 128,  # Too many CPUs for simple task
        'memory': '1TB'  # Excessive memory
    }

# Solution: Right-sized resource requests
@cluster
def efficient_resources():
    yield {
        'cmd': ['python', 'simple_script.py'],
        'cpus': 2,  # Appropriate for the task
        'memory': '4GB'
    }
```

**3. Error Propagation**
```python
# Problem: Errors not properly handled
@cluster
def silent_failure():
    yield {'cmd': ['might_fail.py']}  # Errors not caught

# Solution: Explicit error handling
@cluster
def handled_failure():
    try:
        yield {'cmd': ['might_fail.py']}
    except Exception as e:
        logger.error(f"Job failed: {e}")
        # Cleanup or alternative action
        yield {'cmd': ['cleanup.py']}
        raise
```

This advanced usage guide provides patterns and techniques for building robust, scalable computational workflows with Molq. The key is to start simple and gradually add complexity as needed, always keeping maintainability and reliability in mind.
