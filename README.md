# Molq: Molcrafts Queue Interface

[![Tests](https://github.com/molcrafts/molq/workflows/Tests/badge.svg)](https://github.com/molcrafts/molq/actions)
[![PyPI version](https://badge.fury.io/py/molq.svg)](https://badge.fury.io/py/molq)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Molq** is a unified and flexible job queue system designed for both local execution and cluster computing environments. It provides a clean, decorator-based API that makes it easy to submit, monitor, and manage computational tasks across different execution backends.

## ✨ Key Features

- **🎯 Unified Interface**: Single API for local and cluster execution
- **🐍 Decorator-Based**: Simple, Pythonic syntax using decorators
- **⚡ Generator Support**: Advanced control flow with generator-based tasks
- **🔌 Multiple Backends**: Support for local execution, SLURM clusters, and more
- **📊 Job Monitoring**: Built-in status tracking and error handling
- **💾 Resource Management**: Flexible resource allocation and cleanup
- **🔄 Job Dependencies**: Chain jobs and manage complex workflows
- **📧 Notifications**: Email alerts for job status changes

## 🚀 Quick Start

### Installation

```bash
pip install molq
```

### Basic Usage

```python
from molq import submit

# Create submitters for different environments
local = submit('dev', 'local')           # Local execution
cluster = submit('hpc', 'slurm')         # SLURM cluster

@local
def hello_world(name: str):
    """A simple local job."""
    job_id = yield {
        'cmd': ['echo', f'Hello, {name}!'],
        'job_name': 'greeting'
    }
    return job_id

@cluster
def train_model():
    """A GPU training job on the cluster."""
    job_id = yield {
        'cmd': ['python', 'train.py'],
        'cpus': 16,
        'memory': '64GB',
        'time': '08:00:00',
        'gpus': 2,
        'partition': 'gpu'
    }
    return job_id

# Run jobs
hello_world("Molq")
job_id = train_model()
```

### Command Line Integration

```python
from molq import cmdline

@cmdline
def get_system_info():
    """Execute command and capture output."""
    result = yield {'cmd': ['uname', '-a']}
    return result.stdout.decode().strip()

system_info = get_system_info()
print(system_info)
```

## 📖 Documentation

- **[Tutorial](https://molcrafts.github.io/molq/tutorial/getting-started/)** - Step-by-step guide
- **[API Reference](https://molcrafts.github.io/molq/api/)** - Complete API documentation
- **[Recipes](https://molcrafts.github.io/molq/recipes/machine-learning/)** - Real-world examples
- **[Examples](examples/)** - Practical code examples

## 🎯 Supported Backends

| Backend | Description | Status |
|---------|-------------|---------|
| **Local** | Local machine execution | ✅ Full support |
| **SLURM** | HPC cluster scheduler | ✅ Full support |
| **PBS/Torque** | Legacy cluster scheduler | 🚧 Basic support |
| **LSF** | IBM cluster scheduler | 🚧 Basic support |

## 🔧 Advanced Features

### Multi-Step Workflows
```python
@cluster
def analysis_pipeline():
    # Step 1: Preprocessing
    prep_job = yield {
        'cmd': ['python', 'preprocess.py'],
        'cpus': 8, 'memory': '32GB', 'time': '02:00:00'
    }

    # Step 2: Analysis (depends on preprocessing)
    analysis_job = yield {
        'cmd': ['python', 'analyze.py'],
        'cpus': 16, 'memory': '64GB', 'time': '08:00:00',
        'dependency': prep_job  # Wait for preprocessing
    }

    return [prep_job, analysis_job]
```

### Error Handling
```python
@cluster
def robust_job():
    try:
        return yield {'cmd': ['python', 'risky_script.py']}
    except Exception:
        # Fallback to safer approach
        return yield {'cmd': ['python', 'safe_script.py']}
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by [Hamilton](https://hamilton.dagworks.io) for dataflow patterns
- Built for the scientific computing and HPC community
