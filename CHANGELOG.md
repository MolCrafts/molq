# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.1] - 2025-06-24

### Added
- Initial release of Molq
- Support for local job execution
- Support for SLURM cluster job submission
- Decorator-based API with `@submit` and `@cmdline`
- Generator-based job control flow
- Unified resource specification system
- Support for job dependencies and monitoring
- CLI interface for job management
- Comprehensive documentation and examples

### Features
- **Job Submitters**: Local and SLURM backends
- **Resource Management**: CPU, memory, GPU, and time specifications
- **Job Arrays**: Support for parameter sweep jobs
- **Priority Control**: Job priority and QoS settings
- **Email Notifications**: Job status notifications
- **Job Dependencies**: Wait for job completion before starting
- **Error Handling**: Robust error handling and retry mechanisms
- **Documentation**: Complete user guide and API reference

### Supported Schedulers
- Local execution (for development and testing)
- SLURM (for HPC clusters)
- PBS/Torque (basic support)
- LSF (basic support)
