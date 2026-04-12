# Release Notes

## Unreleased

Project engineering refresh focused on documentation, packaging metadata, and GitHub automation.

### What's included

- New repository-level templates for issues, pull requests, and code ownership
- A dedicated `CI` workflow plus a trusted-publishing `Release` workflow
- Rewritten `README.md`, `docs/`, `CONTRIBUTING.md`, and `RELEASING.md`

## 0.1.0

Release date: 2025-06-24

Initial beta release of `molq`.

### What's included

- `Submitor` and `JobHandle` as the public job submission surface
- Typed submission models: `Memory`, `Duration`, `Script`, `JobResources`, `JobScheduling`, `JobExecution`
- Backends for local execution, SLURM, PBS, and LSF
- SQLite-backed persistence with reconciliation and monitoring
- CLI commands for submit, inspect, watch, cancel, and log access
