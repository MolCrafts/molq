# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-04-18

### Added
- `--all` flag for the `watch` command to monitor all active jobs simultaneously.
- Release engineering files aligned with the `molcfg` repository structure.
- GitHub issue templates, pull request template, and `CODEOWNERS`.
- Rebuilt docs covering getting started, schedulers, monitoring, API, CLI, and release notes.

### Changed
- Rewrote `README.md` to match the current public API and CLI surface.
- Replaced legacy GitHub Actions workflows with a dedicated `CI` workflow and a tag-driven `Release` workflow.
- Tightened packaging metadata in `pyproject.toml` and added a typed-package marker.
- Replaced `tomllib` with `molcfg` for configuration loading.
- Refined artifact defaults and runtime behavior.

## [0.1.0] - 2025-06-24

### Added
- Initial beta release of `molq`.
- Local, SLURM, PBS, and LSF scheduler backends behind a unified `Submitor` API.
- Typed job submission models including `Memory`, `Duration`, `Script`, `JobResources`, `JobScheduling`, and `JobExecution`.
- SQLite-backed job store with reconciliation, monitoring, and CLI support.
