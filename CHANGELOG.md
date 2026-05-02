# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-05-02

### Changed
- **Unified the local execution path.** `scheduler="local"` is now the
  no-batch-system backend (a `ShellScheduler` paired with whatever
  `Transport` you give the `Cluster`). All four scheduler kinds — `local`,
  `slurm`, `pbs`, `lsf` — now route every shell call through the
  Transport, so `Cluster(scheduler="local", host="...")` runs jobs on a
  remote workstation over SSH instead of silently ignoring `host=`.

### Removed
- `LocalScheduler` (the in-process `subprocess.Popen` + reaper
  implementation). Existing user code that constructs a `Cluster` with
  `scheduler="local"` is unaffected — the shell-based implementation is a
  drop-in replacement and writes the same `run.sh` / `_wrapper.sh` /
  `.exit_code` files. Code that imported `molq.scheduler.LocalScheduler`
  directly should switch to `ShellScheduler` (or `create_scheduler("local")`).
- The `"shell"` scheduler kind. Use `"local"` instead — the behavior is
  identical and now picks Transport from the `Cluster`.

### Fixed
- `ty check src/` now reports zero diagnostics (was 129 on 0.3.0). Most of
  the cleanup was annotating the `_conn` / `_store` invariants on
  `JobStore` and `Submitor`, switching `mollog` logger calls to f-strings
  (the `%s`-style positional API was rejected), making `_merge_one`
  generic, and threading explicit kwargs through
  `JobStore.compare_and_update_state` instead of an untyped `**kwargs`
  dict.

### Tooling
- `ty check src/`, `ruff check`, and `ruff format --check` are now
  enforced both in `.pre-commit-config.yaml` (commit-time) and in CI via
  `pre-commit run --all-files`. Tests run as a pre-push hook locally and
  in CI's matrix job.
- `default_install_hook_types: [pre-commit, pre-push]` so a single
  `pre-commit install` registers both hooks.

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
