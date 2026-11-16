# 0.9.0

molq 0.9.0 replaces the 0.8 API. It provides one Go service with command-line,
Python, async Python, and TypeScript clients.

Before 1.0, there is no independent protocol version or backward compatibility.
The schema is `protocol/contract.json`; `--version` reports the product version.

## In this release

- One allocation model for Shell, Slurm, OpenPBS, and LSF.
- Python `submit(...)` / `asubmit(...)` and TypeScript generator decorators with `yield JobSpec`.
- Explicit resources, scheduling, MPI launch, and composable execution plans.
- Saved job references, batch observation, native history, and bounded log reads.
- Shared status, completion, errors and time units (seconds) across clients.
- JSON output for CLI commands, help, version and diagnostics; NDJSON for watch.
- An RPC watcher with optional notifications.

See [scheduler mappings](schedulers.md#scheduler-mappings) for representation limits.

## Upgrade from 0.8

Use `submit(...)` to bind a destination, then decorate functions that yield
`JobSpec`. Use `asubmit(...)` for async Python.
`Submitor`, `JobStore`, the previous plugins, and old CLI commands are removed.
The [quickstart](getting-started.md) covers their replacement.

Use a fresh registry for 0.9.0. Older registry layouts are rejected and left
unchanged; no job-database migration runs. Preserve old files and use the native
scheduler to follow allocations already accepted before upgrading.

Settings and presets now live in `~/.molcrafts/molq/config.json`. Job state and
history come from the native scheduler or Shell target receipts.
