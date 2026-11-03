# Changelog

## Unreleased

Slurm maps `after_started` to the native `after` dependency, which waits until the named allocation has begun.

Breaking: Cluster definitions drop `target_root`. Every job directory is `~/.molcrafts/molq/jobs/<id>` under the target login's home; re-register Clusters without the field. SSH Clusters keep no molq files after a job ends. The first observation of a confirmed terminal state moves the job directory (script, stdout, stderr, receipts and files written to the default working directory) to `collected/<cluster-id>/` next to the registry and removes it from the target; `logs.read` and Shell receipts then use the local copy. SSH `files.transfer` now passes the transport's rsh before the operand separator, so remote copies no longer fail.

`files.transfer` is the only file copy, in rsync syntax: `molq transfer [OPTION...] SRC... DEST` with `CLUSTER:PATH` operands. Options pass to rsync unchanged and nothing is implied, so `-a`/`--delete` are written by the caller; transport-owned options (`-e`, `--rsync-path`, `-M`, daemon) are rejected. The wire takes `sources`, `destination` and `options`; the result carries rsync's `output`.

`mq.Path("CLUSTER:/path")` is a `PurePosixPath` that keeps its cluster through path arithmetic and pickling, with the `molab.Path` I/O methods plus `is_symlink`, `glob`, `rglob`, `touch`, `rmdir`, `rename`, `replace` and `transfer`. Cluster I/O uses the new single-path `files.stat`, `list`, `read`, `write`, `mkdir`, `remove`, `rename` and `touch` RPCs. Protocol major is 2.

## 0.9.0

Breaking architecture replacement: one serve process and one RPC contract. CLI, Python sync/async SDK and the TypeScript SDK are peer clients. There is no web UI and no compatibility layer for an older API.

Python restores the `submit(...)` → `@destination` → `yield JobSpec` style. Each yield receives a Job, and the function's return is preserved. Async Python uses `asubmit(...)`. TypeScript uses `await submit(...)` and destination-wrapped generator functions, with typed arguments and results; Node starts temporary stdio without an HTTP factory. Submission, validation and preview require JobSpec values; ordinary decorated functions return normally without submitting.

Scheduler/Transport are orthogonal async modules; OpenSSH owns connection configuration and multiplexing. Native scheduler/accounting and original Shell receipts supply task truth. SQLite persists only registry metadata and Clusters; settings and presets live in JSON or YAML. Detached Shell jobs survive client/Runtime exit.

Allocations use separate Resources/Scheduling/Execution, explicit bounded Unit/Sequence/Parallel plans, CPU/task budgets, site-bound MPI and formal nvidia_mps requests. Unsupported mappings fail explicitly. Agent-first JSON output, structured errors and exit codes, and stdio, HTTP and WebSocket entry points replace the old CLI, dashboard and daemon.

Removed Submitor, mirrored job state/history, retry, retention, workspace, plugin host and implicit local destination fallback. Re-register destinations and save registry-qualified native JobRefs. HPC mappings are fixture-tested; local execution is POSIX/Bash, with Windows control-plane checks in CI.

All SDK durations use seconds. SDKs are silent; CLI/help/version/diagnostics use JSON-RPC envelopes and watch uses NDJSON. Examples serialize results as JSON. Invalid request controls fail before dispatch; disconnected mutations retain unknown outcomes without resend.

Before 1.0 there is no independent protocol version or backward compatibility. The canonical schema is protocol/contract.json; hello reports the product release, registry identity, methods and limits without protocol/schema version fields.
