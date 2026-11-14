# Changelog

## Unreleased

The CLI names the object. Destinations are `molq cluster list|get|register|update|remove`. Allocations are `molq job list|submit|preview|status|cancel|requeue|logs|wait|history|observe`. `molq list`, `molq clusters`, and the old top-level job verbs are rejected with the replacement usage. On a terminal, the two list commands print a table; a pipe, a redirect, or `--json` keeps the JSON object. `--help` is text.

CLI argv submissions accept partition, account, and CPU resource flags.
`job preview` renders the same job specification without submitting.
`transfer --transfer-deadline` sets the total copy limit. In Python,
`JobSpec.argv` and `ExecutionUnit.argv` accept a prepared Environment, and
`Workspace.upload` and `Workspace.download` copy inputs and named outputs
using the existing transfer path. The Arrhenius CPU harness exercises these
operations with an explicitly selected SSH alias, project, account, partition,
and SIF.

SSH uses the alias's OpenSSH ControlPath when one is configured, so an existing ControlMaster is the connection. A private ControlPath remains the fallback. `AUTH_REQUIRED`, `TRANSPORT_UNAVAILABLE`, and `CONNECTION_TIMEOUT` from that connection stay those kinds, with the SSH diagnostic, instead of being rewritten as `SCHEDULER_UNAVAILABLE`.

Slurm maps `after_started` to the native `after` dependency, which waits until the named allocation has begun.

Slurm receives the batch script through `sbatch` stdin. The command line pins the default working directory and allocation logs to the MolQ job directory, taking precedence over inherited `SBATCH_*` settings.

Submissions now require an explicit target project root. Default logs and Shell
receipts live under `<project_root>/.molq/jobs/<run_id>`. Terminal observation
leaves them on the target for explicit reads and transfers. PBS and Shell
use temporary batch scripts, and Slurm and LSF stream them on stdin.

The registry now holds durable keyed submission evidence in SQLite. On upgrade,
legacy submission keys remain reserved. Explicit maintenance checks old
archives for complete duplicates before deleting local copies.

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
