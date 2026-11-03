# molq 0.9.0 development contract

The architecture principles in docs/spec/molq-1-0-principles.md and concrete contract in docs/spec/molq-1-0-runtime-spec.md apply to 0.9.0. This is a breaking replacement; do not restore Submitor, JobStore, old plugins/CLI or backward compatibility.

## Ownership and dependencies

Clients (CLI, Python sync/async OOP SDK, TypeScript SDK, and `molq watch`) are peers over public RPC. There is no web UI. They may share wire codecs and value constructors, but must not wrap another client's façade or import Scheduler, Transport, registry or serve services for business operations. The `molq serve` process entry point assembles the service only.

The serve process has one implementation, in Go: protocol, registry, scheduler, transport, observation and RPC. No extra backend/driver/engine/controller abstraction or internal service boundary. Do not put the service back in Python.

Scheduler is the implementation abstraction. SlurmScheduler/PBSScheduler/LSFScheduler/ShellScheduler own native resource/directive/dependency/launcher mapping, rendering, submit/query/history/cancel and native state normalization. Runtime does not branch on Scheduler names for business logic. Transport is orthogonal and owns bounded target I/O.

No capability inventory, probing, feature negotiation or fallback. Validation proves schema validity and exact representation only; real scheduling resources and permissions remain native decisions.

SQLite persists exactly schema_meta and clusters. Settings and presets live in a JSON or YAML file, default `~/.molcrafts/molq/config.json`; clients change them through RPC. There is no earlier registry schema and no migration. No mirrored Job state/history, event, lease or retry tables. Native scheduler/accounting is task truth. Shell launch/exit receipts are original target execution evidence. Job directories live at the fixed `~/.molcrafts/molq/jobs` on the target; Cluster has no job root. An SSH target keeps no molq files after a job ends: serve moves each confirmed-terminal job directory to the serve host's `collected/` archive and reads logs/receipts there. Job directories on targets and in `collected/` are deleted outright, never archived, once unchanged for `settings.job_retention_days` (default 30, 0 disables); a listening serve purges at start and every 24 hours, and `jobs.purge` purges on demand. Clients never open the registry. Do not read `MOLQ_*` environment variables.

Cluster stores destination semantics only: ID/name, Scheduler/Transport binding, SSH alias, defaults and necessary site launcher/dialect configuration. No cadence or notification policies. One registry normally has one active serve process; temporary stdio is the no-service mode. Independent instances do not share cache/events/cursors.

The serve process owns canonical snapshots/completion and scoped diff/events. `molq watch` chooses cadence, backoff and notification and uses RPC only. Notifications fail open. Subscriptions passively deliver already observed events.

Use asynchronous bounded subprocesses and Cluster/batch concurrency. No permanent Job coroutine/timer/session. OpenSSH owns SSH configuration/multiplexing; invoke system ssh/rsync and never shut down user masters. No retry/workflow/MPS manager.

## Public model and wire contract

Job is one allocation, with independent Resources/Scheduling/Execution. Execution units compose through UnitRef/Sequence/Parallel; each unit appears once. Single-unit argv stays simple. Memory is decimal-string bytes per node. MPI and CPU/task budgets are explicit. nvidia_mps is a formal Scheduler resource; no automatic MPS launch or whole-GPU fallback.

Python request values are frozen dataclasses. Molq/Cluster/Job/JobCollection are RPC-only OOP handles. Before 1.0, the protocol has no independent version and no backward compatibility. RPC uses JSON-RPC 2.0 with named params, 1 MiB frames, 128-request batches and structured errors. CLI outputs JSON with stable exit semantics.

Edit tools/build_contract.py for schemas and src/molq/client/objects.py for the Python projection. Regenerate protocol/contract.json, protocol/types.py, async_objects.py and TypeScript types; do not edit generated files directly. The TypeScript build compiles the SDK only.

## Required checks

    uv sync --extra dev
    uv run python tools/build_contract.py
    uv run python tools/generate_clients.py
    npm --prefix sdk/typescript ci
    npm --prefix sdk/typescript test
    uv run python tools/check_generated.py
    uv run ruff check src tests
    uv run ruff format --check src tests
    uv run ty check src/
    CGO_ENABLED=0 go test ./...
    uv run pytest --cov=molq --cov-report=xml
    uv run python -m build

Go tests cover the serve process, including scheduler fixtures and local Shell execution. Python tests in tests/v09 cover the client. Use real SQLite and temporary files. Do not claim fixture tests prove live HPC resources. Windows checks cover the remote control plane; POSIX/Bash local execution runs on Linux/macOS. The wheel build compiles the current platform binary into the package.

No import side effects. Keep source modules below 800 lines. Preserve user data and accepted native jobs during failures/client exit. Mutation disconnection means uncertain outcome; no automatic resend/fallback.
