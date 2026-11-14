# molq 0.9.0 development contract

The architecture principles in docs/spec/molq-1-0-principles.md and concrete contract in docs/spec/molq-1-0-runtime-spec.md apply to 0.9.0. This is a breaking replacement; do not restore Submitor, JobStore, old plugins/CLI or backward compatibility.

## Ownership and dependencies

Clients (CLI, Python sync/async OOP SDK, TypeScript SDK, and `molq watch`) are peers over public RPC. There is no web UI. They may share wire codecs and value constructors, but must not wrap another client's façade or import Scheduler, Transport, registry or serve services for business operations. The `molq serve` process entry point assembles the service only.

The serve process has one implementation, in Go: protocol, registry, scheduler, transport, observation and RPC. No extra backend/driver/engine/controller abstraction or internal service boundary. Do not put the service back in Python.

Scheduler is the implementation abstraction. SlurmScheduler/PBSScheduler/LSFScheduler/ShellScheduler own native resource/directive/dependency/launcher mapping, rendering, submit/query/history/cancel and native state normalization. Runtime does not branch on Scheduler names for business logic. Transport is orthogonal and owns bounded target I/O.

No capability inventory, probing, feature negotiation or fallback. Validation proves schema validity and exact representation only; real scheduling resources and permissions remain native decisions.

The serve host uses one SQLite registry for `schema_meta`, `clusters` and `submission_operations`. The upgrade from schema 2 imports legacy key evidence once and reserves those keys. Settings and presets use the JSON or YAML config, default `~/.molcrafts/molq/config.json`. No mirrored Job state/history, event, lease or retry tables. Native scheduler/accounting and original Shell receipts are task truth. New submissions receive an explicit target project root and store default logs and Shell receipts beneath `<project_root>/.molq/jobs`. The project owner controls their lifetime. Observation and log reads never copy or delete job files. Explicit legacy maintenance only removes proven duplicates on the serve host and verified imported sidecars. Clients never open the registry. Do not read `MOLQ_*` environment variables.

Cluster stores destination semantics only: ID/name, Scheduler/Transport binding, SSH alias, defaults and necessary site launcher/dialect configuration. No cadence or notification policies. One registry normally has one active serve process; temporary stdio is the no-service mode. Independent instances do not share cache/events/cursors.

The serve process owns canonical snapshots/completion and scoped diff/events. `molq watch` chooses cadence, backoff and notification and uses RPC only. Notifications fail open. Subscriptions passively deliver already observed events.

Use asynchronous bounded subprocesses and Cluster/batch concurrency. No permanent Job coroutine/timer/session. OpenSSH owns SSH configuration/multiplexing; invoke system ssh/rsync and never shut down user masters. When an alias already has a ControlPath, use that master instead of a private one. No retry/workflow/MPS manager.

For one-off submissions, pass argv, inline JSON or `--spec @-`. Put transient specs, scripts, and local test outputs in an OS temporary directory. For test builds and other temporary files needed on HPC compute nodes, use a shared scratch directory on the target and keep `~/work` for project files.

## Public model and wire contract

Job is one allocation, with independent Resources/Scheduling/Execution. Execution units compose through UnitRef/Sequence/Parallel; each unit appears once. Single-unit argv stays simple. Memory is decimal-string bytes per node. MPI and CPU/task budgets are explicit. nvidia_mps is a formal Scheduler resource; no automatic MPS launch or whole-GPU fallback.

Python request values are frozen dataclasses. Molq/Cluster/Job/JobCollection are RPC-only OOP handles. Before 1.0, the protocol has no independent version and no backward compatibility. RPC uses JSON-RPC 2.0 with named params, 1 MiB frames, 128-request batches and structured errors. CLI output is JSON with stable exit semantics. On a terminal, `cluster list` and `job list` print a table; `--json` or a pipe keeps the JSON object. `--help` is text. A transport failure keeps its own kind and diagnostic.

Edit tools/build_contract.py for schemas and src/molq/client/objects.py and src/molq/client/jobs.py for the Python projection. Regenerate protocol/contract.json, protocol/types.py, the async projection and TypeScript types; do not edit generated files directly. The TypeScript build compiles the SDK only.

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
