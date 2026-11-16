# Contributing

Read AGENTS.md and docs/spec/molq-1-0-runtime-spec.md before changing architecture. There is one Runtime implementation and one public RPC contract; clients are peers. Do not restore old JobStore, Submitor, capability negotiation or direct client Scheduler access.

Install with uv sync --extra dev. Use Ruff formatting/lint and ty type checks. Run pytest with coverage (80% floor, including Runtime subprocesses). Build and smoke-test the installed wheel outside the source checkout.

Schema changes start in tools/build_contract.py. Python façade changes start in client/objects.py. Regenerate via tools/generate_clients.py, build/test sdk/typescript and run tools/check_generated.py. All affected client projections and documentation must agree.

Native Scheduler tests use bounded transport fixtures for exact directives/state/accounting/error behavior. Real POSIX Shell integration verifies accepted jobs survive session exit, execution plan semantics and identity-checked cancellation. Windows CI covers the remote control plane. Fixtures cannot establish live HPC resource/configuration support.

Use typed structured errors, explicit uncertain mutation outcomes, finite deadlines and bounded memory. No permanent coroutine/session/timer per Job. No SQLite transactions surrounding SSH or native scheduler operations.
