---
name: molq-arch
description: "Validate molq code against architecture rules: module dependencies, protocol compliance, Pydantic model composition, anti-patterns. Use before PR or after refactoring."
---

# Architecture Validation

Validate the molq codebase against its architecture rules.

## Checks to Perform

### 1. Module Dependency Layers

Read all imports in `src/molq/` and verify they follow this hierarchy:

```
Layer 0: status.py, jobstatus.py (foundation — stdlib only)
Layer 1: resources.py, tempfiles.py, strategies.py, callbacks.py (data — stdlib + pydantic)
Layer 2: lifecycle.py, scripts.py (persistence — can use Layer 0-1)
Layer 3: adapters.py (scheduler interface — can use Layer 0-2)
Layer 4: reconciler.py (state sync — can use Layer 0-3)
Layer 5: monitor.py, dashboard.py (engine — can use Layer 0-4)
Layer 6: submitor/*.py (coordination — can use Layer 0-5)
Layer 7: submit.py, __init__.py (public API — can use Layer 0-6)
Layer 8: cli/main.py (interface — can use anything)
```

**Rule**: No upward imports. Report any violation with file, line, and fix suggestion.

### 2. Protocol Compliance

Every `SchedulerAdapter` implementation must have:
- `submit(spec: JobSpec) -> int`
- `query(job_id: int | None = None) -> dict[int, ...]`
- `cancel(job_id: int) -> None`
- `terminal_status(job_id: int) -> JobStatus | None`

### 3. Pydantic Model Rules

- Data classes extend `BaseModel`
- All fields use `Field()` with `description`
- `field_validator` for user-facing input
- `JobSpec` = `ExecutionSpec` + `ResourceSpec` + `ClusterSpec` (never flattened)

### 4. Anti-patterns

- Circular imports
- Mutable spec objects (should use `model_copy(update={})`)
- Flat config dicts in new code (legacy only in `_convert_legacy_config`)
- Direct subprocess calls in submitors (must go through adapters)
- Star imports outside `__init__.py`
- Bare `except:` clauses

## Output Format

For each check: `[OK]` or `[VIOLATION]` with file:line, rule broken, and fix.

End with: `Result: PASS (0 violations)` or `Result: FAIL (N violations)`.
