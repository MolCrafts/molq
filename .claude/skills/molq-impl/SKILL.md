---
name: molq-impl
description: "Full implementation workflow for molq features. Use when implementing new adapters, mappers, CLI commands, or any non-trivial code change. Orchestrates: spec → design → TDD → implement → review → docs."
argument-hint: "<feature description or spec file path>"
---

# Implementation Workflow

You are orchestrating a full development cycle for the molq project. The user provided: `$ARGUMENTS`

**Execution discipline**: Before writing any code, enter **Plan Mode** to lay out the full plan, then create **Tasks** for each phase below. Update task status as work progresses (`in_progress` → `completed`). This enforces a structured, auditable workflow — the agent must not skip phases or jump ahead without completing prior tasks.

## Step 1: Parse Input

- If `$ARGUMENTS` is a file path ending in `.md` → read it as a spec
- Otherwise → invoke `/molq-spec $ARGUMENTS` to generate a spec first

## Step 2: Architecture Review

Read the spec and the CLAUDE.md architecture section. Validate:
- Which modules need changes (check the module dependency graph)
- New adapters must implement `SchedulerAdapter` protocol (`submit`, `query`, `cancel`, `terminal_status`)
- New mappers must extend `SchedulerMapper`
- New specs must use Pydantic `BaseModel` with `Field()`
- No layer violations (lower layers cannot import higher layers)

Report the module impact map before proceeding.

## Step 3: TDD — Write Tests First (RED)

Create test file(s) in `tests/`. Follow existing patterns:
- Use fixtures from `conftest.py`: `tmp_molq_home`, `mock_job_environment`, `test_script_dir`
- Mock subprocess calls for adapter tests
- Test Pydantic validation with `pytest.raises(ValidationError)`
- Name tests: `test_<component>_<scenario>[_<expected>]`

Run tests — verify they FAIL:
```bash
python -m pytest tests/test_<name>.py -v
```

## Step 4: Implement (GREEN)

Write minimal code to pass tests:
- Immutability: create new objects, never mutate
- Functions < 50 lines, files < 800 lines
- Error handling at system boundaries
- `model_copy(update={})` for Pydantic modifications

Run tests — verify they PASS.

## Step 5: Refactor (IMPROVE)

- Remove duplication
- Run `/molq-review` for quality + architecture + performance check
- Fix any issues found

## Step 6: Documentation

- Add Google-style docstrings to all public symbols
- Update `__init__.py` exports if new public API
- Update relevant docs in `docs/`

## Step 7: Final Verification

Run in parallel:
```bash
python -m pytest --cov=src/molq --cov-report=term-missing -v
black --check src/ tests/
isort --check-only src/ tests/
```

Report: files changed, test results, coverage, remaining TODOs.
