---
name: molq-test
description: "Run molq tests with coverage analysis, identify gaps, and suggest missing test cases. Use after implementing features or before PR."
argument-hint: "[test file or module path]"
---

# Test & Coverage Analysis

Run tests and analyze coverage for molq.

## Step 1: Run Tests

```bash
python -m pytest --cov=src/molq --cov-report=term-missing -v $ARGUMENTS
```

If `$ARGUMENTS` is empty, run the full suite.

## Step 2: Analyze Coverage

Report per-module coverage. Targets:

| Module | Target |
|--------|--------|
| status.py, strategies.py, callbacks.py | 90%+ |
| resources.py, reconciler.py | 90%+ |
| monitor.py, base.py, submit.py | 85%+ |
| lifecycle.py, adapters.py, scripts.py | 80%+ |
| dashboard.py, cli/main.py | 70%+ |
| submitor/local.py, submitor/slurm.py | excluded |

## Step 3: Identify Gaps

For each uncovered line:
1. Read the uncovered code
2. Determine what test would cover it
3. Suggest a concrete test function name and approach

## Step 4: Check Test Quality

- Tests use proper fixtures (`tmp_molq_home`, `mock_job_environment`)
- Subprocess calls are mocked
- Tests are isolated (no shared mutable state)
- Assertions are specific (not just `assert result`)
