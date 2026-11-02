---
name: molq-docs
description: "Audit molq docstring and documentation completeness. Checks Google-style docstrings, API docs, and export consistency. Use before PR or release."
argument-hint: "[module path]"
---

# Documentation Audit

Check documentation completeness for molq. Target: `$ARGUMENTS` (or all `src/molq/` if empty).

## Checks

### 1. Docstring Coverage
Scan all `.py` files for public symbols (classes, methods, functions) missing docstrings.

### 2. Docstring Format (Google Style)
Required sections for public methods:
- One-line summary
- Args (with types and descriptions)
- Returns (with type)
- Raises (if applicable)

Complex methods should also include Example.

### 3. Export Consistency
- All symbols in `__all__` have docstrings
- All public symbols in `__init__.py` are documented

### 4. Docs Directory
- `docs/api/` covers all public modules
- `docs/tutorial/` references current API
- `docs/spec/` specs match current design

## Output

Per-module coverage percentage and list of missing/incomplete docstrings.
