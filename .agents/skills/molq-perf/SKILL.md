---
name: molq-perf
description: "Performance review for molq code: subprocess handling, polling strategies, SQLite operations, resource leaks, regex compilation. Use after modifying adapters, monitor, or lifecycle."
argument-hint: "[module path]"
---

# Performance Review

Analyze molq code for performance issues. Target: `$ARGUMENTS` (or all `src/molq/` if empty).

## Check These Areas

### Subprocess Management
- All `subprocess.run()` must have `timeout` parameter
- `capture_output=True` only when output is needed
- Arguments as list, never `shell=True`

### Polling & Blocking
- Polling uses `PollingStrategy` (not hardcoded `time.sleep`)
- `threading.Event.wait()` for interruptible sleep (not `time.sleep`)
- Batch queries where possible (one scheduler call for N jobs)

### SQLite Operations
- WAL mode enabled
- Connections via context managers
- Parameterized queries (no f-strings in SQL)
- Indexes on frequently queried columns

### Resource Management
- File handles closed via context managers
- No resource leaks in error paths (try/finally)
- Temp files tracked and cleaned up

### Parsing
- Regex patterns pre-compiled at module level
- No repeated Pydantic model creation in hot loops

## Output

For each finding: `[CRIT]`, `[WARN]`, or `[INFO]` with location, risk, and fix.
