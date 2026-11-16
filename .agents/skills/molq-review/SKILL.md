---
name: molq-review
description: Comprehensive code review aggregating architecture, performance, documentation, and system safety checks. Use after writing code or during PR review.
argument-hint: "[path or module]"
user-invocable: true
---

Review code for: $ARGUMENTS

If no path given, review all files modified in `git diff --name-only HEAD`.

**Invoke all dimensions in parallel:**

1. **Architecture** → invoke `/molq-arch` on $ARGUMENTS
2. **Performance** → invoke `/molq-perf` on $ARGUMENTS
3. **Documentation** → invoke `/molq-docs` on $ARGUMENTS
4. **System Safety**:
   - Subprocess: timeout required, capture_output selective, list args not shell=True
   - Polling: uses PollingStrategy, threading.Event for interruptible sleep, batch queries
   - SQLite: WAL mode, context managers, parameterized queries
   - Resource cleanup: context managers, try/finally, temp file tracking
   - Protocol compliance: SchedulerAdapter has submit/query/cancel/terminal_status
   - Registry: submit.CLUSTERS consistency
5. **Code Quality** (inline):
   - Functions < 50 lines, files < 800 lines
   - No deep nesting (> 4 levels)
   - No hardcoded magic numbers
   - Type annotations on all public APIs
   - Google-style docstrings
   - Black + isort formatting
6. **Immutability** (inline):
   - JobSpec not mutated after creation (use model_copy)
   - No mutable default arguments
   - Transformations return new objects

**Severity levels**:
- CRITICAL — must fix (architecture violations, subprocess safety)
- HIGH — should fix (missing tests, performance issues)
- MEDIUM — fix when possible (style, documentation gaps)
- LOW — nice to have

**Output**: Merged report:
```
CODE REVIEW: <path>
ARCHITECTURE: ✅/❌ per check
PERFORMANCE: ✅/⚠️ per check
DOCUMENTATION: ✅/⚠️ per check
SYSTEM SAFETY: ✅/❌ per check
CODE QUALITY: ✅/⚠️ per check
IMMUTABILITY: ✅/❌ per check
SUMMARY: N CRITICAL, N HIGH, N MEDIUM, N LOW
```
