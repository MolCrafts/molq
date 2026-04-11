---
name: molq-spec
description: "Convert natural language requirements into a detailed technical spec for molq. Use before /molq-impl or when requirements need formalization."
argument-hint: "<natural language feature description>"
---

# Spec Generation

Convert this requirement into a technical spec: `$ARGUMENTS`

## Process

1. **Read CLAUDE.md** to understand molq's architecture (decorator + generator + adapter pattern)
2. **Search existing code** for similar implementations to ensure consistency
3. **Generate spec** in the format below
4. **Save** to `docs/spec/<kebab-case-name>.md`

## Spec Template

Generate a markdown document with these sections:

```markdown
# Spec: <Feature Name>

## Summary
One paragraph: what and why.

## Design

### New Types
Classes/enums with fields, types, and purpose.

### Modified Types
Existing types that change, with before/after.

### Module Changes
| File | Action | Description |
|------|--------|-------------|

### Interface Contracts
Protocols and what implementations must provide.

## Resource Mapping
(If scheduler-related) Table mapping JobSpec fields → scheduler parameters.

## Error Handling
What errors, how handled.

## Testing Strategy
### Unit Tests — specific test cases
### Integration Tests — cross-module scenarios
### Edge Cases — boundary conditions

## Migration
Breaking changes and migration steps.

## Open Questions
Unresolved decisions needing input.
```

## Rules

- Every new adapter must implement `SchedulerAdapter` protocol (submit, query, cancel, terminal_status)
- Every new data class must use Pydantic `BaseModel` with `Field()` descriptions
- JobSpec composition stays orthogonal: ExecutionSpec + ResourceSpec + ClusterSpec
- Check module dependency layers — lower cannot import higher
