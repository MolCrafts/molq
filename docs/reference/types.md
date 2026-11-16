# Request values

These frozen dataclasses describe requests. `to_wire()` returns their JSON form,
omitting `None` fields. Validation and destination defaults are applied by the
service when you call `validate`, `preview`, or `submit`.

`JobSpec.argv([...])` constructs a single-command specification. Decorators
yield this value and receive a `Job`; plain lists or dictionaries are not requests.

Memory is a decimal string of bytes per node. MPI ranks, threads, and allocation
task budgets are explicit. See [submit jobs](../jobs.md) for complete examples.

::: molq.domain
    options:
      show_root_heading: false
      members: [JobSpec, Resources, Accelerator, Scheduling, Execution, ExecutionUnit, Launch, UnitRef, Sequence, Parallel]
