# Jobs and clusters

A **cluster** is a named destination: which scheduler to use, how to reach it,
and where to place job files. It may be your laptop or an HPC login account.

A **job** is one allocation. It contains one command or a plan of commands that
run inside that allocation.

| Value | Describes | Example |
|---|---|---|
| `Resources` | Allocation size | 4 tasks, 2 CPUs per task, 8 GB per node |
| `Scheduling` | Placement in the queue | Account, partition, name, dependencies |
| `Execution` | Work inside the allocation | Commands, environment, working directory |
| `JobRef` | A saved allocation identity | Registry, cluster, native ID, incarnation |

An execution unit is one command. `Sequence` runs units in order; `Parallel`
runs branches together. Both compose into a single job. Use separate submissions
when you want separate allocations. See [execution plans](jobs.md#execution-plans).

## Where state lives

The scheduler owns queue and accounting records. For local Shell jobs, target
launch and exit receipts provide execution evidence. The registry stores cluster
definitions; it does not copy job history into a database.

A reference includes the native ID and its incarnation. Keep the whole reference:
a bare Slurm or PBS number is insufficient to identify a molq job safely.

## Connecting

`Molq()` starts a temporary service over stdio. A shared `molq serve` endpoint lets
the CLI, SDKs, and watcher use the same observations and event cursors. Each
independent service has its own cache and cursors.

Both connections use the same public API. See [configuration](configuration.md)
for a shared service and [monitoring](monitoring.md) for status freshness.
