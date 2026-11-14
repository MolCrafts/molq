# Submit jobs

Decorate a function with a registered destination:

```python
from molq import JobSpec, submit

local = submit("local")  # Bound in the quickstart.


@local
def hello(name):
    job = yield JobSpec.argv(["echo", f"hello {name}"])
    return job


job = hello("molq")
```

Every yielded request is a `JobSpec`. Use `JobSpec.argv(...)` for one command;
use the full constructor for resources, scheduling, and execution plans.
Each yield receives its `Job`. The generator's return is the decorated call's
result. See the [Python API](api.md) for synchronous and async examples.

Arguments remain literal. Request shell interpretation explicitly with
`ExecutionUnit.shell()` when you need pipes or redirection.

## Request an allocation

On a registered Slurm destination, request resources and queue settings separately
from the command:

```python
import json

from molq import Execution, ExecutionUnit, JobSpec, Resources, Scheduling, submit

spec = JobSpec(
    resources=Resources(
        nodes=1,
        tasks=1,
        cpus_per_task=4,
        memory_bytes="8000000000",
        time_limit_seconds=600,
    ),
    scheduling=Scheduling(name="simulation", account="your-project"),
    execution=Execution(
        cwd="/scratch/your-user/project",
        env={"OMP_NUM_THREADS": "4"},
        units=(ExecutionUnit.argv("main", ["python", "simulate.py"]),),
    ),
)
hpc = submit("hpc")
print(json.dumps(hpc.preview(spec), ensure_ascii=False))


@hpc
def simulate():
    job = yield spec
    return job
```

`memory_bytes` is a decimal string of bytes **per node**. The scheduler rounds
up to its native unit. CPU allocation does not set your application's thread
count; specify that in its arguments or environment.

`validate(spec)` checks the request. `preview(spec)` also returns the rendered
script. Neither proves that an account, partition, or resource is available.

## Execution plans

Run two branches together, then a final command. This example fits a single-node
Slurm allocation with two CPUs:

```python
import json

from molq import Parallel, Sequence, UnitRef

plan_spec = JobSpec(
    resources=Resources(nodes=1, tasks=2, cpus_per_task=1),
    execution=Execution(
        units=(
            ExecutionUnit.argv(
                "a", ["echo", "a"], placement={"tasks": 1, "cpus_per_task": 1}
            ),
            ExecutionUnit.argv(
                "b", ["echo", "b"], placement={"tasks": 1, "cpus_per_task": 1}
            ),
            ExecutionUnit.argv(
                "finish", ["echo", "done"], placement={"tasks": 1, "cpus_per_task": 1}
            ),
        ),
        plan=Sequence((Parallel((UnitRef("a"), UnitRef("b"))), UnitRef("finish"))),
    ),
)
print(json.dumps(hpc.preview(plan_spec), ensure_ascii=False))
```

Each unit appears once in the plan. A single unit needs no explicit plan.
`Sequence` stops on failure. `Parallel` waits for all started branches and
reports the first failure in plan order. Multiple units in a batch job require
an explicit task budget; parallel branches must fit that budget.

## MPI

Declare ranks and threads, and configure the cluster's launcher:

```python
from molq import Launch

mpi_spec = JobSpec(
    resources=Resources(nodes=2, tasks=8, cpus_per_task=2),
    execution=Execution(
        units=(
            ExecutionUnit.argv(
                "main",
                ["./solver"],
                launch=Launch(kind="mpi", ranks=8, threads=2),
            ),
        )
    ),
)
```

On Slurm with `launcher="srun"`, this renders an eight-rank native step.
The target must provide the executable and MPI environment.

## Dependencies

A dependency refers to another allocation in the same cluster:

```python
# previous is a submitted Job; next_execution is an Execution value.
dependent = JobSpec(
    execution=next_execution,
    scheduling=Scheduling(
        dependencies=({"ref": previous.ref, "condition": "after_success"},)
    ),
)
```

Conditions are `after`, `after_success`, `after_failure`, and `after_started`.
The scheduler must support the requested condition exactly; see [mappings](schedulers.md#scheduler-mappings).

Save `job.ref` after submission, or `collection.refs` for an array. Provide a
`request_key` to make one submission recoverable across serve restarts. Repeat
the same request and key to retrieve its receipt or reconcile with native
evidence. An unresolved submission returns `OUTCOME_UNKNOWN` and repeating
the key never resends it. Different parameters with the same key conflict.
Cancel keys remain scoped to one serve instance.

### Slurm job arrays

Set `Scheduling(array=JobArray((0, 1, 4), max_concurrent=2))` on a job spec.
Slurm receives one submission and runs each index in its own working directory.
The payload can read `SLURM_ARRAY_TASK_ID`. Python and TypeScript return a
`JobCollection`; CLI and RPC return `array_id`, `refs`, and `snapshots`.
Each element reference can be used independently for status, logs, dependencies,
and cancellation. The supported request size is 1–64 elements. Slurm decides
which indices and concurrency limits the site accepts.

For a Slurm plan with parallel branches, request one node, explicit tasks and
memory, and CPU-only resources. Each step receives its own CPU allocation
and a share of the requested memory proportional to its CPU count. Parallel
MPI uses the cluster's explicit `srun` launcher.

## Job directory retention

Each submission's job directory (`~/.molcrafts/molq/jobs/<id>/` on the target,
or its collected copy under `collected/` on the serve host) is deleted outright
once the job is confirmed terminal, any required SSH collection has completed,
and nothing in it has changed for `job_retention_days` (30 days by default).
Its script, logs and receipts go with it, and so do files the job wrote into its
default working directory; set an explicit execution `cwd` for results you want
to keep. See [retention](configuration.md#job-directory-retention) to change or
disable the window and to purge on demand.
