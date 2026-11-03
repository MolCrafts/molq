# Clusters

Bind a destination once, then select it with `submit("name")` or the CLI.

## Local execution

```python
from molq import submit

local = submit("local", "local")
local.close()
```

Shell launches detached Bash jobs. Use it for local commands; formal allocation
resources such as memory, CPU counts, or queue accounts require a batch scheduler.

## Slurm over SSH

Set up an SSH alias in `~/.ssh/config` and confirm that you can connect without
an interactive prompt. molq uses system OpenSSH in BatchMode; your SSH config
owns authentication, jump hosts, and multiplexing.

```python
from molq import submit

hpc = submit(
    "hpc",
    "slurm",
    transport={"kind": "ssh", "alias": "research"},
    launcher="srun",
    defaults={
        "resources": {"tasks": 1, "cpus_per_task": 2},
        "scheduling": {"account": "your-project"},
    },
)
hpc.close()
```

Replace the alias and account with your site's values. Paths in execution
specs refer to the target filesystem. Sync inputs with `transfer`, then
submit. The job runs in its execution working directory.

For the CLI, save the equivalent definition as JSON:

```sh
molq clusters register --definition @hpc.json
molq clusters list
molq clusters get hpc
```

## Scheduler mappings

| Scheduler | Supported representation | Limits in 0.9.0 |
|---|---|---|
| `shell` | Detached commands and target receipts | No formal batch resources or queue policies |
| `slurm` | Nodes, tasks, CPU budgets, memory, time, GPUs, MPS, dependencies, native steps | One accelerator request |
| `pbs` | OpenPBS JSON/select dialect, even task layout, per-node memory/GPU, time, dependencies | Set `pbs_dialect="openpbs"`; no QoS, reservation, exclusive, model, or MPS mapping |
| `lsf` | Single node, one CPU per task, time, common scheduling and dependencies | Other layouts, memory, GPU, and MPS requests are rejected |

Set `launcher` explicitly for MPI. Use `srun` for Slurm native steps, or a site
`mpirun` / `mpiexec` binding for supported MPI layouts.

`validate()` and `preview()` check whether a request can be represented exactly.
The native scheduler decides whether resources and permissions are available.

## Update a destination

Read the definition and its `revision`, then pass a complete replacement to
`cluster.update(definition, expected_revision=revision)`. CLI equivalents use
`--definition` and `--revision`.

Rename preserves the cluster ID. Changing its scheduler or transport requires a
new cluster. Every job directory is `~/.molcrafts/molq/jobs/<id>` on the target
login. Removing a definition leaves native jobs running; keep the definition if
you need to query those jobs later.
