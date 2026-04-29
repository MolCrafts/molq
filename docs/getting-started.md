# Getting Started

## Installation

```bash
pip install molcrafts-molq
```

The PyPI distribution name is `molcrafts-molq`; the import name is `molq`.

For local development:

```bash
pip install -e ".[dev]"
pip install -e ".[docs]"
```

## The two-axis model in 30 seconds

Two classes, two responsibilities:

| Class       | Owns                                            | Knows about                       |
|-------------|-------------------------------------------------|-----------------------------------|
| `Cluster`   | scheduler kind, transport, scheduler options   | **where** jobs run                |
| `Submitor`  | JobStore, reconciler, monitor, defaults         | **how** jobs are tracked          |

Read [Concepts](concepts.md) for the full picture. The rest of this page
is a hands-on walk-through.

## First Cluster + Submitor

Start with the local scheduler so you can validate your workflow without a
cluster:

```python
import molq as mq

cluster  = mq.Cluster("dev", "local")
submitor = mq.Submitor(target=cluster)
```

The first argument to `Cluster` is a name — it scopes persisted records and
appears in CLI listings. The second argument is the scheduler kind. For a
remote SLURM cluster you'd write:

```python
cluster = mq.Cluster("hpc", "slurm", host="user@hpc.example.com")
```

Passing `host=` switches the Transport from `LocalTransport` to
`SshTransport`. molq shells out to your system `ssh`/`rsync`/`scp`, so it
inherits `~/.ssh/config`, agent forwarding, ProxyJump, ControlMaster, and
Kerberos for free.

## Submit a Job

Every job uses exactly one command form: `argv`, `command`, or `script`.

```python
handle = submitor.submit_job(argv=["echo", "hello from molq"])

print(handle.job_id)
print(handle.status())  # cached state, no scheduler I/O
```

`submit_job` returns a `JobHandle`. `submitor.list_jobs()` returns
persisted `JobRecord`s for this Cluster.

## Wait for Completion

```python
record = handle.wait()

print(record.state.value)
print(record.exit_code)
```

`JobHandle.wait()` blocks until the job reaches a terminal state and
returns an immutable `JobRecord`.

## Add Typed Resources

```python
import molq as mq

handle = submitor.submit_job(
    argv=["python", "experiment.py"],
    resources=mq.JobResources(
        cpu_count=4,
        memory=mq.Memory.gb(8),
        time_limit=mq.Duration.hours(2),
    ),
    scheduling=mq.JobScheduling(partition="gpu"),
)
```

Resource values are typed objects, not raw scheduler strings:

```python
mq.Memory.parse("32G")
mq.Duration.parse("04:00:00")
mq.Duration.parse("2h30m")
```

> **Note:** the field is named `partition`, not `queue`. Old SQLite rows
> and TOML profiles using `queue` still load (one-release deprecation).

## Use a Script

Inline scripts are useful for multi-step workflows:

```python
handle = submitor.submit_job(
    script=mq.Script.inline("""
cd /workspace
python preprocess.py
python train.py
""")
)
```

You can also submit an existing file:

```python
handle = submitor.submit_job(script=mq.Script.path("./run_experiment.sh"))
```

## Snapshot the cluster's queue

`cluster.get_queue()` returns a parsed snapshot of the scheduler's current
queue (`squeue --me` for SLURM, `qstat -u $USER` for PBS, `bjobs` for LSF).
Local clusters return an empty list.

```python
for entry in cluster.get_queue():
    print(entry.scheduler_job_id, entry.state, entry.partition)
```

This is a *live scheduler view* — distinct from `submitor.list_jobs()`,
which is the persisted molq record.

## Stage files into a remote project

For SSH clusters, `Workspace` and `Project` give you remote-directory
handles whose file ops go through the cluster's Transport:

```python
ws   = cluster.get_workspace("scratch", path="/scratch/$USER")
proj = ws.get_project("alphafold")

proj.ensure()                                 # mkdir -p
proj.upload("./inputs", recursive=True)       # rsync local → cluster
handle = proj.submit_job(submitor, argv=["python", "run.py"])
proj.download("results.csv", "./out.csv")
```

`Project.submit_job` is sugar that overrides `JobExecution.cwd` to the
project path before forwarding to `submitor.submit_job(...)`.

## Multi-cluster

Multi-cluster on one process is just multiple Submitors — they share the
JobStore by default but each filters by their target's name:

```python
sub_local = mq.Submitor(target=mq.Cluster("dev", "local"))
sub_hpc   = mq.Submitor(target=mq.Cluster("hpc", "slurm", host="..."))

sub_local.list_jobs()  # only "dev" records
sub_hpc.list_jobs()    # only "hpc" records
```

## CLI Basics

```bash
molq submit local echo "hello"
molq list local
molq status <job-id> local
molq watch <job-id> local
molq logs <job-id> local --stream stdout
```

The `monitor` command opens a full-screen dashboard across all clusters:

```bash
molq monitor
```

## Next Steps

- [Concepts](concepts.md) — Cluster, Submitor, Scheduler, Transport, Workspace, Project
- [Schedulers](schedulers.md) — scheduler-specific options
- [Monitoring](monitoring.md) — lifecycle, reconciliation, dashboards
- [API Reference](api.md) — full exported surface
- [CLI Reference](cli.md) — command syntax
