# Concepts

`molq` is built around a small number of orthogonal abstractions. Knowing
which one owns what saves you from guessing where to put configuration or
where a bug lives.

## The big picture

```
                         ┌───────────────────────────┐
                         │         Submitor          │   lifecycle engine
                         │  (store, monitor, events) │   "how jobs are tracked"
                         └────────────┬──────────────┘
                                      │ target=
                                      ▼
                         ┌───────────────────────────┐
                         │          Cluster          │   destination
                         │  scheduler kind + options │   "where jobs run"
                         └─────┬───────────────┬─────┘
                               │               │
               ┌───────────────▼─┐       ┌─────▼─────────────┐
               │    Scheduler    │       │     Transport     │
               │   (protocol)    │       │    (protocol)     │
               │ Local/Slurm/    │       │  Local / SSH      │
               │ PBS/LSF/Shell   │       │                   │
               └─────────────────┘       └───────────────────┘
                  HOW to talk                WHERE commands and
                  to the scheduler           file ops execute
                  (sbatch / qsub / bjobs)    (subprocess vs ssh+rsync)
```

The point of the split: **Scheduler × Transport are independent axes.** You
can drive a remote SLURM cluster via SSH, or a local Shell cluster via
subprocess, or any other combination — without touching `Submitor` or
`Cluster` code.

## Cluster — *where* jobs run

A `Cluster` is a destination spec. It owns:

- a **name** (used to scope persisted records)
- a **scheduler kind** (`"local"`, `"slurm"`, `"pbs"`, `"lsf"`, `"shell"`)
- a **Transport** (defaults to `LocalTransport`; pass `host="user@host"` to
  use SSH)
- optional **scheduler options** (`SlurmSchedulerOptions`, etc.)

A Cluster has no lifecycle state — no store, no monitor, no event bus. It
is cheap to construct. Multiple Submitors can share a Cluster, or a Cluster
can outlive a Submitor.

```python
import molq as mq

local = mq.Cluster("dev", "local")
hpc   = mq.Cluster("hpc",  "slurm", host="user@hpc.example.com")
```

Cluster exposes only **destination-side reads**:

- `cluster.get_queue()` — snapshot of `squeue --me` / `qstat -u $USER` /
  `bjobs` (empty for local)
- `cluster.get_workspace(name, path=...)` — handle to a remote directory
- `cluster.get_project(name, workspace=...)` — sub-namespace under a workspace

See `Cluster` in the [API reference](api.md#cluster).

## Submitor — *how* jobs are tracked

A `Submitor` is the lifecycle engine. It owns:

- the **`JobStore`** (SQLite at `~/.molq/jobs.db` by default)
- the **`JobReconciler`** (syncs persisted state with the scheduler)
- the **`JobMonitor`** (blocking waits, polling strategies)
- the **`EventBus`** (lifecycle event pub/sub)
- per-job defaults, retry policy, retention policy

Each Submitor is bound to **one** Cluster as its `target` at construction.
All lifecycle ops are implicitly scoped to that target's name, so two
Submitors targeting different Clusters can share a JobStore without seeing
each other's records.

```python
submitor = mq.Submitor(target=hpc)

handle = submitor.submit_job(argv=["python", "train.py"])
records = submitor.list_jobs()
submitor.cancel_job(handle.job_id)
```

The Submitor surface is verb_noun: `submit_job`, `list_jobs`, `get_job`,
`cancel_job`, `watch_jobs`, `refresh_jobs`, `cleanup_jobs`, `run_daemon`,
`on_event`, `off_event`. See [API reference](api.md#submitor).

### Multi-cluster

Multi-cluster on one process is just multiple Submitors:

```python
sub_local = mq.Submitor(target=mq.Cluster("dev", "local"))
sub_hpc   = mq.Submitor(target=mq.Cluster("hpc", "slurm", host="..."))

# Each Submitor's list_jobs() only sees its own target's records,
# even though they share the same JobStore file.
```

## Scheduler — the protocol behind the kind string

The `Scheduler` protocol is **internal**: users don't construct a Scheduler
directly. It is the abstract interface that `LocalScheduler`,
`SlurmScheduler`, `PBSScheduler`, `LSFScheduler`, and `ShellScheduler`
implement, with methods:

- `submit(spec, job_dir)` — translate a `JobSpec` into a scheduler submission
  (writes `run_slurm.sh`, calls `sbatch`, etc.)
- `poll_many(ids)` — batch query for current state
- `cancel(id)` — cancel a job
- `resolve_terminal(id)` — determine how a vanished job ended
- `list_queue(user=None)` — snapshot the scheduler's current queue

You configure schedulers indirectly by passing `scheduler_options=...` to
`Cluster`. See [Schedulers](schedulers.md) for option types.

## Transport — *physical* where commands run

The `Transport` protocol is also internal but worth understanding because
it is what makes "remote SLURM" work without any new dependencies:

- `LocalTransport` — runs commands via `subprocess`, file ops via `pathlib`
- `SshTransport` — shells out to OpenSSH / rsync; inherits your
  `~/.ssh/config`, agents, ProxyJump, ControlMaster, Kerberos

Schedulers use `self._transport.run(...)` for **every** shell call, so a
SLURM scheduler with an SshTransport runs `sbatch`, `squeue`, `scancel`,
and `sacct` over SSH — automatically.

You normally pick a Transport implicitly:

```python
mq.Cluster("hpc", "slurm")                            # LocalTransport
mq.Cluster("hpc", "slurm", host="user@hpc.example")   # SshTransport (shortcut)

# Or explicitly, when you need custom SSH options:
from molq.options import SshTransportOptions
from molq.transport import SshTransport

ssh = SshTransport(SshTransportOptions(
    host="user@bastion",
    identity_file="~/.ssh/hpc_key",
    ssh_opts=("-o", "ProxyJump=jump.example.com"),
))
mq.Cluster("hpc", "slurm", transport=ssh)
```

`host=` and `transport=` are mutually exclusive.

## Workspace and Project — remote directories

A `Workspace` is a base directory on the cluster's filesystem. A `Project`
is a sub-namespace under a workspace. Both share a tiny file-ops surface
that goes through the cluster's Transport, so the same code works against
a local filesystem or a remote cluster over SSH.

```python
ws   = cluster.get_workspace("scratch", path="/scratch/$USER")
proj = ws.get_project("alphafold")        # /scratch/$USER/alphafold

proj.ensure()                             # mkdir -p
proj.upload("./inputs", recursive=True)   # rsync local → cluster
handle = proj.submit_job(submitor, argv=["python", "run.py"])
proj.download("results.csv", "./out.csv")
```

`Project.submit_job` is sugar that overrides `JobExecution.cwd` to the
project path before forwarding to `submitor.submit_job(...)`.

Workspace and Project are deliberately thin. They do **not** auto-stage
local files referenced in argv — call `proj.upload(...)` explicitly.

## Job objects

These flow back from a submission:

- **`JobHandle`** — lightweight handle returned by
  `submitor.submit_job(...)`. Methods: `status()`, `wait(timeout)`,
  `cancel()`, `refresh()`. Fields: `job_id`, `cluster_name`, `scheduler`,
  `scheduler_job_id`.
- **`JobRecord`** — immutable snapshot of a job's full lifecycle state.
  Returned by `submitor.get_job(...)` and `handle.wait()`.
- **`QueueEntry`** — one row from `cluster.get_queue()`. Fields include
  `scheduler_job_id`, `name`, `user`, `state`, `partition`,
  `submit_time`, `start_time`. **Distinct from `JobRecord`**: `JobRecord`
  is molq's view of a job; `QueueEntry` is what the scheduler client sees,
  including jobs submitted outside molq.

## Cheat sheet

| Question | Answer |
|---|---|
| Where do I configure SSH? | `Cluster(host="user@host")` or `Cluster(transport=SshTransport(...))` |
| Where do I configure the SLURM partition for a single job? | `submit_job(scheduling=JobScheduling(partition="gpu"))` |
| Where do I set per-cluster defaults? | `Submitor(target=cluster, defaults=SubmitorDefaults(...))` |
| Where do I customize SQLite path? | `Submitor(target=cluster, store=JobStore(path))` |
| Where do retries live? | `Submitor(target=cluster, default_retry_policy=...)`, or per-call `submit_job(retry=...)` |
| Where do I see jobs *other people* submitted? | `cluster.get_queue()` (live scheduler snapshot) |
| Where do I see jobs *I* submitted via molq? | `submitor.list_jobs()` (persisted records) |
| Why is my SLURM `--partition` flag missing? | You passed `JobScheduling(queue=...)` — the field is `partition` now (legacy `queue` still loads from disk for one release) |
