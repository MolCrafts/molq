# Monitoring

`molq` separates submission from observation. Jobs are persisted in
SQLite, refreshed by reconciliation, and surfaced through handles, record
lists, CLI commands, and a Rich dashboard. Reconciliation, polling, and
events all live on `Submitor`; live scheduler introspection lives on
`Cluster`.

## Two views of "what is running"

| View                      | Source                                  | Includes other users? |
|---------------------------|-----------------------------------------|-----------------------|
| `submitor.list_jobs()`    | molq's persisted SQLite records         | No — only your jobs   |
| `cluster.get_queue()`     | live `squeue --me` / `qstat` / `bjobs`  | Scheduler-dependent     |

Use `list_jobs` when you want molq's own state machine view (with retries,
transitions, dependencies). Use `get_queue` when you want what the
scheduler client itself shows.

## Job Lifecycle

Jobs move through a terminal-aware state enum:

```python
from molq import JobState

JobState.CREATED
JobState.SUBMITTED
JobState.QUEUED
JobState.RUNNING
JobState.SUCCEEDED
JobState.FAILED
JobState.CANCELLED
JobState.TIMED_OUT
JobState.LOST
```

All terminal states report `True` from `is_terminal`.

## Persistence

`JobStore` persists all job records in SQLite with WAL mode enabled. By
default the database is stored at `~/.molq/jobs.db`. Multiple Submitors
(across multiple Clusters) share this store and filter by their target's
name.

Each record contains:

- stable `job_id`
- `cluster_name` (the Submitor's target name)
- `scheduler` (the scheduler kind)
- scheduler identity (`scheduler_job_id`)
- cached state
- timestamps such as `submitted_at`, `started_at`, and `finished_at`
- exit code and failure reason
- execution metadata including default stdout and stderr paths

## Reconciliation

`JobReconciler` is responsible for syncing persisted state with the
scheduler. Its main loop is:

1. Load active jobs for a cluster.
2. Batch-query the scheduler (via the Cluster's Transport).
3. Compare cached state with reported state.
4. Persist transitions and updated timestamps.

`submitor.refresh_jobs()` runs a reconciliation pass for the current
target Cluster.

## Blocking waits

`JobMonitor` provides higher-level waiting primitives on top of
reconciliation.

```python
handle = submitor.submit_job(argv=["python", "train.py"])
record = handle.wait(timeout=3600)
```

For multiple jobs:

```python
records = submitor.watch_jobs([h.job_id for h in handles], timeout=7200)
```

## Polling strategies

`molq` ships three public polling strategies:

| Strategy                      | Use case                                   |
|-------------------------------|--------------------------------------------|
| `FixedStrategy`               | Simple fixed-interval polling              |
| `ExponentialBackoffStrategy`  | Lower scheduler pressure for long jobs     |
| `AdaptiveStrategy`            | Polling cadence based on expected runtime  |

These strategies live in `molq.strategies` and back the monitor layer.

## Logs

Each submitted job gets default stdout and stderr paths unless you
override them in `JobExecution`. By default these files live under the
submission working directory at `.molq/jobs/<job-id>/`.

At runtime, those resolved paths are stored in `JobRecord.metadata`
under:

- `molq.stdout_path`
- `molq.stderr_path`
- `molq.job_dir`

The CLI exposes them through:

```bash
molq logs <job-id> local --stream stdout
molq logs <job-id> local --stream stderr --tail 50
```

## Lifecycle events

`Submitor` emits typed events through an `EventBus`. Subscribe with
`on_event` / unsubscribe with `off_event`:

```python
from molq import EventType

def on_terminated(payload):
    print(f"{payload.job_id}: {payload.transition.new_state.value}")

submitor.on_event(EventType.JOB_COMPLETED, on_terminated)
submitor.on_event(EventType.JOB_FAILED, on_terminated)

# Later:
submitor.off_event(EventType.JOB_COMPLETED, on_terminated)
```

## Background reconciliation

`submitor.run_daemon(once=False, interval=5.0, run_cleanup=True)` runs a
lightweight loop that calls `refresh_jobs` (and optionally
`cleanup_jobs`) on a fixed interval. Useful in long-running services that
want fresh state without explicit polling at every read.

## Full-screen dashboard

The Rich dashboard is available in two forms:

- `RunDashboard` for a focused dashboard experience
- `MolqMonitor` for a full-screen view across all persisted jobs

From the CLI:

```bash
molq monitor
molq monitor --all --limit 500 --refresh 1.5
```

Closing the dashboard does not cancel running jobs.
