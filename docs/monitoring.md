# Monitoring

`molq` separates submission from observation. Jobs are persisted in SQLite, refreshed by reconciliation, and surfaced through handles, record lists, CLI commands, and a Rich dashboard.

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

`JobStore` persists all job records in SQLite with WAL mode enabled.
By default the database is stored at `~/.molq/jobs.db`.

Each record contains:

- stable `job_id`
- scheduler identity and scheduler job id
- cached state
- timestamps such as `submitted_at`, `started_at`, and `finished_at`
- exit code and failure reason
- execution metadata including default stdout and stderr paths

## Reconciliation

`JobReconciler` is responsible for syncing persisted state with the scheduler.

Its main loop is:

1. Load active jobs for a cluster.
2. Batch-query the scheduler backend.
3. Compare cached state with reported state.
4. Persist transitions and updated timestamps.

`Submitor.refresh()` runs a reconciliation pass for the current cluster.

## Blocking Waits

`JobMonitor` provides higher-level waiting primitives on top of reconciliation.

```python
job = submitor.submit(argv=["python", "train.py"])
record = job.wait(timeout=3600)
```

For multiple jobs:

```python
records = submitor.watch([job.job_id for job in jobs], timeout=7200)
```

## Polling Strategies

`molq` ships three public polling strategies:

| Strategy | Use case |
|----------|----------|
| `FixedStrategy` | Simple fixed-interval polling |
| `ExponentialBackoffStrategy` | Lower scheduler pressure for long-running jobs |
| `AdaptiveStrategy` | Polling cadence based on expected runtime |

These strategies live in `molq.strategies` and back the monitor layer.

## Logs

Each submitted job gets default stdout and stderr paths unless you override them in `JobExecution`.
By default these files live under the submission working directory at
`.molq/jobs/<job-id>/`.

At runtime, those resolved paths are stored in `JobRecord.metadata` under:

- `molq.stdout_path`
- `molq.stderr_path`
- `molq.job_dir`

The CLI exposes them through:

```bash
molq logs <job-id> local --stream stdout
molq logs <job-id> local --stream stderr --tail 50
```

## Full-Screen Dashboard

The Rich dashboard is available in two forms:

- `RunDashboard` for a focused dashboard experience
- `MolqMonitor` for a full-screen view across all persisted jobs

From the CLI:

```bash
molq monitor
molq monitor --all --limit 500 --refresh 1.5
```

Closing the dashboard does not cancel running jobs.
