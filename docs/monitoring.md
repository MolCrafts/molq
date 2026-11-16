# Follow jobs

Read one job or query saved references together:

```python
import json

result = job.status()
print(json.dumps(result["snapshots"][0]["state"], ensure_ascii=False))
result = mq.jobs([item.ref for item in jobs]).status()
```

## Read a result

| Field | Meaning |
|---|---|
| `snapshots` | State and evidence for each allocation |
| `coverage.complete` | Whether the requested scope was fully observed |
| `completion.all_confirmed_terminal` | Whether every allocation has confirmed terminal evidence |
| `completion.successful` | `true` for confirmed success, `false` for confirmed failure, otherwise `null` |

States are `queued`, `running`, `succeeded`, `failed`, `cancelled`, `timed_out`,
and `unknown`. Queue absence alone does not confirm completion. Inspect coverage
and freshness before treating a result as complete.

Choose freshness with `job.status(consistency=...)`:

| Mode | Behavior |
|---|---|
| `live` | Query the target now; the default |
| `bounded` | Reuse observations no older than `max_age` seconds |
| `cached` | Read available cached evidence without a native query |

Cached snapshots include their age. A failed sample does not turn a job into a
failed or terminal allocation.

## Wait and cancel

```python
import json

result = job.wait(timeout=600, interval=2)
if result["completion"]["successful"]:
    print(json.dumps(job.logs(), ensure_ascii=False))
```

Python wait durations are seconds. Waiting ends when all references have confirmed
terminal evidence; check `successful` separately. A wait timeout or client exit
leaves the job running. Cancellation acceptance also needs a later status query
to confirm its final state.

## Queue and history

```python
from molq import submit

hpc = submit("hpc")
queue = hpc.list(states=["running"], name="simulation")

# Slurm: terminal records for the target login user, including non-molq jobs.
history = hpc.history(states=["failed"], limit=100)

# All schedulers: retained evidence for specific saved references.
records = hpc.history(refs=[job.ref])
```

Range discovery is available on Slurm in 0.9.0. The default window is the last
24 hours. Set `since` and `until` to RFC3339 timestamps with a timezone; results
filter completion time in `[since, until)`. Limits are 1–1024, default 100.
Ref mode cannot be combined with range filters.

History retention belongs to the scheduler. If coverage is incomplete, narrow
the window or inspect its errors. An empty result does not prove that no older
job failed.

## Changes and events

```python
import json

first = hpc.observe()
next_sample = hpc.observe(cursor=first["cursor"])
print(json.dumps(next_sample["changes"], ensure_ascii=False))
```

The first sample establishes a baseline. An expired cursor or service restart
returns `resync` and a new baseline, without inventing completion events.
Use history explicitly to inspect earlier completions.

For continuous output and notifications, use [molq watch](plugins.md). Share one
[service endpoint](configuration.md#shared-service) when clients need the same
cache and cursors. Event subscriptions passively deliver changes already observed;
a watcher or other client must perform the observations.
