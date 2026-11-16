---
title: molq
description: Submit and follow local commands and HPC jobs from Python or the command line.
hide: [navigation, toc]
hero:
  kicker: molq 0.9.0 Manual
  title: molq
  description: Run local commands and HPC jobs. Submit, wait, and read the results from one API.
  install:
    label: Install
    command: pip install molcrafts-molq
  actions:
    - {label: Get started, href: getting-started/, style: primary}
    - {label: Submit jobs, href: jobs/}
    - {label: API reference, href: api/}
---

<h1 class="molcrafts-sr-only">molq</h1>

<div class="molcrafts-manual-home" markdown>

<section class="molcrafts-manual-section molcrafts-manual-section--compact" markdown>

<div class="molcrafts-manual-section__header" markdown>

<span class="molcrafts-manual-eyebrow">Run</span>

## A function, a job

Choose a registered destination, decorate a function, and call it.

[Quickstart →](getting-started.md)

</div>

```python
import json
from pathlib import Path

from molq import JobSpec, submit

local = submit("local", "local", project_root=Path.cwd())


@local
def hello(name):
    job = yield JobSpec.argv(["echo", f"hello {name}"])
    return job


job = hello("molq")
result = job.wait(timeout=30)
print(json.dumps(job.logs(), ensure_ascii=False))
local.close()
```

</section>

<section class="molcrafts-manual-section molcrafts-manual-section--compact" markdown>

<div class="molcrafts-manual-section__header" markdown>

<span class="molcrafts-manual-eyebrow">Scale</span>

## The same job on a cluster

Choose the allocation and account. Your SSH configuration handles the connection.

[Cluster guide →](schedulers.md)

</div>

```python
from molq import JobSpec, Resources, Scheduling, submit

hpc = submit("hpc", project_root="/shared/project")


@hpc
def simulate():
    job = yield JobSpec.argv(
        ["python", "simulate.py"],
        resources=Resources(tasks=1, cpus_per_task=4, time_limit_seconds=600),
        scheduling=Scheduling(account="your-project"),
    )
    return job


job = simulate()
hpc.close()
```

</section>

<section class="molcrafts-manual-section molcrafts-manual-section--stack" markdown>

<div class="molcrafts-manual-section__header" markdown>

<span class="molcrafts-manual-eyebrow">Manual</span>

## Find your next step

</div>

<div class="molcrafts-manual-grid molcrafts-manual-grid--cols-3">
<a href="jobs/"><strong>Execution plans</strong><em>Resources, MPI, sequential and parallel commands.</em></a>
<a href="monitoring/"><strong>Follow jobs</strong><em>Status, waiting, history, and notifications.</em></a>
<a href="api/"><strong>Use the API</strong><em>Python, async Python, and TypeScript.</em></a>
</div>

</section>

</div>
