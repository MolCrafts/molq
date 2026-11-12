# Quickstart

Run a local command, wait for it, and save its reference so you can return later.
Install molq with `pip install molcrafts-molq`.

## Python

Register a local destination, then use it as a decorator:

```python
import json

from molq import JobSpec, submit

local = submit("local", "local")


@local
def hello(name):
    job = yield JobSpec.argv(["echo", f"hello {name}"])
    return job


job = hello("molq")
result = job.wait(timeout=30)
print(json.dumps(job.logs(), ensure_ascii=False))
local.close()
```

Output:

```json
{"text": "hello molq\n", "next_offset": 11, "bytes_read": 11}
```

The decorated function runs on your submission machine. Each `yield JobSpec` submits an
allocation to the destination and receives its `Job`; `return` sets the function's
result. Calling `hello()` again submits another allocation.

Later scripts use `local = submit("local")` with the same `@local` decorator.
`local.close()` releases the connection and leaves jobs running.

Save a job's reference before closing if you want to return later:

```python
import json
from pathlib import Path

Path("ref.json").write_text(json.dumps(job.ref))
```

## Command line

Save this as `local.json`. Jobs run in `~/.molcrafts/molq/jobs` on the
execution machine.

```json
{
  "name": "local",
  "scheduler": "shell",
  "transport": {"kind": "local"}
}
```

Register it and submit a command:

```sh
molq cluster register --definition @local.json
molq job submit local -- echo "hello molq" > submit.json
```

Extract the reference from the JSON response:

```sh
python - <<'PY'
import json
from pathlib import Path
response = json.loads(Path("submit.json").read_text())
Path("ref.json").write_text(json.dumps(response["result"]["ref"]))
PY
molq job wait --ref @ref.json --timeout 30
molq job logs --ref @ref.json
```

A redirected command returns JSON. On a terminal, `cluster list` and `job list`
print a table; `--json` keeps the JSON object. `--ref` accepts the reference
itself, rather than the complete submission response. See [CLI reference](cli.md)
for exit codes.

## Return to a job

Keep the registry and reference. Closing a client leaves accepted jobs running.

```python
import json
from pathlib import Path

from molq import Molq

mq = Molq()
job = mq.job(json.loads(Path("ref.json").read_text()))
result = job.status()
print(json.dumps(result["snapshots"][0]["state"], ensure_ascii=False))
mq.close()
```

Use the registry that accepted the job. Next, [register an HPC destination](schedulers.md)
or [describe an allocation](jobs.md).
