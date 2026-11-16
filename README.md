# molq

Run local commands and HPC jobs from Python or the command line.
Supports Shell, Slurm, OpenPBS, and LSF.

```sh
pip install molcrafts-molq
```

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

Bind once, then reuse `submit("local", project_root=Path.cwd())` as a decorator. Each call submits
a new allocation. Keep `job.ref` to query it later. Closing the client leaves accepted jobs running.

The package includes the CLI and Go service. Python requires 3.12+; local
execution requires Bash on Linux/macOS. Remote destinations use system OpenSSH.

[Manual](https://docs.molcrafts.org/molq/) ·
[Quickstart](https://docs.molcrafts.org/molq/getting-started/) ·
[Python API](https://docs.molcrafts.org/molq/api/) ·
[TypeScript SDK](sdk/typescript)

**0.9.0 is a breaking release.** The 0.8 API and registry are replaced.
Read the [upgrade notes](docs/release-notes.md) before switching.

## Development

```sh
uv sync --extra dev --extra docs
uv run python tools/check_generated.py
CGO_ENABLED=0 go test ./...
uv run pytest
uv run --extra docs zensical build --strict
```

See [AGENTS.md](AGENTS.md) for the full development contract and release checks.
