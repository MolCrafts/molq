# Python API

Create a destination with `submit(...)`, decorate a function, and yield `JobSpec`.
Each yield receives a `Job`; the function's `return` is its result.

## Define and submit

```python
import json

from molq import JobSpec, submit

local = submit("dev", "local")


@local
def hello(name):
    job = yield JobSpec.argv(["echo", f"hello {name}"])
    return job


job = hello("molq")
job.wait(timeout=30)
print(json.dumps(job.logs(), ensure_ascii=False))
local.close()
```

`submit("dev", "local")` binds a local Shell destination. Jobs run in
`~/.molcrafts/molq/jobs` on the execution machine. Repeating an
explicit binding reuses the destination if the supplied fields match.
Use `submit("dev")` or `submit("hpc")` to select an existing destination.
See [clusters](schedulers.md) for SSH and scheduler settings.

The function runs on your submission machine. Each yield submits one allocation;
multiple yields submit separate allocations. Use an [execution plan](jobs.md#execution-plans)
for multiple commands inside an allocation. Ordinary functions retain their
normal return value and do not submit. Decoration itself performs no RPC.

Requests must be `JobSpec` values. Raw lists and dictionaries are rejected.
Submission errors propagate without automatic resend.

## Async Python

Use `asubmit(...)` and await decorated calls and job operations:

```python
import asyncio
import json

from molq import JobSpec, asubmit


async def main():
    local = await asubmit("dev", "local")

    @local
    def hello(name):
        job = yield JobSpec.argv(["echo", f"hello {name}"])
        return job

    try:
        job = await hello("molq")
        result = await job.wait(timeout=30)
        if result["completion"]["successful"]:
            print(json.dumps(await job.logs(), ensure_ascii=False))
    finally:
        await local.close()


asyncio.run(main())
```

The builder remains a synchronous generator. Each yield receives an `AsyncJob`.
Cancelling the decorated call leaves accepted allocations running.

## Job handles

`job.status()`, `job.wait()`, `job.logs()` and `job.cancel()` operate on the
submitted allocation. Save `job.ref` to return later. Results are dictionaries;
request values are frozen dataclasses. All durations are seconds. SDK calls are silent; serialize returned values with
`json.dumps(...)` for machine-readable stdout.

A destination also exposes `submit(spec)`, `validate(spec)`, and `preview(spec)`;
all three require a `JobSpec`. `transfer(source_path, destination,
destination_path, *options)` copies a path to another destination handle with
one rsync run. Close the destination when finished. For file I/O on a cluster,
use [`mq.Path`](remote-files.md#cluster-paths).

## Client operations

`Molq` and `AsyncMolq` provide registry, configuration, saved references and batch
operations. They use the same RPC connection as destination handles:

```python
import json

from molq import Molq

mq = Molq()
try:
    print(json.dumps(mq.clusters.list(), ensure_ascii=False))
finally:
    mq.close()
```

Pass `endpoint`, `token`, `registry`, or `config` to `submit`, `asubmit`, or the
client constructor. Submit and transfer go through destination methods.
Failures raise [MolqError](reference/utilities.md#errors).

| Reference | Contents |
|---|---|
| [Sync API](reference/core.md) | `submit`, destinations, jobs and clients |
| [Async API](reference/async.md) | `asubmit` and async operations |
| [Request values](reference/types.md) | Resources, scheduling, commands and plans |
| [TypeScript](reference/typescript.md) | Generator decorators in Node and browsers |
| [RPC and errors](reference/utilities.md) | Methods, wire limits and failure outcomes |

## Prepared environments and explicit artifacts

An `Environment` builds the argv for a command on the execution host. Reuse
a `CondaEnvironment(name="simulation")`, a
`UvEnvironment("/shared/project")`, or a
`DockerEnvironment("simulation:v2")` across jobs. Use a Conda prefix when
the environment is stored at a specific path. For Docker, pass
`DockerMount(source, target, read_only=True)` entries and a container
`workdir` as needed. Pass container environment variables as explicit
`(name, value)` pairs. The wrapped executable becomes the container
entrypoint. Docker uses an image already on the target daemon, and uv
uses a prepared project without syncing or downloading Python.

`Artifact.deliver()` is an explicit operation independent of job status.
`CopyArtifact` uses `Path.transfer`, so uploads and downloads use the same
rsync RPC as the CLI. Supply a destination filename for a file or a target
directory for a directory's contents. Pass rsync options explicitly.
`SharedPathArtifact` checks a file or directory through the path's access
context and returns a reference without copying. An unqualified shared path
is checked on the Python client's filesystem. An unqualified **transfer**
operand denotes a path on the serve host. Use absolute paths and a
cluster-qualified `Path` for shared paths on a different machine. Set
`directory=True` when copying an unqualified directory that lives on a
separate serve host, so source type detection does not use the client filesystem.

This example assumes the remote shared directory, Conda environment, and
local results directory already exist. The input directory contains
`simulate.py` and its input data. The program writes to the output path
passed as its final argument.

```python
from molq import CondaEnvironment, CopyArtifact, JobSpec, Molq, Path

with Molq() as mq:
    hpc = mq.cluster("hpc")
    workspace = Path("hpc:/shared/project/run", molq=mq)
    inputs = workspace / "inputs"
    prepared = CondaEnvironment(name="simulation")
    CopyArtifact(Path("/srv/inputs", molq=mq), inputs, ("-a",)).deliver()

    jobs = []
    for sample in ("sample-a", "sample-b"):
        output = workspace / f"{sample}.json"
        argv = prepared.wrap(
            ["python", (inputs / "simulate.py").as_posix(), sample, output.as_posix()]
        )
        jobs.append((hpc.submit(JobSpec.argv(argv)), output))

    for job, output in jobs:
        job.wait()
        delivery = CopyArtifact(output, Path(f"/srv/results/{output.name}", molq=mq))
        print(delivery.deliver().location)
```

If delivery fails, inspect the transfer error and destination before
calling `delivery.deliver()` again. A transfer acknowledgement records the
rsync result. Your application can verify content and publish it separately.
Keep persistent results in a shared workspace outside MolQ's managed job
directory, which still follows the existing terminal collection policy.

### Select named outputs with a manifest

Create the manifest before or after submission. A task that discovers output
names at runtime can publish a JSON catalog after writing its files. Write
the catalog to a temporary file, then rename it into place. For example,
`{"checkpoints":{"latest":"checkpoints/step-400.chk"}}` maps the member
`latest` to a literal path relative to the output root.

```python
from molq import ArtifactManifest, Catalog, Glob, OutputPath, Path

manifest = ArtifactManifest({
    "summary": OutputPath("result.json"),
    "frames": Glob("frames/*.xyz"),
    "checkpoints": Catalog("artifacts.json", key="checkpoints"),
    "logs": Glob("diagnostics/*.log", required=False),
})
source = Path("hpc:/shared/runs/run-42/output", molq=mq)
selected = manifest.resolve(source, select=("summary", "checkpoints/latest"))
print(selected.paths)
delivery = selected.download(Path("/srv/results/run-42", molq=mq), options=("-a",))
```

Select a catalog group by name or one member with `group/member`. Fixed paths
and catalog members may name directories. Globs select only matching files
and support recursive `**`. An unmatched required selection raises an error.
Only selected outputs are resolved. The task supplies source paths, while
the caller chooses the destination and transfer options.

Download preserves the relative layout and uses `files.transfer`. Directory
downloads require explicit recursion, such as `-a`. Manifest downloads
accept a limited set of rsync options and reject destructive or filtering
options. Scoped SSH operands with shell metacharacters or whitespace are
rejected because older rsync clients can expand them on the target. For a
separate serve host, use a cluster with local transport to address its
destination directory, including parent creation over RPC.

After a failed download, reuse the resolved selection. Earlier copies may
already exist, and the SDK does not reread the catalog or rerun the glob.
The selection fixes paths and kinds. It does not snapshot file contents.
