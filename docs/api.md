# Python API

Create a destination with `submit(...)`, decorate a function, and yield `JobSpec`.
Each yield receives a `Job`; the function's `return` is its result.

## Define and submit

```python
import json
from pathlib import Path

from molq import JobSpec, submit

local = submit("dev", "local", project_root=Path.cwd())


@local
def hello(name):
    job = yield JobSpec.argv(["echo", f"hello {name}"])
    return job


job = hello("molq")
job.wait(timeout=30)
print(json.dumps(job.logs(), ensure_ascii=False))
local.close()
```

The decorator binds an existing target directory through `project_root`.
Each run creates a directory under `<project_root>/.molq/jobs` for default
logs and Shell receipts. A cluster-qualified `Path` must belong to the
selected cluster. `Workspace.path()` supplies a scoped project path.
Repeating an explicit cluster binding reuses the destination if its definition
matches.
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
from pathlib import Path

from molq import JobSpec, asubmit


async def main():
    local = await asubmit("dev", "local", project_root=Path.cwd())

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

A destination also exposes `submit(spec, project_root=...)`, `validate(spec)`, and `preview(spec, project_root=...)`;
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

`ApptainerEnvironment` takes an absolute target-side `.sif` path and wraps
the command with `apptainer exec`. Prepare or transfer the SIF before
submission. The wrapper performs no I/O, and a job fails through Apptainer
if the file is missing or unusable. Supply the cluster, Slurm partition,
and account through the existing submission interface.

```python
from molq import ApptainerEnvironment, ApptainerMount, JobSpec, Molq, Scheduling

project = "/shared/project/runs/simulation"
environment = ApptainerEnvironment(
    image="/shared/project/images/simulation-amd64.sif",
    mounts=(ApptainerMount(project, "/workspace"),),
    workdir="/workspace",
)

with Molq() as mq:
    job = mq.cluster("Arrhenius").submit(
        JobSpec.argv(
            ["python", "/opt/sim/run.py"],
            environment=environment,
            scheduling=Scheduling(partition="cpu", account="your-cpu-account"),
        ),
        project_root=project,
    )
```

The bind source is on the compute node's filesystem. The workdir is inside
the container. Apptainer also applies the site's default mounts. On Arrhenius,
keep SIF files on project storage outside the home directory and choose an
image for the partition's CPU architecture.
[Arrhenius container guide](https://user.naiss.se/arrhenius_hpc/software_development/containers/)

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
        jobs.append((hpc.submit(JobSpec.argv(argv), project_root=workspace), output))

    for job, output in jobs:
        job.wait()
        delivery = CopyArtifact(output, Path(f"/srv/results/{output.name}", molq=mq))
        print(delivery.deliver().location)
```

If delivery fails, inspect the transfer error and destination before
calling `delivery.deliver()` again. A transfer acknowledgement records the
rsync result. Your application can verify content and publish it separately.
Keep application outputs in the project paths you choose. MolQ leaves those paths intact when the job ends.

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

### Reuse a workspace across jobs

Bind a shared directory with `Workspace(Path("hpc:/shared/project/run-42", molq=mq))`.
Construction only saves the path. Call `workspace.ensure()` explicitly to
create it, then use `workspace.path("inputs")` and
`workspace.path("outputs/attempt-01")` for scoped file operations. Callers
choose a directory visible from the compute nodes.

Call `workspace.upload` to copy into an existing parent directory. Call
`workspace.download` to select names from a manifest and copy them to an
existing destination directory. Both use the same rsync RPC as `Path.transfer`.
Save the root's `CLUSTER:/absolute/path` spelling to reopen it with a new
connection and download outputs later.

```python
workspace.upload(Path("/local/inputs"), "inputs", "-a")
workspace.download(
    ArtifactManifest({"checkpoints": Catalog("catalog.json", "checkpoints")}),
    Path("/local/downloads"),
    relative="outputs",
    select=("checkpoints/latest",),
)
```

Pass a workspace path as `Execution.cwd`, an Environment mount, or a program
argument as appropriate. `JobSpec.argv` and `ExecutionUnit.argv` accept an
optional `environment=` that wraps argv when building the spec.

The caller owns the directory and its files, including directories created
by `ensure()`. Workspace has no automatic cleanup or registration. Its scope
constrains SDK file operations. Programs using the directory as their working
directory run with the target account's permissions.

### Run the Arrhenius acceptance harness

Provide a registered SSH alias, a cluster name for an isolated test registry,
an existing shared project directory, the intended CPU partition and account,
and a prepared SIF with `/bin/sh`.

```sh
uv run python tools/check_arrhenius.py \
  --ssh-alias "$SSH_ALIAS" --cluster "$CLUSTER" --project-root "$PROJECT" \
  --partition "$PARTITION" --account "$ACCOUNT" --sif "$SIF"
```

The harness creates a unique subdirectory and submits one short CPU job. It
compares the SDK and CLI previews, checks logs from a new connection, and
verifies that named download copies just one output. Its JSON result gives
the job reference and both evidence locations. The workspace and local
registry remain available for inspection, including after a wait timeout.
