# Files and logs

Each submission gets one job directory, `~/.molcrafts/molq/jobs/<directory>/` under the execution machine's login home, holding the job script, stdout, stderr, and shell receipts. It is the job's default working directory. A transfer path is an absolute path on that cluster's filesystem. The SSH account's permissions are the boundary.

On an SSH cluster that job directory exists only while the job runs. The first time serve observes a confirmed terminal state (a status query, `wait`, `watch` or `logs`), it moves the directory to the serve host with one rsync, at `collected/<cluster-id>/<directory>/` next to the registry (by default `~/.molcrafts/molq/collected/`), and removes it from the target. rsync deletes each remote file only after copying it, so an interrupted move keeps every file on one side and resumes at the next observation; a failure leaves the remote copy in place and is reported on serve's stderr. Logs of a collected job are read from the local copy. Files a job writes into its own job directory, which is its default working directory, move with it; anything that must stay on the cluster belongs in an explicit execution `cwd` elsewhere.

Job directories on targets and in `collected/` are deleted, not archived, once unchanged for `job_retention_days` (30 by default); see [retention](configuration.md#job-directory-retention).

## Read job output

```python
import json

chunk = job.logs(stream="stdout", offset=0, limit=65536)
print(json.dumps(chunk, ensure_ascii=False))
next_offset = chunk["next_offset"]
```

Use `stream="stderr"` for errors. Read subsequent chunks from `next_offset`.
Each read is bounded to 256 KiB and checks the job reference's identity.

`job.logs()` reads the allocation's native output paths. Copy a redirected
stdout or stderr file back with `transfer`.

## Transfer files

`molq transfer` is rsync syntax. Operands are `CLUSTER:PATH` or a local path,
read the way rsync reads `HOST:PATH`: a colon before the first slash names a
registered cluster. Options go to rsync unchanged and nothing is implied, so
write `-a` and `--delete` yourself. A trailing slash means what it means to
rsync.

```sh
molq transfer -av --delete --exclude build/ ./project/ hpc:/scratch/you/project/
molq transfer -a hpc:runs/run_01/out.xyz .
```

`CLUSTER:relative` is relative to that login's home. A local path is resolved
against the shell's working directory, and it names the serve host's
filesystem; with a remote `--endpoint`, that is the serve machine. One rsync
run copies between a local path and one SSH cluster, or within one host.
Two distinct SSH clusters require explicit local staging, as with rsync.
`-e`/`--rsh`, `--rsync-path`, `-M`/`--remote-option` and daemon options belong
to the cluster transport and are rejected. The result carries rsync's stdout
under `output`, bounded to 1 MiB, and the copy has a 90 second limit.

A destination handle keeps the same copy:

```python
local.transfer("/home/you/project/", hpc, "/scratch/you/project/", "-a", "--exclude=build/")
```

In Python, each option is one word with its value attached (`--exclude=build/`).

## Cluster paths

`mq.Path` is a POSIX path that also names its cluster. Path arithmetic keeps
the cluster; I/O goes through the serve process.

```python
import molq as mq

run = mq.Path("hpc:/scratch/you/run_01")
log = run / "out.log"              # Path('hpc:/scratch/you/run_01/out.log')
if log.exists():
    print(log.read_text())
for xyz in run.glob("frames/*.xyz"):
    print(xyz.name, xyz.stat().st_size)
(run / "input.json").write_text("{}")
run.transfer(mq.Path("results/run_01"), "-a")
```

A path without a cluster is local and uses `pathlib`. The methods follow
`pathlib`: `exists`, `is_file`, `is_dir`, `is_symlink`, `stat`, `iterdir`,
`glob`, `rglob`, `read_text`, `read_bytes`, `write_text`, `write_bytes`,
`mkdir`, `touch`, `unlink`, `rmdir`, `rename`, `replace`. Precondition
failures raise the matching `FileNotFoundError`, `FileExistsError`,
`NotADirectoryError`, `IsADirectoryError` or `OSError`; other failures raise
`MolqError`.

- `str(path)` is `CLUSTER:PATH` and round-trips through `mq.Path`;
  `as_posix()` is the path on its host. Pickling keeps the cluster.
- A cluster path has no `os.fspath`, so it cannot reach a local `open()` by
  mistake.
- `transfer(destination, *options)` makes `destination` a copy of the path. A
  directory is copied as its contents, `SRC/ DEST/` in rsync terms.
- `stat()` returns mode, uid, gid, size and whole-second mtime; the other
  fields are zero. Writes create missing parent directories. A write larger
  than 256 KiB is staged beside the file and renamed into place. Each listed
  directory costs one RPC, so a deep `rglob` on a cluster is slow.
- Paths use one process-wide `Molq()` unless you bind a client:
  `mq.Path("hpc:/scratch", molq=client)`. Derived paths keep it.

## Scoped application access

Bind paths to the application's own client and allowed root:

```python
with mq.Molq(timeout=3610) as client:
    root = mq.Path("hpc:/scratch/team/workspace", molq=client,
                   root="/scratch/team/workspace")
    page = root.list_page(with_stat=True, limit=256)
    batch = root.read_many([(root / "workspace.json").as_posix()])
    root.transfer(mq.Path("downloads/workspace", molq=client), "-a", timeout=3600)
```

`list_page` returns `entries` and `next_after`. Pass `after=page["next_after"]`
for the next page. `read_many` accepts destination paths, up to 128 per call,
with 256 KiB per file and 512 KiB total raw content. Each item has `data` or
an `error`; a capacity error never means a truncated success. Root constraints
are checked on the destination, including symlinks inside directory transfers.
They constrain operations and do not create a separate user permission system.

For an existing SSH alias that needs a verification response, call
`client.cluster("hpc").authenticate(code)`. The response is used once through a
private temporary helper and is never persisted. Key-based file operations work
directly; they do not require a pre-existing master. Authentication rejection,
connection timeout and an unreachable destination have separate error kinds.
