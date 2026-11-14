# Command line

A cluster is a registered destination. A job is one allocation on that cluster.
The SSH host in `~/.ssh/config` is only how the cluster is reached.

```sh
molq cluster list
molq job list hpc
```

On a terminal, those two lists print a table. A pipe, a redirect, or `--json`
prints one JSON object with either `result` or `error`. `--help` is text.
`--version` is JSON. Other commands print JSON. Watch streams one JSON response
per line. Diagnostics go to stderr. Exit codes are below.

## Connections

Without an endpoint, commands use a temporary stdio service. Select an HTTP
service with `--endpoint` and `--token`, or the configuration's `client` section.

```sh
molq --config ./config.json cluster list
molq --registry ./registry.db cluster list
```

The same connection applies to diagnostics, generic RPC, and `watch`.

## Jobs

```sh
molq job submit local --project-root "$PWD" -- echo hello
molq job submit hpc --project-root /shared/project --spec @job.json
molq transfer -a --exclude build/ ./inputs/ hpc:/shared/project/inputs/
molq job status --ref @ref.json
molq job wait --ref @ref.json --timeout 600 --interval 2
molq job logs --ref @ref.json --stream stderr --offset 0 --limit 65536
molq job cancel --ref @ref.json
```

`transfer` is rsync syntax: options, then `SRC... DEST`. `CLUSTER:PATH` names a
registered cluster the way rsync's `HOST:PATH` names a host; a plain path is
local. See [Files and logs](remote-files.md#transfer-files).

Use either `--spec` or argv after `--`. In argv mode, use `--name`, `--comment`,
`--partition`, and `--account` for scheduling. Use `--nodes`, `--tasks`,
`--cpus-per-task`, `--memory-bytes` (decimal bytes per node), and
`--time-limit-seconds` for resources. With a spec, include these in the
`scheduling` and `resources` objects. `--spec @-` reads JSON from stdin.
Slurm and LSF support comments. Each scheduler validates the representation.

`job preview` takes the same project and specification or argv and returns
the rendered script without submitting. In the commands below, set
`CLUSTER`, `PROJECT`, `PARTITION`, `ACCOUNT`, and `SIF` explicitly. `PROJECT`
and `SIF` are absolute paths on Arrhenius, visible from the compute node.

```sh
molq job preview "$CLUSTER" --project-root "$PROJECT" \
  --partition "$PARTITION" --account "$ACCOUNT" \
  --tasks 1 --cpus-per-task 1 --time-limit-seconds 120 -- \
  apptainer exec --bind "$PROJECT:/work" --pwd /work -- "$SIF" /bin/sh inputs/run.sh
```

Use the same arguments with `job submit` when ready. Pass a
`--request-key KEY` to reconcile that exact submission if the response is lost.
See the [Arrhenius quick start](https://user.naiss.se/arrhenius_hpc/quickstart/)
for site-specific scheduling and container requirements.

`--ref` accepts a saved JobRef, not the complete submit response. The
[quickstart](getting-started.md#command-line) shows how to extract it.
Wait timeouts and intervals are in seconds.

## Queue and history

```sh
molq job list hpc --state running --name simulation
molq job history hpc --state failed --limit 100
molq job history hpc --refs @refs.json
molq job observe hpc
molq watch --cluster hpc --interval 5
molq job requeue --ref @ref.json
```

`--state` is repeatable. Slurm range history accepts `--since` and `--until` as
RFC3339 timestamps and filters completion time in `[since, until)`; the default
window is 24 hours. `--refs` takes a JSON array and excludes range filters.
See [monitoring](monitoring.md) for coverage and cursor semantics.
For Shell, pass `--project-root PATH` to `job list`, `job observe`, and
`watch`. See [legacy maintenance](configuration.md#layout-upgrade-and-legacy-maintenance)
to inspect old archives.

## Destinations and diagnostics

```sh
molq cluster register --definition @hpc.json
molq cluster get hpc
molq cluster update hpc --definition @hpc.json --revision 1
molq ping
molq health
molq doctor
molq discover
molq rpc config.reload
```

`ping`, `health`, and `doctor` do not submit work. `rpc METHOD --params` accepts
inline JSON, `@file`, or `@-` for stdin.

## Output contract

`molq --version` returns the product version in `result.version`.
`molq --help` and `molq COMMAND --help` print text. Arguments after `--` belong
to your executable, including `--help`.

`molq cluster list` and `molq job list` print a table when stdout is a terminal.
Pass `--json`, or redirect stdout, to receive the JSON object instead. Agents
use that object. They do not parse the table.

SDK methods return values and produce no console output. Python examples use
`json.dumps(...)`; TypeScript examples use `JSON.stringify(...)`. Logs remain
inside a JSON `text` field with offsets for bounded reads.

## Exit codes

| Exit | Meaning |
|---|---|
| 0 | Successful operation |
| 1 | Unexpected internal failure |
| 2 | Invalid input or unknown method |
| 3 | Missing destination, file, or cache |
| 4 | Service, scheduler, or transport unavailable |
| 5 | Unsupported representation, protocol, or channel |
| 6 | Wait or operation deadline |
| 7 | Authentication, permission, or native rejection |
| 8 | Partial batch failure |
| 9 | Confirmed terminal execution failure |
| 10 | Conflict, uncertain mutation, or identity mismatch |

Cancellation acceptance does not confirm task completion. Check the JSON outcome
and later status. An unknown mutation outcome needs reconciliation before resend.
