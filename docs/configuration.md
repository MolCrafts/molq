# Configuration

The default configuration is `~/.molcrafts/molq/config.json`. Select another JSON
or YAML file with `--config`. Configuration files are used individually, not merged.

## Temporary service

With no endpoint, the CLI starts a temporary stdio service. Python does the same
with `Molq()`. Cluster definitions persist in the default registry at
`~/.molcrafts/molq/registry.db`.

Choose separate files for a project:

```python
import json

from molq import Molq

mq = Molq(registry="./registry.db", config="./config.json")
print(json.dumps(mq.clusters.list(), ensure_ascii=False))
mq.close()
```

```sh
molq --registry ./registry.db --config ./config.json clusters list
```

Keep the registry on local disk. Use one active service per registry when sharing
observations; independent instances do not share caches or cursors.

## Shared service

Save this as `config.json`. Replace the example token with your own token of at
least 16 characters; server and client tokens must match.

```json
{
  "serve": {
    "host": "127.0.0.1",
    "port": 17891,
    "token": "replace-with-your-own-token"
  },
  "client": {
    "endpoint": "http://127.0.0.1:17891",
    "token": "replace-with-your-own-token"
  }
}
```

Start the service in one terminal:

```sh
molq serve --config ./config.json
```

Use it from another:

```sh
molq --config ./config.json clusters list
```

Python and TypeScript take the endpoint and token explicitly:

```python
import json

mq = Molq(endpoint="http://127.0.0.1:17891", token="replace-with-your-own-token")
print(json.dumps(mq.clusters.list(), ensure_ascii=False))
mq.close()
```

For HTTP connections, the registry and settings file belong to the service.
`Molq(config=...)` selects a temporary service's file; it does not select an HTTP
endpoint from the file's `client` section. The CLI does read that section.

## Settings and presets

Cluster defaults apply to submissions on that destination. Presets are reusable
request fragments; retrieve one and apply its fields explicitly.

```json
{
  "settings": {"cache_max_entries": 4096, "cache_max_age": 5, "job_retention_days": 30},
  "presets": {"small": {"resources": {"tasks": 1, "cpus_per_task": 2}}}
}
```

Manage these through the service:

```sh
molq rpc config.get --params '{"key":"cache_max_age"}'
molq rpc config.set --params '{"key":"cache_max_age","value":5}'
molq rpc presets.get --params '{"key":"small"}'
molq rpc config.reload
```

`config.reload` applies external edits and resets observation cursors. Invalid
edits leave the previous settings active. `default_cluster` is a saved preference;
RPC operations still require an explicit scope.

## Job directory retention

`job_retention_days` (default `30`) is how long job directories are kept. A job
directory whose newest modification time, taken over the directory and its
immediate entries, is older than that many days is deleted with its contents. It
is not archived and cannot be recovered. This covers every registered cluster's
`~/.molcrafts/molq/jobs/` and the serve host's `collected/` archive next to the
registry, including archives of removed clusters. Only directories named by a
32-digit job identity are touched; anything else under those roots stays.
`0` disables purging. Values are whole days from 0 to 36500.

```sh
molq rpc config.set --params '{"key":"job_retention_days","value":30}'
molq purge
```

A listening `molq serve` purges at start and every 24 hours, and reads the
setting on each pass. A temporary stdio service does not purge on its own; run
`molq purge [CLUSTER]` (the `jobs.purge` method). Each cluster target and each
archive gets one result listing the removed directories; a cluster that cannot
be reached reports an error there, sets `partial_failure` (CLI exit 8), and does
not stop the others. The listening service writes those failures to stderr.
A job that runs longer than the window without changing its job directory
(no stdout, stderr or script change) loses that directory, including Shell
receipts. Once a directory is gone, its logs and Shell job state can no longer
be read.

SSH connection options belong in OpenSSH configuration. molq does not read
`MOLQ_*` environment variables. See [watch configuration](plugins.md#defaults)
for cadence and notification routes.
