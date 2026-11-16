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
molq --registry ./registry.db --config ./config.json cluster list
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
molq --config ./config.json cluster list
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
  "settings": {"cache_max_entries": 4096, "cache_max_age": 5},
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

## Layout upgrade and legacy maintenance

Stop every old serve and stdio process using the registry. Back up
`registry.db` and `submissions/<registry-id>/` and remove
`job_retention_days` from the configuration. On open, the new runtime imports
legacy submission evidence into the existing SQLite database. Imported keys
remain reserved and return `CONFLICT` on new submissions. Run
`maintenance.prune_submissions` to remove only files verified against the
SQLite copy. The result reports removed and retained filenames.

Old archives are preserved during upgrade. Inspect a cluster's archive, then
choose specific verified duplicates to delete:

```sh
molq rpc maintenance.inspect --params '{"cluster":"hpc"}'
molq rpc maintenance.prune --params '{"cluster":"hpc","entries":["0123456789abcdef0123456789abcdef"]}'
molq rpc maintenance.prune_submissions
```

Inspection checks the local manifest and returns an optional `after` value
for the next page. For a removed cluster, inspect by its original cluster ID. Pruning checks the manifest again and verifies the remote copy. It deletes only
serve-host archive files whose complete matching copy is still present on the
target. Interrupted copies, mixed archives, and the only known copies of
logs or Shell receipts stay available for manual export. The new runtime does
not look up old Shell runs through these archives.

SSH connection options belong in OpenSSH configuration. molq does not read
`MOLQ_*` environment variables. See [watch configuration](plugins.md#defaults)
for cadence and notification routes.
