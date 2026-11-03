# RPC and errors

All clients use JSON-RPC 2.0 with named parameters. The service exposes HTTP RPC
at `/rpc`, WebSocket subscriptions at the same path, and newline-delimited stdio.
Before 1.0, there is no independent protocol version or backward compatibility. Frames are limited to 1 MiB and batches to 128 requests.
Mutations require a request ID.

```sh
molq discover
molq rpc jobs.list --params '{"cluster":"hpc"}'
```

`discover` returns the method catalog and schemas. The canonical schema is
[contract.json](https://github.com/MolCrafts/molq/blob/master/src/molq/protocol/contract.json).
The hello handshake reports protocol and registry identity; it does not negotiate
scheduler resources.

## Methods

| Area | Methods |
|---|---|
| Connection | `molq.hello`, `schema.discover` |
| Clusters | `clusters.list`, `get`, `register`, `update`, `remove` |
| Submission | `jobs.validate`, `preview`, `submit` |
| State | `jobs.get`, `get_many`, `list`, `history`, `observe` |
| Cancellation | `jobs.cancel`, `cancel_many` |
| Retention | `jobs.purge` |
| Events | `events.subscribe` |
| Files | `logs.read`, `files.transfer`, `stat`, `list`, `read`, `write`, `mkdir`, `remove`, `rename`, `touch` |
| Settings | `config.get`, `set`, `reload` |
| Presets | `presets.list`, `get`, `set` |

Names in a row share the first method's prefix. Subscriptions passively deliver
already observed events; they do not sample the scheduler.

## Errors

Error data contains `kind`, `message`, `outcome`, and `context`. Python exposes
these through `MolqError`:

```python
import json

from molq import MolqError, submit

hpc = submit("hpc")
try:
    job = hpc.submit(spec)
except MolqError as error:
    print(json.dumps(error.to_wire(), ensure_ascii=False))
finally:
    hpc.close()
```

| Outcome | Meaning |
|---|---|
| `not_applied` | The mutation was not applied |
| `applied` | The operation took effect |
| `unknown` | It may have taken effect; reconcile before resending |

A disconnected or timed-out mutation can return `OUTCOME_UNKNOWN`. Check native
scheduler state or the affected configuration before another mutation. Clients
do not automatically resend. A `request_key` is retained for up to 10 minutes
and 1024 operations in one service; it is not recovery across a restart.

::: molq.errors.MolqError
