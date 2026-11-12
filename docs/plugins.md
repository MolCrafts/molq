# Watch and notifications

Follow registered destinations from the command line:

```sh
molq watch --cluster hpc --interval 5
molq watch --cluster hpc --once
```

`watch` samples job state, prints changes as JSON, and backs off after failures.
The first sample establishes a baseline; it does not replay historical
completions. Use `molq job history hpc` to inspect earlier jobs.

## Defaults

Add a watcher configuration to your configuration file:

```json
{
  "client": {
    "watch": {
      "clusters": ["hpc"],
      "interval": 5
    }
  }
}
```

```sh
molq watch --config ./config.json
```

Pass `--cluster` repeatedly to watch several destinations. If no clusters are
selected, the watcher uses registered destinations.

## Notifications

Set `client.watch.nerve_endpoint` to your receiver URL. The receiver gets the
same JSON envelopes that the watcher prints. Cadence and notification routing
belong to the watcher configuration, not to a cluster definition.

Delivery failures do not block observations. Slow receivers may miss notifications;
stdout remains the record of observed changes. A repeated scheduler error is
printed once until the destination succeeds again.

Use a [shared service](configuration.md#shared-service) when other clients should
receive the same observed events.
