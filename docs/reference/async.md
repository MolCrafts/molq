# Async handles

Await decorated calls and operations that contact the service; close with
`await local.close()` or `await mq.close()`.
The API mirrors the [synchronous handles](core.md). Request values are shared.

## Destination binding

::: molq.client.async_objects.asubmit

## Cluster decorator

Decorate a synchronous request builder with an AsyncCluster and await the
decorated call. Yield a `JobSpec`; each yield receives an `AsyncJob`.

::: molq.client.async_objects.AsyncCluster.__call__

::: molq.client.async_objects
    options:
      show_root_heading: false
      members: [AsyncMolq, AsyncClusterRegistry, AsyncCluster, AsyncJob, AsyncJobCollection]
