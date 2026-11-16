# Client handles

`Molq()` uses a temporary stdio service. Pass `endpoint` and `token` for a shared
HTTP service. Wait durations and RPC timeouts are measured in seconds.

## Destination binding

::: molq.client.objects.submit

## Cluster decorator

Use `local = submit("name", project_root="/shared/project")` and `@local`. Yield a `JobSpec` to receive a
`Job`; the generator returns its own result. See the [Python guide](../api.md) for complete examples.

::: molq.client.objects.Cluster.__call__

::: molq.client.objects
    options:
      show_root_heading: false
      members: [Molq, ClusterRegistry, Cluster, Job, JobCollection]
