# TypeScript

Use a destination as a function decorator. Yield `JobSpec`, receive `Job`, and
return your result. Node operations use `async/await`.

## Define and submit

Install molq so its executable is on your PATH. Then:

```typescript
import {submit, JobSpec} from "@molcrafts/molq";

const local = await submit("dev", "local");

const hello = local(function* (name: string) {
  const job = yield JobSpec.argv(["echo", `hello ${name}`]);
  return job;
});

try {
  const job = await hello("molq");
  await job.wait(30);
  console.log(JSON.stringify(await job.logs()));
} finally {
  await local.close();
}
```

`local(function* (...) {...})` decorates a top-level function. TypeScript's `@`
syntax applies to class members, so top-level functions use this form.

`submit("dev", "local")` binds a local destination. Jobs run in
`~/.molcrafts/molq/jobs` on the execution machine.
Later scripts select it with `submit("dev")`. Explicit bindings must match the
existing destination. Node starts a temporary service when needed.

Decoration preserves argument and return types and the function's `this`.
Each yield submits one allocation. Ordinary functions return normally without
submitting; failures propagate without automatic resend.

## HPC destinations

```typescript
const hpc = await submit("hpc", "slurm", {
  transport: {kind: "ssh", alias: "research"},
  launcher: "srun",
  defaults: {
    resources: {tasks: 1, cpus_per_task: 2},
    scheduling: {account: "your-project"},
  },
});
```

Choose your site's alias, root and account. Register once, then use
`submit("hpc")`. SSH destinations follow your OpenSSH configuration.

## Connection settings

Choose a separate registry or configuration file for Node:

```typescript
const local = await submit("dev", "local", {
  registry: "./registry.db",
  config: "./config.json",
});
```

For a shared service or a browser application, pass its connection settings:

```typescript
import {submit, JobSpec} from "@molcrafts/molq";

const hpc = await submit("hpc", {
  endpoint: "http://127.0.0.1:17891",
  token: "replace-with-your-own-token",
});

const hello = hpc(function* () {
  const job = yield JobSpec.argv(["echo", "hello"]);
  return job;
});

try {
  const job = await hello();
  await job.wait(30);
} finally {
  await hpc.close();
}
```

The browser entry requires a reachable shared service. Node supports stdio and
shared services with the same API; no separate HTTP factory is needed.

## Requests and handles

SDK calls are silent. Use `JSON.stringify(...)` when writing results to stdout.

`JobSpec.argv([...])` constructs a typed single-command specification. For an
execution plan, supply a full `JobSpec` object. Request fields use snake_case.

| Object | Operations |
|---|---|
| Destination | Decorate a function; `submit`, `validate`, `preview`, `list`, `history`, `observe`, `definition`, `update`, `remove`, `close` |
| `Job` | `status`, `wait`, `logs`, `cancel`, `requeue` |
| `JobCollection` | `status`, `observe`, `wait`, `cancel` |
| `Molq` | Registry, saved references, batches, direct RPC and connection lifetime |

All durations, including wait, RPC timeout and status `maxAge`, are **seconds**. Save
`job.ref` to return later. Array submissions return a `JobCollection` with
`refs` for its elements. Closing a connection leaves accepted jobs running.

`MolqError` carries structured error data. An unknown mutation outcome must be
reconciled before another submission. Repeat the original request with its
supplied request key to reconcile across serve restarts.
