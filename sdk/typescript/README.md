# @molcrafts/molq 0.9.0

Decorate a generator, yield `JobSpec`, and receive its submitted `Job`.

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

Install `molcrafts-molq` so the `molq` executable is on PATH. Node starts a
temporary service; jobs run in `~/.molcrafts/molq/jobs` on the execution
machine. Use `submit("dev")` to reuse a registered destination. Pass `registry`
and `config` when you need separate paths.

For a browser or shared service, use the same entry with explicit settings:

```typescript
const hpc = await submit("hpc", {endpoint: "http://127.0.0.1:17891", token});
```

Function decorators preserve parameter/return types and `this`. Each yield submits
one allocation; errors propagate without automatic resend. TypeScript's `@`
syntax applies to class members, so top-level functions use `local(function* ...)`.

`JobSpec.argv` constructs a typed request; full JobSpec objects support resources,
scheduling and execution plans. All durations are seconds. Closing a
connection leaves accepted jobs running; save `job.ref` to return later.

[Manual](https://docs.molcrafts.org/molq/reference/typescript/)

Run `npm ci`, `npm test`, and `npm pack --dry-run`. The SDK contains no Scheduler,
SSH or SQLite implementation. Local Shell execution requires POSIX/Bash.
