/** Compile-time assertions for the callable destination API. */
import {submit, JobSpec, Job, type JobCollection, type JobSpec as RequestSpec} from "@molcrafts/molq";

async function types() {
  const local = await submit("dev", "local", {registry: "registry.db"});
  const hello = local(function* (name: string) {
    const spec: RequestSpec = JobSpec.argv(["echo", name]);
    const job = yield spec;
    return job;
  });
  const job: Job | JobCollection = await hello("molq");
  if (job instanceof Job) await job.wait(30);
  const array: JobCollection = await local.submit({
    ...JobSpec.argv(["true"]),
    scheduling: { array: { indices: [0, 1], max_concurrent: 1 } },
  });
  await array.status();
  // @ts-expect-error Decorated calls retain the builder's parameter types.
  await hello(123);
  const double = local((value: number) => value * 2);
  const number: number = await double(3);
  // @ts-expect-error Plain-function result types are preserved.
  const wrong: Job = await double(3);
  await local.close();
  await submit("hpc", {endpoint: "http://127.0.0.1:17891", token: "token"});
}
void types;
