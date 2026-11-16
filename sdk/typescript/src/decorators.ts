/** Destination decorators over the public RPC client. */
import { Cluster, type Job, type JobCollection, Molq, MolqError } from "./index.js";
import type { ClusterDefinition, JobSpec } from "./types.js";
export type SubmitOptions = Partial<Omit<ClusterDefinition, "name" | "scheduler">> & {
    endpoint?: string;
    token?: string;
    timeout?: number;
    registryId?: string;
    projectRoot?: string;
};
/** Wrap a generator: each yielded JobSpec receives a Job or JobCollection. */
export type Destination = Cluster & {
    <Args extends unknown[], Result>(builder: (...args: Args) => Generator<JobSpec, Result, Job | JobCollection>): (...args: Args) => Promise<Result>;
    <Args extends unknown[], Result>(builder: (...args: Args) => Result): (...args: Args) => Promise<Awaited<Result>>;
};
function invalid(message: string): MolqError {
    return new MolqError({ kind: "INVALID_INPUT", message, outcome: "not_applied", context: {} });
}
/** Bind or select a destination; registration is explicit and never resent. */
export async function bindDestination(mq: Molq, name: string, kind?: ClusterDefinition["scheduler"] | "local", options: SubmitOptions = {}): Promise<Destination> {
    let cluster = mq.cluster(name, options.projectRoot);
    const { endpoint, token, timeout, registryId, projectRoot, ...definition } = options;
    try {
        const allowed = new Set(["transport", "defaults", "launcher", "pbs_dialect"]);
        if (Object.keys(definition).some(key => !allowed.has(key)))
            throw invalid("Unknown destination fields");
        if (kind && !["local", "shell", "slurm", "pbs", "lsf"].includes(kind))
            throw invalid("Unknown destination scheduler");
        if (Object.hasOwn(definition, "transport") && (!definition.transport || typeof definition.transport !== "object" || Array.isArray(definition.transport)))
            throw invalid("Destination transport must be an object");
        if (!kind && Object.keys(definition).length)
            throw invalid("Destination fields require cluster_type");
        if (kind) {
            const scheduler = kind === "local" ? "shell" : kind;
            const expected = { ...definition, scheduler, ...(kind === "local" && !definition.transport ? { transport: { kind: "local" as const } } : {}) };
            let existing: Record<string, unknown> | undefined;
            try {
                existing = await cluster.definition() as Record<string, unknown>;
            }
            catch (error) {
                if (!(error instanceof MolqError) || error.kind !== "CLUSTER_NOT_FOUND")
                    throw error;
            }
            if (existing) {
                for (const [key, value] of Object.entries(expected)) {
                    if (!equal(existing[key], value))
                        throw invalid(`Existing destination binding differs: ${key}`);
                }
            }
            else {
                const registered = await mq.clusters.register({ name, ...expected });
                cluster = mq.cluster(registered.name, projectRoot);
            }
        }
        const decorate = function <Args extends unknown[], Result>(builder: (...args: Args) => Generator<JobSpec, Result, Job | JobCollection>): (...args: Args) => Promise<Result> {
            if (typeof builder !== "function" || ["[object AsyncGeneratorFunction]", "[object AsyncFunction]"].includes(Object.prototype.toString.call(builder)))
                throw new TypeError("Decorate a synchronous generator function");
            return async function (this: unknown, ...args: Args): Promise<Result> {
                const generator = builder.apply(this, args);
                if (!generator || typeof generator.next !== "function")
                    return generator as unknown as Result;
                let failed = false;
                try {
                    let step = generator.next();
                    while (!step.done) {
                        const job = await cluster.submit(step.value);
                        step = generator.next(job);
                    }
                    return step.value;
                }
                catch (error) {
                    failed = true;
                    throw error;
                }
                finally {
                    try {
                        generator.return(undefined as Result);
                    }
                    catch (error) {
                        if (!failed)
                            throw error;
                    }
                }
            };
        };
        // The callable keeps Cluster's methods and one connection, without job state.
        return new Proxy(decorate, {
            get(target, property, receiver) {
                if (property in cluster) {
                    const value = Reflect.get(cluster, property);
                    return typeof value === "function" ? value.bind(cluster) : value;
                }
                return Reflect.get(target, property, receiver);
            },
        }) as Destination;
    }
    catch (error) {
        try {
            await mq.close();
        }
        catch { /* Preserve the original uncertain outcome. */ }
        throw error;
    }
}
function equal(left: unknown, right: unknown): boolean {
    if (left === right)
        return true;
    if (!left || !right || typeof left !== "object" || typeof right !== "object")
        return false;
    if (Array.isArray(left) !== Array.isArray(right))
        return false;
    const a = Object.keys(left), b = Object.keys(right);
    return a.length === b.length && a.every(key => Object.hasOwn(right, key) && equal((left as Record<string, unknown>)[key], (right as Record<string, unknown>)[key]));
}
