/** Equal OOP projections of molq's public RPC contract. */
import { HttpWire, Wire, MolqError } from "./wire.js";
import type { JobSpec as RequestSpec, JobRef, ClusterDefinition, ObservationResult, JobsListParams, JobsHistoryParams } from "./types.js";
export * from "./types.js";
export { HttpWire, MolqError };
export type { Wire };
export type Observation = ObservationResult;
export { bindDestination } from "./decorators.js";
export type { Destination, SubmitOptions } from "./decorators.js";
import { bindDestination, type SubmitOptions, type Destination } from "./decorators.js";
/** Construct one typed command request; allocation defaults remain in the service. */
export type JobSpec = RequestSpec;
export const JobSpec = {
    argv(argv: readonly string[], fields: Pick<RequestSpec, "resources" | "scheduling"> = {}): RequestSpec {
        return { ...fields, execution: { units: [{ id: "main", command: { kind: "argv", argv: [...argv] } }] } };
    },
};
/** Browser/shared-service destination binding. Node's entry also supports stdio. */
export async function submit(name: string, kindOrOptions?: ClusterDefinition["scheduler"] | "local" | SubmitOptions, options: SubmitOptions = {}): Promise<Destination> {
    const kind = typeof kindOrOptions === "string" ? kindOrOptions : undefined;
    if (typeof kindOrOptions === "object")
        options = kindOrOptions;
    if (!options || typeof options !== "object" || Array.isArray(options))
        throw new MolqError({ kind: "INVALID_INPUT", message: "Submission options must be an object", outcome: "not_applied", context: {} });
    if (!options.endpoint)
        throw new MolqError({ kind: "INVALID_INPUT", message: "Browser submission requires endpoint", outcome: "not_applied", context: {} });
    return bindDestination(new Molq(new HttpWire(options.endpoint, options.token, options.timeout), options.registryId), name, kind, options);
}
export class Molq {
    readonly clusters = new ClusterRegistry(this);
    private hello?: any;
    private connecting?: Promise<any>;
    constructor(public wire: Wire, public registryId?: string) {
        if (registryId !== undefined && (typeof registryId !== "string" || !registryId)) throw new MolqError({kind:"INVALID_INPUT",message:"Registry identity must be a nonempty string",outcome:"not_applied",context:{}});
    }
    /** Verify optional registry identity before business RPC. */
    async connect(): Promise<any> {
        if (!this.hello) {
            this.connecting ??= (async () => {
                const hello: any = await this.wire.call("molq.hello");
                if (!hello || typeof hello !== "object" || typeof hello.registry_id !== "string")
                    throw new MolqError({kind: "RUNTIME_UNAVAILABLE", message: "Invalid RPC handshake", outcome: "not_applied", context: {}});
                if (this.registryId && this.registryId !== hello.registry_id)
                    throw new MolqError({ kind: "REGISTRY_MISMATCH", message: "Registry identity mismatch", outcome: "not_applied", context: {} });
                this.hello = hello;
            })();
            try {
                await this.connecting;
            }
            finally {
                this.connecting = undefined;
            }
        }
        return this.hello;
    }
    async rpc<T = unknown>(method: string, params: Record<string, unknown> = {}): Promise<T> { await this.connect(); return this.wire.call<T>(method, params); }
    /** Select a registered destination without querying it. */
    cluster(name: string): Cluster { return new Cluster(this, name); }
    /** Reconstruct a handle from a copied, registry-qualified reference. */
    job(ref: JobRef): Job { return new Job(this, ref); }
    /** Group copied references for batch operations. */
    jobs(refs: JobRef[]): JobCollection { return new JobCollection(this, refs); }
    /** Release this connection without cancelling accepted allocations. */
    async close(): Promise<void> { try {
        await this.wire.close();
    }
    finally {
        this.hello = undefined;
        this.connecting = undefined;
    } }
}
export class ClusterRegistry {
    constructor(private molq: Molq) { }
    /** Register an explicit destination and return its stable-ID handle. */
    async register(definition: ClusterDefinition): Promise<Cluster> { const value = await this.molq.rpc<{
        id: string;
    }>("clusters.register", definition); return this.molq.cluster(value.id); }
    /** List results through the public RPC contract. */
    list(): Promise<unknown[]> { return this.molq.rpc("clusters.list"); }
}
export class Cluster {
    constructor(private molq: Molq, public name: string) { }
    /** Release this connection without cancelling accepted allocations. */
    close(): Promise<void> { return this.molq.close(); }
    /** Answer one bounded SSH authentication prompt; the response is never persisted. */
    authenticate(code: string): Promise<{ authenticated: boolean }> { return this.molq.rpc("connections.authenticate", { cluster: this.name, code }); }
    /** Read the destination definition and revision through RPC. */
    definition(): Promise<unknown> { return this.molq.rpc("clusters.get", { cluster: this.name }); }
    /** Replace a destination definition using its expected revision. */
    update(definition: ClusterDefinition, expectedRevision: number): Promise<unknown> { return this.molq.rpc("clusters.update", { cluster: this.name, definition, expected_revision: expectedRevision }); }
    /** Remove a destination definition without cancelling native jobs. */
    remove(expectedRevision: number): Promise<unknown> { return this.molq.rpc("clusters.remove", { cluster: this.name, expected_revision: expectedRevision }); }
    /** Copy a path from this destination to another with one rsync run; options pass to rsync unchanged. */
    async transfer(sourcePath: string, destination: Cluster, destinationPath: string, ...options: string[]): Promise<{ outcome: string; output: string }> {
        if (!(destination instanceof Cluster))
            throw new MolqError({ kind: "INVALID_INPUT", message: "Expected a Cluster destination", outcome: "not_applied", context: {} });
        return this.molq.rpc("files.transfer", {
            sources: [{ cluster: this.name, path: sourcePath }],
            destination: { cluster: destination.name, path: destinationPath },
            options,
        });
    }
    /** Submit one allocation or a native array. A supplied key survives restart. */
    submit(spec: JobSpec & { scheduling?: { array?: undefined } }, requestKey?: string): Promise<Job>;
    submit(spec: JobSpec & { scheduling: { array: NonNullable<NonNullable<JobSpec["scheduling"]>["array"]> } }, requestKey?: string): Promise<JobCollection>;
    submit(spec: JobSpec, requestKey?: string): Promise<Job | JobCollection>;
    async submit(spec: JobSpec, requestKey?: string): Promise<Job | JobCollection> {
        checkSpec(spec);
        const value = await this.molq.rpc<{ref: JobRef} | {refs: JobRef[]}>(
            "jobs.submit", { cluster: this.name, spec, ...(requestKey ? { request_key: requestKey } : {}) });
        return "refs" in value ? this.molq.jobs(value.refs) : this.molq.job(value.ref);
    }
    /** Validate schema and native representation without probing resources. */
    async validate(spec: RequestSpec): Promise<unknown> { checkSpec(spec); return this.molq.rpc("jobs.validate", { cluster: this.name, spec }); }
    /** Render one JobSpec without submitting an allocation. */
    async preview(spec: RequestSpec): Promise<unknown> { checkSpec(spec); return this.molq.rpc("jobs.preview", { cluster: this.name, spec }); }
    /** List results through the public RPC contract. */
    list(filters: Omit<JobsListParams, "cluster"> = {}): Promise<Observation> { return this.molq.rpc("jobs.list", { cluster: this.name, ...filters }); }
    /** Query native accounting history; coverage is returned with observations. */
    history(filters: Omit<Extract<JobsHistoryParams, {
        refs: unknown;
    }>, "cluster"> | Omit<Extract<JobsHistoryParams, {
        since?: unknown;
    }>, "cluster"> = {}): Promise<Observation> { return this.molq.rpc("jobs.history", { cluster: this.name, ...filters }); }
    /** Read a scoped observation and cursor from the service. */
    observe(cursor?: string): Promise<Observation> { return this.molq.rpc("jobs.observe", { cluster: this.name, ...(cursor ? { cursor } : {}) }); }
}
export class Job {
    readonly ref: JobRef;
    constructor(private molq: Molq, ref: JobRef) { this.ref = { ...ref }; }
    /** Read canonical snapshots with explicit consistency and freshness. */
    status(consistency: "live" | "bounded" | "cached" = "live", maxAge = 5): Promise<Observation> { return this.molq.rpc("jobs.get", { ref: this.ref, consistency, max_age: maxAge }); }
    /** Request native cancellation; acceptance does not confirm completion. */
    requeue(): Promise<unknown> { return this.molq.rpc("jobs.requeue", { ref: this.ref }); }
    cancel(requestKey?: string): Promise<unknown> { return this.molq.rpc("jobs.cancel", { ref: this.ref, ...(requestKey ? { request_key: requestKey } : {}) }); }
    /** Observe until terminal completion; timeout and interval are seconds. */
    wait(timeout = 3600, interval = 1): Promise<Observation> { return this.molq.jobs([this.ref]).wait(timeout, interval); }
    /** Read bounded logs as structured text and offset fields. */
    logs(stream: "stdout" | "stderr" = "stdout", offset = 0, limit = 262144): Promise<{
        text: string;
        next_offset: number;
    }> { return this.molq.rpc("logs.read", { ref: this.ref, stream, offset, limit }); }
}
export class JobCollection {
    readonly refs: JobRef[];
    constructor(private molq: Molq, refs: JobRef[]) { this.refs = refs.map(ref => ({ ...ref })); }
    /** Read canonical snapshots with explicit consistency and freshness. */
    status(consistency: "live" | "bounded" | "cached" = "live", maxAge = 5): Promise<Observation> { return this.molq.rpc("jobs.get_many", { refs: this.refs, consistency, max_age: maxAge }); }
    /** Request native cancellation; acceptance does not confirm completion. */
    cancel(): Promise<unknown> { return this.molq.rpc("jobs.cancel_many", { refs: this.refs }); }
    /** Read a scoped observation and cursor from the service. */
    observe(cursor?: string): Promise<Observation> { return this.molq.rpc("jobs.observe", { refs: this.refs, ...(cursor ? { cursor } : {}) }); }
    /** Observe until terminal completion; timeout and interval are seconds. */
    async wait(timeout = 3600, interval = 1): Promise<Observation> {
        if (!Number.isFinite(timeout) || !Number.isFinite(interval) || timeout <= 0 || interval <= 0)
            throw new MolqError({ kind: "INVALID_INPUT", message: "wait timeout/interval must be finite and positive", outcome: "not_applied", context: {} });
        const deadline = performance.now() + timeout * 1000;
        let cursor: string | undefined;
        while (true) {
            const result = await this.observe(cursor);
            cursor = result.cursor;
            if (result.completion.all_confirmed_terminal)
                return result;
            const remaining = deadline - performance.now();
            if (remaining <= 0)
                throw new MolqError({ kind: "WAIT_TIMEOUT", message: "Foreground wait deadline reached", outcome: "not_applied", context: {} });
            await new Promise(resolve => setTimeout(resolve, Math.min(interval * 1000, remaining)));
        }
    }
}
function checkSpec(spec: RequestSpec): void {
    if (!spec || typeof spec !== "object" || Array.isArray(spec) || !("execution" in spec)) {
        throw new MolqError({ kind: "INVALID_INPUT", message: "Expected a JobSpec request", outcome: "not_applied", context: {} });
    }
}
