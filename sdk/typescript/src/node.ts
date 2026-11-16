/** Native Node stdio transport calls Runtime, never another client. */
import { spawn, ChildProcessWithoutNullStreams } from "node:child_process";
import { Wire, MolqError, unwrap, isMutation, invalidInput, encodeRequest, decodeFrame } from "./wire.js";
import { Molq, HttpWire, bindDestination, type SubmitOptions, type Destination, type ClusterDefinition } from "./index.js";
export * from "./index.js";
export type NodeSubmitOptions = SubmitOptions & {
    registry?: string;
    config?: string;
    command?: string[];
};
/** Bind a callable destination; ordinary Node use starts temporary stdio lazily. */
export async function submit(name: string, kindOrOptions?: ClusterDefinition["scheduler"] | "local" | NodeSubmitOptions, options: NodeSubmitOptions = {}): Promise<Destination> {
    const kind = typeof kindOrOptions === "string" ? kindOrOptions : undefined;
    if (typeof kindOrOptions === "object")
        options = kindOrOptions;
    if (!options || typeof options !== "object" || Array.isArray(options))
        throw invalidInput("Submission options must be an object");
    const { registry, config, command, ...shared } = options;
    if (command !== undefined && (!Array.isArray(command) || !command.length || command.some(part => typeof part !== "string")))
        throw invalidInput("Expected a nonempty command argv");
    const argv = command ? [...command] : ["molq", "serve", "--stdio"];
    if (registry)
        argv.push("--registry", registry);
    if (config)
        argv.push("--config", config);
    const wire = shared.endpoint !== undefined ? new HttpWire(shared.endpoint, shared.token, shared.timeout) : new StdioWire(argv, shared.timeout);
    return bindDestination(new Molq(wire, shared.registryId), name, kind, shared);
}
export class StdioWire implements Wire {
    private process?: ChildProcessWithoutNullStreams;
    private sequence = 0;
    private pending = new Map<number, {
        resolve: (value: unknown) => void;
        reject: (error: unknown) => void;
        timer: ReturnType<typeof setTimeout>;
        mutation: boolean;
    }>();
    constructor(private command: string[] = ["molq", "serve", "--stdio"], private timeout = 130) {
        if (!command.length || command.some(part => typeof part !== "string") || !Number.isFinite(timeout) || timeout <= 0)
            throw new MolqError({ kind: "INVALID_INPUT", message: "Expected a command and finite positive timeout in seconds", outcome: "not_applied", context: {} });
    }
    private start(): void {
        if (this.process)
            return;
        this.process = spawn(this.command[0], this.command.slice(1), { stdio: ["pipe", "pipe", "pipe"] });
        this.process.stderr.pipe(process.stderr);
        const receive = (line: string) => {
            if (Buffer.byteLength(line) > 1048576) {
                this.process?.kill("SIGKILL");
                return;
            }
            try {
                const response = decodeFrame(line);
                // A startup failure has no request ID, but is authoritative for every
                // operation waiting for this one process to start.
                if (response.id === null && response.error) {
                    for (const [id, pending] of this.pending) {
                        clearTimeout(pending.timer);
                        this.pending.delete(id);
                        try {
                            unwrap(response, id);
                        }
                        catch (error) {
                            pending.reject(error instanceof MolqError ? error : this.connectionError(pending.mutation));
                        }
                    }
                    return;
                }
                const pending = this.pending.get(response.id);
                if (!pending)
                    return;
                clearTimeout(pending.timer);
                this.pending.delete(response.id);
                try {
                    pending.resolve(unwrap(response, response.id));
                }
                catch (error) {
                    pending.reject(error instanceof MolqError ? error : this.connectionError(pending.mutation));
                }
            }
            catch {
                this.process?.kill("SIGKILL");
            }
        };
        let buffered = Buffer.alloc(0);
        this.process.stdout.on("data", (chunk: Buffer) => {
            buffered = Buffer.concat([buffered, chunk]);
            let newline: number;
            while ((newline = buffered.indexOf(10)) >= 0) {
                const line = buffered.subarray(0, newline);
                buffered = buffered.subarray(newline + 1);
                if (line.length > 1048576) {
                    this.process?.kill("SIGKILL");
                    return;
                }
                try {
                    receive(new TextDecoder("utf-8", { fatal: true }).decode(line));
                }
                catch {
                    this.process?.kill("SIGKILL");
                    return;
                }
            }
            if (buffered.length > 1048576)
                this.process?.kill("SIGKILL");
        });
        const failed = () => { for (const pending of this.pending.values()) {
            clearTimeout(pending.timer);
            pending.reject(new MolqError({ kind: pending.mutation ? "OUTCOME_UNKNOWN" : "RUNTIME_UNAVAILABLE", message: "Runtime stdio closed", outcome: pending.mutation ? "unknown" : "not_applied", context: {} }));
        } this.pending.clear(); };
        this.process.on("error", failed);
        this.process.on("close", failed);
        this.process.stdin.on("error", failed);
    }
    private connectionError(mutation: boolean): MolqError {
        return new MolqError({ kind: mutation ? "OUTCOME_UNKNOWN" : "RUNTIME_UNAVAILABLE", message: "Invalid RPC response", outcome: mutation ? "unknown" : "not_applied", context: {} });
    }
    call<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
        const id = ++this.sequence;
        let payload: string;
        try {
            payload = encodeRequest(id, method, params);
        }
        catch (error) {
            return Promise.reject(error);
        }
        if (Buffer.byteLength(payload) > 1048576)
            return Promise.reject(new MolqError({ kind: "INVALID_INPUT", message: "RPC frame exceeds 1 MiB", outcome: "not_applied", context: {} }));
        this.start();
        if (this.process!.exitCode !== null || this.process!.signalCode !== null || this.process!.stdin.destroyed) {
            return Promise.reject(new MolqError({ kind: "RUNTIME_UNAVAILABLE", message: "RPC session is closed; close before reconnecting", outcome: "not_applied", context: {} }));
        }
        const mutation = isMutation(method);
        return new Promise<T>((resolve, reject) => {
            const timer = setTimeout(() => { this.pending.delete(id); reject(new MolqError({ kind: mutation ? "OUTCOME_UNKNOWN" : "RUNTIME_UNAVAILABLE", message: "RPC deadline", outcome: mutation ? "unknown" : "not_applied", context: {} })); this.process?.kill("SIGKILL"); }, this.timeout * 1000);
            this.pending.set(id, { resolve: value => resolve(value as T), reject, timer, mutation });
            this.process!.stdin.write(payload + "\n");
        });
    }
    async close(): Promise<void> {
        const child = this.process;
        if (!child)
            return;
        child.stdin.end();
        await new Promise<void>(resolve => { if (child.exitCode !== null || child.signalCode !== null) {
            resolve();
            return;
        } const timer = setTimeout(() => child.kill("SIGKILL"), this.timeout * 1000); child.once("close", () => { clearTimeout(timer); resolve(); }); });
        this.process = undefined;
    }
}
