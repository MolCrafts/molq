/** Wire framing only: no default merging, resource mapping or state machine. */
import type { ErrorData } from "./types.js";
export type { ErrorData } from "./types.js";
const mutations = new Set(["clusters.register", "clusters.update", "clusters.remove", "config.set", "config.reload", "presets.set", "jobs.submit", "jobs.cancel", "jobs.cancel_many", "jobs.purge", "files.transfer", "files.write", "files.mkdir", "files.remove", "files.rename", "files.touch"]);
export function isMutation(method: string): boolean { return mutations.has(method); }
export function invalidInput(message: string): MolqError { return new MolqError({ kind: "INVALID_INPUT", message, outcome: "not_applied", context: {} }); }
/** Encode strict JSON values; optional undefined object properties are omitted. */
export function encodeRequest(id: number, method: string, params: Record<string, unknown>): string {
    if (typeof method !== "string" || !method || !params || typeof params !== "object" || Array.isArray(params))
        throw invalidInput("RPC requires a method and named parameters");
    try {
        return JSON.stringify({ jsonrpc: "2.0", id, method, params }, function (_key, value: unknown) {
            if ((typeof value === "number" && !Number.isFinite(value)) || typeof value === "function" || typeof value === "symbol" || typeof value === "bigint")
                throw new Error("Unsupported JSON value");
            if (value && typeof value === "object" && Object.getOwnPropertySymbols(value).length)
                throw new Error("Symbol JSON key");
            return value;
        });
    }
    catch {
        throw invalidInput("RPC params must be finite JSON values");
    }
}
export class MolqError extends Error {
    constructor(public data: ErrorData) { super(data.message); this.name = "MolqError"; }
    get kind(): string { return this.data.kind; }
    get outcome(): ErrorData["outcome"] { return this.data.outcome; }
    get context(): ErrorData["context"] { return this.data.context; }
    toJSON(): ErrorData { return this.data; }
}
export interface Wire {
    call<T = unknown>(method: string, params?: Record<string, unknown>): Promise<T>;
    close(): Promise<void>;
}
/** Decode UTF-8 JSON without accepting numbers that overflow the wire model. */
export function decodeFrame(source: string): Record<string, any> {
    const value = JSON.parse(source, (_key, item: unknown) => {
        if (typeof item === "number" && !Number.isFinite(item)) throw new Error("Nonfinite JSON number");
        return item;
    });
    if (!value || typeof value !== "object" || Array.isArray(value) || value.jsonrpc !== "2.0") throw new Error("Invalid RPC envelope");
    return value;
}
export function unwrap<T>(response: any, id: number): T {
    if (!response || response.jsonrpc !== "2.0" || (response.id !== id && !(response.id === null && response.error)))
        throw new Error("Invalid RPC response identity");
    if ("method" in response || Object.hasOwn(response, "result") === Object.hasOwn(response, "error"))
        throw new Error("Invalid RPC response envelope");
    if (Object.hasOwn(response, "error")) {
        if (!response.error || typeof response.error !== "object" || Array.isArray(response.error)) throw new Error("Invalid RPC error envelope");
        const data = response.error.data;
        if (!Number.isInteger(response.error.code) || typeof response.error.message !== "string" || !data || typeof data.kind !== "string" || typeof data.message !== "string" || !["not_applied", "unknown", "applied"].includes(data.outcome) || !data.context || typeof data.context !== "object" || Array.isArray(data.context))
            throw new Error("Invalid RPC error envelope");
        throw new MolqError(data);
    }
    return response.result as T;
}
export class HttpWire implements Wire {
    private sequence = 0;
    private active = new Set<AbortController>();
    private subscriptions = new Set<WebSocket>();
    constructor(public endpoint: string, public token = "", public timeout = 130) {
        if (typeof endpoint !== "string" || !endpoint || /[\s\u0000-\u001f\u0085]/u.test(endpoint)) throw invalidInput("Endpoint must be a nonempty HTTP(S) URL");
        let parsed: URL;
        try { parsed = new URL(endpoint); } catch { throw invalidInput("Endpoint must be a valid HTTP(S) URL"); }
        if (!["http:", "https:"].includes(parsed.protocol) || !parsed.hostname) throw invalidInput("Endpoint must use HTTP(S)");
        if (typeof token !== "string" || /[\u0000-\u001f\u007f]/u.test(token)) throw invalidInput("Token must be a string");
        if (!Number.isFinite(timeout) || timeout <= 0)
            throw invalidInput("RPC timeout must be finite positive seconds");
    }
    async call<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
        const id = ++this.sequence;
        const data = encodeRequest(id, method, params);
        if (new TextEncoder().encode(data).length > 1048576)
            throw invalidInput("RPC frame exceeds 1 MiB");
        const mutation = isMutation(method);
        const controller = new AbortController();
        this.active.add(controller);
        const timer = setTimeout(() => controller.abort(), this.timeout * 1000);
        try {
            const response = await fetch(this.endpoint.replace(/\/$/, "") + "/rpc", { method: "POST", headers: { "Content-Type": "application/json", Authorization: "Bearer " + this.token }, body: data, signal: controller.signal });
            if (!response.body)
                throw new Error("Empty RPC response");
            const reader = response.body.getReader();
            const parts: Uint8Array[] = [];
            let size = 0;
            try {
                while (true) {
                    const { done, value } = await reader.read();
                    if (done)
                        break;
                    size += value.length;
                    if (size > 1048576)
                        throw new Error("RPC response exceeds 1 MiB");
                    parts.push(value);
                }
            }
            finally {
                try {
                    await reader.cancel();
                }
                catch { /* Keep the primary read error. */ }
            }
            const merged = new Uint8Array(size);
            let offset = 0;
            for (const part of parts) {
                merged.set(part, offset);
                offset += part.length;
            }
            return unwrap<T>(decodeFrame(new TextDecoder("utf-8", { fatal: true }).decode(merged)), id);
        }
        catch (error) {
            if (error instanceof MolqError)
                throw error;
            throw new MolqError({ kind: mutation ? "OUTCOME_UNKNOWN" : "RUNTIME_UNAVAILABLE", message: "RPC connection failed", outcome: mutation ? "unknown" : "not_applied", context: {} });
        }
        finally {
            clearTimeout(timer);
            this.active.delete(controller);
        }
    }
    async close(): Promise<void> { for (const controller of this.active)
        controller.abort(); for (const socket of this.subscriptions)
        socket.close(); this.subscriptions.clear(); }
    subscribe(callback: (event: unknown) => void, cursor?: string): WebSocket {
        const url = this.endpoint.replace(/^http/, "ws").replace(/\/$/, "") + "/rpc";
        const socket = new WebSocket(url);
        this.subscriptions.add(socket);
        socket.addEventListener("close", () => this.subscriptions.delete(socket));
        socket.addEventListener("open", () => { socket.send(JSON.stringify({ token: this.token })); socket.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "events.subscribe", params: cursor ? { cursor } : {} })); });
        socket.addEventListener("message", event => {
            if (typeof event.data !== "string" || new TextEncoder().encode(event.data).length > 1048576) {
                socket.close(1009, "Invalid RPC frame");
                return;
            }
            let value: unknown;
            try {
                value = decodeFrame(event.data);
            }
            catch {
                socket.close(1002, "Invalid RPC JSON");
                return;
            }
            callback(value);
        });
        return socket;
    }
}
