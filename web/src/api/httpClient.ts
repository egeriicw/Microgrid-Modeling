// ApiClient for the real API (contract: api/openapi.json, types in ./generated/openapi.ts).
// Auth is a bearer API key; the role is decided by the server. The key lives in memory and sessionStorage only
// (gone when the tab closes), never localStorage. All HTTP in the app goes through this file.
import type { components } from "./generated/openapi";
import { resultsFromApi, runEventFrom, runStatusFromApi } from "./mapping";
import { SseParser } from "./sse";
import { ApiError, type ApiClient, type FieldError, type RunEvent, type RunRequest, type Session } from "./types";

type Schemas = components["schemas"];

export const KEY_STORAGE = "microgrid.apiKey";
const API_KEY_PATTERN = /^[\x21-\x7e]+$/;
const BAD_KEY_MESSAGE =
  "That API key contains characters a key cannot have (spaces, line breaks or non-English letters). Copy the key again and paste only the key.";
/** The stream answered 200 but is not an event stream (e.g. a proxy or login page): retrying will not help. */
class NotAStream extends ApiError {}

export interface HttpClientOptions {
  /** Default `/api/v1` (same origin; nginx or the Vite dev server proxies it to the API). */
  baseUrl?: string;
  fetch?: typeof fetch;
  /** Where the key survives a reload. Default: window.sessionStorage. Pass null to keep it in memory only. */
  storage?: Storage | null;
  /** Base delay before an SSE reconnect (multiplied by the attempt number). */
  retryDelayMs?: number;
  /** Consecutive failed SSE connects before giving up. */
  maxStreamRetries?: number;
}

const SESSION: Session = { role: null, displayName: "API key" };

function messageFor(status: number, detail: unknown): { message: string; fieldErrors: FieldError[] } {
  // The server's 401 text ("authentication required") does not tell a person what to do.
  if (status === 401) return { message: "Your API key was not accepted. Check it and sign in again.", fieldErrors: [] };
  if (typeof detail === "string" && detail) return { message: detail, fieldErrors: [] };
  if (Array.isArray(detail)) {
    const fieldErrors: FieldError[] = detail
      .filter((d): d is { loc?: unknown; msg?: unknown } => typeof d === "object" && d !== null)
      .map((d) => ({
        field: Array.isArray(d.loc) ? d.loc.filter((p) => p !== "body").map(String).join(".") : "",
        message: String(d.msg ?? "invalid value"),
      }));
    if (fieldErrors.length > 0) {
      return { message: fieldErrors.map((f) => (f.field ? `${f.field}: ${f.message}` : f.message)).join("; "), fieldErrors };
    }
  }
  const generic: Record<number, string> = {
    403: "You do not have permission to do that.",
    404: "Not found.",
    422: "The request was not valid.",
    503: "The service is temporarily unavailable. Try again later.",
  };
  return { message: generic[status] ?? `The API answered with an error (HTTP ${status}).`, fieldErrors: [] };
}

async function errorFrom(res: Response): Promise<ApiError> {
  let detail: unknown;
  try {
    detail = ((await res.json()) as { detail?: unknown }).detail;
  } catch {
    detail = undefined;
  }
  const { message, fieldErrors } = messageFor(res.status, detail);
  return new ApiError(res.status, message, fieldErrors);
}

const isAbort = (e: unknown) => e instanceof DOMException && e.name === "AbortError";
const unreachable = () => new ApiError(0, "Cannot reach the API. Check that it is running and try again.");
const sleep = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve) => {
    const t = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => (clearTimeout(t), resolve()), { once: true });
  });

export class HttpApiClient implements ApiClient {
  readonly kind = "http" as const;
  private readonly base: string;
  private readonly doFetch: typeof fetch;
  private readonly storage: Storage | null;
  private readonly retryDelayMs: number;
  private readonly maxStreamRetries: number;
  private key: string | null;
  private readonly listeners = new Set<() => void>();

  constructor(opts: HttpClientOptions = {}) {
    this.base = (opts.baseUrl ?? "/api/v1").replace(/\/+$/, "");
    this.doFetch = opts.fetch ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
    this.storage = opts.storage === undefined ? (typeof sessionStorage === "undefined" ? null : sessionStorage) : opts.storage;
    this.retryDelayMs = opts.retryDelayMs ?? 1000;
    this.maxStreamRetries = opts.maxStreamRetries ?? 5;
    this.key = this.read();
  }

  // --- key storage -------------------------------------------------------------------------------------------

  private read(): string | null {
    try {
      return this.storage?.getItem(KEY_STORAGE) || null;
    } catch {
      return null; // storage blocked: memory only
    }
  }

  private write(key: string | null): void {
    this.key = key;
    try {
      if (key) this.storage?.setItem(KEY_STORAGE, key);
      else this.storage?.removeItem(KEY_STORAGE);
    } catch {
      /* storage blocked: the key lasts until reload */
    }
  }

  private signedOut(): void {
    const had = this.key !== null;
    this.write(null);
    if (had) for (const l of [...this.listeners]) l();
  }

  // --- HTTP --------------------------------------------------------------------------------------------------

  private async send(path: string, init: RequestInit = {}, key = this.key): Promise<Response> {
    if (!key) throw new ApiError(401, "Sign in first.");
    const headers = new Headers(init.headers);
    try {
      headers.set("Authorization", `Bearer ${key}`);
    } catch {
      // A stored key the runtime cannot send (the runtime's message may contain the key, so it is never shown).
      if (key === this.key) this.signedOut();
      throw new ApiError(401, BAD_KEY_MESSAGE);
    }
    if (init.body !== undefined) headers.set("Content-Type", "application/json");
    let res: Response;
    try {
      res = await this.doFetch(`${this.base}${path}`, { ...init, headers });
    } catch (e) {
      if (isAbort(e)) throw e;
      throw unreachable();
    }
    if (!res.ok) {
      const err = await errorFrom(res);
      if (res.status === 401 && key === this.key) this.signedOut();
      throw err;
    }
    return res;
  }

  private async json<T>(path: string, init?: RequestInit): Promise<T> {
    const res = await this.send(path, init);
    try {
      return (await res.json()) as T;
    } catch {
      throw new ApiError(res.status, "The API sent a response that could not be read.");
    }
  }

  private runPath(runId: string, suffix = ""): string {
    return `/runs/${encodeURIComponent(runId)}${suffix}`;
  }

  // --- ApiClient ---------------------------------------------------------------------------------------------

  async login(apiKey: string): Promise<Session> {
    const key = apiKey.trim();
    if (!key) throw new ApiError(401, "Paste your API key.");
    // Printable ASCII only: anything else cannot travel in an HTTP header. The message never repeats the key.
    if (!API_KEY_PATTERN.test(key)) throw new ApiError(400, BAD_KEY_MESSAGE);
    // Cheapest authenticated endpoint. It proves the key is valid; it does NOT report the role (API gap).
    await this.send("/demo-compositions", {}, key);
    this.write(key);
    return SESSION;
  }

  async logout(): Promise<void> {
    this.write(null);
  }

  async getSession(): Promise<Session | null> {
    this.key = this.read() ?? this.key;
    return this.key ? SESSION : null;
  }

  onSignedOut(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async listBuildingTypes(): Promise<never> {
    throw new ApiError(
      501,
      "The API does not offer the building-type list yet, so custom compositions cannot be built here. Use a predefined composition.",
    );
  }

  async listFilterOptions() {
    return []; // the API has no filter-options endpoint yet
  }

  async listPredefinedCompositions(): Promise<string[]> {
    return (await this.json<Schemas["DemoCompositions"]>("/demo-compositions")).items;
  }

  async startRun(request: RunRequest): Promise<{ runId: string }> {
    let body: Schemas["RunCreate"];
    if (request.kind === "predefined") body = { composition_id: request.compositionId };
    else if (request.kind === "replay") body = { config: request.config, seeds: request.seeds };
    else {
      throw new ApiError(
        501,
        "Custom compositions are not supported by the API yet: it needs a full scenario config, which the web app cannot build without the building-type list. Use a predefined composition.",
      );
    }
    const out = await this.json<Schemas["RunOut"]>("/runs", { method: "POST", body: JSON.stringify(body) });
    return { runId: out.id };
  }

  async getRun(runId: string) {
    return runStatusFromApi(await this.json<Schemas["RunDetail"]>(this.runPath(runId)));
  }

  async cancelRun(runId: string): Promise<void> {
    await this.json<Schemas["RunOut"]>(this.runPath(runId, "/cancel"), { method: "POST" });
  }

  async getResults(runId: string) {
    const doc = await this.json<unknown>(this.runPath(runId, "/results"));
    return resultsFromApi(runId, doc);
  }

  async watchRun(runId: string, onEvent: (e: RunEvent) => void, signal: AbortSignal): Promise<void> {
    let lastId: string | undefined;
    let failures = 0;
    while (!signal.aborted) {
      let finished = false;
      try {
        const headers: Record<string, string> = { Accept: "text/event-stream" };
        if (lastId !== undefined) headers["Last-Event-ID"] = lastId;
        const res = await this.send(this.runPath(runId, "/events"), { headers, signal });
        const type = (res.headers.get("content-type") ?? "").toLowerCase();
        if (!type.startsWith("text/event-stream")) {
          void res.body?.cancel().catch(() => undefined);
          throw new NotAStream(0, "The server did not answer with a progress stream (a proxy or login page may be in the way).");
        }
        if (!res.body) throw unreachable();
        const reader = res.body.getReader();
        try {
          const decoder = new TextDecoder();
          const parser = new SseParser();
          for (;;) {
            const { value, done } = await reader.read();
            const text = done ? decoder.decode() : decoder.decode(value, { stream: true });
            for (const m of parser.push(text)) {
              if (m.event !== "progress" && m.event !== "status") continue; // unknown events are ignored unparsed
              const ev = runEventFrom(m.event, m.data, m.id);
              if (!ev) continue;
              if (ev.type === "progress" && ev.id) lastId = ev.id;
              failures = 0;
              onEvent(ev);
              if (ev.type === "status") finished = true;
            }
            if (done || finished) break;
          }
        } finally {
          // Always release the connection: after the final status, a bad event, a throwing handler or an abort.
          void reader.cancel().catch(() => undefined);
        }
        if (finished) return;
        // The stream ended without a final status (proxy timeout, server restart): reconnect and resume.
      } catch (e) {
        if (signal.aborted || isAbort(e)) return;
        if (e instanceof NotAStream) throw e;
        if (e instanceof ApiError && e.status !== 0 && e.status < 500) throw e; // 401/403/404/422: retrying will not help
      }
      failures += 1;
      if (failures > this.maxStreamRetries) throw new ApiError(0, "Lost the connection to the progress stream.");
      await sleep(this.retryDelayMs * failures, signal);
    }
  }
}
