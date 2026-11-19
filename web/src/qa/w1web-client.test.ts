// QA review of W1-WEB: the real HTTP client (key handling, SSE over fetch, error mapping, env selection).
// Tests marked `it.fails` document a defect: they pass while the defect exists and flip to a failure when it is
// fixed (then change them to `it`).
import { beforeEach, describe, expect, it, vi } from "vitest";
import { HttpApiClient, KEY_STORAGE } from "../api/httpClient";
import { resolveApiConfig } from "../api/select";
import { SseParser } from "../api/sse";
import { ApiError, type RunEvent } from "../api/types";
import pkg from "../../package.json";

type Handler = (url: string, init: RequestInit, n: number) => Response | Promise<Response>;

const KEY = "client-key-alice-0123456789";
const RUN_ID = "7c0f2a2e-1111-4222-8333-444455556666";
const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const hdr = (init: RequestInit, name: string) => new Headers(init.headers).get(name);
/** A `GET /me` body (sign-in) that also answers list calls (`items`). */
const ME_OK = {
  subject: "alice",
  role: "client",
  permissions: { "filters.use": false },
  run_inputs: ["composition", "composition_id", "config"],
  may_set_seeds: true,
  limits: { max_total_buildings: 5000, max_ensemble_runs: 1000, default_min_runs: 20, demo_caps: null },
  items: [] as string[],
};

function fakeFetch(handler: Handler) {
  const calls: { url: string; init: RequestInit }[] = [];
  const fn = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    calls.push({ url: String(input), init });
    return handler(String(input), init, calls.length);
  });
  return { fn: fn as unknown as typeof fetch, calls };
}

/** A body stream that records whether the client cancelled it (a leaked reader keeps the connection open). */
function trackedStream(chunks: (string | Uint8Array)[], { stayOpen = false, signal }: { stayOpen?: boolean; signal?: AbortSignal | null } = {}) {
  const enc = new TextEncoder();
  const state = { cancelled: false };
  let i = 0;
  const body = new ReadableStream<Uint8Array>({
    start(c) {
      // like a browser fetch: aborting the request errors the body
      signal?.addEventListener("abort", () => c.error(new DOMException("aborted", "AbortError")), { once: true });
    },
    pull(c) {
      if (i < chunks.length) {
        const ch = chunks[i++];
        c.enqueue(typeof ch === "string" ? enc.encode(ch) : ch);
      } else if (!stayOpen) c.close();
      else return new Promise<void>(() => undefined); // like a live SSE connection: no more bytes, never closes
    },
    cancel() {
      state.cancelled = true;
    },
  });
  return { res: new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } }), state };
}
const sse = (chunks: string[]) => trackedStream(chunks).res;

const progress = (id: number, n: number, extra: Record<string, unknown> = {}) =>
  `event: progress\nid: ${id}\ndata: ${JSON.stringify({ stage: "running", message: `run ${n}/3 feasible`, payload: { n_attempted: n, n_feasible: n, max_runs: 3, ...extra }, ts: "2026-11-17T00:00:00" })}\n\n`;
const status = (s: string) => `event: status\ndata: {"status": "${s}"}\n\n`;

async function signedIn(handler: Handler, opts: { retryDelayMs?: number; maxStreamRetries?: number } = {}) {
  let loggedIn = false;
  const f = fakeFetch((url, init, n) => (!loggedIn ? json(200, { ...ME_OK, items: ["small"] }) : handler(url, init, n)));
  const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage, retryDelayMs: 1, ...opts });
  await c.login(KEY);
  loggedIn = true;
  f.calls.length = 0;
  return { c, calls: f.calls };
}

const collect = async (c: HttpApiClient, signal = new AbortController().signal) => {
  const events: RunEvent[] = [];
  await c.watchRun(RUN_ID, (e) => events.push(e), signal);
  return events;
};

beforeEach(() => {
  sessionStorage.clear();
  localStorage.clear();
});

// ------------------------------------------------------------------------------------------------------------------
describe("QA W1-WEB: API key handling", () => {
  it("every authenticated call carries exactly `Authorization: Bearer <key>` (login, list, run, cancel, results, events)", async () => {
    const { c, calls } = await signedIn((url) => {
      if (url.endsWith("/events")) return sse([status("succeeded")]);
      if (url.endsWith("/results")) return json(404, { detail: "results not available" });
      if (url.endsWith("/runs")) return json(202, { id: RUN_ID });
      if (url.endsWith("/demo-compositions")) return json(200, ME_OK);
      return json(200, { id: RUN_ID, status: "running", owner: "a", seeds: {}, engine_version: "x", progress: {}, cancel_requested: false, error_message: null, created_at: "", started_at: null, finished_at: null, composition_snapshot: {}, result: null });
    });
    await c.listPredefinedCompositions();
    await c.startRun({ kind: "predefined", compositionId: "small" });
    await c.getRun(RUN_ID);
    await c.cancelRun(RUN_ID);
    await c.getResults(RUN_ID).catch(() => undefined);
    await collect(c);
    expect(calls).toHaveLength(6);
    for (const call of calls) expect(hdr(call.init, "Authorization")).toBe(`Bearer ${KEY}`);
  });

  it("the key never goes into a URL, localStorage, or an error message", async () => {
    const { c, calls } = await signedIn(() => json(500, { detail: "boom" }));
    const err = await c.getRun(RUN_ID).catch((e: unknown) => e);
    for (const call of calls) expect(call.url).not.toContain(KEY);
    expect(String((err as Error).message)).not.toContain(KEY);
    expect(JSON.stringify({ ...localStorage })).not.toContain(KEY);
    expect(sessionStorage.getItem(KEY_STORAGE)).toBe(KEY);
  });

  it("stores the key in sessionStorage by default (not localStorage) when no storage option is given", async () => {
    const f = fakeFetch(() => json(200, ME_OK));
    const c = new HttpApiClient({ fetch: f.fn });
    await c.login(KEY);
    expect(sessionStorage.getItem(KEY_STORAGE)).toBe(KEY);
    expect(localStorage.length).toBe(0);
  });

  it("a 401 on any call removes the key from memory and sessionStorage and notifies once", async () => {
    const { c, calls } = await signedIn(() => json(401, { detail: "authentication required" }));
    const onOut = vi.fn();
    c.onSignedOut(onOut);
    await expect(c.listPredefinedCompositions()).rejects.toMatchObject({ status: 401 });
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
    expect(await c.getSession()).toBeNull();
    expect(onOut).toHaveBeenCalledTimes(1);
    // nothing is sent with the old key afterwards
    await expect(c.getRun(RUN_ID)).rejects.toMatchObject({ status: 401 });
    expect(calls).toHaveLength(1);
  });

  it("a 401 on the progress stream signs out and does not retry", async () => {
    const { c, calls } = await signedIn(() => json(401, { detail: "authentication required" }));
    await expect(collect(c)).rejects.toMatchObject({ status: 401 });
    expect(calls).toHaveLength(1);
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
  });

  it("sign-out removes the key everywhere and later calls send nothing", async () => {
    const { c, calls } = await signedIn(() => json(200, ME_OK));
    await c.logout();
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
    expect(await c.getSession()).toBeNull();
    await expect(c.listPredefinedCompositions()).rejects.toBeInstanceOf(ApiError);
    expect(calls).toHaveLength(0);
  });

  it("a late 401 for a request sent with an OLD key does not sign out the new key", async () => {
    let release!: (r: Response) => void;
    const f = fakeFetch((url, init) =>
      hdr(init, "Authorization") === "Bearer old-key-0123456789abcd" && url.endsWith("/runs/" + RUN_ID)
        ? new Promise<Response>((r) => (release = r))
        : json(200, ME_OK),
    );
    const c = new HttpApiClient({ fetch: f.fn, storage: sessionStorage });
    await c.login("old-key-0123456789abcd");
    const pending = c.getRun(RUN_ID).catch(() => undefined);
    await c.login("new-key-0123456789abcd");
    release(json(401, { detail: "authentication required" }));
    await pending;
    expect(sessionStorage.getItem(KEY_STORAGE)).toBe("new-key-0123456789abcd");
  });

  it("DEFECT: a key the runtime cannot put in a header fails as a plain sign-in error that does not echo the key", async () => {
    const f = fakeFetch(() => json(200, ME_OK));
    const c = new HttpApiClient({ fetch: f.fn, storage: sessionStorage });
    const bad = "abc\ndef-secret-0123456789";
    const err = await c.login(bad).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(Error);
    expect(String((err as Error).message)).not.toContain("def-secret");
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
  });

  it("a failed sign-in (API down or 5xx) stores nothing", async () => {
    for (const h of [() => Promise.reject(new TypeError("Failed to fetch")), () => json(503, { detail: "x" })]) {
      const c = new HttpApiClient({ fetch: fakeFetch(h as Handler).fn, storage: sessionStorage });
      await expect(c.login(KEY)).rejects.toBeInstanceOf(ApiError);
      expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
    }
  });

  it("the real client never invents a role: it is GET /me's role, and an answer without one is refused", async () => {
    const f = fakeFetch((url) => (url.endsWith("/me") ? json(200, { ...ME_OK, role: "demo" }) : json(200, { ...ME_OK, role: "admin" })));
    const c = new HttpApiClient({ fetch: f.fn, storage: sessionStorage });
    expect((await c.login(KEY)).role).toBe("demo");
    expect((await c.getSession())?.role).toBe("demo");
    const noRole = new HttpApiClient({ fetch: fakeFetch(() => json(200, { items: [] })).fn, storage: sessionStorage });
    sessionStorage.clear();
    await expect(noRole.login(KEY)).rejects.toBeInstanceOf(ApiError);
  });
});

// ------------------------------------------------------------------------------------------------------------------
describe("QA W1-WEB: mock mode can only be selected by exactly VITE_API_MODE=mock", () => {
  it.each(["MOCK", "Mock", " mock", "mock ", "mocks", "true", "1", "", undefined])("%j selects the real client", (v) => {
    expect(resolveApiConfig({ VITE_API_MODE: v }).mode).toBe("http");
  });
  it("the default base URL is same-origin and relative (CSP connect-src 'self')", () => {
    expect(resolveApiConfig({}).baseUrl).toBe("/api/v1");
  });
});

// ------------------------------------------------------------------------------------------------------------------
describe("QA W1-WEB: SSE parser edge cases", () => {
  it("ignores retry: fields and unknown fields without breaking the event", () => {
    const p = new SseParser();
    expect(p.push('retry: 3000\nevent: status\nfoo: bar\ndata: {"status":"succeeded"}\n\n')).toEqual([
      { event: "status", data: '{"status":"succeeded"}' },
    ]);
  });
  it("keeps 'data:' with no space and an empty data line", () => {
    const p = new SseParser();
    expect(p.push("data:a\ndata\ndata: b\n\n")).toEqual([{ event: "message", data: "a\n\nb" }]);
  });
  it("does not carry an id or event name from a comment-only block into the next message", () => {
    const p = new SseParser();
    expect(p.push(": ping - 2026-11-17\r\n\r\nevent: x\nid: 4\n\n" + "data: y\n\n")).toEqual([{ event: "message", data: "y" }]);
  });
});

describe("QA W1-WEB: progress stream (SSE over fetch) edge cases", () => {
  it("decodes a multi-byte character split across two chunks", async () => {
    const whole = new TextEncoder().encode(progress(1, 1).replace("feasible", "feasible — ok é"));
    const cut = whole.indexOf(0xe2) + 1; // inside the 3-byte em dash
    const { c } = await signedIn((_u, _i, n) => (n === 1 ? trackedStream([whole.slice(0, cut), whole.slice(cut), status("succeeded")]).res : sse([])));
    const events = await collect(c);
    expect(events[0]).toMatchObject({ type: "progress", message: "run 1/3 feasible — ok é" });
  });

  it("joins an event whose JSON is spread over several data: lines", async () => {
    const { c } = await signedIn(() => sse(['event: status\ndata: {"status":\ndata:  "cancelled"}\n\n']));
    expect(await collect(c)).toEqual([{ type: "status", status: "cancelled" }]);
  });

  it("handles comment pings and retry: lines between events", async () => {
    const { c } = await signedIn(() => sse([": ping - 2026\n\nretry: 15000\n\n", progress(1, 1), ": ping\n\n", status("succeeded")]));
    expect((await collect(c)).map((e) => e.type)).toEqual(["progress", "status"]);
  });

  it("Last-Event-ID is the last PROGRESS id seen, across several reconnects", async () => {
    const { c, calls } = await signedIn((_u, _i, n) =>
      n === 1 ? sse([progress(4, 1)]) : n === 2 ? sse([": ping\n\n"]) : n === 3 ? sse([progress(9, 2)]) : sse([status("succeeded")]),
    );
    await collect(c);
    expect(calls.map((x) => hdr(x.init, "Last-Event-ID"))).toEqual([null, "4", "4", "9"]);
  });

  it("an HTTP 200 HTML page (proxy error) ends in a rejection, not a hang", async () => {
    const html = () => new Response("<html><body>502 Bad Gateway: upstream</body></html>", { status: 200, headers: { "content-type": "text/html" } });
    const { c, calls } = await signedIn(html, { maxStreamRetries: 3 });
    await expect(collect(c)).rejects.toBeInstanceOf(ApiError);
    // Was 4 (capped retries); fix D4 makes a non-event-stream 200 fail at once, so a single attempt.
    expect(calls).toHaveLength(1);
  });

  it("DEFECT: a 200 response that is not text/event-stream is rejected at once instead of retried", async () => {
    const html = () => new Response("<html>login page</html>", { status: 200, headers: { "content-type": "text/html" } });
    const { c, calls } = await signedIn(html, { maxStreamRetries: 3 });
    await expect(collect(c)).rejects.toBeInstanceOf(ApiError);
    expect(calls).toHaveLength(1);
  });

  it("5xx answers are retried with growing delays and a cap (no reconnect storm)", async () => {
    const times: number[] = [];
    const { c, calls } = await signedIn(
      () => {
        times.push(performance.now());
        return json(502, { detail: "bad gateway" });
      },
      { retryDelayMs: 20, maxStreamRetries: 3 },
    );
    await expect(collect(c)).rejects.toMatchObject({ status: 0 });
    expect(calls).toHaveLength(4);
    const gaps = times.slice(1).map((t, i) => t - times[i]);
    expect(gaps[0]).toBeGreaterThanOrEqual(15);
    expect(gaps[2]).toBeGreaterThan(gaps[0]);
  });

  it("aborting while the stream is open resolves and cancels the response body (no leaked connection)", async () => {
    let t!: ReturnType<typeof trackedStream>;
    const { c } = await signedIn((_u, init) => (t = trackedStream([progress(1, 1)], { stayOpen: true, signal: init.signal })).res);
    const ctrl = new AbortController();
    const seen: RunEvent[] = [];
    const p = c.watchRun(RUN_ID, (e) => seen.push(e), ctrl.signal);
    await vi.waitFor(() => expect(seen).toHaveLength(1));
    ctrl.abort();
    await expect(p).resolves.toBeUndefined();
    expect(t.res.bodyUsed).toBe(true); // the abort reached the request the client made (its signal was passed on)
  });

  it("after the final status event the response body is cancelled", async () => {
    const t = trackedStream([status("succeeded")], { stayOpen: true });
    const { c } = await signedIn(() => t.res);
    await collect(c);
    await vi.waitFor(() => expect(t.state.cancelled).toBe(true));
  });

  it("DEFECT: a malformed event cancels the open stream before reconnecting (otherwise each retry leaks a connection)", async () => {
    const streams: ReturnType<typeof trackedStream>[] = [];
    const { c } = await signedIn((_u, _i, n) => {
      const t = n === 1 ? trackedStream(["event: progress\nid: 1\ndata: {not json\n\n"], { stayOpen: true }) : trackedStream([status("succeeded")]);
      streams.push(t);
      return t.res;
    });
    await collect(c);
    expect(streams[0].state.cancelled).toBe(true);
  });

  it("DEFECT: an unknown event type with non-JSON data is ignored, not treated as a broken stream", async () => {
    const { c, calls } = await signedIn(() => sse(["event: heartbeat\ndata: tick\n\n", status("succeeded")]));
    expect(await collect(c)).toEqual([{ type: "status", status: "succeeded" }]);
    expect(calls).toHaveLength(1);
  });

  it("two clients (two tabs) keep separate keys and separate streams", async () => {
    const s1 = new Map<string, string>();
    const s2 = new Map<string, string>();
    const store = (m: Map<string, string>) =>
      ({ getItem: (k: string) => m.get(k) ?? null, setItem: (k: string, v: string) => void m.set(k, v), removeItem: (k: string) => void m.delete(k) }) as unknown as Storage;
    const f = fakeFetch((url) => (url.endsWith("/events") ? sse([status("succeeded")]) : json(200, ME_OK)));
    const a = new HttpApiClient({ fetch: f.fn, storage: store(s1) });
    const b = new HttpApiClient({ fetch: f.fn, storage: store(s2) });
    await a.login("key-a-0123456789abcdef");
    await b.login("key-b-0123456789abcdef");
    await a.logout();
    expect(await a.getSession()).toBeNull();
    expect(await b.getSession()).not.toBeNull();
    f.calls.length = 0;
    await b.watchRun(RUN_ID, () => undefined, new AbortController().signal);
    expect(hdr(f.calls[0].init, "Authorization")).toBe("Bearer key-b-0123456789abcdef");
  });
});

// ------------------------------------------------------------------------------------------------------------------
describe("QA W1-WEB: error mapping against real FastAPI bodies", () => {
  it("422 with nested loc lists every field, dropping only the leading 'body'", async () => {
    const detail = [
      { type: "int_type", loc: ["body", "seeds", "composition_seed"], msg: "Input should be a valid integer", input: "x" },
      { type: "value_error", loc: ["body"], msg: "Value error, send exactly one of 'config' or 'composition_id'", input: {} },
    ];
    const { c } = await signedIn(() => json(422, { detail }));
    const err = (await c.startRun({ kind: "predefined", compositionId: "x" }).catch((e: unknown) => e)) as ApiError;
    expect(err.fieldErrors).toEqual([
      { field: "seeds.composition_seed", message: "Input should be a valid integer" },
      { field: "", message: "Value error, send exactly one of 'config' or 'composition_id'" },
    ]);
    expect(err.message).not.toContain('"x"'); // the rejected input value is not echoed
  });

  it("a 500 with an HTML body gives a plain generic message without the HTML", async () => {
    const { c } = await signedIn(() => new Response("<html><h1>Internal Server Error</h1></html>", { status: 500, headers: { "content-type": "text/html" } }));
    const err = (await c.getRun(RUN_ID).catch((e: unknown) => e)) as ApiError;
    expect(err.status).toBe(500);
    expect(err.message).toBe("The API answered with an error (HTTP 500).");
  });

  it("a 202 from POST /runs is success (the API answers 202 Accepted)", async () => {
    const { c } = await signedIn(() => json(202, { id: RUN_ID }));
    await expect(c.startRun({ kind: "predefined", compositionId: "small" })).resolves.toEqual({ runId: RUN_ID });
  });

  it("the stream's 422 (bad Last-Event-ID) is not retried", async () => {
    const { c, calls } = await signedIn(() => json(422, { detail: "Last-Event-ID must be an integer event id" }));
    await expect(collect(c)).rejects.toMatchObject({ status: 422 });
    expect(calls).toHaveLength(1);
  });
});

// ------------------------------------------------------------------------------------------------------------------
describe("QA W1-WEB: generated types staleness guard is wired into npm test", () => {
  it("`npm test` runs `check:api` before vitest", () => {
    expect(pkg.scripts.test).toMatch(/^npm run check:api && /);
    expect(pkg.scripts["check:api"]).toContain("--check");
  });
});
