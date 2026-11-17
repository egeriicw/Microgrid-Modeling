import { beforeEach, describe, expect, it, vi } from "vitest";
import { HttpApiClient, KEY_STORAGE } from "./httpClient";
import { ApiError, type RunEvent } from "./types";

type Handler = (url: string, init: RequestInit) => Response | Promise<Response>;

const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });

function fakeFetch(handler: Handler) {
  const calls: { url: string; init: RequestInit }[] = [];
  const fn = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = String(input);
    calls.push({ url, init });
    return handler(url, init);
  });
  return { fn: fn as unknown as typeof fetch, calls };
}

const header = (init: RequestInit, name: string) => new Headers(init.headers).get(name);

function streamOf(chunks: string[], { fail = false } = {}): Response {
  const enc = new TextEncoder();
  let i = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(c) {
      if (i < chunks.length) c.enqueue(enc.encode(chunks[i++]));
      else if (fail) c.error(new TypeError("network error"));
      else c.close();
    },
  });
  return new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } });
}

const RUN_ID = "7c0f2a2e-1111-4222-8333-444455556666";
const SEEDS = { composition_seed: 11, archetype_seed: 22, perturbation_seed: 33 };
const runOut = (over: Record<string, unknown> = {}) => ({
  id: RUN_ID,
  status: "running",
  owner: "alice",
  seeds: SEEDS,
  engine_version: "0.3.0",
  progress: {},
  cancel_requested: false,
  error_message: null,
  created_at: "2026-11-17T00:00:00Z",
  started_at: null,
  finished_at: null,
  ...over,
});

async function signedIn(handler: Handler) {
  const f = fakeFetch((url, init) => (url.endsWith("/demo-compositions") ? json(200, { items: ["small"] }) : handler(url, init)));
  const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage, retryDelayMs: 1 });
  await c.login("secret-key");
  f.calls.length = 0;
  return { c, calls: f.calls };
}

beforeEach(() => {
  sessionStorage.clear();
  localStorage.clear();
});

describe("HttpApiClient: sign-in (FR-WEB-5)", () => {
  it("validates the key against an authenticated endpoint with a bearer header", async () => {
    const f = fakeFetch(() => json(200, { items: [] }));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    const s = await c.login("  secret-key  ");
    expect(f.calls[0].url).toBe("/api/v1/demo-compositions");
    expect(header(f.calls[0].init, "Authorization")).toBe("Bearer secret-key");
    expect(s.role).toBeNull(); // the API does not report the role; the client never invents one
  });

  it("keeps the key in sessionStorage only, never localStorage", async () => {
    const f = fakeFetch(() => json(200, { items: [] }));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    await c.login("secret-key");
    expect(sessionStorage.getItem(KEY_STORAGE)).toBe("secret-key");
    expect(JSON.stringify({ ...localStorage })).not.toContain("secret-key");
    expect(await c.getSession()).not.toBeNull();
    await c.logout();
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
    expect(await c.getSession()).toBeNull();
  });

  it("restores the session from sessionStorage after a reload", async () => {
    sessionStorage.setItem(KEY_STORAGE, "secret-key");
    const f = fakeFetch(() => json(200, runOut()));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    expect(await c.getSession()).not.toBeNull();
    await c.getRun(RUN_ID);
    expect(header(f.calls[0].init, "Authorization")).toBe("Bearer secret-key");
  });

  it("rejects a wrong key with 401 and stores nothing", async () => {
    const f = fakeFetch(() => json(401, { detail: "authentication required" }));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    await expect(c.login("bad")).rejects.toMatchObject({ status: 401, message: expect.stringMatching(/api key was not accepted/i) });
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
  });

  it("rejects an empty key without calling the API", async () => {
    const f = fakeFetch(() => json(200, {}));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    await expect(c.login("   ")).rejects.toBeInstanceOf(ApiError);
    expect(f.calls).toHaveLength(0);
  });

  it("signs out and notifies listeners when a later call gets 401", async () => {
    const { c } = await signedIn(() => json(401, { detail: "authentication required" }));
    const listener = vi.fn();
    c.onSignedOut(listener);
    await expect(c.getRun(RUN_ID)).rejects.toMatchObject({ status: 401 });
    expect(listener).toHaveBeenCalledTimes(1);
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
    expect(await c.getSession()).toBeNull();
  });

  it("calls without a key fail as 401 without a request", async () => {
    const f = fakeFetch(() => json(200, {}));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    await expect(c.getRun(RUN_ID)).rejects.toMatchObject({ status: 401 });
    expect(f.calls).toHaveLength(0);
  });
});

describe("HttpApiClient: error mapping", () => {
  it("surfaces 422 field messages from a validation error list", async () => {
    const { c } = await signedIn(() =>
      json(422, { detail: [{ loc: ["body", "seeds", "composition_seed"], msg: "Input should be a valid integer", type: "int_type" }] }),
    );
    const err = (await c.startRun({ kind: "predefined", compositionId: "x" }).catch((e: unknown) => e)) as ApiError;
    expect(err.status).toBe(422);
    expect(err.fieldErrors).toEqual([{ field: "seeds.composition_seed", message: "Input should be a valid integer" }]);
    expect(err.message).toContain("Input should be a valid integer");
  });

  it("uses the server's text for a 422 with a string detail", async () => {
    const { c } = await signedIn(() => json(422, { detail: "ensemble.max_runs exceeds the server cap of 500" }));
    const err = (await c.startRun({ kind: "predefined", compositionId: "x" }).catch((e: unknown) => e)) as ApiError;
    expect(err.message).toBe("ensemble.max_runs exceeds the server cap of 500");
    expect(err.fieldErrors).toEqual([]);
  });

  it.each([
    [403, "the demo role may only run a predefined composition with its fixed seed"],
    [404, "run not found"],
    [503, "the run queue is unavailable; try again later"],
  ])("maps %i to an ApiError carrying the server's message", async (status, detail) => {
    const { c } = await signedIn(() => json(status, { detail }));
    await expect(c.getRun(RUN_ID)).rejects.toMatchObject({ status, message: detail });
  });

  it("reports an unreachable API as status 0 with a plain message", async () => {
    const { c } = await signedIn(() => {
      throw new TypeError("Failed to fetch");
    });
    await expect(c.getRun(RUN_ID)).rejects.toMatchObject({ status: 0, message: expect.stringMatching(/cannot reach/i) });
  });

  it("falls back to a generic message when the error body is not JSON", async () => {
    const { c } = await signedIn(() => new Response("<html>bad gateway</html>", { status: 502 }));
    await expect(c.getRun(RUN_ID)).rejects.toMatchObject({ status: 502, message: expect.stringMatching(/502/) });
  });
});

describe("HttpApiClient: runs", () => {
  it("starts a predefined composition with composition_id only", async () => {
    const { c, calls } = await signedIn(() => json(202, runOut({ status: "queued" })));
    expect(await c.startRun({ kind: "predefined", compositionId: "small" })).toEqual({ runId: RUN_ID });
    expect(calls[0].url).toBe("/api/v1/runs");
    expect(calls[0].init.method).toBe("POST");
    expect(header(calls[0].init, "Content-Type")).toBe("application/json");
    expect(JSON.parse(String(calls[0].init.body))).toEqual({ composition_id: "small" });
  });

  it("replays a stored config with explicit seeds", async () => {
    const { c, calls } = await signedIn(() => json(202, runOut({ status: "queued" })));
    await c.startRun({ kind: "replay", config: { composition: { total_buildings: 3 } }, seeds: SEEDS });
    expect(JSON.parse(String(calls[0].init.body))).toEqual({ config: { composition: { total_buildings: 3 } }, seeds: SEEDS });
  });

  it("refuses a custom composition loudly instead of guessing a scenario config", async () => {
    const { c, calls } = await signedIn(() => json(500, {}));
    const req = { kind: "custom" as const, composition: { name: "x", totalBuildings: 1, entries: [], seeds: {}, maxRuns: 1 } };
    await expect(c.startRun(req)).rejects.toMatchObject({ status: 501 });
    expect(calls).toHaveLength(0);
  });

  it("lists predefined compositions", async () => {
    const { c } = await signedIn(() => json(200, {}));
    // signedIn answers /demo-compositions itself
    expect(await c.listPredefinedCompositions()).toEqual(["small"]);
  });

  it("maps a run detail to RunStatus: progress, seeds, config for replay", async () => {
    const detail = {
      ...runOut({
        progress: { stage: "run", run_index: 4, n_attempted: 5, n_feasible: 4, max_runs: 50, converged: null, message: "run 5 of 50" },
      }),
      composition_snapshot: { ensemble: { max_runs: 50 }, rng: SEEDS },
      result: null,
    };
    const { c, calls } = await signedIn(() => json(200, detail));
    const s = await c.getRun(RUN_ID);
    expect(calls[0].url).toBe(`/api/v1/runs/${RUN_ID}`);
    expect(s).toMatchObject({
      runId: RUN_ID,
      state: "running",
      stage: "run 5 of 50",
      completedRuns: 5,
      maxRuns: 50,
      feasible: 4,
      rejected: 1,
      converged: false,
      seeds: SEEDS,
      config: { ensemble: { max_runs: 50 }, rng: SEEDS },
      engineVersion: "0.3.0",
    });
  });

  it("takes max runs from the stored config before the first progress event", async () => {
    const detail = { ...runOut({ status: "queued" }), composition_snapshot: { ensemble: { max_runs: 20 } }, result: null };
    const { c } = await signedIn(() => json(200, detail));
    const s = await c.getRun(RUN_ID);
    expect(s).toMatchObject({ state: "queued", maxRuns: 20, completedRuns: 0, stage: "Queued" });
  });

  it("reports the server's error message for a failed run", async () => {
    const detail = { ...runOut({ status: "failed", error_message: "engine run failed (ValueError); see worker logs" }), composition_snapshot: {}, result: null };
    const { c } = await signedIn(() => json(200, detail));
    expect((await c.getRun(RUN_ID)).message).toBe("engine run failed (ValueError); see worker logs");
  });

  it("cancels with POST /runs/{id}/cancel", async () => {
    const { c, calls } = await signedIn(() => json(200, runOut({ cancel_requested: true })));
    await c.cancelRun(RUN_ID);
    expect(calls[0].url).toBe(`/api/v1/runs/${RUN_ID}/cancel`);
    expect(calls[0].init.method).toBe("POST");
  });

  it("encodes the run id in the path", async () => {
    const { c, calls } = await signedIn(() => json(404, { detail: "run not found" }));
    await c.getRun("../x?y").catch(() => undefined);
    expect(calls[0].url).toBe("/api/v1/runs/..%2Fx%3Fy");
  });
});

describe("HttpApiClient: progress stream (SSE over fetch)", () => {
  const progress = (id: number, n: number) =>
    `event: progress\r\nid: ${id}\r\ndata: ${JSON.stringify({ stage: "run", message: `run ${n}`, payload: { n_attempted: n, n_feasible: n, max_runs: 3 }, ts: "2026-11-17T00:00:00" })}\r\n\r\n`;

  it("sends the bearer header and parses events split across chunks, ignoring pings", async () => {
    const whole = `: ping\r\n\r\n${progress(1, 1)}${progress(2, 2)}event: status\r\ndata: {"status": "succeeded"}\r\n\r\n`;
    const chunks = [whole.slice(0, 7), whole.slice(7, 50), whole.slice(50, 51), whole.slice(51)];
    const { c, calls } = await signedIn(() => streamOf(chunks));
    const events: RunEvent[] = [];
    await c.watchRun(RUN_ID, (e) => events.push(e), new AbortController().signal);
    expect(calls[0].url).toBe(`/api/v1/runs/${RUN_ID}/events`);
    expect(header(calls[0].init, "Authorization")).toBe("Bearer secret-key");
    expect(header(calls[0].init, "Accept")).toBe("text/event-stream");
    expect(events).toEqual([
      { type: "progress", id: "1", stage: "run", message: "run 1", payload: { n_attempted: 1, n_feasible: 1, max_runs: 3 }, ts: "2026-11-17T00:00:00" },
      { type: "progress", id: "2", stage: "run", message: "run 2", payload: { n_attempted: 2, n_feasible: 2, max_runs: 3 }, ts: "2026-11-17T00:00:00" },
      { type: "status", status: "succeeded" },
    ]);
  });

  it("reconnects after a dropped connection, resuming with Last-Event-ID", async () => {
    let n = 0;
    const { c, calls } = await signedIn(() => {
      n += 1;
      return n === 1 ? streamOf([progress(7, 1)], { fail: true }) : streamOf([progress(8, 2), 'event: status\ndata: {"status":"cancelled"}\n\n']);
    });
    const events: RunEvent[] = [];
    await c.watchRun(RUN_ID, (e) => events.push(e), new AbortController().signal);
    expect(calls).toHaveLength(2);
    expect(header(calls[0].init, "Last-Event-ID")).toBeNull();
    expect(header(calls[1].init, "Last-Event-ID")).toBe("7");
    expect(events.map((e) => (e.type === "progress" ? e.id : e.status))).toEqual(["7", "8", "cancelled"]);
  });

  it("reconnects when the stream ends without a final status", async () => {
    let n = 0;
    const { c, calls } = await signedIn(() => {
      n += 1;
      return n === 1 ? streamOf([progress(3, 1)]) : streamOf(['event: status\ndata: {"status":"succeeded"}\n\n']);
    });
    await c.watchRun(RUN_ID, () => undefined, new AbortController().signal);
    expect(calls).toHaveLength(2);
    expect(header(calls[1].init, "Last-Event-ID")).toBe("3");
  });

  it("gives up after repeated failures and rejects", async () => {
    const { c, calls } = await signedIn(() => {
      throw new TypeError("Failed to fetch");
    });
    await expect(c.watchRun(RUN_ID, () => undefined, new AbortController().signal)).rejects.toMatchObject({ status: 0 });
    expect(calls.length).toBeGreaterThan(1);
  });

  it("does not retry a 404 and rejects with it", async () => {
    const { c, calls } = await signedIn(() => json(404, { detail: "run not found" }));
    await expect(c.watchRun(RUN_ID, () => undefined, new AbortController().signal)).rejects.toMatchObject({ status: 404 });
    expect(calls).toHaveLength(1);
  });

  it("stops quietly when aborted", async () => {
    const ctrl = new AbortController();
    const { c } = await signedIn((_url, init) => {
      return new Promise<Response>((_resolve, reject) => {
        init.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
      });
    });
    const p = c.watchRun(RUN_ID, () => undefined, ctrl.signal);
    ctrl.abort();
    await expect(p).resolves.toBeUndefined();
  });
});

describe("HttpApiClient: results", () => {
  const env = (base: number) => ({ mean_kw: [base, base + 1], p10_kw: [base - 1, base], p50_kw: [base, base + 1], p90_kw: [base + 1, base + 2] });
  const doc = (over: Record<string, unknown> = {}) => ({
    schema_version: "1.0",
    seeds: SEEDS,
    datasets: {},
    results: {
      n_runs: 2,
      time: { hourly: { start: "2018-01-01T00:00:00", step_minutes: 60, count: 2 }, "15min": { start: "2018-01-01T00:00:00", step_minutes: 15, count: 2 } },
      envelopes: {
        hourly: { total: env(100), residential: env(60), commercial: env(40) },
        "15min": { total: env(10), residential: env(6), commercial: env(4) },
      },
    },
    ...over,
  });

  it("maps the 15-minute envelopes into a LoadSeries (no temperature in the API results)", async () => {
    const { c, calls } = await signedIn(() => json(200, doc()));
    const r = await c.getResults(RUN_ID);
    expect(calls[0].url).toBe(`/api/v1/runs/${RUN_ID}/results`);
    expect(r.series.stepMs).toBe(15 * 60_000);
    expect(Array.from(r.series.ts)).toEqual([Date.UTC(2018, 0, 1), Date.UTC(2018, 0, 1, 0, 15)]);
    expect(Array.from(r.series.total)).toEqual([10, 11]);
    expect(Array.from(r.series.residential)).toEqual([6, 7]);
    expect(Array.from(r.series.commercial)).toEqual([4, 5]);
    expect(Array.from(r.series.p10)).toEqual([9, 10]);
    expect(Array.from(r.series.p50)).toEqual([10, 11]);
    expect(Array.from(r.series.p90)).toEqual([11, 12]);
    expect(r.series.temperature).toBeNull();
    expect(r.weatherYear).toBeNull();
    expect(r.calibrationStatus).toBeNull();
  });

  it("falls back to hourly when there is no 15-minute block", async () => {
    const d = doc();
    (d.results.time as Record<string, unknown>)["15min"] = null;
    (d.results.envelopes as Record<string, unknown>)["15min"] = null;
    const { c } = await signedIn(() => json(200, d));
    const r = await c.getResults(RUN_ID);
    expect(r.series.stepMs).toBe(3_600_000);
    expect(Array.from(r.series.total)).toEqual([100, 101]);
  });

  it("reads the weather year from the dataset ids when they agree", async () => {
    const datasets = {
      resstock: { dataset_id: "resstock:2025r1:amy2018:baseline:DC" },
      comstock: { dataset_id: "comstock:2025r3:amy2018:baseline:DC" },
    };
    const { c } = await signedIn(() => json(200, doc({ datasets })));
    expect((await c.getResults(RUN_ID)).weatherYear).toBe("AMY2018");
  });

  it("fails loudly when the run has no time-series results", async () => {
    const d = doc();
    d.results = { n_runs: 0, time: null, envelopes: null } as unknown as typeof d.results;
    const { c } = await signedIn(() => json(200, d));
    await expect(c.getResults(RUN_ID)).rejects.toThrow(/no feasible runs|no time series/i);
  });

  it("fails loudly on arrays whose length does not match the time axis", async () => {
    const d = doc();
    d.results.time["15min"].count = 3;
    const { c } = await signedIn(() => json(200, d));
    await expect(c.getResults(RUN_ID)).rejects.toThrow(/length/i);
  });
});
