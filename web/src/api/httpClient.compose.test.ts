// W1-WEB2: the real client learns the session from GET /me, lists building types, sends a `composition`, and maps
// data_files, the engine-reported convergence rule and the results' temperature block.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { HttpApiClient, KEY_STORAGE } from "./httpClient";
import { ApiError, type Composition } from "./types";

type Handler = (url: string, init: RequestInit) => Response | Promise<Response>;
const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

const ME_CLIENT = {
  subject: "alice",
  role: "client",
  permissions: { "filters.use": false },
  run_inputs: ["composition", "composition_id", "config"],
  may_set_seeds: true,
  limits: { max_total_buildings: 5000, max_ensemble_runs: 1000, default_min_runs: 20, demo_caps: null },
};
const ME_DEMO = {
  ...ME_CLIENT,
  subject: "demo",
  role: "demo",
  run_inputs: ["composition_id"],
  may_set_seeds: false,
  limits: { ...ME_CLIENT.limits, demo_caps: { max_buildings: 50, max_runs: 20 } },
};

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
const body = (init: RequestInit) => JSON.parse(String(init.body)) as Record<string, unknown>;

async function signedIn(handler: Handler, me: unknown = ME_CLIENT) {
  const f = fakeFetch((url, init) => (url.endsWith("/me") ? json(200, me) : handler(url, init)));
  const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage, retryDelayMs: 1 });
  await c.login("secret-key-0123456789");
  f.calls.length = 0;
  return { c, calls: f.calls };
}

const RUN_ID = "7c0f2a2e-1111-4222-8333-444455556666";
const SEEDS = { composition_seed: 11, archetype_seed: 22, perturbation_seed: 33 };
const runOut = (over: Record<string, unknown> = {}) => ({
  id: RUN_ID,
  status: "queued",
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

beforeEach(() => {
  sessionStorage.clear();
});

describe("session from GET /me (FR-WEB-5: the server decides the role)", () => {
  it("login validates the key with /me and takes role, permissions, run inputs and limits from it", async () => {
    const f = fakeFetch(() => json(200, ME_CLIENT));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    const s = await c.login("secret-key-0123456789");
    expect(f.calls[0].url).toBe("/api/v1/me");
    expect(header(f.calls[0].init, "Authorization")).toBe("Bearer secret-key-0123456789");
    expect(s).toEqual({
      subject: "alice",
      role: "client",
      displayName: "alice",
      permissions: { "filters.use": false },
      runInputs: ["composition", "composition_id", "config"],
      maySetSeeds: true,
      limits: { maxTotalBuildings: 5000, maxEnsembleRuns: 1000, defaultMinRuns: 20, demoCaps: null },
    });
  });

  it("maps the demo caps", async () => {
    const f = fakeFetch(() => json(200, ME_DEMO));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    const s = await c.login("secret-key-0123456789");
    expect(s.limits.demoCaps).toEqual({ maxBuildings: 50, maxRuns: 20 });
    expect(s.runInputs).toEqual(["composition_id"]);
    expect(s.maySetSeeds).toBe(false);
  });

  it.each([
    ["not an object", []],
    ["no role", { ...ME_CLIENT, role: undefined }],
    ["permission not boolean", { ...ME_CLIENT, permissions: { "filters.use": "yes" } }],
    ["run_inputs not strings", { ...ME_CLIENT, run_inputs: [1] }],
    ["may_set_seeds missing", { ...ME_CLIENT, may_set_seeds: undefined }],
    ["limit not a number", { ...ME_CLIENT, limits: { ...ME_CLIENT.limits, max_total_buildings: "5000" } }],
  ])("refuses a /me answer that cannot be read (%s) and stores no key", async (_what, me) => {
    const f = fakeFetch(() => json(200, me));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    await expect(c.login("secret-key-0123456789")).rejects.toBeInstanceOf(ApiError);
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
  });

  it("getSession asks /me again after a reload", async () => {
    sessionStorage.setItem(KEY_STORAGE, "secret-key-0123456789");
    const f = fakeFetch(() => json(200, ME_DEMO));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    expect((await c.getSession())?.role).toBe("demo");
    expect(f.calls[0].url).toBe("/api/v1/me");
  });

  it.each([
    ["401", () => json(401, { detail: "authentication required" })],
    ["500", () => json(500, {})],
    ["network", () => Promise.reject(new TypeError("Failed to fetch"))],
  ])("getSession: when /me fails (%s) the user is not signed in and the key is dropped", async (_what, answer) => {
    sessionStorage.setItem(KEY_STORAGE, "secret-key-0123456789");
    const f = fakeFetch(answer);
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    expect(await c.getSession()).toBeNull();
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
  });

  it("getSession without a stored key does not call the API", async () => {
    const f = fakeFetch(() => json(200, ME_CLIENT));
    const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: f.fn, storage: sessionStorage });
    expect(await c.getSession()).toBeNull();
    expect(f.calls).toHaveLength(0);
  });
});

describe("building types (GET /building-types)", () => {
  it("maps the catalogue, keeping inactive types and descriptions", async () => {
    const items = [
      { key: "sf_detached", label: "Single-family detached", building_class: "residential", source: "resstock", description: null, active: true },
      { key: "civic", label: "Civic", building_class: "commercial", source: "comstock", description: "Town halls", active: false },
    ];
    const { c, calls } = await signedIn(() => json(200, { items }));
    expect(await c.listBuildingTypes()).toEqual([
      { id: "sf_detached", label: "Single-family detached", sector: "residential", source: "resstock", description: null, active: true },
      { id: "civic", label: "Civic", sector: "commercial", source: "comstock", description: "Town halls", active: false },
    ]);
    expect(calls[0].url).toBe("/api/v1/building-types");
  });

  it("fails loudly on an unreadable catalogue", async () => {
    const { c } = await signedIn(() => json(200, { items: [{ key: "x" }] }));
    await expect(c.listBuildingTypes()).rejects.toThrow(/building type/i);
  });
});

describe("starting a custom composition (POST /runs with `composition`)", () => {
  const comp: Composition = {
    name: "Block A",
    totalBuildings: 10,
    entries: [
      { typeId: "sf_detached", mode: "count", count: 3 },
      { typeId: "mf_small", mode: "fraction", minFraction: 0.2, maxFraction: 1 },
    ],
    seeds: {},
    maxRuns: 40,
    convergence: true,
  };

  it("sends the composition in the API's terms with no convergence rule (the server's default applies)", async () => {
    const { c, calls } = await signedIn(() => json(202, runOut()));
    expect(await c.startRun({ kind: "custom", composition: comp })).toEqual({ runId: RUN_ID });
    expect(calls[0].url).toBe("/api/v1/runs");
    expect(calls[0].init.method).toBe("POST");
    expect(body(calls[0].init)).toEqual({
      composition: {
        name: "Block A",
        total_buildings: 10,
        building_types: [
          { building_type: "sf_detached", fixed_count: 3 },
          { building_type: "mf_small", min_fraction: 0.2, max_fraction: 1 },
        ],
        max_runs: 40,
        convergence: true,
      },
    });
  });

  it("sends convergence off as false and includes the seeds that were given", async () => {
    const { c, calls } = await signedIn(() => json(202, runOut()));
    await c.startRun({ kind: "custom", composition: { ...comp, convergence: false, seeds: { composition_seed: 42 } } });
    const b = body(calls[0].init);
    expect((b.composition as Record<string, unknown>).convergence).toBe(false);
    expect(b.seeds).toEqual({ composition_seed: 42 });
  });

  it("refuses a vintage filter loudly instead of dropping it (no filter input on the real API yet)", async () => {
    const { c, calls } = await signedIn(() => json(202, runOut()));
    await expect(c.startRun({ kind: "custom", composition: { ...comp, vintageFilter: "Before 1980" } })).rejects.toBeInstanceOf(ApiError);
    expect(calls).toHaveLength(0);
  });

  it("passes the server's 422 text through unchanged", async () => {
    const detail = "the fixed counts add up to 12, more than total_buildings (10)";
    const { c } = await signedIn(() => json(422, { detail }));
    await expect(c.startRun({ kind: "custom", composition: comp })).rejects.toMatchObject({ status: 422, message: detail });
  });
});

describe("run detail: data used and convergence rule", () => {
  const dataFiles = {
    resstock_characteristics_file: "DC_upgrade0.xlsx",
    comstock_characteristics_file: "DC_upgrade0_agg.xlsx",
    temperature: { source: "timeseries_column", file: null, column: "out.temp", time_column: null, units: "C" },
  };
  const detail = (over: Record<string, unknown> = {}) => ({
    ...runOut({ status: "succeeded" }),
    composition_snapshot: { ensemble: { mode: "adaptive", max_runs: 20 }, convergence: { rule: "legacy_window" } },
    data_files: dataFiles,
    result: { convergence: { rule: "prd_default", converged: true, checks: [] } },
    effective_filters: {},
    filter_selections: null,
    pool_summary: null,
    ...over,
  });

  it("maps data_files, the ensemble mode and the rule the engine reports in the result", async () => {
    const { c } = await signedIn(() => json(200, detail()));
    const s = await c.getRun(RUN_ID);
    expect(s.dataFiles).toEqual({
      resstockCharacteristicsFile: "DC_upgrade0.xlsx",
      comstockCharacteristicsFile: "DC_upgrade0_agg.xlsx",
      temperature: { source: "timeseries_column", file: null, column: "out.temp", timeColumn: null, units: "C" },
    });
    expect(s.ensembleMode).toBe("adaptive");
    expect(s.convergenceRule).toBe("prd_default"); // from the result, not the snapshot
  });

  it("reports no rule before the run has a result, and no data files when the server does not say", async () => {
    const { c } = await signedIn(() => json(200, detail({ result: null, data_files: undefined })));
    const s = await c.getRun(RUN_ID);
    expect(s.convergenceRule).toBeNull();
    expect(s.dataFiles).toBeNull();
  });
});

describe("results: temperature block (schema 1.1)", () => {
  const env = (base: number) => ({ mean_kw: [base, base + 1], p10_kw: [base - 1, base], p50_kw: [base, base + 1], p90_kw: [base + 1, base + 2] });
  const doc = (temperature: unknown, with15 = true) => ({
    schema_version: "1.1",
    datasets: {},
    results: {
      n_runs: 2,
      time: {
        hourly: { start: "2018-01-01T00:00:00", step_minutes: 60, count: 2 },
        "15min": with15 ? { start: "2018-01-01T00:00:00", step_minutes: 15, count: 2 } : null,
      },
      envelopes: {
        hourly: { total: env(100), residential: env(60), commercial: env(40) },
        "15min": with15 ? { total: env(10), residential: env(6), commercial: env(4) } : null,
      },
      ...(temperature === undefined ? {} : { temperature }),
    },
  });
  const available = { available: true, reason: null, units: "degC", hourly: [5, 6], "15min": [1.5, 2.5] };

  it("uses the 15-minute temperature with 15-minute load", async () => {
    const { c } = await signedIn(() => json(200, doc(available)));
    const r = await c.getResults(RUN_ID);
    expect(Array.from(r.series.temperature!)).toEqual([1.5, 2.5]);
    expect(r.temperatureNote).toBeNull();
  });

  it("uses the hourly temperature with hourly load", async () => {
    const { c } = await signedIn(() => json(200, doc({ ...available, "15min": null }, false)));
    expect(Array.from((await c.getResults(RUN_ID)).series.temperature!)).toEqual([5, 6]);
  });

  it("has no temperature, with the server's reason, when available is false", async () => {
    const { c } = await signedIn(() => json(200, doc({ available: false, reason: "no temperature source declared", hourly: null, "15min": null })));
    const r = await c.getResults(RUN_ID);
    expect(r.series.temperature).toBeNull();
    expect(r.temperatureNote).toBe("no temperature source declared");
  });

  it("treats a schema 1.0 document (no temperature key) as not available", async () => {
    const { c } = await signedIn(() => json(200, doc(undefined)));
    const r = await c.getResults(RUN_ID);
    expect(r.series.temperature).toBeNull();
    expect(r.temperatureNote).toMatch(/1\.0/);
  });

  it("fails loudly when temperature is marked available but its series is missing or the wrong length", async () => {
    const missing = await signedIn(() => json(200, doc({ ...available, "15min": null })));
    await expect(missing.c.getResults(RUN_ID)).rejects.toThrow(/temperature/i);
    const short = await signedIn(() => json(200, doc({ ...available, "15min": [1] })));
    await expect(short.c.getResults(RUN_ID)).rejects.toThrow(/temperature/i);
  });

  it("fails loudly on units other than degrees Celsius", async () => {
    const { c } = await signedIn(() => json(200, doc({ ...available, units: "degF" })));
    await expect(c.getResults(RUN_ID)).rejects.toThrow(/units/i);
  });
});
