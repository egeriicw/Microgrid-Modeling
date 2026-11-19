// W1-WEB2 independent QA: the real client (HttpApiClient + mapping) against hostile or unusual API answers.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { HttpApiClient, KEY_STORAGE } from "../api/httpClient";
import { capabilitiesOf } from "../auth/roles";
import { ApiError } from "../api/types";

const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const ME = {
  subject: "alice",
  role: "client",
  permissions: { "filters.use": false },
  run_inputs: ["composition", "composition_id", "config"],
  may_set_seeds: true,
  limits: { max_total_buildings: 5000, max_ensemble_runs: 1000, default_min_runs: 20, demo_caps: null },
};
const RUN_ID = "7c0f2a2e-1111-4222-8333-444455556666";

function client(handler: (url: string, init: RequestInit) => Response | Promise<Response>, me: unknown = ME) {
  const calls: { url: string; init: RequestInit }[] = [];
  const fetchFn = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = String(input);
    calls.push({ url, init });
    return url.endsWith("/me") ? json(200, me) : handler(url, init);
  });
  const c = new HttpApiClient({ baseUrl: "/api/v1", fetch: fetchFn as unknown as typeof fetch, storage: sessionStorage, retryDelayMs: 1 });
  return { c, calls };
}

beforeEach(() => sessionStorage.clear());

describe("QA W1-WEB2: /me decides capabilities, whatever the role is called", () => {
  it.each([
    ["a 'client' limited to predefined compositions", { ...ME, run_inputs: ["composition_id"] }, { composeCustom: false, runPredefined: true, replay: false, setSeeds: true }],
    ["a 'demo' that may set seeds and compose", { ...ME, role: "demo", may_set_seeds: true }, { composeCustom: true, runPredefined: true, replay: true, setSeeds: true }],
    ["an unknown role name with every input", { ...ME, role: "auditor" }, { composeCustom: true, runPredefined: true, replay: true, setSeeds: true }],
    ["an 'admin' with no run inputs", { ...ME, role: "admin", run_inputs: [], may_set_seeds: false }, { composeCustom: false, runPredefined: false, replay: false, setSeeds: false }],
    ["extra unknown fields and run inputs", { ...ME, run_inputs: ["composition", "teleport"], extra: { x: 1 } }, { composeCustom: true, runPredefined: false, replay: false, setSeeds: true }],
  ])("%s: the UI follows the flags", async (_what, me, expected) => {
    const { c } = client(() => json(500, {}), me);
    const s = await c.login("secret-key-0123456789");
    expect(capabilitiesOf(s)).toMatchObject(expected);
  });

  it.each([
    ["run_inputs is a string", { ...ME, run_inputs: "composition" }],
    ["permissions has a non-boolean flag", { ...ME, permissions: { "filters.use": "yes" } }],
    ["demo_caps is malformed", { ...ME, limits: { ...ME.limits, demo_caps: { max_buildings: -1, max_runs: 2 } } }],
    ["default_min_runs missing", { ...ME, limits: { max_total_buildings: 5000, max_ensemble_runs: 1000, demo_caps: null } }],
    ["the answer is an array", [ME]],
  ])("a malformed /me (%s) is refused and no key is kept", async (_what, me) => {
    const { c } = client(() => json(500, {}), me);
    await expect(c.login("secret-key-0123456789")).rejects.toBeInstanceOf(ApiError);
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
  });

  it("a key revoked after sign-in: the next call's 401 signs the user out and clears the stored key", async () => {
    const { c } = client(() => json(401, { detail: "authentication required" }));
    await c.login("secret-key-0123456789");
    expect(sessionStorage.getItem(KEY_STORAGE)).toBe("secret-key-0123456789");
    const signedOut = vi.fn();
    c.onSignedOut(signedOut);
    await expect(c.listBuildingTypes()).rejects.toMatchObject({ status: 401 });
    expect(signedOut).toHaveBeenCalledTimes(1);
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
  });

  it("a 403 or 503 after sign-in does not sign the user out", async () => {
    for (const status of [403, 503]) {
      const { c } = client(() => json(status, { detail: "nope" }));
      await c.login("secret-key-0123456789");
      const signedOut = vi.fn();
      c.onSignedOut(signedOut);
      await expect(c.listBuildingTypes()).rejects.toMatchObject({ status, message: "nope" });
      expect(signedOut).not.toHaveBeenCalled();
      expect(sessionStorage.getItem(KEY_STORAGE)).not.toBeNull();
    }
  });
});

describe("QA W1-WEB2: replay sends exactly the stored config and the same three seeds", () => {
  it("POST /runs body is {config, seeds} and nothing else", async () => {
    const stored = { scenario: { name: "x" }, composition: { total_buildings: 6, categories: { sf_detached: { fixed_count: 2, source: "resstock" } } }, ensemble: { mode: "fixed", max_runs: 3 }, rng: { composition_seed: 11, archetype_seed: 22, perturbation_seed: 33 } };
    const seeds = { composition_seed: 11, archetype_seed: 22, perturbation_seed: 33 };
    const { c, calls } = client(() => json(202, { id: RUN_ID }));
    await c.login("secret-key-0123456789");
    await c.startRun({ kind: "replay", config: structuredClone(stored), seeds });
    const post = calls.find((x) => x.init.method === "POST")!;
    expect(JSON.parse(String(post.init.body))).toEqual({ config: stored, seeds });
  });
});

describe("QA W1-WEB2: server error text reaches the user unchanged", () => {
  it("a pydantic 422 list becomes field messages with the body prefix removed", async () => {
    const detail = [{ type: "value_error", loc: ["body", "composition", "name"], msg: "Value error, name must not contain control, line-separator or text-direction characters" }];
    const { c } = client(() => json(422, { detail }));
    await c.login("secret-key-0123456789");
    const err = await c.startRun({ kind: "custom", composition: { name: "a", totalBuildings: 1, entries: [], seeds: {}, maxRuns: 1, convergence: false } }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).fieldErrors).toEqual([{ field: "composition.name", message: detail[0].msg }]);
  });
});

describe("QA W1-WEB2: results document honesty", () => {
  const env = (b: number) => ({ mean_kw: [b, b], p10_kw: [b, b], p50_kw: [b, b], p90_kw: [b, b] });
  const doc = (temperature: unknown, start = "2018-01-01T00:00:00") => ({
    schema_version: "1.1",
    datasets: {},
    results: {
      n_runs: 2,
      time: { hourly: { start, step_minutes: 60, count: 2 }, "15min": null },
      envelopes: { hourly: { total: env(10), residential: env(6), commercial: env(4) }, "15min": null },
      temperature,
    },
  });
  const ok = { available: true, reason: null, units: "degC", hourly: [5, 6], "15min": null };

  it("reads a well-formed temperature block", async () => {
    const { c } = client(() => json(200, doc(ok)));
    await c.login("secret-key-0123456789");
    expect(Array.from((await c.getResults(RUN_ID)).series.temperature!)).toEqual([5, 6]);
  });

  // The schema says missing or non-finite temperature values are errors; the mapper turns them into NaN and the chart
  // draws a gap instead of failing with a clear message.
  it("fails loudly when an available temperature series contains a null", async () => {
    const { c } = client(() => json(200, doc({ ...ok, hourly: [5, null] })));
    await c.login("secret-key-0123456789");
    await expect(c.getResults(RUN_ID)).rejects.toThrow(/temperature/i);
  });

  it("fails loudly when an available temperature series contains text", async () => {
    const { c } = client(() => json(200, doc({ ...ok, hourly: [5, "6"] })));
    await c.login("secret-key-0123456789");
    await expect(c.getResults(RUN_ID)).rejects.toThrow(/temperature/i);
  });

  // The schema documents `start` as ISO local time (no offset). A start WITH an offset is silently read as wall clock
  // and labelled "local time"; it should be refused, not ignored.
  it("refuses a time axis start that carries a time-zone offset instead of ignoring the offset", async () => {
    const { c } = client(() => json(200, doc(ok, "2018-01-01T05:00:00+00:00")));
    await c.login("secret-key-0123456789");
    await expect(c.getResults(RUN_ID)).rejects.toThrow(/time/i);
  });
});
