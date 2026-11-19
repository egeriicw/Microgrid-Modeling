import { describe, expect, it, vi } from "vitest";
import { createMockClient, DEMO_RUN_ID, PREDEFINED } from "./mockClient";
import { ApiError, type Composition, type RunEvent, type RunRequest } from "./types";

const comp: Composition = {
  name: "t",
  totalBuildings: 10,
  seeds: { composition_seed: 1 },
  maxRuns: 50,
  convergence: false,
  entries: [{ typeId: "sf", mode: "fraction", minFraction: 0, maxFraction: 1 }],
};
const custom: RunRequest = { kind: "custom", composition: comp };
const withComp = (over: Partial<Composition>): RunRequest => ({ kind: "custom", composition: { ...comp, ...over } });

async function drain(c: ReturnType<typeof createMockClient>, runId: string) {
  const events: RunEvent[] = [];
  await c.watchRun(runId, (e) => events.push(e), new AbortController().signal);
  return events;
}

describe("mock ApiClient (mirrors the real API's rules)", () => {
  it("signs in with a key naming the role, and rejects other keys with 401", async () => {
    const c = createMockClient({ pointsPerYear: 96 * 8 });
    const s = await c.login("client");
    expect(s.role).toBe("client");
    expect(await c.getSession()).toEqual(s);
    await expect(c.login("wrong")).rejects.toMatchObject({ status: 401 });
  });

  it("requires login before other calls (server-side enforcement stand-in)", async () => {
    const c = createMockClient();
    await expect(c.startRun(custom)).rejects.toMatchObject({ status: 401 });
  });

  it("demo may run a predefined composition only; admin can read filters", async () => {
    const c = createMockClient();
    await c.login("demo");
    await expect(c.startRun(custom)).rejects.toMatchObject({ status: 403 });
    await expect(c.listFilterOptions()).resolves.toEqual([]);
    const names = await c.listPredefinedCompositions();
    expect(names).toEqual(PREDEFINED);
    await expect(c.startRun({ kind: "predefined", compositionId: names[0] })).resolves.toHaveProperty("runId");
    await expect(c.startRun({ kind: "predefined", compositionId: "nope" })).rejects.toMatchObject({ status: 404 });
    await c.login("admin");
    expect((await c.listFilterOptions()).length).toBeGreaterThan(0);
  });

  it("starts queued, records seeds (filling unset ones), streams progress to success", async () => {
    const c = createMockClient({ runsPerTick: 20, tickMs: 1 });
    await c.login("client");
    const { runId } = await c.startRun(custom);
    const before = await c.getRun(runId);
    expect(before.state).toBe("queued");
    expect(before.maxRuns).toBe(50);
    expect(before.seeds?.composition_seed).toBe(1);
    expect(Number.isInteger(before.seeds?.archetype_seed)).toBe(true);
    const events = await drain(c, runId);
    const prog = events.filter((e) => e.type === "progress");
    expect(prog.map((e) => (e.type === "progress" ? e.payload.n_attempted : 0))).toEqual([20, 40, 50]);
    expect(events.at(-1)).toEqual({ type: "status", status: "succeeded" });
    const st = await c.getRun(runId);
    expect(st.state).toBe("succeeded");
    expect(st.completedRuns).toBe(50);
    expect(st.feasible + st.rejected).toBe(st.completedRuns);
    expect(st.converged).toBe(true);
  });

  it("replays a run with the same seeds", async () => {
    const c = createMockClient();
    await c.login("client");
    const first = await c.getRun((await c.startRun(custom)).runId);
    const { runId } = await c.startRun({ kind: "replay", config: first.config!, seeds: first.seeds! });
    const again = await c.getRun(runId);
    expect(runId).not.toBe(first.runId);
    expect(again.seeds).toEqual(first.seeds);
    expect(again.config).toEqual(first.config);
  });

  it("cancels a run and stops progress", async () => {
    const c = createMockClient({ runsPerTick: 10, tickMs: 1 });
    await c.login("client");
    const { runId } = await c.startRun(custom);
    await c.cancelRun(runId);
    expect((await c.getRun(runId)).state).toBe("cancelled");
    expect(await drain(c, runId)).toEqual([{ type: "status", status: "cancelled" }]);
    await expect(c.getResults(runId)).rejects.toMatchObject({ status: 404 });
  });

  it("returns 404 for an unknown run", async () => {
    const c = createMockClient();
    await c.login("client");
    await expect(c.getRun("nope")).rejects.toMatchObject({ status: 404 });
    await expect(drain(c, "nope")).rejects.toBeInstanceOf(ApiError);
  });

  it("serves the fixture demo run results to every role, labelled AMY2018", async () => {
    const c = createMockClient({ pointsPerYear: 96 * 8 });
    await c.login("demo");
    const r = await c.getResults(DEMO_RUN_ID);
    expect(r.weatherYear).toBe("AMY2018");
    expect(r.calibrationStatus).toMatch(/not calibrated/i);
    expect(r.series.ts).toHaveLength(96 * 8);
    expect(r.series.total[0]).toBeCloseTo(r.series.residential[0] + r.series.commercial[0], 6);
  });

  it("reports each role's session like GET /me: permissions, run inputs, seeds and limits", async () => {
    const c = createMockClient();
    const client = await c.login("client");
    expect(client).toMatchObject({
      role: "client",
      permissions: { "filters.use": false },
      runInputs: ["composition", "composition_id", "config"],
      maySetSeeds: true,
      limits: { maxTotalBuildings: 5000, maxEnsembleRuns: 1000, defaultMinRuns: 20, demoCaps: null },
    });
    const demo = await c.login("demo");
    expect(demo).toMatchObject({ runInputs: ["composition_id"], maySetSeeds: false, permissions: { "filters.use": false } });
    expect(demo.limits.demoCaps).toEqual({ maxBuildings: 50, maxRuns: 20 });
    expect((await c.login("admin")).permissions["filters.use"]).toBe(true);
  });

  it("lists building types with an inactive one, and refuses compositions the server would refuse (422)", async () => {
    const c = createMockClient();
    await c.login("client");
    const types = await c.listBuildingTypes();
    expect(types.some((t) => !t.active)).toBe(true);
    const inactive = types.find((t) => !t.active)!.id;
    const cases: [RunRequest, RegExp][] = [
      [withComp({ entries: [{ typeId: inactive, mode: "fraction", minFraction: 0, maxFraction: 1 }] }), /not active/],
      [withComp({ entries: [{ typeId: "nope", mode: "fraction", minFraction: 0, maxFraction: 1 }] }), /unknown building type 'nope'/],
      [withComp({ entries: [{ typeId: "sf", mode: "count", count: 11 }, { typeId: "mf", mode: "fraction", minFraction: 0, maxFraction: 1 }] }), /fixed counts add up to 11, more than total_buildings \(10\)/],
      [withComp({ entries: [{ typeId: "sf", mode: "count", count: 4 }] }), /add up to 4, not total_buildings \(10\)/],
      [withComp({ entries: [{ typeId: "sf", mode: "fraction", minFraction: 0.6, maxFraction: 1 }, { typeId: "mf", mode: "fraction", minFraction: 0.6, maxFraction: 1 }] }), /minimum fractions.*more than 1/],
      [withComp({ entries: [{ typeId: "sf", mode: "fraction", minFraction: 0, maxFraction: 0.4 }, { typeId: "mf", mode: "fraction", minFraction: 0, maxFraction: 0.4 }] }), /maximum fractions.*less than 1/],
      [withComp({ convergence: true, maxRuns: 10 }), /max_runs must be at least min_runs \(20/],
      [withComp({ totalBuildings: 5001 }), /exceeds the server cap of 5000/],
      [withComp({ maxRuns: 1001 }), /exceeds the server cap of 1000/],
    ];
    for (const [req, msg] of cases) {
      const err = (await c.startRun(req).catch((e: unknown) => e)) as ApiError;
      expect(err).toBeInstanceOf(ApiError);
      expect(err.status).toBe(422);
      expect(err.message).toMatch(msg);
    }
  });

  it("refuses seeds and custom compositions from demo with 403 (run inputs)", async () => {
    const c = createMockClient();
    await c.login("demo");
    await expect(c.startRun(withComp({ seeds: {} }))).rejects.toMatchObject({ status: 403 });
    await expect(c.startRun({ kind: "replay", config: {}, seeds: { composition_seed: 1, archetype_seed: 2, perturbation_seed: 3 } })).rejects.toMatchObject({ status: 403 });
  });

  it("reports ensemble mode, the convergence rule after success, and the data files", async () => {
    const c = createMockClient({ runsPerTick: 50, tickMs: 1 });
    await c.login("client");
    const { runId } = await c.startRun(withComp({ convergence: true }));
    const before = await c.getRun(runId);
    expect(before.ensembleMode).toBe("adaptive");
    expect(before.convergenceRule).toBeNull();
    expect(before.dataFiles?.resstockCharacteristicsFile).toBeTruthy();
    await drain(c, runId);
    expect((await c.getRun(runId)).convergenceRule).toBe("legacy_window");
    const fixed = await c.getRun((await c.startRun(custom)).runId);
    expect(fixed.ensembleMode).toBe("fixed");
  });

  it("persists the session in sessionStorage only when asked, and survives a bad storage", async () => {
    sessionStorage.clear();
    const a = createMockClient({ persistSession: true });
    await a.login("admin");
    const b = createMockClient({ persistSession: true });
    expect((await b.getSession())?.role).toBe("admin");
    await b.logout();
    expect(await createMockClient({ persistSession: true }).getSession()).toBeNull();
    // not persisted by default
    await createMockClient().login("client");
    expect(await createMockClient({ persistSession: true }).getSession()).toBeNull();
    // storage that throws must not break the client
    const spy = vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    expect(await createMockClient({ persistSession: true }).getSession()).toBeNull();
    spy.mockRestore();
  });
});
