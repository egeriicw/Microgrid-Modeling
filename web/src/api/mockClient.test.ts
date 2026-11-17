import { describe, expect, it, vi } from "vitest";
import { createMockClient } from "./mockClient";
import { ApiError, type Composition } from "./types";

const comp: Composition = {
  name: "t",
  totalBuildings: 10,
  seed: 1,
  maxRuns: 50,
  entries: [{ typeId: "sf", mode: "fraction", minFraction: 0, maxFraction: 1 }],
};

describe("mock ApiClient", () => {
  it("logs in with a role and reports the session", async () => {
    const c = createMockClient({ pointsPerYear: 96 * 8 });
    const s = await c.login("client");
    expect(s.role).toBe("client");
    expect(await c.getSession()).toEqual(s);
  });

  it("requires login before other calls (server-side enforcement stand-in)", async () => {
    const c = createMockClient();
    await expect(c.startRun(comp)).rejects.toMatchObject({ status: 401 });
  });

  it("demo cannot start runs or read filters; admin can read filters", async () => {
    const c = createMockClient();
    await c.login("demo");
    await expect(c.startRun(comp)).rejects.toBeInstanceOf(ApiError);
    await expect(c.listFilterOptions()).resolves.toEqual([]);
    await c.login("admin");
    expect((await c.listFilterOptions()).length).toBeGreaterThan(0);
  });

  it("advances a run to success across polls with consistent counters", async () => {
    const c = createMockClient({ runsPerPoll: 20 });
    await c.login("client");
    const { runId } = await c.startRun(comp);
    let st = await c.getRun(runId);
    expect(st.state).toBe("running");
    const seen: number[] = [st.completedRuns];
    while (st.state === "running") {
      st = await c.getRun(runId);
      seen.push(st.completedRuns);
    }
    expect(st.state).toBe("succeeded");
    expect(st.completedRuns).toBe(50);
    expect(st.feasible + st.rejected).toBe(st.completedRuns);
    expect(st.converged).toBe(true);
    expect(seen).toEqual([...seen].sort((a, b) => a - b));
  });

  it("cancels a run and stops progress", async () => {
    const c = createMockClient({ runsPerPoll: 10 });
    await c.login("client");
    const { runId } = await c.startRun(comp);
    await c.cancelRun(runId);
    const st = await c.getRun(runId);
    expect(st.state).toBe("cancelled");
    await expect(c.getResults(runId)).rejects.toMatchObject({ status: 409 });
  });

  it("returns 404 for an unknown run", async () => {
    const c = createMockClient();
    await c.login("client");
    await expect(c.getRun("nope")).rejects.toMatchObject({ status: 404 });
  });

  it("serves the predefined demo run results to every role, labelled AMY2018", async () => {
    const c = createMockClient({ pointsPerYear: 96 * 8 });
    await c.login("demo");
    const r = await c.getResults("demo-run");
    expect(r.weatherYear).toBe("AMY2018");
    expect(r.calibrationStatus).toMatch(/not calibrated/i);
    expect(r.series.ts).toHaveLength(96 * 8);
    expect(r.series.total[0]).toBeCloseTo(r.series.residential[0] + r.series.commercial[0], 6);
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
