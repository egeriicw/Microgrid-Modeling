// Independent QA review tests (defects D1-D3 are fixed; their former `it.fails` markers are now plain tests).
import { cleanup, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createMockClient } from "../api/mockClient";
import type { ApiClient, Composition } from "../api/types";
import { buildTimeSeriesOption } from "../features/results/chartOptions";
import { generateDataset } from "../api/fixtures";
import { validateComposition } from "../features/composition/validate";
import { renderApp } from "../test/utils";

vi.mock("../charts/EChart", () => ({
  EChart: (p: { ariaLabel: string }) => <div role="img" aria-label={p.ariaLabel} />,
}));

afterEach(cleanup);

const ok: Composition = {
  name: "demo",
  totalBuildings: 100,
  seeds: {},
  maxRuns: 200,
  convergence: false,
  entries: [
    { typeId: "a", mode: "fraction", minFraction: 0.2, maxFraction: 0.8 },
    { typeId: "b", mode: "fraction", minFraction: 0, maxFraction: 1 },
  ],
};
/** A run that outlasts the test: the server's cap of runs, building types the (mock) catalogue knows. */
const LONG_RUN: Composition = { ...ok, maxRuns: 1000, entries: [{ typeId: "sf", mode: "fraction", minFraction: 0, maxFraction: 1 }] };

describe("QA: composition validation holes", () => {
  it("rejects NaN, negative and non-finite numbers", () => {
    expect(validateComposition({ ...ok, totalBuildings: NaN }).totalBuildings).toBeTruthy();
    expect(validateComposition({ ...ok, seeds: { composition_seed: -1 } }).composition_seed).toBeTruthy();
    expect(validateComposition({ ...ok, maxRuns: Infinity }).maxRuns).toBeTruthy();
    expect(validateComposition({ ...ok, entries: [{ typeId: "a", mode: "fraction", minFraction: -0.1, maxFraction: 1 }] }).entries).toBeTruthy();
  });

  // D1 (resolved, with a changed expectation): QA first expected "60 fixed + fraction 1 of 100" to be rejected as
  // over-filling. The engine removes fixed counts first and applies the fraction ranges to the REMAINDER (40 here), so
  // this composition is valid. Over-filling is caught where it is real: fixed counts above the total.
  it("accepts 60 fixed + a 0..1 range (the range applies to the remaining 40) and rejects fixed above the total", () => {
    const valid = validateComposition({
      ...ok,
      entries: [
        { typeId: "a", mode: "count", count: 60 },
        { typeId: "b", mode: "fraction", minFraction: 0, maxFraction: 1 },
      ],
    });
    expect(valid).toEqual({});
    const over = validateComposition({
      ...ok,
      entries: [
        { typeId: "a", mode: "count", count: 101 },
        { typeId: "b", mode: "fraction", minFraction: 0, maxFraction: 1 },
      ],
    });
    expect(over.entries).toMatch(/exceed/i);
  });

  // DEFECT D2: no upper bounds. A seed above the 32/64-bit range or a billion max runs passes client validation.
  it("rejects an overflowing seed", () => {
    expect(validateComposition({ ...ok, seeds: { archetype_seed: 1e300 } }).archetype_seed).toBeTruthy();
  });
  it("rejects an absurd maxRuns", () => {
    expect(validateComposition({ ...ok, maxRuns: 1e12 }).maxRuns).toBeTruthy();
  });
});

describe("QA: results labels come from the API", () => {
  // DEFECT D3: the weather-year subtitle on each card is hardcoded "AMY2018" instead of results.weatherYear.
  it("does not hardcode AMY2018 when the API says another weather year", async () => {
    const base = createMockClient({ pointsPerYear: 96 * 20 });
    const client: ApiClient = { ...base, getResults: async (id) => ({ ...(await base.getResults(id)), weatherYear: "TMY3" }) };
    await renderApp({ route: "/results/demo-run", role: "client", client });
    await screen.findByText(/weather year: TMY3/i);
    expect(screen.queryAllByText(/AMY2018/).length).toBe(0);
  });
});

describe("QA: role gating and run lifecycle", () => {
  it("demo reaching /composition by URL sees no custom form and no Start run", async () => {
    await renderApp({ route: "/composition", role: "demo" });
    await screen.findByText(/demo accounts can run the predefined compositions only/i);
    expect(screen.queryByRole("button", { name: /start run/i })).toBeNull();
    expect(screen.queryByText(/building filters/i)).toBeNull();
  });

  it("client never sees the admin-only filter section", async () => {
    await renderApp({ route: "/composition", role: "client" });
    await screen.findByRole("button", { name: /start run/i });
    expect(screen.queryByText(/building filters/i)).toBeNull();
  });

  it("signed-out deep link to results redirects to login", async () => {
    await renderApp({ route: "/results/demo-run" });
    expect(await screen.findByRole("heading", { name: /sign in/i })).toBeInTheDocument();
  });

  it("stops the progress stream after the run page unmounts", async () => {
    const base = createMockClient({ runsPerTick: 1, tickMs: 5 });
    let signal: AbortSignal | undefined;
    const watchRun = vi.fn((id: string, on: Parameters<ApiClient["watchRun"]>[1], s: AbortSignal) => ((signal = s), base.watchRun(id, on, s)));
    const client: ApiClient = { ...base, watchRun };
    await base.login("client");
    const { runId } = await base.startRun({ kind: "custom", composition: LONG_RUN });
    const { unmount } = await renderApp({ route: `/runs/${runId}`, client, role: "client" });
    await waitFor(() => expect(watchRun).toHaveBeenCalled());
    expect(signal?.aborted).toBe(false);
    unmount();
    expect(signal?.aborted).toBe(true);
  });

  it("cancel button is disabled while the cancel request is in flight (no double cancel)", async () => {
    const base = createMockClient({ runsPerTick: 1, tickMs: 5 });
    let release = () => {};
    const cancelRun = vi.fn(() => new Promise<void>((r) => (release = r)));
    const client: ApiClient = { ...base, cancelRun };
    await base.login("client");
    const { runId } = await base.startRun({ kind: "custom", composition: LONG_RUN });
    await renderApp({ route: `/runs/${runId}`, client, role: "client" });
    const user = userEvent.setup();
    const btn = await screen.findByRole("button", { name: /cancel/i });
    await user.click(btn);
    expect(btn).toBeDisabled();
    expect(cancelRun).toHaveBeenCalledTimes(1);
    release();
  });
});

describe("QA: gaps found by mutation (survived the author's tests)", () => {
  it("client role hides filters even if the server wrongly returned options (UI gate, not mock luck)", async () => {
    const base = createMockClient({ pointsPerYear: 96 * 20 });
    const client: ApiClient = { ...base, listFilterOptions: async () => [{ id: "vintage", label: "Vintage", values: ["Any", "1980s"] }] };
    await renderApp({ route: "/composition", role: "client", client });
    await screen.findByRole("button", { name: /start run/i });
    expect(screen.queryByText(/building filters/i)).toBeNull();
    expect(screen.queryByLabelText(/vintage/i)).toBeNull();
  });

  it("band is drawn from P10 up to P90: base series reads p10, span is p90 - p10, P50 reads p50", () => {
    const series = generateDataset({ points: 96, seed: 1 });
    const opt = buildTimeSeriesOption({ series, resolution: "15min", showBand: true, showTemperature: false }) as {
      dataset: { source: Record<string, number[]> };
      series: { name: string; encode: { y: string } }[];
    };
    const y = Object.fromEntries(opt.series.map((s) => [s.name, s.encode.y]));
    expect(y["P10 (base)"]).toBe("p10");
    expect(y["P10-P90 band"]).toBe("span");
    expect(y["P50"]).toBe("p50");
    opt.dataset.source.span.forEach((v, i) => expect(v).toBeCloseTo(series.p90[i] - series.p10[i]));
  });
});
