import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { createMockClient } from "../../api/mockClient";
import { ApiError, type ApiClient, type Composition, type RunStatus } from "../../api/types";
import { renderApp } from "../../test/utils";

const comp: Composition = {
  name: "t",
  totalBuildings: 10,
  seeds: { composition_seed: 1, archetype_seed: 2, perturbation_seed: 3 },
  maxRuns: 100,
  convergence: true,
  entries: [{ typeId: "sf", mode: "fraction", minFraction: 0, maxFraction: 1 }],
};

async function startedRun(runsPerTick: number, tickMs = 5) {
  const client = createMockClient({ runsPerTick, tickMs, pointsPerYear: 96 * 20 });
  await client.login("client");
  const { runId } = await client.startRun({ kind: "custom", composition: comp });
  return { client, runId };
}

describe("run screen (FR-WEB-2)", () => {
  it("shows a progress bar, stage text, runs completed and feasible/rejected counts from the stream", async () => {
    const { client, runId } = await startedRun(1, 20);
    await renderApp({ route: `/runs/${runId}`, role: "client", client });
    const bar = await screen.findByRole("progressbar", { name: /run progress/i });
    expect(bar).toHaveAttribute("aria-valuemin", "0");
    expect(bar).toHaveAttribute("aria-valuemax", "100");
    expect(await screen.findByText(/runs completed: [1-9]\d* of 100/i)).toBeInTheDocument();
    expect(screen.getByText(/feasible/i)).toBeInTheDocument();
    expect(screen.getByText(/rejected/i)).toBeInTheDocument();
    expect(screen.getByTestId("stage-text")).not.toBeEmptyDOMElement();
  });

  it("reaches 100%, reports success and links to the results", async () => {
    const { client, runId } = await startedRun(50);
    await renderApp({ route: `/runs/${runId}`, role: "client", client });
    const link = await screen.findByRole("link", { name: /view results/i });
    expect(link).toHaveAttribute("href", `/results/${runId}`);
    expect(screen.queryByRole("button", { name: /cancel/i })).not.toBeInTheDocument();
    expect(screen.getByText(/convergence: converged/i)).toBeInTheDocument();
  });

  it("cancels a run", async () => {
    const user = userEvent.setup();
    const { client, runId } = await startedRun(1, 20);
    await renderApp({ route: `/runs/${runId}`, role: "client", client });
    await user.click(await screen.findByRole("button", { name: /cancel/i }));
    await waitFor(() => expect(screen.getByTestId("stage-text")).toHaveTextContent(/cancelled/i));
    expect(screen.queryByRole("link", { name: /view results/i })).not.toBeInTheDocument();
  });

  it("shows an error for an unknown run instead of spinning forever", async () => {
    await renderApp({ route: "/runs/nope", role: "client" });
    expect(await screen.findByRole("alert")).toHaveTextContent(/not found/i);
  });

  it("falls back to polling when the progress stream fails", async () => {
    const { client, runId } = await startedRun(50);
    let polls = 0;
    const broken: ApiClient = {
      ...client,
      watchRun: async () => {
        throw new ApiError(0, "Lost the connection to the progress stream.");
      },
      getRun: async (id) => {
        polls += 1;
        const s = await client.getRun(id);
        return polls > 2 ? { ...s, state: "succeeded", completedRuns: 100, maxRuns: 100 } : s;
      },
    };
    await renderApp({ route: `/runs/${runId}`, role: "client", client: broken });
    expect(await screen.findByRole("link", { name: /view results/i })).toBeInTheDocument();
    expect(polls).toBeGreaterThan(2);
  });

  it("shows an indeterminate bar while the run is queued and max runs is unknown", async () => {
    const base = createMockClient();
    await base.login("client");
    const client: ApiClient = {
      ...base,
      getRun: async (id) => ({ runId: id, state: "queued", stage: "Queued", maxRuns: null, completedRuns: 0, feasible: 0, rejected: 0, converged: false, seeds: null, config: null, engineVersion: "x", cancelRequested: false, ensembleMode: null, convergenceRule: null, dataFiles: null }),
      watchRun: () => new Promise(() => undefined),
    };
    const { runId } = await base.startRun({ kind: "custom", composition: comp });
    await renderApp({ route: `/runs/${runId}`, role: "client", client });
    const bar = await screen.findByRole("progressbar", { name: /run progress/i });
    expect(bar).not.toHaveAttribute("aria-valuenow");
    expect(screen.getByTestId("stage-text")).toHaveTextContent(/queued/i);
  });
});

describe("convergence wording and cancelling (honest status)", () => {
  const status = (over: Partial<RunStatus>): RunStatus => ({
    runId: "r",
    state: "running",
    stage: "run 3/10 feasible",
    completedRuns: 3,
    maxRuns: 10,
    feasible: 3,
    rejected: 0,
    converged: null,
    seeds: null,
    config: null,
    engineVersion: "x",
    cancelRequested: false,
    ensembleMode: "adaptive",
    convergenceRule: null,
    dataFiles: null,
    ...over,
  });
  async function show(s: RunStatus) {
    const base = createMockClient();
    const client: ApiClient = { ...base, getRun: async () => s, watchRun: () => new Promise(() => undefined) };
    await renderApp({ route: "/runs/r", role: "client", client });
    await screen.findByText(/runs completed/i);
  }

  it.each([
    [{ converged: null }, /convergence: not checked yet/i],
    [{ converged: false }, /convergence: not converged yet/i],
    [{ converged: true }, /convergence: converged/i],
    [{ state: "succeeded" as const, converged: false }, /convergence: did not converge \(stopped at the run limit\)/i],
    [{ state: "succeeded" as const, converged: null }, /convergence: not reported/i],
  ])("%j is described as %s", async (over, text) => {
    await show(status(over));
    expect(screen.getByText(text)).toBeInTheDocument();
    if (over.converged !== true) expect(screen.queryByText(/convergence: converged/i)).toBeNull();
  });

  it("a run with convergence off says so instead of reporting the engine's 'converged' flag", async () => {
    await show(status({ ensembleMode: "fixed", state: "succeeded", converged: true }));
    expect(screen.getByText(/convergence: off \(a fixed number of runs\)/i)).toBeInTheDocument();
    expect(screen.queryByText(/convergence: converged/i)).toBeNull();
  });

  it("shows the convergence rule the engine reports for a finished run, and none before", async () => {
    await show(status({ state: "succeeded", converged: true, convergenceRule: "legacy_window" }));
    expect(screen.getByText(/convergence rule used: legacy_window/i)).toBeInTheDocument();
  });

  it("does not claim a rule while the run has not reported one", async () => {
    await show(status({}));
    expect(screen.queryByText(/convergence rule used/i)).toBeNull();
  });

  it("shows Cancelling… while a cancel has been requested and the run is not yet terminal", async () => {
    await show(status({ cancelRequested: true }));
    expect(screen.getByText(/cancelling…/i)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /cancel run/i })).toBeNull();
  });

  it("does not show Cancelling… once the run is cancelled", async () => {
    await show(status({ state: "cancelled", cancelRequested: true }));
    expect(screen.queryByText(/cancelling…/i)).toBeNull();
  });
});

describe("data used (data_files, in plain language)", () => {
  const files = (temperature: Partial<NonNullable<RunStatus["dataFiles"]>["temperature"]>): RunStatus["dataFiles"] => ({
    resstockCharacteristicsFile: "DC_upgrade0.xlsx",
    comstockCharacteristicsFile: "DC_upgrade0_agg.xlsx",
    temperature: { source: "none", file: null, column: null, timeColumn: null, units: null, ...temperature },
  });
  async function showFiles(dataFiles: RunStatus["dataFiles"]) {
    const base = createMockClient();
    await base.login("client");
    const { runId } = await base.startRun({ kind: "custom", composition: comp });
    const s = await base.getRun(runId);
    const client: ApiClient = { ...base, getRun: async () => ({ ...s, dataFiles }), watchRun: () => new Promise(() => undefined) };
    const view = await renderApp({ route: `/runs/${runId}`, role: "client", client });
    return { view, panel: dataFiles ? await screen.findByRole("region", { name: /data used/i }) : null };
  }

  it("names the characteristics files and says when no temperature source was declared", async () => {
    const { panel } = await showFiles(files({}));
    expect(panel).toHaveTextContent(/home \(ResStock\) characteristics: DC_upgrade0\.xlsx/i);
    expect(panel).toHaveTextContent(/building \(ComStock\) characteristics: DC_upgrade0_agg\.xlsx/i);
    expect(panel).toHaveTextContent(/no outdoor temperature source was declared/i);
  });

  it("describes a temperature column in the building files", async () => {
    const { panel } = await showFiles(files({ source: "timeseries_column", column: "out.temp_c", units: "C" }));
    expect(panel).toHaveTextContent(/column out\.temp_c of each sampled building's load file, in degrees Celsius/i);
  });

  it("describes a weather file", async () => {
    const { panel } = await showFiles(files({ source: "weather_file", file: "weather_2018.csv", column: "temp_f", timeColumn: "timestamp", units: "F" }));
    expect(panel).toHaveTextContent(/weather file weather_2018\.csv \(column temp_f, time column timestamp\), in degrees Fahrenheit/i);
  });

  it("says when the run did not record a source or a file", async () => {
    const { panel } = await showFiles({ resstockCharacteristicsFile: null, comstockCharacteristicsFile: null, temperature: { source: null, file: null, column: null, timeColumn: null, units: null } });
    expect(panel).toHaveTextContent(/home \(ResStock\) characteristics: not recorded for this run/i);
    expect(panel).toHaveTextContent(/outdoor temperature: not recorded for this run/i);
  });

  it("renders file names as text, never markup", async () => {
    const { view } = await showFiles(files({ source: "weather_file", file: "<img src=x onerror=alert(1)>.csv", column: "t", timeColumn: "ts", units: "C" }));
    expect(view.container.querySelector("main img")).toBeNull();
    expect(screen.getByText(/<img src=x onerror=alert\(1\)>\.csv/)).toBeInTheDocument();
  });

  it("shows no panel when the server does not report data files", async () => {
    await showFiles(null);
    await screen.findByText(/runs completed/i);
    expect(screen.queryByRole("region", { name: /data used/i })).toBeNull();
  });
});

describe("seeds and replay (FR-ENG-4 d)", () => {
  it("shows the three seeds the run used", async () => {
    const { client, runId } = await startedRun(50);
    await renderApp({ route: `/runs/${runId}`, role: "client", client });
    const seeds = await screen.findByRole("table", { name: /seeds/i });
    expect(within(seeds).getByText("composition_seed")).toBeInTheDocument();
    expect(within(seeds).getByText("1")).toBeInTheDocument();
    expect(within(seeds).getByText("2")).toBeInTheDocument();
    expect(within(seeds).getByText("3")).toBeInTheDocument();
  });

  it("replays with the same seeds as a new run", async () => {
    const user = userEvent.setup();
    const { client, runId } = await startedRun(50);
    const startRun = vi.fn(client.startRun);
    await renderApp({ route: `/runs/${runId}`, role: "client", client: { ...client, startRun } });
    await screen.findByRole("link", { name: /view results/i });
    await user.click(screen.getByRole("button", { name: /replay with same seeds/i }));
    await waitFor(() => expect(screen.getByRole("heading", { name: /run run-2/i })).toBeInTheDocument());
    const original = await client.getRun(runId);
    expect(startRun).toHaveBeenCalledWith({ kind: "replay", config: original.config, seeds: original.seeds });
    expect((await client.getRun("run-2")).seeds).toEqual(original.seeds);
  });

  it("does not offer replay when the server says this caller may not set seeds", async () => {
    const { client, runId } = await startedRun(50);
    const noSeeds: ApiClient = { ...client, login: async (k) => ({ ...(await client.login(k)), maySetSeeds: false }) };
    await renderApp({ route: `/runs/${runId}`, role: "client", client: noSeeds });
    await screen.findByRole("link", { name: /view results/i });
    expect(screen.queryByRole("button", { name: /replay/i })).not.toBeInTheDocument();
  });

  it("does not offer replay to the demo role (the server forbids custom configs for demo)", async () => {
    const client = createMockClient({ runsPerTick: 50, tickMs: 5 });
    await client.login("demo");
    const { runId } = await client.startRun({ kind: "predefined", compositionId: "dc_small_mixed" });
    await renderApp({ route: `/runs/${runId}`, role: "demo", client });
    await screen.findByRole("link", { name: /view results/i });
    expect(screen.getByRole("table", { name: /seeds/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /replay/i })).not.toBeInTheDocument();
  });
});
