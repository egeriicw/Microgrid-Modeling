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
    expect(await screen.findByText(/neighbourhoods simulated \(runs\): [1-9]\d* of 100/i)).toBeInTheDocument();
    expect(screen.getByText(/^valid runs: \d+ \(sampled neighbourhoods that fit the rules\)$/i)).toBeInTheDocument();
    expect(screen.getByText(/^rejected runs: \d+ \(neighbourhoods that could not be built and were skipped\)$/i)).toBeInTheDocument();
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

describe("plain-language status (U1, U3, U9)", () => {
  const status = (over: Partial<RunStatus>): RunStatus => ({
    runId: "r1",
    state: "running",
    stage: "run 3/10 feasible",
    completedRuns: 3,
    maxRuns: 10,
    feasible: 3,
    rejected: 0,
    converged: null,
    seeds: { composition_seed: 1, archetype_seed: 2, perturbation_seed: 3 },
    config: { ensemble: { max_runs: 10 } },
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
    await renderApp({ route: "/runs/r1", role: "client", client });
    await screen.findByText(/neighbourhoods simulated/i);
  }

  it.each([
    ["queued" as const, "Estimate in progress"],
    ["running" as const, "Estimate in progress"],
    ["succeeded" as const, "Estimate finished"],
    ["cancelled" as const, "Estimate cancelled"],
    ["failed" as const, "Estimate failed"],
  ])("a %s run is headed %j, with the run id as secondary text and the one-sentence explanation", async (state, heading) => {
    await show(status({ state }));
    expect(screen.getByRole("heading", { level: 1, name: heading })).toBeInTheDocument();
    expect(screen.getByText("Run r1")).toBeInTheDocument();
    expect(
      screen.getByText("Each run builds one plausible neighbourhood by sampling real buildings; the results show the spread across all of them."),
    ).toBeInTheDocument();
  });

  it("describes a finished run in plain words, never the raw word 'finished' alone", async () => {
    await show(status({ state: "succeeded", stage: "finished", completedRuns: 10, feasible: 10, rejected: 0 }));
    expect(screen.getByTestId("stage-text")).toHaveTextContent("Finished. All 10 sampled neighbourhoods were simulated.");
    expect(screen.queryByText(/^finished$/i)).toBeNull();
  });

  it("a finished run with skipped neighbourhoods says how many were valid and skipped", async () => {
    await show(status({ state: "succeeded", completedRuns: 10, feasible: 8, rejected: 2 }));
    expect(screen.getByTestId("stage-text")).toHaveTextContent(
      "Finished. 10 sampled neighbourhoods were tried; 8 were valid and 2 could not be built and were skipped.",
    );
  });

  it("does not show the engine's 'finished' message as a step while the run is still marked running", async () => {
    await show(status({ stage: "finished: 10 feasible of 10 attempted" }));
    expect(screen.queryByText(/finished: 10 feasible/i)).toBeNull();
  });

  it("a queued run says it is waiting (queued)", async () => {
    await show(status({ state: "queued" }));
    expect(screen.getByTestId("stage-text")).toHaveTextContent("Waiting for the server to start this estimate (queued).");
  });

  it("a cancelled run: status line, no View results, and 'Start again with the same seeds'", async () => {
    await show(status({ state: "cancelled", completedRuns: 4, maxRuns: 10 }));
    expect(screen.getByTestId("stage-text")).toHaveTextContent("Cancelled at 4 of 10 runs. No results were saved.");
    expect(screen.queryByRole("link", { name: /view results/i })).toBeNull();
    expect(screen.getByRole("button", { name: "Start again with the same seeds" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /replay with same seeds/i })).toBeNull();
  });

  it("the cancelled run's restart button follows the same permission rule as replay", async () => {
    const base = createMockClient();
    const s = status({ state: "cancelled" });
    const client: ApiClient = {
      ...base,
      login: async (k) => ({ ...(await base.login(k)), maySetSeeds: false }),
      getRun: async () => s,
      watchRun: () => new Promise(() => undefined),
    };
    await renderApp({ route: "/runs/r1", role: "client", client });
    await screen.findByText(/neighbourhoods simulated/i);
    expect(screen.queryByRole("button", { name: /start again/i })).toBeNull();
  });

  it("a failed run shows the server's reason as text and that no results were saved", async () => {
    await show(status({ state: "failed", message: "engine run failed <b>(ValueError)</b>" }));
    expect(screen.getByRole("alert")).toHaveTextContent("engine run failed <b>(ValueError)</b> No results were saved.");
    expect(screen.getByRole("alert").querySelector("b")).toBeNull();
  });

  it("a finished run shows the one-line accuracy caveat next to View results", async () => {
    await show(status({ state: "succeeded", completedRuns: 10 }));
    expect(screen.getByRole("note")).toHaveTextContent(
      "Methodology demonstration with benchmark checks — not validated for engineering or investment decisions.",
    );
  });

  it("no caveat while the run is still going", async () => {
    await show(status({}));
    expect(screen.queryByRole("note")).toBeNull();
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
    await screen.findByText(/neighbourhoods simulated/i);
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

  it("names the datasets and files in plain words and says when no temperature source was set up", async () => {
    const { panel } = await showFiles(files({}));
    expect(panel).toHaveTextContent("Homes: NREL ResStock building characteristics (file DC_upgrade0.xlsx)");
    expect(panel).toHaveTextContent("Commercial buildings: NREL ComStock building characteristics (file DC_upgrade0_agg.xlsx)");
    expect(panel).toHaveTextContent(/no temperature source was set up for this run, so the results have no temperature/i);
  });

  it("describes a temperature column in the building data; the column name only inside the collapsed technical details", async () => {
    const { panel } = await showFiles(files({ source: "timeseries_column", column: "out.temp_c", units: "C" }));
    expect(within(panel!).getByText(/taken from each sampled building's own load data, in degrees Celsius/i)).toBeInTheDocument();
    const details = panel!.querySelector("details")!;
    expect(details.open).toBe(false);
    expect(within(details).getByText(/^technical details$/i).tagName).toBe("SUMMARY");
    expect(within(details).getByText("Temperature column: out.temp_c")).toBeInTheDocument();
    const outside = Array.from(panel!.querySelectorAll("li")).filter((li) => !details.contains(li));
    expect(outside.map((li) => li.textContent).join(" ")).not.toContain("out.temp_c");
  });

  it("describes a weather file", async () => {
    const { panel } = await showFiles(files({ source: "weather_file", file: "weather_2018.csv", column: "temp_f", timeColumn: "timestamp", units: "F" }));
    expect(panel).toHaveTextContent(/taken from one weather file \(file weather_2018\.csv\), in degrees Fahrenheit \(converted to Celsius in the results\)/i);
    expect(within(panel!.querySelector("details")!).getByText("Time column: timestamp")).toBeInTheDocument();
  });

  it("says when the run did not record a source or a file", async () => {
    const { panel } = await showFiles({ resstockCharacteristicsFile: null, comstockCharacteristicsFile: null, temperature: { source: null, file: null, column: null, timeColumn: null, units: null } });
    expect(panel).toHaveTextContent(/homes: NREL ResStock building characteristics \(file not recorded for this run\)/i);
    expect(panel).toHaveTextContent(/outdoor temperature: not recorded for this run/i);
  });

  it("renders file names as text, never markup", async () => {
    const { view } = await showFiles(files({ source: "weather_file", file: "<img src=x onerror=alert(1)>.csv", column: "t", timeColumn: "ts", units: "C" }));
    expect(view.container.querySelector("main img")).toBeNull();
    expect(screen.getAllByText(/<img src=x onerror=alert\(1\)>\.csv/).length).toBeGreaterThan(0);
  });

  it("shows no panel when the server does not report data files", async () => {
    await showFiles(null);
    await screen.findByText(/neighbourhoods simulated/i);
    expect(screen.queryByRole("region", { name: /data used/i })).toBeNull();
  });
});

describe("seeds and replay (FR-ENG-4 d)", () => {
  it("shows the three seeds the run used", async () => {
    const { client, runId } = await startedRun(50);
    await renderApp({ route: `/runs/${runId}`, role: "client", client });
    const seeds = await screen.findByRole("table", { name: /seeds/i });
    expect(screen.getByRole("heading", { name: "Random seeds (only needed to repeat this run exactly)" })).toBeInTheDocument();
    for (const [label, why, tech] of [
      ["Building mix seed", "Which mix of building types is drawn.", "composition_seed"],
      ["Building pick seed", "Which individual buildings are picked.", "archetype_seed"],
      ["Variation seed", "Small random variations applied to each building.", "perturbation_seed"],
    ]) {
      expect(within(seeds).getByText(label)).toBeInTheDocument();
      expect(within(seeds).getByText(why)).toBeInTheDocument();
      expect(within(seeds).getByText(tech)).toBeInTheDocument();
    }
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
    await waitFor(() => expect(screen.getByText("Run run-2")).toBeInTheDocument());
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
