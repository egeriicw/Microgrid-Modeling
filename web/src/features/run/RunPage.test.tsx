import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { createMockClient } from "../../api/mockClient";
import { ApiError, type ApiClient, type Composition } from "../../api/types";
import { renderApp } from "../../test/utils";

const comp: Composition = {
  name: "t",
  totalBuildings: 10,
  seeds: { composition_seed: 1, archetype_seed: 2, perturbation_seed: 3 },
  maxRuns: 100,
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
      getRun: async (id) => ({ runId: id, state: "queued", stage: "Queued", maxRuns: null, completedRuns: 0, feasible: 0, rejected: 0, converged: false, seeds: null, config: null, engineVersion: "x", cancelRequested: false }),
      watchRun: () => new Promise(() => undefined),
    };
    const { runId } = await base.startRun({ kind: "custom", composition: comp });
    await renderApp({ route: `/runs/${runId}`, role: "client", client });
    const bar = await screen.findByRole("progressbar", { name: /run progress/i });
    expect(bar).not.toHaveAttribute("aria-valuenow");
    expect(screen.getByTestId("stage-text")).toHaveTextContent(/queued/i);
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
