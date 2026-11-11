import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { createMockClient } from "../../api/mockClient";
import type { Composition } from "../../api/types";
import { renderApp } from "../../test/utils";

const comp: Composition = {
  name: "t",
  totalBuildings: 10,
  seed: 1,
  maxRuns: 100,
  entries: [{ typeId: "sf", mode: "fraction", value: 1 }],
};

async function startedRun(runsPerPoll: number) {
  const client = createMockClient({ runsPerPoll, pointsPerYear: 96 * 20 });
  await client.login("client");
  const { runId } = await client.startRun(comp);
  return { client, runId };
}

describe("run screen (FR-WEB-2)", () => {
  it("shows a progress bar, stage text, runs completed and feasible/rejected counts", async () => {
    const { client, runId } = await startedRun(1);
    await renderApp({ route: `/runs/${runId}`, role: "client", client });
    const bar = await screen.findByRole("progressbar", { name: /run progress/i });
    expect(bar).toHaveAttribute("aria-valuemin", "0");
    expect(bar).toHaveAttribute("aria-valuemax", "100");
    expect(await screen.findByText(/runs completed/i)).toBeInTheDocument();
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
    expect(screen.getByText(/converged/i)).toBeInTheDocument();
  });

  it("cancels a run", async () => {
    const user = userEvent.setup();
    const { client, runId } = await startedRun(1);
    await renderApp({ route: `/runs/${runId}`, role: "client", client });
    await user.click(await screen.findByRole("button", { name: /cancel/i }));
    await waitFor(() => expect(screen.getByText(/cancelled/i)).toBeInTheDocument());
    expect(screen.queryByRole("link", { name: /view results/i })).not.toBeInTheDocument();
  });

  it("shows an error for an unknown run instead of spinning forever", async () => {
    await renderApp({ route: "/runs/nope", role: "client" });
    expect(await screen.findByRole("alert")).toHaveTextContent(/not found/i);
  });
});
