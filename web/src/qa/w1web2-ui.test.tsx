// W1-WEB2 independent QA: the composition form and run screen follow GET /me's flags, show the server's refusals as
// text near the form, and offer replay only when the server allows it.
import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { createMockClient } from "../api/mockClient";
import { ApiError, type ApiClient, type Composition, type Session } from "../api/types";
import { renderApp } from "../test/utils";

/** A mock client whose /me answer is replaced by `over` (the mock still runs things as a client). */
async function withSession(over: Partial<Session>, extra: Partial<ApiClient> = {}) {
  const base = createMockClient({ runsPerTick: 50, tickMs: 5, pointsPerYear: 96 * 20 });
  const real = await base.login("client");
  const session: Session = { ...real, ...over };
  const client: ApiClient = { ...base, login: async () => session, ...extra };
  return { base, client };
}

const comp: Composition = {
  name: "QA",
  totalBuildings: 10,
  seeds: { composition_seed: 1, archetype_seed: 2, perturbation_seed: 3 },
  maxRuns: 50,
  convergence: false,
  entries: [{ typeId: "sf", mode: "fraction", minFraction: 0, maxFraction: 1 }],
};

describe("QA W1-WEB2: the form follows /me, not the role name", () => {
  it("a 'client' whose /me allows only predefined compositions gets no custom form", async () => {
    const { client } = await withSession({ runInputs: ["composition_id"] });
    await renderApp({ route: "/composition", role: "client", client });
    expect(await screen.findByRole("button", { name: /^run /i })).toBeInTheDocument();
    expect(screen.queryByRole("form", { name: /your own composition/i })).toBeNull();
  });

  // The notice is chosen by the flags but words them as a fact about the role ("Demo accounts ..."), which /me did
  // not say for this caller. LOW: word it from the flags ("This account can run the predefined compositions only").
  it.fails("does not call a non-demo account a demo account", async () => {
    const { client } = await withSession({ runInputs: ["composition_id"] });
    await renderApp({ route: "/composition", role: "client", client });
    await screen.findByRole("button", { name: /^run /i });
    expect(screen.queryByText(/demo accounts/i)).toBeNull();
  });

  it("a 'demo' whose /me allows composing and seeds gets the form with seed fields", async () => {
    const { client } = await withSession({ role: "demo", runInputs: ["composition", "composition_id"], maySetSeeds: true });
    await renderApp({ route: "/composition", role: "client", client });
    expect(await screen.findByRole("form", { name: /your own composition/i })).toBeInTheDocument();
    expect(screen.getByLabelText(/composition seed/i)).toBeInTheDocument();
  });

  it("an unknown role name with every input works like any other", async () => {
    const { client } = await withSession({ role: "auditor" });
    await renderApp({ route: "/composition", role: "client", client });
    expect(await screen.findByRole("form", { name: /your own composition/i })).toBeInTheDocument();
  });

  it("an account with no run inputs sees neither section", async () => {
    const { client } = await withSession({ runInputs: [] });
    await renderApp({ route: "/composition", role: "client", client });
    await screen.findByRole("heading", { name: /define a community composition/i });
    expect(screen.queryByRole("form")).toBeNull();
    expect(screen.queryByRole("region", { name: /predefined compositions/i })).toBeNull();
  });
});

describe("QA W1-WEB2: server refusals appear as text near the form", () => {
  async function submitWith(err: ApiError) {
    const startRun = vi.fn(async () => Promise.reject(err));
    const { client } = await withSession({}, { startRun });
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client", client });
    await user.click(await screen.findByRole("checkbox", { name: /include single-family detached/i }));
    await user.click(screen.getByRole("button", { name: /start run/i }));
    expect(startRun).toHaveBeenCalledTimes(1);
    return screen.getByRole("form", { name: /your own composition/i });
  }

  it.each([
    [422, "the maximum fractions of the ranged types add up to 0.9, less than 1; no mix can satisfy them"],
    [403, "the demo role may only run a predefined composition with its fixed seed"],
    [503, "the run queue is unavailable; try again later"],
  ])("HTTP %i: the server's message is shown inside the form", async (status, message) => {
    const form = await submitWith(new ApiError(status, message));
    expect(await within(form).findByText(message)).toBeInTheDocument();
  });

  it("a 422 field list is shown item by item", async () => {
    const msg = "Value error, name must not contain control, line-separator or text-direction characters";
    const form = await submitWith(new ApiError(422, `composition.name: ${msg}`, [{ field: "composition.name", message: msg }]));
    expect(await within(form).findByText(/the server did not accept the request/i)).toBeInTheDocument();
    expect(within(form).getByText(new RegExp(`composition\\.name: ${msg}`))).toBeInTheDocument();
  });

  it("HTML in a server message is shown as text, never rendered", async () => {
    const message = '<img src=x onerror="window.__pwned=1"><b>bold</b>';
    const form = await submitWith(new ApiError(422, message));
    expect(await within(form).findByText(message)).toBeInTheDocument();
    expect(form.querySelector("img")).toBeNull();
    expect(form.querySelector("b")).toBeNull();
  });

  it("an inactive type cannot be ticked by mouse", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 });
    await renderApp({ route: "/composition", role: "client" });
    const civic = await screen.findByRole("checkbox", { name: /include civic building/i });
    await user.click(civic);
    expect(civic).not.toBeChecked();
    expect(screen.queryByLabelText(/civic building mode/i)).toBeNull();
  });

  it("an inactive type cannot be ticked by keyboard", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    const civic = await screen.findByRole("checkbox", { name: /include civic building/i });
    civic.focus();
    await user.keyboard(" ");
    expect(civic).not.toBeChecked();
  });
});

describe("QA W1-WEB2: replay follows /me", () => {
  async function finishedRun(over: Partial<Session>) {
    const startRun = vi.fn();
    const { base, client } = await withSession(over);
    const { runId } = await base.startRun({ kind: "custom", composition: comp });
    startRun.mockImplementation(base.startRun);
    const spied: ApiClient = { ...client, startRun };
    await renderApp({ route: `/runs/${runId}`, role: "client", client: spied });
    await screen.findByRole("link", { name: /view results/i });
    return { base, runId, startRun };
  }

  it("replays with exactly the stored config and the same three seeds", async () => {
    const { base, runId, startRun } = await finishedRun({});
    const before = await base.getRun(runId);
    await userEvent.setup().click(screen.getByRole("button", { name: /replay with same seeds/i }));
    expect(startRun).toHaveBeenCalledWith({ kind: "replay", config: before.config, seeds: before.seeds });
  });

  it("hidden when /me does not list `config` as a run input", async () => {
    await finishedRun({ runInputs: ["composition", "composition_id"] });
    expect(screen.queryByRole("button", { name: /replay with same seeds/i })).toBeNull();
  });

  it("hidden when /me says the caller may not set seeds", async () => {
    await finishedRun({ maySetSeeds: false });
    expect(screen.queryByRole("button", { name: /replay with same seeds/i })).toBeNull();
  });
});
