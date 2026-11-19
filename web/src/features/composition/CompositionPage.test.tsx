import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { createMockClient } from "../../api/mockClient";
import { ApiError, type ApiClient, type RunRequest } from "../../api/types";
import { renderApp } from "../../test/utils";

type User = ReturnType<typeof userEvent.setup>;

async function setText(user: User, label: RegExp, value: string) {
  const el = screen.getByLabelText(label);
  await user.clear(el);
  if (value) await user.type(el, value);
}

async function include(user: User, label: string) {
  await user.click(await screen.findByRole("checkbox", { name: new RegExp(`^include ${label}`, "i") }));
}

/** A client whose startRun is spied on (the mock still answers). */
function spied() {
  const base = createMockClient();
  const startRun = vi.fn(base.startRun);
  return { client: { ...base, startRun } as ApiClient, startRun };
}

describe("composition form (FR-WEB-1) on the server's catalogue", () => {
  it("groups the building types into residential and commercial, with inactive types disabled and explained", async () => {
    await renderApp({ route: "/composition", role: "client" });
    const res = await screen.findByRole("group", { name: /residential building types/i });
    const com = screen.getByRole("group", { name: /commercial building types/i });
    expect(within(res).getByRole("checkbox", { name: /include single-family detached/i })).toBeEnabled();
    expect(within(com).getByRole("checkbox", { name: /include office/i })).toBeEnabled();
    const civic = within(com).getByRole("checkbox", { name: /include civic building/i });
    expect(civic).toBeDisabled();
    expect(within(com).getByText(/not available yet/i)).toBeInTheDocument();
  });

  it("an included type has a switch between share range and exact number", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    expect(screen.queryByLabelText(/smallest share \(0 to 1\): single-family detached/i)).toBeNull();
    await include(user, "single-family detached");
    expect(screen.getByLabelText(/smallest share \(0 to 1\): single-family detached/i)).toHaveValue("0");
    expect(screen.getByLabelText(/largest share \(0 to 1\): single-family detached/i)).toHaveValue("1");
    await user.selectOptions(screen.getByLabelText(/how many\? single-family detached/i), "count");
    expect(screen.getByLabelText(/number of buildings: single-family detached/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/smallest share \(0 to 1\): single-family detached/i)).toBeNull();
  });

  it("asks for at least one type, and disables Start while the form is invalid", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    expect(await screen.findByText(/include at least one building type/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /start run/i })).toBeDisabled();
    await include(user, "office");
    expect(screen.getByRole("button", { name: /start run/i })).toBeEnabled();
    await setText(user, /total buildings/i, "0");
    expect(await screen.findByText(/at least 1/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /start run/i })).toBeDisabled();
  });

  it("flags minimum shares that add up to more than 1", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    for (const t of ["single-family detached", "multifamily"]) {
      await include(user, t);
      await setText(user, new RegExp(`smallest share \\(0 to 1\\): ${t}$`, "i"), "0.9");
    }
    expect(await screen.findByText(/smallest shares add up to more than 1/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /start run/i })).toBeDisabled();
  });

  it("flags exact numbers above the total", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await include(user, "single-family detached");
    await include(user, "office");
    await user.selectOptions(screen.getByLabelText(/how many\? single-family detached/i), "count");
    await setText(user, /number of buildings: single-family detached/i, "60");
    expect(screen.queryByText(/exceeds/i)).toBeNull();
    await setText(user, /number of buildings: single-family detached/i, "101");
    expect(await screen.findByText(/exceeds the total/i)).toBeInTheDocument();
  });

  it("uses the server's limits (GET /me): total cap, run cap and the convergence minimum", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await include(user, "office");
    await setText(user, /total buildings/i, "5001");
    expect(await screen.findByText(/at most 5000/i)).toBeInTheDocument();
    await setText(user, /total buildings/i, "50");
    await setText(user, /number of runs \(at most\)/i, "10");
    expect(screen.getByRole("button", { name: /start run/i })).toBeEnabled();
    await user.click(screen.getByRole("switch", { name: /convergence/i }));
    expect(await screen.findByText(/number of runs must be at least 20/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /start run/i })).toBeDisabled();
  });

  it("starts a hybrid composition (exact numbers and share ranges) and sends only the included types, no convergence rule", async () => {
    const user = userEvent.setup();
    const { client, startRun } = spied();
    await renderApp({ route: "/composition", role: "client", client });
    await setText(user, /composition name/i, "Block A");
    await setText(user, /total buildings/i, "40");
    await include(user, "single-family detached");
    await include(user, "multifamily");
    await include(user, "office");
    await user.selectOptions(screen.getByLabelText(/how many\? single-family detached/i), "count");
    await setText(user, /number of buildings: single-family detached/i, "10");
    await setText(user, /smallest share \(0 to 1\): multifamily/i, "0.25");
    await setText(user, /largest share \(0 to 1\): office/i, "0.5");
    await setText(user, /number of runs \(at most\)/i, "30");
    await user.click(screen.getByRole("switch", { name: /convergence/i }));
    await user.click(screen.getByRole("button", { name: /start run/i }));
    expect(await screen.findByRole("progressbar", { name: /run progress/i })).toBeInTheDocument();
    const req = startRun.mock.calls[0][0] as RunRequest;
    expect(req).toEqual({
      kind: "custom",
      composition: {
        name: "Block A",
        totalBuildings: 40,
        seeds: {},
        maxRuns: 30,
        convergence: true,
        entries: [
          { typeId: "sf", mode: "count", count: 10 },
          { typeId: "mf", mode: "fraction", minFraction: 0.25, maxFraction: 1 },
          { typeId: "office", mode: "fraction", minFraction: 0, maxFraction: 0.5 },
        ],
      },
    });
  });

  it("sends optional seeds; blank seeds are left to the server", async () => {
    const user = userEvent.setup();
    const { client, startRun } = spied();
    await renderApp({ route: "/composition", role: "client", client });
    await include(user, "office");
    await user.type(screen.getByLabelText(/building mix seed/i), "42");
    await user.click(screen.getByRole("button", { name: /start run/i }));
    await screen.findByRole("progressbar", { name: /run progress/i });
    const req = startRun.mock.calls[0][0];
    expect(req.kind === "custom" && req.composition.seeds).toEqual({ composition_seed: 42 });
  });

  it("rejects a seed outside 0..2^32-1 before sending", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await include(user, "office");
    await user.type(await screen.findByLabelText(/building pick seed/i), "4294967296");
    expect(await screen.findByText(/at most 4294967295/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /start run/i })).toBeDisabled();
  });

  it("hides the seed fields when the server says this caller may not set seeds", async () => {
    const base = createMockClient();
    const client: ApiClient = { ...base, login: async (k) => ({ ...(await base.login(k)), maySetSeeds: false }) };
    await renderApp({ route: "/composition", role: "client", client });
    await screen.findByRole("group", { name: /residential building types/i });
    expect(screen.queryByLabelText(/building mix seed/i)).toBeNull();
  });

  it("shows the server's refusal (422 text) next to the form, as text", async () => {
    const user = userEvent.setup();
    const base = createMockClient();
    const msg = "the server's data has no buildings for 'office', so it cannot be sampled; <b>remove it</b>";
    const client: ApiClient = { ...base, startRun: async () => { throw new ApiError(422, msg); } };
    const { container } = await renderApp({ route: "/composition", role: "client", client });
    await include(user, "office");
    await user.click(screen.getByRole("button", { name: /start run/i }));
    const form = screen.getByRole("form", { name: /your own composition/i });
    expect(await within(form).findByText(msg)).toBeInTheDocument();
    expect(container.querySelector("main b")).toBeNull();
  });

  it("lists the server's 422 field messages", async () => {
    const user = userEvent.setup();
    const base = createMockClient();
    const startRun = vi.fn(async () => {
      throw new ApiError(422, "x", [{ field: "composition.total_buildings", message: "exceeds the server cap of 500" }]);
    });
    await renderApp({ route: "/composition", role: "client", client: { ...base, startRun } });
    await include(user, "office");
    await user.click(screen.getByRole("button", { name: /start run/i }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("composition.total_buildings");
    expect(alert).toHaveTextContent("exceeds the server cap of 500");
  });

  it("explains when the building-type list cannot be loaded, and keeps predefined runs available", async () => {
    const base = createMockClient();
    const client: ApiClient = { ...base, listBuildingTypes: async () => { throw new ApiError(503, "the service is temporarily unavailable"); } };
    await renderApp({ route: "/composition", role: "client", client });
    expect(await screen.findByText(/temporarily unavailable/i)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /start run/i })).not.toBeInTheDocument();
    expect(await screen.findByRole("button", { name: /run this neighbourhood: dc small mixed/i })).toBeInTheDocument();
  });

  it("hides the building filters without the filters.use permission and shows them with it", async () => {
    const { unmount } = await renderApp({ route: "/composition", role: "client" });
    await screen.findByLabelText(/total buildings/i);
    expect(screen.queryByLabelText(/vintage/i)).not.toBeInTheDocument();
    unmount();
    await renderApp({ route: "/composition", role: "admin" });
    await waitFor(() => expect(screen.getByLabelText(/vintage/i)).toBeInTheDocument());
  });

  it("offers the demo role only the predefined compositions: no form, no seed fields", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "demo" });
    expect(await screen.findByText(/this account can run the ready-made neighbourhoods below\. pick one and press run\./i)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /start run/i })).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/total buildings/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/building mix seed/i)).not.toBeInTheDocument();
    await user.click(await screen.findByRole("button", { name: /run this neighbourhood: dc small mixed/i }));
    expect(await screen.findByRole("progressbar", { name: /run progress/i })).toBeInTheDocument();
  });

  it("shows every problem on its own field, naming the building type, linked by aria-describedby, with a count near Start", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await include(user, "single-family detached");
    await include(user, "office");
    await setText(user, /smallest share \(0 to 1\): single-family detached/i, "1.5");
    await setText(user, /largest share \(0 to 1\): office/i, "abc");
    const sf = screen.getByLabelText(/smallest share \(0 to 1\): single-family detached/i);
    const office = screen.getByLabelText(/largest share \(0 to 1\): office/i);
    expect(sf).toHaveAttribute("aria-invalid", "true");
    expect(office).toHaveAttribute("aria-invalid", "true");
    expect(sf).toHaveAccessibleDescription("Single-family detached: the smallest share must be between 0 and 1.");
    expect(office).toHaveAccessibleDescription("Office: enter the largest share as a number from 0 to 1 (for example 0.25).");
    expect(screen.getByRole("status")).toHaveTextContent("There are 2 problems to fix before the run can start. They are marked above.");
    expect(screen.getByRole("button", { name: /start run/i })).toHaveAccessibleDescription(/there are 2 problems/i);
  });

  it("an exact number that is not whole is marked on its own field", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await include(user, "single-family detached");
    await user.selectOptions(screen.getByLabelText(/how many\? single-family detached/i), "count");
    await setText(user, /number of buildings: single-family detached/i, "2.5");
    expect(screen.getByLabelText(/number of buildings: single-family detached/i)).toHaveAccessibleDescription(
      "Single-family detached: the number of buildings must be a whole number, 0 or more.",
    );
    expect(screen.getByRole("status")).toHaveTextContent("There is 1 problem to fix before the run can start. It is marked above.");
  });

  it("accepts '.25' as a share", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await include(user, "office");
    await setText(user, /smallest share \(0 to 1\): office/i, ".25");
    expect(screen.getByLabelText(/smallest share \(0 to 1\): office/i)).not.toHaveAttribute("aria-invalid", "true");
    expect(screen.getByRole("button", { name: /start run/i })).toBeEnabled();
  });

  it("uses plain wording: type names without 'Include', 'How many?' with two plain options, and the inactive note", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    const tick = await screen.findByRole("checkbox", { name: "Include Single-family detached" });
    expect(tick.closest("label")).toHaveTextContent(/^Single-family detached$/);
    expect(screen.getByText("Not available yet: this building type will be added later.")).toBeInTheDocument();
    await user.click(tick);
    const how = screen.getByLabelText(/how many\? single-family detached/i);
    expect(within(how).getByRole("option", { name: "An exact number of buildings" })).toBeInTheDocument();
    expect(within(how).getByRole("option", { name: "A share of the neighbourhood (range)" })).toBeInTheDocument();
    expect(screen.getAllByText("How many?").length).toBeGreaterThan(0); // label and its outline notch
    expect(screen.getAllByText("Smallest share (0 to 1)").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Largest share (0 to 1)").length).toBeGreaterThan(0);
    expect(screen.getByLabelText("Number of runs (at most)")).toBeInTheDocument();
  });

  it("the convergence switch says On or Off in words, not only by colour", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    const sw = await screen.findByRole("switch", { name: "Stop early when the results settle (convergence)" });
    expect(screen.getByTestId("convergence-state")).toHaveTextContent("Off");
    await user.click(sw);
    expect(screen.getByTestId("convergence-state")).toHaveTextContent("On");
  });

  it("seed fields use the plain names and keep the technical name as secondary text", async () => {
    await renderApp({ route: "/composition", role: "client" });
    expect(await screen.findByLabelText("Building mix seed")).toHaveAccessibleDescription(
      "Which mix of building types is drawn. Blank = random. Technical name: composition_seed.",
    );
    expect(screen.getByLabelText("Building pick seed")).toHaveAccessibleDescription(/Technical name: archetype_seed\./);
    expect(screen.getByLabelText("Variation seed")).toHaveAccessibleDescription(/Technical name: perturbation_seed\./);
  });

  it("shows ready-made neighbourhoods with a readable name, the id as secondary text and a plain button", async () => {
    await renderApp({ route: "/composition", role: "client" });
    const section = await screen.findByRole("region", { name: "Ready-made neighbourhoods" });
    expect(within(section).getByText("Dc small mixed")).toBeInTheDocument();
    expect(within(section).getByText("dc_small_mixed")).toBeInTheDocument();
    expect(within(section).getByRole("button", { name: "Run this neighbourhood: Dc small mixed" })).toHaveTextContent(/^Run this neighbourhood$/);
  });
});
