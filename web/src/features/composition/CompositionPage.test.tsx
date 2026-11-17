import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { createMockClient } from "../../api/mockClient";
import { ApiError, type ApiClient } from "../../api/types";
import { renderApp } from "../../test/utils";

async function fillValid(user: ReturnType<typeof userEvent.setup>) {
  await user.clear(screen.getByLabelText(/composition name/i));
  await user.type(screen.getByLabelText(/composition name/i), "Test block");
  await user.clear(screen.getByLabelText(/total buildings/i));
  await user.type(screen.getByLabelText(/total buildings/i), "50");
}

describe("composition form (FR-WEB-1)", () => {
  it("lists every building type from the API with a mode and a min/max fraction", async () => {
    await renderApp({ route: "/composition", role: "client" });
    expect(await screen.findByLabelText(/single-family detached min fraction/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/single-family detached max fraction/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/office min fraction/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/composition seed/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/archetype seed/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/perturbation seed/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/maximum runs/i)).toBeInTheDocument();
  });

  it("shows live validation messages and disables Start while invalid", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await screen.findByLabelText(/total buildings/i);
    await user.clear(screen.getByLabelText(/total buildings/i));
    await user.type(screen.getByLabelText(/total buildings/i), "0");
    expect(await screen.findByText(/at least 1/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /start run/i })).toBeDisabled();
  });

  it("flags minimum fractions that add up to more than 1", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    for (const t of ["single-family detached", "multifamily"]) {
      const min = await screen.findByLabelText(new RegExp(`^${t} min fraction`, "i"));
      await user.clear(min);
      await user.type(min, "0.9");
    }
    expect(await screen.findByText(/minimum.*more than 1/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /start run/i })).toBeDisabled();
  });

  it("switching a type to a fixed count shows a count field and applies the remainder rule", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await user.selectOptions(await screen.findByLabelText(/single-family detached mode/i), "count");
    const count = screen.getByLabelText(/single-family detached count/i);
    expect(screen.queryByLabelText(/single-family detached min fraction/i)).not.toBeInTheDocument();
    await user.clear(count);
    await user.type(count, "60"); // 60 fixed of 100; the other types share the remaining 40
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    await user.clear(count);
    await user.type(count, "101");
    expect(await screen.findByText(/exceed/i)).toBeInTheDocument();
  });

  it("starts a run and goes to the run screen", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await screen.findByLabelText(/total buildings/i);
    await fillValid(user);
    await user.click(screen.getByRole("button", { name: /start run/i }));
    expect(await screen.findByRole("progressbar")).toBeInTheDocument();
  });

  it("hides the building filters from client but shows them to admin", async () => {
    const { unmount } = await renderApp({ route: "/composition", role: "client" });
    await screen.findByLabelText(/total buildings/i);
    expect(screen.queryByLabelText(/vintage/i)).not.toBeInTheDocument();
    unmount();
    await renderApp({ route: "/composition", role: "admin" });
    await waitFor(() => expect(screen.getByLabelText(/vintage/i)).toBeInTheDocument());
  });

  it("lets the demo role run only the predefined compositions", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "demo" });
    expect(await screen.findByText(/demo accounts can run the predefined compositions only/i)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /start run/i })).not.toBeInTheDocument();
    await user.click(await screen.findByRole("button", { name: /run dc_small_mixed/i }));
    expect(await screen.findByRole("progressbar", { name: /run progress/i })).toBeInTheDocument();
  });

  it("sends optional seeds; blank seeds are left to the server", async () => {
    const user = userEvent.setup();
    const base = createMockClient();
    const startRun = vi.fn(base.startRun);
    await renderApp({ route: "/composition", role: "client", client: { ...base, startRun } });
    await screen.findByLabelText(/total buildings/i);
    await user.type(screen.getByLabelText(/composition seed/i), "42");
    await user.click(screen.getByRole("button", { name: /start run/i }));
    await screen.findByRole("progressbar", { name: /run progress/i });
    const req = startRun.mock.calls[0][0];
    expect(req.kind === "custom" && req.composition.seeds).toEqual({ composition_seed: 42 });
  });

  it("rejects a seed outside 0..2^32-1 before sending", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await user.type(await screen.findByLabelText(/archetype seed/i), "4294967296");
    expect(await screen.findByText(/at most 4294967295/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /start run/i })).toBeDisabled();
  });

  it("shows the server's 422 field messages", async () => {
    const user = userEvent.setup();
    const base = createMockClient();
    const startRun = vi.fn(async () => {
      throw new ApiError(422, "composition.total_buildings: too many", [{ field: "composition.total_buildings", message: "exceeds the server cap of 500" }]);
    });
    await renderApp({ route: "/composition", role: "client", client: { ...base, startRun } });
    await screen.findByLabelText(/total buildings/i);
    await user.click(screen.getByRole("button", { name: /start run/i }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("composition.total_buildings");
    expect(alert).toHaveTextContent("exceeds the server cap of 500");
  });

  it("explains when the API cannot list building types, and keeps predefined runs available", async () => {
    const base = createMockClient();
    const client: ApiClient = {
      ...base,
      listBuildingTypes: async () => {
        throw new ApiError(501, "The API does not offer the building-type list yet");
      },
    };
    await renderApp({ route: "/composition", role: "client", client });
    expect(await screen.findByText(/does not offer the building-type list/i)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /start run/i })).not.toBeInTheDocument();
    expect(await screen.findByRole("button", { name: /run dc_small_mixed/i })).toBeInTheDocument();
  });
});
