import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { renderApp } from "../../test/utils";

async function fillValid(user: ReturnType<typeof userEvent.setup>) {
  await user.clear(screen.getByLabelText(/composition name/i));
  await user.type(screen.getByLabelText(/composition name/i), "Test block");
  await user.clear(screen.getByLabelText(/total buildings/i));
  await user.type(screen.getByLabelText(/total buildings/i), "50");
}

describe("composition form (FR-WEB-1)", () => {
  it("lists every building type from the API with a mode and value", async () => {
    await renderApp({ route: "/composition", role: "client" });
    expect(await screen.findByLabelText(/single-family detached value/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/office value/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/random seed/i)).toBeInTheDocument();
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

  it("flags fractions that do not sum to 1", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    const sf = await screen.findByLabelText(/single-family detached value/i);
    await user.clear(sf);
    await user.type(sf, "0.2");
    expect(await screen.findByText(/sum to 1/i)).toBeInTheDocument();
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

  it("is read-only for the demo role", async () => {
    await renderApp({ route: "/composition", role: "demo" });
    expect(await screen.findByText(/demo accounts cannot start runs/i)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /start run/i })).not.toBeInTheDocument();
  });
});
