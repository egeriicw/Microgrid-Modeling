import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { createMockClient } from "./api/mockClient";
import type { ApiClient } from "./api/types";
import { renderApp } from "./test/utils";

describe("role-aware shell", () => {
  it("shows Compose and (mock only) Demo results navigation to a client", async () => {
    await renderApp({ route: "/composition", role: "client" });
    const nav = await screen.findByRole("navigation");
    expect(nav).toHaveTextContent(/compose/i);
    expect(nav).toHaveTextContent(/demo results/i);
    expect(screen.getByText(/signed in as client/i)).toBeInTheDocument();
  });

  it("shows Compose to demo too (predefined compositions only)", async () => {
    await renderApp({ route: "/results/demo-run", role: "demo" });
    const nav = await screen.findByRole("navigation");
    expect(nav).toHaveTextContent(/compose/i);
  });

  it("does not invent a role when the server did not report one", async () => {
    const base = createMockClient();
    const client: ApiClient = { ...base, kind: "http", login: async () => ({ role: null, displayName: "API key" }) };
    await renderApp({ route: "/composition", role: "client", client });
    expect(await screen.findByText(/signed in with an api key/i)).toBeInTheDocument();
    expect(screen.queryByText(/signed in as client/i)).not.toBeInTheDocument();
    expect(screen.getByRole("navigation")).not.toHaveTextContent(/demo results/i);
  });

  it("signs out back to the sign-in page", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/composition", role: "client" });
    await user.click(await screen.findByRole("button", { name: /sign out/i }));
    expect(await screen.findByRole("heading", { name: /sign in/i })).toBeInTheDocument();
  });

  it("has a skip link for keyboard users", async () => {
    await renderApp({ route: "/composition", role: "client" });
    expect(await screen.findByRole("link", { name: /skip to main content/i })).toBeInTheDocument();
  });
});
