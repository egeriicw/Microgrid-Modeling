import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { renderApp } from "./test/utils";

describe("role-aware shell", () => {
  it("shows Compose and Results navigation to a client", async () => {
    await renderApp({ route: "/composition", role: "client" });
    const nav = await screen.findByRole("navigation");
    expect(nav).toHaveTextContent(/compose/i);
    expect(nav).toHaveTextContent(/demo results/i);
    expect(screen.getByText(/signed in as client/i)).toBeInTheDocument();
  });

  it("hides Compose from demo", async () => {
    await renderApp({ route: "/results/demo-run", role: "demo" });
    const nav = await screen.findByRole("navigation");
    expect(nav).not.toHaveTextContent(/compose/i);
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
