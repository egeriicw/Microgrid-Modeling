import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { renderApp } from "../test/utils";

describe("login (mocked auth)", () => {
  it("redirects signed-out visitors to the sign-in page", async () => {
    await renderApp({ route: "/composition" });
    expect(await screen.findByRole("heading", { name: /sign in/i })).toBeInTheDocument();
  });

  it("offers the three roles and sends client/admin to the composition form", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/login" });
    expect(screen.getByRole("radio", { name: /admin/i })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: /client/i })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: /demo/i })).toBeInTheDocument();
    await user.click(screen.getByRole("radio", { name: /client/i }));
    await user.click(screen.getByRole("button", { name: /sign in/i }));
    expect(await screen.findByRole("heading", { name: /composition/i })).toBeInTheDocument();
  });

  it("sends a demo user straight to the predefined demo results", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/login" });
    await user.click(screen.getByRole("radio", { name: /demo/i }));
    await user.click(screen.getByRole("button", { name: /sign in/i }));
    await waitFor(() => expect(screen.getByRole("heading", { name: /results/i })).toBeInTheDocument());
  });

  it("says the sign-in is a mock and the server enforces roles", async () => {
    await renderApp({ route: "/login" });
    expect(screen.getByText(/mock sign-in/i)).toBeInTheDocument();
  });
});
