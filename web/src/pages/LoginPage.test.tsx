import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { createMockClient } from "../api/mockClient";
import { ApiError, type ApiClient } from "../api/types";
import { renderApp } from "../test/utils";

describe("sign-in with an API key (FR-WEB-5)", () => {
  it("redirects signed-out visitors to the sign-in page", async () => {
    await renderApp({ route: "/composition" });
    expect(await screen.findByRole("heading", { name: /sign in/i })).toBeInTheDocument();
  });

  it("takes a pasted key in a password field and goes to the composition screen", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/login" });
    const key = screen.getByLabelText(/api key/i);
    expect(key).toHaveAttribute("type", "password");
    expect(key).toHaveAttribute("autocomplete", "off");
    await user.type(key, "client");
    await user.click(screen.getByRole("button", { name: /sign in/i }));
    expect(await screen.findByRole("heading", { name: /define a community composition/i })).toBeInTheDocument();
  });

  it("shows the server's message for a rejected key and stays on the sign-in page", async () => {
    const user = userEvent.setup();
    await renderApp({ route: "/login" });
    await user.type(screen.getByLabelText(/api key/i), "not-a-key");
    await user.click(screen.getByRole("button", { name: /sign in/i }));
    expect(await screen.findByText(/not accepted/i)).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /sign in/i })).toBeInTheDocument();
  });

  it("shows a progress indicator while the key is checked", async () => {
    const user = userEvent.setup();
    const base = createMockClient();
    let release!: () => void;
    const slow: ApiClient = { ...base, login: (k) => new Promise((r) => (release = () => r(base.login(k)))) };
    await renderApp({ route: "/login", client: slow });
    await user.type(screen.getByLabelText(/api key/i), "client");
    await user.click(screen.getByRole("button", { name: /sign in/i }));
    expect(screen.getByRole("progressbar", { name: /checking/i })).toBeInTheDocument();
    await act(async () => release());
    expect(await screen.findByRole("heading", { name: /define a community composition/i })).toBeInTheDocument();
  });

  it("explains mock mode only when the mock client is in use", async () => {
    await renderApp({ route: "/login" });
    expect(screen.getByText(/mock mode/i)).toBeInTheDocument();
  });

  it("returns to the sign-in page when the server rejects the key later (401)", async () => {
    const base = createMockClient({ pointsPerYear: 96 * 4 });
    let signOut!: () => void;
    const client: ApiClient = {
      ...base,
      onSignedOut: (l) => {
        signOut = l;
        return () => undefined;
      },
      getRun: vi.fn(async () => {
        throw new ApiError(401, "Your API key was not accepted. Sign in again.");
      }),
    };
    await renderApp({ route: "/runs/x", role: "client", client });
    await act(async () => signOut());
    expect(await screen.findByRole("heading", { name: /sign in/i })).toBeInTheDocument();
  });
});
