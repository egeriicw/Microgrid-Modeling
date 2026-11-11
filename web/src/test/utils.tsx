import { render } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { AppRoutes } from "../App";
import { ApiClientProvider } from "../api/context";
import { createMockClient, type MockOptions } from "../api/mockClient";
import type { ApiClient, Role, Session } from "../api/types";
import { AuthProvider } from "../auth/AuthContext";

export interface RenderAppOptions {
  route?: string;
  /** Pre-authenticated role; omit to start signed out. */
  role?: Role;
  mock?: MockOptions;
  client?: ApiClient;
}

/** Render the whole app on the mock client. Polling is fast so progress tests finish quickly. */
export async function renderApp(opts: RenderAppOptions = {}) {
  const client = opts.client ?? createMockClient({ pointsPerYear: 96 * 20, ...opts.mock });
  let session: Session | null = null;
  if (opts.role) session = await client.login(opts.role);
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  const view = render(
    <QueryClientProvider client={qc}>
      <ApiClientProvider client={client}>
        <AuthProvider initialSession={session}>
          <MemoryRouter initialEntries={[opts.route ?? "/"]}>
            <AppRoutes pollMs={5} />
          </MemoryRouter>
        </AuthProvider>
      </ApiClientProvider>
    </QueryClientProvider>,
  );
  return { client, ...view };
}
