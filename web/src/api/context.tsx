import { createContext, useContext, type ReactNode } from "react";
import type { ApiClient } from "./types";

const Ctx = createContext<ApiClient | null>(null);

export function ApiClientProvider({ client, children }: { client: ApiClient; children: ReactNode }) {
  return <Ctx.Provider value={client}>{children}</Ctx.Provider>;
}

export function useApiClient(): ApiClient {
  const c = useContext(Ctx);
  if (!c) throw new Error("useApiClient: no ApiClientProvider above this component");
  return c;
}
