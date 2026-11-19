// Which ApiClient the app uses (contract with the deploy story):
//   VITE_API_MODE=mock      -> fixture mock client (no backend needed)
//   anything else / unset   -> real API at VITE_API_BASE_URL, default "/api/v1" (same origin; nginx in the container
//                              and the Vite dev server proxy /api to the API).
import { HttpApiClient } from "./httpClient";
import { createMockClient } from "./mockClient";
import type { ApiClient } from "./types";

export interface ApiEnv {
  VITE_API_MODE?: string;
  VITE_API_BASE_URL?: string;
}

export function resolveApiConfig(env: ApiEnv): { mode: "mock" | "http"; baseUrl: string } {
  return { mode: env.VITE_API_MODE === "mock" ? "mock" : "http", baseUrl: env.VITE_API_BASE_URL || "/api/v1" };
}

export function createClientFromEnv(env: ApiEnv): ApiClient {
  const cfg = resolveApiConfig(env);
  return cfg.mode === "mock" ? createMockClient({ persistSession: true }) : new HttpApiClient({ baseUrl: cfg.baseUrl });
}
