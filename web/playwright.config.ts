import { defineConfig } from "@playwright/test";

// e2e runs against the Vite dev server with the mock API client (VITE_API_MODE=mock; no backend needed).
export default defineConfig({
  testDir: "./e2e",
  webServer: {
    command: "npm run dev -- --port 5173",
    url: "http://localhost:5173",
    reuseExistingServer: true,
    env: { VITE_API_MODE: "mock" },
  },
  use: { baseURL: "http://localhost:5173" },
});
