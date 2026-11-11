import { defineConfig } from "@playwright/test";

// e2e runs against the Vite dev server, which uses the mock API client (no backend needed).
export default defineConfig({
  testDir: "./e2e",
  webServer: { command: "npm run dev -- --port 5173", url: "http://localhost:5173", reuseExistingServer: true },
  use: { baseURL: "http://localhost:5173" },
});
