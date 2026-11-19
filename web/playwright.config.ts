import { defineConfig } from "@playwright/test";

// e2e runs against the Vite dev server with the mock API client (VITE_API_MODE=mock; no backend needed).
// PW_CHROMIUM_EXECUTABLE (optional): path to an already-installed Chromium, for machines whose browser build does not
// match this @playwright/test version (e.g. /opt/pw-browsers/chromium-1243/chrome-linux64/chrome).
// Playwright loads this file in Node; declared here so the browser-only tsconfig needs no @types/node.
declare const process: { env: Record<string, string | undefined> };
const executablePath = process.env.PW_CHROMIUM_EXECUTABLE || undefined;

export default defineConfig({
  testDir: "./e2e",
  webServer: {
    command: "npm run dev -- --port 5173",
    url: "http://localhost:5173",
    reuseExistingServer: true,
    env: { VITE_API_MODE: "mock" },
  },
  use: { baseURL: "http://localhost:5173", ...(executablePath ? { launchOptions: { executablePath } } : {}) },
});
