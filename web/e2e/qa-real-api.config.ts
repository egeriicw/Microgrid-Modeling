// QA: run e2e/qa-real-api.spec.ts against a REAL API behind the Vite dev proxy (real client, no mock).
// Start the API on 127.0.0.1:8000 and `npx vite --port 5174` (VITE_API_MODE unset), then:
//   QA_REAL_API=1 [PW_CHROMIUM_EXECUTABLE=/path/to/chrome] npx playwright test -c e2e/qa-real-api.config.ts
// Keys are the ones the QA launcher configures (see the spec). Skipped unless QA_REAL_API is set.
import { defineConfig } from "@playwright/test";

// Node globals without @types/node (the web tsconfig has no node types).
const env = (globalThis as unknown as { process: { env: Record<string, string | undefined> } }).process.env;

const exe = env.PW_CHROMIUM_EXECUTABLE;
export default defineConfig({
  testDir: ".",
  testMatch: /(qa-real-api|real-api-compose)\.spec\.ts$/,
  use: { baseURL: env.QA_WEB_URL ?? "http://localhost:5174", ...(exe ? { launchOptions: { executablePath: exe } } : {}) },
});
