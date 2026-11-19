// QA W1-WEB2: run e2e/qa-w1web2.spec.ts against a REAL API behind the Vite dev proxy (real client, no mock).
// Start the API on 127.0.0.1:8000 with the synthetic fixture and `npx vite --port 5174` (VITE_API_MODE unset), then:
//   QA_W1WEB2=1 [PW_CHROMIUM_EXECUTABLE=/path/to/chrome] npx playwright test -c e2e/qa-w1web2.config.ts
// The key-revocation test also needs QA_REVOKE=1 and someone restarting the API without the client key meanwhile.
import { defineConfig } from "@playwright/test";

const env = (globalThis as unknown as { process: { env: Record<string, string | undefined> } }).process.env;
const exe = env.PW_CHROMIUM_EXECUTABLE;
export default defineConfig({
  testDir: ".",
  testMatch: /qa-w1web2\.spec\.ts$/,
  timeout: 180_000,
  use: { baseURL: env.QA_WEB_URL ?? "http://localhost:5174", ...(exe ? { launchOptions: { executablePath: exe } } : {}) },
});
