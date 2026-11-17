/**
 * QA (W1-WEB): the real client against the real API through the Vite dev proxy. Not part of `npm run e2e`
 * (that runs the mock); run with e2e/qa-real-api.config.ts. Skipped unless QA_REAL_API is set.
 */
import { expect, test, type Page } from "@playwright/test";

// Node globals without @types/node (the web tsconfig has no node types).
const env = (globalThis as unknown as { process: { env: Record<string, string | undefined> } }).process.env;

const KEYS = {
  client: env.QA_CLIENT_KEY ?? "client-key-alice-0123456789",
  demo: env.QA_DEMO_KEY ?? "demo-key-0123456789abcdefgh",
};
test.skip(!env.QA_REAL_API, "needs a running API (QA_REAL_API=1)");

// Any CSP violation (when served with the production CSP, QA_WEB_URL pointing at such a server) fails the test.
const violations: string[] = [];
test.beforeEach(({ page }) => {
  violations.length = 0;
  page.on("console", (m) => {
    if (/content security policy|refused to/i.test(m.text())) violations.push(m.text());
  });
});
test.afterEach(() => {
  expect(violations).toEqual([]);
});

async function signIn(page: Page, key: string) {
  await page.goto("/login");
  await page.getByLabel(/api key/i).fill(key);
  await page.getByRole("button", { name: /sign in/i }).click();
}

async function keyLeaks(page: Page, key: string): Promise<string[]> {
  const where: string[] = [];
  if (page.url().includes(key)) where.push("url");
  if ((await page.content()).includes(key)) where.push("dom");
  const local = await page.evaluate(() => JSON.stringify({ ...localStorage }));
  if (local.includes(key)) where.push("localStorage");
  return where;
}

test("wrong key: plain message, stays on sign-in, nothing stored", async ({ page }) => {
  await signIn(page, "wrong-key-0123456789abcdef");
  await expect(page.getByText(/your api key was not accepted/i)).toBeVisible();
  await expect(page.getByRole("heading", { name: /sign in/i })).toBeVisible();
  expect(await page.evaluate(() => sessionStorage.length + localStorage.length)).toBe(0);
});

test("client key: run a predefined composition, watch it live, results with caveat and honest labels", async ({ page }) => {
  const requests: string[] = [];
  page.on("request", (r) => requests.push(r.url()));
  await signIn(page, KEYS.client);
  await expect(page.getByRole("heading", { name: /define a community composition/i })).toBeVisible();
  expect(await keyLeaks(page, KEYS.client)).toEqual([]);
  await page.getByRole("button", { name: /^run small-mixed$/i }).click();
  await expect(page).toHaveURL(/\/runs\/[0-9a-f-]{36}$/);
  await expect(page.getByRole("table", { name: /seeds/i })).toBeVisible();
  // live stage text from the engine via SSE
  await expect(page.getByTestId("stage-text")).toHaveText(/run \d+\/6|archetype pool ready|convergence/i, { timeout: 30_000 });
  await expect(page.getByRole("link", { name: /view results/i })).toBeVisible({ timeout: 60_000 });
  await expect(page.getByText(/runs completed: 6 of 6/i)).toBeVisible();
  await page.getByRole("link", { name: /view results/i }).click();
  await expect(page.getByText(/not validated for engineering or investment decisions/i)).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText(/calibration status: not reported/i)).toBeVisible();
  await expect(page.locator("canvas").first()).toBeVisible(); // ECharts drew under the CSP
  expect(await keyLeaks(page, KEYS.client)).toEqual([]);
  // every request stayed on the same origin (CSP connect-src 'self')
  const origin = new URL(page.url()).origin;
  expect(requests.filter((u) => !u.startsWith(origin) && !u.startsWith("data:") && !u.startsWith("blob:"))).toEqual([]);
  // sign out removes the key from the tab
  await page.getByRole("button", { name: /sign out/i }).click();
  await expect(page.getByRole("heading", { name: /sign in/i })).toBeVisible();
  expect(await page.evaluate(() => JSON.stringify({ ...sessionStorage }))).not.toContain(KEYS.client);
});

test("client key: cancel a running run", async ({ page }) => {
  await signIn(page, KEYS.client);
  await page.getByRole("button", { name: /^run small-mixed$/i }).click();
  await expect(page).toHaveURL(/\/runs\//);
  await page.getByRole("button", { name: /cancel run/i }).click();
  await expect(page.getByTestId("stage-text")).toHaveText(/cancelled/i, { timeout: 30_000 });
});

test("demo key: replay is refused by the server (role enforced by the API, not the browser)", async ({ page }) => {
  await signIn(page, KEYS.demo);
  await page.getByRole("button", { name: /^run small-mixed$/i }).click();
  await expect(page.getByRole("table", { name: /seeds/i })).toBeVisible();
  const replay = page.getByRole("button", { name: /replay with same seeds/i });
  await expect(replay).toBeVisible(); // the real API does not report the role, so the UI cannot hide it
  await replay.click();
  await expect(page.getByText(/demo role may only run a predefined composition/i)).toBeVisible();
});

test("client key: replay with same seeds starts a new run with identical seeds", async ({ page }) => {
  await signIn(page, KEYS.client);
  await page.getByRole("button", { name: /^run small-mixed$/i }).click();
  await expect(page.getByRole("link", { name: /view results/i })).toBeVisible({ timeout: 60_000 });
  const firstUrl = page.url();
  const seeds = (await page.getByRole("table", { name: /seeds/i }).textContent()) ?? "";
  await page.getByRole("button", { name: /replay with same seeds/i }).click();
  await expect(page).not.toHaveURL(firstUrl);
  await expect(page.getByRole("table", { name: /seeds/i })).toHaveText(seeds);
  await expect(page.getByRole("link", { name: /view results/i })).toBeVisible({ timeout: 60_000 });
  // what the finished run screen says about convergence (fixed-mode run: the engine never checks)
  console.log("convergence line:", await page.getByText(/^convergence:/i).innerText());
  await page.getByRole("link", { name: /view results/i }).click();
  console.log("weather chip:", await page.getByText(/^weather year:/i).first().innerText());
});
