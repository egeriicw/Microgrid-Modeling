/**
 * W1-WEB2 manual check: the composition form against a REAL API (real client, no mock), through the Vite dev proxy.
 * Not part of `npm run e2e` (skipped unless REAL_API_COMPOSE is set). Needs an API whose data is the synthetic
 * fixture (tests/fixtures/synthetic_data.py: no sf_attached, mf_large, warehouse or hospital buildings) and the keys
 * below. Run with e2e/qa-real-api.config.ts:
 *   REAL_API_COMPOSE=1 SHOT_DIR=/some/dir [PW_CHROMIUM_EXECUTABLE=...] npx playwright test -c e2e/qa-real-api.config.ts real-api-compose
 */
import { expect, test, type Page } from "@playwright/test";

// Node globals without @types/node (the web tsconfig has no node types).
const env = (globalThis as unknown as { process: { env: Record<string, string | undefined> } }).process.env;
const KEYS = {
  client: env.QA_CLIENT_KEY ?? "client-key-alice-0123456789",
  demo: env.QA_DEMO_KEY ?? "demo-key-0123456789abcdefgh",
};
const SHOTS = env.SHOT_DIR;
test.skip(!env.REAL_API_COMPOSE, "needs a running API with the synthetic fixture (REAL_API_COMPOSE=1)");
test.describe.configure({ mode: "serial" });

async function signIn(page: Page, key: string) {
  await page.goto("/login");
  await page.getByLabel(/api key/i).fill(key);
  await page.getByRole("button", { name: /sign in/i }).click();
  await expect(page.getByRole("heading", { name: /define a community composition/i })).toBeVisible();
}

/** Full-page screenshots at 1280 and 390 px wide (skipped without SHOT_DIR). */
async function shots(page: Page, name: string) {
  if (!SHOTS) return;
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.screenshot({ path: `${SHOTS}/${name}-1280.png`, fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: `${SHOTS}/${name}-390.png`, fullPage: true });
  await page.setViewportSize({ width: 1280, height: 900 });
}

async function include(page: Page, label: RegExp) {
  await page.getByRole("checkbox", { name: label }).check();
}

test("client: hybrid composition -> run with progress -> data used -> results with temperature -> replay", async ({ page }) => {
  await signIn(page, KEYS.client);
  await expect(page.getByText(/signed in as alice \(client\)/i)).toBeVisible();
  await expect(page.getByRole("group", { name: /residential building types/i })).toBeVisible();
  await expect(page.getByRole("checkbox", { name: /include civic and public buildings/i })).toBeDisabled();

  await page.getByLabel(/composition name/i).fill("Hybrid block");
  await page.getByLabel(/total buildings/i).fill("12");
  await include(page, /include single-family detached/i);
  await page.getByLabel(/^single-family detached mode/i).selectOption("count");
  await page.getByLabel(/^single-family detached number of buildings/i).fill("3");
  await include(page, /include multifamily, 1-3 stories/i);
  await page.getByLabel(/^multifamily, 1-3 stories minimum share/i).fill("0.2");
  await include(page, /include multifamily, 4-7 stories/i);
  await page.getByLabel(/^multifamily, 4-7 stories maximum share/i).fill("0.8");
  await include(page, /include private commercial/i);
  await page.getByLabel(/^private commercial.* mode/i).selectOption("count");
  await page.getByLabel(/^private commercial.* number of buildings/i).fill("1");
  await page.getByLabel(/maximum runs/i).fill("30");
  await page.getByRole("switch", { name: /convergence/i }).check();
  await expect(page.getByRole("button", { name: /start run/i })).toBeEnabled();
  await shots(page, "w1web2-composition-form-client");

  const posted = page.waitForRequest((r) => r.url().endsWith("/api/v1/runs") && r.method() === "POST");
  await page.getByRole("button", { name: /start run/i }).click();
  const body = JSON.parse((await posted).postData() ?? "{}") as Record<string, unknown>;
  console.log("POST /runs body:", JSON.stringify(body));
  expect(body).toEqual({
    composition: {
      name: "Hybrid block",
      total_buildings: 12,
      building_types: [
        { building_type: "sf_detached", fixed_count: 3 },
        { building_type: "mf_small", min_fraction: 0.2, max_fraction: 1 },
        { building_type: "mf_mid", min_fraction: 0, max_fraction: 0.8 },
        { building_type: "commercial_private", fixed_count: 1 },
      ],
      max_runs: 30,
      convergence: true,
    },
  });
  await expect(page).toHaveURL(/\/runs\/[0-9a-f-]{36}$/);
  await expect(page.getByRole("progressbar", { name: /run progress/i })).toBeVisible();
  console.log("first stage text:", await page.getByTestId("stage-text").innerText());
  await expect(page.getByTestId("stage-text")).not.toHaveText(/^(queued|starting)$/i, { timeout: 60_000 });
  console.log("live stage text:", await page.getByTestId("stage-text").innerText(), "|", await page.getByText(/^runs completed/i).innerText());
  await shots(page, "w1web2-run-screen-in-progress");
  await expect(page.getByRole("link", { name: /view results/i })).toBeVisible({ timeout: 120_000 });
  await expect(page.getByText(/convergence rule used: legacy_window/i)).toBeVisible();
  const panel = page.getByRole("region", { name: /data used/i });
  await expect(panel).toContainText("DC_upgrade0.xlsx");
  await expect(panel).toContainText(/column out\.outdoor_temp_c of each sampled building's load file, in degrees Celsius/i);
  console.log("run screen:", (await page.locator("main").innerText()).replace(/\n+/g, " | "));
  await shots(page, "w1web2-run-screen-finished");
  const firstUrl = page.url();
  const seeds = (await page.getByRole("table", { name: /seeds/i }).textContent()) ?? "";

  await page.getByRole("link", { name: /view results/i }).click();
  await expect(page.getByText(/not validated for engineering or investment decisions/i)).toBeVisible({ timeout: 60_000 });
  await expect(page.getByRole("checkbox", { name: /temperature overlay/i })).toBeChecked();
  await expect(page.getByRole("region", { name: /data used/i })).toBeVisible();
  await expect(page.locator("canvas").first()).toBeVisible();
  await shots(page, "w1web2-results");

  // replay of a composition run: the stored config with the same seeds
  await page.goto(firstUrl);
  await page.getByRole("button", { name: /replay with same seeds/i }).click();
  await expect(page).not.toHaveURL(firstUrl);
  await expect(page.getByRole("table", { name: /seeds/i })).toHaveText(seeds);
  await expect(page.getByRole("link", { name: /view results/i })).toBeVisible({ timeout: 120_000 });
  console.log("replay finished:", page.url());
});

test("client: the server's refusals are shown next to the form, as text", async ({ page }) => {
  await signIn(page, KEYS.client);
  await page.getByLabel(/total buildings/i).fill("10");
  await include(page, /include single-family detached/i);
  await page.getByLabel(/^single-family detached mode/i).selectOption("count");
  await page.getByLabel(/^single-family detached number of buildings/i).fill("3");
  await include(page, /include multifamily, 1-3 stories/i);

  // 1. Counts above the total. The form blocks this itself, so the request is altered on its way out to check that
  //    the SERVER's own message reaches the screen (the server, not the browser, is the authority).
  await page.route("**/api/v1/runs", async (route) => {
    const req = route.request();
    if (req.method() !== "POST") return route.continue();
    const b = JSON.parse(req.postData() ?? "{}") as { composition: { building_types: { fixed_count?: number }[] } };
    b.composition.building_types[0].fixed_count = 11;
    await route.continue({ postData: JSON.stringify(b) });
  });
  await page.getByRole("button", { name: /start run/i }).click();
  const form = page.getByRole("form", { name: /your own composition/i });
  await expect(form.getByText("the fixed counts add up to 11, more than total_buildings (10)")).toBeVisible();
  await page.unroute("**/api/v1/runs");
  await shots(page, "w1web2-composition-form-server-refusal");

  // 2. A type with no buildings in the server's data: only the server can know this.
  await include(page, /include hospital/i);
  await page.getByRole("button", { name: /start run/i }).click();
  await expect(form.getByText(/the server's data has no buildings for 'institutional_hospital'/)).toBeVisible();
  console.log("refusal 2:", await form.getByRole("alert").last().innerText());

  // 3. The form's own check for counts above the total (before anything is sent).
  await page.getByLabel(/^single-family detached number of buildings/i).fill("11");
  await expect(page.getByText(/exceeds the total number of buildings/i)).toBeVisible();
  await expect(page.getByRole("button", { name: /start run/i })).toBeDisabled();
});

test("demo: no composition form, no seed fields, no replay", async ({ page }) => {
  await signIn(page, KEYS.demo);
  await expect(page.getByText(/signed in as demo \(demo\)/i)).toBeVisible();
  await expect(page.getByText(/demo accounts can run the predefined compositions only/i)).toBeVisible();
  await expect(page.getByRole("form", { name: /your own composition/i })).toHaveCount(0);
  await expect(page.getByRole("button", { name: /start run/i })).toHaveCount(0);
  await expect(page.getByLabel(/composition seed/i)).toHaveCount(0);
  await shots(page, "w1web2-composition-demo");
  await page.getByRole("button", { name: /^run small-mixed$/i }).click();
  await expect(page.getByRole("link", { name: /view results/i })).toBeVisible({ timeout: 120_000 });
  await expect(page.getByRole("table", { name: /seeds/i })).toBeVisible();
  await expect(page.getByRole("button", { name: /replay/i })).toHaveCount(0);
  await shots(page, "w1web2-run-screen-demo");
});
