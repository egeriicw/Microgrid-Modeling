/**
 * QA W1-WEB2: the composition form on a REAL API (real client, Vite dev proxy). Skipped unless QA_W1WEB2 is set.
 * Accessibility basics, 390 px layout, server refusals (real and injected), demo, and a key revoked mid-session.
 * See e2e/qa-w1web2.config.ts for how to run it.
 */
import { expect, test, type Page } from "@playwright/test";

const env = (globalThis as unknown as { process: { env: Record<string, string | undefined> } }).process.env;
const KEYS = { client: "client-key-alice-0123456789", demo: "demo-key-0123456789abcdefgh" };
test.skip(!env.QA_W1WEB2, "needs a running API with the synthetic fixture (QA_W1WEB2=1)");

async function signIn(page: Page, key: string) {
  await page.goto("/login");
  await page.getByLabel(/api key/i).fill(key);
  await page.getByRole("button", { name: /sign in/i }).click();
  await expect(page.getByRole("heading", { name: /define a community composition/i })).toBeVisible();
  if (key !== KEYS.demo) await expect(page.getByRole("checkbox", { name: /include single-family detached/i })).toBeVisible();
}

const noHorizontalOverflow = (page: Page) =>
  page.evaluate(() => {
    const el = document.scrollingElement!;
    return { scroll: el.scrollWidth, client: el.clientWidth };
  });

test.describe("accessibility basics on the composition form", () => {
  test("every input has an accessible name, with all types included in both modes", async ({ page }) => {
    await signIn(page, KEYS.client);
    const form = page.getByRole("form", { name: /your own composition/i });
    for (const cb of await form.getByRole("checkbox").all()) if (await cb.isEnabled()) await cb.check();
    const controls = form.locator("input:not([type=hidden]), select, textarea");
    const n = await controls.count();
    expect(n).toBeGreaterThan(10);
    for (let i = 0; i < n; i++) await expect(controls.nth(i)).toHaveAccessibleName(/\S/);
    for (const sel of await form.locator("select").all()) await sel.selectOption("count");
    for (let i = 0; i < (await controls.count()); i++) await expect(controls.nth(i)).toHaveAccessibleName(/\S/);
  });

  test("the include tick, the mode switch and the convergence switch work from the keyboard", async ({ page }) => {
    await signIn(page, KEYS.client);
    const tick = page.getByRole("checkbox", { name: /include single-family detached/i });
    await tick.focus();
    await page.keyboard.press("Space");
    await expect(tick).toBeChecked();
    const mode = page.getByLabel(/^single-family detached mode/i);
    await mode.focus();
    await page.keyboard.press("ArrowDown");
    await expect(mode).toHaveValue("count");
    await expect(page.getByLabel(/^single-family detached number of buildings/i)).toBeVisible();
    const conv = page.getByRole("switch", { name: /convergence/i });
    await conv.focus();
    await page.keyboard.press("Space");
    await expect(conv).toBeChecked();
  });

  test("field errors are tied to their field; the composition error is announced", async ({ page }) => {
    await signIn(page, KEYS.client);
    const total = page.getByLabel(/total buildings/i);
    await total.fill("0");
    await expect(total).toHaveAttribute("aria-invalid", "true");
    await expect(total).toHaveAccessibleDescription(/at least 1/i);
    const name = page.getByLabel(/composition name/i);
    await name.fill("   ");
    await expect(name).toHaveAccessibleDescription(/enter a name/i);
    await expect(page.getByRole("alert").filter({ hasText: /include at least one building type/i })).toBeVisible();
    await page.getByRole("switch", { name: /convergence/i }).check();
    const runs = page.getByLabel(/maximum runs/i);
    await runs.fill("5");
    await expect(runs).toHaveAccessibleDescription(/at least 20/i);
  });

  test("tab order follows the page: name, total, types, run settings, seeds, start", async ({ page }) => {
    await signIn(page, KEYS.client);
    await page.getByRole("checkbox", { name: /include single-family detached/i }).check();
    await page.getByLabel(/composition name/i).focus();
    const seen: string[] = [];
    for (let i = 0; i < 40; i++) {
      seen.push(
        await page.evaluate(() => {
          const a = document.activeElement as (HTMLElement & { labels?: NodeListOf<HTMLLabelElement> }) | null;
          if (!a) return "";
          return a.getAttribute("aria-label") ?? a.labels?.[0]?.textContent ?? a.textContent ?? "";
        }),
      );
      await page.keyboard.press("Tab");
    }
    const at = (re: RegExp) => seen.findIndex((s) => re.test(s));
    const order = [/composition name/i, /total buildings/i, /single-family detached mode/i, /maximum runs/i, /convergence/i, /composition seed/i, /start run/i].map(at);
    expect(order.every((x) => x >= 0), JSON.stringify(seen)).toBe(true);
    expect([...order].sort((a, b) => a - b)).toEqual(order);
  });

  test("nothing overflows horizontally at 390 px, with every type included", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await signIn(page, KEYS.client);
    const form = page.getByRole("form", { name: /your own composition/i });
    for (const cb of await form.getByRole("checkbox").all()) if (await cb.isEnabled()) await cb.check();
    const o = await noHorizontalOverflow(page);
    expect(o.scroll).toBeLessThanOrEqual(o.client);
  });
});

test.describe("server refusals near the form", () => {
  test("a real server refusal (a type with no buildings in the data) is shown in the form", async ({ page }) => {
    await signIn(page, KEYS.client);
    await page.getByRole("checkbox", { name: /include single-family attached/i }).check();
    await page.getByLabel(/maximum runs/i).fill("2");
    await page.getByRole("button", { name: /start run/i }).click();
    const form = page.getByRole("form", { name: /your own composition/i });
    await expect(form.getByText(/the server's data has no buildings for 'sf_attached'/)).toBeVisible();
    await expect(page).toHaveURL(/\/composition/);
  });

  test("a very long refusal with HTML stays text and does not widen the page at 390 px", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    const long = `<img src=x onerror="document.title='pwned'">` + "x".repeat(3000) + " /a/very/long/path/" + "segment".repeat(200);
    await page.route("**/api/v1/runs", (route) =>
      route.request().method() === "POST" ? route.fulfill({ status: 422, contentType: "application/json", body: JSON.stringify({ detail: long }) }) : route.continue(),
    );
    await signIn(page, KEYS.client);
    await page.getByRole("checkbox", { name: /include single-family detached/i }).check();
    await page.getByRole("button", { name: /start run/i }).click();
    await expect(page.getByText(/<img src=x/)).toBeVisible();
    expect(await page.locator("main img").count()).toBe(0);
    expect(await page.title()).not.toBe("pwned");
    const o = await noHorizontalOverflow(page);
    expect(o.scroll).toBeLessThanOrEqual(o.client);
  });

  test("a 503 and a 403 are shown in the form", async ({ page }) => {
    let status = 503;
    await page.route("**/api/v1/runs", (route) =>
      route.request().method() === "POST" ? route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ detail: `server says ${status}` }) }) : route.continue(),
    );
    await signIn(page, KEYS.client);
    await page.getByRole("checkbox", { name: /include single-family detached/i }).check();
    const form = page.getByRole("form", { name: /your own composition/i });
    await page.getByRole("button", { name: /start run/i }).click();
    await expect(form.getByText("server says 503")).toBeVisible();
    status = 403;
    await page.getByRole("button", { name: /start run/i }).click();
    await expect(form.getByText("server says 403")).toBeVisible();
  });
});

test("demo: predefined only, fixed seeds shown, no replay", async ({ page }) => {
  await signIn(page, KEYS.demo);
  await expect(page.getByRole("form", { name: /your own composition/i })).toHaveCount(0);
  await page.getByRole("button", { name: /run small-mixed/i }).click();
  await expect(page).toHaveURL(/\/runs\//);
  await expect(page.getByRole("link", { name: /view results/i })).toBeVisible({ timeout: 120_000 });
  await expect(page.getByText("20260101").first()).toBeVisible();
  await expect(page.getByRole("button", { name: /replay with same seeds/i })).toHaveCount(0);
});

test("client key revoked mid-session: the next call signs the user out and clears the key", async ({ page, request }) => {
  test.skip(!env.QA_REVOKE, "needs QA_REVOKE=1 and an API restart without the client key during the test");
  await signIn(page, KEYS.client);
  expect(await page.evaluate(() => sessionStorage.getItem("microgrid.apiKey"))).toBe(KEYS.client);
  // Wait until the operator has restarted the API without the client key.
  await expect
    .poll(async () => (await request.get("http://127.0.0.1:8000/api/v1/me", { headers: { Authorization: `Bearer ${KEYS.client}` } }).catch(() => null))?.status() ?? 0, {
      timeout: 150_000,
      intervals: [2000],
    })
    .toBe(401);
  await page.getByRole("checkbox", { name: /include single-family detached/i }).check();
  await page.getByRole("button", { name: /start run/i }).click();
  await expect(page).toHaveURL(/\/login/);
  expect(await page.evaluate(() => sessionStorage.getItem("microgrid.apiKey"))).toBeNull();
  await page.reload();
  await expect(page).toHaveURL(/\/login/);
});
