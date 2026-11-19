/**
 * End-to-end happy path against the mock API client (playwright.config.ts starts Vite with VITE_API_MODE=mock; no
 * backend needed). In mock mode the API key is the role name.
 * Sign in as client -> composition -> run with progress -> results with caveat and charts -> data table.
 */
import { expect, test } from "@playwright/test";

test("client: compose, run, inspect results", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: /sign in/i })).toBeVisible();
  await page.getByLabel(/api key/i).fill("client");
  await page.getByRole("button", { name: /sign in/i }).click();

  await expect(page.getByRole("heading", { name: /define a community composition/i })).toBeVisible();
  await page.getByLabel(/total buildings/i).fill("50");
  await page.getByRole("checkbox", { name: /include single-family detached/i }).check();
  await page.getByRole("checkbox", { name: /include office/i }).check();
  await page.getByLabel(/single-family detached mode/i).selectOption("count");
  await page.getByLabel(/single-family detached number of buildings/i).fill("20");
  await expect(page.getByRole("checkbox", { name: /include civic building/i })).toBeDisabled();
  await page.getByRole("button", { name: /start run/i }).click();

  await expect(page.getByRole("progressbar", { name: /run progress/i })).toBeVisible();
  await expect(page.getByRole("table", { name: /seeds/i })).toBeVisible();
  await page.getByRole("link", { name: /view results/i }).click({ timeout: 30_000 });

  await expect(page.getByText(/not validated for engineering or investment decisions/i)).toBeVisible();
  await expect(page.getByText(/weather year: AMY2018/i)).toBeVisible();
  for (const name of [/load over the year/i, /load-duration curve/i, /seasonal daily profile/i, /residential and commercial split/i]) {
    await expect(page.getByRole("heading", { name })).toBeVisible();
  }

  // keyboard zoom, then the data-table alternative
  await page.getByRole("group", { name: /time series chart/i }).focus();
  await page.keyboard.press("+");
  await page.getByRole("button", { name: "Hourly" }).click();
  const section = page.getByRole("region", { name: /residential and commercial split/i });
  await section.getByRole("checkbox", { name: /show data table/i }).check();
  await expect(section.getByRole("table")).toContainText("Residential");
});

test("demo: runs a predefined composition only", async ({ page }) => {
  await page.goto("/login");
  await page.getByLabel(/api key/i).fill("demo");
  await page.getByRole("button", { name: /sign in/i }).click();
  await expect(page.getByText(/demo accounts can run the predefined compositions only/i)).toBeVisible();
  await expect(page.getByRole("button", { name: /start run/i })).toHaveCount(0);
  await expect(page.getByLabel(/composition seed/i)).toHaveCount(0);
  await page.getByRole("button", { name: /^run /i }).first().click();
  await expect(page.getByRole("link", { name: /view results/i })).toBeVisible({ timeout: 30_000 });
  await expect(page.getByRole("button", { name: /replay/i })).toHaveCount(0);
  await page.getByRole("link", { name: /view results/i }).click();
  await expect(page.getByText(/not validated for engineering or investment decisions/i)).toBeVisible();
});
