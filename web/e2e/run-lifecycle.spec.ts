/**
 * End-to-end happy path against the mock API client (the Vite app wires the mock; no backend needed).
 * Sign in as client -> composition -> run with progress -> results with caveat and charts -> data table.
 * Skeleton: not yet run in CI (needs `npx playwright install chromium`; see web/README.md).
 */
import { expect, test } from "@playwright/test";

test("client: compose, run, inspect results", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: /sign in/i })).toBeVisible();
  await page.getByRole("radio", { name: /client/i }).check();
  await page.getByRole("button", { name: /sign in/i }).click();

  await expect(page.getByRole("heading", { name: /composition/i })).toBeVisible();
  await page.getByLabel(/total buildings/i).fill("50");
  await page.getByRole("button", { name: /start run/i }).click();

  await expect(page.getByRole("progressbar", { name: /run progress/i })).toBeVisible();
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

test("demo: sees results but not the composition form", async ({ page }) => {
  await page.goto("/login");
  await page.getByRole("radio", { name: /demo/i }).check();
  await page.getByRole("button", { name: /sign in/i }).click();
  await expect(page.getByText(/not validated for engineering or investment decisions/i)).toBeVisible();
  await expect(page.getByRole("link", { name: /compose/i })).toHaveCount(0);
});
