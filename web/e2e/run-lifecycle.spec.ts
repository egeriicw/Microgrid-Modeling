/**
 * FR-F10 — end-to-end happy path (Playwright).
 * Runs against `docker compose -f deploy/docker-compose.yml -f
 * deploy/docker-compose.ci.yml up` with MICROGRID_RUNNER_MODE=mock and the
 * fixture dataset. RED placeholder — see docs/test-plan.md §4 Phase 5.
 *
 * UJ-1: template -> clone -> edit -> validate passes -> launch -> progress
 * completes -> results dashboard renders every panel -> download one artifact.
 */
import { test, expect } from "@playwright/test";

test.skip("UJ-1: create from template, run, inspect results", async ({ page }) => {
  await page.goto("/configs/new");

  // pick the seeded template and clone
  await page.getByRole("row", { name: /DC Multifamily Baseline/ }).getByRole("button", { name: /use/i }).click();
  await page.getByLabel(/name/i).fill("e2e-run");
  await page.getByRole("button", { name: /create/i }).click();

  // editor: change total_buildings, expect validation to pass
  await expect(page).toHaveURL(/\/configs\/\d+/);
  await page.getByLabel(/total buildings/i).fill("4");
  await expect(page.getByText(/no validation errors/i)).toBeVisible();
  await page.getByRole("button", { name: /save/i }).click();

  // launch
  await page.getByRole("button", { name: /launch/i }).click();
  await page.getByRole("button", { name: /submit/i }).click();
  await expect(page).toHaveURL(/\/runs\/[0-9a-f-]+/);

  // progress completes (mock runner is fast)
  await expect(page.getByText(/succeeded/i)).toBeVisible({ timeout: 60_000 });
  await page.getByRole("tab", { name: /results/i }).click();

  // every dashboard panel present
  for (const panel of [
    /hourly load envelope/i,
    /peak demand distribution/i,
    /annual energy distribution/i,
    /load[- ]duration/i,
    /convergence history/i,
    /representative cases/i,
    /validation/i,
    /artifacts/i,
  ]) {
    await expect(page.getByText(panel)).toBeVisible();
  }

  // download one artifact
  const [download] = await Promise.all([
    page.waitForEvent("download"),
    page.getByRole("link", { name: /compiled-runs\.csv/i }).click(),
  ]);
  expect(await download.suggestedFilename()).toMatch(/compiled-runs\.csv$/);
});
