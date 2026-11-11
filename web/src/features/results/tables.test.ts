import { describe, expect, it } from "vitest";
import { generateDataset } from "../../api/fixtures";
import { dailyTable, loadDurationTable, seasonalTable, splitTable } from "./tables";

const series = generateDataset({ points: 96 * 10, seed: 3 });

describe("data-table alternatives to the charts", () => {
  it("daily table has one row per day with kW and temperature columns", () => {
    const t = dailyTable(series);
    expect(t.columns).toEqual(["Date (UTC)", "Mean load (kW)", "Peak load (kW)", "Mean temperature (°C)"]);
    expect(t.rows).toHaveLength(10);
    expect(t.rows[0][0]).toBe("2018-01-01");
  });
  it("load-duration table samples 0..100 percent in 10% steps for each load series", () => {
    const t = loadDurationTable(series);
    expect(t.rows).toHaveLength(11);
    expect(t.columns[0]).toMatch(/percent/i);
    expect(Number(t.rows[0][1])).toBeGreaterThanOrEqual(Number(t.rows[10][1]));
  });
  it("seasonal table has 24 hourly rows and four season columns", () => {
    const t = seasonalTable(series);
    expect(t.rows).toHaveLength(24);
    expect(t.columns).toHaveLength(5);
    expect(t.rows[1][0]).toBe("01:00");
  });
  it("split table has residential and commercial MWh and share", () => {
    const t = splitTable(series);
    expect(t.rows.map((r) => r[0])).toEqual(["Residential", "Commercial"]);
    const shares = t.rows.map((r) => Number(String(r[2]).replace("%", "")));
    expect(shares[0] + shares[1]).toBeCloseTo(100, 0);
  });
  it("every table carries a caption naming the weather year", () => {
    for (const t of [dailyTable(series), loadDurationTable(series), seasonalTable(series), splitTable(series)]) {
      expect(t.caption).toMatch(/AMY2018/);
    }
  });
});
