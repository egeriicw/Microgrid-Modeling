import { describe, expect, it } from "vitest";
import { annualEnergyMwh, dailySummary, hourlyMean, loadDurationCurve, percentile, seasonalDailyProfile } from "./stats";

describe("stats", () => {
  it("percentile interpolates and rejects bad input", () => {
    expect(percentile([1, 2, 3, 4, 5], 50)).toBe(3);
    expect(percentile([0, 10], 25)).toBe(2.5);
    expect(() => percentile([], 50)).toThrow(/empty/);
    expect(() => percentile([1], 101)).toThrow(/within/);
  });

  it("hourlyMean averages groups of four and rejects ragged input", () => {
    expect(Array.from(hourlyMean([1, 2, 3, 4, 10, 10, 10, 10]))).toEqual([2.5, 10]);
    expect(() => hourlyMean([1, 2, 3])).toThrow(/multiple of 4/);
  });

  it("loadDurationCurve sorts high to low over 0..100 percent", () => {
    const c = loadDurationCurve([3, 9, 1, 5]);
    expect(Array.from(c.values)).toEqual([9, 5, 3, 1]);
    expect(c.percent[0]).toBe(0);
    expect(c.percent[3]).toBe(100);
  });

  it("seasonalDailyProfile averages per time-of-day slot and season", () => {
    const day = 86_400_000;
    const jan1 = Date.UTC(2018, 0, 1);
    const step = day / 2; // two slots per day
    const ts = [0, 1, 2, 3].map((k) => jan1 + k * step);
    const p = seasonalDailyProfile(ts, [1, 3, 5, 7]);
    expect(Array.from(p.winter)).toEqual([3, 5]);
    expect(Array.from(p.summer)).toEqual([0, 0]);
  });

  it("annualEnergyMwh integrates kW over the step", () => {
    // 4 quarter-hours at 1000 kW = 1 hour = 1 MWh
    expect(annualEnergyMwh([1000, 1000, 1000, 1000], 900_000)).toBeCloseTo(1, 9);
  });

  it("dailySummary gives mean, peak and mean temperature per UTC day", () => {
    const start = Date.UTC(2018, 0, 1);
    const step = 12 * 3_600_000;
    const ts = Float64Array.from([0, 1, 2, 3].map((k) => start + k * step));
    const rows = dailySummary(ts, Float64Array.from([2, 4, 10, 20]), Float64Array.from([0, 10, 20, 30]));
    expect(rows).toEqual([
      { date: "2018-01-01", meanKw: 3, peakKw: 4, meanTempC: 5 },
      { date: "2018-01-02", meanKw: 15, peakKw: 20, meanTempC: 25 },
    ]);
  });
});
