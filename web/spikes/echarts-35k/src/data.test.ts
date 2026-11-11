import { describe, expect, it } from "vitest";
import { generateDataset, STEP_15_MIN_MS, YEAR_2018_START_MS } from "./data";
import { hourlyMean, lttb, loadDurationCurve, percentile, seasonalDailyProfile } from "./stats";

describe("generateDataset", () => {
  const d = generateDataset({ points: 35040, seed: 7 });

  it("produces 35,040 aligned points at 15-minute spacing", () => {
    expect(d.ts.length).toBe(35040);
    expect(d.ts[0]).toBe(YEAR_2018_START_MS);
    expect(d.ts[1] - d.ts[0]).toBe(STEP_15_MIN_MS);
    for (const k of ["total", "residential", "commercial", "p10", "p50", "p90", "temperature"] as const) {
      expect(d[k].length).toBe(35040);
    }
  });

  it("is deterministic for a seed and differs across seeds", () => {
    const again = generateDataset({ points: 35040, seed: 7 });
    expect(again.total[1234]).toBe(d.total[1234]);
    const other = generateDataset({ points: 35040, seed: 8 });
    expect(other.total[1234]).not.toBe(d.total[1234]);
  });

  it("keeps the band ordered and the parts summing to the total", () => {
    for (let i = 0; i < d.ts.length; i += 97) {
      expect(d.p10[i]).toBeLessThanOrEqual(d.p50[i]);
      expect(d.p50[i]).toBeLessThanOrEqual(d.p90[i]);
      expect(d.residential[i] + d.commercial[i]).toBeCloseTo(d.total[i], 6);
      expect(d.total[i]).toBeGreaterThan(0);
    }
  });

  it("has plausible seasons: summer and winter warmer/colder, peak in the afternoon", () => {
    const mean = (a: ArrayLike<number>, from: number, to: number) => {
      let s = 0;
      for (let i = from; i < to; i++) s += a[i];
      return s / (to - from);
    };
    const day = 96;
    expect(mean(d.temperature, 195 * day, 225 * day)).toBeGreaterThan(mean(d.temperature, 15 * day, 45 * day) + 10);
    const profile = seasonalDailyProfile(d.ts, d.total);
    expect(profile.summer.length).toBe(96);
    const peakSlot = profile.summer.indexOf(Math.max(...profile.summer));
    expect(peakSlot).toBeGreaterThan(12 * 4);
    expect(peakSlot).toBeLessThan(21 * 4);
  });

  it("supports the 8,760-point hourly size", () => {
    const h = generateDataset({ points: 8760, seed: 7 });
    expect(h.ts.length).toBe(8760);
    expect(h.ts[1] - h.ts[0]).toBe(3_600_000);
  });
});

describe("percentile", () => {
  it("uses linear interpolation and handles edges", () => {
    expect(percentile([1, 2, 3, 4, 5], 50)).toBe(3);
    expect(percentile([1, 2, 3, 4], 50)).toBe(2.5);
    expect(percentile([10, 20], 0)).toBe(10);
    expect(percentile([10, 20], 100)).toBe(20);
    expect(percentile([5], 90)).toBe(5);
  });
  it("does not require sorted input and does not mutate it", () => {
    const a = [5, 1, 4, 2, 3];
    expect(percentile(a, 10)).toBeCloseTo(1.4, 10);
    expect(a).toEqual([5, 1, 4, 2, 3]);
  });
  it("fails loudly on bad input", () => {
    expect(() => percentile([], 50)).toThrow();
    expect(() => percentile([1], 101)).toThrow();
  });
});

describe("hourlyMean", () => {
  it("averages groups of four 15-minute values", () => {
    expect(Array.from(hourlyMean(Float64Array.from([1, 2, 3, 4, 10, 10, 10, 14])))).toEqual([2.5, 11]);
  });
  it("rejects lengths that are not a multiple of 4", () => {
    expect(() => hourlyMean(Float64Array.from([1, 2, 3]))).toThrow();
  });
});

describe("lttb", () => {
  const n = 1000;
  const x = Float64Array.from({ length: n }, (_, i) => i);
  const y = Float64Array.from({ length: n }, (_, i) => Math.sin(i / 20) + (i === 500 ? 5 : 0));

  it("returns exactly the threshold count, keeping first and last", () => {
    const idx = lttb(x, y, 100);
    expect(idx.length).toBe(100);
    expect(idx[0]).toBe(0);
    expect(idx[99]).toBe(n - 1);
    for (let i = 1; i < idx.length; i++) expect(idx[i]).toBeGreaterThan(idx[i - 1]);
  });
  it("keeps an isolated spike", () => {
    expect(Array.from(lttb(x, y, 100))).toContain(500);
  });
  it("returns all indices when the threshold is not smaller than the data", () => {
    expect(lttb(x, y, 5000).length).toBe(n);
  });
});

describe("loadDurationCurve", () => {
  it("sorts descending and maps to percent of time", () => {
    const c = loadDurationCurve(Float64Array.from([3, 9, 1, 5]));
    expect(Array.from(c.values)).toEqual([9, 5, 3, 1]);
    expect(c.percent[0]).toBeCloseTo(0, 10);
    expect(c.percent[3]).toBeCloseTo(100, 10);
  });
});
