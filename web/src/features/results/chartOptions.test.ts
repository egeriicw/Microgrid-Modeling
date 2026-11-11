import { describe, expect, it } from "vitest";
import { generateDataset } from "../../api/fixtures";
import {
  OKABE_ITO,
  buildLoadDurationOption,
  buildSeasonalOption,
  buildSplitOption,
  buildTimeSeriesOption,
  formatTooltip,
  viewAtResolution,
} from "./chartOptions";

const series = generateDataset({ points: 96 * 30, seed: 7 });

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Opt = any;

describe("viewAtResolution", () => {
  it("returns the same object at 15 minutes and a 4x shorter hourly mean view", () => {
    expect(viewAtResolution(series, "15min")).toBe(series);
    const h = viewAtResolution(series, "1h");
    expect(h.ts.length).toBe(series.ts.length / 4);
    expect(h.stepMs).toBe(3_600_000);
    expect(h.total[0]).toBeCloseTo((series.total[0] + series.total[1] + series.total[2] + series.total[3]) / 4, 9);
    expect(h.temperature[1]).toBeCloseTo((series.temperature[4] + series.temperature[5] + series.temperature[6] + series.temperature[7]) / 4, 9);
  });
  it("caches the hourly view", () => {
    expect(viewAtResolution(series, "1h")).toBe(viewAtResolution(series, "1h"));
  });
});

describe("formatTooltip: load and temperature at the SAME timestamp", () => {
  it("reads every shown series and the temperature at the nearest sample", () => {
    const i = 100;
    const t = formatTooltip(series.ts[i] + 1000, series, ["total", "residential"]);
    expect(t.time).toContain("UTC");
    expect(t.loads.map((l) => l.name)).toEqual(["Total", "Residential"]);
    expect(t.loads[0].kw).toBeCloseTo(series.total[i], 9);
    expect(t.temperatureC).toBeCloseTo(series.temperature[i], 9);
  });
  it("clamps outside the range and rejects non-finite timestamps", () => {
    expect(formatTooltip(-1e15, series, ["total"]).loads[0].kw).toBeCloseTo(series.total[0], 9);
    expect(() => formatTooltip(Number.NaN, series, ["total"])).toThrow(/timestamp/);
  });
});

describe("buildTimeSeriesOption", () => {
  const opt = (o: Partial<Parameters<typeof buildTimeSeriesOption>[0]> = {}): Opt =>
    buildTimeSeriesOption({ series, resolution: "15min", showBand: false, showTemperature: true, ...o });

  it("is tuned for 35k points: no animation, no symbols, lttb sampling, own tooltip, zoom inside + slider", () => {
    const o = opt();
    expect(o.animation).toBe(false);
    expect(o.tooltip.show).toBe(false);
    for (const s of o.series) {
      expect(s.showSymbol).toBe(false);
      expect(s.sampling).toBe("lttb");
    }
    expect(o.dataZoom.map((z: Opt) => z.type).sort()).toEqual(["inside", "slider"]);
    expect(o.dataset.source.ts).toHaveLength(series.ts.length);
  });

  it("puts temperature on a second axis and can omit it", () => {
    const withT = opt().series.filter((s: Opt) => s.yAxisIndex === 1);
    expect(withT).toHaveLength(1);
    expect(opt({ showTemperature: false }).series.filter((s: Opt) => s.yAxisIndex === 1)).toHaveLength(0);
  });

  it("adds the P10-P90 band as a stacked transparent base plus a span, and a P50 line, only when asked", () => {
    expect(opt().series.some((s: Opt) => s.stack === "band")).toBe(false);
    const o = opt({ showBand: true });
    const stacked = o.series.filter((s: Opt) => s.stack === "band");
    expect(stacked).toHaveLength(2);
    expect(stacked[0].areaStyle.opacity).toBe(0);
    expect(stacked[1].areaStyle.opacity).toBeGreaterThan(0);
    expect(o.series.some((s: Opt) => s.name === "P50")).toBe(true);
    const span = o.dataset.source.span;
    expect(span[3]).toBeCloseTo(series.p90[3] - series.p10[3], 9);
  });

  it("uses only colour-blind-safe palette colours and a distinct line style per load series", () => {
    const o = opt();
    const loads = o.series.filter((s: Opt) => s.yAxisIndex !== 1);
    for (const s of loads) expect(Object.values(OKABE_ITO)).toContain(s.color);
    const dashes = new Set(loads.map((s: Opt) => s.lineStyle.type));
    expect(dashes.size).toBe(loads.length);
  });

  it("uses the hourly dataset when asked", () => {
    expect(opt({ resolution: "1h" }).dataset.source.ts).toHaveLength(series.ts.length / 4);
  });
});

describe("other chart options", () => {
  it("load-duration: one curve per load series, x is percent of time", () => {
    const o = buildLoadDurationOption(series) as Opt;
    expect(o.series).toHaveLength(3);
    expect(o.xAxis.max).toBe(100);
    const y = o.series[0].data.map((p: number[]) => p[1]);
    expect(y[0]).toBeGreaterThanOrEqual(y[y.length - 1]);
  });
  it("seasonal profile: four seasons by time of day", () => {
    const o = buildSeasonalOption(series) as Opt;
    expect(o.series).toHaveLength(4);
    expect(o.xAxis.data).toHaveLength(96);
    expect(o.xAxis.data[0]).toBe("00:00");
  });
  it("split: residential vs commercial annual energy in MWh", () => {
    const o = buildSplitOption(series) as Opt;
    expect(o.xAxis.data).toEqual(["Residential", "Commercial"]);
    const [r, c] = o.series[0].data as number[];
    expect(r).toBeGreaterThan(0);
    expect(c).toBeGreaterThan(0);
  });
});
