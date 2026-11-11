import { describe, expect, it } from "vitest";
import { generateDataset } from "./data";
import { buildOption, formatTooltip, viewAtResolution, SERIES_ORDER } from "./charts";

const data = generateDataset({ points: 35040, seed: 3 });

describe("viewAtResolution", () => {
  it("returns hourly data 4x shorter, with hourly means and matching timestamps", () => {
    const h = viewAtResolution(data, "1h");
    expect(h.ts.length).toBe(8760);
    expect(h.stepMs).toBe(3_600_000);
    expect(h.total[0]).toBeCloseTo((data.total[0] + data.total[1] + data.total[2] + data.total[3]) / 4, 9);
    expect(h.ts[1]).toBe(data.ts[4]);
  });
  it("is the identity at 15-minute resolution", () => {
    expect(viewAtResolution(data, "15min")).toBe(data);
  });
});

describe("formatTooltip", () => {
  it("shows every load series and the temperature at the same timestamp", () => {
    const i = 5000;
    const html = formatTooltip(data.ts[i], data, SERIES_ORDER.slice(0, 3));
    expect(html).toContain("kW");
    expect(html).toContain("°C");
    expect(html).toContain(data.total[i].toFixed(1));
    expect(html).toContain(data.temperature[i].toFixed(1));
    expect(html).toContain("Residential");
  });
  it("snaps an in-between timestamp to the nearest sample", () => {
    const i = 777;
    const html = formatTooltip(data.ts[i] + 4 * 60 * 1000, data, SERIES_ORDER.slice(0, 1));
    expect(html).toContain(data.total[i].toFixed(1));
  });
});

describe("buildOption", () => {
  const sampling = (o: ReturnType<typeof buildOption>) =>
    (o.series as Array<{ sampling?: string }>).map((s) => s.sampling);

  it("naive has no sampling and no dataset", () => {
    const o = buildOption({ view: "timeseries", mode: "naive", seriesCount: 3, resolution: "15min", data });
    expect(sampling(o).every((s) => s === undefined)).toBe(true);
    expect(o.dataset).toBeUndefined();
  });
  it("lttb mode samples every load series and uses a dataset", () => {
    const o = buildOption({ view: "timeseries", mode: "lttb", seriesCount: 3, resolution: "15min", data });
    expect(sampling(o).filter((s) => s === "lttb").length).toBeGreaterThanOrEqual(3);
    expect(o.dataset).toBeDefined();
  });
  it("has both slider and inside zoom, and N load series plus temperature", () => {
    const o = buildOption({ view: "timeseries", mode: "tuned", seriesCount: 5, resolution: "15min", data });
    const types = (o.dataZoom as Array<{ type: string }>).map((z) => z.type).sort();
    expect(types).toEqual(["inside", "slider"]);
    expect((o.series as unknown[]).length).toBe(6);
  });
  it("band view stacks P10 and the P90-P10 span", () => {
    const o = buildOption({ view: "band", mode: "tuned", seriesCount: 3, resolution: "15min", data });
    const stacked = (o.series as Array<{ stack?: string }>).filter((s) => s.stack === "band");
    expect(stacked.length).toBe(2);
  });
  it("load-duration view has 100 percent as its x range", () => {
    const o = buildOption({ view: "ldc", mode: "tuned", seriesCount: 1, resolution: "15min", data });
    expect((o.xAxis as { max: number }).max).toBe(100);
  });
  it("scatter view uses ECharts large mode when tuned", () => {
    const o = buildOption({ view: "scatter", mode: "tuned", seriesCount: 1, resolution: "15min", data });
    expect((o.series as Array<{ large?: boolean }>)[0].large).toBe(true);
  });
  it("rejects an unsupported series count", () => {
    expect(() => buildOption({ view: "timeseries", mode: "tuned", seriesCount: 9, resolution: "15min", data })).toThrow();
  });
});
