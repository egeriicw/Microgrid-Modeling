// ECharts option builders. The time-series options reuse the settings tuned in the Sprint 1 spike
// (docs/research/echarts-spike.md): column-wise `dataset` + `encode`, no symbols, no animation, `lttb` sampling,
// and the built-in tooltip replaced by a DOM overlay (see charts/hoverOverlay.ts). With these a 35,040-point,
// 3-series chart renders in about 140 ms and hover costs about 8 ms on the spike's (slow, no-GPU) machine.
import type { EChartsCoreOption } from "echarts/core";
import type { LoadSeries } from "../../api/types";
import { annualEnergyMwh, hourlyMean, loadDurationCurve, seasonalDailyProfile, SEASONS } from "./stats";

// Okabe-Ito colour-blind-safe palette. Series are also told apart by line style, never by hue alone.
export const OKABE_ITO = {
  black: "#000000",
  orange: "#E69F00",
  skyBlue: "#56B4E9",
  bluishGreen: "#009E73",
  yellow: "#F0E442",
  blue: "#0072B2",
  vermillion: "#D55E00",
  reddishPurple: "#CC79A7",
} as const;

export type Resolution = "15min" | "1h";
export type LoadKey = "total" | "residential" | "commercial";

export const LOAD_DEFS: { key: LoadKey; name: string; color: string; dash: "solid" | "dashed" | "dotted" }[] = [
  { key: "total", name: "Total", color: OKABE_ITO.black, dash: "solid" },
  { key: "residential", name: "Residential", color: OKABE_ITO.orange, dash: "dashed" },
  { key: "commercial", name: "Commercial", color: OKABE_ITO.blue, dash: "dotted" },
];

const STEP_1_H_MS = 3_600_000;
const hourlyCache = new WeakMap<LoadSeries, LoadSeries>();

/** The data at the requested resolution. Hourly = mean of each 4 quarter-hours (temperature too). */
export function viewAtResolution(data: LoadSeries, resolution: Resolution): LoadSeries {
  if (resolution === "15min" || data.stepMs === STEP_1_H_MS) return data;
  const cached = hourlyCache.get(data);
  if (cached) return cached;
  const ts = new Float64Array(data.ts.length / 4);
  for (let i = 0; i < ts.length; i++) ts[i] = data.ts[4 * i];
  const h: LoadSeries = {
    ts,
    stepMs: STEP_1_H_MS,
    total: hourlyMean(data.total),
    residential: hourlyMean(data.residential),
    commercial: hourlyMean(data.commercial),
    p10: hourlyMean(data.p10),
    p50: hourlyMean(data.p50),
    p90: hourlyMean(data.p90),
    temperature: hourlyMean(data.temperature),
  };
  hourlyCache.set(data, h);
  return h;
}

const pad = (n: number) => String(n).padStart(2, "0");
export function stamp(ms: number): string {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())} UTC`;
}

export interface TooltipContent {
  time: string;
  loads: { name: string; kw: number }[];
  temperatureC: number;
}

/**
 * Every shown load series (kW) and the temperature (deg C) at the SAME timestamp, looked up in the raw arrays so
 * it stays exact even when the drawn line is down-sampled.
 */
export function formatTooltip(tsMs: number, data: LoadSeries, keys: LoadKey[]): TooltipContent {
  if (typeof tsMs !== "number" || !Number.isFinite(tsMs)) throw new Error(`formatTooltip: bad timestamp ${String(tsMs)}`);
  const i = Math.min(data.ts.length - 1, Math.max(0, Math.round((tsMs - data.ts[0]) / data.stepMs)));
  return {
    time: stamp(data.ts[i]),
    loads: keys.map((k) => ({ name: LOAD_DEFS.find((d) => d.key === k)!.name, kw: data[k][i] })),
    temperatureC: data.temperature[i],
  };
}

const colCache = new WeakMap<LoadSeries, Record<string, number[]>>();
function columns(d: LoadSeries): Record<string, number[]> {
  let c = colCache.get(d);
  if (!c) {
    c = {
      ts: Array.from(d.ts),
      total: Array.from(d.total),
      residential: Array.from(d.residential),
      commercial: Array.from(d.commercial),
      p10: Array.from(d.p10),
      p50: Array.from(d.p50),
      p90: Array.from(d.p90),
      span: Array.from(d.p90, (v, i) => v - d.p10[i]),
      temperature: Array.from(d.temperature),
    };
    colCache.set(d, c);
  }
  return c;
}

function pairs(x: ArrayLike<number>, y: ArrayLike<number>): number[][] {
  const out = new Array<number[]>(x.length);
  for (let i = 0; i < x.length; i++) out[i] = [x[i], y[i]];
  return out;
}

const fast = { showSymbol: false, emphasis: { disabled: true }, progressive: 5000, progressiveThreshold: 3000, sampling: "lttb" as const };
const base = {
  animation: false,
  useUTC: true,
  color: [OKABE_ITO.black, OKABE_ITO.orange, OKABE_ITO.blue, OKABE_ITO.bluishGreen, OKABE_ITO.vermillion],
  grid: { left: 64, right: 64, top: 48, bottom: 80 },
  legend: { top: 4 },
  aria: { enabled: true },
};
const yKw = { type: "value" as const, name: "kW", scale: true };

export interface TimeSeriesParams {
  series: LoadSeries;
  resolution: Resolution;
  showBand: boolean;
  showTemperature: boolean;
}

export function buildTimeSeriesOption(p: TimeSeriesParams): EChartsCoreOption {
  const d = viewAtResolution(p.series, p.resolution);
  const loads = LOAD_DEFS.map((s) => ({
    type: "line" as const,
    name: s.name,
    color: s.color,
    encode: { x: "ts", y: s.key },
    lineStyle: { width: 1.2, type: s.dash },
    ...fast,
  }));
  const band = p.showBand
    ? [
        { type: "line" as const, name: "P10 (base)", encode: { x: "ts", y: "p10" }, stack: "band", silent: true, lineStyle: { opacity: 0 }, areaStyle: { opacity: 0 }, ...fast },
        { type: "line" as const, name: "P10-P90 band", encode: { x: "ts", y: "span" }, stack: "band", lineStyle: { opacity: 0 }, areaStyle: { color: OKABE_ITO.skyBlue, opacity: 0.35 }, ...fast },
        { type: "line" as const, name: "P50", color: OKABE_ITO.bluishGreen, encode: { x: "ts", y: "p50" }, lineStyle: { width: 1.5, type: "solid" as const }, ...fast },
      ]
    : [];
  const temp = p.showTemperature
    ? [
        {
          type: "line" as const,
          name: "Temperature (°C)",
          yAxisIndex: 1,
          color: OKABE_ITO.reddishPurple,
          encode: { x: "ts", y: "temperature" },
          lineStyle: { width: 1, type: "dashed" as const },
          ...fast,
        },
      ]
    : [];
  return {
    ...base,
    legend: { top: 4, data: [...loads.map((l) => l.name), ...(p.showBand ? ["P10-P90 band", "P50"] : []), ...(p.showTemperature ? ["Temperature (°C)"] : [])] },
    dataset: { source: columns(d) },
    tooltip: { show: false }, // replaced by the DOM overlay
    xAxis: { type: "time" },
    yAxis: [yKw, { type: "value", name: "°C", position: "right", splitLine: { show: false }, scale: true }],
    dataZoom: [
      { type: "inside", xAxisIndex: 0, filterMode: "filter" },
      { type: "slider", xAxisIndex: 0, filterMode: "filter", height: 24, bottom: 24 },
    ],
    series: [...loads, ...band, ...temp],
  };
}

export function buildLoadDurationOption(series: LoadSeries): EChartsCoreOption {
  return {
    ...base,
    tooltip: { trigger: "axis", axisPointer: { type: "cross" } },
    xAxis: { type: "value", name: "% of time at or above", min: 0, max: 100 },
    yAxis: yKw,
    series: LOAD_DEFS.map((s) => {
      const c = loadDurationCurve(series[s.key]);
      return { type: "line", name: s.name, color: s.color, data: pairs(c.percent, c.values), lineStyle: { width: 1.5, type: s.dash }, ...fast };
    }),
  };
}

const SEASON_DEFS = {
  winter: { name: "Winter (Dec-Feb)", color: OKABE_ITO.blue, dash: "solid" },
  spring: { name: "Spring (Mar-May)", color: OKABE_ITO.bluishGreen, dash: "dashed" },
  summer: { name: "Summer (Jun-Aug)", color: OKABE_ITO.vermillion, dash: "dotted" },
  fall: { name: "Fall (Sep-Nov)", color: OKABE_ITO.orange, dash: "dashdot" },
} as const;
export { SEASON_DEFS };

export function slotLabels(slots: number): string[] {
  return Array.from({ length: slots }, (_, i) => {
    const m = Math.round((i * 1440) / slots);
    return `${pad(Math.floor(m / 60))}:${pad(m % 60)}`;
  });
}

export function buildSeasonalOption(series: LoadSeries): EChartsCoreOption {
  const prof = seasonalDailyProfile(series.ts, series.total);
  return {
    ...base,
    tooltip: { trigger: "axis" },
    xAxis: { type: "category", data: slotLabels(prof.winter.length), name: "time of day (UTC)" },
    yAxis: yKw,
    series: SEASONS.map((s) => ({
      type: "line",
      name: SEASON_DEFS[s].name,
      color: SEASON_DEFS[s].color,
      data: Array.from(prof[s]),
      showSymbol: false,
      lineStyle: { width: 2, type: SEASON_DEFS[s].dash === "dashdot" ? [8, 3, 2, 3] : SEASON_DEFS[s].dash },
    })),
  };
}

export function sectorEnergyMwh(series: LoadSeries): { residential: number; commercial: number } {
  return {
    residential: annualEnergyMwh(series.residential, series.stepMs),
    commercial: annualEnergyMwh(series.commercial, series.stepMs),
  };
}

export function buildSplitOption(series: LoadSeries): EChartsCoreOption {
  const e = sectorEnergyMwh(series);
  return {
    ...base,
    color: [OKABE_ITO.orange, OKABE_ITO.blue],
    legend: { show: false },
    tooltip: { trigger: "axis" },
    xAxis: { type: "category", data: ["Residential", "Commercial"] },
    yAxis: { type: "value", name: "MWh" },
    series: [{ type: "bar", name: "Energy (MWh)", colorBy: "data", data: [e.residential, e.commercial], barWidth: "40%" }],
  };
}
