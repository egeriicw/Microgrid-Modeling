import type { EChartsOption } from "echarts";
import type { Dataset } from "./data";
import { STEP_1_H_MS } from "./data";
import { hourlyMean, loadDurationCurve, seasonalDailyProfile } from "./stats";

/** overlay = lttb + the built-in tooltip replaced by a DOM overlay driven by the app (see main.tsx). */
export type Mode = "naive" | "tuned" | "lttb" | "overlay";
export type View = "timeseries" | "band" | "ldc" | "seasonal" | "scatter";
export type Resolution = "15min" | "1h";

export interface SeriesDef {
  key: "total" | "residential" | "commercial" | "p10" | "p90";
  name: string;
  color: string;
}

// Okabe-Ito colour-blind-safe palette. Series are also distinguished by line style / order, never by hue alone.
export const SERIES_ORDER: SeriesDef[] = [
  { key: "total", name: "Total", color: "#000000" },
  { key: "residential", name: "Residential", color: "#E69F00" },
  { key: "commercial", name: "Commercial", color: "#0072B2" },
  { key: "p10", name: "P10", color: "#009E73" },
  { key: "p90", name: "P90", color: "#D55E00" },
];
export const BAND_DEFS: SeriesDef[] = [
  { key: "p10", name: "P10", color: "" },
  { key: "p50" as SeriesDef["key"], name: "P50", color: "" },
  { key: "p90", name: "P90", color: "" },
];
const TEMP_COLOR = "#CC79A7";
const DASH: Array<"solid" | "dashed" | "dotted"> = ["solid", "solid", "solid", "dashed", "dotted"];

const hourlyCache = new WeakMap<Dataset, Dataset>();

/** The data at the requested resolution. Hourly = mean of each 4 quarter-hours (temperature too). */
export function viewAtResolution(data: Dataset, resolution: Resolution): Dataset {
  if (resolution === "15min" || data.stepMs === STEP_1_H_MS) return data;
  const cached = hourlyCache.get(data);
  if (cached) return cached;
  const ts = new Float64Array(data.ts.length / 4);
  for (let i = 0; i < ts.length; i++) ts[i] = data.ts[4 * i];
  const h: Dataset = {
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
function stamp(ms: number): string {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

/**
 * Tooltip body for one timestamp: every shown load series (kW) and the temperature (deg C) at the SAME
 * timestamp, looked up directly from the raw arrays (so it stays exact even when the line is down-sampled).
 */
export function formatTooltip(tsMs: number, data: Dataset, series: SeriesDef[]): string {
  if (typeof tsMs !== "number" || !Number.isFinite(tsMs)) throw new Error(`formatTooltip: bad timestamp ${String(tsMs)}`);
  const i = Math.min(data.ts.length - 1, Math.max(0, Math.round((tsMs - data.ts[0]) / data.stepMs)));
  const rows = series
    .map((s) => `<div>${s.name}: <b>${data[s.key][i].toFixed(1)}</b> kW</div>`)
    .join("");
  return `<div>${stamp(data.ts[i])} UTC</div>${rows}<div>Temperature: <b>${data.temperature[i].toFixed(1)}</b> °C</div>`;
}

const colCache = new WeakMap<Dataset, Record<string, number[]>>();
function columns(data: Dataset): Record<string, number[]> {
  let c = colCache.get(data);
  if (!c) {
    c = {
      ts: Array.from(data.ts),
      total: Array.from(data.total),
      residential: Array.from(data.residential),
      commercial: Array.from(data.commercial),
      p10: Array.from(data.p10),
      p50: Array.from(data.p50),
      p90: Array.from(data.p90),
      span: Array.from(data.p90, (v, i) => v - data.p10[i]),
      temperature: Array.from(data.temperature),
    };
    colCache.set(data, c);
  }
  return c;
}

function pairs(ts: ArrayLike<number>, v: ArrayLike<number>): number[][] {
  const out = new Array<number[]>(ts.length);
  for (let i = 0; i < ts.length; i++) out[i] = [ts[i], v[i]];
  return out;
}

interface Params {
  view: View;
  mode: Mode;
  seriesCount: number;
  resolution: Resolution;
  data: Dataset;
}

export function buildOption(p: Params): EChartsOption {
  const { view, mode } = p;
  if (!Number.isInteger(p.seriesCount) || p.seriesCount < 1 || p.seriesCount > SERIES_ORDER.length) {
    throw new Error(`seriesCount must be 1..${SERIES_ORDER.length}, got ${p.seriesCount}`);
  }
  const d = viewAtResolution(p.data, p.resolution);
  const defs = SERIES_ORDER.slice(0, p.seriesCount);
  // Common cost-saving switches for the tuned and lttb modes.
  const fast = mode !== "naive";
  const base: EChartsOption = {
    animation: !fast,
    useUTC: true,
    color: SERIES_ORDER.map((s) => s.color),
    grid: { left: 64, right: 64, top: 48, bottom: 80 },
    legend: { top: 4 },
    aria: { enabled: true },
  };
  const lineCommon = (extra: object = {}) => ({
    type: "line" as const,
    ...(fast ? { showSymbol: false, emphasis: { disabled: true }, progressive: 5000, progressiveThreshold: 3000 } : {}),
    ...(mode === "lttb" || mode === "overlay" ? { sampling: "lttb" as const } : {}),
    ...extra,
  });
  const zoom = [
    { type: "inside" as const, xAxisIndex: 0, filterMode: "filter" as const },
    { type: "slider" as const, xAxisIndex: 0, filterMode: "filter" as const, height: 24, bottom: 24 },
  ];
  const timeTooltip = mode === "overlay" ? { show: false } : {
    trigger: "axis" as const,
    axisPointer: { type: "line" as const },
    ...(fast
      ? {
          formatter: (params: unknown) => {
            const first = (Array.isArray(params) ? params[0] : params) as { axisValue: number };
            return formatTooltip(Number(first.axisValue), d, view === "band" ? BAND_DEFS : defs);
          },
        }
      : {}),
  };
  const yKw = { type: "value" as const, name: "kW", scale: true };
  const yTemp = { type: "value" as const, name: "°C", position: "right" as const, splitLine: { show: false }, scale: true };
  const tempSeries = (x: "ts") =>
    fast
      ? lineCommon({
          name: "Temperature (°C)",
          yAxisIndex: 1,
          encode: { x, y: "temperature" },
          lineStyle: { width: 1, type: "dashed", color: TEMP_COLOR },
          itemStyle: { color: TEMP_COLOR },
        })
      : lineCommon({
          name: "Temperature (°C)",
          yAxisIndex: 1,
          data: pairs(d.ts, d.temperature),
          lineStyle: { width: 1, type: "dashed", color: TEMP_COLOR },
          itemStyle: { color: TEMP_COLOR },
        });

  if (view === "timeseries") {
    const series = defs.map((s, k) =>
      fast
        ? lineCommon({ name: s.name, encode: { x: "ts", y: s.key }, lineStyle: { width: 1.2, type: DASH[k] }, color: s.color })
        : lineCommon({ name: s.name, data: pairs(d.ts, d[s.key]), lineStyle: { width: 1.2, type: DASH[k] }, color: s.color }),
    );
    return {
      ...base,
      ...(fast ? { dataset: { source: columns(d) } } : {}),
      tooltip: timeTooltip,
      xAxis: { type: "time" },
      yAxis: [yKw, yTemp],
      dataZoom: zoom,
      series: [...series, tempSeries("ts")],
    } as EChartsOption;
  }

  if (view === "band") {
    const bandColor = "#0072B2";
    const mk = (name: string, y: string, v: Float64Array | number[], extra: object) =>
      fast
        ? lineCommon({ name, encode: { x: "ts", y }, ...extra })
        : lineCommon({ name, data: pairs(d.ts, v), ...extra });
    const spanV = Array.from(d.p90, (v, i) => v - d.p10[i]);
    return {
      ...base,
      legend: { top: 4, data: ["P10-P90 band", "P50", "Temperature (°C)"] },
      ...(fast ? { dataset: { source: columns(d) } } : {}),
      tooltip: timeTooltip,
      xAxis: { type: "time" },
      yAxis: [yKw, yTemp],
      dataZoom: zoom,
      series: [
        mk("P10 (base)", "p10", d.p10, { stack: "band", lineStyle: { opacity: 0 }, areaStyle: { opacity: 0 }, silent: true }),
        mk("P10-P90 band", "span", spanV, { stack: "band", lineStyle: { opacity: 0 }, areaStyle: { color: bandColor, opacity: 0.25 } }),
        mk("P50", "p50", d.p50, { lineStyle: { width: 1.5, color: bandColor }, color: bandColor }),
        tempSeries("ts"),
      ],
    } as EChartsOption;
  }

  if (view === "ldc") {
    const defsLdc = defs.slice(0, 3);
    const curves = defsLdc.map((s) => loadDurationCurve(d[s.key]));
    const series = defsLdc.map((s, k) =>
      lineCommon({ name: s.name, data: pairs(curves[k].percent, curves[k].values), lineStyle: { width: 1.5, type: DASH[k] }, color: s.color }),
    );
    return {
      ...base,
      tooltip: { trigger: "axis", axisPointer: { type: "cross" } },
      xAxis: { type: "value", name: "% of time at or above", min: 0, max: 100 },
      yAxis: { ...yKw },
      dataZoom: zoom,
      series,
    } as EChartsOption;
  }

  if (view === "seasonal") {
    const prof = seasonalDailyProfile(d.ts, d.total);
    const slots = prof.winter.length;
    const labels = Array.from({ length: slots }, (_, i) => {
      const m = Math.round((i * 1440) / slots);
      return `${pad(Math.floor(m / 60))}:${pad(m % 60)}`;
    });
    const seasons = [
      ["winter", "Winter (Dec-Feb)", "#0072B2"],
      ["spring", "Spring (Mar-May)", "#009E73"],
      ["summer", "Summer (Jun-Aug)", "#D55E00"],
      ["fall", "Fall (Sep-Nov)", "#E69F00"],
    ] as const;
    return {
      ...base,
      tooltip: { trigger: "axis" },
      xAxis: { type: "category", data: labels, name: "time of day" },
      yAxis: yKw,
      series: seasons.map(([k, name, color], i) => ({
        type: "line",
        name,
        data: Array.from(prof[k]),
        showSymbol: false,
        color,
        lineStyle: { width: 2, type: DASH[i % 3] },
      })),
    } as EChartsOption;
  }

  // scatter: load vs temperature, one dot per timestamp. This is where ECharts `large` mode applies.
  return {
    ...base,
    tooltip: { trigger: "item" },
    xAxis: { type: "value", name: "°C", scale: true },
    yAxis: { ...yKw },
    series: [
      {
        type: "scatter",
        name: "Load vs temperature",
        data: pairs(d.temperature, d.total),
        symbolSize: 3,
        itemStyle: { color: "#0072B2", opacity: 0.4 },
        ...(fast ? { large: true, largeThreshold: 2000, progressive: 5000, progressiveThreshold: 3000 } : {}),
      },
    ],
  } as EChartsOption;
}
