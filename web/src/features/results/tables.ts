// Data-table alternatives to each chart (keyboard and screen-reader friendly).
import type { LoadSeries } from "../../api/types";
import { SEASON_DEFS, sectorEnergyMwh, slotLabels } from "./chartOptions";
import { dailySummary, loadDurationCurve, seasonalDailyProfile, SEASONS } from "./stats";

export interface TableData {
  caption: string;
  columns: string[];
  rows: (string | number)[][];
}

const WEATHER = "weather year AMY2018 (actual 2018 weather, not a typical year)";
const f1 = (v: number) => v.toFixed(1);

export function dailyTable(s: LoadSeries): TableData {
  return {
    caption: `Daily load and temperature, ${WEATHER}`,
    columns: ["Date (UTC)", "Mean load (kW)", "Peak load (kW)", "Mean temperature (°C)"],
    rows: dailySummary(s.ts, s.total, s.temperature).map((r) => [r.date, f1(r.meanKw), f1(r.peakKw), f1(r.meanTempC)]),
  };
}

export function loadDurationTable(s: LoadSeries): TableData {
  const curves = [s.total, s.residential, s.commercial].map(loadDurationCurve);
  const rows = Array.from({ length: 11 }, (_, k) => {
    const pct = k * 10;
    return [`${pct}`, ...curves.map((c) => f1(c.values[Math.round((pct / 100) * (c.values.length - 1))]))];
  });
  return {
    caption: `Load-duration curve sampled every 10 percent of time, ${WEATHER}`,
    columns: ["Percent of time at or above", "Total (kW)", "Residential (kW)", "Commercial (kW)"],
    rows,
  };
}

export function seasonalTable(s: LoadSeries): TableData {
  const prof = seasonalDailyProfile(s.ts, s.total);
  const slots = prof.winter.length;
  const perHour = slots / 24;
  const labels = slotLabels(slots);
  const rows = Array.from({ length: 24 }, (_, h) => [
    labels[Math.round(h * perHour)],
    ...SEASONS.map((season) => {
      let sum = 0;
      for (let k = 0; k < perHour; k++) sum += prof[season][Math.round(h * perHour) + k];
      return f1(sum / perHour);
    }),
  ]);
  return {
    caption: `Average daily profile of total load by season (hourly means), ${WEATHER}`,
    columns: ["Time of day (UTC)", ...SEASONS.map((x) => `${SEASON_DEFS[x].name} (kW)`)],
    rows,
  };
}

export function splitTable(s: LoadSeries): TableData {
  const e = sectorEnergyMwh(s);
  const total = e.residential + e.commercial;
  const row = (name: string, v: number) => [name, f1(v), `${((100 * v) / total).toFixed(1)}%`];
  return {
    caption: `Annual energy by sector, ${WEATHER}`,
    columns: ["Sector", "Energy (MWh)", "Share"],
    rows: [row("Residential", e.residential), row("Commercial", e.commercial)],
  };
}
