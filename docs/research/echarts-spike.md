# ECharts spike: can it carry a year of 15-minute load data? (Sprint 1, story 1.12)

Status: complete. Code: `web/spikes/echarts-35k/`. Re-run: `cd web/spikes/echarts-35k && npm ci && npm run measure`.
Raw output of the run quoted here: `web/spikes/echarts-35k/measure/results.md`.

## 1. Questions (from PRD FR-WEB-3)

1. Can Apache ECharts draw and interact with a year of 15-minute data (35,040 points per series), several series at once
   (total, residential, commercial, P10/P50/P90), without freezing?
2. Do zoom/pan and the hourly <-> 15-minute switch stay usable?
3. Can a hover show **load (kW) and temperature at the same timestamp**, quickly?
4. Which chart types fit: multi-series line + zoom, P10-P90 stacked band, load-duration curve, seasonal daily profiles.
5. What does it cost in bundle size and memory, and which settings matter?

## 2. Setup (what was actually built)

- Vite + React 19 + TypeScript, ECharts used directly (`echarts/core`, tree-shaken, canvas renderer). No MUI, no TanStack
  Query, no routing in the spike: those are not measured (see section 8).
- Exact pins (all published at least two weeks before 2026-11-11, from the public npm registry): echarts 6.1.0
  (2026-05-19), react/react-dom 19.2.9 (2026-10-14), vite 8.3.4 (2026-10-08), typescript 6.0.3, vitest 4.1.12,
  @types/react 19.2.18, @types/react-dom 19.2.7. A lockfile is committed.
- Deterministic synthetic data (`src/data.ts`, seeded): 2018 calendar, seasonal + diurnal + weekday shapes, temperature
  with seasonal/diurnal/AR(1) weather, residential and commercial load driven by heating/cooling degrees, and a
  P10/P50/P90 band computed per timestamp over 25 synthetic ensemble members. It is **not** a model of any real
  neighborhood; it only has the right size and shape.
- Chart types (`src/charts.ts`): (a) multi-series time series with `dataZoom` slider + inside, temperature on a second axis;
  (b) P10-P90 band as two stacked series (transparent P10 base + P90-P10 span) with a P50 line; (c) load-duration curve;
  (d) seasonal average daily profile (4 lines); plus (e) load-vs-temperature scatter, the one place ECharts `large` mode
  applies.
- Four configurations compared:
  - **naive**: `series.data` as `[t, v]` pairs, default symbols, animation on, default tooltip, no sampling.
  - **tuned**: `dataset` (column-wise) + `encode`, `showSymbol: false`, animation off, `emphasis.disabled`, `progressive`,
    custom tooltip formatter. No sampling.
  - **lttb**: tuned + `sampling: 'lttb'`.
  - **overlay**: lttb + the ECharts tooltip turned off and replaced by an app-drawn crosshair + tooltip `<div>`
    (`main.tsx`), looking up the raw arrays by timestamp.
- Harness (`measure/measure.mjs`, `npm run measure`): builds, serves the **production** build with `vite preview`, drives
  headless Chromium through Playwright, real mouse wheel / drag / hover events, 3 runs per case (medians), **1 run for the
  35,040-point naive cases** because each run takes 2-5 minutes (`--naive-runs` changes this).
  Metrics: setOption-to-"finished" render time, time to first tooltip (TTFI), tooltip latency (mouse move to tooltip DOM
  update, with a check that the tooltip text contained load and temperature), frame gaps via `requestAnimationFrame` during
  12 wheel-zoom steps and a 50-step drag pan, long-task time, `dispatchAction(dataZoom)` to "finished", hourly <-> 15-min
  switch, JS heap after forced GC (CDP), gzip bundle size.

### Environment of the recorded run (NOT the owner's Mac)

Cloud VM, Intel Xeon 2.1 GHz, 4 logical cores, 15.7 GiB RAM, Linux 6.18, headless Chromium 153.0.8010.12 with **software
rendering (no GPU)**, Node 22.23.4, Playwright 1.63.0, viewport 1100x700, chart 1000x520. The owner's MacBook Air M2 with a
real GPU-composited Chromium/Safari will differ, probably in ECharts' favor for painting, but **that was not measured**.
Treat the table as relative (naive vs tuned) with absolute numbers pessimistic. A frame gap of 16.7 ms means 60 fps.

## 3. Results

All rows: 15-minute data unless stated; "series" counts load series, plus temperature on a second axis in every case.
Tooltip = mouse move to tooltip update. Zoom p95 = 95th-percentile gap between frames while wheel-zooming (idle frames are
16.7 ms, so the median hides the cost; the p95 is the interesting number). Source: `measure/results.md`.

### 3.1 Time series, 35,040 points per series

| Load series | Config | Render ms | TTFI ms | Tooltip median / p95 ms | Zoom frame p95 ms | Pan frame p95 ms | Switch to hourly / to 15-min ms | JS heap MB |
|---|---|---|---|---|---|---|---|---|
| 1 | naive | 2973 | 4140 | 893 / 1022 | 1450 | 483 | 1496 / 1386 | 126 |
| 1 | tuned | 135 | 1177 | 100 / 118 | 167 | 50 | 32 / 55 | 13 |
| 1 | lttb | 128 | 1122 | 24 / 35 | 83 | 33 | 28 / 37 | 13 |
| 1 | overlay | 124 | 1100 | 7.5 / 8.5 | 67 | 33 | 25 / 50 | 13 |
| 3 | naive | 4209 | 5528 | 1867 / 2085 | 2550 | 900 | 3048 / 2937 | 246 |
| 3 | tuned | 190 | 1417 | 221 / 264 | 317 | 100 | 29 / 91 | 13 |
| 3 | lttb | 173 | 1252 | 88 / 107 | 150 | 50 | 30 / 40 | 13 |
| 3 | overlay | 138 | 1207 | 7.5 / 9.5 | 150 | 50 | 27 / 64 | 13 |
| 5 | naive | 6580 | 8245 | 3054 / 3473 | 4183 | 1400 | 4627 / 5808 | 362 |
| 5 | tuned | 195 | 1715 | 437 / 496 | 467 | 150 | 35 / 102 | 13 |
| 5 | lttb | 142 | 1290 | 197 / 232 | 233 | 83 | 27 / 61 | 13 |
| 5 | overlay | 146 | 1292 | 7.6 / 9.1 | 217 | 83 | 28 / 49 | 13 |

TTFI includes loading the page and generating the synthetic data (about 250-360 ms), which the real app will replace with
an API fetch, so it is an upper bound on chart cost only in that sense.

### 3.2 8,760 versus 35,040 points (3 load series)

| Points | Config | Render ms | Tooltip median ms | Zoom frame p95 ms | JS heap MB |
|---|---|---|---|---|---|
| 8,760 | naive | 2657 | 719 | 1083 | 65 |
| 35,040 | naive | 4209 | 1867 | 2550 | 246 |
| 8,760 | lttb | 94 | 86 | 133 | 7 |
| 35,040 | lttb | 173 | 88 | 150 | 13 |
| 8,760 | overlay | 96 | 7.8 | 133 | 6 |
| 35,040 | overlay | 138 | 7.5 | 150 | 13 |

With `sampling: 'lttb'` the cost no longer scales with the number of raw points (ECharts reduces each line to about the
pixel width); it scales with the number of series. Naive scales with both.

### 3.3 Other chart types (35,040 points)

| Chart | Config | Render ms | Tooltip median ms | Zoom frame p95 ms | JS heap MB |
|---|---|---|---|---|---|
| P10/P50/P90 band (stacked) + temperature | naive | 5118 | 2370 | 3250 | 246 |
| | lttb | 188 | 60 | 167 | 13 |
| | overlay | 178 | 7.8 | 150 | 13 |
| Load-duration curve (3 curves) | naive | 4142 | 1035 | 1817 | 186 |
| | lttb | 176 | 7.6 | 17 | 26 |
| Seasonal daily profile (4 x 96 points) | naive / lttb | 1067 / 73 | 7.7 / 7.5 | 17 / 17 | 4 / 4 |
| Load vs temperature scatter (35,040 dots) | naive | 3052 | 9.7 (16/40 updates) | 17 | 63 |
| | tuned (`large: true`) | 198 | 7.4 (17/40 updates) | 17 | 11 |

Notes: the seasonal profile is tiny (96 points per line), so its first render is cheap and its 1,067 ms "naive" figure is
mostly first-use cost (page, fonts, canvas). The scatter has no zoom, so its zoom column is only idle frames; the
`dispatch zoom` column for it (2,938 ms naive vs 68 ms with `large`) is a full-redraw cost and shows `large` is very
effective on scatter. In the scatter and load-duration rows the tooltip is a point (item) tooltip that only fires over a
dot, hence fewer than 40 updates. The load-duration curve is already cheap because it is a single monotone line.

### 3.4 Bundle size (production build, all JS, gzip level 9)

| Import style | Raw KB | Gzip KB |
|---|---|---|
| Tree-shaken ECharts (core, line, scatter, grid, tooltip, legend, dataZoom, dataset, aria, canvas) + React 19 | 742 | 242 |
| Full `import * from 'echarts'` + React 19 | 1289 | 420 |

React alone is a large part of the 242 KB; the ECharts-only share was not measured separately. MUI and TanStack Query are
additional and were not included.

## 4. What worked

- **Production feasibility is real once configured.** A tuned chart with 35,040 points x 3 series + temperature renders in
  about 140-190 ms and a series switch hourly <-> 15-minute costs 30-90 ms. Naive takes 4-7 seconds and a quarter of a
  gigabyte of heap and is not usable.
- **`sampling: 'lttb'` is the biggest single lever for hover and zoom** (tooltip 221 ms to 88 ms, zoom p95 317 ms to 150 ms
  at 3 series) and makes cost independent of point count. ECharts re-samples as the visible window changes, so zoomed-in
  views show true detail (see `screenshots/timeseries-zoom-hover-overlay.png`).
- **The linked load + temperature tooltip works** with an exact lookup by timestamp into the raw arrays, so it stays correct
  even when the drawn line is down-sampled (screenshots `band-zoom-hover.png`, `timeseries-zoom-hover-overlay.png`).
- **`large: true` works on scatter** (about 40x faster full redraw: 2,938 ms vs 68 ms).
- `dataset` + `encode`, `showSymbol: false`, `animation: false`, `emphasis.disabled`, `progressive` removed the 2-4 second
  naive cost (naive to tuned: render 4209 to 190 ms at 3 series). These are cumulative; the spike did not isolate each one.
- Hourly <-> 15-minute switching is fast because the app simply replaces the dataset; the hourly series is a mean of four
  quarter-hours.
- Bundle: tree-shaking saves about 178 KB gzip (242 vs 420).
- A color-blind-safe palette (Okabe-Ito) plus line styles (solid / dashed / dotted) is used; temperature is dashed.

## 5. What did not work, or surprised us

1. **Hover repaints the chart; its cost scales with series count, not just point count.** Even with lttb, the built-in
   tooltip took 88 ms (3 series) and 197 ms (5 series) per hover in this software-rendered browser. A CPU profile showed the
   time is native painting (JS under 10 ms), not ECharts logic, and the same page with the tooltip turned off stays at the
   frame floor. Things that did **not** remove it: axisPointer type `line` vs `cross`, `tooltip.showContent: false`,
   `transitionDuration: 0`, `appendToBody`, `triggerEmphasis: false`, `axisPointer.animation: false`, a separate `zlevel`
   for the series, hiding the slider's data shadow, and removing the legend.
2. **The workaround that did work: an app-drawn tooltip** (`overlay` mode). ECharts' tooltip is switched off; we draw a 1 px
   crosshair `<div>` and a tooltip `<div>` from the zrender `mousemove` event and look the values up by timestamp with
   `chart.convertFromPixel`. Latency is about 7.5 ms regardless of series count (that is the measurement floor of this
   harness: the seasonal chart, which is trivial, measures the same). The cost is that we own the tooltip: positioning near
   edges, keyboard access, touch, and screen-reader text are our responsibility.
3. **Zoom/pan frames are still heavy in this environment.** Each wheel step re-renders the series; frame p95 was 150 ms at
   3 series and 217-233 ms at 5 series (about 5-7 fps in the worst frames), versus the 16.7 ms ideal. Pan frames are
   better (p95 50-83 ms). This is software rasterisation; **the numbers on a GPU-backed M2 are unknown**.
4. **At year scale the P10-P90 band is a thin sliver** (the synthetic ensemble spread is about 5-10 percent of load; the
   band is only legible once zoomed to a few days; see `band-zoom-hover.png`). That is a property of the data and the
   screen width, not a library fault, but the real UI should offer a default zoom (for example, the most recent month or a
   daily-mean view) rather than opening on 35,040 overlapping points. There was also a faint horizontal line at 0 degrees C in
   the year-wide band view that was not traced; treat it as an open visual defect to check in the real component.
5. **Wheel-zoom is not keyboard accessible out of the box.** `dataZoom` `inside` handles mouse/touch only, and the slider
   is not focusable. Keyboard operation (a stated project requirement) needs app-level controls (buttons and arrow-key
   handlers calling `dispatchAction({type: 'dataZoom', ...})`). Not built or measured in this spike.
6. Measurement honesty: my first overlay run reported 7.8 ms while the overlay was actually throwing an exception (a wrong
   `convertFromPixel` argument), so the harness now counts tooltip updates seen vs mouse moves and checks that the tooltip
   text contains load and temperature. The reported numbers are from the corrected run. Also, long-task time is a sum of
   task durations and is reported only as a relative indicator.
7. Tooling note: `npm install` crashed with an arborist error ("Cannot read properties of null (reading 'edgesOut')") on
   this dependency set (vite 8 + vitest) until `legacy-peer-deps=true` was set in `web/spikes/echarts-35k/.npmrc`. Peer
   dependency conflicts are therefore hidden in this spike; re-check when the real `web/` package is created.

## 6. Recommendation

**Use Apache ECharts, with specific settings and one custom piece.** It meets the chart requirements in FR-WEB-3 on a
35,040-point year when configured as below; the naive configuration does not. No fallback library is needed on the evidence
here. Conditions:

1. Always use the "lttb" configuration below; never ship series as raw `[t, v]` pairs without sampling.
2. Replace ECharts' built-in tooltip with an app-drawn linked tooltip (the `overlay` approach) for the multi-series time
   and band charts, if hover latency on the owner's Mac proves to be an issue. **Cheapest next step: have the owner open
   the spike (`npm run dev` in `web/spikes/echarts-35k`, choose Mode "tuned + lttb" then "lttb + DOM overlay tooltip") on
   the M2 and tell us whether the built-in tooltip already feels smooth.** If it does, the simpler built-in tooltip with
   the custom formatter is enough.
3. Limit the default view (for example, 3 load series plus temperature, or the band chart on its own) and let users add
   more; five load series is the heaviest case measured and was noticeably slower on zoom.
4. Provide keyboard controls and an accessible data table alternative; ECharts' `aria` option gives only a generated
   description.

Because the evidence comes from a software-rendering cloud VM, "meets" here means: render, hourly/15-minute switch and
overlay tooltip are comfortably fast; zoom/pan frames at 3-5 series are only borderline (p95 150-230 ms) and need
confirmation on the M2.

## 7. Settings to use in the real chart components

```ts
// Import only what is used (saves ~178 KB gzip vs the full package)
import * as echarts from "echarts/core";
import { LineChart, ScatterChart } from "echarts/charts";
import { AriaComponent, DataZoomComponent, DatasetComponent, GridComponent,
         LegendComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";   // canvas, not SVG, for >5k points
```

Global: `animation: false`, `useUTC: true` (timestamps are UTC), `aria: { enabled: true }`, Okabe-Ito colors
(`#000000 #E69F00 #0072B2 #009E73 #D55E00`, temperature `#CC79A7` dashed) with distinct line styles.

Data: one `dataset` with column-wise source (`{ ts: [...], total: [...], ... }`) and `series.encode: { x: 'ts', y: key }`.
Temperature on a second `yAxis` as its own dashed series.

Per line series: `type: 'line', showSymbol: false, sampling: 'lttb', emphasis: { disabled: true }, progressive: 5000,
progressiveThreshold: 3000`.

Zoom: both `{ type: 'inside', filterMode: 'filter' }` and `{ type: 'slider', filterMode: 'filter' }` on the x axis. `filter`
(not `none`) is what lets lttb re-sample the visible window.

Band: stack a transparent P10 base and the (P90 - P10) span with `areaStyle`, plus a P50 line. Compute percentiles in the
engine/API (not in the browser): this spike computed them client-side only because it generates its own data.

Tooltip: `trigger: 'axis'` with a `formatter` that takes the axis timestamp and reads **all** values (every load series
and temperature) from the raw arrays by index `round((t - t0) / step)`. If hover latency matters, use the overlay version
(`src/main.tsx`): `tooltip: { show: false }`, zrender `mousemove`, `chart.containPixel('grid', pt)` and
`chart.convertFromPixel({ xAxisIndex: 0 }, offsetX)`.

Scatter (load vs temperature): `large: true, largeThreshold: 2000, progressive: 5000`.

Load-duration curve: compute the sorted curve in the engine/API; ECharts only draws it (lttb sampling is fine on it).

Hourly <-> 15-minute: swap the dataset (or fetch the other resolution); do not mix resolutions in one chart.

Things not to rely on: `large` on line series (it applies to bar/scatter; it was only exercised on scatter here).

Unit-tested pure helpers in the spike that can be reused or ported: `percentile`, `hourlyMean`, `lttb`, `loadDurationCurve`,
`seasonalDailyProfile`, `formatTooltip`. Per the project rule, business logic for the real product (percentiles, load
duration, seasonal profiles, hourly aggregation) belongs in the engine/API; the UI should receive ready-made arrays.

Required labels (project rules, not exercised by the spike beyond one caption): show the weather year ("AMY2018: actual 2018
weather, not a typical year") and the FR-WEB-8 accuracy caveat on every results screen. The spike page carries the AMY2018
caption only.

## 8. Known limits and what was not measured

- Not measured: a real GPU or the owner's M2; Safari or Firefox; touch/pinch; slider-handle dragging; keyboard operation;
  MUI and TanStack Query overhead (React re-render cost around the chart); `echarts-for-react` (used ECharts directly; the
  wrapper adds a thin layer but was not benchmarked); SVG renderer; server-side downsampling as an alternative; browser
  memory beyond JS heap (canvas memory is not included); long sessions or repeated zoom cycles (leak check).
- Single machine, single day; medians of 3 runs (1 run for naive 35k). Variation between two full runs was typically within
  15 percent for the tuned/lttb/overlay rows (first run vs final run in this session), but no formal confidence interval.
- Synthetic data: real series will have similar point counts but different noise; line density (and therefore paint cost)
  could be higher or lower.
- "Responsive" thresholds are our own proposals (tooltip under about 50 ms, frame p95 under about 100 ms), not from the PRD,
  which gives no numbers.
- The 35,040-point stacked band relies on ECharts sampling each stacked series independently; edges looked right when
  zoomed, but this was inspected visually, not tested numerically.

## 9. Screenshots

`web/spikes/echarts-35k/screenshots/` (small PNGs, color-reduced, force-added to git because `*.png` is ignored):
`timeseries-year-lttb.png`, `timeseries-zoom-hover-overlay.png` (linked load + temperature tooltip), `band-zoom-hover.png`,
`load-duration.png`, `seasonal-profile.png`, `load-vs-temperature-large.png`.
