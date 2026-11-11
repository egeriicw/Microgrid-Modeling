import { useCallback, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { init, type Chart } from "echarts-lib";
import { generateDataset, type Dataset } from "./data";
import { buildOption, type Mode, type Resolution, type View } from "./charts";

interface SpikeApi {
  ready: boolean;
  chart: Chart | null;
  marks: { generateMs: number; optionMs: number; renderMs: number; finishedAt: number };
  /** Rebuild the chart at another resolution; resolves with ms until ECharts reports "finished". */
  switchResolution: (r: Resolution) => Promise<number>;
}
declare global {
  interface Window {
    __spike: SpikeApi;
  }
}

const q = new URLSearchParams(location.search);
const initial = {
  view: (q.get("view") ?? "timeseries") as View,
  mode: (q.get("mode") ?? "lttb") as Mode,
  series: Number(q.get("series") ?? 3),
  points: (Number(q.get("points") ?? 35040) === 8760 ? 8760 : 35040) as 8760 | 35040,
};

window.__spike = {
  ready: false,
  chart: null,
  marks: { generateMs: 0, optionMs: 0, renderMs: 0, finishedAt: 0 },
  switchResolution: () => Promise.reject(new Error("not ready")),
};

function App() {
  const box = useRef<HTMLDivElement>(null);
  const chartRef = useRef<Chart | null>(null);
  const dataRef = useRef<Dataset | null>(null);
  const [view, setView] = useState<View>(initial.view);
  const [mode, setMode] = useState<Mode>(initial.mode);
  const [series, setSeries] = useState(initial.series);
  const [resolution, setResolution] = useState<Resolution>("15min");

  const draw = useCallback(
    (v: View, m: Mode, s: number, r: Resolution, first: boolean) =>
      new Promise<number>((resolve) => {
        const g0 = performance.now();
        if (!dataRef.current) dataRef.current = generateDataset({ points: initial.points, seed: 2018 });
        const t0 = performance.now();
        const option = buildOption({ view: v, mode: m, seriesCount: s, resolution: r, data: dataRef.current });
        const t1 = performance.now();
        if (!chartRef.current) {
          chartRef.current = init(box.current!, undefined, { renderer: "canvas" });
          window.__spike.chart = chartRef.current;
        }
        const c = chartRef.current;
        c.off("finished");
        c.on("finished", () => {
          c.off("finished");
          const t2 = performance.now();
          if (first) {
            window.__spike.marks = { generateMs: t0 - g0, optionMs: t1 - t0, renderMs: t2 - t1, finishedAt: t2 };
            window.__spike.ready = true;
          }
          resolve(t2 - t1);
        });
        c.setOption(option, true);
      }),
    [],
  );

  useEffect(() => {
    void draw(view, mode, series, resolution, !window.__spike.ready);
    window.__spike.switchResolution = (r) => {
      setResolution(r);
      return draw(view, mode, series, r, false);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view, mode, series, resolution, draw]);

  useEffect(() => {
    const onResize = () => chartRef.current?.resize();
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  return (
    <main>
      <form style={{ display: "flex", gap: 16, padding: 8, flexWrap: "wrap" }} onSubmit={(e) => e.preventDefault()}>
        <label>
          View{" "}
          <select value={view} onChange={(e) => setView(e.target.value as View)}>
            <option value="timeseries">Time series</option>
            <option value="band">P10/P50/P90 band</option>
            <option value="ldc">Load-duration curve</option>
            <option value="seasonal">Seasonal daily profile</option>
            <option value="scatter">Load vs temperature</option>
          </select>
        </label>
        <label>
          Mode{" "}
          <select value={mode} onChange={(e) => setMode(e.target.value as Mode)}>
            <option value="naive">naive</option>
            <option value="tuned">tuned</option>
            <option value="lttb">tuned + lttb</option>
          </select>
        </label>
        <label>
          Load series{" "}
          <select value={series} onChange={(e) => setSeries(Number(e.target.value))}>
            {[1, 3, 5].map((n) => (
              <option key={n}>{n}</option>
            ))}
          </select>
        </label>
        <button type="button" onClick={() => setResolution(resolution === "15min" ? "1h" : "15min")}>
          Resolution: {resolution === "15min" ? "15-minute" : "hourly"}
        </button>
        <span>Synthetic data. Weather year: AMY2018 (actual 2018 weather, not a typical year).</span>
      </form>
      <div ref={box} id="chart" role="img" aria-label="Load and temperature chart" style={{ width: 1000, height: 520 }} />
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
