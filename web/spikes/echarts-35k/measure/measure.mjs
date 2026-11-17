// Re-runnable measurement harness:  npm run measure   (options: --runs 3  --quick  --screenshots  --out measure/results.md)
// Builds the spike, serves the production build, drives headless Chromium with Playwright and writes a markdown report.
// Playwright is NOT a dependency of the spike: it must be resolvable (global install, NODE_PATH, or `npm i -D playwright`).
// Chromium: uses Playwright's own browser, or set CHROMIUM_PATH=/path/to/chrome to use another binary.
import { execSync } from "node:child_process";
import { createRequire } from "node:module";
import { readFileSync, readdirSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { gzipSync } from "node:zlib";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);
let chromium;
try {
  ({ chromium } = require("playwright"));
} catch {
  console.error("Cannot resolve 'playwright'. Install it (npm i -D playwright) or put it on NODE_PATH.");
  process.exit(1);
}
const args = process.argv.slice(2);
const flag = (n) => args.includes(`--${n}`);
const opt = (n, d) => (args.includes(`--${n}`) ? args[args.indexOf(`--${n}`) + 1] : d);
const RUNS = Number(opt("runs", 3));
const QUICK = flag("quick");
const SHOTS = flag("screenshots");
const FILTER = opt("filter", "");
const NAIVE_RUNS = Number(opt("naive-runs", 1));
const OUT = path.resolve(root, opt("out", "measure/results.md"));
const PORT = 4173;

const median = (a) => {
  const s = a.filter((v) => Number.isFinite(v)).sort((x, y) => x - y);
  if (!s.length) return NaN;
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const pct = (a, p) => {
  const s = [...a].sort((x, y) => x - y);
  return s.length ? s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] : NaN;
};
const f = (v, d = 0) => (Number.isFinite(v) ? v.toFixed(d) : "n/a");

// ---------- 1. bundle sizes ----------
function bundle(outDir, env) {
  execSync("npx vite build", { cwd: root, stdio: "pipe", env: { ...process.env, ...env } });
  const dir = path.join(root, outDir, "assets");
  let raw = 0;
  let gz = 0;
  for (const file of readdirSync(dir).filter((n) => n.endsWith(".js"))) {
    const buf = readFileSync(path.join(dir, file));
    raw += buf.length;
    gz += gzipSync(buf, { level: 9 }).length;
  }
  return { raw, gz };
}
console.log("building...");
const bundleTree = bundle("dist", {});
const bundleFull = bundle("dist-full", { BUNDLE: "full" });
// Build the tree-shaken bundle last so `dist` is the one being served.
bundle("dist", {});

// ---------- 2. serve production build ----------
const { preview } = await import("vite");
const server = await preview({ root, preview: { port: PORT, strictPort: true, host: "127.0.0.1" } });
const base = `http://127.0.0.1:${PORT}/`;

const launchOpts = { headless: true, args: ["--no-sandbox", "--enable-precise-memory-info"] };
if (process.env.CHROMIUM_PATH) launchOpts.executablePath = process.env.CHROMIUM_PATH;
const browser = await chromium.launch(launchOpts);

// ---------- 3. one measured run ----------
async function runOnce(cfg, shotPath) {
  const ctx = await browser.newContext({ viewport: { width: 1100, height: 700 } });
  const page = await ctx.newPage();
  const cdp = await ctx.newCDPSession(page);
  await cdp.send("Performance.enable");
  await page.addInitScript(() => {
    window.__lt = [];
    try {
      new PerformanceObserver((l) => l.getEntries().forEach((e) => window.__lt.push(e.duration))).observe({ entryTypes: ["longtask"] });
    } catch {}
  });
  const qs = new URLSearchParams({ view: cfg.view, mode: cfg.mode, series: String(cfg.series), points: String(cfg.points) });
  await page.goto(`${base}?${qs}`);
  await page.waitForFunction(() => window.__spike && window.__spike.ready, null, { timeout: 180000, polling: "raf" });
  const r = {};
  Object.assign(r, await page.evaluate(() => ({ ...window.__spike.marks })));
  await page.waitForTimeout(400);
  const heap = async () => {
    await cdp.send("HeapProfiler.collectGarbage");
    const m = (await cdp.send("Performance.getMetrics")).metrics;
    return m.find((x) => x.name === "JSHeapUsedSize").value / 1048576;
  };
  r.heapMB = await heap();
  if (shotPath) await page.screenshot({ path: shotPath, clip: { x: 0, y: 0, width: 1000, height: 580 } });

  // Tooltip latency: mouse move -> first tooltip DOM mutation. Also time of the very first tooltip (time to first interaction).
  await page.evaluate(() => {
    window.__tt = [];
    window.__firstTip = null;
    let last = 0;
    let pending = false;
    window.addEventListener("mousemove", (e) => { last = e.timeStamp; pending = true; }, true);
    new MutationObserver(() => {
      if (!pending) return;
      pending = false;
      const now = performance.now();
      if (window.__firstTip === null) window.__firstTip = now;
      window.__tt.push(now - last);
    }).observe(document.getElementById("chart"), { subtree: true, childList: true, attributes: true, characterData: true });
  });
  const box = await page.locator("#chart").boundingBox();
  const gx0 = box.x + 80;
  const gx1 = box.x + box.width - 80;
  const gy = box.y + box.height * 0.4;
  const moves = QUICK ? 15 : 40;
  for (let i = 0; i < moves; i++) {
    await page.mouse.move(gx0 + ((gx1 - gx0) * (i + 0.5)) / moves + (i % 2) * 3, gy);
    await page.waitForTimeout(40);
  }
  const tt = await page.evaluate(() => {
    const text = document.getElementById("chart").textContent ?? "";
    return { tt: window.__tt, first: window.__firstTip, content: text.includes("Temperature") && (text.includes("Total") || text.includes("P50")) };
  });
  r.moves = moves;
  r.tipHasLoadAndTemp = tt.content;
  r.ttMedian = median(tt.tt);
  r.ttP95 = pct(tt.tt, 95);
  r.ttSeen = tt.tt.length;
  r.ttfi = tt.first;

  // Frame sampling around real mouse-wheel zoom and drag pan.
  const rec = {
    start: () => page.evaluate(() => {
      window.__fr = [];
      window.__lt.length = 0;
      window.__run = true;
      const loop = (t) => { window.__fr.push(t); if (window.__run) requestAnimationFrame(loop); };
      requestAnimationFrame(loop);
    }),
    stop: () => page.evaluate(() => {
      window.__run = false;
      const dts = [];
      for (let i = 1; i < window.__fr.length; i++) dts.push(window.__fr[i] - window.__fr[i - 1]);
      return { dts, lt: window.__lt.reduce((a, b) => a + b, 0) };
    }),
  };
  const sum = (x) => ({ frames: x.dts.length, med: median(x.dts), p95: pct(x.dts, 95), max: Math.max(0, ...x.dts), longTask: x.lt });
  const cx = box.x + box.width / 2;
  await page.mouse.move(cx, gy);
  await rec.start();
  for (let i = 0; i < (QUICK ? 6 : 12); i++) {
    await page.mouse.wheel(0, -240);
    await page.waitForTimeout(33);
  }
  await page.waitForTimeout(150);
  r.zoomFrames = sum(await rec.stop());
  if (cfg.view === "timeseries" || cfg.view === "band") {
    await rec.start();
    await page.mouse.down();
    const steps = QUICK ? 20 : 50;
    for (let i = 0; i < steps; i++) {
      await page.mouse.move(cx + i * 6, gy);
      await page.waitForTimeout(16);
    }
    await page.mouse.up();
    await page.waitForTimeout(150);
    r.panFrames = sum(await rec.stop());
  }

  // Programmatic zoom: dispatchAction(dataZoom) -> ECharts "finished", averaged over several windows.
  r.zoomActionMs = await page.evaluate(async () => {
    const c = window.__spike.chart;
    const wins = [[0, 100], [10, 40], [30, 60], [55, 85], [0, 100], [20, 30], [60, 70]];
    const times = [];
    for (const [start, end] of wins) {
      await new Promise((res) => {
        const t0 = performance.now();
        c.off("finished");
        c.on("finished", () => { c.off("finished"); times.push(performance.now() - t0); res(); });
        c.dispatchAction({ type: "dataZoom", start, end });
      });
    }
    c.dispatchAction({ type: "dataZoom", start: 0, end: 100 });
    return times.reduce((a, b) => a + b, 0) / times.length;
  });

  // Hourly <-> 15-minute switch (time series only, 15-minute data only).
  if (cfg.view === "timeseries" && cfg.points === 35040) {
    const sw = async (res) => {
      const before = await page.evaluate(() => window.__spike.renders);
      await page.evaluate((x) => window.__spike.setResolution(x), res);
      await page.waitForFunction((n) => window.__spike.renders > n, before, { timeout: 120000, polling: "raf" });
      return page.evaluate(() => window.__spike.lastRenderMs);
    };
    r.toHourlyMs = await sw("1h");
    r.to15Ms = await sw("15min");
  }
  r.heapAfterMB = await heap();
  await ctx.close();
  return r;
}

async function measure(cfg, shotPath) {
  const runs = [];
  // Naive 35k configurations take minutes per run, so they get fewer repetitions (see --naive-runs).
  const n = cfg.mode === "naive" && cfg.points === 35040 ? Math.min(RUNS, NAIVE_RUNS) : RUNS;
  for (let i = 0; i < n; i++) {
    try {
      runs.push(await runOnce(cfg, i === 0 ? shotPath : null));
    } catch (e) {
      console.error("run failed", JSON.stringify(cfg), String(e).split("\n")[0]);
    }
  }
  const m = (get) => median(runs.map(get));
  return {
    cfg,
    ok: runs.length,
    generateMs: m((r) => r.generateMs),
    optionMs: m((r) => r.optionMs),
    renderMs: m((r) => r.renderMs),
    ttiMs: m((r) => r.finishedAt),
    ttfiMs: m((r) => r.ttfi),
    ttSeen: m((r) => r.ttSeen),
    moves: m((r) => r.moves),
    tipOk: runs.length > 0 && runs.every((r) => r.tipHasLoadAndTemp),
    ttMedian: m((r) => r.ttMedian),
    ttP95: m((r) => r.ttP95),
    zoomMed: m((r) => r.zoomFrames.med),
    zoomP95: m((r) => r.zoomFrames.p95),
    zoomMax: m((r) => r.zoomFrames.max),
    panP95: m((r) => r.panFrames?.p95 ?? NaN),
    panMax: m((r) => r.panFrames?.max ?? NaN),
    longTask: m((r) => r.zoomFrames.longTask + (r.panFrames?.longTask ?? 0)),
    zoomActionMs: m((r) => r.zoomActionMs),
    toHourlyMs: m((r) => r.toHourlyMs ?? NaN),
    to15Ms: m((r) => r.to15Ms ?? NaN),
    heapMB: m((r) => r.heapMB),
    heapAfterMB: m((r) => r.heapAfterMB),
  };
}

// ---------- 4. the matrix ----------
const cases = [];
for (const points of QUICK ? [35040] : [8760, 35040])
  for (const series of QUICK ? [3] : [1, 3, 5])
    for (const mode of ["naive", "tuned", "lttb", "overlay"]) cases.push({ view: "timeseries", mode, series, points });
const others = [
  ["band", ["naive", "lttb", "overlay"], 3],
  ["ldc", ["naive", "lttb"], 3],
  ["seasonal", ["naive", "lttb"], 3],
  ["scatter", ["naive", "tuned"], 1],
];
if (!QUICK || FILTER) for (const [view, modes, series] of others) for (const mode of modes) cases.push({ view, mode, series, points: 35040 });

const selected = FILTER ? cases.filter((c) => `${c.view} ${c.mode} ${c.series} ${c.points}`.includes(FILTER)) : cases;
const shotsDir = path.join(root, "screenshots");
if (SHOTS) mkdirSync(shotsDir, { recursive: true });
const results = [];
for (const cfg of selected) {
  const wantShot = SHOTS && cfg.mode !== "naive" && cfg.points === 35040 && (cfg.view !== "timeseries" || cfg.series === 5);
  const shot = wantShot ? path.join(shotsDir, `${cfg.view}-${cfg.mode}.png`) : null;
  const t0 = Date.now();
  const res = await measure(cfg, shot);
  results.push(res);
  console.log(`${cfg.view} ${cfg.mode} s=${cfg.series} n=${cfg.points}: render ${f(res.renderMs)} ms, zoom p95 ${f(res.zoomP95, 1)} ms, tooltip ${f(res.ttMedian, 1)} ms (${((Date.now() - t0) / 1000).toFixed(0)}s)`);
}
const chromiumVersion = browser.version();
const userAgent = await (async () => {
  const c = await browser.newContext();
  const p = await c.newPage();
  const ua = await p.evaluate(() => navigator.userAgent);
  await c.close();
  return ua;
})();
await browser.close();
await server.close();

// ---------- 5. report ----------
const pkg = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8"));
const lock = existsSync(path.join(root, "package-lock.json"));
const cpus = os.cpus();
const label = (c) => `${c.view} / ${c.mode} / ${c.series} load series${c.view === "timeseries" ? ` / ${c.points.toLocaleString("en-US")} pts` : ""}`;
const ts = results.filter((r) => r.cfg.view === "timeseries");
const oth = results.filter((r) => r.cfg.view !== "timeseries");
let md = `# ECharts 35k spike: measurement results\n\n`;
md += `Generated ${new Date().toISOString()} by \`npm run measure\` (runs per case: ${RUNS}, medians reported${QUICK ? ", QUICK mode" : ""}).\n\n`;
md += `## Environment\n\n`;
md += `- Machine: ${cpus[0]?.model ?? "unknown"} x ${cpus.length} logical cores, ${(os.totalmem() / 2 ** 30).toFixed(1)} GiB RAM, ${os.platform()} ${os.release()} (${os.arch()})\n`;
md += `- Browser: headless Chromium ${chromiumVersion}, software rendering (no GPU), Playwright, viewport 1100x700, chart 1000x520, canvas renderer\n`;
md += `- UA: \`${userAgent}\`\n`;
md += `- Node ${process.version}; echarts ${pkg.dependencies.echarts}; react ${pkg.dependencies.react}; vite ${pkg.devDependencies.vite}; lockfile ${lock ? "present" : "absent"}\n`;
md += `- Production build served by \`vite preview\`. These numbers are from a cloud VM, not an Apple M2 laptop; absolute values will differ.\n\n`;
md += `## Time-series chart (total, residential, commercial, P10, P90 + temperature on a second axis)\n\n`;
md += `Columns: render = setOption to ECharts "finished"; TTFI = page navigation to first tooltip shown; tooltip = mouse move to tooltip DOM update, with (updates seen / mouse moves) and a check that the tooltip text shows load and temperature together (time-series/band views only); zoom/pan frame = gap between animation frames during real wheel/drag (16.7 ms = 60 fps); dispatch = mean dispatchAction(dataZoom) to "finished"; heap = JS heap after GC.\n\n`;
const head = `| Case | data gen ms | option ms | render ms | TTFI ms | tooltip med / p95 ms | zoom frame med / p95 / max ms | pan frame p95 / max ms | long-task ms | dispatch zoom ms | ->hourly / ->15min ms | heap MB | runs |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|\n`;
const row = (r) =>
  `| ${label(r.cfg)} | ${f(r.generateMs)} | ${f(r.optionMs)} | ${f(r.renderMs)} | ${f(r.ttfiMs)} | ${r.ttSeen > 0 ? `${f(r.ttMedian, 1)} / ${f(r.ttP95, 1)}` : "n/a"} (${f(r.ttSeen)}/${f(r.moves)}) ${["timeseries", "band"].includes(r.cfg.view) ? (r.tipOk ? "load+temp ok" : "load+temp MISSING") : "-"} | ${f(r.zoomMed, 1)} / ${f(r.zoomP95, 1)} / ${f(r.zoomMax)} | ${f(r.panP95, 1)} / ${f(r.panMax)} | ${f(r.longTask)} | ${f(r.zoomActionMs)} | ${f(r.toHourlyMs)} / ${f(r.to15Ms)} | ${f(r.heapMB)} | ${r.ok} |`;
md += head + ts.map(row).join("\n") + "\n\n";
if (oth.length) md += `## Other chart types (35,040 points)\n\n` + head + oth.map(row).join("\n") + "\n\n";
md += `## Bundle size (production build, all JS)\n\n| Import style | raw KB | gzip KB |\n|---|---|---|\n`;
md += `| tree-shaken (\`echarts/core\` + line, scatter, grid, tooltip, legend, dataZoom, dataset, aria, canvas) + React 19 | ${f(bundleTree.raw / 1024)} | ${f(bundleTree.gz / 1024)} |\n`;
md += `| full \`import * from "echarts"\` + React 19 | ${f(bundleFull.raw / 1024)} | ${f(bundleFull.gz / 1024)} |\n\n`;
md += `Not measured by this script: real-GPU rendering, Safari/Firefox, MUI/TanStack Query overhead, touch input, slider-handle drag.\n`;
writeFileSync(OUT, md);
console.log(`\nwrote ${OUT}`);
