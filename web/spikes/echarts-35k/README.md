# ECharts 35k-point spike (story 1.12)

Throwaway spike, not the product. Findings and recommendation: `docs/research/echarts-spike.md`.

```bash
cd web/spikes/echarts-35k
npm ci               # .npmrc sets legacy-peer-deps (see findings, section 5)
npm test             # vitest: data generator, percentile, lttb, hourly mean, load-duration, option builders
npm run dev          # interactive page: pick view, mode (naive / tuned / lttb / overlay), series, resolution
npm run measure      # builds, serves the production build, drives headless Chromium, writes measure/results.md
                     # options: --runs 3 --naive-runs 1 --quick --screenshots --filter "timeseries lttb"
```

`npm run measure` needs Playwright resolvable from Node (`npm i -D playwright`, a global install, or `NODE_PATH`) and a
Chromium that Playwright can launch (or `CHROMIUM_PATH=/path/to/chrome`). A full run takes about 65 minutes because the
naive 35,040-point cases take minutes each; `--quick` takes a few minutes.
`measure/results.md` is the recorded cloud-VM run (not an Apple M2); re-running overwrites it.
Data is deterministic synthetic data only.
