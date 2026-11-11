# ECharts 35k spike: measurement results

Generated 2026-11-11T05:23:03.350Z by `npm run measure` (runs per case: 3, medians reported).

## Environment

- Machine: Intel(R) Xeon(R) Processor @ 2.10GHz x 4 logical cores, 15.7 GiB RAM, linux 6.18.44-fc-v378 (x64)
- Browser: headless Chromium 153.0.8010.12, software rendering (no GPU), Playwright, viewport 1100x700, chart 1000x520, canvas renderer
- UA: `Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/153.0.8010.12 Safari/537.36`
- Node v22.23.4; echarts 6.1.0; react 19.2.9; vite 8.3.4; lockfile present
- Production build served by `vite preview`. These numbers are from a cloud VM, not an Apple M2 laptop; absolute values will differ.

## Time-series chart (total, residential, commercial, P10, P90 + temperature on a second axis)

Columns: render = setOption to ECharts "finished"; TTFI = page navigation to first tooltip shown; tooltip = mouse move to tooltip DOM update, with (updates seen / mouse moves) and a check that the tooltip text shows load and temperature together (time-series/band views only); zoom/pan frame = gap between animation frames during real wheel/drag (16.7 ms = 60 fps); dispatch = mean dispatchAction(dataZoom) to "finished"; heap = JS heap after GC.

| Case | data gen ms | option ms | render ms | TTFI ms | tooltip med / p95 ms | zoom frame med / p95 / max ms | pan frame p95 / max ms | long-task ms | dispatch zoom ms | ->hourly / ->15min ms | heap MB | runs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| timeseries / naive / 1 load series / 8,760 pts | 74 | 2 | 1974 | 2797 | 360.7 / 432.2 (40/40) load+temp ok | 200.0 / 566.6 / 650 | 166.7 / 217 | 20980 | 788 | n/a / n/a | 35 | 3 |
| timeseries / tuned / 1 load series / 8,760 pts | 79 | 6 | 89 | 905 | 68.0 / 87.7 (40/40) load+temp ok | 16.7 / 133.4 / 167 | 33.4 / 50 | 1245 | 51 | n/a / n/a | 6 | 3 |
| timeseries / lttb / 1 load series / 8,760 pts | 90 | 8 | 108 | 917 | 24.9 / 39.8 (40/40) load+temp ok | 16.7 / 83.3 / 83 | 33.3 / 33 | 728 | 36 | n/a / n/a | 6 | 3 |
| timeseries / overlay / 1 load series / 8,760 pts | 86 | 6 | 108 | 889 | 7.4 / 9.9 (40/40) load+temp ok | 16.7 / 83.3 / 100 | 33.4 / 33 | 630 | 37 | n/a / n/a | 6 | 3 |
| timeseries / naive / 3 load series / 8,760 pts | 71 | 3 | 2657 | 3566 | 719.0 / 829.5 (40/40) load+temp ok | 366.7 / 1083.4 / 1217 | 316.6 / 383 | 39092 | 1041 | n/a / n/a | 65 | 3 |
| timeseries / tuned / 3 load series / 8,760 pts | 76 | 6 | 102 | 1003 | 160.5 / 186.9 (40/40) load+temp ok | 16.7 / 233.3 / 283 | 66.6 / 67 | 5339 | 99 | n/a / n/a | 6 | 3 |
| timeseries / lttb / 3 load series / 8,760 pts | 76 | 6 | 94 | 919 | 85.5 / 102.8 (40/40) load+temp ok | 16.7 / 133.3 / 150 | 50.0 / 67 | 3032 | 63 | n/a / n/a | 7 | 3 |
| timeseries / overlay / 3 load series / 8,760 pts | 72 | 6 | 96 | 882 | 7.8 / 9.1 (40/40) load+temp ok | 16.7 / 133.3 / 150 | 50.0 / 67 | 2381 | 59 | n/a / n/a | 6 | 3 |
| timeseries / naive / 5 load series / 8,760 pts | 72 | 9 | 3147 | 4024 | 1153.4 / 1281.8 (40/40) load+temp ok | 583.3 / 1366.6 / 1633 | 500.0 / 683 | 59274 | 1597 | n/a / n/a | 95 | 3 |
| timeseries / tuned / 5 load series / 8,760 pts | 75 | 6 | 106 | 1143 | 297.3 / 329.4 (40/40) load+temp ok | 16.7 / 333.3 / 383 | 83.5 / 117 | 7896 | 136 | n/a / n/a | 7 | 3 |
| timeseries / lttb / 5 load series / 8,760 pts | 75 | 5 | 107 | 1003 | 182.8 / 214.0 (40/40) load+temp ok | 16.7 / 200.0 / 233 | 83.3 / 117 | 6292 | 103 | n/a / n/a | 7 | 3 |
| timeseries / overlay / 5 load series / 8,760 pts | 75 | 6 | 86 | 974 | 7.8 / 9.2 (40/40) load+temp ok | 16.7 / 216.7 / 250 | 83.3 / 100 | 5916 | 32 | n/a / n/a | 7 | 3 |
| timeseries / naive / 1 load series / 35,040 pts | 267 | 9 | 2973 | 4140 | 893.4 / 1022.0 (40/40) load+temp ok | 549.9 / 1450.0 / 1567 | 483.2 / 517 | 54064 | 1634 | 1496 / 1386 | 126 | 1 |
| timeseries / tuned / 1 load series / 35,040 pts | 251 | 14 | 135 | 1177 | 99.6 / 117.6 (40/40) load+temp ok | 16.7 / 166.7 / 183 | 50.0 / 67 | 3187 | 68 | 32 / 55 | 13 | 3 |
| timeseries / lttb / 1 load series / 35,040 pts | 268 | 15 | 128 | 1122 | 24.2 / 35.4 (40/40) load+temp ok | 16.7 / 83.3 / 83 | 33.3 / 33 | 694 | 34 | 28 / 37 | 13 | 3 |
| timeseries / overlay / 1 load series / 35,040 pts | 266 | 13 | 124 | 1100 | 7.5 / 8.5 (40/40) load+temp ok | 16.7 / 66.8 / 83 | 33.3 / 33 | 620 | 34 | 25 / 50 | 13 | 3 |
| timeseries / naive / 3 load series / 35,040 pts | 244 | 13 | 4209 | 5528 | 1866.7 / 2085.3 (40/40) load+temp ok | 1033.2 / 2549.9 / 2983 | 900.0 / 1250 | 106678 | 2944 | 3048 / 2937 | 246 | 1 |
| timeseries / tuned / 3 load series / 35,040 pts | 259 | 14 | 190 | 1417 | 221.2 / 264.1 (40/40) load+temp ok | 16.7 / 316.6 / 333 | 100.0 / 117 | 7787 | 127 | 29 / 91 | 13 | 3 |
| timeseries / lttb / 3 load series / 35,040 pts | 267 | 19 | 173 | 1252 | 88.2 / 107.0 (40/40) load+temp ok | 16.7 / 150.0 / 167 | 50.0 / 67 | 3756 | 62 | 30 / 40 | 13 | 3 |
| timeseries / overlay / 3 load series / 35,040 pts | 272 | 14 | 138 | 1207 | 7.5 / 9.5 (40/40) load+temp ok | 16.7 / 149.9 / 167 | 50.0 / 67 | 3535 | 62 | 27 / 64 | 13 | 3 |
| timeseries / naive / 5 load series / 35,040 pts | 361 | 22 | 6580 | 8245 | 3053.8 / 3473.2 (40/40) load+temp ok | 1533.3 / 4183.1 / 4950 | 1400.0 / 1650 | 162791 | 4663 | 4627 / 5808 | 362 | 1 |
| timeseries / tuned / 5 load series / 35,040 pts | 286 | 13 | 195 | 1715 | 436.8 / 495.8 (40/40) load+temp ok | 16.7 / 466.6 / 500 | 149.9 / 200 | 11468 | 202 | 35 / 102 | 13 | 3 |
| timeseries / lttb / 5 load series / 35,040 pts | 250 | 14 | 142 | 1290 | 196.8 / 231.6 (40/40) load+temp ok | 16.7 / 233.2 / 267 | 83.4 / 150 | 7060 | 108 | 27 / 61 | 13 | 3 |
| timeseries / overlay / 5 load series / 35,040 pts | 244 | 13 | 146 | 1292 | 7.6 / 9.1 (40/40) load+temp ok | 16.7 / 216.6 / 267 | 83.4 / 117 | 6482 | 108 | 28 / 49 | 13 | 3 |

## Other chart types (35,040 points)

| Case | data gen ms | option ms | render ms | TTFI ms | tooltip med / p95 ms | zoom frame med / p95 / max ms | pan frame p95 / max ms | long-task ms | dispatch zoom ms | ->hourly / ->15min ms | heap MB | runs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| band / naive / 3 load series | 254 | 12 | 5118 | 6469 | 2370.4 / 2735.5 (40/40) load+temp ok | 1074.9 / 3249.8 / 3550 | 1049.9 / 1217 | 115673 | 3380 | n/a / n/a | 246 | 1 |
| band / lttb / 3 load series | 280 | 16 | 188 | 1180 | 59.5 / 73.4 (40/40) load+temp ok | 16.7 / 166.7 / 183 | 66.7 / 117 | 5486 | 83 | n/a / n/a | 13 | 3 |
| band / overlay / 3 load series | 258 | 16 | 178 | 1165 | 7.8 / 9.1 (40/40) load+temp ok | 16.7 / 150.1 / 167 | 66.7 / 100 | 5183 | 52 | n/a / n/a | 13 | 3 |
| ldc / naive / 3 load series | 249 | 23 | 4142 | 5391 | 1034.6 / 1246.9 (39/40) - | 583.3 / 1816.5 / 1983 | n/a / n/a | 20707 | 2286 | n/a / n/a | 186 | 1 |
| ldc / lttb / 3 load series | 272 | 23 | 176 | 1121 | 7.6 / 14.1 (39/40) - | 16.7 / 16.8 / 33 | n/a / n/a | 0 | 17 | n/a / n/a | 26 | 3 |
| seasonal / naive / 3 load series | 266 | 10 | 1067 | 1963 | 7.7 / 17.0 (40/40) - | 16.7 / 16.7 / 17 | n/a / n/a | 0 | 16 | n/a / n/a | 4 | 1 |
| seasonal / lttb / 3 load series | 269 | 10 | 73 | 986 | 7.5 / 13.3 (40/40) - | 16.7 / 16.8 / 17 | n/a / n/a | 0 | 16 | n/a / n/a | 4 | 3 |
| scatter / naive / 1 load series | 299 | 4 | 3052 | 4054 | 9.7 / 58.2 (16/40) - | 16.7 / 16.8 / 17 | n/a / n/a | 0 | 2938 | n/a / n/a | 63 | 1 |
| scatter / tuned / 1 load series | 276 | 3 | 198 | 1099 | 7.4 / 9.0 (17/40) - | 16.7 / 16.8 / 17 | n/a / n/a | 0 | 68 | n/a / n/a | 11 | 3 |

## Bundle size (production build, all JS)

| Import style | raw KB | gzip KB |
|---|---|---|
| tree-shaken (`echarts/core` + line, scatter, grid, tooltip, legend, dataZoom, dataset, aria, canvas) + React 19 | 742 | 242 |
| full `import * from "echarts"` + React 19 | 1289 | 420 |

Not measured by this script: real-GPU rendering, Safari/Firefox, MUI/TanStack Query overhead, touch input, slider-handle drag.
