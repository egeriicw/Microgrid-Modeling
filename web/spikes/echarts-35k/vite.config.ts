import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";

// BUNDLE=full swaps the tree-shaken ECharts import for the full one, only to compare bundle sizes.
const full = process.env.BUNDLE === "full";
const lib = fileURLToPath(new URL(full ? "./src/echarts-lib-full.ts" : "./src/echarts-lib.ts", import.meta.url));

export default defineConfig({
  resolve: { alias: { "echarts-lib": lib } },
  build: { outDir: full ? "dist-full" : "dist", chunkSizeWarningLimit: 2000 },});
