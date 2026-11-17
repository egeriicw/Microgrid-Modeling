/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: { chunkSizeWarningLimit: 1200 },
  // `npm run dev` talks to a locally running API (uvicorn on port 8000) through the same /api path nginx proxies.
  server: { proxy: { "/api": { target: "http://127.0.0.1:8000", changeOrigin: false } } },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
    css: false,
  },
});
