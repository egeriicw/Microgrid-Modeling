# Community Energy Profiler web app

React + TypeScript + Vite, MUI, TanStack Query, react-router, Apache ECharts (`echarts/core`, tree-shaken).
Runs today against a **mock API client** (`src/api/mockClient.ts`, fixtures in `src/api/fixtures.ts`). All HTTP must
stay in `src/api/` (lint forbids `fetch` elsewhere); the UI depends only on the `ApiClient` interface in
`src/api/types.ts`. When `api/openapi.json` is final, generate a client, implement `ApiClient` with it, and swap it in
`src/main.tsx` (separate story).

```bash
npm ci            # .npmrc sets legacy-peer-deps; see the note in that file
npm run dev       # http://localhost:5173, mock API, sign in with any role
npm test          # vitest + testing-library
npm run typecheck && npm run lint && npm run build
npx playwright install chromium && npm run e2e   # e2e against the mock
```

Roles in the UI (admin / client / demo) only hide controls. The server enforces permissions; the mock client plays
that part (401/403) so the UI handles refusals.

`spikes/echarts-35k/` is the Sprint 1 performance spike, kept for reference; its tuned options live on in
`src/features/results/chartOptions.ts` and `src/charts/hoverOverlay.ts`.
