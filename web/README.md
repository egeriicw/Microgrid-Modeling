# Community Energy Profiler web app

React + TypeScript + Vite, MUI, TanStack Query, react-router, Apache ECharts (`echarts/core`, tree-shaken).
The UI depends only on the `ApiClient` interface in `src/api/types.ts`. Two implementations:

- `src/api/httpClient.ts` — the real API. Types are generated from `api/openapi.json` into
  `src/api/generated/openapi.ts` (`npm run gen:api`; `npm test` fails if that file is out of date). Bearer API key,
  kept in sessionStorage only; live progress over server-sent events read with `fetch` (so the key can be sent),
  reconnecting with `Last-Event-ID`.
- `src/api/mockClient.ts` — fixtures (`src/api/fixtures.ts`) with the same rules and shapes as the real API, for
  tests and offline work. In mock mode the API key is the role name: `admin`, `client` or `demo`.

All HTTP stays in `src/api/` (lint forbids `fetch` elsewhere).

Which client runs (`src/api/select.ts`): `VITE_API_MODE=mock` selects the mock; anything else uses the real API at
`VITE_API_BASE_URL` (default `/api/v1`, same origin). In the container nginx proxies `/api/` to the API; `npm run dev`
proxies `/api` to `http://127.0.0.1:8000`.

```bash
npm ci                          # .npmrc sets legacy-peer-deps; see the note in that file
npm run dev                     # http://localhost:5173 against a local API on port 8000
VITE_API_MODE=mock npm run dev  # no backend: mock API (key = admin, client or demo)
npm test                        # generated-types check + vitest + testing-library
npm run gen:api                 # regenerate API types after api/openapi.json changes
npm run typecheck && npm run lint && npm run build
npm run e2e                     # Playwright against the mock (needs a Chromium matching @playwright/test)
```

Roles (admin / client / demo) only hide controls; the server enforces permissions. The real API does not report the
caller's role yet, so with the real client every control is shown and the server's 403 is displayed.

`spikes/echarts-35k/` is the Sprint 1 performance spike, kept for reference; its tuned options live on in
`src/features/results/chartOptions.ts` and `src/charts/hoverOverlay.ts`.
