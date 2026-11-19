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
PW_CHROMIUM_EXECUTABLE=/opt/pw-browsers/chromium-1243/chrome-linux64/chrome npm run e2e   # use an installed Chromium
```

`PW_CHROMIUM_EXECUTABLE` (optional) points Playwright at an already-installed Chromium when the bundled browser build
does not match the `@playwright/test` version (the cloud image ships build 1243). With the mock client active, the
header shows "Demo data — not connected to a server" on every page.

**Security rule: server text is text.** Anything that comes from the server (error details today; admin-written
filter labels and help text later) is rendered as plain text through React or `textContent`. Never use
`dangerouslySetInnerHTML`, `innerHTML`/`outerHTML`, `insertAdjacentHTML`, or an HTML/markdown renderer on server
strings. ESLint enforces the first three (`no-restricted-syntax` in `eslint.config.js`).

Roles (admin / client / demo) only hide controls; the server enforces permissions. Sign-in calls `GET /me`; the
session's role, permission flags, allowed run inputs, seed permission and limits come only from that answer
(`src/auth/roles.ts` turns them into what the UI offers; the role name itself is never interpreted). If `/me` fails,
the user is not signed in. The composition form uses `GET /building-types` and posts `composition` to `POST /runs`;
it never sends a convergence rule (the server's default applies). Its checks mirror the API's rules using the limits
from `/me`, but every server refusal is shown as text.

Manual check against a real API with the synthetic fixture: `e2e/real-api-compose.spec.ts` (see its header).

`spikes/echarts-35k/` is the Sprint 1 performance spike, kept for reference; its tuned options live on in
`src/features/results/chartOptions.ts` and `src/charts/hoverOverlay.ts`.
