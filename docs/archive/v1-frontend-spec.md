# Frontend Specification — `web` (Next.js)

Stack (matches `feat/frontend-foundation-nextjs`): **Next.js 14, Pages
Router, React 18, TypeScript, MUI 5**. Charts: **Recharts**. Tests: Jest +
React Testing Library (config already scaffolded on the feat branch) +
Playwright for e2e. Reflects D4 and FR-F1..F10.

---

## 1. App structure

```
web/src/
  pages/
    _app.tsx              MUI theme + CssBaseline + ApiTokenProvider + QueryClient
    _document.tsx         emotion SSR
    index.tsx             dashboard: recent runs + quick "new from template"
    configs/
      index.tsx           config list (FR-F2)
      new.tsx             create-from-template picker
      [id].tsx            config editor (FR-F3)
    runs/
      index.tsx           runs table (FR-F5)
      [id].tsx            run detail + live progress (FR-F6) + results tabs (FR-F7)
    compare.tsx           two-run compare (FR-F8, P2)
  components/
    AppShell.tsx          nav, HealthDot, token menu (FR-F1)
    HealthDot.tsx
    ApiTokenDialog.tsx
    ConfigForm/           structured editor sections
      CompositionSection.tsx
      ArchetypeFiltersSection.tsx
      FeasibilitySection.tsx
      PerturbationSection.tsx
      EnsembleSection.tsx
      ValidationSection.tsx
      OutputSection.tsx
    YamlEditor.tsx        raw-YAML tab (textarea + import/export)
    ValidationErrors.tsx  renders {loc,msg}[] inline + summary
    LaunchRunDialog.tsx   override seeds / max_runs / mode (FR-F4)
    RunsTable.tsx         live-updating (FR-F5)
    RunStatusChip.tsx
    RunProgress.tsx       SSE progress bar + n_feasible + stage (FR-F6)
    LogPane.tsx           virtualized tail log
    results/
      EnvelopeChart.tsx        P10–P90 area + P50 line (FR-F7a)
      PeakHistogram.tsx        (FR-F7b)
      EnergyHistogram.tsx      (FR-F7c)
      LoadDurationChart.tsx    case selector (FR-F7d)
      ConvergenceChart.tsx     normalized lines (FR-F7e)
      RepresentativeCases.tsx  table (FR-F7f)
      ValidationPanel.tsx      pass/fail + warnings (FR-F7g)
      ArtifactList.tsx         download links (FR-F7h)
  lib/
    apiClient.ts          typed REST client + error normalization (FR-F9)
    sse.ts                EventSource wrapper w/ backoff (FR-F9)
    scenarioSchema.ts     TS types mirroring ScenarioConfigBody
    useRun.ts / useConfig.ts / useRuns.ts   data hooks (react-query)
    format.ts            kW / MWh / % / duration formatters
  styles/theme.tsx        MUI theme, light+dark, chart palette tokens
  e2e/
    run-lifecycle.spec.ts (FR-F10)
```

State: **@tanstack/react-query** for server state (polling, cache,
invalidation); local component state for form drafts; `ApiTokenProvider`
(React context + `localStorage`) for the bearer token.

---

## 2. Routes & flows

| Route | Purpose | Key API calls |
|---|---|---|
| `/` | Landing: last 10 runs (`RunsTable` compact), "New from template" | `GET /runs`, `GET /configs?template=true` |
| `/configs` | All configs; row actions Edit / Clone / Launch / Delete | `GET /configs`, `DELETE /configs/{id}` |
| `/configs/new` | Choose a template → `POST /configs/{tpl}/clone` → redirect to editor | `GET /configs?template=true`, `POST /configs/{id}/clone` |
| `/configs/[id]` | Editor: Form ⇄ YAML tabs; debounced validate; Save (new version) | `GET /configs/{id}`, `POST /configs/validate`, `PUT /configs/{id}`, `POST /configs/import`, `GET /configs/{id}/export` |
| `/runs` | Runs table; filters status + config; live | `GET /runs?...&active=true` (3s poll) |
| `/runs/[id]` | Tabs: **Progress** (live) and **Results** (when terminal) | `GET /runs/{id}`, `GET /runs/{id}/events` (SSE), `POST /runs/{id}/cancel`, `GET /runs/{id}/summary`, `/metrics`, `/series/*`, `/artifacts` |
| `/compare?a=&b=` | Overlaid envelopes + scalar delta table (P2) | `GET /runs/{id}/summary` ×2, `/series/envelope` ×2 |

### UJ-1 flow (the acceptance path, PRD §3)

1. `/configs/new` → pick "DC Multifamily Baseline" → clone → `/configs/{new}`.
2. Editor Form tab: change `composition.total_buildings`, tweak a
   category's `max_fraction`. Each edit → `POST /configs/validate` (400ms
   debounce) → `ValidationErrors` shows none → **Save** (confirms "creates
   version 2").
3. Row **Launch** (or editor's Launch button) → `LaunchRunDialog`
   (defaults) → `POST /runs` → redirect `/runs/{id}` Progress tab.
4. `RunProgress` subscribes to SSE: bar advances `current/total`,
   `n_feasible` climbs, `LogPane` tails. **Cancel** available.
5. Terminal `done` event → toast + auto-switch to **Results** tab.
6. Results: `EnvelopeChart`, `PeakHistogram`, `EnergyHistogram`,
   `LoadDurationChart` (case = `median`), `ConvergenceChart`,
   `RepresentativeCases`, `ValidationPanel`, `ArtifactList`.
7. `ArtifactList` → click `compiled_runs_csv` → browser downloads via
   presigned URL.

---

## 3. Component contracts (test targets)

### `apiClient.ts` (FR-F9)
- One method per endpoint in `api-spec.md`, fully typed.
- Injects `Authorization: Bearer` from the token context.
- Normalizes non-2xx to `ApiError { status, code, message, fieldErrors?:
  {loc:string[], msg:string}[] }`.
- `createRunEvents(runId): SseHandle` — wraps `EventSource`, exposes
  `on('progress'|'done'|'error')`, reconnects with capped exponential
  backoff, stops on terminal event.
- **Tests** (`apiClient.test.ts`): token header attached; 401 →
  `ApiError.status===401`; 422 → `fieldErrors` populated; SSE handle emits
  parsed events and closes on terminal.

### `RunProgress.tsx` (FR-F6)
- Props: `{ runId }`. Uses `createRunEvents`.
- Renders `LinearProgress` (`value = current/total*100`), `n_feasible`
  chip, `stage` label, and `LogPane`.
- On `done` → calls `onComplete()` (parent switches tab); on `failed` →
  shows `error_message`; on `cancelled` → shows cancelled state.
- **Tests** (`RunProgress.test.tsx`): given a mocked SSE stream of 3
  progress + 1 done, the bar reaches 100% and `onComplete` fires; a
  `failed` event renders the error text.

### `ConfigForm` + `ValidationErrors` (FR-F3)
- Controlled form bound to a `ScenarioConfigBody` draft.
- `onChange` → debounced `apiClient.validateConfig(draft)` →
  `setErrors(res.errors)`.
- Save button `disabled={errors.length>0 || !dirty}`.
- `ValidationErrors` maps `loc` (e.g. `["composition","categories","mf_small","max_fraction"]`)
  to a human path and, where possible, scrolls/focuses the field.
- **Tests**: entering `min_fraction` sum > 1 shows the simplex error from
  the API; fixing it re-enables Save; YAML tab paste of a malformed doc
  shows a parse error.

### `EnvelopeChart.tsx` / `LoadDurationChart.tsx` (FR-F7)
- Props: `{ series }` from `/series/*`. Recharts `AreaChart` (p10→p90 band)
  + `Line` (p50); `LoadDurationChart` = `AreaChart` of `kw` vs
  `exceedance` with a `case` `Select`.
- Theme-aware colors from `styles/theme.tsx` palette tokens; follows the
  `dataviz` skill (accessible contrast, single palette, axis labels with
  units, tooltip with formatted values).
- Empty state ("no feasible runs") when arrays are empty.
- **Tests**: renders an SVG with the expected series count; case selector
  triggers a refetch with the new `case` param; empty data → empty-state
  text.

### `RunsTable.tsx` (FR-F5)
- Polls `GET /runs` (react-query `refetchInterval: 3000` while any row is
  active). Columns per `api-spec.md` §3. Status chip via `RunStatusChip`.
- **Tests**: active runs trigger polling; terminal-only list stops
  polling; filter props narrow the query.

### `AppShell.tsx` + `HealthDot.tsx` (FR-F1)
- `HealthDot` polls `/health` (10s) and `/ready` (30s); green/amber/red.
- `ApiTokenDialog` sets/clears the token; a 401 anywhere opens it.
- **Tests**: 503 `/ready` → amber with tooltip listing the down dependency;
  saving a token updates context and retries the last failed query.

---

## 4. Error & loading UX

- Global react-query `onError` → snackbar with `ApiError.message`.
- Forms show field errors inline; non-field errors as an alert atop the
  form.
- Skeletons for tables/charts; charts never render partial axes on
  `undefined` data.
- SSE disconnect → `RunProgress` shows "reconnecting…" and falls back to
  polling `GET /runs/{id}` every 5s until the stream returns.

---

## 5. Visualization rules (defer to `dataviz` skill at build time)

- One categorical palette defined once in `theme.tsx`; charts import
  tokens, never hardcode hex.
- Light **and** dark variants; verify AA contrast (NFR-11).
- Axes always labelled with units (`kW`, `MWh`, `hour of year`,
  `exceedance`); tooltips use `format.ts`.
- Envelope band = translucent fill + solid median line; histograms =
  single hue + median reference line; convergence = one line per metric,
  normalized to run 1, with a `y=1` reference.
- Load-duration curve: x = exceedance fraction 0→1, y = kW, log-y optional
  toggle.

---

## 6. Build/run

- Dev: `web` container runs `next dev` on `:3000`; `NEXT_PUBLIC_API_BASE_URL`
  → `http://api:8000` inside Compose, `http://localhost:8000` for local
  `npm run dev`.
- Prod image: `next build` + `next start` (multi-stage Dockerfile,
  `infrastructure.md` §4).
- `npm test` (jest), `npm run test:e2e` (Playwright against the Compose
  stack with `MICROGRID_RUNNER_MODE=mock`).
