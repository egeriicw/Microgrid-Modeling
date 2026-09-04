# API Specification — `api` (FastAPI)

Base URL (Compose): `http://localhost:8000`. OpenAPI/Swagger at `/docs`,
ReDoc at `/redoc`, schema at `/openapi.json` (generated — this file is the
human contract and the source for contract tests).

Conventions:
- JSON everywhere except `/configs/import` (`text/yaml` in) and
  `/configs/{id}/export` (`text/yaml` out) and SSE (`text/event-stream`).
- Auth: `Authorization: Bearer <API_TOKEN>` on every `POST/PUT/DELETE`
  (FR-A17). `GET` is open on the local network.
- Errors: `{ "error": { "code": str, "message": str, "fields"?: [{"loc": [..], "msg": str}] } }`.
- Pagination: `?limit` (default 25, max 100) + `?cursor` (opaque);
  responses wrap lists as `{ "items": [...], "next_cursor": str | null }`.
- Timestamps: ISO-8601 UTC. IDs: configs `int`, runs `uuid4` string.

---

## 1. Health

### `GET /health` → 200
`{ "status": "ok" }` — liveness only, no dependency checks. (FR-A1)

### `GET /ready` → 200 | 503
```json
{ "ready": true, "checks": { "db": "ok", "redis": "ok", "storage": "ok" } }
```
`503` with the same shape and `"ready": false` if any check fails.

---

## 2. Configs (FR-A2..A5)

### `GET /configs?template={bool}&limit=&cursor=` → 200
`items[]`: `{ id, name, version, is_template, created_at, updated_at }`.

### `POST /configs` → 201 | 401 | 422
Body: `{ "name": str, "body": ScenarioConfigBody }`.
`422` when `body` fails scenario validation (`fields[]` populated).
Response: full config (see `GET /configs/{id}`).

### `POST /configs/validate` → 200
Body: `{ "body": ScenarioConfigBody }`.
Response: `{ "valid": bool, "errors": [{ "loc": [..], "msg": str }] }`.
**Always 200** — this endpoint reports validity, it does not enforce it.
Validity semantics == `snlg.config.validate_config` + Pydantic structural
checks + engine-parity checks (FR-A16).

### `GET /configs/{id}` → 200 | 404
`{ id, name, version, is_template, body: ScenarioConfigBody, created_at, updated_at }`.

### `PUT /configs/{id}` → 200 | 401 | 404 | 409 | 422
Body: `{ "name"?: str, "body"?: ScenarioConfigBody }`.
Creates a new `config_versions` row, bumps `configs.version`. `409` if
`is_template`. Response: updated config.

### `GET /configs/{id}/versions` → 200
`items[]`: `{ version, body, created_at, note? }` (immutable history).

### `POST /configs/{id}/clone` → 201 | 401 | 404
Body: `{ "name": str }`. New config, `version=1`, `is_template=false`,
`body` copied.

### `DELETE /configs/{id}` → 204 | 401 | 404 | 409
`409` if `is_template` or any `runs.config_id == id`.

### `POST /configs/import` → 201 | 401 | 422
`Content-Type: text/yaml`, raw YAML body. Parsed through the same path as
`snlg.config.load_config`; stored as structured JSON. Response: full config.

### `GET /configs/{id}/export` → 200 | 404
`Content-Type: text/yaml`. Round-trips through `/configs/import` to an
equivalent body.

### `ScenarioConfigBody` (schema mirror of `snlg.config.ScenarioConfig`, D6)

```jsonc
{
  "name": "DC Multifamily Baseline",
  "description": "…",
  "rng": { "composition_seed": 1001, "archetype_seed": 2002, "perturbation_seed": 3003 },
  "data": {
    "upgrade": "0", "state": "DC",
    "resstock_chars_file": "DC_upgrade0.xlsx",
    "comstock_chars_file": "DC_upgrade0_agg.xlsx"
    // timeseries dirs are supplied by the server (DATA_DIR), not the client
  },
  "composition": {
    "total_buildings": 50,
    "method": "dirichlet",                 // enum: dirichlet | uniform_renorm
    "dirichlet_concentration": 5.0,
    "categories": {
      "mf_small": { "min_fraction": 0.10, "max_fraction": 0.85, "source": "resstock" },
      "commercial_private": { "fixed_count": 4, "source": "comstock" }
    }
  },
  "archetype_filters": {
    "resstock": {
      "mf_small": {
        "building_type_heights": ["Multi-Family with 2 - 4 Units"],
        "hvac_types": null, "hvac_types_exclude": null,
        "max_sqft": 2000, "require_no_garage": false,
        "require_data_complete": true
        // FR-E6: heatpump/electric_vehicle/solar_pv adoption keys — either
        // typed here or rejected; NOT silently ignored
      }
    },
    "comstock": {
      "commercial_private": { "building_types": ["SmallOffice","MediumOffice","Outpatient"], "include_public": false }
    }
  },
  "perturbation": {
    "enabled": false, "max_temporal_shift_hours": 2, "energy_tolerance": 0.05,
    "end_use_factors": { "lighting": { "sigma": 0.10 }, "hvac": { "sigma": 0.12 } }
  },
  "feasibility": {
    "hard": { "peak_kw": {"min":5,"max":50000}, "annual_energy_kwh": {"min":1000,"max":1e8}, "load_factor": {"min":0.05,"max":0.99} },
    "soft": { "weights": {"peak_kw":1.0,"load_factor":2.0},
              "bounds": {"peak_kw": {"min":50,"max":5000}, "load_factor": {"min":0.2,"max":0.8}} }
  },
  "ensemble": { "mode": "adaptive", "min_runs": 50, "max_runs": 300, "check_every": 10 },
  "convergence": { "lookback_window": 10, "min_stable_window": 10, "epsilon": 0.01,
                   "metrics": ["peak_kw","annual_energy_kwh","load_factor"] },
  "validation": { "benchmarks": { "peak_demand_kw": [50,5000], "load_factor": [0.2,0.8] }, "min_cv_peak": 0.01 },
  "output": { "per_run_csvs": true, "percentiles": [0.10,0.50,0.90],
              "representative_cases": true, "validation_report": true, "preserve_legacy_format": true }
}
```

---

## 3. Runs (FR-A6..A9)

### `POST /runs` → 202 | 401 | 404 | 422
Body:
```json
{ "config_id": 12,
  "overrides": {
    "seeds": { "composition_seed": 7, "archetype_seed": 8, "perturbation_seed": 9 },
    "ensemble": { "max_runs": 40, "mode": "fixed" }
  } }
```
`overrides` optional; any subset. `max_runs > MAX_RUNS_CEILING` → `422`.
Response: `{ "run_id": "…", "status": "queued" }`. Side effects: insert
`runs` row with frozen `config_snapshot`, `seeds`, `engine_version`; enqueue
`run_ensemble_task` on the `runs` RQ queue.

### `GET /runs?status=&config_id=&active={bool}&limit=&cursor=` → 200
`items[]`:
```json
{ "id":"…","status":"running","config_id":12,"config_name":"DC MF Baseline",
  "created_at":"…","started_at":"…","finished_at":null,
  "progress": { "current": 37, "total": 300, "n_feasible": 31, "message": "run 37 feasible", "converged": false } }
```
`active=true` ⇔ `status in (queued, running)`.

### `GET /runs/{id}` → 200 | 404
Adds to the list shape: `engine_version`, `seeds`, `config_snapshot`,
`error_message`, `log_tail` (≤200 lines), and `summary` (null until
terminal — see `/summary`).

### `POST /runs/{id}/cancel` → 202 | 401 | 404 | 409
`queued` → dequeued + `cancelled`. `running` → Redis cancel flag set;
worker finalizes `cancelled` within 5s. Terminal → `409`.

### `GET /runs/{id}/events` → 200 `text/event-stream`
SSE. First event = current state; then live `RunProgressEvent`s from Redis
channel `progress:{run_id}`; a terminal event (`stage ∈ {done,failed,
cancelled}`) then stream close.
```
event: progress
data: {"stage":"running","run_index":37,"n_attempted":42,"n_feasible":31,"converged":false,"message":"…","timestamp":1712345678.9}

event: progress
data: {"stage":"done","n_attempted":180,"n_feasible":150,"converged":true,"message":"ensemble converged","timestamp":…}
```

---

## 4. Results (FR-A10..A12)

### `GET /runs/{id}/metrics` → 200 | 404 | 409
Per-Monte-Carlo-run scalars (all attempted runs, feasible flag included):
```json
{ "items": [
  { "run_index":0, "feasible":true, "peak_kw":812.4, "annual_energy_kwh":3.1e6,
    "load_factor":0.44, "soft_score":0.0, "coincidence_factor":0.61 }
]}
```
`409 {Retry-After}` while `running` with no partial data.

### `GET /runs/{id}/summary` → 200 | 404 | 409
```json
{
  "converged": true, "n_attempted": 180, "n_feasible": 150, "n_rejected": 30,
  "diversity_metrics": { "mean_peak_kw": 790.2, "std_peak_kw": 61.0, "cv_peak": 0.077,
                         "mean_annual_energy_kwh": 3.0e6, "mean_load_factor": 0.43 },
  "convergence_history": { "peak_kw": [812.4, 800.1, …], "annual_energy_kwh": [...], "load_factor": [...] },
  "validation_report": { "passed": true, "warnings": [], "checks": { "peak_demand_kw": true, "diversity_collapse": true, "synchronization": true } },
  "representative_cases": { "median": {"run_index":73,"peak_kw":789.9,"annual_energy_kwh":3.0e6,"load_factor":0.43},
                            "high_peak": {...}, "low_load_factor": {...} }
}
```

### `GET /runs/{id}/series/envelope` → 200 (ETag, gzip)
`{ "index": ["2018-01-01T00:00:00Z", …8760], "p10": [...], "p50": [...], "p90": [...] }`
(keys follow `output.percentiles`). Read from the MinIO Parquet artifact.

### `GET /runs/{id}/series/load-duration?case={median|high_peak|low_load_factor|p50|total}` → 200
`{ "exceedance": [0.000114, …], "kw": [1420.0, 1418.3, …] }` (sorted desc).

### `GET /runs/{id}/artifacts` → 200 | 404
```json
{ "items": [
  { "kind":"compiled_runs_csv", "filename":"…_compiled-runs.csv",
    "content_type":"text/csv", "bytes":184320, "sha256":"…",
    "partial": false, "download_url":"https://minio…?X-Amz-Expires=900…" }
]}
```
`kind` ∈ `per_run_csv`, `compiled_runs_csv`, `load_profile_total_csv`,
`percentile_envelopes_csv`, `ensemble_statistics_csv`,
`diversity_metrics_csv`, `convergence_history_csv`,
`representative_case_csv`, `series_parquet`, `summary_figure_png`,
`load_duration_png`, `overview_txt`, `full_log`.

### `DELETE /runs/{id}` → 204 | 401 | 404 | 409
Transactionally deletes DB rows + purges MinIO objects under
`runs/{id}/`. On partial failure → run row kept with `status=delete_failed`
(NFR-8).

---

## 5. Templates & seed

`GET /configs?template=true` lists templates. The seed script (FR-D5)
inserts `DC Multifamily Baseline` (from
`configs/scenarios/dc_multifamily_baseline.yaml`, `total_buildings=10`) as
`is_template=true`.

---

## 6. Status-code summary

| Code | Meaning |
|---|---|
| 200 | OK (read, validate, export) |
| 201 | Created (config, clone, import) |
| 202 | Accepted (run enqueued, cancel requested) |
| 204 | Deleted |
| 401 | Missing/invalid bearer token on a mutation |
| 404 | Unknown config/run id |
| 409 | Conflict (delete config with runs, edit/delete template, cancel terminal run) |
| 422 | Body failed validation (`fields[]`) or `max_runs` over ceiling |
| 503 | `/ready` — a dependency is down |

---

## 7. OpenAPI contract test

`api/tests/test_openapi_contract.py` snapshots `/openapi.json` and fails on
unreviewed drift; every endpoint above has a green functional test per the
traceability index in `requirements.md`.
