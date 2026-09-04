# Data Model — Postgres schema + MinIO object layout

Reflects D3 (Postgres metadata + object-store artifacts) and D5 (plain
Postgres 16; no per-hour rows). All schema changes ship as Alembic
revisions (FR-A15).

---

## 1. Entity overview

```mermaid
erDiagram
  CONFIGS ||--o{ CONFIG_VERSIONS : "has history"
  CONFIGS ||--o{ RUNS : "launched as"
  RUNS ||--o{ RUN_METRICS : "per Monte-Carlo run"
  RUNS ||--o{ ARTIFACTS : "produces"
  RUNS }o--o| BATCHES : "optional (v1.1)"

  CONFIGS { int id PK; text name; int version; bool is_template; jsonb body; timestamptz created_at; timestamptz updated_at }
  CONFIG_VERSIONS { bigint id PK; int config_id FK; int version; jsonb body; text note; timestamptz created_at }
  RUNS { uuid id PK; int config_id FK; int config_version; text status; jsonb config_snapshot; jsonb seeds; text engine_version; int progress_current; int progress_total; int n_feasible; text progress_message; bool converged; jsonb summary; text error_message; text log_tail; timestamptz created_at; timestamptz started_at; timestamptz finished_at }
  RUN_METRICS { bigint id PK; uuid run_id FK; int run_index; bool feasible; double peak_kw; double annual_energy_kwh; double load_factor; double soft_score; double coincidence_factor }
  ARTIFACTS { bigint id PK; uuid run_id FK; text kind; text object_key; text filename; text content_type; bigint bytes; char64 sha256; bool partial; timestamptz created_at }
  BATCHES { uuid id PK; text name; timestamptz created_at }
```

---

## 2. DDL (authoritative; Alembic generates the migrations)

```sql
-- 0001_init --------------------------------------------------------------
CREATE TABLE configs (
  id           SERIAL PRIMARY KEY,
  name         VARCHAR(200) NOT NULL,
  version      INTEGER NOT NULL DEFAULT 1,
  is_template  BOOLEAN NOT NULL DEFAULT FALSE,
  body         JSONB   NOT NULL,          -- validated ScenarioConfigBody
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX ux_configs_name ON configs (lower(name));
CREATE INDEX ix_configs_template ON configs (is_template);

CREATE TABLE config_versions (
  id          BIGSERIAL PRIMARY KEY,
  config_id   INTEGER NOT NULL REFERENCES configs(id) ON DELETE CASCADE,
  version     INTEGER NOT NULL,
  body        JSONB   NOT NULL,
  note        TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (config_id, version)
);

-- 0002_runs --------------------------------------------------------------
CREATE TYPE run_status AS ENUM
  ('queued','running','succeeded','failed','cancelled','delete_failed');

CREATE TABLE runs (
  id                UUID PRIMARY KEY,
  config_id         INTEGER NOT NULL REFERENCES configs(id) ON DELETE RESTRICT,
  config_version    INTEGER NOT NULL,
  status            run_status NOT NULL DEFAULT 'queued',
  config_snapshot   JSONB   NOT NULL,        -- frozen effective config (base+overrides)
  seeds             JSONB   NOT NULL,        -- {composition_seed, archetype_seed, perturbation_seed}
  engine_version    TEXT    NOT NULL,        -- snlg.__version__ at launch
  progress_current  INTEGER,
  progress_total    INTEGER,
  n_feasible        INTEGER NOT NULL DEFAULT 0,
  progress_message  VARCHAR(500),
  converged         BOOLEAN NOT NULL DEFAULT FALSE,
  summary           JSONB,                   -- diversity_metrics, convergence_history,
                                             -- validation_report, representative_cases, counts
  error_message     TEXT,
  log_tail          TEXT,                    -- last ~200 lines; full log is an artifact
  batch_id          UUID,                    -- nullable; v1.1 sweeps (OQ-4)
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at        TIMESTAMPTZ,
  finished_at       TIMESTAMPTZ
);
CREATE INDEX ix_runs_status  ON runs (status);
CREATE INDEX ix_runs_config  ON runs (config_id);
CREATE INDEX ix_runs_created ON runs (created_at DESC);

CREATE TABLE run_metrics (
  id                 BIGSERIAL PRIMARY KEY,
  run_id             UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  run_index          INTEGER NOT NULL,
  feasible           BOOLEAN NOT NULL,
  peak_kw            DOUBLE PRECISION,
  annual_energy_kwh  DOUBLE PRECISION,
  load_factor        DOUBLE PRECISION,
  soft_score         DOUBLE PRECISION,
  coincidence_factor DOUBLE PRECISION,
  UNIQUE (run_id, run_index)
);
CREATE INDEX ix_run_metrics_run ON run_metrics (run_id);

CREATE TABLE artifacts (
  id            BIGSERIAL PRIMARY KEY,
  run_id        UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  kind          TEXT NOT NULL,
  object_key    TEXT NOT NULL,               -- MinIO key, unique
  filename      TEXT NOT NULL,
  content_type  TEXT NOT NULL,
  bytes         BIGINT NOT NULL,
  sha256        CHAR(64) NOT NULL,
  partial       BOOLEAN NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (object_key)
);
CREATE INDEX ix_artifacts_run ON artifacts (run_id, kind);
```

### Notes

- **`configs.body` and `runs.config_snapshot` are the same schema**
  (`ScenarioConfigBody`, `api-spec.md` §2). `body` evolves with edits;
  `config_snapshot` is immutable for reproducibility (NFR-1).
- Timeseries directories are **not** stored in `body` — the server injects
  `DATA_DIR/input/{resstock,comstock}/...` at run time (engine-reference
  Gap G-4).
- `summary` mirrors `EnsembleResult` minus the heavy Series
  (`percentile_envelopes` and profiles live in MinIO). Shape:
  ```json
  { "n_attempted": 180, "n_feasible": 150, "n_rejected": 30, "converged": true,
    "diversity_metrics": {…}, "convergence_history": {"peak_kw":[…],…},
    "validation_report": {"passed":true,"warnings":[],"checks":{…}},
    "representative_cases": {"median":{"run_index":73,"peak_kw":…},…} }
  ```
- No per-hour table. Envelope/profile arrays are Parquet artifacts
  (`series_parquet` kind). If SQL-side envelope queries ever become a hard
  requirement, revisit TimescaleDB (D5) with a new `run_series` hypertable
  migration — deliberately out of scope for v1.
- `run_metrics` is expected to reach ~`max_runs` rows per run (≤500);
  ~50k rows for 100 runs — trivial for Postgres, covered by NFR-3.

---

## 3. MinIO object layout

Bucket: `S3_BUCKET` (default `snlg-artifacts`). One prefix per run:

```
runs/{run_id}/
  csv/
    Run-1/…_Run-1_residential_merged_community_load_profile.csv
    Run-1/…_Run-1_comstock_merged_community_load_profile.csv
    Run-1/…_Run-1_total_merged_community_load_profile.csv
    Run-1/…_Run-1_building_selections.csv
    …_merged_community_load_profile_total_compiled-runs.csv
    …_load_profile_total.csv
    …_percentile_envelopes.csv
    …_ensemble_statistics.csv
    …_diversity_metrics.csv
    …_convergence_history.csv
    …_representative_median.csv   (+ high_peak, low_load_factor)
    …_representative_cases_summary.csv
  series/
    envelope.parquet            # index + p10/p50/p90 columns (chart source, FR-A11)
    total_by_run.parquet        # 8760 × n_feasible (compiled, columnar)
    representative_median.parquet  (+ high_peak, low_load_factor)
  figures/
    summary.png
    load_duration_median.png
  overview.txt
  full.log
```

- `object_key` in `artifacts` = the path under the bucket (e.g.
  `runs/1f2e…/series/envelope.parquet`).
- CSV filenames keep the **legacy** names/shapes from `outputs.py`
  (`preserve_legacy_format`, NFR-9) so downstream tooling and the notebook
  path stay compatible.
- Downloads are always via **presigned GET** (TTL `PRESIGN_TTL_SECONDS`,
  default 900); the API never proxies bytes.
- `DELETE /runs/{id}` issues a `list` + batch `delete` under
  `runs/{run_id}/` then deletes DB rows in one transaction (NFR-8).

---

## 4. Retention (OQ-1)

v1 default: **keep everything.** `DELETE /runs/{id}` is the only purge path.
A future `retention_days` setting + a scheduled sweep can be added without a
schema change (query `runs.finished_at < now() - interval`).

---

## 5. Reproducibility invariant (NFR-1 / A5)

For any `run` with `status='succeeded'`:
`run_scenario(run.config_snapshot, seeds=run.seeds)` on the same
`engine_version` must reproduce `run_metrics` rows exactly (int/bool equal,
float `==`). `api/tests/test_reproduce.py` re-executes a seeded run against
fixture data and diffs `run_metrics`.
