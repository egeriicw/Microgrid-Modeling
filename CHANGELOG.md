# Changelog

Format: [Keep a Changelog](https://keepachangelog.com). Versions follow SemVer. Entries that change numeric output are
marked **[numbers change]**.

## [Unreleased]

### Added (Batch 2, 2026-11-11)
- Celery worker (`microgrid_api.worker`), run service layer, DB cancel flag, `/ready` Redis check, migration 0002.
- Attribute filters (eight operators, pool preview with warnings), exposed-attribute list, `filters.use` permission flag, dataset-id resolution (`resolve_dataset_paths`), `config_to_dict`.
- `scripts/inspect_dataset.py` (read-only report on real data), `scripts/catalog_fill.py`, `microgrid.datasets.pathsafety`.
- Lite Docker stack (`make lite-up/lite-down/lite-nuke`), `deploy/preflight.sh` fail-closed disk guard, `make disk-plan`.
- Chart spike (story 1.12): `web/spikes/echarts-35k/`, `docs/research/echarts-spike.md` (ECharts confirmed with settings; 27 vitest tests).
- Research notes: `docs/research/footprint-sources-dc.md`, `docs/research/hosting-options.md` (unverified).

### Changed (Batch 2)
- **`RunResult.config_snapshot` changed shape**: it is now the `config_from_dict` shape (so it can be replayed); the API only reads `composition.total_buildings`.
- Dataset ids and filter values are stricter (padded ids, NaN/inf bounds rejected). A lone dataset id that disagrees with `data.upgrade`/`data.state` is refused.
- `catalog_fill` records a content hash of the dataset's files (no mtimes). Old file-hash values are not comparable.
- `scripts/inspect_dataset.py --hash` now requires `--out`; a corrupt sampled parquet is a per-file ERROR note, not an abort.
- No entry in this batch changes numeric results (verified byte-identical).

### Fixed (Batch 2)
- Worker: terminal run states can no longer be overwritten by late writes; cancel no longer races the worker claim.
- Preflight disk guard failed open on leading-zero or huge values; now fails closed.
- Inspector could write a report inside the data folder via a differently capitalised path on macOS (identity check).

### Added (Wave A, 2026-11-11)
- `run_scenario` facade with typed `ProgressEvent`s, `RunCancelled`, `RunResult.to_dict()`, `config_from_dict`; observer hooks on `run_ensemble`. No numbers change.
- Golden determinism test, replay test and full-ensemble seed-robustness test running on a generated synthetic dataset.
- `microgrid.datasets` (dataset identity, catalog, safe manifest tooling, characteristics dictionary), `scripts/data_manifest.py`, synthetic fixture generator.
- `api/` service skeleton (`/api/v1/health`, `/ready` with db and schema checks, fail-loud settings, CORS, Alembic migrations packaged with `python -m microgrid_api.db.migrate`, SQLAlchemy spine models).
- Local Docker Compose stack definition (db with PostGIS+TimescaleDB, redis, migrate, api, worker placeholder, web placeholder), runbook, compose static checks in CI.

### Fixed (Wave A)
- `RunCancelled` can be pickled (needed by queue workers).
- Manifest writes could overwrite a data file through a symlinked temporary name; filesystem errors now exit 2 with a clear message.
- API settings no longer silently fall back to SQLite or print credentials embedded in connection URLs.

### Added
- **Building-type registry (FR-ENG-1).** Building types are now data (`community_load/building_types.yaml`):
  single-family detached and attached, multifamily small/mid/large, private commercial, community-scale warehouse,
  school and hospital. `civic` is listed but inactive until its ComStock mapping is decided. A type is added by adding
  an entry (`BuildingTypeRegistry.extended` in code), with no engine change; `with_default_filters` supplies sampling
  filters for categories the scenario file does not define.
- GIS: new use classes `residential_sfa` (townhouse, row house, semi-detached) and `warehouse`; `Structure.subtype`
  keeps the normalised raw label so schools, hospitals and civic buildings can be told apart. `warehouse` used to be
  lumped with `industrial`; factories and manufacturing stay unmodeled.
- **Strict configuration (FR-ENG-3).** Unknown keys at any level of a scenario YAML now raise a single error that lists
  every problem with its path and a "did you mean" hint. Enumerations, counts, fractions and percentiles are
  validated.

### Removed / rejected
- Keys that older configs carried but the engine never applied are now **rejected with a message**: `arealimit`,
  `garage`, `heatpump`, `electric_vehicle`, `solar_pv`. The shipped scenario YAMLs have them commented out; behavior
  is unchanged (the engine ignored them before).

### Fixed
- **[numbers may change] `require_data_complete` now works (owner decision 2026-11-07).** It was parsed but never applied.
  With the default `true`, only buildings with `completed_status == "Success"` and `applicability == true` are
  sampled. If your characteristics files contain only successful, applicable buildings, nothing changes. A missing
  column is an error, not a silent skip. Column meanings are to be confirmed against the release data dictionary.
- `scenario.name` / `description` given as a YAML list are now normalised to a string.

### Changed
- `categorize` and `scenario_for_neighborhood` take a registry instead of `CategoryRules`. Single-family structures are
  now modeled (previously reported as unmodeled).
- **[numbers change] RNG derivation (FR-ENG-4).** Per-run random streams are now built with
  `numpy.random.SeedSequence` as a pure function of (seed, layer, run index). The old scheme drew a value from the root
  generator and added the run index, which depended on call order and could collide across neighbouring seeds. The same
  fixed seeds now produce different (statistically equivalent) neighborhoods than before. Not a methodology change.
- Seeds are random by default: `rng.*_seed` may be omitted (None) and fresh values are drawn and recorded in
  `EnsembleResult.seeds`; supply the recorded seeds to replay a run exactly.
- Packaging: build backend added, runtime dependencies reduced, Python floor 3.12, single version source.
- CI: lint, format (ruff) and mypy now block; engine tests run on the installed package with coverage.
