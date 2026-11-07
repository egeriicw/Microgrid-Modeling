# Changelog

Format: [Keep a Changelog](https://keepachangelog.com). Versions follow SemVer. Entries that change numeric output are
marked **[numbers change]**.

## [Unreleased]

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
