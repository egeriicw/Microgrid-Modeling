# Changelog

Format: [Keep a Changelog](https://keepachangelog.com). Versions follow SemVer. Entries that change numeric output are
marked **[numbers change]**.

## [Unreleased]

### Added
- **Strict configuration (FR-ENG-3).** Unknown keys at any level of a scenario YAML now raise a single error that lists
  every problem with its path and a "did you mean" hint. Enumerations, counts, fractions and percentiles are
  validated.

### Removed / rejected
- Keys that older configs carried but the engine never applied are now **rejected with a message**: `arealimit`,
  `garage`, `heatpump`, `electric_vehicle`, `solar_pv`, `require_data_complete`. The shipped scenario YAMLs had them
  commented out; behavior is unchanged (the engine ignored them before). `ResidentialFilterConfig.require_data_complete`
  (a field with no effect) was removed.

### Fixed
- `scenario.name` / `description` given as a YAML list are now normalised to a string.

### Changed
- **[numbers change] RNG derivation (FR-ENG-4).** Per-run random streams are now built with
  `numpy.random.SeedSequence` as a pure function of (seed, layer, run index). The old scheme drew a value from the root
  generator and added the run index, which depended on call order and could collide across neighbouring seeds. The same
  fixed seeds now produce different (statistically equivalent) neighborhoods than before. Not a methodology change.
- Seeds are random by default: `rng.*_seed` may be omitted (None) and fresh values are drawn and recorded in
  `EnsembleResult.seeds`; supply the recorded seeds to replay a run exactly.
- Packaging: build backend added, runtime dependencies reduced, Python floor 3.12, single version source.
- CI: lint, format (ruff) and mypy now block; engine tests run on the installed package with coverage.
