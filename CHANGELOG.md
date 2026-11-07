# Changelog

Format: [Keep a Changelog](https://keepachangelog.com). Versions follow SemVer. Entries that change numeric output are
marked **[numbers change]**.

## [Unreleased]

### Changed
- **[numbers change] RNG derivation (FR-ENG-4).** Per-run random streams are now built with
  `numpy.random.SeedSequence` as a pure function of (seed, layer, run index). The old scheme drew a value from the root
  generator and added the run index, which depended on call order and could collide across neighbouring seeds. The same
  fixed seeds now produce different (statistically equivalent) neighborhoods than before. Not a methodology change.
- Seeds are random by default: `rng.*_seed` may be omitted (None) and fresh values are drawn and recorded in
  `EnsembleResult.seeds`; supply the recorded seeds to replay a run exactly.
- Packaging: build backend added, runtime dependencies reduced, Python floor 3.12, single version source.
- CI: lint, format (ruff) and mypy now block; engine tests run on the installed package with coverage.
