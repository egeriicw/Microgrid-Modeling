# Test fixture data (FR-DATA-2): synthetic, generated, never committed

No real ResStock or ComStock file is committed to this repository. Tests use a **synthetic** dataset: made-up
numbers in the real on-disk layout and column names, so the engine runs with no network and no NREL data.

- Generator: `tests/fixtures/synthetic_data.py` (`build_synthetic_dataset(root, ...)`, deterministic for a given
  `seed`).
- Test access: the session-scoped pytest fixture `synthetic_dataset` (in `tests/conftest.py`). It builds the files once
  per test session into a pytest temp directory. Nothing is written into git; this directory holds only this README.
- Do not write into the shared session root. Tests that need a different mix of buildings call
  `build_synthetic_dataset(tmp_path, resstock_by_height=..., comstock_by_type=...)` with their own `tmp_path`.

## Layout produced

```
<root>/background/chars_resstock.xlsx   # sheet "building_characteristics"
<root>/background/chars_comstock.xlsx   # sheet "building_characteristics"
<root>/input/resstock/timeseries_individual_buildings/upgrade=0/state=DC/<bldg_id>-0.parquet
<root>/input/comstock/timeseries_individual_buildings/upgrade=0/state=DC/<bldg_id>-0.parquet
```

Parquet files hold one full year (2018) at 15-minute steps (35,040 rows) with a `timestamp` column, the total
electricity column (`out.electricity.total.energy_consumption..kwh` for ResStock, without the `..kwh` suffix for
ComStock) and four end-use columns (cooling, heating, lighting, plug loads). All values are strictly positive.
The characteristics sheets carry `bldg_id`, `completed_status`, `applicability`, `in.geometry_building_type_height`,
`in.geometry_building_number_units_mf`, `in.sqft..ft2`, `in.hvac_heating_type`, `in.geometry_garage` and a few more
filter columns (ComStock: `in.comstock_building_type`). Deliberately bad ResStock rows (`completed_status` not
`Success`, or `applicability` false) are included; their ids are in `SyntheticDataset.bad_resstock_ids`.

## Gotcha: 8+ story multifamily

The default fixture has **no** "Multi-Family with 5+ Units, 8+ Stories" buildings (defaults: single-family detached,
2-4 unit multifamily, 5+ units 4-7 stories). The baseline scenario YAML
(`configs/scenarios/dc_multifamily_baseline.yaml`) draws from 8+ story buildings too, so a test that runs it must pass
`resstock_by_height` including that type, for example
`{"Multi-Family with 2 - 4 Units": 4, "Multi-Family with 5+ Units, 8+ Stories": 3, ...}`. See
`tests/test_qa_1_6_review.py::test_engine_end_to_end_on_synthetic_fixture`.

Real datasets are provisioned on the owner's machine under the data root (git-ignored `data/`), never in git.
