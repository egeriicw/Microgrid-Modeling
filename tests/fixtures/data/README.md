# Committed fixture dataset (FR-E11 / NFR-12)

A **tiny** slice of ResStock / ComStock data so the engine integration
tests, the API worker path, and CI run with **no network and no OEDI
access**.

## Required layout

```
tests/fixtures/data/
  input/resstock/timeseries_individual_buildings/upgrade=0/state=DC/<bldg_id>-0.parquet   # ~3–5 files
  input/comstock/timeseries_individual_buildings/upgrade=0/state=DC/<bldg_id>-0.parquet   # ~1–2 files
  background/chars_resstock.xlsx   # sheet: building_characteristics — only the rows for the bldg_ids above
  background/chars_comstock.xlsx   # sheet: building_characteristics
```

## How to build it (Phase 1.6)

1. Pick ~5 ResStock + 2 ComStock `bldg_id`s that survive the
   `dc_multifamily_baseline.yaml` filters (multifamily + `SmallOffice`).
   The repo already has ~140 DC ComStock parquet files under
   `data/input/comstock/...` to sample from.
2. Copy those parquet files here (optionally downsample each to a few weeks
   to keep the tree < ~5 MB — the engine only needs a valid `timestamp`
   column + the total-electricity column; 8760 output length is produced by
   resampling whatever span is present, so **keep a full year** if any test
   asserts `len == 8760`).
3. Build the two `*.xlsx` characteristics files containing **only** the
   selected `bldg_id` rows with the columns `archetype.py` reads
   (`bldg_id`, `in.geometry_building_type_height`, `in.hvac_heating_type`,
   `in.sqft..ft2`, `in.geometry_garage`,
   `in.geometry_building_number_units_mf`, `in.comstock_building_type`).
4. Regenerate the determinism golden:
   `pytest tests/test_determinism.py --update-golden`.

Keep this directory small and in git. Real datasets are provisioned by
`scripts/download_oedi.py` into `data/input/**` (gitignored).
