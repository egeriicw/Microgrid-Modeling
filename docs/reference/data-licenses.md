# Data licenses and attribution (draft, mostly unverified)

Nothing here has been checked against the source pages from this environment (no network in story 1.6). Every row is
`[verify]` until the owner or a later agent confirms it from the OEDI dataset pages.

| Source | Believed license | Attribution to carry | Status |
|---|---|---|---|
| NREL ResStock 2025 Release 1 (OEDI) | Creative Commons Attribution 4.0 (CC BY 4.0) | Credit NREL / U.S. DOE and the release name; indicate modifications (we aggregate and resample) | `[verify]` |
| NREL ComStock 2025 Release 3 (OEDI) | CC BY 4.0 | Same as above | `[verify]` |
| Building footprints (DC Open Data / OSM / Microsoft / Overture) | Differs by source (OSM is ODbL with share-alike) | Decided by the footprint spike (story 1.12) | `[verify]` |

Implications to confirm: whether derived load profiles shown publicly need the attribution line in the UI and exports;
whether redistributing raw Parquet is allowed (CC BY 4.0 would allow it with attribution, but the plan is not to
redistribute raw files). The catalog (`configs/datasets/catalog.yaml`) carries a `license` string per dataset.
