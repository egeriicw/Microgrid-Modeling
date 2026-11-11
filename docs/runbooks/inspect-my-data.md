# Runbook: inspect my data (one command, read only)

This runs one command on your Mac. It looks at your real ResStock/ComStock files and writes a short report you paste
back to the team. It answers: what is on disk, which release it looks like, whether the files match what the model
expects, and whether any buildings are incomplete. You do not need to know pandas.

**The tool never changes your data.** It only reads. It refuses to write its report inside your data folder, and it
refuses to overwrite an existing file. It does not download anything and does not delete anything.

## Prerequisites
- This repository checked out on your Mac, with `uv` installed (the project's Python tool). Run from the repo root.
- Your data folder (the one that contains `input/` and `background/`). Below it is written `/path/to/data`; yours may be
  `~/Microgrid-Modeling/data`.
- A place for the report **outside** the data folder, for example your home folder.

If you would rather not use `uv`, a plain virtual environment works too:
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python scripts/inspect_dataset.py --data-root /path/to/data --out ~/inspect-report.md
```

## The command (Mac)
```bash
cd Microgrid-Modeling
uv run --extra dev python scripts/inspect_dataset.py \
    --data-root /path/to/data \
    --out ~/inspect-report.md \
    --json ~/inspect-report.json
```
You can set `MICROGRID_DATA_DIR=/path/to/data` instead of passing `--data-root`. There is no default folder: the tool
never guesses where your data is.

Progress lines (`[inspect] ...`) appear in the terminal while it works. It takes seconds to a few minutes (it reads the
two characteristics files fully and only the metadata plus two columns of five sample parquet files per product).
It is designed to be light on memory (not measured on your real files). If the report file already exists, pick a new name or add `--overwrite-report`.

Options:
- `--sample-buildings 5` how many parquet files to sample per product (default 5, evenly spaced by sorted name).
- `--interesting-columns a,b,c` extra characteristics columns to summarise (a sensible default list is built in).
- `--hash` also builds a checksum manifest of every file (slow: it reads all ~11 GB; OFF by default). By default the
  manifest is written next to your report (`inspect-report.manifest.json`), **not** inside your data folder. If it
  already exists, it is verified instead of rebuilt. `--manifest PATH` chooses another location outside the data folder.
  `--manifest-in-root` writes `<data root>/manifest.json`, which is where the older `scripts/data_manifest.py` writes
  by default (that tool is unchanged); use it only if you want that file inside your data folder.

If something is wrong the tool prints one line starting `error:` that names the path, and stops with exit code 2. Paste
that line back.

## What the report means
1. **Layout.** The folders found, how many parquet files exist for each product, upgrade (`upgrade=N`, the scenario
   number; 0 is normally the baseline) and state (`state=XX`), their sizes, and free disk space. A **MISSING** list
   names anything absent, for example a characteristics file or a product with no timeseries.
2. **Characteristics files.** For ResStock and ComStock: how many buildings, all column names grouped by prefix
   (`in.` are building features, `out.` are results), and the values of `completed_status`, `applicability` and
   `upgrade`. This tells us whether any buildings are incomplete or not applicable; the loader setting
   `require_data_complete` would exclude those. The building-type list shows the exact spelling in your files; lines
   saying **NOT FOUND in data** mean our building-type registry uses a name your files do not have. The id check says
   whether every characteristics row has a parquet file and vice versa (extra characteristics rows are normal when the
   file covers more than the downloaded state).
3. **Parquet sample.** A few real timeseries files: number of rows, time step (15-minute or hourly), first and last
   timestamp, the year, and whether the exact total-electricity column the model needs exists (**PASS** or **FAIL**).
   It also shows the yearly sum and the peak as a sanity check and flags empty (NaN) or negative values.
4. **Release fingerprint.** Clues about which release your files came from (columns that only newer releases have,
   release or weather words in folder names or file metadata). This is **evidence, not proof**; the tool never claims a
   release, and neither should we until it is confirmed against NREL's documentation.
5. **Proposed catalog stanza.** A draft entry for `configs/datasets/catalog.yaml`, always `verified: false`, with
   `[verify]` notes. Nothing is marked verified until a person confirms it.
6. **Next steps.** A short list tailored to what was found, for example "N buildings have completed_status != Success".

## What to paste back
Paste the whole `~/inspect-report.md` into the conversation (it is plain text). If it is very long, paste sections 1, 2
(the status and building-type lines), 3 and 6 first. Attach `~/inspect-report.json` if asked. Do not edit the report.

## Optional: record the checksum in the catalog
After a `--hash` run, `scripts/catalog_fill.py` writes a NEW catalog file with the manifest hash and size filled in for
one entry (it never edits the original):
```bash
uv run python scripts/catalog_fill.py --catalog configs/datasets/catalog.yaml \
    --manifest ~/inspect-report.manifest.json \
    --key resstock/2025_release_1/amy2018/baseline/DC --out ~/catalog.filled.yaml
```
The key is `product/release/weather/scenario_key/state` as in the catalog. Review the new file before using it. Note
that the manifest covers the whole data root, so its hash identifies that whole snapshot; `size_bytes` counts only the
files under that entry's folder.

## Licence reminder
ResStock and ComStock data from NREL are, as far as we know, published under CC BY 4.0 with attribution to NREL/DOE
**[verify against the OEDI dataset pages]**. Keep the attribution when results are shown. Footprint (building outline)
data has its own licence **[verify which source and terms]**.
