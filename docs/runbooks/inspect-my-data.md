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
- A place for the report **outside** the data folder: use your home folder (`~/`).
- Free disk space for the tool itself. The first `uv run --extra dev ...` downloads and installs Python packages
  before it does anything else (hundreds of MB), and your Mac has only roughly 6.7 GB free, so check that first. The
  report files are small.

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

Type the data folder path the same way every time, and put the report in your home folder (`~/`). On a Mac, folder
names are not case-sensitive, so `data` and `DATA` can be the same folder and a typo in capitals could, in principle,
slip a report into your data folder. The tool also checks this (it asks the operating system whether two paths are the
same folder, not just whether the text matches), but be safe and keep reports in `~/`.

Progress lines (`[inspect] ...`) appear in the terminal while it works. It takes seconds to a few minutes (it reads the
two characteristics files fully and only the metadata plus two columns of five sample parquet files per product).
In a QA measurement at a realistic size it used about 0.25 GB of memory and about 16 seconds. That was measured on a
cloud machine; your Mac may differ. If the report file already exists, pick a new name or add `--overwrite-report`.

If both a CSV and an XLSX characteristics file exist for a product, the XLSX is read (it is slower) and the CSV is
listed in the report as "not used".

Options:
- `--sample-buildings 5` how many parquet files to sample per product (default 5, evenly spaced by sorted name).
- `--interesting-columns a,b,c` extra characteristics columns to summarise (a sensible default list is built in).
- `--hash` also builds a checksum manifest of every file. It **requires `--out`** (the tool stops with an error
  otherwise). It reads all ~11 GB and can take a long time; it is OFF by default. The manifest is written next to your
  report (`inspect-report.manifest.json`), **not** inside your data folder. If it already exists, it is verified instead
  of rebuilt. `--manifest PATH` chooses another location outside the data folder.
- `--manifest-in-root` writes `<data root>/manifest.json`. It is the **only** way this tool writes anything inside your
  data folder, and most owners should not use it; the default (next to your report) is what you want.

If something is wrong with the whole run (for example the data folder does not exist, or a characteristics file is
corrupt) the tool prints one line starting `error:` that names the path, and stops with exit code 2. Paste that line
back.

If only a **sampled parquet file** is damaged (corrupt, cut short, or empty), the run does not stop. That file gets a
line `ERROR: <path inside the data folder>: <reason>`, the report starts with a bold warning "N sampled files could not
be read", and the exit code is 0. The Next steps list tells you to **copy** the file somewhere else to look at later;
never delete it (the tool never deletes anything, and neither should you without asking). The same errors are in the
JSON. A folder the tool is not allowed to list (for example `.Spotlight-V100` on a Mac) is listed as skipped with the
reason, and the scan continues.

**Privacy:** the report contains the full path of your data folder (which may include your username) and building
ids. Read it before you paste it anywhere and remove anything you do not want to share.

## What the report means
1. **Layout.** The folders found, how many parquet files exist for each product, upgrade (`upgrade=N`, the scenario
   number; 0 is normally the baseline) and state (`state=XX`), their sizes, and free disk space. A **MISSING** list
   names anything absent, for example a characteristics file or a product with no timeseries.
2. **Characteristics files.** For ResStock and ComStock: how many buildings, all column names grouped by prefix
   (`in.` are building features, `out.` are results), and the values of `completed_status`, `applicability` and
   `upgrade`. The line "not Success or not applicable" is exactly the number of buildings the model would drop with the
   loader setting `require_data_complete` on: a building is kept only if `completed_status` is `Success` and
   `applicability` is `true` (capitalisation and spaces are ignored; `yes`, `1`, `false`, empty and anything else count
   as not applicable). If one of those two columns is missing the report says "column missing: the engine will refuse
   to run with require_data_complete on" instead. The report also counts duplicate building ids (rows versus distinct
   ids) when a characteristics file lists the same `bldg_id` more than once. The building-type list shows the exact spelling in your files; lines
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
The key is `product/release/weather/scenario_key/state` as in the catalog. Review the new file before using it. The
`manifest_sha256` it records is a **content hash of that dataset's files only**: a hash over the sorted list of
`path, size, checksum` for the files under the entry's folder (no modification times, no other products), and
`size_bytes` is the total size of the same files. Copying the same data to another place gives the same hash; one
changed byte changes it. Values written by older versions of this tool hashed the whole manifest file and are not
comparable.

## Licence reminder
ResStock and ComStock data from NREL are, as far as we know, published under CC BY 4.0 with attribution to NREL/DOE
**[verify against the OEDI dataset pages]**. Keep the attribution when results are shown. Footprint (building outline)
data has its own licence **[verify which source and terms]**.
