# Microgrid Load Profiles

Generate community load profiles using ResStock (residential) and ComStock (commercial) individual-building parquet timeseries,
plus building characteristics workbooks.

Official outputs (written by default):
- CSVs (hourly, daily, monthly; per-run and averaged)
- TXT overview
- XLSX summary workbook
- Plots

Inputs live under `../data/input/`.
Outputs write to `../data/output/scenario_runs/<run_id>/`.

## Install (uv)
```bash
uv venv
source .venv/bin/activate
uv pip install -e .
```

## Run
```bash
microgrid-profiles --config config/example.yaml
```

## Weather
Enable weather via CLI:
```bash
microgrid-profiles --config config/example.yaml --enable-weather --weather-file /path/to/weather.csv
```

## Typical-day monthly profiles
Disable via CLI:
```bash
microgrid-profiles --config config/example.yaml --no-profiles
```
