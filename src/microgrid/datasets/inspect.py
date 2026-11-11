"""Read-only inspector for a real ResStock/ComStock data root (FR-DATA-1, FR-DATA-7, FR-DATA-8).

Answers the open data questions (what is on disk, what the characteristics files contain, whether the parquet files
match what the loader expects, which release the evidence points to) without the owner needing pandas.

Safety rules, enforced in code and tests:
  * the data root is only ever READ. Reports go to paths the owner names and are REFUSED inside the data root and
    refused if the file already exists (unless ``--overwrite-report``). "Inside" is decided by filesystem identity
    (``pathsafety.is_inside``: device and inode of the deepest existing ancestor), so symlinks, ``..``, upper/lower
    case and Unicode NFC/NFD spellings that the volume treats as the same folder (macOS) are all caught. Not
    guaranteed on network volumes without stable inode numbers; see ``pathsafety`` for the exact limits;
  * only metadata plus a few columns of a few sampled parquet files are read; the characteristics files are loaded
    whole (they are small relative to the timeseries);
  * the manifest (``--hash``) is OFF by default, written OUTSIDE the root unless ``--manifest-in-root`` is given;
  * every failure the owner can cause (missing path, corrupt characteristics file, symlink loop) is a one-line message
    naming the path and exit code 2, never a traceback. A corrupt SAMPLED parquet file is different: it becomes a
    per-file ``ERROR:`` note and the run continues (exit 0). Unreadable folders met while walking are listed as
    skipped, with the reason;
  * nothing is ever marked verified and no release is ever claimed: the fingerprint section is evidence only.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pyarrow.parquet as pq

from microgrid.datasets.catalog import CatalogError, resolve_data_root
from microgrid.datasets.dictionary import describe_characteristics
from microgrid.datasets.manifest import (
    MANIFEST_NAME,
    _human,
    build_manifest,
    read_manifest,
    sha256_file,
    verify_manifest,
    write_manifest,
)
from microgrid.datasets.pathsafety import is_inside

PRODUCTS = ("resstock", "comstock")
CHARS_SHEET = "building_characteristics"
EXPECTED_TOTAL_COLUMN = {
    "resstock": "out.electricity.total.energy_consumption..kwh",
    "comstock": "out.electricity.total.energy_consumption",
}
BUILDING_TYPE_COLUMN = {"resstock": "in.geometry_building_type_height", "comstock": "in.comstock_building_type"}
# Columns the engine reads from the characteristics files (loader, archetype filters, registry).
ENGINE_COLUMNS = {
    "resstock": (
        "bldg_id",
        "completed_status",
        "applicability",
        "in.geometry_building_type_height",
        "in.geometry_building_number_units_mf",
        "in.sqft..ft2",
        "in.hvac_heating_type",
        "in.geometry_garage",
    ),
    "comstock": ("bldg_id", "completed_status", "applicability", "in.comstock_building_type", "in.sqft..ft2"),
}
DEFAULT_INTERESTING = (
    "in.vintage",
    "in.heating_fuel",
    "in.geometry_stories",
    "in.number_stories",
    "in.county",
    "in.state",
    "in.weather_file_city",
    "in.has_ev",
)
STATUS_COLUMNS = ("completed_status", "applicability", "upgrade", "in.upgrade_name")
ID_COLUMNS = ("bldg_id", "building_id")
TIMESTAMP_NAMES = ("timestamp", "time", "datetime", "date_time")
_PARQUET_NAME = re.compile(r"^(\d+)-(\d+)\.parquet$")
_RELEASE_RE = re.compile(r"(?i)(?:\b20\d{2}[._ -]?release[._ -]?\d+\b|\brelease[._ -]?\d+\b)")
_WEATHER_RE = re.compile(r"(?i)\b(?:amy\d{4}|tmy\d*)\b")
_FEATURE_CHECKS: tuple[tuple[str, str], ...] = (
    ("ev columns", r"(?i)(^|[._])ev([._]|$)|electric_vehicle|charger"),
    ("electric panel columns", r"(?i)panel"),
    ("in.energystar_climate_zone_2023", r"^in\.energystar_climate_zone_2023$"),
)
DISCLAIMER = (
    "This section is evidence, not proof. A column or string being present (or absent) is consistent with some "
    "releases but does not identify one. No release is claimed; confirm against the data provider's documentation."
)

Progress = Callable[[str], None]


class InspectError(Exception):
    """A user-facing failure: the message names the path; the command line turns it into exit code 2."""


# ----------------------------------------------------------------------------------------------- layout


def _product_of(parts: Sequence[str]) -> str | None:
    low = [p.lower() for p in parts]
    if len(low) > 1 and low[0] == "input" and low[1] in PRODUCTS:
        return low[1]
    for p in low:
        if p in PRODUCTS:
            return p
    return None


def _walk(root: Path) -> tuple[dict[str, int], list[str], list[dict[str, str]]]:
    """(relative file path -> size, symlinked directories skipped, unreadable directories with reason).

    Directories are never followed. A directory that cannot be listed (permissions, for example macOS
    ``.Spotlight-V100``) is recorded and skipped; it does not abort the walk.
    """
    unreadable: list[dict[str, str]] = []

    def fail(err: OSError) -> None:
        where = Path(str(err.filename or root))
        try:
            rel = where.relative_to(root).as_posix()
        except ValueError:
            rel = str(where)
        unreadable.append({"path": rel, "reason": err.strerror or type(err).__name__})

    files: dict[str, int] = {}
    skipped: list[str] = []
    for dirpath, dirs, names in os.walk(root, followlinks=False, onerror=fail):
        for d in dirs:
            if (Path(dirpath) / d).is_symlink():
                skipped.append((Path(dirpath) / d).relative_to(root).as_posix())
        for name in names:
            full = Path(dirpath) / name
            try:
                size = full.stat().st_size  # follows a file symlink; loops and dangling links raise here
            except OSError as exc:
                raise InspectError(f"cannot read {full} (symlink loop or broken link?): {exc.strerror}") from exc
            files[full.relative_to(root).as_posix()] = size
    return files, sorted(skipped), sorted(unreadable, key=lambda d: d["path"])


def _build_layout(
    root: Path, files: dict[str, int], skipped: list[str], skipped_dirs: list[dict[str, str]]
) -> dict[str, Any]:
    folder_sizes: dict[str, int] = defaultdict(int)
    parts_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    chars: list[str] = []
    for rel, size in files.items():
        parts = rel.split("/")
        for depth in range(1, min(len(parts), 3)):
            folder_sizes["/".join(parts[:depth])] += size
        if len(parts) == 1:
            folder_sizes["."] += size
        if rel.endswith(".parquet"):
            up = next((p.split("=", 1)[1] for p in parts if p.startswith("upgrade=")), "unknown")
            st = next((p.split("=", 1)[1] for p in parts if p.startswith("state=")), "unknown")
            prod = _product_of(parts[:-1]) or "unknown"
            rec = parts_map.setdefault(
                (prod, up, st),
                {"product": prod, "upgrade": up, "state": st, "paths": {}, "parquet_files": 0, "bytes": 0},
            )
            pdir = "/".join(parts[:-1])
            rec["paths"][pdir] = rec["paths"].get(pdir, 0) + 1
            rec["parquet_files"] += 1
            rec["bytes"] += size
            folder_sizes[pdir] += size if len(parts) - 1 > 2 else 0
        elif parts[0] == "background" and rel.lower().endswith((".xlsx", ".csv")):
            chars.append(rel)
    partitions = []
    for key in sorted(parts_map):
        rec = parts_map[key]
        paths = sorted(rec["paths"])
        partitions.append(
            {
                "product": rec["product"],
                "upgrade": rec["upgrade"],
                "state": rec["state"],
                "relative_path": paths[0],
                "other_paths": paths[1:],
                "parquet_files": rec["parquet_files"],
                "bytes": rec["bytes"],
            }
        )
    missing: list[str] = []
    top = {p.split("/")[0] for p in files}
    for folder in ("input", "background"):
        if folder not in top:
            missing.append(f"{folder}/ folder not found under the data root")
    for prod in PRODUCTS:
        if not any(p["product"] == prod for p in partitions):
            missing.append(f"{prod}: no timeseries parquet files found (expected input/{prod}/.../upgrade=N/state=XX/)")
        if not any(_chars_product(r) == prod for r in chars):
            missing.append(f"{prod}: characteristics file (xlsx or csv) not found under background/")
    usage = shutil.disk_usage(root)
    return {
        "root": str(root),
        "total_files": len(files),
        "total_bytes": sum(files.values()),
        "disk_free_bytes": usage.free,
        "disk_total_bytes": usage.total,
        "top_level_dirs": sorted({p.split("/")[0] for p in files if "/" in p}),
        "input_dirs": sorted({r.split("/")[1] for r in files if r.startswith("input/") and r.count("/") > 1}),
        "background_dirs": sorted({r.split("/")[1] for r in files if r.startswith("background/") and r.count("/") > 1}),
        "folder_sizes": dict(sorted(folder_sizes.items())),
        "partitions": partitions,
        "characteristics_candidates": sorted(chars),
        "missing": missing,
        "skipped_symlinks": skipped,
        "skipped_dirs": skipped_dirs,
    }


def _chars_product(rel: str) -> str | None:
    name = rel.lower().rsplit("/", 1)[-1]
    hits = [p for p in PRODUCTS if p in name]
    return hits[0] if len(hits) == 1 else None


# ------------------------------------------------------------------------------- characteristics files


def _read_chars(path: Path, rel: str) -> tuple[pd.DataFrame, str | None, list[str]]:
    """(dataframe, sheet used, all sheet names). Any failure becomes an InspectError naming the file."""
    try:
        if rel.lower().endswith(".xlsx"):
            xl = pd.ExcelFile(path)
            sheets = [str(s) for s in xl.sheet_names]
            sheet = CHARS_SHEET if CHARS_SHEET in sheets else sheets[0]
            return pd.read_excel(xl, sheet_name=sheet), sheet, sheets
        return pd.read_csv(path, low_memory=False), None, []
    except Exception as exc:  # noqa: BLE001 - any parser failure must become a clean message naming the file
        raise InspectError(f"cannot read characteristics file {path}: {type(exc).__name__}: {exc}") from exc


def _classify_unnamed(path: Path, rel: str) -> str | None:
    try:
        head = pd.read_csv(path, nrows=0) if rel.lower().endswith(".csv") else pd.read_excel(path, nrows=0)
    except Exception:  # noqa: BLE001 - classification is best effort; the file is then simply not used
        return None
    cols = set(map(str, head.columns))
    if BUILDING_TYPE_COLUMN["comstock"] in cols:
        return "comstock"
    if BUILDING_TYPE_COLUMN["resstock"] in cols:
        return "resstock"
    return None


def _pick_chars(root: Path, candidates: list[str]) -> dict[str, str]:
    """Best file per product: xlsx with the 'building_characteristics' sheet, then csv, then any xlsx."""
    by_prod: dict[str, list[str]] = defaultdict(list)
    for rel in candidates:
        prod = _chars_product(rel) or _classify_unnamed(root / rel, rel)
        if prod:
            by_prod[prod].append(rel)

    def rank(rel: str) -> tuple[int, str]:
        if rel.lower().endswith(".csv"):
            return (1, rel)
        try:
            ok = CHARS_SHEET in pd.ExcelFile(root / rel).sheet_names
        except Exception:  # noqa: BLE001 - ranked last; reading it later raises the clean error
            ok = False
        return (0 if ok else 2, rel)

    return {prod: min(rels, key=rank) for prod, rels in by_prod.items()}


def _value_counts(s: pd.Series) -> dict[str, int]:
    vc = s.value_counts(dropna=False)
    return {("NaN" if pd.isna(cast(Any, k)) else str(k)): int(v) for k, v in vc.items()}


def _group_by_prefix(columns: Iterable[str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = defaultdict(list)
    for c in columns:
        groups[c.split(".", 1)[0] if "." in c else "(none)"].append(c)
    return dict(sorted(groups.items()))


STATUS_REQUIRED = ("completed_status", "applicability")


def _usable_mask(df: pd.DataFrame) -> pd.Series:
    """The engine's ``require_data_complete`` rule, restated (this layer may not import community_load).

    Mirrors ``community_load.synthetic.archetype._completed_and_applicable``: ``completed_status`` stripped and
    lower-cased equals "success" AND ``applicability`` stripped and lower-cased equals "true", after ``astype(str)``
    (so True, "TRUE" and " true " pass; "yes", "1", False, NaN and None do not). A parity test pins this to the engine.
    """
    ok = df["completed_status"].astype(str).str.strip().str.lower() == "success"
    return ok & (df["applicability"].astype(str).str.strip().str.lower() == "true")


def _inspect_chars(
    prod: str,
    rel: str,
    root: Path,
    parquet_ids: set[int],
    interesting: Sequence[str],
    expected: Sequence[str] | None,
    progress: Progress,
) -> tuple[dict[str, Any], pd.DataFrame]:
    progress(f"reading {prod} characteristics file {rel} (loads the whole file; can take a minute)")
    df, sheet, sheets = _read_chars(root / rel, rel)
    df.columns = [str(c) for c in df.columns]
    cols = list(df.columns)
    out: dict[str, Any] = {
        "source_file": rel,
        "sheet": sheet,
        "sheet_names": sheets,
        "rows": int(len(df)),
        "n_columns": len(cols),
        "columns": cols,
        "columns_by_prefix": _group_by_prefix(cols),
    }
    wanted = list(ENGINE_COLUMNS[prod])
    out["engine_columns_missing"] = [c for c in wanted if c not in df.columns]
    extra = [c for c in interesting if c not in wanted]
    out["interesting_missing"] = [c for c in extra if c not in df.columns]
    use = [c for c in wanted + extra if c in df.columns]
    out["described"] = describe_characteristics(df[use]) if use else []
    out["value_counts"] = {c: _value_counts(df[c]) for c in STATUS_COLUMNS if c in df.columns}
    out["status_columns_missing"] = [c for c in STATUS_REQUIRED if c not in df.columns]
    # None (not 0) when a column is absent: the engine raises ValueError there, so no count exists.
    out["not_usable_rows"] = None if out["status_columns_missing"] else int((~_usable_mask(df)).sum())
    bcol = BUILDING_TYPE_COLUMN[prod]
    if bcol in df.columns:
        counts = _value_counts(df[bcol])
        bt: dict[str, Any] = {"column": bcol, "counts": counts}
        if expected is not None:
            bt["expected_not_found"] = sorted(set(expected) - set(counts))
            bt["found_not_expected"] = sorted(set(counts) - set(expected) - {"NaN"})
        out["building_types"] = bt
    else:
        out["building_types"] = {"column": bcol, "counts": None, "missing_column": True}
    idcol = next((c for c in ID_COLUMNS if c in df.columns), None)
    if idcol is None:
        out["id_match"] = {"available": False, "reason": f"no id column ({' / '.join(ID_COLUMNS)}) in the file"}
    else:
        id_series = pd.to_numeric(df[idcol], errors="coerce").dropna()
        ids = {int(i) for i in id_series}
        only_chars, only_pq = sorted(ids - parquet_ids), sorted(parquet_ids - ids)
        out["id_match"] = {
            "available": True,
            "id_column": idcol,
            "chars_ids": len(ids),
            "rows_with_id": int(len(id_series)),
            "duplicate_rows": int(len(id_series)) - len(ids),
            "parquet_ids": len(parquet_ids),
            "matched": len(ids & parquet_ids),
            "in_chars_not_parquet": len(only_chars),
            "in_parquet_not_chars": len(only_pq),
            "examples_in_chars_not_parquet": only_chars[:5],
            "examples_in_parquet_not_chars": only_pq[:5],
        }
    return out, df


# ----------------------------------------------------------------------------------------- parquet sample


def _pick_evenly(items: list[str], n: int) -> list[str]:
    if n <= 0:
        return []
    if len(items) <= n:
        return list(items)
    if n == 1:
        return [items[0]]
    idx = sorted({round(i * (len(items) - 1) / (n - 1)) for i in range(n)})
    return [items[i] for i in idx]


class SampleError(Exception):
    """A sampled parquet file could not be read; the message is ``<ExceptionClass>: <short message>``."""


def _short_reason(exc: BaseException, root: Path) -> str:
    msg = " ".join(str(exc).split())
    for needle in {str(root), os.path.realpath(root)}:
        msg = msg.replace(needle + os.sep, "").replace(needle, "<data root>")
    return f"{type(exc).__name__}: {msg[:200]}" if msg else type(exc).__name__


def _sample_one(path: Path, rel: str, prod: str, root: Path) -> dict[str, Any]:
    try:
        pf = pq.ParquetFile(path)
        names = [str(n) for n in pf.schema_arrow.names]
        schema = pf.schema_arrow
        ts_col = next((n for n in names if n.lower() in TIMESTAMP_NAMES), None)
        if ts_col is None:
            import pyarrow as pa

            ts_col = next((f.name for f in schema if pa.types.is_timestamp(f.type)), None)
        expected = EXPECTED_TOTAL_COLUMN[prod]
        has_total = expected in names
        want = [c for c in (ts_col, expected if has_total else None) if c]
        table = pf.read(columns=want) if want else None
        meta = {
            (k.decode(errors="replace") if isinstance(k, bytes) else str(k)): (
                v.decode(errors="replace") if isinstance(v, bytes) else str(v)
            )[:200]
            for k, v in (pf.metadata.metadata or {}).items()
            if k not in (b"pandas", b"ARROW:schema")
        }
        rows = int(pf.metadata.num_rows)
    except Exception as exc:  # noqa: BLE001 - corrupt or unreadable parquet must be a clean message naming the file
        raise SampleError(_short_reason(exc, root)) from exc

    candidates = [n for n in names if n.startswith("out.electricity.total")]
    sample: dict[str, Any] = {
        "file": rel,
        "rows": rows,
        "n_columns": len(names),
        "columns": names,
        "total_column": {
            "expected": expected,
            "status": "PASS" if has_total else "FAIL",
            "candidates": candidates,
        },
        "file_metadata": meta,
        "warnings": [],
        "time_step": None,
        "first_timestamp": None,
        "last_timestamp": None,
        "years": [],
        "sum_kwh": None,
        "peak_kw": None,
        "nan_count": None,
        "negative_count": None,
    }
    warn: list[str] = sample["warnings"]
    step_s: float | None = None
    if table is None or ts_col is None:
        warn.append("no timestamp column found; time step, first/last timestamp and year not determined")
    else:
        ts = pd.to_datetime(table.column(ts_col).to_pandas())
        if len(ts):
            sample["first_timestamp"], sample["last_timestamp"] = str(ts.iloc[0]), str(ts.iloc[-1])
            sample["years"] = sorted({int(y) for y in ts.dt.year.unique()})
        if len(ts) >= 2:
            step_s = float(ts.diff().dropna().median().total_seconds())
            sample["time_step"] = (
                "15-minute" if step_s == 900 else "hourly" if step_s == 3600 else f"{step_s:g}-second step"
            )
            if sample["time_step"] not in ("15-minute", "hourly"):
                warn.append(f"unusual time step {step_s:g} seconds")
    if has_total and table is not None:
        vals = pd.to_numeric(table.column(expected).to_pandas(), errors="coerce")
        sample["nan_count"] = int(vals.isna().sum())
        sample["negative_count"] = int((vals < 0).sum())
        sample["sum_kwh"] = float(vals.sum())
        peak = vals.max()
        if step_s and pd.notna(peak):
            sample["peak_kw"] = float(peak) * 3600.0 / step_s
        if sample["nan_count"]:
            warn.append(f"{sample['nan_count']} NaN value(s) in {expected}")
        if sample["negative_count"]:
            warn.append(f"{sample['negative_count']} negative value(s) in {expected}")
    else:
        warn.append(f"expected total-electricity column {expected!r} not found")
    return sample


# ------------------------------------------------------------------------------------------ fingerprint


def _build_fingerprint(
    files: dict[str, int],
    chars: Mapping[str, dict[str, Any] | None],
    samples: Mapping[str, dict[str, Any]],
    chars_df: Mapping[str, pd.DataFrame],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    for prod in PRODUCTS:
        cols: set[str] = set()
        sources: list[str] = []
        if chars.get(prod):
            cols |= set(chars[prod]["columns"])  # type: ignore[index]
            sources.append("characteristics columns")
        if samples.get(prod, {}).get("samples"):
            for s in samples[prod]["samples"]:
                cols |= set(s["columns"])
            sources.append("sampled parquet columns")
        for label, pattern in _FEATURE_CHECKS:
            name = f"{label} ({prod})"
            if not sources:
                checks.append(
                    {"check": name, "found": None, "detail": "no characteristics or parquet read for this product"}
                )
                continue
            hits = sorted(c for c in cols if re.search(pattern, c))
            checks.append({"check": name, "found": bool(hits), "detail": hits[:10], "searched": sources})
    texts: dict[str, str] = {}  # evidence text -> where it came from
    for rel in files:
        for seg in rel.split("/"):
            texts.setdefault(seg, "path")
    for prod, c in chars.items():
        for sheet in (c or {}).get("sheet_names", []):
            texts.setdefault(sheet, f"sheet name in {prod} characteristics")
    for ps in samples.values():
        for s in ps.get("samples", []):
            for k, v in s["file_metadata"].items():
                texts.setdefault(f"{k}={v}", f"parquet file metadata ({s['file']})")
    release_strings = sorted({(m, w) for t, w in texts.items() for m in _RELEASE_RE.findall(t)})
    weather_strings = sorted({(m, w) for t, w in texts.items() for m in _WEATHER_RE.findall(t)})
    weather_cols: dict[str, list[str]] = {}
    for prod, df in chars_df.items():
        for col in df.columns:
            if "weather" in col.lower():
                top = df[col].astype(str).value_counts().head(3)
                weather_cols[f"{prod}:{col}"] = [f"{k} ({v})" for k, v in top.items()]
    return {
        "disclaimer": DISCLAIMER,
        "checks": checks,
        "release_like_strings": [{"text": t, "where": w} for t, w in release_strings[:20]],
        "weather_strings": [{"text": t, "where": w} for t, w in weather_strings[:20]],
        "weather_columns": weather_cols,
    }


# ---------------------------------------------------------------------------------------------- stanza

_LICENSE = {
    "resstock": "NREL ResStock on OEDI, CC BY 4.0, attribute NREL/DOE [verify]",
    "comstock": "NREL ComStock on OEDI, CC BY 4.0, attribute NREL/DOE [verify]",
}


def _build_stanza(layout: dict[str, Any]) -> str:
    out: list[str] = []
    for p in layout["partitions"]:
        if p["product"] not in PRODUCTS:
            continue
        rel = p["relative_path"]
        rel_found = sorted({m for m in _RELEASE_RE.findall(rel)})
        weather_found = sorted({m.lower() for m in _WEATHER_RE.findall(rel)})
        release = rel_found[0] if len(rel_found) == 1 else "unknown"
        weather = weather_found[0] if len(weather_found) == 1 else "unknown"
        up = p["upgrade"]
        key = "baseline" if up == "0" else "upgrade_" + re.sub(r"[^A-Za-z0-9_]", "_", up)
        note = f"Draft from inspect_dataset: {p['parquet_files']} parquet files found; nothing here is verified." + (
            f" Also found at {p['other_paths']}." if p["other_paths"] else ""
        )
        out += [
            f"  - product: {p['product']}",
            f"    release: {release}  # [verify] release/folder name not confirmed",
            f"    weather: {weather}  # [verify] weather basis not confirmed",
            f"    state: {p['state']}",
            f"    scenario_key: {key}",
            f"    partition_id: {up}  # [verify] meaning of this upgrade partition in this release",
            f"    name: {'Baseline' if up == '0' else 'Upgrade ' + up}  # [verify]",
            f"    scenario_family: {'Baseline' if up == '0' else 'Unknown'}  # [verify]",
            "    load_basis: gross  # [verify]",
            f'    license: "{_LICENSE[p["product"]]}"',
            f"    relative_path: {rel}",
            "    verified: false",
            f'    notes: "{note}"',
        ]
    return "\n".join(out) + ("\n" if out else "")


# ------------------------------------------------------------------------------------------ next steps


def _next_steps(rep: dict[str, Any]) -> list[str]:
    steps: list[str] = []
    for m in rep["layout"]["missing"]:
        steps.append(f"Missing: {m}. Locate or download it (or tell us it is intentionally absent).")
    for prod in PRODUCTS:
        c = rep["characteristics"].get(prod)
        if c is None:
            steps.append(f"No {prod} characteristics file was read, so {prod} building types and ids are unchecked.")
            continue
        n, rows = c["not_usable_rows"], c["rows"]
        if n is None:
            steps.append(
                f"{prod}: {c['status_columns_missing']}: column missing: the engine will refuse to run with "
                "require_data_complete on (turn that setting off or supply the column)."
            )
        elif n:
            steps.append(
                f"{n} buildings in the {prod} characteristics file (of {rows}) are not Success or not applicable "
                "(applicable means the text 'true'); the require_data_complete setting would exclude exactly these. "
                "Decide if that is wanted."
            )
        else:
            steps.append(f"All {rows} {prod} rows are Success and applicable: require_data_complete excludes nothing.")
        if c["engine_columns_missing"]:
            steps.append(
                f"{prod}: engine columns missing from the characteristics file: {c['engine_columns_missing']}."
            )
        bt = c["building_types"]
        if bt.get("missing_column"):
            steps.append(
                f"{prod}: column {bt['column']} is absent, so the building-type registry cannot match anything."
            )
        if bt.get("expected_not_found"):
            steps.append(
                f"{prod}: registry names NOT FOUND in the data: {bt['expected_not_found']}. "
                "Send this to the lead; the registry names may need to follow the real spelling."
            )
        if bt.get("found_not_expected"):
            steps.append(f"{prod}: values in the data that no registry entry lists: {bt['found_not_expected']}.")
        idm = c["id_match"]
        if idm.get("available") and idm["duplicate_rows"]:
            steps.append(
                f"{prod}: {idm['duplicate_rows']} duplicate {idm['id_column']} rows in the characteristics file "
                f"({idm['rows_with_id']} rows, {idm['chars_ids']} distinct ids). Ask which row is right before use."
            )
        if idm.get("available") and (idm["in_chars_not_parquet"] or idm["in_parquet_not_chars"]):
            steps.append(
                f"{prod}: {idm['in_chars_not_parquet']} ids are in the characteristics file but have no parquet file "
                f"(normal if the file covers more than the downloaded state); {idm['in_parquet_not_chars']} parquet "
                "files have no characteristics row (these would be unusable)."
            )
        ps = rep["parquet_sample"].get(prod)
        for e in (ps or {}).get("errors", []):
            steps.append(
                f"{prod}: sampled file {e['file']} could not be read ({e['reason']}). Copy it aside to look at later "
                "(never delete it; it may be a damaged download) and re-download or compare with the original."
            )
        for s in (ps or {}).get("samples", []):
            if s["total_column"]["status"] == "FAIL":
                steps.append(
                    f"{prod}: total-electricity column FAIL in {s['file']}; columns that look similar: "
                    f"{s['total_column']['candidates']}. The loader needs an exact name."
                )
                break
        flagged = [s for s in (ps or {}).get("samples", []) if s["warnings"]]
        if flagged:
            steps.append(f"{prod}: warnings in sampled files (NaN, negatives, time step): {flagged[0]['warnings']}.")
        years = sorted({y for s in (ps or {}).get("samples", []) for y in s["years"]})
        if years and years != [2018]:
            steps.append(f"{prod}: sampled timestamps cover year(s) {years}; the catalog assumes AMY2018. Confirm.")
        steps_t = {s["time_step"] for s in (ps or {}).get("samples", [])}
        if steps_t - {"15-minute", None}:
            steps.append(f"{prod}: time step(s) {sorted(str(t) for t in steps_t)}; the engine expects 15-minute data.")
    steps.append("Paste this whole report (or the .json) back to the team. Nothing in it is marked verified.")
    steps.append("When ready, re-run with --hash to build a manifest, then use scripts/catalog_fill.py to record it.")
    return steps


# ------------------------------------------------------------------------------------------------ driver


def inspect_dataset(
    root: Path,
    *,
    sample_buildings: int = 5,
    interesting_columns: Sequence[str] = DEFAULT_INTERESTING,
    expected_types: Mapping[str, Sequence[str]] | None = None,
    progress: Progress | None = None,
) -> dict[str, Any]:
    """Inspect ``root`` (read-only) and return the report as a JSON-serialisable dict."""
    say: Progress = progress or (lambda _m: None)
    root = Path(root)
    if not root.is_dir():
        raise InspectError(f"data root {root} does not exist or is not a directory")
    say(f"scanning {root}")
    files, skipped, unreadable = _walk(root)
    if not files:
        why = f" ({unreadable[0]['path']}: {unreadable[0]['reason']})" if unreadable else ""
        raise InspectError(f"data root {root} contains no readable files; nothing to inspect{why}")
    layout = _build_layout(root, files, skipped, unreadable)
    say(f"found {layout['total_files']} files, {_human(layout['total_bytes'])}")

    parquet_ids: dict[str, set[int]] = defaultdict(set)
    parquet_by_prod: dict[str, list[str]] = defaultdict(list)
    for rel in files:
        if rel.endswith(".parquet"):
            prod = _product_of(rel.split("/")[:-1])
            if prod:
                parquet_by_prod[prod].append(rel)
                m = _PARQUET_NAME.match(rel.rsplit("/", 1)[-1])
                if m:
                    parquet_ids[prod].add(int(m.group(1)))

    picked = _pick_chars(root, layout["characteristics_candidates"])
    chars: dict[str, dict[str, Any] | None] = {}
    chars_df: dict[str, pd.DataFrame] = {}
    for prod in PRODUCTS:
        if prod not in picked:
            chars[prod] = None
            continue
        exp = None if expected_types is None else expected_types.get(prod)
        chars[prod], chars_df[prod] = _inspect_chars(
            prod, picked[prod], root, parquet_ids[prod], interesting_columns, exp, say
        )
        others = [r for r in layout["characteristics_candidates"] if r != picked[prod] and _chars_product(r) == prod]
        chars[prod]["other_candidates_not_used"] = others  # type: ignore[index]

    sample: dict[str, dict[str, Any]] = {}
    for prod in PRODUCTS:
        names = sorted(parquet_by_prod.get(prod, []))
        chosen = _pick_evenly(names, sample_buildings)
        samples = []
        errors: list[dict[str, str]] = []
        for i, rel in enumerate(chosen, start=1):
            say(f"sampling {prod} parquet {i}/{len(chosen)}: {rel}")
            try:
                samples.append(_sample_one(root / rel, rel, prod, root))
            except SampleError as exc:
                errors.append({"file": rel, "reason": str(exc), "error": f"ERROR: {rel}: {exc}"})
                say(f"ERROR reading {rel}: {exc}")
        sample[prod] = {"total_files": len(names), "files": chosen, "samples": samples, "errors": errors}

    rep: dict[str, Any] = {
        "tool": "inspect_dataset",
        "data_root": str(root),
        "layout": layout,
        "characteristics": chars,
        "parquet_sample": sample,
        "sampled_files_unreadable": sum(len(v["errors"]) for v in sample.values()),
        "fingerprint": _build_fingerprint(files, chars, sample, chars_df),
        "catalog_stanza": _build_stanza(layout),
    }
    rep["next_steps"] = _next_steps(rep)
    return rep


# ---------------------------------------------------------------------------------------------- markdown


def _table(header: Sequence[str], rows: Sequence[Sequence[Any]]) -> list[str]:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c).replace("|", "\\|") for c in r) + " |" for r in rows]
    return out + [""]


def _counts_line(d: Mapping[str, int]) -> str:
    return ", ".join(f"{k}: {v}" for k, v in d.items())


def render_markdown(rep: Mapping[str, Any]) -> str:
    L: list[str] = [
        "# Dataset inspection report",
        "",
        f"Data root: `{rep['data_root']}` (read only; nothing was changed)",
        "",
    ]
    if rep["sampled_files_unreadable"]:
        L += [
            f"**WARNING: {rep['sampled_files_unreadable']} sampled files could not be read** "
            "(see the ERROR lines in section 3 and the next steps; the rest of the report is still valid).",
            "",
        ]
    lay = rep["layout"]
    L += ["## 1. Layout", ""]
    L.append(
        f"{lay['total_files']} files, {_human(lay['total_bytes'])} under the root; "
        f"{_human(lay['disk_free_bytes'])} free on this volume (of {_human(lay['disk_total_bytes'])})."
    )
    L += [
        "",
        f"Folders under input/: {lay['input_dirs'] or 'none'}; under background/: {lay['background_dirs'] or 'none'}",
        "",
    ]
    L += _table(["folder", "size"], [(k, _human(v)) for k, v in lay["folder_sizes"].items()])
    L += _table(
        ["product", "upgrade", "state", "parquet files", "size", "path"],
        [
            (p["product"], p["upgrade"], p["state"], p["parquet_files"], _human(p["bytes"]), p["relative_path"])
            for p in lay["partitions"]
        ],
    )
    L.append(
        "Characteristics candidates under background/: " + (", ".join(lay["characteristics_candidates"]) or "none")
    )
    L.append("")
    for d in lay["skipped_dirs"]:
        L.append(f"Folder skipped, could not be read: `{d['path']}` ({d['reason']})")
    if lay["skipped_dirs"]:
        L.append("")
    if lay["skipped_symlinks"]:
        L.append(f"Symlinked folders skipped (not followed): {lay['skipped_symlinks']}")
        L.append("")
    L.append("**MISSING:**" if lay["missing"] else "Nothing missing.")
    L += [f"- {m}" for m in lay["missing"]] + [""]

    L += ["## 2. Characteristics files", ""]
    for prod in PRODUCTS:
        c = rep["characteristics"][prod]
        L += [f"### {prod}", ""]
        if c is None:
            L += ["No characteristics file found/read.", ""]
            continue
        L.append(
            f"File `{c['source_file']}`"
            + (f", sheet `{c['sheet']}`" if c["sheet"] else "")
            + f": {c['rows']} rows, {c['n_columns']} columns."
        )
        if c["sheet"] and c["sheet"] != CHARS_SHEET:
            L.append(f"NOTE: no sheet named {CHARS_SHEET!r}; used the first sheet. Sheets: {c['sheet_names']}")
        if c.get("other_candidates_not_used"):
            L.append(f"Other candidate files NOT used: {c['other_candidates_not_used']}")
        L += ["", "Columns by prefix:"]
        for pre, names in c["columns_by_prefix"].items():
            L.append(f"- `{pre}` ({len(names)}): " + ", ".join(f"`{n}`" for n in names))
        L.append("")
        if c["engine_columns_missing"]:
            L.append(f"**Engine columns MISSING:** {c['engine_columns_missing']}")
        if c["interesting_missing"]:
            L.append(f"Requested interesting columns not present: {c['interesting_missing']}")
        L += ["", "Column summary (engine columns plus interesting ones):", ""]
        rows = []
        for d in c["described"]:
            rng = f"{d['min']} .. {d['max']}" if d["kind"] == "numeric" else ", ".join(d.get("values", [])[:8])
            rows.append((d["name"], d["kind"], d["n_distinct"], d["n_missing"], rng))
        L += _table(["column", "kind", "distinct", "missing", "range / top values"], rows)
        L.append("Status columns (what would `require_data_complete` exclude?):")
        for name, vc in c["value_counts"].items():
            L.append(f"- `{name}`: {_counts_line(vc)}")
        if c["not_usable_rows"] is None:
            L.append(
                f"- {c['status_columns_missing']}: column missing: the engine will refuse to run with "
                "require_data_complete on"
            )
        else:
            L.append(
                f"- rows that are not Success or not applicable: **{c['not_usable_rows']}** of {c['rows']} "
                "(exactly what require_data_complete would drop; applicable means the text 'true')"
            )
        L.append("")
        bt = c["building_types"]
        L.append(f"Building types (`{bt['column']}`):")
        if bt.get("counts") is None:
            L.append("- COLUMN NOT PRESENT")
        else:
            L += [f"- {k}: {v}" for k, v in bt["counts"].items()]
            if "expected_not_found" in bt:
                for n in bt["expected_not_found"]:
                    L.append(f"- registry name `{n}`: **NOT FOUND in data**")
                for n in bt["found_not_expected"]:
                    L.append(f"- data value `{n}` is not listed by any registry entry")
                if not (bt["expected_not_found"] or bt["found_not_expected"]):
                    L.append("- registry names and data values agree")
        L.append("")
        idm = c["id_match"]
        if idm["available"]:
            L.append(
                f"Ids: {idm['chars_ids']} in characteristics, {idm['parquet_ids']} parquet files, "
                f"{idm['matched']} matched; "
                f"{idm['in_chars_not_parquet']} in characteristics only {idm['examples_in_chars_not_parquet']}, "
                f"{idm['in_parquet_not_chars']} parquet-only {idm['examples_in_parquet_not_chars']}."
            )
            if idm["duplicate_rows"]:
                L.append(
                    f"**Duplicate {idm['id_column']} rows: {idm['duplicate_rows']}** "
                    f"({idm['rows_with_id']} rows with an id, {idm['chars_ids']} distinct ids)."
                )
        else:
            L.append(f"Id check not possible: {idm['reason']}.")
        L.append("")

    L += ["## 3. Parquet sample", ""]
    for prod in PRODUCTS:
        ps = rep["parquet_sample"][prod]
        L += [
            f"### {prod}",
            "",
            f"{ps['total_files']} files found; sampled {len(ps['samples'])} (evenly spaced by sorted name).",
            "",
        ]
        for e in ps["errors"]:
            L.append(f"- {e['error']}")
        if ps["errors"]:
            L.append("")
        if not ps["samples"]:
            L += ["(none)", ""]
            continue
        L += _table(
            ["file", "rows", "step", "first", "last", "years", "sum kWh", "peak kW", "NaN", "neg", "total column"],
            [
                (
                    s["file"].rsplit("/", 1)[-1],
                    s["rows"],
                    s["time_step"],
                    s["first_timestamp"],
                    s["last_timestamp"],
                    s["years"],
                    None if s["sum_kwh"] is None else f"{s['sum_kwh']:.1f}",
                    None if s["peak_kw"] is None else f"{s['peak_kw']:.2f}",
                    s["nan_count"],
                    s["negative_count"],
                    s["total_column"]["status"],
                )
                for s in ps["samples"]
            ],
        )
        for s in ps["samples"]:
            tc = s["total_column"]
            L.append(
                f"- {tc['status']}: `{tc['expected']}` "
                + ("found" if tc["status"] == "PASS" else f"NOT found; similar: {tc['candidates']}")
                + f" in {s['file']}"
            )
            for w in s["warnings"]:
                L.append(f"  - WARNING: {w}")
        first = ps["samples"][0]
        L += [
            "",
            f"Columns in the first sampled file ({first['n_columns']}): "
            + ", ".join(f"`{n}`" for n in first["columns"]),
            "",
        ]

    fp = rep["fingerprint"]
    L += ["## 4. Release fingerprint", "", f"**{fp['disclaimer']}**", ""]
    L += _table(
        ["check", "present?", "matching columns"],
        [(c["check"], {True: "yes", False: "no", None: "not checked"}[c["found"]], c["detail"]) for c in fp["checks"]],
    )
    L.append(
        "Release-like strings found: "
        + (", ".join(f"`{x['text']}` ({x['where']})" for x in fp["release_like_strings"]) or "none")
    )
    L.append(
        "Weather strings (AMY/TMY) found: "
        + (", ".join(f"`{x['text']}` ({x['where']})" for x in fp["weather_strings"]) or "none")
    )
    for k, v in fp["weather_columns"].items():
        L.append(f"- weather column `{k}`: {v}")
    L.append("")
    if "hash" in rep:
        h = rep["hash"]
        L += ["## Manifest (--hash)", ""]
        L.append(
            f"Manifest `{h['manifest_path']}` {h['mode']}; {h['files']} files; "
            f"manifest sha256 `{h['manifest_sha256']}`."
        )
        if h["mode"] == "verified":
            L.append(f"Verification: missing {h['missing']}, modified {h['modified']}, not in manifest {h['extra']}.")
        L.append("")
    L += [
        "## 5. Proposed catalog stanza",
        "",
        "Draft only; `verified: false`; every `[verify]` needs a human check.",
        "",
        "```yaml",
    ]
    L += [rep["catalog_stanza"].rstrip("\n") or "# no partitions found", "```", ""]
    L += ["## 6. Next steps", ""] + [f"{i}. {s}" for i, s in enumerate(rep["next_steps"], start=1)] + [""]
    return "\n".join(L)


# ------------------------------------------------------------------------------------------------- CLI


def _inside(root: Path, path: Path) -> bool:
    """True if ``path`` is the data root or below it, by filesystem identity (see ``pathsafety``)."""
    try:
        return is_inside(root, Path(path))
    except (OSError, RuntimeError) as exc:  # unreadable ancestor or a symlink loop in the output path
        raise InspectError(f"cannot check where {path} is: {exc}") from exc


def _check_output(root: Path, path: Path | None, label: str, overwrite: bool) -> None:
    if path is None:
        return
    if _inside(root, path):
        raise InspectError(f"{label} {path} is inside the data root {root}; refusing (choose a path outside it)")
    if not path.parent.is_dir():
        raise InspectError(f"{label} {path}: folder {path.parent} does not exist")
    if (path.exists() or path.is_symlink()) and not overwrite:
        raise InspectError(f"{label} {path} already exists; choose a new name or pass --overwrite-report")


def _write_text(path: Path, text: str, overwrite: bool) -> None:
    try:
        with open(path, "w" if overwrite else "x") as fh:
            fh.write(text)
    except OSError as exc:
        raise InspectError(f"cannot write {path}: {exc.strerror}") from exc


def _do_hash(root: Path, target: Path, in_root: bool, say: Progress) -> dict[str, Any]:
    def prog(i: int, total: int, rel: str) -> None:
        if i == total or i % 25 == 0 or total <= 25:
            say(f"hashing [{i}/{total}] {rel}")

    try:
        if target.exists():
            m = read_manifest(target)
            say(f"verifying existing manifest {target}")
            rep = verify_manifest(root, m, progress=prog)
            info: dict[str, Any] = {
                "mode": "verified",
                "missing": rep.missing,
                "modified": rep.modified,
                "extra": rep.extra,
            }
        else:
            say(f"building manifest {target} (reads every file; can take a long time)")
            m = build_manifest(root, progress=prog)
            write_manifest(m, target, root=root if in_root else None)
            info = {"mode": "built"}
    except CatalogError as exc:
        raise InspectError(str(exc)) from exc
    except OSError as exc:
        raise InspectError(f"manifest {target}: {exc.strerror or exc}") from exc
    info.update(manifest_path=str(target), files=len(m.files), manifest_sha256=sha256_file(target))
    return info


def main(argv: list[str] | None = None, *, expected_types: Mapping[str, Sequence[str]] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Read-only inspection of a ResStock/ComStock data root. Never modifies the data root.",
    )
    ap.add_argument("--data-root", default=None, help="data root (else env MICROGRID_DATA_DIR); no default")
    ap.add_argument(
        "--out", type=Path, default=None, help="write the markdown report here (else printed); outside root"
    )
    ap.add_argument("--json", type=Path, default=None, help="write the JSON report here; outside the data root")
    ap.add_argument("--sample-buildings", type=int, default=5, help="parquet files to sample per product (default 5)")
    ap.add_argument(
        "--interesting-columns", default=",".join(DEFAULT_INTERESTING), help="comma-separated extra columns"
    )
    ap.add_argument("--overwrite-report", action="store_true", help="allow replacing an existing --out/--json file")
    ap.add_argument(
        "--hash", action="store_true", help="build or verify a manifest (needs --out; reads every file; OFF by default)"
    )
    ap.add_argument("--manifest", type=Path, default=None, help="manifest path for --hash (default: next to --out)")
    ap.add_argument(
        "--manifest-in-root",
        action="store_true",
        help=f"with --hash, use <data-root>/{MANIFEST_NAME} (the data_manifest.py default; writes INSIDE the root)",
    )
    args = ap.parse_args(argv)

    def say(msg: str) -> None:
        print(f"[inspect] {msg}", file=sys.stderr, flush=True)

    try:
        root = resolve_data_root(args.data_root)
        if args.sample_buildings < 0:
            raise InspectError("--sample-buildings must be 0 or more")
        _check_output(root, args.out, "report", args.overwrite_report)
        _check_output(root, args.json, "JSON report", args.overwrite_report)
        manifest_target: Path | None = None
        if args.hash:
            if args.out is None:
                raise InspectError(
                    "--hash requires --out (the manifest is written next to the report; without it a stray file "
                    "would land in the current folder)"
                )
            if args.manifest_in_root and args.manifest:
                raise InspectError("use either --manifest or --manifest-in-root, not both")
            if args.manifest_in_root:
                manifest_target = root / MANIFEST_NAME
            else:
                manifest_target = args.manifest or args.out.with_name(args.out.stem + ".manifest.json")
                if _inside(root, manifest_target):
                    raise InspectError(
                        f"manifest {manifest_target} is inside the data root {root}; choose a path outside it "
                        "or pass --manifest-in-root to accept that"
                    )
                if not manifest_target.parent.is_dir():
                    raise InspectError(f"manifest {manifest_target}: folder {manifest_target.parent} does not exist")
        elif args.manifest or args.manifest_in_root:
            raise InspectError("--manifest/--manifest-in-root only apply together with --hash")
        interesting = tuple(c.strip() for c in args.interesting_columns.split(",") if c.strip())
        rep = inspect_dataset(
            root,
            sample_buildings=args.sample_buildings,
            interesting_columns=interesting,
            expected_types=expected_types,
            progress=say,
        )
        if manifest_target is not None:
            rep["hash"] = _do_hash(root, manifest_target, args.manifest_in_root, say)
        md = render_markdown(rep)
        import json

        if args.out:
            _write_text(args.out, md, args.overwrite_report)
            say(f"wrote {args.out}")
        else:
            print(md)
        if args.json:
            _write_text(args.json, json.dumps(rep, indent=1, default=str) + "\n", args.overwrite_report)
            say(f"wrote {args.json}")
    except (InspectError, CatalogError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0
