"""Independent QA review of W1-TEMP (FR-ENG-10 temperature on results, FR-ENG-13 weather interface and
per-building load). Hostile and edge inputs, the "no temperature" paths, per-building profiles and the layering rule.

Tests marked ``xfail(strict=True)`` document real defects found in review; they start failing (XPASS) when the
defect is fixed, which forces the marker to be removed. Nothing here changes production code. Weather-file column
names are fixture-only; real NREL names are not confirmed [verify].
"""

from __future__ import annotations

import ast
import copy
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from microgrid.community_load.synthetic import archetype, ensemble, loader, run
from microgrid.community_load.synthetic import temperature as temp_mod
from microgrid.community_load.synthetic.config import config_from_dict
from microgrid.community_load.synthetic.results import build_results
from tests import test_architecture as arch
from tests.fixtures.synthetic_data import build_synthetic_dataset, temperature_c
from tests.test_run_facade import build_tiny_config

T_COL = "temp_x"  # fixture-only weather column names
TIME_COL = "ts"
H_IDX = pd.date_range("2018-01-01", periods=8760, freq="h")
Q_IDX = pd.date_range("2018-01-01", periods=35040, freq="15min")
BLOCK_KEYS = {
    "available",
    "reason",
    "units",
    "source",
    "source_resolution",
    "hourly",
    "15min",
    "derivation",
    "consistency",
}


@pytest.fixture(autouse=True)
def _fresh_cache():
    loader.clear_cache()
    yield
    loader.clear_cache()


def _write(path: Path, times, values, *, as_str: bool = True) -> Path:
    t = pd.DatetimeIndex(times).strftime("%Y-%m-%d %H:%M:%S") if as_str else times
    pd.DataFrame({TIME_COL: t, T_COL: values}).to_csv(path, index=False)
    return path


def _cfg(ds, path: Path | str, units: str = "C") -> dict:
    cfg = build_tiny_config(ds)
    cfg["data"].update(
        temperature_source="weather_file",
        temperature_weather_file=str(path),
        temperature_time_column=TIME_COL,
        temperature_column=T_COL,
        temperature_units=units,
    )
    return cfg


def _temp(cfg) -> dict:
    return run.run_scenario(cfg).to_dict()["results"]["temperature"]


# ---- Fahrenheit conversion at known points ------------------------------------------------------------------------


@pytest.mark.parametrize(("f", "c"), [(-40.0, -40.0), (32.0, 0.0), (212.0, 100.0), (-459.67, -273.15)])
def test_fahrenheit_known_points(synthetic_dataset, tmp_path, f, c):
    p = _write(tmp_path / "w.csv", H_IDX, np.full(len(H_IDX), f))
    t = _temp(_cfg(synthetic_dataset, p, units="F"))
    np.testing.assert_allclose(t["hourly"], c, rtol=0, atol=1e-9)
    np.testing.assert_allclose(t["15min"], c, rtol=0, atol=1e-9)


def test_celsius_is_not_converted(synthetic_dataset, tmp_path):
    p = _write(tmp_path / "w.csv", H_IDX, np.full(len(H_IDX), 32.0))
    t = _temp(_cfg(synthetic_dataset, p, units="C"))
    assert set(t["hourly"]) == {32.0}


# ---- time stamp hostility ------------------------------------------------------------------------------------------


def test_weather_file_shifted_by_one_interval_fails_loudly(synthetic_dataset, tmp_path):
    """End-of-interval labelling (01:00 .. next-year 00:00) must not be silently realigned."""
    p = _write(tmp_path / "w.csv", H_IDX + pd.Timedelta(hours=1), temperature_c(H_IDX))
    with pytest.raises(ValueError, match="does not cover"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_weather_file_with_utc_offset_strings_fails_loudly(synthetic_dataset, tmp_path):
    times = [f"{s}-05:00" for s in H_IDX.strftime("%Y-%m-%d %H:%M:%S")]
    p = tmp_path / "w.csv"
    pd.DataFrame({TIME_COL: times, T_COL: temperature_c(H_IDX)}).to_csv(p, index=False)
    with pytest.raises(ValueError, match="time zone"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_weather_file_with_mixed_utc_offsets_fails_loudly(synthetic_dataset, tmp_path):
    """DST-style mixed offsets (-05:00 / -04:00) must not parse into a silently shifted naive axis."""
    s = list(H_IDX.strftime("%Y-%m-%d %H:%M:%S"))
    times = [f"{x}-04:00" if 2000 <= i < 7000 else f"{x}-05:00" for i, x in enumerate(s)]
    p = tmp_path / "w.csv"
    pd.DataFrame({TIME_COL: times, T_COL: temperature_c(H_IDX)}).to_csv(p, index=False)
    with pytest.raises((ValueError, TypeError)):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_parquet_with_tz_aware_timestamps_fails_loudly(synthetic_dataset, tmp_path):
    p = tmp_path / "w.parquet"
    pd.DataFrame({TIME_COL: H_IDX.tz_localize("UTC"), T_COL: temperature_c(H_IDX)}).to_parquet(p)
    with pytest.raises(ValueError, match="time zone"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_timestamps_that_match_only_after_rounding_fail_loudly(synthetic_dataset, tmp_path):
    p = _write(tmp_path / "w.csv", H_IDX + pd.Timedelta(seconds=30), temperature_c(H_IDX))
    with pytest.raises(ValueError, match="does not cover"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_one_timestamp_off_by_a_second_fails_loudly(synthetic_dataset, tmp_path):
    times = H_IDX.to_series().copy()
    times.iloc[500] += pd.Timedelta(seconds=1)
    p = _write(tmp_path / "w.csv", pd.DatetimeIndex(times), temperature_c(H_IDX))
    with pytest.raises(ValueError, match="evenly spaced"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_leap_year_8784_hour_file_for_another_year_fails_loudly(synthetic_dataset, tmp_path):
    idx = pd.date_range("2020-01-01", periods=8784, freq="h")
    p = _write(tmp_path / "w.csv", idx, temperature_c(idx))
    with pytest.raises(ValueError, match="does not cover"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_longer_shuffled_file_is_aligned_by_time_stamp_not_by_row(synthetic_dataset, tmp_path):
    idx = pd.date_range("2017-07-01", "2019-07-01", freq="h", inclusive="left")
    order = np.random.default_rng(0).permutation(len(idx))
    p = _write(tmp_path / "w.csv", idx[order], temperature_c(idx)[order])
    t = _temp(_cfg(synthetic_dataset, p))
    np.testing.assert_allclose(t["hourly"], temperature_c(H_IDX), rtol=0, atol=1e-9)


def test_fifteen_minute_weather_file(synthetic_dataset, tmp_path):
    p = _write(tmp_path / "w.csv", Q_IDX, temperature_c(Q_IDX))
    t = _temp(_cfg(synthetic_dataset, p))
    assert t["source_resolution"] == "15min"
    np.testing.assert_allclose(t["15min"], temperature_c(Q_IDX), rtol=0, atol=1e-9)
    np.testing.assert_allclose(t["hourly"], temperature_c(Q_IDX).reshape(-1, 4).mean(axis=1), rtol=0, atol=1e-9)
    assert "mean" in t["derivation"]["hourly"] and t["derivation"]["15min"] == "source values"


def test_single_row_file_fails_loudly(synthetic_dataset, tmp_path):
    p = _write(tmp_path / "w.csv", H_IDX[:1], [1.0])
    with pytest.raises(ValueError, match="at least two"):
        run.run_scenario(_cfg(synthetic_dataset, p))


# ---- value hostility -----------------------------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [np.inf, -np.inf])
def test_infinite_values_fail_loudly(synthetic_dataset, tmp_path, bad):
    v = temperature_c(H_IDX)
    v[1234] = bad
    p = _write(tmp_path / "w.csv", H_IDX, v)
    with pytest.raises(ValueError, match="non-finite"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_column_present_but_all_null_fails_loudly(synthetic_dataset, tmp_path):
    p = _write(tmp_path / "w.csv", H_IDX, [None] * len(H_IDX))
    with pytest.raises(ValueError, match="non-finite"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_non_numeric_values_fail_loudly(synthetic_dataset, tmp_path):
    v = temperature_c(H_IDX).astype(object)
    v[10] = "N/A?"
    p = _write(tmp_path / "w.csv", H_IDX, v)
    with pytest.raises(ValueError):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_unsupported_file_type_fails_loudly(synthetic_dataset, tmp_path):
    p = _write(tmp_path / "w.txt", H_IDX, temperature_c(H_IDX))
    with pytest.raises(ValueError, match="unsupported format"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_uppercase_suffix_is_accepted(synthetic_dataset, tmp_path):
    p = _write(tmp_path / "W.CSV", H_IDX, temperature_c(H_IDX))
    assert _temp(_cfg(synthetic_dataset, p))["available"] is True


def test_missing_time_column_fails_loudly(synthetic_dataset, tmp_path):
    p = tmp_path / "w.csv"
    pd.DataFrame({"other": H_IDX.strftime("%Y-%m-%d %H:%M:%S"), T_COL: temperature_c(H_IDX)}).to_csv(p, index=False)
    with pytest.raises(ValueError, match=r"time column 'ts' not found"):
        run.run_scenario(_cfg(synthetic_dataset, p))


def test_directory_instead_of_file_fails_loudly(synthetic_dataset, tmp_path):
    d = tmp_path / "w.csv"
    d.mkdir()
    with pytest.raises(FileNotFoundError, match="weather file"):
        run.run_scenario(_cfg(synthetic_dataset, d))


# ---- paths: results never carry server directories ---------------------------------------------------------------


def test_path_like_file_values_never_reach_the_results(synthetic_dataset, tmp_path):
    """Containment of this path is the API's job (it rejects client paths, see api/tests); the engine, like for its
    other data paths, reads what the trusted config says. What the result publishes must be a file name only."""
    real = _write(tmp_path / "real.csv", H_IDX, temperature_c(H_IDX))
    sub = tmp_path / "sub"
    sub.mkdir()
    link = tmp_path / "link.csv"
    link.symlink_to(real)
    for value in (str(sub / ".." / "real.csv"), str(link)):
        doc = run.run_scenario(_cfg(synthetic_dataset, value)).to_dict()
        t = doc["results"]["temperature"]
        assert t["source"]["file"] in ("real.csv", "link.csv")
        assert str(tmp_path) not in json.dumps(doc["results"])


def test_missing_file_message_does_not_leak_into_results(synthetic_dataset, tmp_path):
    with pytest.raises(FileNotFoundError):
        run.run_scenario(_cfg(synthetic_dataset, tmp_path / "absent.csv"))


# ---- "no temperature" paths: same keys, explicit reason -----------------------------------------------------------


def test_block_has_the_same_keys_whether_available_or_not(synthetic_dataset, tmp_path):
    p = _write(tmp_path / "w.csv", H_IDX, temperature_c(H_IDX))
    on = _temp(_cfg(synthetic_dataset, p))
    off = _temp(build_tiny_config(synthetic_dataset))
    not_computed = build_results(run.run_scenario(build_tiny_config(synthetic_dataset)).ensemble).to_dict()
    assert set(on) == set(off) == set(not_computed["temperature"]) == BLOCK_KEYS
    assert off["available"] is False and off["reason"]


def test_no_feasible_runs_with_a_declared_source_says_why(synthetic_dataset, tmp_path):
    p = _write(tmp_path / "w.csv", H_IDX, temperature_c(H_IDX))
    cfg = _cfg(synthetic_dataset, p)
    cfg["feasibility"] = {"hard": {"annual_energy_kwh": {"max": 1.0}}}  # rejects every run
    d = run.run_scenario(cfg).to_dict()
    assert d["results"]["n_runs"] == 0
    t = d["results"]["temperature"]
    assert set(t) == BLOCK_KEYS
    assert t["available"] is False and "no feasible runs" in t["reason"]
    assert t["source"]["kind"] == "weather_file" and t["source"]["file"] == "w.csv"
    json.dumps(d, allow_nan=False)


def test_alignment_without_a_15min_load_axis_gives_hourly_only():
    """A load with no 15-minute series yields hourly temperature only (15min null, no derivation for it)."""
    s = temp_mod._series(H_IDX, temperature_c(H_IDX), "C", "x")
    res, h, q, deriv = temp_mod._align(s, H_IDX, None, "x")
    assert res == "hourly" and q is None and deriv["15min"] is None
    np.testing.assert_allclose(h, temperature_c(H_IDX))


# ---- timeseries_column: NaN in a non-reference building ----------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (LOW): in timeseries_column mode a building whose temperature is NaN at some hours passes the "
    "consistency check (np.nanmax skips the NaN comparisons), so a partly empty series is reported as 'compared'",
)
def test_nan_in_a_non_reference_building_is_not_silently_skipped(tmp_path):
    def holes(df, col):
        df.loc[100:110, col] = np.nan
        return df

    cfg, runs, dirs = _two_building_case(tmp_path, holes)
    with pytest.raises(ValueError):
        temp_mod._from_buildings(cfg, runs, dirs)  # type: ignore[arg-type]


# ---- per-building profiles -----------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def other_dataset(tmp_path_factory):
    """Same layout and building ids as the session fixture, different random loads and characteristics."""
    return build_synthetic_dataset(tmp_path_factory.mktemp("other"), seed=7)


def _run_with_pool(ds, perturb):
    raw = build_tiny_config(ds)
    raw["perturbation"] = perturb
    cfg = config_from_dict(raw)
    bg = Path(cfg.data.background_dir)
    pool = archetype.build_archetype_pool(bg / cfg.data.resstock_chars_file, bg / cfg.data.comstock_chars_file, cfg)
    return cfg, pool, run.run_scenario(cfg, pool=pool)


def test_building_profiles_sum_to_run_totals_at_15min_including_commercial(synthetic_dataset):
    raw = build_tiny_config(synthetic_dataset)
    raw["perturbation"] = {"enabled": True, "max_temporal_shift_hours": 3}
    raw["composition"] = {
        "total_buildings": 5,
        "method": "dirichlet",
        "categories": {
            "mf_small": {"min_fraction": 0.2, "max_fraction": 0.8, "source": "resstock"},
            "office": {"min_fraction": 0.2, "max_fraction": 0.8, "source": "comstock"},
        },
    }
    raw["archetype_filters"]["comstock"] = {"office": {"building_types": ["SmallOffice", "MediumOffice"]}}
    raw["archetype_filters"]["resstock"].pop("mf_mid")
    cfg = config_from_dict(raw)
    bg = Path(cfg.data.background_dir)
    pool = archetype.build_archetype_pool(bg / cfg.data.resstock_chars_file, bg / cfg.data.comstock_chars_file, cfg)
    result = run.run_scenario(cfg, pool=pool)
    for r in result.ensemble.runs:
        ps = ensemble.building_profiles(r, pool, cfg, synthetic_dataset.data_dirs)
        com_q = sum(p.kwh_15min.to_numpy() for p in ps if p.source == "comstock")
        np.testing.assert_allclose(com_q, r.commercial_profile_15min.to_numpy(), rtol=1e-12, atol=0)
        res_h = sum(p.hourly_kwh.to_numpy() for p in ps if p.source == "resstock")
        np.testing.assert_allclose(res_h, r.residential_profile.to_numpy(), rtol=1e-12, atol=0)


def test_building_profiles_do_not_draw_random_numbers_or_mutate_the_run(synthetic_dataset):
    cfg, pool, result = _run_with_pool(synthetic_dataset, {"enabled": True, "max_temporal_shift_hours": 3})
    r = result.ensemble.runs[0]
    before = copy.deepcopy(r.perturbation_logs), r.total_profile.copy()
    a = ensemble.building_profiles(r, pool, cfg, synthetic_dataset.data_dirs)
    b = ensemble.building_profiles(r, pool, cfg, synthetic_dataset.data_dirs)
    for x, y in zip(a, b, strict=True):
        pd.testing.assert_series_equal(x.hourly_kwh, y.hourly_kwh)
    assert r.perturbation_logs == before[0]
    pd.testing.assert_series_equal(r.total_profile, before[1])


def test_building_profiles_with_mismatched_logs_fail_loudly(synthetic_dataset):
    cfg, pool, result = _run_with_pool(synthetic_dataset, {"enabled": False})
    r = result.ensemble.runs[0]
    r.perturbation_logs = r.perturbation_logs[:-1]
    with pytest.raises(ValueError, match="perturbation logs"):
        ensemble.building_profiles(r, pool, cfg, synthetic_dataset.data_dirs)


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (MEDIUM): building_profiles with data_dirs other than the run's silently returns different "
    "per-building loads that no longer sum to the run's totals; it should check against the run and fail loudly",
)
def test_building_profiles_with_other_data_dirs_fail_loudly(synthetic_dataset, other_dataset):
    cfg, pool, result = _run_with_pool(synthetic_dataset, {"enabled": False})
    r = result.ensemble.runs[0]
    with pytest.raises(ValueError):
        ensemble.building_profiles(r, pool, cfg, other_dataset.data_dirs)


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (MEDIUM): building_profiles with a pool other than the run's (here one whose characteristics "
    "table lacks the sampled ids, so multifamily scaling is silently skipped) returns loads that do not sum to the "
    "run's totals instead of failing",
)
def test_building_profiles_with_other_pool_fail_loudly(synthetic_dataset):
    import dataclasses

    cfg, pool, result = _run_with_pool(synthetic_dataset, {"enabled": False})
    r = next(x for x in result.ensemble.runs if any(s.unit_multiplier > 1 for s in x.selections))
    ids = {s.bldg_id for s in r.selections}
    other = dataclasses.replace(pool, resstock_chars=pool.resstock_chars[~pool.resstock_chars["bldg_id"].isin(ids)])
    try:
        ps = ensemble.building_profiles(r, other, cfg, synthetic_dataset.data_dirs)
    except ValueError:
        return
    total = sum(p.hourly_kwh.to_numpy() for p in ps)
    # sanity: the mismatch really changed the numbers (otherwise this test proves nothing)
    assert not np.allclose(total, r.total_profile.to_numpy(), rtol=1e-12, atol=0)
    pytest.fail("a pool different from the run's was accepted and returned loads that do not match the run")


# ---- layering rule for the weather module --------------------------------------------------------------------------


def _bad_imports(source: str, mod: str = "microgrid.community_load.weather") -> list[str]:
    tree_path = Path("_qa_tmp.py")
    banned = next(
        (b for pkg, b in sorted(arch.FORBIDDEN.items(), key=lambda kv: -len(kv[0])) if arch._under(mod, pkg)), []
    )
    found: set[str] = set()
    for node in ast.walk(ast.parse(source, str(tree_path))):
        if isinstance(node, ast.Import):
            found.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            found.add(node.module)
            found.update(f"{node.module}.{a.name}" for a in node.names)
    return sorted(i for i in found if any(arch._under(i, b) for b in banned))


@pytest.mark.parametrize(
    "source",
    [
        "from microgrid.community_load import synthetic",
        "from microgrid.community_load.synthetic import run",
        "from microgrid import community_load",
        "import microgrid.geo",
        "from microgrid import datasets",
        "import microgrid.community_load.sources as s",
    ],
)
def test_weather_rule_catches_absolute_imports(source):
    assert _bad_imports(source), source


def test_weather_rule_uses_the_architecture_scanner():
    """The helper above mirrors tests/test_architecture._imports; keep them in step."""
    p = Path(__file__).parent / "_qa_arch_probe.py"
    try:
        p.write_text("from microgrid.community_load import synthetic\n")
        assert "microgrid.community_load.synthetic" in arch._imports(p)
    finally:
        p.unlink()


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (LOW, pre-existing, now also covers weather): tests/test_architecture._imports skips relative "
    "imports (node.level > 0), so `from .synthetic import run` inside community_load/weather.py passes the rule",
)
def test_architecture_scanner_catches_relative_imports():
    p = Path(__file__).parent / "_qa_arch_probe_rel.py"
    try:
        p.write_text("from .synthetic import run\nfrom . import geo_bridge\n")
        found = arch._imports(p)
        assert any("synthetic" in f for f in found)
    finally:
        p.unlink()


def test_weather_module_imports_nothing_from_the_engine_at_runtime():
    import subprocess
    import sys

    code = (
        "import sys; import microgrid.community_load.weather as w; "
        "bad=[m for m in sys.modules if m.startswith('microgrid.community_load.synthetic')]; print(bad)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout.strip()
    # The package __init__ may import siblings; this records what importing weather actually pulls in.
    assert out == "[]", f"importing weather loads the engine: {out}"


def _two_building_case(tmp_path, edit):
    """Two ResStock buildings with a temperature column; ``edit`` changes the second building's frame."""
    from microgrid.community_load.synthetic._types import BuildingSelection

    col = "t_c"
    ds = build_synthetic_dataset(tmp_path / "d", temperature_column=col)
    cfg = build_tiny_config(ds)
    cfg["data"].update(temperature_source="timeseries_column", temperature_column=col, temperature_units="C")
    files = sorted((ds.root / "input" / "resstock").rglob("*.parquet"))
    a_id, b_id = (int(f.name.split("-")[0]) for f in files[:2])
    edit(pd.read_parquet(files[1]), col).to_parquet(files[1], index=False)

    class _R:
        selections = [
            BuildingSelection(bldg_id=a_id, category="x", source="resstock", unit_multiplier=1.0),
            BuildingSelection(bldg_id=b_id, category="x", source="resstock", unit_multiplier=1.0),
        ]

    return config_from_dict(cfg), [_R()], ds.data_dirs


def test_buildings_with_different_time_stamps_fail_loudly(tmp_path):
    cfg, runs, dirs = _two_building_case(tmp_path, lambda df, col: df.iloc[:-4])
    with pytest.raises(ValueError, match="different temperature time stamps"):
        temp_mod._from_buildings(cfg, runs, dirs)  # type: ignore[arg-type]


def test_buildings_differing_just_above_tolerance_fail_and_just_below_pass(tmp_path):
    tol = temp_mod.CONSISTENCY_TOLERANCE_C

    def bump(delta):
        def edit(df, col):
            df[col] = (df[col].astype("float64") + delta).astype("float64")
            return df

        return edit

    cfg, runs, dirs = _two_building_case(tmp_path / "hi", bump(tol * 1.5))
    with pytest.raises(ValueError, match="different temperature series"):
        temp_mod._from_buildings(cfg, runs, dirs)  # type: ignore[arg-type]
    loader.clear_cache()
    cfg, runs, dirs = _two_building_case(tmp_path / "lo", bump(tol * 0.5))
    _, consistency = temp_mod._from_buildings(cfg, runs, dirs)  # type: ignore[arg-type]
    assert 0 < consistency["max_abs_difference_c"] <= tol


# ---- docs match the code ---------------------------------------------------------------------------------------


SCHEMA_DOC = Path(__file__).parent.parent / "docs" / "reference" / "results-schema.md"


def test_schema_doc_lists_exactly_the_temperature_block_keys():
    text = SCHEMA_DOC.read_text()
    section = text.split("## `temperature` (FR-ENG-10)", 1)[1].split("\n## ", 1)[0]
    rows = [ln.split("|")[1].strip().strip("`") for ln in section.splitlines() if ln.startswith("| `")]
    assert set(rows) == BLOCK_KEYS
    assert '`schema_version` | string | `"1.1"`' in text
    assert '"temperature": "degC"' in text


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (LOW, docs): results-schema.md version history says 1.1 changed 'nothing else', but every "
    "document's config_snapshot.data now also carries the five data.temperature_* fields",
)
def test_schema_doc_version_history_mentions_the_new_config_fields():
    history = SCHEMA_DOC.read_text().split("**Version history.**", 1)[1].split("\n\n", 1)[0]
    assert "config_snapshot" in history
