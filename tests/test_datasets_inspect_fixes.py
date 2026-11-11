"""Fix-pass tests for the inspector: engine parity, honest status text, per-file errors, duplicates, --hash rules."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from microgrid.community_load.synthetic.archetype import _completed_and_applicable
from microgrid.datasets import inspect as ins

EXPECTED = {
    "resstock": ("Single-Family Detached", "Multi-Family with 2 - 4 Units", "Multi-Family with 5+ Units, 4-7 Stories"),
    "comstock": ("SmallOffice", "MediumOffice", "PrimarySchool"),
}
RES_PART = Path("input/resstock/timeseries_individual_buildings/upgrade=0/state=DC")


@pytest.fixture
def root(tmp_path: Path, synthetic_dataset) -> Path:
    dst = tmp_path / "data"
    shutil.copytree(synthetic_dataset.root, dst)
    return dst


def _edit(root: Path, fn) -> pd.DataFrame:
    p = root / "background" / "chars_resstock.xlsx"
    df = fn(pd.read_excel(p, sheet_name="building_characteristics"))
    df.to_excel(p, sheet_name="building_characteristics", index=False)
    return df


# ------------------------------------------------------------------------------------------ engine parity


@pytest.mark.parametrize("value", ["yes", "1", "Yes", True, False, "true", " TRUE ", "false", np.nan, None, 1, 0, "0"])
@pytest.mark.parametrize("status", ["Success", "success ", "Fail", np.nan, None, True])
def test_not_usable_equals_engine_for_value_variants(root, value, status):
    def edit(d: pd.DataFrame) -> pd.DataFrame:
        d = d.copy()
        d["completed_status"] = pd.Series([status] * len(d), dtype=object)
        d["applicability"] = pd.Series([value] * len(d), dtype=object)
        return d

    _edit(root, edit)
    df = pd.read_excel(root / "background" / "chars_resstock.xlsx", sheet_name="building_characteristics")
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    assert rep["characteristics"]["resstock"]["not_usable_rows"] == int((~_completed_and_applicable(df)).sum())


def test_not_usable_equals_engine_on_mixed_column(root):
    df = _edit(
        root,
        lambda d: d.assign(
            applicability=pd.Series(["true", "yes", True, False, np.nan, None] * len(d), dtype=object)[: len(d)]
        ),
    )
    df = pd.read_excel(root / "background" / "chars_resstock.xlsx", sheet_name="building_characteristics")
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    assert rep["characteristics"]["resstock"]["not_usable_rows"] == int((~_completed_and_applicable(df)).sum())


# ------------------------------------------------------------------------ missing status columns are explicit


@pytest.mark.parametrize("col", ["completed_status", "applicability"])
def test_missing_status_column_is_stated_explicitly(root, col):
    _edit(root, lambda d: d.drop(columns=[col]))
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    md = ins.render_markdown(rep)
    marker = "column missing: the engine will refuse to run with require_data_complete on"
    assert marker in md
    assert any(marker in s and col in s and "resstock" in s for s in rep["next_steps"])
    assert not any("excludes nothing" in s and "resstock" in s for s in rep["next_steps"])
    assert rep["characteristics"]["resstock"]["not_usable_rows"] is None
    assert "**None**" not in md


# ------------------------------------------------------------------------------------ per-file parquet errors


def _first_res_parquet(root: Path) -> Path:
    return sorted((root / RES_PART).glob("*.parquet"))[0]


@pytest.mark.parametrize("kind", ["garbage", "zero", "truncated"])
def test_bad_sampled_parquet_becomes_error_note_and_run_continues(root, tmp_path, capsys, kind):
    p = _first_res_parquet(root)
    if kind == "garbage":
        p.write_bytes(b"not parquet")
    elif kind == "zero":
        p.write_bytes(b"")
    else:
        p.write_bytes(p.read_bytes()[: p.stat().st_size // 2])
    before = p.read_bytes()
    out, js = tmp_path / "r.md", tmp_path / "r.json"
    rc = ins.main(["--data-root", str(root), "--sample-buildings", "50", "--out", str(out), "--json", str(js)])
    assert rc == 0
    assert p.read_bytes() == before  # never touched
    md = out.read_text()
    rel = p.relative_to(root).as_posix()
    assert f"ERROR: {rel}" in md
    assert str(root) not in md.split("## 3.")[1].split("## 4.")[0]  # section 3 names files relative to the root
    assert "Traceback" not in md and "Traceback" not in capsys.readouterr().err
    assert "1 sampled files could not be read" in md.split("## 1.")[0]  # prominent: before section 1
    rep = json.loads(js.read_text())
    errs = rep["parquet_sample"]["resstock"]["errors"]
    assert [e["file"] for e in errs] == [rel] and errs[0]["error"].startswith("ERROR: ")
    assert rep["sampled_files_unreadable"] == 1
    step = next(s for s in rep["next_steps"] if "could not be read" in s)
    assert "copy" in step.lower() and "never delete" in step.lower() and rel in step
    assert rep["parquet_sample"]["comstock"]["samples"]  # the other product was still sampled
    assert len(rep["parquet_sample"]["resstock"]["samples"]) >= 1  # and the other resstock files


def test_unreadable_directory_is_skipped_with_reason_and_walk_continues(root, monkeypatch):
    real_walk = ins.os.walk
    bad = root / "input" / ".Spotlight-V100"

    def walk(top, followlinks=False, onerror=None):
        if onerror:
            onerror(PermissionError(13, "Permission denied", str(bad)))
        yield from real_walk(top, followlinks=followlinks, onerror=onerror)

    monkeypatch.setattr(ins.os, "walk", walk)
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    assert rep["layout"]["skipped_dirs"] == [{"path": "input/.Spotlight-V100", "reason": "Permission denied"}]
    assert rep["layout"]["total_files"] > 0
    md = ins.render_markdown(rep)
    assert "input/.Spotlight-V100" in md and "Permission denied" in md


# ------------------------------------------------------------------------------------------ duplicate ids


def test_duplicate_ids_counted_in_json_markdown_and_next_steps(root):
    _edit(root, lambda d: pd.concat([d, d.iloc[[0, 0, 1]]], ignore_index=True))
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    idm = rep["characteristics"]["resstock"]["id_match"]
    assert idm["duplicate_rows"] == 3 and idm["rows_with_id"] == idm["chars_ids"] + 3
    text = ins.render_markdown(rep).replace(str(root), "ROOT")
    assert "Duplicate bldg_id rows: 3" in text
    assert any("duplicate" in s.lower() and "resstock" in s for s in rep["next_steps"])


def test_no_duplicates_no_complaint(root):
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    assert rep["characteristics"]["resstock"]["id_match"]["duplicate_rows"] == 0
    assert not any("duplicate" in s.lower() for s in rep["next_steps"])


# ---------------------------------------------------------------------------------------------- --hash rules


@pytest.mark.parametrize("extra", [[], ["--manifest-in-root"]])
def test_hash_requires_out(root, tmp_path, capsys, extra, monkeypatch):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    rc = ins.main(["--data-root", str(root), "--hash", *extra])
    assert rc == 2 and "--out" in capsys.readouterr().err
    assert list(cwd.iterdir()) == [] and not (root / "manifest.json").exists()


def test_inside_check_delegates_to_identity_helper(root, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(ins, "is_inside", lambda r, p: True)
    assert ins.main(["--data-root", str(root), "--out", str(tmp_path / "x.md")]) == 2
    assert "inside the data root" in capsys.readouterr().err
