"""Read-only dataset inspector: report content, discrepancy detection, read-only guarantee, hostile inputs."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

from microgrid.datasets import inspect as ins
from microgrid.datasets.catalog import parse_catalog

REPO = Path(__file__).parent.parent
RES = "resstock"
COM = "comstock"
EXPECTED = {
    RES: ("Single-Family Detached", "Multi-Family with 2 - 4 Units", "Multi-Family with 5+ Units, 4-7 Stories"),
    COM: ("SmallOffice", "MediumOffice", "PrimarySchool"),
}
RES_PART = Path("input") / RES / "timeseries_individual_buildings" / "upgrade=0" / "state=DC"
COM_PART = Path("input") / COM / "timeseries_individual_buildings" / "upgrade=0" / "state=DC"
RES_COL = "out.electricity.total.energy_consumption..kwh"
COM_COL = "out.electricity.total.energy_consumption"


@pytest.fixture
def root(tmp_path: Path, synthetic_dataset) -> Path:
    dst = tmp_path / "data"
    shutil.copytree(synthetic_dataset.root, dst)  # never touch the shared session fixture
    return dst


def snapshot(root: Path) -> dict[str, tuple[int, int, str]]:
    out = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            st = p.stat()
            digest = hashlib.sha256(p.read_bytes()).hexdigest()
            out[p.relative_to(root).as_posix()] = (st.st_size, st.st_mtime_ns, digest)
    return out


def run(root: Path, **kw):
    return ins.inspect_dataset(root, expected_types=EXPECTED, **kw)


def _rewrite(path: Path, fn) -> None:
    fn(pd.read_parquet(path)).to_parquet(path, index=False)


def test_sections_present_and_markdown_renders(root):
    rep = run(root)
    for key in ("layout", "characteristics", "parquet_sample", "fingerprint", "catalog_stanza", "next_steps"):
        assert key in rep
    md = ins.render_markdown(rep)
    for heading in (
        "## 1. Layout",
        "## 2. Characteristics",
        "## 3. Parquet sample",
        "## 4. Release fingerprint",
        "## 5. Proposed catalog stanza",
        "## 6. Next steps",
    ):
        assert heading in md
    json.dumps(rep)  # machine-readable


def test_layout_counts_partitions_and_sizes(root):
    lay = run(root)["layout"]
    parts = {(p["product"], p["upgrade"], p["state"]): p for p in lay["partitions"]}
    assert parts[(RES, "0", "DC")]["parquet_files"] == 11
    assert parts[(COM, "0", "DC")]["parquet_files"] == 4
    assert parts[(RES, "0", "DC")]["bytes"] > 0
    assert lay["disk_free_bytes"] > 0
    assert lay["missing"] == []
    assert "input" in lay["folder_sizes"] and "background" in lay["folder_sizes"]


def test_layout_lists_missing_things(root):
    shutil.rmtree(root / "input" / COM)
    (root / "background" / "chars_resstock.xlsx").unlink()
    missing = " | ".join(run(root)["layout"]["missing"])
    assert "comstock" in missing and "timeseries" in missing
    assert "resstock" in missing and "characteristics" in missing


def test_completed_status_counts_match_known_bad_rows(root, synthetic_dataset):
    res = run(root)["characteristics"][RES]
    assert res["rows"] == 11
    assert res["value_counts"]["completed_status"] == {"Success": 10, "Fail": 1}
    assert res["value_counts"]["applicability"] == {"True": 10, "False": 1}
    assert res["not_usable_rows"] == len(synthetic_dataset.bad_resstock_ids) == 2
    assert res["source_file"].endswith("chars_resstock.xlsx")
    assert res["sheet"] == "building_characteristics"


def test_columns_grouped_by_prefix_and_described(root):
    res = run(root, interesting_columns=("in.vintage", "in.not_a_column"))["characteristics"][RES]
    assert "in.vintage" in res["columns_by_prefix"]["in"]
    assert "bldg_id" in res["columns_by_prefix"]["(none)"]
    described = {d["name"] for d in res["described"]}
    assert {"in.geometry_building_type_height", "in.sqft..ft2", "in.vintage"} <= described
    assert "in.not_a_column" in res["interesting_missing"]


def test_building_type_values_and_clean_registry_match(root):
    rep = run(root)
    res, com = rep["characteristics"][RES], rep["characteristics"][COM]
    assert res["building_types"]["column"] == "in.geometry_building_type_height"
    assert res["building_types"]["counts"]["Single-Family Detached"] == 3
    assert com["building_types"]["counts"] == {"SmallOffice": 2, "MediumOffice": 1, "PrimarySchool": 1}
    assert res["building_types"]["expected_not_found"] == []
    assert res["building_types"]["found_not_expected"] == []


def test_renamed_type_is_flagged(root):
    path = root / "background" / "chars_resstock.xlsx"
    df = pd.read_excel(path, sheet_name="building_characteristics")
    df["in.geometry_building_type_height"] = df["in.geometry_building_type_height"].replace(
        "Single-Family Detached", "Single-Family Detached Home"
    )
    df.to_excel(path, sheet_name="building_characteristics", index=False)
    rep = run(root)
    bt = rep["characteristics"][RES]["building_types"]
    assert bt["expected_not_found"] == ["Single-Family Detached"]
    assert bt["found_not_expected"] == ["Single-Family Detached Home"]
    md = ins.render_markdown(rep)
    assert "NOT FOUND in data" in md and "Single-Family Detached Home" in md


def test_id_match_both_ways(root):
    (root / RES_PART / "1000-0.parquet").unlink()
    shutil.copy(root / COM_PART / "500000-0.parquet", root / COM_PART / "999999-0.parquet")
    rep = run(root)["characteristics"]
    assert rep[RES]["id_match"]["in_chars_not_parquet"] == 1
    assert rep[RES]["id_match"]["in_parquet_not_chars"] == 0
    assert rep[COM]["id_match"]["in_parquet_not_chars"] == 1
    assert rep[COM]["id_match"]["matched"] == 4


def test_parquet_sample_pass(root):
    ps = run(root, sample_buildings=3)["parquet_sample"]
    res = ps[RES]
    assert len(res["files"]) == 3
    assert res["files"] == sorted(res["files"])  # deterministic by sorted name
    f = res["samples"][0]
    assert f["rows"] == 35040 and f["time_step"] == "15-minute" and f["years"] == [2018]
    assert f["first_timestamp"].startswith("2018-01-01") and f["last_timestamp"].startswith("2018-12-31 23:45")
    assert f["total_column"]["expected"] == RES_COL
    assert f["total_column"]["status"] == "PASS"
    assert f["sum_kwh"] > 0 and f["peak_kw"] > 0 and f["nan_count"] == 0 and f["negative_count"] == 0
    assert ps[COM]["samples"][0]["total_column"]["expected"] == COM_COL
    assert run(root, sample_buildings=3)["parquet_sample"][RES]["files"] == res["files"]
    assert "PASS" in ins.render_markdown(run(root))


def test_missing_total_column_fails_and_suggests(root):
    for p in (root / RES_PART).glob("*.parquet"):
        _rewrite(p, lambda df: df.rename(columns={RES_COL: "out.electricity.total.kwh"}))
    rep = run(root)
    s = rep["parquet_sample"][RES]["samples"][0]
    assert s["total_column"]["status"] == "FAIL"
    assert "out.electricity.total.kwh" in s["total_column"]["candidates"]
    assert "FAIL" in ins.render_markdown(rep)


def test_nan_negative_and_hourly_detected(root):
    def mutate(df):
        df = df.iloc[::4].reset_index(drop=True)  # hourly
        df.loc[1, COM_COL] = float("nan")
        df.loc[2, COM_COL] = -5.0
        return df

    for p in (root / COM_PART).glob("*.parquet"):
        _rewrite(p, mutate)
    s = run(root)["parquet_sample"][COM]["samples"][0]
    assert s["time_step"] == "hourly" and s["nan_count"] == 1 and s["negative_count"] == 1
    assert any("NaN" in w for w in s["warnings"]) and any("negative" in w for w in s["warnings"])


def test_fingerprint_is_evidence_not_claim(root):
    fp = run(root)["fingerprint"]
    assert "evidence, not proof" in fp["disclaimer"]
    checks = {c["check"]: c for c in fp["checks"]}
    assert checks["ev columns (resstock)"]["found"] is True  # fixture has in.has_ev
    assert checks["in.energystar_climate_zone_2023 (resstock)"]["found"] is False
    assert checks["electric panel columns (resstock)"]["found"] is False
    assert "claimed_release" not in fp


def test_catalog_stanza_parses_and_never_verified(root):
    text = run(root)["catalog_stanza"]
    assert "verified: false" in text and "[verify]" in text and "verified: true" not in text
    cat = parse_catalog(yaml.safe_load("version: 1\ndatasets:\n" + text))
    assert {e.product for e in cat.entries} == {RES, COM}
    assert all(not e.verified for e in cat.entries)
    res = next(e for e in cat.entries if e.product == RES)
    assert res.relative_path == RES_PART.as_posix()


def test_next_steps_mention_bad_rows(root):
    steps = " ".join(run(root)["next_steps"])
    assert "2 buildings" in steps and "require_data_complete" in steps


def test_read_only_guarantee(root, tmp_path):
    before = snapshot(root)
    dirs_before = sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_dir())
    out, js = tmp_path / "rep.md", tmp_path / "rep.json"
    assert ins.main(["--data-root", str(root), "--out", str(out), "--json", str(js)], expected_types=EXPECTED) == 0
    assert snapshot(root) == before
    assert sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_dir()) == dirs_before
    assert "## 1. Layout" in out.read_text()
    assert "layout" in json.loads(js.read_text())


def test_refuses_output_inside_root(root, capsys):
    before = snapshot(root)
    for flag in ("--out", "--json"):
        rc = ins.main(["--data-root", str(root), flag, str(root / "background" / "report.out")])
        assert rc == 2
        assert "inside the data root" in capsys.readouterr().err
    assert snapshot(root) == before
    assert not (root / "background" / "report.out").exists()


def test_refuses_output_inside_root_via_symlink(root, tmp_path):
    link = tmp_path / "alias"
    link.symlink_to(root)
    assert ins.main(["--data-root", str(root), "--out", str(link / "r.md")]) == 2
    assert not (root / "r.md").exists()


def test_refuses_to_overwrite_existing_report(root, tmp_path, capsys):
    out = tmp_path / "rep.md"
    out.write_text("precious")
    assert ins.main(["--data-root", str(root), "--out", str(out)]) == 2
    assert out.read_text() == "precious"
    assert "already exists" in capsys.readouterr().err
    assert ins.main(["--data-root", str(root), "--out", str(out), "--overwrite-report"]) == 0


def test_hostile_empty_root(tmp_path, capsys):
    empty = tmp_path / "empty"
    empty.mkdir()
    assert ins.main(["--data-root", str(empty)]) == 2
    assert str(empty) in capsys.readouterr().err


def test_hostile_missing_root_and_no_root(tmp_path, capsys, monkeypatch):
    assert ins.main(["--data-root", str(tmp_path / "nope")]) == 2
    assert "nope" in capsys.readouterr().err
    monkeypatch.delenv("MICROGRID_DATA_DIR", raising=False)
    assert ins.main([]) == 2
    assert "MICROGRID_DATA_DIR" in capsys.readouterr().err


def test_root_from_environment(root, monkeypatch):
    monkeypatch.setenv("MICROGRID_DATA_DIR", str(root))
    assert ins.main([]) == 0


def test_hostile_symlink_loop(root, capsys):
    loop = root / "background" / "loop.xlsx"
    loop.symlink_to(loop)
    assert ins.main(["--data-root", str(root)]) == 2
    assert "loop.xlsx" in capsys.readouterr().err


def test_symlinked_directory_is_skipped_not_followed(root):
    (root / "input" / "alias").symlink_to(root / "input" / RES)
    rep = run(root)
    assert "input/alias" in rep["layout"]["skipped_symlinks"]
    assert sum(p["parquet_files"] for p in rep["layout"]["partitions"]) == 15


def test_missing_characteristics_file_is_reported_not_crash(root):
    shutil.rmtree(root / "background")
    rep = run(root)
    assert rep["characteristics"][RES] is None
    assert any("characteristics" in m for m in rep["layout"]["missing"])
    assert any("characteristics" in s.lower() for s in rep["next_steps"])


def test_hostile_corrupt_parquet(root, capsys):
    for p in (root / RES_PART).glob("*.parquet"):
        p.write_bytes(b"this is not parquet")
    assert ins.main(["--data-root", str(root)]) == 2
    err = capsys.readouterr().err
    assert ".parquet" in err and "Traceback" not in err


def test_hostile_corrupt_characteristics(root, capsys):
    (root / "background" / "chars_resstock.xlsx").write_bytes(b"garbage")
    assert ins.main(["--data-root", str(root)]) == 2
    assert "chars_resstock.xlsx" in capsys.readouterr().err


def test_csv_characteristics_used_when_no_xlsx(root):
    p = root / "background" / "chars_resstock.xlsx"
    pd.read_excel(p, sheet_name="building_characteristics").to_csv(
        root / "background" / "chars_resstock.csv", index=False
    )
    p.unlink()
    res = run(root)["characteristics"][RES]
    assert res["source_file"].endswith(".csv") and res["rows"] == 11 and res["sheet"] is None


def test_progress_goes_to_stderr_only(root, tmp_path, capfd):
    ins.main(["--data-root", str(root), "--out", str(tmp_path / "r.md")])
    cap = capfd.readouterr()
    assert cap.err.strip() != ""
    assert "## 1. Layout" not in cap.err


def test_stdout_gets_report_when_no_out(root, capfd):
    assert ins.main(["--data-root", str(root)]) == 0
    assert "## 1. Layout" in capfd.readouterr().out


def test_hash_off_by_default_and_outside_root_when_on(root, tmp_path):
    before = snapshot(root)
    assert ins.main(["--data-root", str(root), "--out", str(tmp_path / "rep.md")]) == 0
    assert not (tmp_path / "rep.manifest.json").exists()
    assert ins.main(["--data-root", str(root), "--out", str(tmp_path / "r2.md"), "--hash"]) == 0
    man = tmp_path / "r2.manifest.json"
    assert man.exists() and not (root / "manifest.json").exists()
    assert snapshot(root) == before
    assert "manifest" in (tmp_path / "r2.md").read_text().lower()
    argv = ["--data-root", str(root), "--out", str(tmp_path / "r3.md"), "--hash", "--manifest", str(man)]
    assert ins.main(argv) == 0  # existing manifest is verified, not rebuilt
    assert "verified" in (tmp_path / "r3.md").read_text().lower()


def test_manifest_in_root_only_when_asked(root, tmp_path):
    argv = ["--data-root", str(root), "--out", str(tmp_path / "r.md"), "--hash", "--manifest-in-root"]
    assert ins.main(argv) == 0
    assert (root / "manifest.json").exists()


def test_manifest_path_inside_root_refused_without_flag(root):
    rc = ins.main(["--data-root", str(root), "--hash", "--manifest", str(root / "m.json")])
    assert rc == 2 and not (root / "m.json").exists()


def test_cli_script_end_to_end(root, tmp_path):
    script = str(REPO / "scripts" / "inspect_dataset.py")
    out = tmp_path / "r.md"
    cp = subprocess.run(
        [sys.executable, script, "--data-root", str(root), "--out", str(out), "--sample-buildings", "2"],
        capture_output=True,
        text=True,
        env={**os.environ, "MICROGRID_DATA_DIR": ""},
    )
    assert cp.returncode == 0, cp.stderr
    text = out.read_text()
    assert "## 1. Layout" in text and "Single-Family Detached" in text
    bad = subprocess.run(
        [sys.executable, script, "--data-root", str(tmp_path / "missing")], capture_output=True, text=True
    )
    assert bad.returncode == 2 and "Traceback" not in bad.stderr
