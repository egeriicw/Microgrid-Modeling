"""Independent QA tests for story 1.6 (data safety, resume correctness, catalog defaults, engine end-to-end)."""

from __future__ import annotations

import os

import pytest

from microgrid.community_load.synthetic import archetype, ensemble
from microgrid.community_load.synthetic.config import DataConfig, load_config
from microgrid.datasets import manifest as mf
from microgrid.datasets.catalog import parse_catalog
from tests.fixtures.synthetic_data import build_synthetic_dataset

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_build_rehashes_when_size_or_mtime_changed(tmp_path):
    """Resume may only reuse a hash when size AND mtime are unchanged; otherwise the manifest would go stale."""
    (tmp_path / "a.bin").write_bytes(b"aaaa")
    first = mf.build_manifest(tmp_path)
    (tmp_path / "a.bin").write_bytes(b"bbbbbbbb")  # different size (and mtime)
    second = mf.build_manifest(tmp_path, previous=first)
    assert second.files["a.bin"].sha256 != first.files["a.bin"].sha256
    assert second.files["a.bin"].size == 8


def test_catalog_entry_defaults_to_unverified():
    entry = {
        "product": "resstock",
        "release": "r",
        "weather": "amy2018",
        "state": "DC",
        "scenario_key": "baseline",
        "partition_id": 0,
        "name": "Baseline",
        "scenario_family": "Baseline",
        "load_basis": "gross",
        "license": "x [verify]",
        "relative_path": "input/x",
    }
    cat = parse_catalog({"version": 1, "datasets": [entry]})
    assert cat.entries[0].verified is False


def test_cli_never_modifies_data_files(tmp_path):
    """build/verify/report must leave every data file byte-identical (only manifest.json appears)."""
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "x.parquet").write_bytes(b"data")
    (tmp_path / "y.xlsx").write_bytes(b"more")
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    for cmd in ("build", "verify", "report", "build"):
        mf.main([cmd, "--data-root", str(tmp_path)])
    after = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file() and p.name != "manifest.json"}
    assert after == before
    assert sorted(p.name for p in tmp_path.rglob("*") if p.is_file()) == ["manifest.json", "x.parquet", "y.xlsx"]


@pytest.mark.xfail(
    strict=True,
    reason="QA 1.6 finding: manifest.json.tmp that is a symlink to a file INSIDE the root is followed and "
    "the target data file is overwritten by write_manifest (data loss).",
)
def test_tmp_symlink_to_data_file_is_not_overwritten(tmp_path):
    victim = tmp_path / "a.txt"
    victim.write_text("precious")
    (tmp_path / "manifest.json.tmp").symlink_to(victim)
    mf.main(["build", "--data-root", str(tmp_path)])
    assert victim.read_text() == "precious"


@pytest.mark.xfail(
    strict=True,
    reason="QA 1.6 finding: symlink loop / broken symlink raises an uncaught OSError instead of a clear error (exit 2).",
)
@pytest.mark.parametrize("kind", ["loop", "broken"])
def test_cli_handles_bad_symlinks_with_clear_error(tmp_path, kind):
    if kind == "loop":
        (tmp_path / "l1").symlink_to(tmp_path / "l2")
        (tmp_path / "l2").symlink_to(tmp_path / "l1")
    else:
        (tmp_path / "broken").symlink_to(tmp_path / "nope")
    assert mf.main(["build", "--data-root", str(tmp_path)]) == 2


@pytest.mark.xfail(
    strict=True,
    reason="QA 1.6 finding: a manifest entry missing 'size'/'sha256' raises an uncaught KeyError (exit path not 2).",
)
def test_malformed_manifest_entry_is_a_clear_error(tmp_path):
    import json

    (tmp_path / "a.txt").write_text("x")
    (tmp_path / "manifest.json").write_text(json.dumps({"version": 1, "files": [{"path": "a.txt"}]}))
    assert mf.main(["verify", "--data-root", str(tmp_path)]) == 2


def test_engine_end_to_end_on_synthetic_fixture(tmp_path):
    """First real exercise of run_ensemble: the fixture must yield feasible runs through the baseline scenario."""
    ds = build_synthetic_dataset(
        tmp_path,
        resstock_by_height={
            "Multi-Family with 2 - 4 Units": 4,
            "Multi-Family with 5+ Units, 1-3 Stories": 3,
            "Multi-Family with 5+ Units, 4-7 Stories": 4,
            "Multi-Family with 5+ Units, 8+ Stories": 3,
        },
        comstock_by_type={"SmallOffice": 3, "MediumOffice": 2, "Outpatient": 2},
    )
    cfg = load_config(os.path.join(ROOT, "configs", "scenarios", "dc_multifamily_baseline.yaml"))
    cfg.data = DataConfig(**ds.data)
    cfg.ensemble.max_runs = 4
    cfg.ensemble.min_runs = 4
    cfg.ensemble.check_every = 2
    pool = archetype.build_archetype_pool(ds.resstock_chars, ds.comstock_chars, cfg)
    result = ensemble.run_ensemble(pool, cfg, ds.data_dirs)
    assert len(result.runs) >= 1
    assert all((r.hourly_kwh > 0).all() for r in result.runs) if hasattr(result.runs[0], "hourly_kwh") else True
