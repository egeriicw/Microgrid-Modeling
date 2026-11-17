"""Data manifest: build, verify, resume, disk report (FR-DATA-1). Never downloads, never deletes."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from microgrid.datasets import manifest as mf
from microgrid.datasets.catalog import CatalogError

REPO = Path(__file__).parent.parent


def test_round_trip_and_hashes(tmp_path, synthetic_dataset):
    import shutil

    root = tmp_path / "copy"  # never write into the shared session fixture
    shutil.copytree(synthetic_dataset.root, root)
    m = mf.build_manifest(root)
    assert len(m.files) == 17  # 15 parquet + 2 workbooks
    sample = next(iter(m.files.values()))
    assert len(sample.sha256) == 64
    rel = "background/chars_resstock.xlsx"
    assert m.files[rel].sha256 == hashlib.sha256((root / rel).read_bytes()).hexdigest()
    assert m.files[rel].size == (root / rel).stat().st_size
    out = mf.write_manifest(m, root / mf.MANIFEST_NAME)
    assert mf.read_manifest(out) == m
    assert mf.MANIFEST_NAME not in m.files  # the manifest never lists itself


def test_verify_clean_then_detects_modified_and_missing(tmp_path, synthetic_dataset):
    import shutil

    root = tmp_path / "copy"
    shutil.copytree(synthetic_dataset.root, root)
    m = mf.build_manifest(root)
    assert mf.verify_manifest(root, m).ok

    victim = next(iter(sorted(m.files)))
    (root / victim).write_bytes((root / victim).read_bytes() + b"x")
    gone = sorted(m.files)[-1]
    (root / gone).unlink()
    (root / "background" / "extra.txt").write_text("new")
    rep = mf.verify_manifest(root, m)
    assert not rep.ok
    assert rep.modified == [victim]
    assert rep.missing == [gone]
    assert rep.extra == ["background/extra.txt"]


def test_verify_same_size_content_change_is_detected(tmp_path):
    (tmp_path / "a.bin").write_bytes(b"aaaa")
    m = mf.build_manifest(tmp_path)
    (tmp_path / "a.bin").write_bytes(b"bbbb")
    assert mf.verify_manifest(tmp_path, m).modified == ["a.bin"]


def test_resume_reuses_hashes_for_unchanged_files(tmp_path, monkeypatch):
    (tmp_path / "a.bin").write_bytes(b"aaaa")
    (tmp_path / "b.bin").write_bytes(b"bbbb")
    first = mf.build_manifest(tmp_path)
    calls: list[Path] = []
    real = mf.sha256_file
    monkeypatch.setattr(mf, "sha256_file", lambda p: (calls.append(p), real(p))[1])
    (tmp_path / "c.bin").write_bytes(b"cccc")
    second = mf.build_manifest(tmp_path, previous=first)
    assert [p.name for p in calls] == ["c.bin"]
    assert set(second.files) == {"a.bin", "b.bin", "c.bin"}


def test_idempotent_manifest_bytes(tmp_path):
    (tmp_path / "a.bin").write_bytes(b"aaaa")
    p = tmp_path / mf.MANIFEST_NAME
    mf.write_manifest(mf.build_manifest(tmp_path), p)
    first = p.read_bytes()
    mf.write_manifest(mf.build_manifest(tmp_path, previous=mf.read_manifest(p)), p)
    assert p.read_bytes() == first


def test_progress_callback_called_per_file(tmp_path):
    for n in "abc":
        (tmp_path / n).write_bytes(b"x")
    seen: list[tuple[int, int]] = []
    mf.build_manifest(tmp_path, progress=lambda i, total, rel: seen.append((i, total)))
    assert seen == [(1, 3), (2, 3), (3, 3)]


def test_symlink_escaping_root_is_refused(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret")
    os.symlink(outside, root / "link.txt")
    with pytest.raises(CatalogError, match="outside the data root"):
        mf.build_manifest(root)


def test_manifest_with_escaping_path_rejected(tmp_path):
    bad = tmp_path / "m.json"
    bad.write_text(json.dumps({"version": 1, "files": [{"path": "../x", "size": 1, "sha256": "0" * 64}]}))
    with pytest.raises(CatalogError, match="outside the data root"):
        mf.read_manifest(bad)


def test_write_refuses_target_outside_root(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(CatalogError, match="outside the data root"):
        mf.write_manifest(mf.build_manifest(root), tmp_path / "elsewhere.json", root=root)


def test_missing_root_is_an_error(tmp_path):
    with pytest.raises(CatalogError, match="does not exist"):
        mf.build_manifest(tmp_path / "nope")


def test_disk_report(synthetic_dataset):
    rep = mf.disk_report(synthetic_dataset.root, budget_gb=30.0)
    assert rep["total_bytes"] > 0
    assert set(rep["by_top_level"]) == {"background", "input"}
    assert rep["budget_gb"] == 30.0 and 0 < rep["budget_used_fraction"] < 1
    assert rep["free_bytes"] > 0
    assert "MB" in mf.format_disk_report(rep) or "KB" in mf.format_disk_report(rep)


def test_cli_build_then_verify_exit_codes(tmp_path, capsys):
    (tmp_path / "a.bin").write_bytes(b"aaaa")
    assert mf.main(["build", "--data-root", str(tmp_path)]) == 0
    assert (tmp_path / mf.MANIFEST_NAME).exists()
    assert mf.main(["verify", "--data-root", str(tmp_path)]) == 0
    (tmp_path / "a.bin").write_bytes(b"zzzz")
    assert mf.main(["verify", "--data-root", str(tmp_path)]) == 1
    out = capsys.readouterr().out
    assert "modified" in out and "disk" in out.lower()


def test_cli_uses_env_var_and_fails_without_root(tmp_path, monkeypatch, capsys):
    (tmp_path / "a.bin").write_bytes(b"a")
    monkeypatch.setenv("MICROGRID_DATA_DIR", str(tmp_path))
    assert mf.main(["build"]) == 0
    monkeypatch.delenv("MICROGRID_DATA_DIR")
    assert mf.main(["build"]) == 2
    assert "MICROGRID_DATA_DIR" in capsys.readouterr().err


def test_script_entry_point_runs(tmp_path):
    (tmp_path / "a.bin").write_bytes(b"a")
    r = subprocess.run(
        [sys.executable, str(REPO / "scripts" / "data_manifest.py"), "build", "--data-root", str(tmp_path)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(REPO / "src")},
    )
    assert r.returncode == 0, r.stderr
