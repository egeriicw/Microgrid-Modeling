"""catalog_fill: copy a catalog to a NEW file with manifest_sha256 and size_bytes set for one entry."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from microgrid.datasets import manifest as mf
from microgrid.datasets.catalog import CatalogError, load_catalog
from microgrid.datasets.catalog_fill import fill_catalog

REPO = Path(__file__).parent.parent
CATALOG = REPO / "configs" / "datasets" / "catalog.yaml"
KEY = "resstock/2025_release_1/amy2018/baseline/DC"


@pytest.fixture
def manifest_file(tmp_path: Path, synthetic_dataset) -> Path:
    root = tmp_path / "data"
    shutil.copytree(synthetic_dataset.root, root)
    return mf.write_manifest(mf.build_manifest(root), tmp_path / "m.json")


def test_fill_sets_hash_and_size_and_keeps_comments(tmp_path, manifest_file):
    out = tmp_path / "new.yaml"
    fill_catalog(CATALOG, manifest_file, KEY, out)
    cat = load_catalog(out)
    e = next(x for x in cat.entries if str(x.dataset_id) == KEY)
    m = mf.read_manifest(manifest_file)
    assert e.manifest_sha256 == mf.content_hash(m, e.relative_path)
    assert e.manifest_sha256 != hashlib.sha256(manifest_file.read_bytes()).hexdigest()  # not the file hash any more
    prefix = e.relative_path + "/"
    assert e.size_bytes == sum(f.size for r, f in m.files.items() if r.startswith(prefix)) > 0
    other = next(x for x in cat.entries if str(x.dataset_id) != KEY)
    assert other.manifest_sha256 is None and other.size_bytes is None
    assert "[verify]" in out.read_text() and e.verified is False


def test_original_untouched_and_never_in_place(tmp_path, manifest_file):
    before = CATALOG.read_bytes()
    with pytest.raises(CatalogError, match="in place|already exists"):
        fill_catalog(CATALOG, manifest_file, KEY, CATALOG)
    existing = tmp_path / "there.yaml"
    existing.write_text("x")
    with pytest.raises(CatalogError, match="already exists"):
        fill_catalog(CATALOG, manifest_file, KEY, existing)
    assert CATALOG.read_bytes() == before and existing.read_text() == "x"


def test_refill_replaces_existing_values(tmp_path, manifest_file):
    a, b = tmp_path / "a.yaml", tmp_path / "b.yaml"
    fill_catalog(CATALOG, manifest_file, KEY, a)
    fill_catalog(a, manifest_file, KEY, b)
    assert b.read_text().count("manifest_sha256") == 1 and b.read_text().count("size_bytes") == 1
    load_catalog(b)


def test_unknown_key_and_empty_match_fail_loudly(tmp_path):
    empty = mf.write_manifest(mf.Manifest({}), tmp_path / "empty.json")
    with pytest.raises(CatalogError, match="not in catalog"):
        fill_catalog(CATALOG, empty, "resstock/x/y/z/DC", tmp_path / "o.yaml")
    assert not (tmp_path / "o.yaml").exists()
    with pytest.raises(CatalogError, match="no files"):
        fill_catalog(CATALOG, empty, KEY, tmp_path / "o2.yaml")


def test_cli(tmp_path, manifest_file):
    script = str(REPO / "scripts" / "catalog_fill.py")
    out = tmp_path / "cli.yaml"
    cp = subprocess.run(
        [sys.executable, script, "--catalog", str(CATALOG), "--manifest", str(manifest_file), "--key", KEY]
        + ["--out", str(out)],
        capture_output=True,
        text=True,
    )
    assert cp.returncode == 0, cp.stderr
    assert "manifest_sha256" in out.read_text()
    bad = subprocess.run(
        [sys.executable, script, "--catalog", str(CATALOG), "--manifest", str(manifest_file), "--key", "nope"]
        + ["--out", str(tmp_path / "z.yaml")],
        capture_output=True,
        text=True,
    )
    assert bad.returncode == 2 and "Traceback" not in bad.stderr


# --------------------------------------------------------------------------------------------- content hash


def _fe(size: int, sha: str, mtime: int = 0) -> mf.FileEntry:
    return mf.FileEntry(size, sha, mtime)


def test_content_hash_ignores_mtime_and_other_products_and_catches_one_byte():
    a = mf.Manifest(
        {"input/x/a.parquet": _fe(5, "aa", 1), "input/x/b.parquet": _fe(6, "bb", 2), "input/y/c": _fe(1, "cc")}
    )
    same_other_mtime = mf.Manifest(
        {"input/x/a.parquet": _fe(5, "aa", 999), "input/x/b.parquet": _fe(6, "bb", 888), "input/y/c": _fe(9, "zz")}
    )
    moved = mf.Manifest({"input/x/a.parquet": _fe(5, "aa"), "input/x/b.parquet": _fe(6, "bb")})
    changed = mf.Manifest(
        {"input/x/a.parquet": _fe(5, "aa"), "input/x/b.parquet": _fe(6, "bX"), "input/y/c": _fe(1, "cc")}
    )
    h = mf.content_hash(a, "input/x")
    assert h == mf.content_hash(same_other_mtime, "input/x/")  # mtimes and other datasets do not matter
    assert h == mf.content_hash(moved, "input/x")
    assert h != mf.content_hash(changed, "input/x")  # one changed hash
    assert mf.content_hash(a, "input/y") != h  # another entry differs
    assert mf.content_hash(a, "input/x") != mf.content_hash(mf.Manifest({"input/x/a.parquet": _fe(5, "aa")}), "input/x")
    with pytest.raises(CatalogError, match="no files"):
        mf.content_hash(a, "input/zzz")
    assert mf.content_hash(a, "input/x") != mf.content_hash(a, "input")  # prefix is a folder boundary, not a string


def test_fill_hash_is_identical_for_copied_data_with_new_mtimes(tmp_path, synthetic_dataset):
    out = []
    for i in range(2):
        root = tmp_path / f"data{i}"
        shutil.copytree(synthetic_dataset.root, root, copy_function=shutil.copyfile)  # new mtimes
        m = mf.write_manifest(mf.build_manifest(root), tmp_path / f"m{i}.json")
        dst = tmp_path / f"new{i}.yaml"
        fill_catalog(CATALOG, m, KEY, dst)
        out.append(next(e for e in load_catalog(dst).entries if str(e.dataset_id) == KEY))
    assert out[0].manifest_sha256 == out[1].manifest_sha256 and out[0].size_bytes == out[1].size_bytes
