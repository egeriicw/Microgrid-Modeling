"""QA review of the owner-run inspector: read-only proof, engine parity, honesty, hostile paths.

Tests marked ``xfail(strict=True)`` document real defects found in review (see the QA report). Each one fails today
for the stated reason; when the defect is fixed the strict xfail turns into a failure that tells the fixer to delete
the marker. They are NOT weakened tests.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pandas as pd
import pytest

from microgrid.community_load.synthetic.archetype import _completed_and_applicable
from microgrid.datasets import inspect as ins

REPO = Path(__file__).parent.parent
RES_PART = Path("input/resstock/timeseries_individual_buildings/upgrade=0/state=DC")
RES_COL = "out.electricity.total.energy_consumption..kwh"
EXPECTED = {
    "resstock": ("Single-Family Detached", "Multi-Family with 2 - 4 Units", "Multi-Family with 5+ Units, 4-7 Stories"),
    "comstock": ("SmallOffice", "MediumOffice", "PrimarySchool"),
}


@pytest.fixture
def root(tmp_path: Path, synthetic_dataset) -> Path:
    dst = tmp_path / "data"
    shutil.copytree(synthetic_dataset.root, dst)
    return dst


def _chars(root: Path, prod: str = "resstock") -> Path:
    return root / "background" / f"chars_{prod}.xlsx"


def _edit_chars(root: Path, fn, prod: str = "resstock") -> pd.DataFrame:
    p = _chars(root, prod)
    df = fn(pd.read_excel(p, sheet_name="building_characteristics"))
    df.to_excel(p, sheet_name="building_characteristics", index=False)
    return df


# ------------------------------------------------------------------------------- read-only proof (dynamic)

_AUDIT_DRIVER = textwrap.dedent(
    """
    import json, os, sys
    events = []
    WRITE = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC
    def hook(name, args):
        try:
            if name == "open":
                path, mode, flags = args
                if path is None or isinstance(path, int):
                    return
                m = mode or ""
                if any(c in m for c in "wax+") or (isinstance(flags, int) and flags & WRITE):
                    events.append([name, os.fsdecode(path), str(mode), flags])
            elif name in ("os.remove", "os.rmdir", "os.mkdir", "os.chmod", "os.truncate", "os.chown",
                          "os.rename", "os.link", "os.symlink", "shutil.copyfile", "shutil.rmtree",
                          "shutil.move", "shutil.copymode", "shutil.copystat", "shutil.copytree"):
                events.append([name] + [os.fsdecode(a) for a in args if isinstance(a, (str, bytes, os.PathLike))])
        except Exception as exc:  # never let the hook break the run
            events.append(["hook-error", repr(exc)])
    sys.addaudithook(hook)
    from microgrid.datasets.inspect import main
    rc = main(sys.argv[1:])
    sys.stderr.flush()
    with open(os.environ["AUDIT_OUT"], "w") as fh:
        json.dump({"rc": rc, "events": events}, fh)
    """
)


def _audited(args: list[str], tmp_path: Path, cwd: Path) -> dict:
    out = tmp_path / "audit.json"
    env = {**os.environ, "AUDIT_OUT": str(out), "PYTHONPATH": str(REPO / "src")}
    subprocess.run([sys.executable, "-c", _AUDIT_DRIVER, *args], env=env, cwd=cwd, capture_output=True, text=True)
    return json.loads(out.read_text())


def _under(path: str, root: Path, cwd: Path) -> bool:
    p = Path(path)
    p = p if p.is_absolute() else cwd / p
    try:
        p.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def test_audit_hook_no_write_under_root_default_and_hash(root, tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    res = _audited(
        ["--data-root", str(root), "--out", str(out / "r.md"), "--json", str(out / "r.json"), "--hash"], tmp_path, cwd
    )
    assert res["rc"] == 0
    writes = [e for e in res["events"] if any(_under(a, root, cwd) for a in e[1:2] + e[4:] if isinstance(a, str))]
    assert [e for e in res["events"] if e[0] == "hook-error"] == []
    assert writes == [], f"write-type events under the data root: {writes}"
    # and the only files written anywhere are the three report files (+ the exclusive temp that becomes the manifest)
    opened = {Path(e[1]).name for e in res["events"] if e[0] == "open"}
    assert {"r.md", "r.json"} <= opened


def test_audit_hook_manifest_in_root_writes_only_manifest(root, tmp_path):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    res = _audited(
        ["--data-root", str(root), "--hash", "--manifest-in-root", "--out", str(tmp_path / "r.md")], tmp_path, cwd
    )
    assert res["rc"] == 0
    inside = [e for e in res["events"] if e[0] in ("open", "os.rename", "os.chmod") and _under(e[1], root, cwd)]
    names = {Path(e[1]).name for e in inside}
    assert all(n == "manifest.json" or (n.startswith(".manifest.json.") and n.endswith(".tmp")) for n in names), names
    assert (root / "manifest.json").is_file()
    assert not list(root.glob(".manifest.json.*.tmp"))  # temp file removed


def test_default_run_creates_nothing_beside_the_data(root, tmp_path):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    before = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))
    out = tmp_path / "rep.md"
    subprocess.run(
        [sys.executable, str(REPO / "scripts" / "inspect_dataset.py"), "--data-root", str(root), "--out", str(out)],
        cwd=cwd,
        check=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(REPO / "src")},
    )
    after = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))
    assert sorted(set(after) - set(before)) == ["rep.md"]
    assert not list(cwd.iterdir())


@pytest.mark.skipif(shutil.which("strace") is None, reason="strace not installed")
def test_strace_no_write_syscall_under_root(root, tmp_path):
    log = tmp_path / "st.txt"
    out = tmp_path / "rep.md"
    subprocess.run(
        [
            "strace", "-f", "-qq", "-o", str(log),
            "-e", "trace=openat,open,creat,rename,renameat,renameat2,unlink,unlinkat,mkdir,mkdirat,chmod,fchmodat,"
            "truncate,symlink,link,utimensat",
            sys.executable, str(REPO / "scripts" / "inspect_dataset.py"), "--data-root", str(root), "--out", str(out),
            "--hash",
        ],
        check=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(REPO / "src")},
    )  # fmt: skip
    root_s = str(root.resolve())
    bad = []
    for line in log.read_text().splitlines():
        if root_s not in line or " = -1 " in line:
            continue
        if any(f in line for f in ("O_WRONLY", "O_RDWR", "O_CREAT", "O_APPEND", "O_TRUNC")) or any(
            s in line for s in ("rename", "unlink", "mkdir", "chmod", "truncate", "symlink", "link(", "utimens")
        ):
            bad.append(line)
    assert bad == []


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses permission bits; the audit/strace tests cover root")
def test_runs_on_a_fully_read_only_root(root, tmp_path):
    out = tmp_path / "rep.md"
    for p in [root, *root.rglob("*")]:
        p.chmod(0o555 if p.is_dir() else 0o444)
    try:
        assert ins.main(["--data-root", str(root), "--out", str(out), "--hash"]) == 0
    finally:
        for p in [root, *root.rglob("*")]:
            p.chmod(0o755 if p.is_dir() else 0o644)
    assert out.is_file()


# ------------------------------------------------------------------- output-path containment (all spellings)


@pytest.mark.parametrize("flag", ["--out", "--json"])
@pytest.mark.parametrize("spelling", ["relative_cwd_in_root", "dotdot", "symlinked_root", "symlinked_dir", "dot_root"])
def test_output_inside_root_refused_for_every_spelling(root, tmp_path, monkeypatch, capsys, flag, spelling):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "linkroot").symlink_to(root, target_is_directory=True)
    (outside / "lnkdir").symlink_to(root / "background", target_is_directory=True)
    root_arg = str(root)
    if spelling == "relative_cwd_in_root":
        monkeypatch.chdir(root)
        target = "rep.md"
    elif spelling == "dotdot":
        target = str(outside / ".." / "data" / "x" / ".." / "rep.md")
    elif spelling == "symlinked_root":
        target = str(outside / "linkroot" / "rep.md")
    elif spelling == "symlinked_dir":
        target = str(outside / "lnkdir" / "rep.md")
    else:  # root given as "." and the report path is relative to it
        monkeypatch.chdir(root)
        root_arg, target = ".", "./rep.md"
    before = sorted(p.as_posix() for p in root.rglob("*"))
    rc = ins.main(["--data-root", root_arg, flag, target])
    assert rc == 2
    assert "inside the data root" in capsys.readouterr().err
    assert sorted(p.as_posix() for p in root.rglob("*")) == before


def _fs_is_case_insensitive(d: Path) -> bool:
    (d / "CaseProbe").mkdir()
    try:
        return (d / "caseprobe").exists()
    finally:
        (d / "CaseProbe").rmdir()


def test_output_inside_root_refused_on_case_insensitive_filesystem(tmp_path, synthetic_dataset, capsys):
    """macOS APFS/HFS+ is case-insensitive by default: ROOT/Report.MD IS data/Report.MD when the root is data/."""
    if not _fs_is_case_insensitive(tmp_path):
        pytest.skip("filesystem is case-sensitive; this defect only exists on case-insensitive volumes (macOS)")
    root = tmp_path / "datadir"
    shutil.copytree(synthetic_dataset.root, root)
    target = tmp_path / "DATADIR" / "Report.MD"
    rc = ins.main(["--data-root", str(root), "--out", str(target)])
    assert rc == 2, "report was written INSIDE the data root through a different-case spelling"
    assert not (root / "Report.MD").exists()


# ----------------------------------------------------------------------- engine parity (require_data_complete)


def _engine_excluded(df: pd.DataFrame) -> int:
    return int((~_completed_and_applicable(df)).sum())


def test_not_usable_equals_engine_exclusions_on_fixture(root):
    df = pd.read_excel(_chars(root), sheet_name="building_characteristics")
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    assert rep["characteristics"]["resstock"]["not_usable_rows"] == _engine_excluded(df) > 0


@pytest.mark.parametrize(
    "status,applic",
    [
        ("SUCCESS", True),
        (" success ", True),
        ("Success", "TRUE"),
        ("Success", " true "),
        ("Fail", True),
        ("Success", False),
        ("Incomplete", "false"),
        ("success", "True"),
    ],
)
def test_not_usable_matches_engine_on_spelling_variants(root, status, applic):
    df = _edit_chars(root, lambda d: d.assign(completed_status=status, applicability=applic))
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    assert rep["characteristics"]["resstock"]["not_usable_rows"] == _engine_excluded(df)


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT: tool treats applicability 'yes'/'1' as applicable (_truthy); the engine accepts only 'true', so the "
    "report UNDER-counts the buildings require_data_complete would exclude",
)
@pytest.mark.parametrize("applic", ["yes", "1", "Yes"])
def test_not_usable_matches_engine_on_nonstandard_applicability(root, applic):
    df = _edit_chars(root, lambda d: d.assign(applicability=applic))
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    assert rep["characteristics"]["resstock"]["not_usable_rows"] == _engine_excluded(df) == len(df)


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT: with completed_status absent the engine raises ValueError (require_data_complete unusable) but the "
    "report says 'require_data_complete excludes nothing'",
)
def test_missing_status_column_is_not_reported_as_excludes_nothing(root):
    _edit_chars(root, lambda d: d.drop(columns=["completed_status"]).assign(applicability=True))
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    assert not any("excludes nothing" in s and "resstock" in s for s in rep["next_steps"])


@pytest.mark.xfail(
    strict=True, reason="DEFECT: duplicate bldg_id rows are not reported (ids are de-duplicated into a set)"
)
def test_duplicate_building_ids_are_reported(root):
    _edit_chars(root, lambda d: pd.concat([d, d.iloc[[0]]], ignore_index=True))
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    text = ins.render_markdown(rep).replace(str(root), "ROOT").lower()  # the root path contains the test name
    assert "duplicate" in text


# -------------------------------------------------------------------------------- honesty of PASS/FAIL lines


def _first_res_parquet(root: Path) -> Path:
    return sorted((root / RES_PART).glob("*.parquet"))[0]


def _rewrite(path: Path, fn) -> None:
    fn(pd.read_parquet(path)).to_parquet(path, index=False)


def test_pass_fail_lines_follow_the_data(root):
    rep = ins.inspect_dataset(root, expected_types=EXPECTED, sample_buildings=50)
    assert {s["total_column"]["status"] for s in rep["parquet_sample"]["resstock"]["samples"]} == {"PASS"}
    md = ins.render_markdown(rep)
    assert "- FAIL:" not in md
    _rewrite(_first_res_parquet(root), lambda d: d.rename(columns={RES_COL: "out.renamed.total"}))
    rep = ins.inspect_dataset(root, expected_types=EXPECTED, sample_buildings=50)
    statuses = [s["total_column"]["status"] for s in rep["parquet_sample"]["resstock"]["samples"]]
    assert statuses.count("FAIL") == 1 and statuses.count("PASS") == len(statuses) - 1
    md = ins.render_markdown(rep)
    assert md.count("- FAIL:") == 1 and "not found" in md.lower()


def test_report_never_claims_a_release_or_verification(root):
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    md = ins.render_markdown(rep)
    assert "verified: true" not in md and "verified: true" not in rep["catalog_stanza"]
    assert "release: unknown" in rep["catalog_stanza"]  # synthetic folder names carry no release string
    for line in rep["catalog_stanza"].splitlines():
        if line.strip().startswith(("release:", "weather:")):
            assert "[verify]" in line
    assert "evidence, not proof" in md


# ----------------------------------------------------------------------------------------- resource safety


def test_parquet_reads_only_needed_columns_and_few_files(root, monkeypatch):
    import pyarrow.parquet as pq

    calls: list[tuple[str, list[str] | None]] = []
    real = pq.ParquetFile.read

    def spy(self, columns=None, *a, **kw):
        calls.append((str(getattr(self, "_qa_path", "")), columns))
        return real(self, columns=columns, *a, **kw)  # noqa: B026

    monkeypatch.setattr(pq.ParquetFile, "read", spy)
    ins.inspect_dataset(root, expected_types=EXPECTED, sample_buildings=2)
    assert len(calls) <= 4  # 2 per product
    assert all(cols is not None and 0 < len(cols) <= 2 for _, cols in calls), calls


def test_sample_count_is_bounded_and_even(root):
    rep = ins.inspect_dataset(root, expected_types=EXPECTED, sample_buildings=3)
    for prod in ("resstock", "comstock"):
        assert len(rep["parquet_sample"][prod]["samples"]) <= 3
    assert ins._pick_evenly([str(i) for i in range(2900)], 5) == ["0", "725", "1450", "2174", "2899"]


def test_xlsx_preferred_over_csv_and_csv_listed_as_unused(root):
    """Documents current behaviour: when both exist the XLSX is read (slower) and the CSV is named as not used."""
    df = pd.read_excel(_chars(root), sheet_name="building_characteristics")
    df.to_csv(_chars(root).with_suffix(".csv"), index=False)
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    c = rep["characteristics"]["resstock"]
    assert c["source_file"].endswith(".xlsx")
    assert any(x.endswith(".csv") for x in c["other_candidates_not_used"])


# ---------------------------------------------------------------------------------------- hostile inputs


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT: one corrupt SAMPLED parquet aborts the whole run (exit 2); the rest of the report is lost",
)
def test_one_corrupt_sampled_parquet_does_not_hide_the_rest(root, tmp_path):
    _first_res_parquet(root).write_bytes(b"not parquet")
    out = tmp_path / "r.md"
    rc = ins.main(["--data-root", str(root), "--out", str(out)])
    assert rc == 0 and "Layout" in out.read_text()


def test_corrupt_and_zero_byte_inputs_exit_2_without_traceback(root, tmp_path):
    p = _first_res_parquet(root)
    p.write_bytes(b"")
    r = subprocess.run(
        [sys.executable, str(REPO / "scripts" / "inspect_dataset.py"), "--data-root", str(root)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(REPO / "src")},
    )
    assert r.returncode == 2 and "Traceback" not in r.stderr and "error:" in r.stderr


def test_directory_named_like_a_parquet_is_ignored(root):
    (root / RES_PART / "9999-0.parquet").mkdir()
    rep = ins.inspect_dataset(root, expected_types=EXPECTED)
    assert rep["characteristics"]["resstock"]["rows"] > 0


def test_root_with_spaces_and_unicode(tmp_path, synthetic_dataset, capsys):
    dst = tmp_path / "Mi carpeta é 数据"
    shutil.copytree(synthetic_dataset.root, dst)
    assert ins.main(["--data-root", str(dst), "--out", str(tmp_path / "r.md")]) == 0


def test_data_root_that_is_a_file(tmp_path, capsys):
    f = tmp_path / "afile"
    f.write_text("x")
    assert ins.main(["--data-root", str(f)]) == 2
    assert "not a directory" in capsys.readouterr().err


# --------------------------------------------------------------------------------------------- catalog_fill


def _manifest(root: Path, tmp_path: Path) -> Path:
    from microgrid.datasets import manifest as mf

    return mf.write_manifest(mf.build_manifest(root), tmp_path / "m.json")


KEY = "resstock/2025_release_1/amy2018/baseline/DC"
CATALOG = REPO / "configs" / "datasets" / "catalog.yaml"


def test_fill_refuses_in_place_through_a_symlink_and_a_dangling_output(root, tmp_path):
    from microgrid.datasets.catalog import CatalogError
    from microgrid.datasets.catalog_fill import fill_catalog

    m = _manifest(root, tmp_path)
    link = tmp_path / "link.yaml"
    link.symlink_to(CATALOG)
    before = CATALOG.read_bytes()
    with pytest.raises(CatalogError):
        fill_catalog(CATALOG, m, KEY, link)  # a symlink to the catalog is "in place"
    dangling = tmp_path / "dangling.yaml"
    dangling.symlink_to(tmp_path / "nowhere.yaml")
    with pytest.raises(CatalogError, match="already exists"):
        fill_catalog(CATALOG, m, KEY, dangling)
    assert CATALOG.read_bytes() == before and not (tmp_path / "nowhere.yaml").exists()


def test_fill_cannot_be_steered_outside_by_relative_path(root, tmp_path):
    """A catalog relative_path with '..' or an absolute path is rejected by the strict parser; nothing is written."""
    from microgrid.datasets.catalog import CatalogError
    from microgrid.datasets.catalog_fill import fill_catalog

    m = _manifest(root, tmp_path)
    for bad in ("../escape", "/etc"):
        evil = tmp_path / "evil.yaml"
        evil.write_text(CATALOG.read_text().replace("relative_path: input/resstock", f"relative_path: {bad}", 1))
        out = tmp_path / f"o{abs(hash(bad))}.yaml"
        with pytest.raises(CatalogError):
            fill_catalog(evil, m, KEY, out)
        assert not out.exists()


# ------------------------------------------------------------------------------- report-file write semantics


def test_overwrite_report_replaces_rather_than_appends(root, tmp_path):
    out = tmp_path / "r.md"
    out.write_text("OLD-CONTENT-LINE\n")
    assert ins.main(["--data-root", str(root), "--out", str(out)]) == 2  # refused without the flag
    assert out.read_text() == "OLD-CONTENT-LINE\n"
    assert ins.main(["--data-root", str(root), "--out", str(out), "--overwrite-report"]) == 0
    text = out.read_text()
    assert "OLD-CONTENT-LINE" not in text and text.startswith("# Dataset inspection report")


def test_dangling_symlink_pointing_into_root_is_refused(root, tmp_path, capsys):
    link = tmp_path / "link.md"
    link.symlink_to(root / "planted.md")  # target does not exist yet
    for extra in ([], ["--overwrite-report"]):
        assert ins.main(["--data-root", str(root), "--out", str(link), *extra]) == 2
        assert not (root / "planted.md").exists()
    capsys.readouterr()
