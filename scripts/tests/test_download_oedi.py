"""FR-D3 — OEDI input fetcher. RED placeholder; transport is always mocked
(NFR-12: no network in tests). See docs/infrastructure.md §7.
"""
from __future__ import annotations

from pathlib import Path

import pytest

dl = pytest.importorskip(
    "scripts.download_oedi", reason="FR-D3: download_oedi not implemented"
)

pytestmark = pytest.mark.xfail(reason="FR-D3 not implemented", strict=False)


@pytest.fixture
def manifest(tmp_path: Path) -> Path:
    p = tmp_path / "oedi_manifest.yaml"
    p.write_text(
        """
datasets:
  - name: resstock
    release: "2024.2/resstock_amy2018_release_2"
    upgrade: "0"
    state: DC
    transport: s3
    building_ids: [204, 296]
    target: "input/resstock/timeseries_individual_buildings/upgrade=0/state=DC"
characteristics:
  - source: resstock
    target: "background/DC_upgrade0.xlsx"
"""
    )
    return p


def test_dry_run_prints_plan_and_writes_nothing(manifest, tmp_path, capsys):
    dl.main(["--manifest", str(manifest), "--root", str(tmp_path), "--dry-run"])
    out = capsys.readouterr().out
    assert "204-0.parquet" in out and "296-0.parquet" in out
    assert not list((tmp_path / "input").rglob("*.parquet"))


def test_fetch_invokes_transport_per_building(manifest, tmp_path, monkeypatch):
    calls: list[tuple[str, str]] = []

    def fake_transport(src: str, dst: str) -> None:
        calls.append((src, dst))
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        Path(dst).write_bytes(b"PAR1")

    monkeypatch.setattr(dl, "s3_fetch", fake_transport)
    dl.main(["--manifest", str(manifest), "--root", str(tmp_path)])

    assert len(calls) == 2
    assert (tmp_path / "input/resstock/timeseries_individual_buildings/upgrade=0/state=DC/204-0.parquet").exists()


def test_idempotent_skips_existing(manifest, tmp_path, monkeypatch):
    seen = {"n": 0}
    monkeypatch.setattr(dl, "s3_fetch", lambda s, d: seen.__setitem__("n", seen["n"] + 1) or Path(d).write_bytes(b"x"))
    dl.main(["--manifest", str(manifest), "--root", str(tmp_path)])
    first = seen["n"]
    dl.main(["--manifest", str(manifest), "--root", str(tmp_path)])
    assert seen["n"] == first  # second run downloaded nothing
