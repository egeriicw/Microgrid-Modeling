"""Dataset identity and catalog (FR-DATA-8)."""

from __future__ import annotations

from pathlib import Path

import pytest

from microgrid.datasets import Catalog, CatalogError, DatasetId, load_catalog, resolve_data_root

REPO = Path(__file__).parent.parent
SHIPPED = REPO / "configs" / "datasets" / "catalog.yaml"

RES_ID = DatasetId("resstock", "2025_release_1", "amy2018", "baseline", "DC")
COM_ID = DatasetId("comstock", "2025_release_3", "amy2018", "baseline", "DC")


def _entry(**over):
    e = {
        "product": "resstock",
        "release": "2025_release_1",
        "weather": "amy2018",
        "state": "DC",
        "scenario_key": "baseline",
        "partition_id": 0,
        "name": "Baseline",
        "scenario_family": "Baseline",
        "load_basis": "gross",
        "license": "CC-BY-4.0 [verify]",
        "relative_path": "input/resstock/timeseries_individual_buildings/upgrade=0/state=DC",
        "verified": False,
    }
    e.update(over)
    return e


def _write(tmp_path, entries, version=1, extra=None):
    import yaml

    doc = {"version": version, "datasets": entries}
    doc.update(extra or {})
    p = tmp_path / "catalog.yaml"
    p.write_text(yaml.safe_dump(doc))
    return p


def test_shipped_catalog_has_only_unverified_mvp_baselines():
    cat = load_catalog(SHIPPED)
    assert {e.dataset_id for e in cat.entries} == {RES_ID, COM_ID}
    for e in cat.entries:
        assert e.partition_id == "0"
        assert e.verified is False
        assert e.load_basis == "gross"
        assert e.license


def test_lookup_and_missing_id_is_loud(tmp_path):
    cat = load_catalog(_write(tmp_path, [_entry()]))
    assert cat.get(RES_ID).name == "Baseline"
    with pytest.raises(CatalogError, match="not in catalog"):
        cat.get(COM_ID)


def test_two_scenarios_same_state_and_comstock_string_partition(tmp_path):
    entries = [
        _entry(),
        _entry(
            product="comstock",
            release="2025_release_3",
            scenario_key="hvac_0005",
            partition_id="hvac_0005",
            name="Heat pump boilers",
            scenario_family="HVAC",
            load_basis="net",
            relative_path="input/comstock/timeseries_individual_buildings/upgrade=hvac_0005/state=DC",
        ),
        _entry(
            product="comstock",
            release="2025_release_3",
            scenario_key="baseline",
            relative_path="input/comstock/timeseries_individual_buildings/upgrade=0/state=DC",
        ),
    ]
    cat = load_catalog(_write(tmp_path, entries))
    got = cat.for_state("comstock", "2025_release_3", "amy2018", "DC")
    assert {e.scenario_key for e in got} == {"baseline", "hvac_0005"}
    e = cat.get(DatasetId("comstock", "2025_release_3", "amy2018", "hvac_0005", "DC"))
    assert e.partition_id == "hvac_0005" and e.load_basis == "net"


def test_resolve_path_under_data_root(tmp_path):
    cat = load_catalog(_write(tmp_path, [_entry()]))
    root = tmp_path / "root"
    p = cat.get(RES_ID).resolve(root)
    assert p == root / "input/resstock/timeseries_individual_buildings/upgrade=0/state=DC"


@pytest.mark.parametrize(
    "mutation, message",
    [
        ({"bogus": 1}, "unknown key"),
        ({"load_basis": "maybe"}, "load_basis"),
        ({"load_basis": None}, "load_basis"),
        ({"product": "sidewalkstock"}, "product"),
        ({"relative_path": "/abs/path"}, "relative"),
        ({"relative_path": "../escape"}, "relative"),
        ({"name": ""}, "name"),
    ],
)
def test_strict_entry_validation(tmp_path, mutation, message):
    with pytest.raises(CatalogError, match=message):
        load_catalog(_write(tmp_path, [_entry(**mutation)]))


def test_missing_required_key(tmp_path):
    e = _entry()
    del e["load_basis"]
    with pytest.raises(CatalogError, match="load_basis"):
        load_catalog(_write(tmp_path, [e]))


def test_duplicate_id_and_duplicate_partition_rejected(tmp_path):
    with pytest.raises(CatalogError, match="duplicate dataset id"):
        load_catalog(_write(tmp_path, [_entry(), _entry(partition_id=1)]))
    with pytest.raises(CatalogError, match="duplicate partition"):
        load_catalog(_write(tmp_path, [_entry(), _entry(scenario_key="other", relative_path="x")]))


def test_unknown_top_level_key_and_bad_version(tmp_path):
    with pytest.raises(CatalogError, match="unknown key"):
        load_catalog(_write(tmp_path, [_entry()], extra={"oops": 1}))
    with pytest.raises(CatalogError, match="version"):
        load_catalog(_write(tmp_path, [_entry()], version=99))


def test_catalog_is_a_catalog_instance(tmp_path):
    assert isinstance(load_catalog(_write(tmp_path, [_entry()])), Catalog)


class TestDataRoot:
    def test_cli_wins_over_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MICROGRID_DATA_DIR", str(tmp_path / "env"))
        assert resolve_data_root(str(tmp_path / "cli")) == tmp_path / "cli"

    def test_env_used(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MICROGRID_DATA_DIR", str(tmp_path / "env"))
        assert resolve_data_root(None) == tmp_path / "env"

    def test_neither_is_an_error_not_a_default(self, monkeypatch):
        monkeypatch.delenv("MICROGRID_DATA_DIR", raising=False)
        with pytest.raises(CatalogError, match="MICROGRID_DATA_DIR"):
            resolve_data_root(None)
