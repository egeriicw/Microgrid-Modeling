"""Scenario configs name datasets by id; the catalog turns ids into paths (FR-DATA-8, engine side)."""

from __future__ import annotations

import copy

import pytest

from microgrid.community_load.dataset_resolution import parse_dataset_id, resolve_dataset_paths
from microgrid.community_load.synthetic import run
from microgrid.community_load.synthetic.config import config_from_dict
from microgrid.datasets import CatalogError, DatasetId, parse_catalog
from tests.test_run_facade import build_tiny_config

RES = "resstock:2025_release_1:amy2018:baseline:DC"
COM = "comstock:2025_release_3:amy2018:baseline:DC"
RES_PATH = "input/resstock/timeseries_individual_buildings/upgrade=0/state=DC"
COM_PATH = "input/comstock/timeseries_individual_buildings/upgrade=0/state=DC"


def _entry(product, release, key="baseline", partition=0, path=None, basis="gross", verified=True, **over):
    e = {
        "product": product,
        "release": release,
        "weather": "amy2018",
        "state": "DC",
        "scenario_key": key,
        "partition_id": partition,
        "name": key,
        "scenario_family": "Baseline",
        "load_basis": basis,
        "license": "CC-BY-4.0",
        "relative_path": path or f"input/{product}/timeseries_individual_buildings/upgrade={partition}/state=DC",
        "verified": verified,
    }
    e.update(over)
    return e


def _catalog(*entries):
    return parse_catalog({"version": 1, "datasets": list(entries)})


@pytest.fixture
def catalog():
    return _catalog(_entry("resstock", "2025_release_1"), _entry("comstock", "2025_release_3"))


def _cfg(ds, **data):
    raw = build_tiny_config(ds)
    raw["data"] = {
        "resstock_chars_file": ds.resstock_chars.name,
        "comstock_chars_file": ds.comstock_chars.name,
        **data,
    }
    return config_from_dict(raw)


def test_parse_dataset_id_accepts_colon_and_slash_forms():
    want = DatasetId("resstock", "2025_release_1", "amy2018", "baseline", "DC")
    assert parse_dataset_id(RES) == want
    assert parse_dataset_id(str(want)) == want
    for bad in ("resstock:2025:amy2018:baseline", "a:b:c:d:e:f", "", "  :b:c:d:e"):
        with pytest.raises(ValueError, match="dataset id"):
            parse_dataset_id(bad)


def test_resolution_fills_the_path_fields(synthetic_dataset, catalog):
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES, dataset_comstock=COM)
    out = resolve_dataset_paths(cfg, catalog, synthetic_dataset.root)
    d = out.data
    assert d.resstock_timeseries_dir == str(synthetic_dataset.resstock_dir)
    assert d.comstock_timeseries_dir == str(synthetic_dataset.comstock_dir)
    assert d.background_dir == str(synthetic_dataset.background_dir)
    assert (d.upgrade, d.state) == ("0", "DC")
    assert (d.resstock_load_basis, d.comstock_load_basis) == ("gross", "gross")
    assert cfg.data.resstock_load_basis is None  # input is not mutated
    assert cfg.data.background_dir != d.background_dir


def test_a_resolved_config_runs_and_records_dataset_ids(synthetic_dataset, catalog):
    cfg = resolve_dataset_paths(
        _cfg(synthetic_dataset, dataset_resstock=RES, dataset_comstock=COM), catalog, synthetic_dataset.root
    )
    cfg.composition = config_from_dict(build_tiny_config(synthetic_dataset)).composition
    cfg.archetype_filters = config_from_dict(build_tiny_config(synthetic_dataset)).archetype_filters
    cfg.ensemble.max_runs = 3
    result = run.run_scenario(cfg)
    assert result.datasets == {
        "resstock": {"dataset_id": RES, "load_basis": "gross"},
        "comstock": {"dataset_id": COM, "load_basis": "gross"},
    }
    assert result.to_dict()["datasets"] == result.datasets
    assert result.config_snapshot["data"]["dataset_resstock"] == RES


def test_hard_coded_paths_still_work_and_record_no_datasets(synthetic_dataset):
    raw = build_tiny_config(synthetic_dataset)
    raw["ensemble"]["max_runs"] = 3
    assert run.run_scenario(raw).datasets == {}


def test_engine_refuses_ids_that_were_never_resolved(synthetic_dataset):
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES)
    with pytest.raises(ValueError, match="resolve_dataset_paths"):
        run.run_scenario(cfg)


def test_only_one_product_by_id_leaves_the_other_paths_alone(synthetic_dataset, catalog):
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES, comstock_timeseries_dir="/elsewhere")
    out = resolve_dataset_paths(cfg, catalog, synthetic_dataset.root)
    assert out.data.comstock_timeseries_dir == "/elsewhere"
    assert out.data.comstock_load_basis is None


def test_no_ids_is_an_error_not_a_noop(synthetic_dataset, catalog):
    with pytest.raises(ValueError, match="no dataset ids"):
        resolve_dataset_paths(_cfg(synthetic_dataset), catalog, synthetic_dataset.root)


def test_unknown_id_fails_loudly(synthetic_dataset, catalog):
    cfg = _cfg(synthetic_dataset, dataset_resstock="resstock:2099_release_9:amy2018:baseline:DC")
    with pytest.raises(CatalogError, match="not in catalog"):
        resolve_dataset_paths(cfg, catalog, synthetic_dataset.root)


def test_product_in_the_wrong_field_is_an_error(synthetic_dataset, catalog):
    with pytest.raises(ValueError, match="dataset_resstock"):
        config_from_dict({**build_tiny_config(synthetic_dataset), "data": {"dataset_resstock": COM}})


def test_unverified_entries_only_when_verification_is_required(synthetic_dataset):
    cat = _catalog(_entry("resstock", "2025_release_1", verified=False))
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES)
    resolve_dataset_paths(cfg, cat, synthetic_dataset.root)  # allowed by default
    with pytest.raises(ValueError, match="not verified"):
        resolve_dataset_paths(cfg, cat, synthetic_dataset.root, require_verified=True)


def test_missing_directories_and_files_fail_loudly(synthetic_dataset, catalog, tmp_path):
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES)
    with pytest.raises(ValueError, match="does not exist"):
        resolve_dataset_paths(cfg, catalog, tmp_path)  # data root without the partition
    (tmp_path / RES_PATH).mkdir(parents=True)
    with pytest.raises(ValueError, match="background"):
        resolve_dataset_paths(cfg, catalog, tmp_path)
    (tmp_path / "background").mkdir()
    with pytest.raises(ValueError, match=synthetic_dataset.resstock_chars.name):
        resolve_dataset_paths(cfg, catalog, tmp_path)


def test_gross_net_mixing_is_refused_unless_explicitly_allowed(synthetic_dataset):
    cat = _catalog(_entry("resstock", "2025_release_1"), _entry("comstock", "2025_release_3", basis="net"))
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES, dataset_comstock=COM)
    with pytest.raises(ValueError, match="gross.*net"):
        resolve_dataset_paths(cfg, cat, synthetic_dataset.root)
    out = resolve_dataset_paths(cfg, cat, synthetic_dataset.root, allow_mixed_load_basis=True)
    assert (out.data.resstock_load_basis, out.data.comstock_load_basis) == ("gross", "net")


def test_two_scenarios_for_one_state_resolve_to_different_dirs(synthetic_dataset, tmp_path):
    """FR-DATA-8 AC: the catalog describes two scenarios for the same state; ids pick the partition."""
    root = tmp_path
    (root / "background").mkdir()
    for part in (0, 5):
        (root / f"input/resstock/timeseries_individual_buildings/upgrade={part}/state=DC").mkdir(parents=True)
    for name in (synthetic_dataset.resstock_chars.name,):
        (root / "background" / name).write_bytes(b"x")
    cat = _catalog(
        _entry("resstock", "2025_release_1"),
        _entry("resstock", "2025_release_1", key="hvac_0005", partition=5),
    )
    base = resolve_dataset_paths(_cfg(synthetic_dataset, dataset_resstock=RES), cat, root)
    upg = resolve_dataset_paths(
        _cfg(synthetic_dataset, dataset_resstock="resstock:2025_release_1:amy2018:hvac_0005:DC", upgrade="5"),
        cat,
        root,
    )
    # a lone product must agree with the config's data.upgrade (see the QA-F6 tests)
    assert base.data.upgrade == "0" and upg.data.upgrade == "5"
    assert base.data.resstock_timeseries_dir == upg.data.resstock_timeseries_dir  # same parent, other partition


def test_partition_layout_mismatch_and_shared_upgrade_conflicts(synthetic_dataset):
    odd = _catalog(_entry("resstock", "2025_release_1", path="somewhere/else"))
    with pytest.raises(ValueError, match="upgrade=<partition>/state=<state>"):
        resolve_dataset_paths(_cfg(synthetic_dataset, dataset_resstock=RES), odd, synthetic_dataset.root)
    cat = _catalog(
        _entry("resstock", "2025_release_1"),
        _entry("comstock", "2025_release_3", partition=5),
    )
    with pytest.raises(ValueError, match="different partitions"):
        resolve_dataset_paths(
            _cfg(synthetic_dataset, dataset_resstock=RES, dataset_comstock=COM), cat, synthetic_dataset.root
        )


def test_malformed_ids_in_config_are_rejected_at_load(synthetic_dataset):
    raw = copy.deepcopy(build_tiny_config(synthetic_dataset))
    raw["data"] = {"dataset_resstock": "resstock:only:three"}
    with pytest.raises(ValueError, match="dataset_resstock"):
        config_from_dict(raw)
