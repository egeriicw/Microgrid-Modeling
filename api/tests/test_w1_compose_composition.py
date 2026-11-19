"""W1-COMPOSE (C): `POST /runs` with a client-friendly `composition`; the server builds the engine scenario.

The client names building types from the registry with a fixed count or a fraction range (FR-ENG-2: fixed counts,
fraction ranges, or a hybrid) plus run settings. It never sends filters, columns or paths: the server fills the
registry's default filters (``registry.with_default_filters``) and the server-built `data` section. Exactly one of
`config`, `composition_id`, `composition`. Demo may not send a composition.
"""

from __future__ import annotations

import copy
import json
import uuid
from typing import Any

import pytest
from fastapi.testclient import TestClient
from microgrid.community_load.registry import default_registry
from microgrid.community_load.synthetic import ensemble as engine_ensemble
from microgrid.community_load.synthetic.config import config_from_dict, config_to_dict

from microgrid_api import worker
from microgrid_api.db.models import Run
from microgrid_api.main import create_app

from .http_support import bearer, client_config, make_settings

SEEDS = {"composition_seed": 101, "archetype_seed": 202, "perturbation_seed": 303}

# Building types the synthetic fixture has buildings for: sf_detached, mf_small, mf_mid (ResStock);
# commercial_private, institutional_school (ComStock).
FR_ENG_2 = {
    "name": "Mixed block",
    "total_buildings": 10,
    "building_types": [
        {"building_type": "sf_detached", "fixed_count": 2},
        {"building_type": "commercial_private", "fixed_count": 1},
        {"building_type": "institutional_school", "fixed_count": 1},
        {"building_type": "mf_small", "min_fraction": 0.3, "max_fraction": 0.8},
        {"building_type": "mf_mid", "min_fraction": 0.2, "max_fraction": 0.7},
    ],
    "max_runs": 4,
}


@pytest.fixture
def http_settings(tmp_path, synthetic_dataset, tiny_config, monkeypatch):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, max_total_buildings=200, max_ensemble_runs=50)
    monkeypatch.setattr(worker, "_settings_override", s)
    return s


@pytest.fixture
def api(http_settings):
    app = create_app(http_settings)
    app.state.enqueued = []
    app.state.enqueue = lambda run_id: app.state.enqueued.append(str(run_id))
    with TestClient(app) as c:
        yield c


def _post(api: TestClient, body: dict[str, Any], who: str = "alice"):
    return api.post("/api/v1/runs", headers=bearer(who), json=body)


def _snapshot(api: TestClient, rid: str) -> dict[str, Any]:
    with api.app.state.session_factory() as s:
        row = s.get(Run, uuid.UUID(rid))
        assert row is not None
        return copy.deepcopy(row.composition_snapshot)


def _rows(api: TestClient) -> int:
    with api.app.state.session_factory() as s:
        return s.query(Run).count()


# --- FR-ENG-2 acceptance, end to end -------------------------------------------------------------------------------


def test_fixed_plus_ranged_composition_runs_and_counts_sum_to_total(api, http_settings, monkeypatch):
    """PRD FR-ENG-2 AC: 3 fixed counts + 2 ranged types give integer counts summing exactly to total_buildings,
    executed on the synthetic fixture with the real worker function."""
    seen: list[dict[str, int]] = []
    real = engine_ensemble.sample_composition

    def spy(*args: Any, **kwargs: Any) -> Any:
        comp = real(*args, **kwargs)
        seen.append(dict(comp.counts))
        return comp

    monkeypatch.setattr(engine_ensemble, "sample_composition", spy)
    r = _post(api, {"composition": FR_ENG_2, "seeds": SEEDS})
    assert r.status_code == 202, r.text
    rid = r.json()["id"]
    worker.execute_run(rid, api.app.state.session_factory)
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("alice")).json()
    assert detail["status"] == "succeeded", detail["error_message"]
    assert len(seen) >= 4
    for counts in seen:
        assert all(isinstance(v, int) and v >= 0 for v in counts.values()), counts
        assert sum(counts.values()) == 10, counts
        assert counts["sf_detached"] == 2 and counts["commercial_private"] == 1
        assert counts["institutional_school"] == 1
        assert counts["mf_small"] + counts["mf_mid"] == 6
    results = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice"))
    assert results.status_code == 200
    for text in (json.dumps(detail), results.text):
        assert str(http_settings.data_dir) not in text and str(http_settings.data_dir.resolve()) not in text
        assert str(http_settings.output_dir.resolve()) not in text


def test_server_builds_the_scenario_from_registry_defaults(api):
    r = _post(api, {"composition": FR_ENG_2, "seeds": SEEDS})
    assert r.status_code == 202, r.text
    snap = _snapshot(api, r.json()["id"])
    reg = default_registry()
    cats = snap["composition"]["categories"]
    assert snap["scenario"]["name"] == "Mixed block"
    assert snap["composition"]["total_buildings"] == 10
    assert cats["sf_detached"]["fixed_count"] == 2 and cats["sf_detached"]["source"] == "resstock"
    assert cats["institutional_school"]["source"] == "comstock"
    assert (cats["mf_small"]["min_fraction"], cats["mf_small"]["max_fraction"]) == (0.3, 0.8)
    assert cats["mf_small"]["fixed_count"] is None
    af = snap["archetype_filters"]
    assert af["resstock"]["mf_small"]["building_type_heights"] == list(
        reg.get("mf_small").resstock_building_type_heights
    )
    assert af["comstock"]["institutional_school"]["building_types"] == ["PrimarySchool", "SecondarySchool"]
    assert af["comstock"]["institutional_school"]["include_public"] is True
    assert snap["ensemble"]["mode"] == "fixed" and snap["ensemble"]["max_runs"] == 4
    assert snap["rng"] == SEEDS
    # the stored snapshot is a complete engine scenario that round-trips
    parsed = config_from_dict(copy.deepcopy(snap), source="<test>")
    assert config_from_dict(config_to_dict(parsed), source="<test>") == parsed
    # and the client-visible snapshot has no server paths
    detail = api.get(f"/api/v1/runs/{r.json()['id']}", headers=bearer("alice")).json()
    assert "data" not in detail["composition_snapshot"]
    assert "base_dir" not in detail["composition_snapshot"]["output"]


def test_composition_run_equals_the_same_config_run(api, tiny_config):
    """The registry's default filters for mf_small / mf_mid are exactly the tiny config's filters, so with the same
    seeds a composition run and a config run compute the same numbers."""
    comp = {
        "name": "tiny",
        "total_buildings": 4,
        "building_types": [
            {"building_type": "mf_small", "min_fraction": 0.5, "max_fraction": 1.0},
            {"building_type": "mf_mid", "min_fraction": 0.0, "max_fraction": 0.5},
        ],
        "max_runs": 6,
    }
    a = _post(api, {"composition": comp, "seeds": SEEDS}).json()["id"]
    b = _post(api, {"config": client_config(tiny_config), "seeds": SEEDS}).json()["id"]
    docs = []
    for rid in (a, b):
        worker.execute_run(rid, api.app.state.session_factory)
        doc = api.get(f"/api/v1/runs/{rid}/results", headers=bearer("alice")).json()
        docs.append({k: v for k, v in doc.items() if k != "config_snapshot"})
    assert docs[0] == docs[1]


def test_convergence_switch_selects_adaptive_mode(api):
    comp = {**FR_ENG_2, "max_runs": 30, "convergence": True, "min_runs": 5}
    r = _post(api, {"composition": comp})
    assert r.status_code == 202, r.text
    snap = _snapshot(api, r.json()["id"])
    assert snap["ensemble"]["mode"] == "adaptive"
    assert snap["ensemble"]["min_runs"] == 5 and snap["ensemble"]["max_runs"] == 30
    r = _post(api, {"composition": {**comp, "convergence_rule": "prd_default"}})
    assert r.status_code == 202, r.text
    assert _snapshot(api, r.json()["id"])["convergence"]["rule"] == "prd_default"


@pytest.mark.parametrize(
    "extra",
    [{"convergence_rule": "prd_default"}, {"min_runs": 5}, {"convergence": False, "convergence_rule": "legacy_window"}],
)
def test_convergence_settings_without_convergence_are_refused(api, extra):
    r = _post(api, {"composition": {**FR_ENG_2, **extra}})
    assert r.status_code == 422, r.text
    assert api.app.state.enqueued == []


def test_adaptive_min_runs_above_max_runs_is_422(api):
    r = _post(api, {"composition": {**FR_ENG_2, "max_runs": 4, "convergence": True, "min_runs": 10}})
    assert r.status_code == 422
    assert api.app.state.enqueued == []


# --- hostile and invalid compositions: a clear 422, nothing stored or enqueued -------------------------------------


def _entries(*items: dict[str, Any]) -> dict[str, Any]:
    return {**FR_ENG_2, "building_types": list(items)}


BAD_COMPOSITIONS = {
    "unknown type": _entries({"building_type": "spaceport", "fixed_count": 10}),
    "inactive type": _entries({"building_type": "civic", "fixed_count": 10}),
    "duplicate types": _entries(
        {"building_type": "mf_small", "min_fraction": 0.0, "max_fraction": 1.0},
        {"building_type": "mf_small", "min_fraction": 0.0, "max_fraction": 1.0},
    ),
    "max fractions below one": _entries(
        {"building_type": "mf_small", "min_fraction": 0.0, "max_fraction": 0.4},
        {"building_type": "mf_mid", "min_fraction": 0.0, "max_fraction": 0.4},
    ),
    "min fractions above one": _entries(
        {"building_type": "mf_small", "min_fraction": 0.6, "max_fraction": 1.0},
        {"building_type": "mf_mid", "min_fraction": 0.6, "max_fraction": 1.0},
    ),
    "min above max": _entries({"building_type": "mf_small", "min_fraction": 0.9, "max_fraction": 0.1}),
    "fraction above one": _entries({"building_type": "mf_small", "min_fraction": 0.0, "max_fraction": 1.5}),
    "negative fraction": _entries({"building_type": "mf_small", "min_fraction": -0.1, "max_fraction": 1.0}),
    "negative count": _entries({"building_type": "sf_detached", "fixed_count": -1}),
    "huge count": _entries({"building_type": "sf_detached", "fixed_count": 10**12}),
    "bool count": _entries({"building_type": "sf_detached", "fixed_count": True}),
    "text count": _entries({"building_type": "sf_detached", "fixed_count": "10"}),
    "fixed counts above total": _entries(
        {"building_type": "sf_detached", "fixed_count": 8},
        {"building_type": "mf_small", "fixed_count": 8},
        {"building_type": "mf_mid", "min_fraction": 0.0, "max_fraction": 1.0},
    ),
    "all fixed but not the total": _entries(
        {"building_type": "sf_detached", "fixed_count": 3},
        {"building_type": "mf_small", "fixed_count": 3},
    ),
    "both count and fraction": _entries(
        {"building_type": "sf_detached", "fixed_count": 2, "min_fraction": 0.1, "max_fraction": 1.0},
    ),
    "neither count nor fraction": _entries({"building_type": "sf_detached"}),
    "no types": _entries(),
    "path-like type key": _entries({"building_type": "../etc", "fixed_count": 10}),
    "filters smuggled in an entry": _entries(
        {"building_type": "sf_detached", "fixed_count": 10, "building_type_heights": ["Single-Family Detached"]}
    ),
    "column smuggled in an entry": _entries(
        {"building_type": "sf_detached", "fixed_count": 10, "attributes": [{"column": "in.vintage", "op": "not_null"}]}
    ),
    "path smuggled in an entry": _entries(
        {"building_type": "sf_detached", "fixed_count": 10, "resstock_chars_file": "../../etc/passwd"}
    ),
    "data section smuggled": {**FR_ENG_2, "data": {"background_dir": "/etc"}},
    "path key smuggled": {**FR_ENG_2, "background_dir": "/etc"},
    "output dir smuggled": {**FR_ENG_2, "output": {"base_dir": "/tmp/x"}},
    "unknown field": {**FR_ENG_2, "colour": "blue"},
    "filters smuggled": {**FR_ENG_2, "archetype_filters": {"resstock": {}}},
    "zero total": {**FR_ENG_2, "total_buildings": 0},
    "huge total": {**FR_ENG_2, "total_buildings": 10**15},
    "zero runs": {**FR_ENG_2, "max_runs": 0},
    "huge runs": {**FR_ENG_2, "max_runs": 10**15},
    "empty name": {**FR_ENG_2, "name": ""},
    "name with control characters": {**FR_ENG_2, "name": "a\nb"},
    "very long name": {**FR_ENG_2, "name": "x" * 500},
    "too many types": _entries(*({"building_type": f"t{i}", "fixed_count": 0} for i in range(100))),
    "missing total": {k: v for k, v in FR_ENG_2.items() if k != "total_buildings"},
    "not a mapping": ["sf_detached"],
}


@pytest.mark.parametrize("case", sorted(BAD_COMPOSITIONS))
def test_invalid_composition_is_422_and_nothing_is_enqueued(api, case):
    r = _post(api, {"composition": BAD_COMPOSITIONS[case]}, who="admin")
    assert r.status_code == 422, (case, r.text)
    assert api.app.state.enqueued == [] and _rows(api) == 0
    assert "/etc" not in r.text and "passwd" not in r.text and "Traceback" not in r.text


def test_nan_fraction_is_422(api):
    body = json.dumps(
        {"composition": _entries({"building_type": "mf_small", "min_fraction": float("nan"), "max_fraction": 1.0})}
    )
    r = api.post("/api/v1/runs", headers={**bearer("alice"), "Content-Type": "application/json"}, content=body.encode())
    assert r.status_code == 422
    assert api.app.state.enqueued == []


def test_server_caps_apply_to_compositions(api):
    over_total = {**FR_ENG_2, "total_buildings": 201}
    r = _post(api, {"composition": over_total})
    assert r.status_code == 422 and "200" in r.text
    over_runs = {**FR_ENG_2, "max_runs": 51}
    r = _post(api, {"composition": over_runs})
    assert r.status_code == 422 and "50" in r.text
    over_count = _entries({"building_type": "sf_detached", "fixed_count": 201})
    r = _post(api, {"composition": {**over_count, "total_buildings": 201}})
    assert r.status_code == 422
    assert api.app.state.enqueued == [] and _rows(api) == 0


def test_inactive_type_message_says_inactive_and_unknown_says_unknown(api):
    r = _post(api, {"composition": BAD_COMPOSITIONS["inactive type"]})
    assert "not active" in r.text
    r = _post(api, {"composition": BAD_COMPOSITIONS["unknown type"]})
    assert "unknown building type" in r.text and "spaceport" in r.text


# --- one source only; demo; permissions ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "keys",
    [(), ("config", "composition"), ("composition_id", "composition"), ("config", "composition_id", "composition")],
)
def test_exactly_one_of_config_composition_id_composition(api, tiny_config, keys):
    sources = {"config": client_config(tiny_config), "composition_id": "small-mixed", "composition": FR_ENG_2}
    r = _post(api, {k: sources[k] for k in keys})
    assert r.status_code == 422, r.text
    assert api.app.state.enqueued == []


def test_demo_may_not_send_a_composition(api):
    r = _post(api, {"composition": FR_ENG_2}, who="demo")
    assert r.status_code == 403
    assert api.app.state.enqueued == [] and _rows(api) == 0


def test_demo_still_runs_its_predefined_composition(api):
    assert _post(api, {"composition_id": "small-mixed"}, who="demo").status_code == 202


SELECTION = {"mf_small": [{"attribute": "vintage", "op": "in", "values": ["1960s", "1980s", "2000s", "2010s"]}]}


def test_filter_selections_apply_to_a_composition(api):
    r = _post(api, {"composition": FR_ENG_2, "filter_selections": SELECTION, "seeds": SEEDS}, who="admin")
    assert r.status_code == 202, r.text
    rid = r.json()["id"]
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("admin")).json()
    assert detail["effective_filters"]["resstock/mf_small"][0]["column"] == "in.vintage"
    assert detail["filter_selections"]["categories"] == SELECTION
    worker.execute_run(rid, api.app.state.session_factory)
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer("admin")).json()
    assert detail["status"] == "succeeded", detail["error_message"]


def test_filter_selections_on_a_composition_need_the_flag(api):
    r = _post(api, {"composition": FR_ENG_2, "filter_selections": SELECTION}, who="alice")
    assert r.status_code == 403
    assert api.app.state.enqueued == []


def test_filter_selections_must_name_a_type_in_the_composition(api):
    sel = {"mf_large": SELECTION["mf_small"]}
    r = _post(api, {"composition": FR_ENG_2, "filter_selections": sel}, who="admin")
    assert r.status_code == 422
    assert api.app.state.enqueued == []


def test_validation_errors_do_not_echo_the_input(api):
    r = _post(api, {"composition": {**FR_ENG_2, "name": "echo-marker-91ac" * 100}})
    assert r.status_code == 422 and "echo-marker-91ac" not in r.text


def test_convergence_rules_offered_are_the_engines():
    from typing import get_args

    from microgrid.community_load.synthetic.config import CONVERGENCE_RULES

    from microgrid_api.composition_schemas import ConvergenceRule

    assert set(get_args(ConvergenceRule)) == set(CONVERGENCE_RULES)


def test_openapi_documents_the_composition_shape():
    from microgrid_api import openapi_export

    spec = json.loads(openapi_export.render())
    props = spec["components"]["schemas"]["RunCreate"]["properties"]
    assert {"config", "composition_id", "composition"} <= set(props)
    comp = spec["components"]["schemas"]["CompositionRequest"]
    assert set(comp["required"]) >= {"name", "total_buildings", "building_types", "max_runs"}
    assert "RunDataFiles" in spec["components"]["schemas"]
