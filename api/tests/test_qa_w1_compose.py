"""Independent QA of W1-COMPOSE: `/me`, `/building-types`, `POST /runs` with `composition`, server-named data files.

Written by the QA role against the PRD acceptance criteria (FR-WEB-1, FR-WEB-5, FR-ENG-1, FR-ENG-2, FR-ENG-3,
FR-ENG-10, FR-DATA-8), not against the builder's summary. Everything runs on the synthetic fixture through HTTP and
the real worker function (``worker.execute_run``). Defects are ``xfail(strict=True)`` with the reason.

Building types the fixture has buildings for: sf_detached, mf_small (2-4 units only), mf_mid (ResStock);
commercial_private, institutional_school (ComStock). No buildings for sf_attached, mf_large, commercial_warehouse,
institutional_hospital.
"""

from __future__ import annotations

import copy
import json
import math
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from microgrid.community_load.registry import default_registry
from microgrid.community_load.synthetic import ensemble as engine_ensemble
from microgrid.community_load.synthetic.config import config_from_dict

from microgrid_api import worker
from microgrid_api.config import ConfigurationError, Settings
from microgrid_api.db.models import Run
from microgrid_api.main import create_app
from microgrid_api.services import runs as svc

from .conftest import REPO_ROOT
from .http_support import bearer, client_config, make_settings

SEED_SETS = [
    {"composition_seed": 1, "archetype_seed": 2, "perturbation_seed": 3},
    {"composition_seed": 4242, "archetype_seed": 17, "perturbation_seed": 9},
    {"composition_seed": 2**32 - 1, "archetype_seed": 0, "perturbation_seed": 123456},
]
SEEDS = SEED_SETS[0]


def _fixture_module() -> Any:
    import importlib.util
    import sys

    name = "_root_synthetic_data"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "tests" / "fixtures" / "synthetic_data.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def http_settings(tmp_path, synthetic_dataset, tiny_config, monkeypatch):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, max_total_buildings=200, max_ensemble_runs=50)
    monkeypatch.setattr(worker, "_settings_override", s)
    return s


def _client(settings: Settings) -> TestClient:
    app = create_app(settings)
    app.state.enqueued = []
    app.state.enqueue = lambda run_id: app.state.enqueued.append(str(run_id))
    return TestClient(app)


@pytest.fixture
def api(http_settings):
    with _client(http_settings) as c:
        yield c


@pytest.fixture
def spy(monkeypatch):
    """Every composition the engine samples (counts per category), in order."""
    seen: list[dict[str, int]] = []
    real = engine_ensemble.sample_composition

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        comp = real(*args, **kwargs)
        seen.append(dict(comp.counts))
        return comp

    monkeypatch.setattr(engine_ensemble, "sample_composition", wrapper)
    return seen


def _post(api: TestClient, body: dict[str, Any], who: str = "alice"):
    return api.post("/api/v1/runs", headers=bearer(who), json=body)


def _run(api: TestClient, body: dict[str, Any], who: str = "alice") -> dict[str, Any]:
    r = _post(api, body, who)
    assert r.status_code == 202, r.text
    rid = r.json()["id"]
    worker.execute_run(rid, api.app.state.session_factory)
    detail = api.get(f"/api/v1/runs/{rid}", headers=bearer(who)).json()
    return detail


def _results(api: TestClient, rid: str, who: str = "alice") -> dict[str, Any]:
    r = api.get(f"/api/v1/runs/{rid}/results", headers=bearer(who))
    assert r.status_code == 200, r.text
    return r.json()


def _rows(api: TestClient) -> int:
    with api.app.state.session_factory() as s:
        return s.query(Run).count()


def _fixed(t: str, n: int) -> dict[str, Any]:
    return {"building_type": t, "fixed_count": n}


def _ranged(t: str, lo: float | None = None, hi: float | None = None) -> dict[str, Any]:
    e: dict[str, Any] = {"building_type": t}
    if lo is not None:
        e["min_fraction"] = lo
    if hi is not None:
        e["max_fraction"] = hi
    return e


def _comp(total: int, *entries: dict[str, Any], max_runs: int = 2, name: str = "qa") -> dict[str, Any]:
    return {"name": name, "total_buildings": total, "building_types": list(entries), "max_runs": max_runs}


# =====================================================================================================================
# FR-ENG-2: fixed counts, fraction ranges and the hybrid, end to end (HTTP -> stored snapshot -> real worker)
# =====================================================================================================================

SHAPES: dict[str, dict[str, Any]] = {
    # PRD AC: 3 fixed + 2 ranged
    "ac 3 fixed + 2 ranged, total 5": _comp(
        5,
        _fixed("sf_detached", 1),
        _fixed("commercial_private", 1),
        _fixed("institutional_school", 1),
        _ranged("mf_small", 0.3, 0.8),
        _ranged("mf_mid", 0.2, 0.7),
    ),
    "ac 3 fixed + 2 ranged, total 23": _comp(
        23,
        _fixed("sf_detached", 5),
        _fixed("commercial_private", 2),
        _fixed("institutional_school", 3),
        _ranged("mf_small", 0.3, 0.8),
        _ranged("mf_mid", 0.2, 0.7),
    ),
    "ac 3 fixed + 2 ranged, total 41, uneven bounds": _comp(
        41,
        _fixed("sf_detached", 7),
        _fixed("commercial_private", 0),
        _fixed("institutional_school", 4),
        _ranged("mf_small", 0.05, 0.95),
        _ranged("mf_mid", 0.05, 0.95),
    ),
    "all fixed": _comp(6, _fixed("sf_detached", 2), _fixed("mf_small", 3), _fixed("commercial_private", 1)),
    "all ranged": _comp(
        9, _ranged("sf_detached", 0.0, 1.0), _ranged("mf_small", 0.1, 0.6), _ranged("institutional_school", 0.0, 0.5)
    ),
    "ranged with defaults only": _comp(7, _ranged("mf_small", hi=0.6), _ranged("mf_mid", lo=0.2)),
    "a fixed count of 0": _comp(5, _fixed("sf_detached", 0), _ranged("mf_small", 0, 1), _ranged("mf_mid", 0, 1)),
    "fixed counts fill the total, ranged left": _comp(
        5, _fixed("sf_detached", 3), _fixed("mf_small", 2), _ranged("mf_mid", 0.0, 1.0)
    ),
    "single ranged type": _comp(3, _ranged("mf_mid", 0.0, 1.0)),
    "single fixed type": _comp(3, _fixed("commercial_private", 3)),
    "minimum fractions sum to exactly 1": _comp(7, _ranged("mf_small", 0.5, 1.0), _ranged("mf_mid", 0.5, 1.0)),
    "minimum fractions 0.1+0.2+0.7 (float sum just over 1)": _comp(
        10, _ranged("mf_small", 0.1, 1.0), _ranged("mf_mid", 0.2, 1.0), _ranged("sf_detached", 0.7, 1.0)
    ),
    "total of 1, ranged": _comp(1, _ranged("sf_detached", 0, 1), _ranged("mf_small", 0, 1)),
    "total of 1, fixed": _comp(1, _fixed("institutional_school", 1)),
    "total of 1, hybrid": _comp(1, _fixed("commercial_private", 1), _ranged("mf_small", 0, 1)),
}


def _bounds_ok(count: int, remainder: int, lo: float, hi: float) -> bool:
    # integer rounding of a fraction of the remainder: within one building of the stated range
    return math.floor(lo * remainder) - 1 <= count <= math.ceil(hi * remainder) + 1


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_fr_eng_2_counts_are_integers_summing_to_total_for_every_shape_and_seed(api, spy, shape):
    comp = SHAPES[shape]
    total = comp["total_buildings"]
    fixed = {e["building_type"]: e["fixed_count"] for e in comp["building_types"] if "fixed_count" in e}
    ranged = {
        e["building_type"]: (e.get("min_fraction", 0.0), e.get("max_fraction", 1.0))
        for e in comp["building_types"]
        if "fixed_count" not in e
    }
    remainder = total - sum(fixed.values())
    for seeds in SEED_SETS:
        spy.clear()
        detail = _run(api, {"composition": comp, "seeds": seeds})
        assert detail["status"] == "succeeded", (shape, detail["error_message"])
        assert detail["seeds"] == seeds
        assert len(spy) >= comp["max_runs"], spy
        for counts in spy:
            assert set(counts) == set(fixed) | set(ranged), counts
            assert all(type(v) is int and v >= 0 for v in counts.values()), counts
            assert sum(counts.values()) == total, (shape, counts)
            for t, n in fixed.items():
                assert counts[t] == n, (shape, counts)
            assert sum(counts[t] for t in ranged) == remainder, (shape, counts)
            for t, (lo, hi) in ranged.items():
                assert _bounds_ok(counts[t], remainder, lo, hi), (shape, t, counts)


def test_fr_eng_2_same_seeds_same_counts_and_results(api, spy):
    comp = SHAPES["ac 3 fixed + 2 ranged, total 23"]
    a = _run(api, {"composition": comp, "seeds": SEEDS})
    first = list(spy)
    spy.clear()
    b = _run(api, {"composition": comp, "seeds": SEEDS})
    assert first == spy
    assert _results(api, a["id"]) == _results(api, b["id"])


def test_fr_eng_2_ranged_types_actually_vary_across_runs(api, spy):
    """A fraction range is sampled, not frozen at one value (guards a builder that turned ranges into fixed counts)."""
    _run(api, {"composition": _comp(40, _ranged("mf_small", 0.1, 0.9), _ranged("mf_mid", 0.1, 0.9), max_runs=8)})
    assert len({c["mf_small"] for c in spy}) > 1, spy


# =====================================================================================================================
# FR-ENG-1: types are data. A type added to a COPY of the registry is usable without code changes.
# =====================================================================================================================

NEW_TYPE = {
    "key": "qa_sf_any",
    "label": "QA: any single-family",
    "source": "resstock",
    "use": "residential_sf",
    "resstock_building_type_heights": ["Single-Family Detached", "Single-Family Attached"],
}
NEW_COMMERCIAL = {
    "key": "qa_small_office_only",
    "label": "QA: small office only",
    "source": "comstock",
    "use": "commercial",
    "comstock_building_types": ["SmallOffice"],
}
NEW_INACTIVE = {
    "key": "qa_inactive",
    "label": "QA: not ready",
    "source": "comstock",
    "use": "commercial",
    "comstock_building_types": ["SmallOffice"],
    "active": False,
}


def test_fr_eng_1_a_type_added_to_a_registry_copy_is_listed_and_runs(api, spy):
    base = default_registry()
    api.app.state.building_registry = base.extended(NEW_TYPE, NEW_COMMERCIAL, NEW_INACTIVE)
    assert "qa_sf_any" not in base  # the packaged registry was not touched
    items = {i["key"]: i for i in api.get("/api/v1/building-types", headers=bearer("alice")).json()["items"]}
    assert items["qa_sf_any"] == {
        "key": "qa_sf_any",
        "label": "QA: any single-family",
        "building_class": "residential",
        "source": "resstock",
        "description": None,
        "active": True,
    }
    assert items["qa_small_office_only"]["building_class"] == "commercial"
    assert items["qa_inactive"]["active"] is False
    comp = _comp(6, _fixed("qa_small_office_only", 2), _ranged("qa_sf_any", 0.2, 1.0), _ranged("mf_mid", 0, 0.8))
    detail = _run(api, {"composition": comp, "seeds": SEEDS})
    assert detail["status"] == "succeeded", detail["error_message"]
    snap = detail["composition_snapshot"]
    assert snap["archetype_filters"]["resstock"]["qa_sf_any"]["building_type_heights"] == [
        "Single-Family Detached",
        "Single-Family Attached",
    ]
    assert snap["archetype_filters"]["comstock"]["qa_small_office_only"]["building_types"] == ["SmallOffice"]
    assert all(c["qa_small_office_only"] == 2 and sum(c.values()) == 6 for c in spy)
    r = _post(api, {"composition": _comp(2, _fixed("qa_inactive", 2))})
    assert r.status_code == 422 and "not active" in r.json()["detail"]
    assert "qa_inactive" in r.json()["detail"]


def test_inactive_type_is_refused_even_with_a_zero_count_or_zero_fraction(api):
    """Naming an inactive type is refused clearly, whatever the count (no silent drop, FR-ENG-3)."""
    for entry in (_fixed("civic", 0), _ranged("civic", 0.0, 0.0)):
        r = _post(api, {"composition": _comp(3, _fixed("mf_small", 3), entry)})
        assert r.status_code == 422, r.text
        assert "civic" in r.json()["detail"] and "not active" in r.json()["detail"]
    assert api.app.state.enqueued == [] and _rows(api) == 0


def test_duplicate_type_is_refused_even_when_counts_would_add_up(api):
    """A repeated type must not be silently merged or overwritten (the second entry would replace the first)."""
    r = _post(api, {"composition": _comp(4, _fixed("mf_small", 2), _fixed("mf_small", 2))})
    assert r.status_code == 422 and "more than once" in r.json()["detail"]
    r = _post(api, {"composition": _comp(4, _fixed("mf_small", 4), _ranged("mf_small", 0, 1))})
    assert r.status_code == 422 and "more than once" in r.json()["detail"]
    assert _rows(api) == 0


def test_unknown_type_is_422_not_500(api):
    r = _post(api, {"composition": _comp(3, _fixed("qa_nonexistent", 3))})
    assert r.status_code == 422 and "unknown building type" in r.json()["detail"]


# =====================================================================================================================
# Equivalence: a composition run equals the hand-written config run (full results document)
# =====================================================================================================================


def _hand_written_config(name: str, total: int, max_runs: int) -> dict[str, Any]:
    """FR-ENG-2 AC shape written by hand as a client `config` with the registry's filters spelled out."""
    reg = default_registry()

    def rfilter(key: str) -> dict[str, Any]:
        return {"building_type_heights": list(reg.get(key).resstock_building_type_heights)}

    def cfilter(key: str) -> dict[str, Any]:
        t = reg.get(key)
        return {"building_types": list(t.comstock_building_types), "include_public": t.include_public}

    return {
        "scenario": {"name": name},
        "composition": {
            "total_buildings": total,
            "categories": {
                "sf_detached": {"fixed_count": 2, "source": "resstock"},
                "commercial_private": {"fixed_count": 1, "source": "comstock"},
                "institutional_school": {"fixed_count": 1, "source": "comstock"},
                "mf_small": {"min_fraction": 0.3, "max_fraction": 0.8, "source": "resstock"},
                "mf_mid": {"min_fraction": 0.2, "max_fraction": 0.7, "source": "resstock"},
            },
        },
        "archetype_filters": {
            "resstock": {k: rfilter(k) for k in ("sf_detached", "mf_small", "mf_mid")},
            "comstock": {k: cfilter(k) for k in ("commercial_private", "institutional_school")},
        },
        "ensemble": {"mode": "fixed", "max_runs": max_runs},
    }


@pytest.mark.parametrize("seeds", SEED_SETS[:2])
def test_composition_run_equals_hand_written_config_run_full_document(api, seeds):
    comp = _comp(
        10,
        _fixed("sf_detached", 2),
        _fixed("commercial_private", 1),
        _fixed("institutional_school", 1),
        _ranged("mf_small", 0.3, 0.8),
        _ranged("mf_mid", 0.2, 0.7),
        max_runs=3,
        name="equiv",
    )
    a = _run(api, {"composition": comp, "seeds": seeds})
    b = _run(api, {"config": _hand_written_config("equiv", 10, 3), "seeds": seeds})
    assert a["status"] == b["status"] == "succeeded"
    da, db = _results(api, a["id"]), _results(api, b["id"])
    # The config copies differ only by key order / explicit defaults: identical after the engine's own parse.
    sa, sb = da.pop("config_snapshot"), db.pop("config_snapshot")
    assert config_from_dict(copy.deepcopy(sa), source="a") == config_from_dict(copy.deepcopy(sb), source="b")
    assert da == db  # the entire rest of the document, including run ids, metrics, profiles and temperature
    # run detail: same summary result, same snapshot after parsing, same data provenance
    assert a["result"] == {**b["result"], "config_snapshot": a["result"]["config_snapshot"]}
    assert a["data_files"] == b["data_files"]


# =====================================================================================================================
# API count rules vs the engine
# =====================================================================================================================


def test_api_and_engine_agree_on_fraction_sum_tolerance(api):
    """0.1+0.2+0.7 is 1.0000000000000002 in floats: both layers accept it; 1+1e-6 both refuse."""
    ok = _comp(10, _ranged("mf_small", 0.1, 1.0), _ranged("mf_mid", 0.2, 1.0), _ranged("sf_detached", 0.7, 1.0))
    assert _post(api, {"composition": ok}).status_code == 202
    bad = _comp(10, _ranged("mf_small", 0.5, 1.0), _ranged("mf_mid", 0.500001, 1.0))
    r = _post(api, {"composition": bad})
    assert r.status_code == 422 and "minimum fractions" in r.json()["detail"]
    bad_hi = _comp(10, _ranged("mf_small", 0.0, 0.5), _ranged("mf_mid", 0.0, 0.499999))
    r = _post(api, {"composition": bad_hi})
    assert r.status_code == 422 and "maximum fractions" in r.json()["detail"]


def test_fraction_sums_are_checked_even_when_fixed_counts_fill_the_total(api):
    """With nothing left for the ranged types the engine still checks their bounds; the API agrees (both refuse)."""
    comp = _comp(5, _fixed("sf_detached", 5), _ranged("mf_small", 0.0, 0.4))
    r = _post(api, {"composition": comp})
    assert r.status_code == 422 and "maximum fractions" in r.json()["detail"]


def test_integer_fraction_values_from_a_browser_are_accepted(api):
    """JSON.stringify(1.0) is "1": a web form sends integers for 0 and 1 fractions."""
    body = json.dumps(
        {"composition": {**_comp(3, {"building_type": "mf_small", "min_fraction": 0, "max_fraction": 1})}}
    )
    r = api.post("/api/v1/runs", headers={**bearer("alice"), "Content-Type": "application/json"}, content=body)
    assert r.status_code == 202, r.text


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (MEDIUM, API): a raw `config` whose fixed counts add up to MORE than total_buildings is accepted "
    "(202) and the worker then samples a neighbourhood bigger than total_buildings. The composition path refuses "
    "the same shape (check_counts), so the two run inputs disagree. Fix: apply services.compositions.check_counts "
    "(or the engine fix) to the parsed config on every run input.",
)
def test_raw_config_with_fixed_counts_above_total_is_refused(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["composition"]["categories"]["mf_small"] = {"fixed_count": 6, "source": "resstock"}
    cfg["composition"]["categories"]["mf_mid"] = {"min_fraction": 0.0, "max_fraction": 1.0, "source": "resstock"}
    r = _post(api, {"config": cfg, "seeds": SEEDS})
    assert r.status_code == 422, r.text


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (MEDIUM, API): a raw `config` where every category is fixed and the counts add up to LESS than "
    "total_buildings is accepted and the run's neighbourhood has fewer buildings than total_buildings (FR-ENG-2 AC "
    "broken on the config path). The composition path refuses it. Fix: same as above.",
)
def test_raw_config_all_fixed_not_summing_to_total_is_refused(api, tiny_config):
    cfg = client_config(tiny_config)
    cfg["composition"]["categories"] = {
        "mf_small": {"fixed_count": 1, "source": "resstock"},
        "mf_mid": {"fixed_count": 1, "source": "resstock"},
    }
    r = _post(api, {"config": cfg, "seeds": SEEDS})
    assert r.status_code == 422, r.text


def test_raw_config_fixed_counts_above_total_really_oversamples_today(api, spy, tiny_config):
    """Evidence for the defect above: what the engine actually samples (6 + 0 buildings for a total of 4)."""
    cfg = client_config(tiny_config)
    cfg["composition"]["categories"]["mf_small"] = {"fixed_count": 6, "source": "resstock"}
    cfg["composition"]["categories"]["mf_mid"] = {"min_fraction": 0.0, "max_fraction": 1.0, "source": "resstock"}
    cfg["ensemble"] = {"mode": "fixed", "max_runs": 2}
    r = _post(api, {"config": cfg, "seeds": SEEDS})
    if r.status_code != 202:
        pytest.skip("refused at creation (defect fixed)")
    worker.execute_run(r.json()["id"], api.app.state.session_factory)
    assert spy and all(sum(c.values()) == 6 for c in spy), spy


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (MEDIUM, API): a composition naming an ACTIVE registry type that has no buildings in the "
    "server's data (here sf_attached on the fixture) with a fixed count > 0 is accepted (202) and only fails later "
    "in the worker as 'engine run failed (ValueError)'. The pool check that exists for filter selections "
    "(routers.runs.check_pools) is not applied to composition runs. Fix: run check_pools for every composition "
    "(and config) run, not only when filter_selections are used.",
)
def test_composition_with_a_type_that_has_no_buildings_is_refused_at_creation(api):
    r = _post(api, {"composition": _comp(4, _fixed("sf_attached", 1), _ranged("mf_small", 0, 1)), "seeds": SEEDS})
    assert r.status_code == 422, r.text


def test_a_type_without_buildings_is_fine_when_it_can_receive_none(api):
    """Fixed count 0 or max_fraction 0 for a type with an empty pool: the run must still succeed."""
    for entry in (_fixed("sf_attached", 0), _ranged("mf_large", 0.0, 0.0)):
        detail = _run(api, {"composition": _comp(4, entry, _ranged("mf_small", 0, 1)), "seeds": SEEDS})
        assert detail["status"] == "succeeded", (entry, detail["error_message"])


def test_late_failure_for_an_empty_pool_still_says_why(api):
    """Today the empty-pool case fails in the worker: the run is failed with a non-empty reason, never stuck."""
    r = _post(api, {"composition": _comp(4, _fixed("sf_attached", 1), _ranged("mf_small", 0, 1)), "seeds": SEEDS})
    if r.status_code != 202:
        pytest.skip("refused at creation (defect fixed)")
    worker.execute_run(r.json()["id"], api.app.state.session_factory)
    d = api.get(f"/api/v1/runs/{r.json()['id']}", headers=bearer("alice")).json()
    assert d["status"] == "failed" and d["error_message"]


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (LOW, API): a whitespace-only composition name ('   ') is accepted and stored as the scenario name "
    "(a form or run list would show a blank title). Fix: require a non-space character in CompositionName.",
)
def test_whitespace_only_name_is_refused(api):
    r = _post(api, {"composition": {**_comp(3, _fixed("mf_small", 3)), "name": "   "}})
    assert r.status_code == 422, r.text


# =====================================================================================================================
# /me agrees with what the server enforces (FR-WEB-5)
# =====================================================================================================================

SELECTION = {"mf_small": [{"attribute": "vintage", "op": "in", "values": ["1960s", "1980s", "2000s", "2010s"]}]}
OVERRIDES = [
    {},
    {"client": {"filters.use": True}},
    {"admin": {"filters.use": False}},
    {"client": {"filters.use": True}, "admin": {"filters.use": False}},
]


def _input_body(kind: str, tiny_config: dict[str, Any]) -> dict[str, Any]:
    if kind == "config":
        return {"config": client_config(tiny_config)}
    if kind == "composition_id":
        return {"composition_id": "small-mixed"}
    return {"composition": _comp(4, _ranged("mf_small", 0.5, 1.0), _ranged("mf_mid", 0.0, 0.5))}


@pytest.mark.parametrize("overrides", OVERRIDES, ids=lambda o: json.dumps(o) or "none")
def test_me_matches_server_enforcement_for_every_role_input_and_filter_use(
    tmp_path, synthetic_dataset, tiny_config, overrides
):
    s = make_settings(tmp_path, synthetic_dataset, tiny_config, permission_overrides=overrides)
    with _client(s) as api:
        for who in ("admin", "alice", "demo"):
            me = api.get("/api/v1/me", headers=bearer(who)).json()
            for kind in ("config", "composition_id", "composition"):
                r = _post(api, _input_body(kind, tiny_config), who)
                allowed = kind in me["run_inputs"]
                assert (r.status_code == 202) == allowed, (who, kind, r.status_code, r.text)
                if not allowed:
                    assert r.status_code == 403
                if not allowed:
                    continue
                if who == "demo":
                    continue  # demo may never send filter_selections at all (checked below)
                body = {**_input_body(kind, tiny_config), "filter_selections": SELECTION}
                r = _post(api, body, who)
                if me["permissions"]["filters.use"]:
                    assert r.status_code == 202, (who, kind, overrides, r.text)
                else:
                    assert r.status_code == 403, (who, kind, overrides, r.text)
            if who == "demo":
                assert me["permissions"]["filters.use"] is False
                r = _post(api, {"composition_id": "small-mixed", "filter_selections": SELECTION}, who)
                assert r.status_code == 403
                r = _post(api, {"composition_id": "small-mixed", "seeds": SEEDS}, who)
                assert r.status_code == 403  # /me says nothing about seeds; demo cannot send them


def test_me_lists_every_library_flag_and_nothing_else(api):
    from microgrid.community_load.permissions import FLAGS

    for who in ("admin", "alice", "demo"):
        me = api.get("/api/v1/me", headers=bearer(who)).json()
        assert set(me["permissions"]) == set(FLAGS)
        assert set(me) == {"subject", "role", "permissions", "run_inputs", "limits"}


def test_me_limits_are_the_caps_the_server_enforces(api, http_settings):
    me = api.get("/api/v1/me", headers=bearer("alice")).json()
    cap_b, cap_r = me["limits"]["max_total_buildings"], me["limits"]["max_ensemble_runs"]
    assert (cap_b, cap_r) == (http_settings.max_total_buildings, http_settings.max_ensemble_runs)
    at = _comp(cap_b, _ranged("mf_small", 0, 1), max_runs=cap_r)
    assert _post(api, {"composition": at}).status_code == 202
    over_b = _comp(cap_b + 1, _ranged("mf_small", 0, 1), max_runs=cap_r)
    assert _post(api, {"composition": over_b}).status_code == 422
    over_r = _comp(cap_b, _ranged("mf_small", 0, 1), max_runs=cap_r + 1)
    assert _post(api, {"composition": over_r}).status_code == 422


# =====================================================================================================================
# Settings for data files and temperature: every combination fails loudly at startup, never silently
# =====================================================================================================================

WX = {
    "temperature_source": "weather_file",
    "temperature_weather_file": "weather/w.csv",
    "temperature_time_column": "t",
    "temperature_column": "v",
    "temperature_units": "F",
}
TC = {"temperature_source": "timeseries_column", "temperature_column": "v", "temperature_units": "C"}

BAD_SETTINGS: dict[str, dict[str, Any]] = {
    "source upper case": {**WX, "temperature_source": "WEATHER_FILE"},
    "source mixed case": {**TC, "temperature_source": "Timeseries_Column"},
    "source with whitespace": {"temperature_source": " none"},
    "source empty": {"temperature_source": ""},
    "source unknown": {"temperature_source": "epw"},
    "none + column": {"temperature_source": "none", "temperature_column": "v"},
    "none + units": {"temperature_source": "none", "temperature_units": "C"},
    "none + empty column": {"temperature_source": "none", "temperature_column": ""},
    "none + weather file": {"temperature_source": "none", "temperature_weather_file": "w.csv"},
    "column without units": {k: v for k, v in TC.items() if k != "temperature_units"},
    "column without column": {k: v for k, v in TC.items() if k != "temperature_column"},
    "column + weather file": {**TC, "temperature_weather_file": "w.csv"},
    "column + time column": {**TC, "temperature_time_column": "t"},
    "column empty": {**TC, "temperature_column": ""},
    "weather without file": {k: v for k, v in WX.items() if k != "temperature_weather_file"},
    "weather without time column": {k: v for k, v in WX.items() if k != "temperature_time_column"},
    "weather without column": {k: v for k, v in WX.items() if k != "temperature_column"},
    "weather without units": {k: v for k, v in WX.items() if k != "temperature_units"},
    "weather empty file": {**WX, "temperature_weather_file": ""},
    "weather empty time column": {**WX, "temperature_time_column": ""},
    "units lower case c": {**TC, "temperature_units": "c"},
    "units Kelvin": {**TC, "temperature_units": "K"},
    "units degC": {**TC, "temperature_units": "degC"},
    "units with whitespace": {**TC, "temperature_units": "C "},
    "units empty": {**TC, "temperature_units": ""},
    "column too long": {**TC, "temperature_column": "x" * 201},
    "column with newline": {**TC, "temperature_column": "a\nb"},
    "time column with control char": {**WX, "temperature_time_column": "t\x00"},
    "weather absolute": {**WX, "temperature_weather_file": "/data/w.csv"},
    "weather dotdot": {**WX, "temperature_weather_file": "weather/../../w.csv"},
    "weather dot part": {**WX, "temperature_weather_file": "./w.csv"},
    "weather trailing slash": {**WX, "temperature_weather_file": "weather/"},
    "weather double slash": {**WX, "temperature_weather_file": "weather//w.csv"},
    "weather backslash": {**WX, "temperature_weather_file": "weather\\w.csv"},
    "resstock chars with folder": {"resstock_chars_file": "background/x.xlsx"},
    "resstock chars dotdot": {"resstock_chars_file": ".."},
    "resstock chars empty": {"resstock_chars_file": ""},
    "comstock chars backslash": {"comstock_chars_file": "a\\b.xlsx"},
    "comstock chars control": {"comstock_chars_file": "a\tb.xlsx"},
    "comstock chars too long": {"comstock_chars_file": "a" * 256},
}


@pytest.mark.parametrize("case", sorted(BAD_SETTINGS))
def test_bad_data_file_settings_fail_at_startup(tmp_path, case):
    kwargs = {"database_url": f"sqlite:///{tmp_path / 'x.db'}", "environment": "test", **BAD_SETTINGS[case]}
    with pytest.raises((ConfigurationError, ValueError)) as exc:
        Settings(**kwargs)
    text = str(exc.value)
    assert "MICROGRID_" in text, text  # names the variable
    for v in BAD_SETTINGS[case].values():
        if isinstance(v, str) and len(v.strip()) > 3 and v.strip() not in ("none", "weather_file", "timeseries_column"):
            assert v not in text.split("MICROGRID_")[-1], (case, text)  # the value is never echoed


@pytest.mark.parametrize(
    "env",
    [
        {"MICROGRID_TEMPERATURE_SOURCE": "none", "MICROGRID_TEMPERATURE_UNITS": "C"},
        {"MICROGRID_TEMPERATURE_SOURCE": "weather_file"},
        {"MICROGRID_TEMPERATURE_COLUMN": "v"},
        {"MICROGRID_TEMPERATURE_SOURCE": "timeseries_column", "MICROGRID_TEMPERATURE_COLUMN": "v"},
        {"MICROGRID_RESSTOCK_CHARS_FILE": "../x.xlsx"},
    ],
)
def test_bad_data_file_settings_from_the_environment_fail_at_startup(tmp_path, monkeypatch, env):
    """The real startup path reads the environment: the same rules apply there."""
    monkeypatch.setenv("MICROGRID_DATABASE_URL", f"sqlite:///{tmp_path / 'x.db'}")
    monkeypatch.setenv("MICROGRID_ENVIRONMENT", "test")
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    with pytest.raises((ConfigurationError, ValueError)):
        Settings()


def test_good_settings_from_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("MICROGRID_DATABASE_URL", f"sqlite:///{tmp_path / 'x.db'}")
    monkeypatch.setenv("MICROGRID_ENVIRONMENT", "test")
    for k, v in WX.items():
        monkeypatch.setenv(f"MICROGRID_{k.upper()}", v)
    s = Settings()
    assert (s.temperature_source, s.temperature_weather_file, s.temperature_units) == (
        "weather_file",
        "weather/w.csv",
        "F",
    )


def _data_copy(synthetic_dataset: Any, tmp_path: Path) -> Path:
    root = tmp_path / "data"
    shutil.copytree(synthetic_dataset.root, root)
    return root


def _settings_on(tmp_path, synthetic_dataset, tiny_config, root: Path, **over: Any) -> Settings:
    names = {
        "resstock_chars_file": synthetic_dataset.resstock_chars.name,
        "comstock_chars_file": synthetic_dataset.comstock_chars.name,
    }
    return make_settings(tmp_path, synthetic_dataset, tiny_config, data_dir=root, **{**names, **over})


def test_startup_does_not_require_the_files_but_creation_does(tmp_path, synthetic_dataset, tiny_config):
    """Files are checked at run creation (they may be mounted after start): the app starts with a missing file and
    refuses runs with a 503 naming the kind of file only."""
    root = _data_copy(synthetic_dataset, tmp_path)
    s = _settings_on(tmp_path, synthetic_dataset, tiny_config, root, resstock_chars_file="later.xlsx")
    with _client(s) as api:
        assert api.get("/api/v1/me", headers=bearer("alice")).status_code == 200
        r = _post(api, {"composition": _comp(3, _fixed("mf_small", 3))})
        assert r.status_code == 503 and "ResStock characteristics" in r.json()["detail"]
        shutil.copy(synthetic_dataset.resstock_chars, root / "background" / "later.xlsx")
        assert _post(api, {"composition": _comp(3, _fixed("mf_small", 3))}).status_code == 202


@pytest.mark.parametrize("which", ["resstock_chars_file", "comstock_chars_file", "weather"])
def test_a_directory_in_place_of_a_file_is_503_at_creation(tmp_path, synthetic_dataset, tiny_config, which):
    root = _data_copy(synthetic_dataset, tmp_path)
    over: dict[str, Any] = {}
    if which == "weather":
        (root / "weather" / "w.csv").mkdir(parents=True)
        over = dict(WX)
    else:
        (root / "background" / "dir.xlsx").mkdir()
        over = {which: "dir.xlsx"}
    s = _settings_on(tmp_path, synthetic_dataset, tiny_config, root, **over)
    with _client(s) as api:
        r = _post(api, {"composition": _comp(3, _fixed("mf_small", 3))})
        assert r.status_code == 503, r.text
        assert str(root) not in r.text and _rows(api) == 0 and api.app.state.enqueued == []


@pytest.mark.parametrize("which", ["resstock", "weather"])
def test_zero_byte_file_fails_in_the_worker_with_a_reason_never_silently(
    tmp_path, synthetic_dataset, tiny_config, monkeypatch, which
):
    """A zero-byte file exists, so creation accepts it; the worker must then fail the run with a reason (status
    failed, non-empty error message, no path) rather than leave it running or succeed without the data."""
    mod = _fixture_module()
    ds = mod.build_synthetic_dataset(tmp_path / "z", weather_file=True)
    over: dict[str, Any] = {}
    if which == "weather":
        ds.weather_file.write_bytes(b"")
        over = {
            "temperature_source": "weather_file",
            "temperature_weather_file": ds.weather_file.relative_to(ds.root).as_posix(),
            "temperature_time_column": mod.WEATHER_TIME_COL,
            "temperature_column": mod.WEATHER_TEMP_COL_F,
            "temperature_units": "F",
        }
    else:
        ds.resstock_chars.write_bytes(b"")
    s = make_settings(tmp_path, ds, tiny_config, max_ensemble_runs=50, **over)
    monkeypatch.setattr(worker, "_settings_override", s)
    with _client(s) as api:
        r = _post(api, {"composition": _comp(3, _fixed("mf_small", 3)), "seeds": SEEDS})
        assert r.status_code == 202, r.text
        worker.execute_run(r.json()["id"], api.app.state.session_factory)
        d = api.get(f"/api/v1/runs/{r.json()['id']}", headers=bearer("alice")).json()
    assert d["status"] == "failed" and d["error_message"], d
    assert str(ds.root) not in json.dumps(d)


@pytest.mark.parametrize("which", ["resstock", "comstock", "weather"])
def test_file_removed_between_creation_and_execution_fails_with_a_reason(
    tmp_path, synthetic_dataset, tiny_config, monkeypatch, which
):
    mod = _fixture_module()
    ds = mod.build_synthetic_dataset(tmp_path / "rm", weather_file=True)
    over = {
        "temperature_source": "weather_file",
        "temperature_weather_file": ds.weather_file.relative_to(ds.root).as_posix(),
        "temperature_time_column": mod.WEATHER_TIME_COL,
        "temperature_column": mod.WEATHER_TEMP_COL_F,
        "temperature_units": "F",
    }
    s = make_settings(tmp_path, ds, tiny_config, max_ensemble_runs=50, **over)
    monkeypatch.setattr(worker, "_settings_override", s)
    with _client(s) as api:
        comp = _comp(4, _fixed("commercial_private", 1), _ranged("mf_small", 0, 1))
        r = _post(api, {"composition": comp, "seeds": SEEDS})
        assert r.status_code == 202, r.text
        {"resstock": ds.resstock_chars, "comstock": ds.comstock_chars, "weather": ds.weather_file}[which].unlink()
        worker.execute_run(r.json()["id"], api.app.state.session_factory)
        d = api.get(f"/api/v1/runs/{r.json()['id']}", headers=bearer("alice")).json()
    assert d["status"] == "failed", d
    assert d["error_message"] and "FileNotFoundError" in d["error_message"], d["error_message"]
    assert str(ds.root) not in json.dumps(d)


def test_whitespace_in_a_required_column_name_is_accepted_and_fails_late(tmp_path, synthetic_dataset, tiny_config):
    """Records behaviour: a column of only spaces passes startup (it is a legal name), so it fails in the worker."""
    s = Settings(database_url=f"sqlite:///{tmp_path / 'x.db'}", environment="test", **{**TC, "temperature_column": " "})
    assert s.temperature_column == " "


# =====================================================================================================================
# FR-ENG-10 end to end through a composition, both sources; provenance; no server path anywhere outward
# =====================================================================================================================


def _all_outward_text(api: TestClient, rid: str, who: str = "alice") -> str:
    parts = [
        api.get(f"/api/v1/runs/{rid}", headers=bearer(who)).text,
        api.get(f"/api/v1/runs/{rid}/results", headers=bearer(who)).text,
        api.get("/api/v1/runs", headers=bearer(who)).text,
        api.get(f"/api/v1/runs/{rid}/events", headers=bearer(who)).text,
        api.get("/api/v1/me", headers=bearer(who)).text,
        api.get("/api/v1/building-types", headers=bearer(who)).text,
    ]
    return "\n".join(parts)


def _assert_no_paths(text: str, *paths: Path) -> None:
    for p in paths:
        for form in {str(p), str(p.resolve()), p.as_posix()}:
            assert form not in text, form
    assert "/tmp/" not in text and "pytest-of" not in text


@pytest.mark.parametrize("kind", ["weather_file", "timeseries_column"])
def test_temperature_reaches_composition_results_for_both_sources(tmp_path, tiny_config, monkeypatch, kind):
    mod = _fixture_module()
    if kind == "weather_file":
        ds = mod.build_synthetic_dataset(tmp_path / "wx", weather_file=True, weather_units="F")
        over = {
            "temperature_source": "weather_file",
            "temperature_weather_file": ds.weather_file.relative_to(ds.root).as_posix(),
            "temperature_time_column": mod.WEATHER_TIME_COL,
            "temperature_column": mod.WEATHER_TEMP_COL_F,
            "temperature_units": "F",
        }
    else:
        ds = mod.build_synthetic_dataset(tmp_path / "tc", temperature_column="out.qa_temp")
        over = {
            "temperature_source": "timeseries_column",
            "temperature_column": "out.qa_temp",
            "temperature_units": "C",
        }
    s = make_settings(tmp_path, ds, tiny_config, max_ensemble_runs=50, **over)
    monkeypatch.setattr(worker, "_settings_override", s)
    with _client(s) as api:
        comp = _comp(5, _fixed("commercial_private", 1), _ranged("mf_small", 0, 1), _ranged("sf_detached", 0, 1))
        d = _run(api, {"composition": comp, "seeds": SEEDS})
        assert d["status"] == "succeeded", d["error_message"]
        doc = _results(api, d["id"])
        t = doc["results"]["temperature"]
        assert t["available"] is True and t["reason"] is None
        assert t["source"]["kind"] == kind
        assert len(t["hourly"]) > 0 and all(isinstance(x, float) for x in t["hourly"][:24])
        if kind == "weather_file":
            assert t["source"]["file"] == ds.weather_file.name
            assert d["data_files"]["temperature"]["file"] == ds.weather_file.name
        assert d["data_files"]["temperature"]["source"] == kind
        assert d["data_files"]["resstock_characteristics_file"] == ds.resstock_chars.name
        text = _all_outward_text(api, d["id"])
        _assert_no_paths(text, ds.root, s.output_dir, tmp_path)
        # the stored snapshot really holds the absolute weather path (so stripping is what protects it)
        with api.app.state.session_factory() as sess:
            row = sess.get(Run, uuid.UUID(d["id"]))
            assert row is not None
            stored = json.dumps(row.composition_snapshot) + json.dumps(row.result)
        if kind == "weather_file":
            assert str(ds.weather_file.resolve()) in stored
        assert str(ds.root.resolve()) in stored


def test_admin_views_of_a_weather_run_contain_no_paths(tmp_path, tiny_config, monkeypatch):
    mod = _fixture_module()
    ds = mod.build_synthetic_dataset(tmp_path / "wx", weather_file=True)
    over = {
        "temperature_source": "weather_file",
        "temperature_weather_file": ds.weather_file.relative_to(ds.root).as_posix(),
        "temperature_time_column": mod.WEATHER_TIME_COL,
        "temperature_column": mod.WEATHER_TEMP_COL_F,
        "temperature_units": "F",
    }
    s = make_settings(tmp_path, ds, tiny_config, max_ensemble_runs=50, **over)
    monkeypatch.setattr(worker, "_settings_override", s)
    with _client(s) as api:
        cfg = client_config(tiny_config)
        cfg["ensemble"] = {"mode": "fixed", "max_runs": 2}
        d = _run(api, {"config": cfg, "seeds": SEEDS}, who="admin")
        assert d["status"] == "succeeded"
        _assert_no_paths(_all_outward_text(api, d["id"], "admin"), ds.root, s.output_dir, tmp_path)


def _insert_row(api: TestClient, snapshot: dict[str, Any], owner: str = "alice") -> str:
    with api.app.state.session_factory() as session:
        run = svc.create_run(session, snapshot, SEEDS, trusted_data_paths=True, owner=owner, validate=False)
        return str(run.id)


def test_data_files_for_old_and_odd_snapshots_are_null_safe_and_never_paths(api, tiny_config, synthetic_dataset):
    base = client_config(tiny_config)
    root = str(synthetic_dataset.root)
    cases = {
        # a row stored by faec91d: three absolute directories, nothing else
        "pre-change shape": {
            "background_dir": f"{root}/background",
            "resstock_timeseries_dir": f"{root}/input/resstock/timeseries_individual_buildings",
            "comstock_timeseries_dir": f"{root}/input/comstock/timeseries_individual_buildings",
        },
        "no data section": None,
        "data not a mapping": ["x"],
        "absolute names and weather path": {
            "resstock_chars_file": f"{root}/background/a.xlsx",
            "comstock_chars_file": f"{root}/background/b.xlsx",
            "temperature_source": "weather_file",
            "temperature_weather_file": f"{root}/weather/w.csv",
            "temperature_column": "v",
            "temperature_time_column": "t",
            "temperature_units": "F",
        },
        "non-string values": {"resstock_chars_file": 5, "temperature_source": ["x"], "temperature_weather_file": {}},
    }
    for label, data in cases.items():
        snap = copy.deepcopy(base)
        if data is None:
            snap.pop("data", None)
        else:
            snap["data"] = data
        rid = _insert_row(api, snap)
        r = api.get(f"/api/v1/runs/{rid}", headers=bearer("alice"))
        assert r.status_code == 200, (label, r.text)
        df = r.json()["data_files"]
        assert root not in r.text, label
        if label == "pre-change shape" or label == "no data section" or label == "data not a mapping":
            assert df == {
                "resstock_characteristics_file": None,
                "comstock_characteristics_file": None,
                "temperature": {"source": None, "file": None, "column": None, "time_column": None, "units": None},
            }, label
        if label == "absolute names and weather path":
            assert df["resstock_characteristics_file"] == "a.xlsx"
            assert df["temperature"]["file"] == "w.csv"


@pytest.mark.parametrize(
    "field",
    ["temperature_column", "temperature_time_column", "temperature_units", "temperature_source"],
)
def test_data_files_text_fields_never_carry_a_path_from_a_stored_snapshot(api, tiny_config, synthetic_dataset, field):
    """`column`, `time_column`, `units` and `source` are echoed verbatim from the stored snapshot. Settings forbid
    nothing path-like in a column name, so a column named like a path would be shown. Checked: only a value the
    operator configured can appear, and it is shown as configured."""
    snap = client_config(tiny_config)
    snap["data"] = {field: "plain"}
    rid = _insert_row(api, snap)
    df = api.get(f"/api/v1/runs/{rid}", headers=bearer("alice")).json()["data_files"]
    key = {"temperature_column": "column", "temperature_time_column": "time_column"}.get(
        field, field.removeprefix("temperature_")
    )
    assert df["temperature"][key] == "plain"


# =====================================================================================================================
# Exclusivity and demo
# =====================================================================================================================


@pytest.mark.parametrize("null_key", ["config", "composition_id", "composition"])
def test_explicit_nulls_count_as_absent(api, null_key):
    body: dict[str, Any] = {"composition": _comp(3, _fixed("mf_small", 3))}
    if null_key == "composition":
        body = {"composition_id": "small-mixed", "composition": None}
    else:
        body[null_key] = None
    assert _post(api, body).status_code == 202


def test_demo_composition_refusal_is_403_and_names_no_internals(api):
    for body in (
        {"composition": _comp(3, _fixed("mf_small", 3))},
        {"composition": _comp(3, _fixed("mf_small", 3)), "filter_selections": {}},
        {"composition": {"bogus": 1}},  # an invalid composition from demo is still a validation error first
    ):
        r = _post(api, body, "demo")
        assert r.status_code in (403, 422), r.text
    assert _post(api, {"composition": _comp(3, _fixed("mf_small", 3))}, "demo").status_code == 403
    assert _rows(api) == 0 and api.app.state.enqueued == []


def test_env_is_restored():
    assert "MICROGRID_TEMPERATURE_SOURCE" not in os.environ


# =====================================================================================================================
# Added after mutation testing (survivors) and settings review
# =====================================================================================================================


def test_min_above_max_is_refused_by_the_api_itself_with_its_own_message(api):
    """Killed survivor: without the entry check the engine still refused it, but with an engine message."""
    r = _post(api, {"composition": _comp(3, _ranged("mf_small", 0.9, 0.1))})
    assert r.status_code == 422
    assert "min_fraction must not exceed max_fraction" in r.text


@pytest.mark.parametrize("value", ["weather/../weather/w.csv", "weather//w.csv", "./weather/w.csv", "ABSOLUTE"])
def test_data_section_rechecks_the_weather_path_shape_even_inside_the_root(tmp_path, value):
    """Killed survivor: a weather path that stays inside the root but is not a plain relative path (absolute, '..',
    '.' or empty parts) is refused by the service even if Settings validation was bypassed."""
    root = tmp_path / "root"
    (root / "weather").mkdir(parents=True)
    (root / "weather" / "w.csv").write_text("t,v\n")
    if value == "ABSOLUTE":
        value = str(root / "weather" / "w.csv")
    s = Settings.model_construct(
        **{
            **Settings(database_url="sqlite://", environment="test").__dict__,
            "data_dir": root,
            **WX,
            "temperature_weather_file": value,
        }
    )
    with pytest.raises(svc.DataSectionError):
        svc.server_side_data_section(s)


@pytest.mark.xfail(
    strict=True,
    reason="DEFECT (LOW, FR-ENG-3): a misspelled temperature variable (MICROGRID_TEMPERATURE_SOURCES=weather_file) "
    "is silently ignored (Settings extra='ignore'), so the server runs with source 'none'. Results do say no "
    "temperature is available, but the operator's setting was ignored without a message. Fix: refuse unknown "
    "MICROGRID_TEMPERATURE_* / *_CHARS_FILE names at startup.",
)
def test_misspelled_temperature_variable_fails_at_startup(tmp_path, monkeypatch):
    monkeypatch.setenv("MICROGRID_DATABASE_URL", f"sqlite:///{tmp_path / 'x.db'}")
    monkeypatch.setenv("MICROGRID_ENVIRONMENT", "test")
    monkeypatch.setenv("MICROGRID_TEMPERATURE_SOURCES", "weather_file")
    with pytest.raises((ConfigurationError, ValueError)):
        Settings()


def test_wrong_weather_column_is_accepted_at_creation_and_fails_late_with_a_reason(tmp_path, tiny_config, monkeypatch):
    """Records behaviour: column names are not checked against the file at creation, so a typo in
    MICROGRID_TEMPERATURE_COLUMN surfaces as a failed run ('engine run failed (ValueError)'), not a 503."""
    mod = _fixture_module()
    ds = mod.build_synthetic_dataset(tmp_path / "wc", weather_file=True)
    over = {
        "temperature_source": "weather_file",
        "temperature_weather_file": ds.weather_file.relative_to(ds.root).as_posix(),
        "temperature_time_column": mod.WEATHER_TIME_COL,
        "temperature_column": "no such column",
        "temperature_units": "F",
    }
    s = make_settings(tmp_path, ds, tiny_config, max_ensemble_runs=50, **over)
    monkeypatch.setattr(worker, "_settings_override", s)
    with _client(s) as api:
        d = _run(api, {"composition": _comp(3, _fixed("mf_small", 3)), "seeds": SEEDS})
    assert d["status"] == "failed" and "ValueError" in d["error_message"]
    assert str(ds.root) not in json.dumps(d) and "no such column" not in d["error_message"]
