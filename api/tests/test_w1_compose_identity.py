"""W1-COMPOSE (A, B): who am I (`GET /me`) and the building-type catalogue (`GET /building-types`).

Both endpoints are for any authenticated role. `/me` reports the caller's own subject, role, effective permission flags
(the library's `can` with the server's overrides) and the caps a composition form needs; it never carries key
material or anything about other subjects. `/building-types` is the registry as a catalogue for a form: no filter
internals, column names or paths.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from microgrid.community_load.registry import default_registry

from microgrid_api.main import create_app

from .http_support import KEYS, ROLE_OF, bearer, make_settings

ME = "/api/v1/me"
TYPES = "/api/v1/building-types"


@pytest.fixture
def http_settings(tmp_path, synthetic_dataset, tiny_config):
    return make_settings(
        tmp_path,
        synthetic_dataset,
        tiny_config,
        max_total_buildings=777,
        max_ensemble_runs=333,
        demo_max_buildings=12,
        demo_max_runs=7,
    )


@pytest.fixture
def api(http_settings):
    with TestClient(create_app(http_settings)) as c:
        yield c


# --- /me ---------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("who", ["admin", "alice", "demo"])
def test_me_reports_own_subject_role_and_caps(api, who):
    r = api.get(ME, headers=bearer(who))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["subject"] == who
    assert body["role"] == ROLE_OF[who]
    assert body["limits"]["max_total_buildings"] == 777
    assert body["limits"]["max_ensemble_runs"] == 333
    if ROLE_OF[who] == "demo":
        assert body["limits"]["demo_caps"] == {"max_buildings": 12, "max_runs": 7}
        assert body["run_inputs"] == ["composition_id"]
    else:
        assert body["limits"]["demo_caps"] is None
        assert body["run_inputs"] == ["composition", "composition_id", "config"]


def test_me_permission_flags_follow_the_library_defaults(api):
    flags = {who: api.get(ME, headers=bearer(who)).json()["permissions"] for who in ("admin", "alice", "demo")}
    assert flags == {
        "admin": {"filters.use": True},
        "alice": {"filters.use": False},
        "demo": {"filters.use": False},
    }


def test_me_permission_flags_follow_the_server_overrides(tmp_path, synthetic_dataset, tiny_config):
    s = make_settings(
        tmp_path,
        synthetic_dataset,
        tiny_config,
        permission_overrides={"client": {"filters.use": True}, "admin": {"filters.use": False}},
    )
    with TestClient(create_app(s)) as c:
        assert c.get(ME, headers=bearer("alice")).json()["permissions"] == {"filters.use": True}
        assert c.get(ME, headers=bearer("admin")).json()["permissions"] == {"filters.use": False}


@pytest.mark.parametrize(
    "headers", [{}, {"Authorization": "Bearer wrong-key-0123456789abcdef"}, {"Authorization": "Basic x"}]
)
def test_me_and_building_types_need_a_valid_key(api, headers):
    for path in (ME, TYPES):
        r = api.get(path, headers=headers)
        assert r.status_code == 401, (path, r.text)
        assert "wrong-key" not in r.text


@pytest.mark.parametrize("who", ["admin", "alice", "demo"])
def test_me_never_contains_key_material_or_other_subjects(api, who):
    text = api.get(ME, headers=bearer(who)).text
    for other, key in KEYS.items():
        assert key not in text
        assert key[:12] not in text  # not even a prefix
        if other != who:
            assert f'"{other}"' not in text
    assert "key" not in api.get(ME, headers=bearer(who)).json()


def test_me_carries_no_server_paths_or_secrets(api, http_settings):
    text = api.get(ME, headers=bearer("admin")).text
    assert str(http_settings.data_dir) not in text
    assert str(http_settings.output_dir) not in text
    assert "sqlite" not in text and "database" not in text


# --- /building-types ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("who", ["admin", "alice", "demo"])
def test_building_types_lists_the_registry_for_every_role(api, who):
    r = api.get(TYPES, headers=bearer(who))
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    reg = default_registry()
    assert [i["key"] for i in items] == [t.key for t in reg.types]  # registry order, nothing hidden
    for item, t in zip(items, reg.types, strict=True):
        assert set(item) == {"key", "label", "building_class", "source", "description", "active"}
        assert item["label"] == t.label
        assert item["source"] == t.source
        assert item["building_class"] == ("residential" if t.source == "resstock" else "commercial")
        assert item["active"] is t.active
        assert item["description"] is None  # the registry carries no description today


def test_inactive_types_are_listed_as_inactive(api):
    items = {i["key"]: i for i in api.get(TYPES, headers=bearer("alice")).json()["items"]}
    assert items["civic"]["active"] is False
    assert items["mf_small"]["active"] is True


def test_building_types_expose_no_filter_internals(api):
    text = api.get(TYPES, headers=bearer("admin")).text
    for internal in (
        "Multi-Family with",
        "SmallOffice",
        "building_type_heights",
        "comstock_building_types",
        "include_public",
        "in.geometry",
        "subtypes",
    ):
        assert internal not in text, internal


def test_building_types_follow_the_registry_on_app_state(tmp_path, synthetic_dataset, tiny_config):
    """The catalogue is read from the registry the app uses, so a registry entry is all a new type needs."""
    s = make_settings(tmp_path, synthetic_dataset, tiny_config)
    app = create_app(s)
    app.state.building_registry = default_registry().extended(
        {
            "key": "fake_lab",
            "label": "Laboratory",
            "source": "comstock",
            "use": "institutional",
            "comstock_building_types": ["Laboratory"],
        }
    )
    with TestClient(app) as c:
        items = c.get(TYPES, headers=bearer("alice")).json()["items"]
    assert items[-1] == {
        "key": "fake_lab",
        "label": "Laboratory",
        "building_class": "commercial",
        "source": "comstock",
        "description": None,
        "active": True,
    }


def test_openapi_documents_me_and_building_types():
    import json

    from microgrid_api import openapi_export

    spec = json.loads(openapi_export.render())
    assert "get" in spec["paths"][ME]
    assert "get" in spec["paths"][TYPES]
