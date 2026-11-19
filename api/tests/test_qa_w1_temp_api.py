"""Independent QA for W1-TEMP at the API trust boundary.

The engine's new ``data.temperature_weather_file`` is a server-side file path. A client must never be able to set
it (nor any other ``data.temperature_*`` field, since the whole ``data`` section is server-built). These tests prove
that, and record that the server-built section declares the source from settings (`none` by default), so API results carry
``temperature.available = false`` unless the operator declares where the weather lives [verify].
"""

from __future__ import annotations

import pytest

from microgrid_api.services import runs as svc

from .http_support import bearer, client_config, make_settings

TEMP_FIELDS = {
    "temperature_source": "weather_file",
    "temperature_weather_file": "../../../etc/passwd.csv",
    "temperature_time_column": "ts",
    "temperature_column": "t",
    "temperature_units": "C",
}


def test_the_weather_file_field_is_a_recognised_path_key():
    assert "temperature_weather_file" in svc.DATA_PATH_KEYS


@pytest.mark.parametrize("value", ["../../../etc/x.csv", "/etc/x.csv", "weather.csv"])
def test_untrusted_snapshot_cannot_set_the_weather_file(session, value):
    snap = {"scenario": {"name": "x"}, "data": {"temperature_weather_file": value}}
    with pytest.raises(ValueError) as exc:
        svc.create_run(session, snap)
    assert "temperature_weather_file" in str(exc.value)
    assert value not in str(exc.value)  # keys only, never the value


@pytest.fixture
def api(tmp_path, synthetic_dataset, tiny_config):
    from fastapi.testclient import TestClient

    from microgrid_api.main import create_app

    app = create_app(make_settings(tmp_path, synthetic_dataset, tiny_config))
    app.state.enqueued = []
    app.state.enqueue = lambda run_id: app.state.enqueued.append(str(run_id))
    with TestClient(app) as c:
        yield c


@pytest.mark.parametrize("fields", [TEMP_FIELDS, {"temperature_weather_file": "/etc/shadow.csv"}])
def test_http_client_cannot_declare_a_temperature_source(api, tiny_config, fields):
    cfg = client_config(tiny_config)
    cfg["data"] = dict(fields)
    r = api.post("/api/v1/runs", headers=bearer("alice"), json={"config": cfg})
    assert r.status_code == 422
    assert "/etc" not in r.text and "passwd" not in r.text
    assert api.app.state.enqueued == []


def test_server_built_data_section_declares_no_temperature_by_default(tmp_path, synthetic_dataset, tiny_config):
    """W1-COMPOSE: the source is declared explicitly from settings; by default it is `none` (the results block says
    no temperature is available) and nothing else about temperature is set."""
    data = svc.server_side_data_section(make_settings(tmp_path, synthetic_dataset, tiny_config))
    assert {k: v for k, v in data.items() if k.startswith("temperature_")} == {"temperature_source": "none"}
