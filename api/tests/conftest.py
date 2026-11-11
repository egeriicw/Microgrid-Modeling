"""Shared fixtures for the API test suite.

Real fixtures: `settings`, `app`, `client` (sync TestClient, SQLite file per test).
Placeholder fixtures (`db`, `storage`, `fake_queue`, `async_client`) stay skipped until their stories land
(storage/runs/configs: later).
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from microgrid_api.config import Settings
from microgrid_api.db.migrate import upgrade_to_head
from microgrid_api.main import create_app


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    s = Settings(database_url=f"sqlite:///{tmp_path / 'test.db'}", environment="test")
    upgrade_to_head(s.require_database_url())  # /ready also checks the schema, so the test DB is migrated
    return s


@pytest.fixture
def app(settings: Settings):
    return create_app(settings)


@pytest.fixture
def client(app) -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer test-token"}


@pytest.fixture
def db():
    pytest.skip("db fixture for placeholder tests lands with the runs/configs stories")


@pytest.fixture
def storage():
    pytest.skip("storage not implemented")


@pytest.fixture
def async_client():
    pytest.skip("async client fixture lands with the runs/configs stories")


# --- worker fixtures (story 1.10) ----------------------------------------------------------------------------------
# api/tests is itself a package called `tests`, so the root `tests.fixtures` package cannot be imported by name.
# The synthetic-data builder is loaded by file path instead (no sys.path or pythonpath changes needed).
REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def synthetic_dataset(tmp_path_factory: pytest.TempPathFactory):
    path = REPO_ROOT / "tests" / "fixtures" / "synthetic_data.py"
    spec = importlib.util.spec_from_file_location("_root_synthetic_data", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(module)
    return module.build_synthetic_dataset(tmp_path_factory.mktemp("synthetic_data"))


@pytest.fixture
def tiny_config(synthetic_dataset) -> dict:
    """Minimal scenario dict (6 runs, fixed mode) pointing at the synthetic dataset; seeds left unset."""
    return {
        "scenario": {"name": "tiny"},
        "data": synthetic_dataset.data,
        "composition": {
            "total_buildings": 4,
            "method": "dirichlet",
            "categories": {
                "mf_small": {"min_fraction": 0.5, "max_fraction": 1.0, "source": "resstock"},
                "mf_mid": {"min_fraction": 0.0, "max_fraction": 0.5, "source": "resstock"},
            },
        },
        "archetype_filters": {
            "resstock": {
                "mf_small": {
                    "building_type_heights": [
                        "Multi-Family with 2 - 4 Units",
                        "Multi-Family with 5+ Units, 1-3 Stories",
                    ]
                },
                "mf_mid": {"building_type_heights": ["Multi-Family with 5+ Units, 4-7 Stories"]},
            }
        },
        "ensemble": {"mode": "fixed", "max_runs": 6},
        "perturbation": {"enabled": False},
    }


@pytest.fixture
def session_factory(settings: Settings):
    engine = create_engine(settings.require_database_url())
    yield sessionmaker(engine, expire_on_commit=False)
    engine.dispose()


@pytest.fixture
def session(session_factory):
    with session_factory() as s:
        yield s
