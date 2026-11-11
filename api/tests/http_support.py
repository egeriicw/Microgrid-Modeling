"""Shared helpers for the HTTP run tests (not collected as tests)."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from microgrid_api.config import ApiKey, Settings
from microgrid_api.db.migrate import upgrade_to_head

KEYS = {
    "admin": "admin-key-0123456789abcdef",
    "alice": "client-key-alice-0123456789",
    "bob": "client-key-bob-0123456789ab",
    "demo": "demo-key-0123456789abcdefgh",
}
ROLE_OF = {"admin": "admin", "alice": "client", "bob": "client", "demo": "demo"}


def bearer(who: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {KEYS[who]}"}


def api_keys() -> list[ApiKey]:
    return [ApiKey(subject=s, role=ROLE_OF[s], key=k) for s, k in KEYS.items()]


def client_config(tiny_config: dict[str, Any]) -> dict[str, Any]:
    """The scenario a client may send: no `data` section."""
    cfg = copy.deepcopy(tiny_config)
    cfg.pop("data")
    return cfg


def write_demo_file(path: Path, tiny_config: dict[str, Any], name: str = "small-mixed") -> Path:
    path.write_text(yaml.safe_dump({"compositions": {name: client_config(tiny_config)}}))
    return path


def make_settings(tmp_path: Path, synthetic_dataset: Any, tiny_config: dict[str, Any], **over: Any) -> Settings:
    kwargs: dict[str, Any] = {
        "database_url": f"sqlite:///{tmp_path / 'http.db'}",
        "environment": "test",
        "data_dir": synthetic_dataset.root,
        "output_dir": tmp_path / "out",
        "api_keys": api_keys(),
        "demo_compositions_file": write_demo_file(tmp_path / "demo.yaml", tiny_config),
        "sse_poll_interval_s": 0.02,
    }
    kwargs.update(over)
    s = Settings(**kwargs)
    upgrade_to_head(s.require_database_url())
    return s
