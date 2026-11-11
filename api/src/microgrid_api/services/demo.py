"""Predefined compositions for the demo role (FR-WEB-5). Loaded and validated once at startup; any problem fails
loudly (ConfigurationError) rather than leaving a half-configured demo."""

from __future__ import annotations

import copy
from typing import Any

import yaml
from microgrid.community_load.permissions import FilterPermissionError, validate_filters_allowed
from microgrid.community_load.synthetic.config import config_from_dict

from microgrid_api.config import ConfigurationError, Settings
from microgrid_api.services import runs as svc

DEMO_ROLE = "demo"


def load_demo_compositions(settings: Settings) -> dict[str, dict[str, Any]]:
    path = settings.demo_compositions_file
    if path is None:
        return {}
    try:
        raw = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigurationError(f"demo compositions file could not be read ({type(exc).__name__}).") from exc
    comps = raw.get("compositions") if isinstance(raw, dict) else None
    if not isinstance(comps, dict) or not all(isinstance(v, dict) for v in comps.values()):
        raise ConfigurationError("demo compositions file must contain a 'compositions' mapping of name -> scenario.")
    return {str(name): _validate(str(name), cfg, settings) for name, cfg in comps.items()}


def _validate(name: str, cfg: dict[str, Any], settings: Settings) -> dict[str, Any]:
    if "data" in cfg or svc.path_keys_present(cfg):
        raise ConfigurationError(f"demo composition '{name}': 'data' and path fields are built by the server.")
    try:
        parsed = config_from_dict(copy.deepcopy(cfg), source=f"<demo composition {name}>")
        validate_filters_allowed(DEMO_ROLE, parsed)
    except FilterPermissionError as exc:
        raise ConfigurationError(f"demo composition '{name}' uses attribute filters, which demo cannot: {exc}") from exc
    except ValueError as exc:
        raise ConfigurationError(f"demo composition '{name}' is invalid: {exc}") from exc
    total = parsed.composition.total_buildings
    if total > settings.demo_max_buildings:
        raise ConfigurationError(
            f"demo composition '{name}': total_buildings {total} exceeds the demo cap {settings.demo_max_buildings}."
        )
    max_runs = parsed.ensemble.max_runs
    if max_runs > settings.demo_max_runs:
        raise ConfigurationError(
            f"demo composition '{name}': ensemble max_runs {max_runs} exceeds the demo cap {settings.demo_max_runs}."
        )
    return cfg
