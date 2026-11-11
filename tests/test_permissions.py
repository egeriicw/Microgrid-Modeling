"""Role permission flags for attribute filters (FR-ENG-11 b): a flag, not a hard-coded role check."""

from __future__ import annotations

import copy

import pytest

from microgrid.community_load import permissions
from microgrid.community_load.permissions import (
    FilterPermissionError,
    can,
    validate_filter_edit,
    validate_filters_allowed,
)
from microgrid.community_load.synthetic import run
from microgrid.community_load.synthetic.attribute_filters import AttributeFilter
from microgrid.community_load.synthetic.config import ArchetypeFilterConfig, ResidentialFilterConfig, config_from_dict
from tests.test_run_facade import build_tiny_config

ATTR = {"column": "in.geometry_stories", "op": "range", "min": 2}


def _cfg_dict(ds, with_filter: bool):
    raw = build_tiny_config(ds)
    raw = copy.deepcopy(raw)
    if with_filter:
        raw["archetype_filters"]["resstock"]["mf_small"]["attributes"] = [ATTR]
    return raw


def test_defaults_admin_only():
    assert can("admin", "filters.use") is True
    assert can("client", "filters.use") is False
    assert can("demo", "filters.use") is False


def test_overrides_can_enable_clients_but_never_demo():
    assert can("client", "filters.use", {"client": {"filters.use": True}}) is True
    with pytest.raises(ValueError, match="demo.*never"):
        can("demo", "filters.use", {"demo": {"filters.use": True}})
    # overrides can also switch the admin flag off
    assert can("admin", "filters.use", {"admin": {"filters.use": False}}) is False


def test_unknown_role_flag_and_bad_overrides_raise():
    with pytest.raises(ValueError, match="unknown role"):
        can("owner", "filters.use")
    with pytest.raises(ValueError, match="unknown permission flag"):
        can("admin", "filters.edit")
    with pytest.raises(ValueError, match="unknown role"):
        can("admin", "filters.use", {"owner": {"filters.use": True}})
    with pytest.raises(ValueError, match="unknown permission flag"):
        can("admin", "filters.use", {"client": {"nope": True}})
    with pytest.raises(ValueError, match="true or false"):
        can("admin", "filters.use", {"client": {"filters.use": "yes"}})


def test_non_permitted_role_cannot_submit_filters(synthetic_dataset):
    raw = _cfg_dict(synthetic_dataset, with_filter=True)
    validate_filters_allowed("admin", raw)
    for role in ("client", "demo"):
        with pytest.raises(FilterPermissionError, match="filters.use") as ei:
            validate_filters_allowed(role, raw)
        assert isinstance(ei.value, PermissionError)
    validate_filters_allowed("client", raw, {"client": {"filters.use": True}})


def test_compositions_without_filters_are_fine_for_everyone(synthetic_dataset):
    raw = _cfg_dict(synthetic_dataset, with_filter=False)
    for role in ("admin", "client", "demo"):
        validate_filters_allowed(role, raw)
        validate_filters_allowed(role, config_from_dict(raw))
    validate_filters_allowed("demo", config_from_dict({**raw, "archetype_filters": {"resstock": {}}}))


def test_accepts_config_objects_filter_configs_and_lists(synthetic_dataset):
    cfg = config_from_dict(_cfg_dict(synthetic_dataset, with_filter=True))
    with pytest.raises(FilterPermissionError):
        validate_filters_allowed("client", cfg)
    with pytest.raises(FilterPermissionError):
        validate_filters_allowed("client", cfg.archetype_filters)
    with pytest.raises(FilterPermissionError):
        validate_filters_allowed("client", [AttributeFilter("a", "not_null")])
    validate_filters_allowed("client", [])
    empty = ArchetypeFilterConfig(resstock={"x": ResidentialFilterConfig()})
    validate_filters_allowed("client", empty)
    with pytest.raises(TypeError):
        validate_filters_allowed("client", 42)


def test_permission_is_checked_on_editing_not_on_running(synthetic_dataset):
    """An admin-saved composition with filters stays runnable by anyone; only changing filters is gated."""
    saved = _cfg_dict(synthetic_dataset, with_filter=True)
    # Running has no role check at all: the engine facade takes no role, so a client run works.
    result = run.run_scenario(saved)
    assert result.pool_summary["resstock/mf_small"]["matched"] > 0
    # A client re-saving it unchanged is allowed; changing the filter is not.
    validate_filter_edit("client", new=saved, saved=saved)
    changed = copy.deepcopy(saved)
    changed["archetype_filters"]["resstock"]["mf_small"]["attributes"][0]["min"] = 3
    with pytest.raises(FilterPermissionError):
        validate_filter_edit("client", new=changed, saved=saved)
    with pytest.raises(FilterPermissionError):  # adding filters to a filter-free composition
        validate_filter_edit("client", new=saved, saved=_cfg_dict(synthetic_dataset, with_filter=False))
    # Removing them is also an edit.
    with pytest.raises(FilterPermissionError):
        validate_filter_edit("client", new=_cfg_dict(synthetic_dataset, with_filter=False), saved=saved)
    validate_filter_edit("admin", new=changed, saved=saved)


def test_flags_table_is_data():
    assert permissions.FLAGS["filters.use"] == {"admin": True, "client": False, "demo": False}
