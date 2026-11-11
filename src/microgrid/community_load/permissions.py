"""Role permission flags (FR-ENG-11 b, FR-WEB-5): what each role may do, as data rather than role checks.

Callers ask ``can(role, flag)``; they never test ``role == "admin"``. MVP defaults:

====================  =====  ======  ====
flag                  admin  client  demo
====================  =====  ======  ====
``filters.use``       yes    no      no
====================  =====  ======  ====

Turning filters on for clients later is a configuration change (an override), not a code change. The
Demo role can never get ``filters.use``, even by override.

Permission is checked when **editing** filters, not when **running** a composition: a composition saved by
an admin with filters stays runnable by everyone (the engine facade takes no role at all). Use
``validate_filters_allowed`` when someone submits a new composition, and ``validate_filter_edit`` when they
re-save an existing one (re-saving with unchanged filters is not an edit of the filters).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from microgrid.community_load.synthetic.attribute_filters import AttributeFilter
from microgrid.community_load.synthetic.config import ArchetypeFilterConfig, ScenarioConfig, config_from_dict

ROLES = ("admin", "client", "demo")

#: flag -> {role: allowed}
FLAGS: dict[str, dict[str, bool]] = {
    "filters.use": {"admin": True, "client": False, "demo": False},
}

#: (role, flag) pairs that no override may turn on.
NEVER: frozenset[tuple[str, str]] = frozenset({("demo", "filters.use")})

Overrides = Mapping[str, Mapping[str, bool]]


class FilterPermissionError(PermissionError):
    """A role without the ``filters.use`` flag submitted (or edited) attribute filters."""


def _check_names(role: str, flag: str) -> None:
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r} (one of {', '.join(ROLES)})")
    if flag not in FLAGS:
        raise ValueError(f"unknown permission flag {flag!r} (one of {', '.join(sorted(FLAGS))})")


def can(role: str, flag: str, overrides: Overrides | None = None) -> bool:
    """Whether ``role`` has ``flag``: the default, unless ``overrides[role][flag]`` says otherwise.

    Unknown roles or flags (in the arguments or in the overrides) raise ValueError, and so does an
    override that tries to give a role a flag it can never have.
    """
    _check_names(role, flag)
    for o_role, o_flags in (overrides or {}).items():
        if o_role not in ROLES:
            raise ValueError(f"unknown role {o_role!r} in permission overrides (one of {', '.join(ROLES)})")
        for o_flag, value in o_flags.items():
            if o_flag not in FLAGS:
                raise ValueError(f"unknown permission flag {o_flag!r} in permission overrides")
            if not isinstance(value, bool):
                raise ValueError(f"override {o_role}.{o_flag} must be true or false, got {value!r}")
            if value and (o_role, o_flag) in NEVER:
                raise ValueError(f"the {o_role} role can never have {o_flag}")
    if overrides and role in overrides and flag in overrides[role]:
        return overrides[role][flag]
    return FLAGS[flag][role]


def _filters_of(composition: Any) -> dict[str, tuple[AttributeFilter, ...]]:
    """Attribute filters by category, from a scenario dict/config, filter config, or a bare list of filters."""
    if isinstance(composition, Mapping):
        composition = config_from_dict(dict(composition), source="<submitted composition>")
    if isinstance(composition, ScenarioConfig):
        composition = composition.archetype_filters
    if isinstance(composition, ArchetypeFilterConfig):
        out: dict[str, tuple[AttributeFilter, ...]] = {}
        for kind, cats in (("resstock", composition.resstock), ("comstock", composition.comstock)):
            for name, cfg in cats.items():
                if cfg.attributes:
                    out[f"{kind}/{name}"] = tuple(cfg.attributes)
        return out
    if isinstance(composition, Sequence) and not isinstance(composition, str):
        if not all(isinstance(f, AttributeFilter) for f in composition):
            raise TypeError("a list of filters must contain AttributeFilter objects")
        return {"filters": tuple(composition)} if composition else {}
    raise TypeError(
        f"expected a scenario dict/config, ArchetypeFilterConfig or list of AttributeFilter, got {type(composition).__name__}"
    )


def validate_filters_allowed(role: str, composition: Any, overrides: Overrides | None = None) -> None:
    """Raise FilterPermissionError if ``composition`` contains attribute filters and ``role`` lacks ``filters.use``."""
    allowed = can(role, "filters.use", overrides)
    found = _filters_of(composition)
    if found and not allowed:
        raise FilterPermissionError(
            f"role {role!r} does not have the filters.use permission, but the composition has attribute filters "
            f"on: {', '.join(sorted(found))}"
        )


def validate_filter_edit(role: str, *, new: Any, saved: Any = None, overrides: Overrides | None = None) -> None:
    """Gate a save: only changing the attribute filters needs ``filters.use``.

    ``saved`` is the stored composition (``None`` for a brand-new one). Saving with filters identical to
    the stored ones passes for any role; adding, changing or removing filters requires the flag.
    """
    allowed = can(role, "filters.use", overrides)
    if allowed:
        return
    if _filters_of(new) != (_filters_of(saved) if saved is not None else {}):
        raise FilterPermissionError(
            f"role {role!r} does not have the filters.use permission and cannot add, change or remove attribute filters"
        )
