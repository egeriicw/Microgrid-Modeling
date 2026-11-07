"""Building-type registry: the extensible list of building types the load profiler can model (FR-ENG-1).

A *type* ties together three things that used to be scattered through the code:
  * how a mapped structure (GIS) is assigned to it (use class, optional subtype and story range),
  * which source buildings it samples from (ResStock/ComStock filter values),
  * whether it is currently active.

The default registry is data (``building_types.yaml``). Adding a type means adding an entry; the engine
(``community_load.synthetic``) is unaware of the registry and keeps working from the filters it is given.
"""

from __future__ import annotations

import copy
import functools
from dataclasses import dataclass, field, fields, replace
from importlib import resources
from pathlib import Path
from typing import Any

import yaml

from microgrid.community_load.synthetic.config import (
    CommercialFilterConfig,
    ResidentialFilterConfig,
    ScenarioConfig,
)
from microgrid.geo.types import Structure, UseClass

_SOURCES = ("resstock", "comstock")
_TOP_KEYS = {"version", "assumed_stories_when_unknown", "building_types"}


@dataclass(frozen=True)
class BuildingType:
    key: str
    label: str
    source: str
    use: UseClass
    subtypes: tuple[str, ...] = ()
    min_stories: int | None = None
    max_stories: int | None = None
    resstock_building_type_heights: tuple[str, ...] = ()
    comstock_building_types: tuple[str, ...] = ()
    include_public: bool = False
    active: bool = True

    def story_span(self) -> float:
        """Width of the story range; narrower ranges are more specific."""
        lo = self.min_stories if self.min_stories is not None else 0
        hi = self.max_stories if self.max_stories is not None else float("inf")
        return hi - lo

    def matches(self, structure: Structure, assumed_stories: int) -> bool:
        if not self.active or structure.use is not self.use:
            return False
        if self.subtypes and structure.subtype not in self.subtypes:
            return False
        if self.min_stories is not None or self.max_stories is not None:
            stories = structure.stories if structure.stories is not None else assumed_stories
            if self.min_stories is not None and stories < self.min_stories:
                return False
            if self.max_stories is not None and stories > self.max_stories:
                return False
        return True

    def default_filter(self) -> ResidentialFilterConfig | CommercialFilterConfig:
        """The sampling filter this type implies for the engine."""
        if self.source == "resstock":
            return ResidentialFilterConfig(building_type_heights=list(self.resstock_building_type_heights))
        return CommercialFilterConfig(
            building_types=list(self.comstock_building_types), include_public=self.include_public
        )


def _parse_entry(entry: dict[str, Any], index: int) -> BuildingType:
    where = f"building_types[{index}] ({entry.get('key', '?')})"
    allowed = {f.name for f in fields(BuildingType)}
    unknown = sorted(set(entry) - allowed)
    if unknown:
        raise ValueError(f"{where}: unknown field(s) {', '.join(unknown)}")
    missing = [k for k in ("key", "label", "source", "use") if k not in entry]
    if missing:
        raise ValueError(f"{where}: missing field(s) {', '.join(missing)}")
    if entry["source"] not in _SOURCES:
        raise ValueError(f"{where}: source must be one of {_SOURCES}, got {entry['source']!r}")
    try:
        use = UseClass(entry["use"])
    except ValueError:
        valid = ", ".join(u.value for u in UseClass)
        raise ValueError(f"{where}: use {entry['use']!r} is not a UseClass (one of {valid})") from None

    lo, hi = entry.get("min_stories"), entry.get("max_stories")
    if lo is not None and hi is not None and lo > hi:
        raise ValueError(f"{where}: min_stories {lo} exceeds max_stories {hi}")

    bt = BuildingType(
        key=str(entry["key"]),
        label=str(entry["label"]),
        source=entry["source"],
        use=use,
        subtypes=tuple(entry.get("subtypes", ())),
        min_stories=lo,
        max_stories=hi,
        resstock_building_type_heights=tuple(entry.get("resstock_building_type_heights", ())),
        comstock_building_types=tuple(entry.get("comstock_building_types", ())),
        include_public=bool(entry.get("include_public", False)),
        active=bool(entry.get("active", True)),
    )
    if bt.active:
        needed = bt.resstock_building_type_heights if bt.source == "resstock" else bt.comstock_building_types
        if not needed:
            field_name = "resstock_building_type_heights" if bt.source == "resstock" else "comstock_building_types"
            raise ValueError(f"{where}: an active {bt.source} type needs sampling filter values in {field_name}")
    return bt


@dataclass(frozen=True)
class BuildingTypeRegistry:
    types: tuple[BuildingType, ...]
    assumed_stories: int = 3
    _by_key: dict[str, BuildingType] = field(init=False, repr=False, compare=False, default_factory=dict)

    def __post_init__(self) -> None:
        by_key: dict[str, BuildingType] = {}
        for t in self.types:
            if t.key in by_key:
                raise ValueError(f"duplicate building type key {t.key!r}")
            by_key[t.key] = t
        object.__setattr__(self, "_by_key", by_key)

    # --- construction -----------------------------------------------------------------------
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BuildingTypeRegistry:
        unknown = sorted(set(data) - _TOP_KEYS)
        if unknown:
            raise ValueError(f"registry: unknown top-level key(s) {', '.join(unknown)}")
        types = tuple(_parse_entry(e, i) for i, e in enumerate(data.get("building_types", [])))
        return cls(types, int(data.get("assumed_stories_when_unknown", 3)))

    @classmethod
    def from_yaml(cls, path: Path | str) -> BuildingTypeRegistry:
        return cls.from_dict(yaml.safe_load(Path(path).read_text()) or {})

    def extended(self, *entries: dict[str, Any]) -> BuildingTypeRegistry:
        """A new registry with extra types appended (the original is unchanged)."""
        new = tuple(_parse_entry(e, len(self.types) + i) for i, e in enumerate(entries))
        return replace(self, types=self.types + new)

    def with_assumed_stories(self, stories: int) -> BuildingTypeRegistry:
        return replace(self, assumed_stories=stories)

    # --- queries ----------------------------------------------------------------------------
    def get(self, key: str) -> BuildingType:
        try:
            return self._by_key[key]
        except KeyError:
            raise KeyError(f"unknown building type {key!r}; known: {', '.join(sorted(self._by_key))}") from None

    def __contains__(self, key: object) -> bool:
        return key in self._by_key

    def active_types(self) -> list[BuildingType]:
        return [t for t in self.types if t.active]

    def match(self, structure: Structure) -> BuildingType | None:
        """The most specific active type for a mapped structure, or None when it is unmodeled."""
        candidates = [t for t in self.types if t.matches(structure, self.assumed_stories)]
        if not candidates:
            return None
        return min(candidates, key=lambda t: (not t.subtypes, t.story_span()))


@functools.lru_cache(maxsize=1)
def default_registry() -> BuildingTypeRegistry:
    """The registry shipped with the package (``building_types.yaml``)."""
    text = resources.files("microgrid.community_load").joinpath("building_types.yaml").read_text()
    return BuildingTypeRegistry.from_dict(yaml.safe_load(text))


def with_default_filters(cfg: ScenarioConfig, registry: BuildingTypeRegistry) -> ScenarioConfig:
    """Copy of ``cfg`` where every composition category has a sampling filter.

    Categories that already have explicit filters in the config keep them; the rest get the registry's
    defaults. A category that is neither configured nor in the registry is an error.
    """
    out = copy.deepcopy(cfg)
    for name, bounds in out.composition.categories.items():
        existing = out.archetype_filters.resstock if bounds.source == "resstock" else out.archetype_filters.comstock
        if name in existing:
            continue
        if name not in registry:
            raise ValueError(
                f"category {name!r} has no archetype filters in the config and is not a registered building type"
            )
        t = registry.get(name)
        if t.source != bounds.source:
            raise ValueError(f"category {name!r} is sourced from {bounds.source} but registered as {t.source}")
        existing[name] = t.default_filter()  # type: ignore[assignment]
    return out
