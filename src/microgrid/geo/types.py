"""Geographic data contracts: where things are and what they are. No computation beyond trivial summaries."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Mapping

LonLat = tuple[float, float]
Ring = tuple[LonLat, ...]  # closed or open ring of (lon, lat), WGS84


class UseClass(str, Enum):
    RESIDENTIAL_SF = "residential_sf"      # single-family / townhouse
    RESIDENTIAL_MF = "residential_mf"      # multifamily
    COMMERCIAL = "commercial"
    INDUSTRIAL = "industrial"
    INSTITUTIONAL = "institutional"        # schools, hospitals, civic
    OTHER = "other"                        # garages, sheds: no modeled load
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Structure:
    """One building footprint with the attributes planners can usually get from GIS."""
    id: str
    use: UseClass
    footprint_m2: float
    centroid: LonLat
    stories: int | None = None
    height_m: float | None = None
    units: int | None = None               # dwelling units, when known
    year_built: int | None = None
    attrs: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True)
class Neighborhood:
    """A concrete (not sampled) set of structures inside a boundary."""
    name: str
    boundary: Ring
    structures: tuple[Structure, ...]
    crs: str = "EPSG:4326"

    @property
    def n_structures(self) -> int:
        return len(self.structures)

    @property
    def total_footprint_m2(self) -> float:
        return sum(s.footprint_m2 for s in self.structures)

    def count_by_use(self) -> dict[UseClass, int]:
        counts: dict[UseClass, int] = {}
        for s in self.structures:
            counts[s.use] = counts.get(s.use, 0) + 1
        return counts
