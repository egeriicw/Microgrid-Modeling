"""Build a Neighborhood from a footprint layer and a boundary."""
from __future__ import annotations

from pathlib import Path

from mgplan.core.types import Neighborhood, Ring
from mgplan.geo.classify import AttributeMapping
from mgplan.geo.geojson import read_structures
from mgplan.geo.geometry import point_in_ring


def build_neighborhood(
    name: str,
    structures_path: Path | str,
    boundary: Ring,
    mapping: AttributeMapping | None = None,
) -> Neighborhood:
    """Keep structures whose centroid falls inside `boundary` (lon/lat ring)."""
    if len(boundary) < 3:
        raise ValueError("boundary needs at least 3 vertices")
    inside = tuple(
        s for s in read_structures(structures_path, mapping) if point_in_ring(s.centroid, boundary)
    )
    return Neighborhood(name=name, boundary=boundary, structures=inside)
