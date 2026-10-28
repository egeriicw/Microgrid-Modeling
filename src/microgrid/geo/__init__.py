"""Geography: the reference layer every other part of the planner builds on.

Defines where structures are (`Structure`, `Neighborhood`), reads footprint layers,
classifies use, and clips to a boundary. Load, DER, dispatch and economics all
take a Neighborhood; none of them own geography.
"""
from microgrid.geo.classify import AttributeMapping, classify_use
from microgrid.geo.geojson import read_structures
from microgrid.geo.neighborhood import build_neighborhood
from microgrid.geo.types import LonLat, Neighborhood, Ring, Structure, UseClass

__all__ = [
    "AttributeMapping", "LonLat", "Neighborhood", "Ring", "Structure", "UseClass",
    "build_neighborhood", "classify_use", "read_structures",
]
