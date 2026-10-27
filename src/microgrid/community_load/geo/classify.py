"""Map raw GIS building attributes onto microgrid's UseClass."""
from __future__ import annotations

from dataclasses import dataclass

from microgrid.core.types import UseClass

# Covers OpenStreetMap `building=*` values plus common municipal land-use words.
_USE_TABLE: dict[str, UseClass] = {
    **dict.fromkeys(
        ["house", "detached", "semidetached_house", "terrace", "rowhouse", "townhouse",
         "bungalow", "single_family", "sf", "residential"], UseClass.RESIDENTIAL_SF),
    **dict.fromkeys(
        ["apartments", "apartment", "dormitory", "multifamily", "multi_family", "mf",
         "condominium", "condo"], UseClass.RESIDENTIAL_MF),
    **dict.fromkeys(
        ["commercial", "retail", "office", "supermarket", "hotel", "restaurant",
         "mixed_use", "shop"], UseClass.COMMERCIAL),
    **dict.fromkeys(["industrial", "warehouse", "factory", "manufacture"], UseClass.INDUSTRIAL),
    **dict.fromkeys(
        ["school", "university", "college", "hospital", "civic", "government", "church",
         "public", "fire_station"], UseClass.INSTITUTIONAL),
    **dict.fromkeys(["garage", "garages", "shed", "carport", "roof", "hut", "greenhouse"],
                    UseClass.OTHER),
}


def classify_use(raw: object) -> UseClass:
    """Normalize a raw use/type string. Unrecognized or missing values are UNKNOWN, never guessed."""
    if raw is None:
        return UseClass.UNKNOWN
    key = str(raw).strip().lower().replace(" ", "_").replace("-", "_")
    return _USE_TABLE.get(key, UseClass.UNKNOWN)


@dataclass(frozen=True)
class AttributeMapping:
    """Which GeoJSON property holds each field. None means 'not available in this layer'."""
    id: str | None = "id"
    use: str | None = "building"
    stories: str | None = "building:levels"
    height_m: str | None = "height"
    units: str | None = "building:flats"
    year_built: str | None = "start_date"
