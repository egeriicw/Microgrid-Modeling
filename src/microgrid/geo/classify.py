"""Map raw GIS building attributes onto microgrid's UseClass."""

from __future__ import annotations

from dataclasses import dataclass

from microgrid.geo.types import UseClass

# Covers OpenStreetMap `building=*` values plus common municipal land-use words.
_USE_TABLE: dict[str, UseClass] = {
    **dict.fromkeys(
        [
            "house",
            "detached",
            "bungalow",
            "single_family",
            "sf",
            "residential",
        ],
        UseClass.RESIDENTIAL_SF,
    ),
    **dict.fromkeys(
        ["semidetached_house", "terrace", "rowhouse", "townhouse", "attached"],
        UseClass.RESIDENTIAL_SFA,
    ),
    **dict.fromkeys(
        ["apartments", "apartment", "dormitory", "multifamily", "multi_family", "mf", "condominium", "condo"],
        UseClass.RESIDENTIAL_MF,
    ),
    **dict.fromkeys(
        ["commercial", "retail", "office", "supermarket", "hotel", "restaurant", "mixed_use", "shop"],
        UseClass.COMMERCIAL,
    ),
    **dict.fromkeys(["warehouse", "storage"], UseClass.WAREHOUSE),
    **dict.fromkeys(["industrial", "factory", "manufacture"], UseClass.INDUSTRIAL),
    **dict.fromkeys(
        ["school", "university", "college", "hospital", "civic", "government", "church", "public", "fire_station"],
        UseClass.INSTITUTIONAL,
    ),
    **dict.fromkeys(["garage", "garages", "shed", "carport", "roof", "hut", "greenhouse"], UseClass.OTHER),
}


def normalize_label(raw: object) -> str | None:
    """Lower-case, underscore-separated form of a raw use label (None when missing)."""
    if raw is None:
        return None
    key = str(raw).strip().lower().replace(" ", "_").replace("-", "_")
    return key or None


def classify_use(raw: object) -> UseClass:
    """Normalize a raw use/type string. Unrecognized or missing values are UNKNOWN, never guessed."""
    key = normalize_label(raw)
    return _USE_TABLE.get(key, UseClass.UNKNOWN) if key else UseClass.UNKNOWN


@dataclass(frozen=True)
class AttributeMapping:
    """Which GeoJSON property holds each field. None means 'not available in this layer'."""

    id: str | None = "id"
    use: str | None = "building"
    stories: str | None = "building:levels"
    height_m: str | None = "height"
    units: str | None = "building:flats"
    year_built: str | None = "start_date"
