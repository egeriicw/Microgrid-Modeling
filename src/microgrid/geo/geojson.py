"""Read building footprints from a GeoJSON FeatureCollection (WGS84)."""
from __future__ import annotations

import json
from pathlib import Path

from microgrid.geo.types import Ring, Structure
from microgrid.geo.classify import AttributeMapping, classify_use
from microgrid.geo.geometry import ring_area_m2, ring_centroid


def _num(v: object) -> float | None:
    try:
        return float(str(v).split()[0])  # tolerates "12 m"
    except (TypeError, ValueError, IndexError):
        return None


def _int(v: object) -> int | None:
    f = _num(v)
    return int(round(f)) if f is not None else None


def _year(v: object) -> int | None:
    y = _int(str(v)[:4]) if v is not None else None
    return y if y and 1600 <= y <= 2100 else None


def _outer_rings(geom: dict) -> list[Ring]:
    if geom.get("type") == "Polygon":
        return [tuple((float(x), float(y)) for x, y, *_ in geom["coordinates"][0])]
    if geom.get("type") == "MultiPolygon":
        return [tuple((float(x), float(y)) for x, y, *_ in poly[0]) for poly in geom["coordinates"]]
    return []


def read_structures(path: Path | str, mapping: AttributeMapping | None = None) -> list[Structure]:
    """Parse polygon features into Structures.

    Non-polygon features are skipped. A MultiPolygon yields one Structure per part
    (id suffixed `#n`). Footprint area is computed from geometry, never read from attributes.
    """
    mapping = mapping or AttributeMapping()
    data = json.loads(Path(path).read_text())
    out: list[Structure] = []
    for i, feat in enumerate(data.get("features", [])):
        props = feat.get("properties") or {}
        rings = _outer_rings(feat.get("geometry") or {})
        base_id = str(props.get(mapping.id, feat.get("id", i))) if mapping.id else str(i)
        for j, ring in enumerate(rings):
            area = ring_area_m2(ring)
            if area <= 0:
                continue
            get = lambda key: props.get(key) if key else None  # noqa: E731
            out.append(Structure(
                id=base_id if len(rings) == 1 else f"{base_id}#{j}",
                use=classify_use(get(mapping.use)),
                footprint_m2=area,
                centroid=ring_centroid(ring),
                stories=_int(get(mapping.stories)),
                height_m=_num(get(mapping.height_m)),
                units=_int(get(mapping.units)),
                year_built=_year(get(mapping.year_built)),
            ))
    return out
