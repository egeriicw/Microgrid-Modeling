"""Small-area planar geometry on lon/lat rings, dependency-free.

Uses a local equirectangular projection about the ring's mean latitude. Error is
well under 0.5% for neighborhood-scale extents (a few km); swap in pyproj/shapely
behind these functions if larger extents or exact geodesy are ever required.
"""
from __future__ import annotations

import math

from microgrid.core.types import LonLat, Ring

_EARTH_R = 6_371_008.8  # mean Earth radius, m


def _project(ring: Ring, lat0: float) -> list[tuple[float, float]]:
    kx = math.radians(1.0) * _EARTH_R * math.cos(math.radians(lat0))
    ky = math.radians(1.0) * _EARTH_R
    # Offset to the first vertex: absolute metres (~1e7) would lose precision in the shoelace sums.
    lon0, lat_0 = ring[0]
    return [((lon - lon0) * kx, (lat - lat_0) * ky) for lon, lat in ring]


def ring_area_m2(ring: Ring) -> float:
    """Absolute planar area of a polygon ring in square metres."""
    if len(ring) < 3:
        return 0.0
    lat0 = sum(p[1] for p in ring) / len(ring)
    pts = _project(ring, lat0)
    s = 0.0
    for (x1, y1), (x2, y2) in zip(pts, pts[1:] + pts[:1]):
        s += x1 * y2 - x2 * y1
    return abs(s) / 2.0


def ring_centroid(ring: Ring) -> LonLat:
    """Area-weighted centroid; falls back to the vertex mean for degenerate rings."""
    lat0 = sum(p[1] for p in ring) / len(ring)
    pts = _project(ring, lat0)
    a = cx = cy = 0.0
    for (x1, y1), (x2, y2) in zip(pts, pts[1:] + pts[:1]):
        cross = x1 * y2 - x2 * y1
        a += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross
    if abs(a) < 1e-9:
        return (sum(p[0] for p in ring) / len(ring), lat0)
    a *= 0.5
    kx = math.radians(1.0) * _EARTH_R * math.cos(math.radians(lat0))
    ky = math.radians(1.0) * _EARTH_R
    lon0, lat_0 = ring[0]
    return (lon0 + cx / (6 * a) / kx, lat_0 + cy / (6 * a) / ky)


def point_in_ring(point: LonLat, ring: Ring) -> bool:
    """Ray-casting point-in-polygon; boundary points are not guaranteed either way."""
    x, y = point
    inside = False
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / (y2 - y1) + x1:
            inside = not inside
    return inside
