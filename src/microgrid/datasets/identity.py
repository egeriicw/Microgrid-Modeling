"""Dataset identity (FR-DATA-8).

A dataset is (product, release, weather, upgrade, state). ``upgrade`` here is the catalog's INTERNAL scenario key
(for example ``baseline`` or ``hvac_0005``), not the on-disk partition id; the catalog maps one to the other because
ResStock partitions are numeric and ComStock partitions are strings, and their meaning is release-specific.
"""

from __future__ import annotations

from dataclasses import dataclass

PRODUCTS = ("resstock", "comstock")


@dataclass(frozen=True, order=True)
class DatasetId:
    product: str
    release: str
    weather: str
    upgrade: str
    state: str

    def __str__(self) -> str:
        return f"{self.product}/{self.release}/{self.weather}/{self.upgrade}/{self.state}"
