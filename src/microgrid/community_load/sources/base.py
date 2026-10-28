from __future__ import annotations

from typing import Protocol

from microgrid.geo.types import Neighborhood
from microgrid.core.types import LoadResult


class LoadSource(Protocol):
    """Anything that can produce an hourly load for a Neighborhood."""

    def profile(self, neighborhood: Neighborhood) -> LoadResult: ...
