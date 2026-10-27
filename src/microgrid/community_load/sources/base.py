from __future__ import annotations

from typing import Protocol

from microgrid.core.types import LoadResult, Neighborhood


class LoadSource(Protocol):
    """Anything that can produce an hourly load for a Neighborhood."""

    def profile(self, neighborhood: Neighborhood) -> LoadResult: ...
