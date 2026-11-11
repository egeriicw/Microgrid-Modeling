"""Pool preview (FR-ENG-11 c): how many source buildings a category's filters leave, and when to worry.

Sampling with replacement from a tiny pool understates diversity (the same few buildings are drawn
repeatedly), so a small or empty pool is reported as a warning. Previews are pure counting: they never
draw random numbers and never change what the engine samples.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from microgrid.community_load.synthetic.archetype import _filter_comstock_category, _filter_resstock_category
from microgrid.community_load.synthetic.config import (
    ArchetypeFilterConfig,
    CommercialFilterConfig,
    ResidentialFilterConfig,
)

DEFAULT_MIN_POOL = 10


@dataclass(frozen=True)
class PoolPreview:
    matched: int  # source buildings left after all filters
    total: int  # rows in the characteristics table
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {"matched": self.matched, "total": self.total, "warnings": list(self.warnings)}


def preview_pool(
    chars_df: pd.DataFrame,
    filter_cfg: ResidentialFilterConfig | CommercialFilterConfig,
    min_pool: int = DEFAULT_MIN_POOL,
) -> PoolPreview:
    """Count the buildings one category's filters match; warn on zero or fewer than ``min_pool``."""
    if isinstance(filter_cfg, ResidentialFilterConfig):
        matched = len(_filter_resstock_category(chars_df, filter_cfg))
    elif isinstance(filter_cfg, CommercialFilterConfig):
        matched = len(_filter_comstock_category(chars_df, filter_cfg))
    else:
        raise TypeError(f"filter_cfg must be a Residential/CommercialFilterConfig, got {type(filter_cfg).__name__}")
    warnings: list[str] = []
    if matched == 0:
        warnings.append("no buildings match these filters; this category cannot be sampled")
    elif matched < min_pool:
        warnings.append(
            f"only {matched} buildings match (fewer than {min_pool}); sampling with replacement from a small pool "
            "understates diversity"
        )
    return PoolPreview(matched=matched, total=len(chars_df), warnings=warnings)


def preview_pools(
    resstock_chars: pd.DataFrame | None,
    comstock_chars: pd.DataFrame | None,
    filters: ArchetypeFilterConfig,
    min_pool: int | None = None,
) -> dict[str, PoolPreview]:
    """Preview every configured category, keyed ``"resstock/<name>"`` / ``"comstock/<name>"``.

    ``min_pool`` defaults to ``filters.min_pool_size``.
    """
    minimum = filters.min_pool_size if min_pool is None else min_pool
    out: dict[str, PoolPreview] = {}
    for kind, df, cats in (
        ("resstock", resstock_chars, filters.resstock),
        ("comstock", comstock_chars, filters.comstock),
    ):
        for name, cfg in cats.items():
            if df is None:
                raise ValueError(f"cannot preview {kind}/{name}: no {kind} characteristics supplied")
            out[f"{kind}/{name}"] = preview_pool(df, cfg, minimum)
    return out
