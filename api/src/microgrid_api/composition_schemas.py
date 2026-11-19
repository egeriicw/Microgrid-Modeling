"""Request shape for `POST /runs` with a client-friendly `composition` (FR-WEB-1, FR-ENG-2).

Only sizes, types and the per-entry mode (fixed count XOR fraction range) are checked here; the registry check
(known, active, not repeated) and the count/fraction feasibility are in ``services.compositions`` and the engine's
strict parser checks the built scenario again. Strict types: JSON ``"3"`` or ``true`` is not a count, NaN and infinity
are refused. Unknown keys are refused at every level, so no filter, column, path or `data` key can ride along.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StringConstraints, model_validator

from microgrid_api.filter_schemas import KEY_PATTERN, MAX_CATEGORIES
from microgrid_api.services.compositions import Composition, Entry

#: Upper bound for any count in a request (the server caps MICROGRID_MAX_TOTAL_BUILDINGS / _MAX_ENSEMBLE_RUNS are
#: the real limits and are checked afterwards). Keeps absurd numbers a plain 422.
MAX_COUNT = 1_000_000
MAX_NAME = 120

TypeKey = Annotated[str, StringConstraints(strict=True, pattern=KEY_PATTERN)]
Count = Annotated[int, Field(strict=True, ge=0, le=MAX_COUNT)]
PositiveCount = Annotated[int, Field(strict=True, ge=1, le=MAX_COUNT)]
Fraction = Annotated[float, Field(strict=True, ge=0.0, le=1.0, allow_inf_nan=False)]
CompositionName = Annotated[
    str, StringConstraints(strict=True, min_length=1, max_length=MAX_NAME, pattern=r"^[^\x00-\x1f\x7f]+$")
]
ConvergenceRule = Literal["legacy_window", "prd_default"]  # the engine's CONVERGENCE_RULES (checked by a test)


class CompositionEntry(BaseModel):
    """One building type: EITHER ``fixed_count`` (exactly this many) OR a fraction range of the buildings left after
    the fixed counts (``min_fraction`` defaults to 0, ``max_fraction`` to 1; give at least one)."""

    model_config = ConfigDict(extra="forbid")

    building_type: TypeKey
    fixed_count: Count | None = None
    min_fraction: Fraction | None = None
    max_fraction: Fraction | None = None

    @model_validator(mode="after")
    def _one_mode(self) -> CompositionEntry:
        ranged = self.min_fraction is not None or self.max_fraction is not None
        if (self.fixed_count is not None) == ranged:
            raise ValueError("give either fixed_count or min_fraction/max_fraction for each building type")
        lo = 0.0 if self.min_fraction is None else self.min_fraction
        hi = 1.0 if self.max_fraction is None else self.max_fraction
        if ranged and lo > hi:
            raise ValueError("min_fraction must not exceed max_fraction")
        return self


class CompositionRequest(BaseModel):
    """A neighbourhood by building type plus run settings. The server builds the engine scenario from the registry;
    filters, columns and paths are never accepted here."""

    model_config = ConfigDict(extra="forbid")

    name: CompositionName
    total_buildings: PositiveCount
    building_types: list[CompositionEntry] = Field(min_length=1, max_length=MAX_CATEGORIES)
    max_runs: PositiveCount
    # Off: a fixed ensemble of max_runs runs. On: adaptive mode, stopping early when the engine's convergence rule
    # holds (rule and min_runs as given, otherwise the engine's defaults).
    convergence: StrictBool = False
    convergence_rule: ConvergenceRule | None = None
    min_runs: PositiveCount | None = None

    @model_validator(mode="after")
    def _convergence_settings(self) -> CompositionRequest:
        if not self.convergence and (self.convergence_rule is not None or self.min_runs is not None):
            raise ValueError("convergence_rule and min_runs only apply when convergence is true")
        return self

    def to_composition(self) -> Composition:
        return Composition(
            name=self.name,
            total_buildings=self.total_buildings,
            entries=tuple(
                Entry(e.building_type, e.fixed_count, e.min_fraction, e.max_fraction) for e in self.building_types
            ),
            max_runs=self.max_runs,
            convergence=self.convergence,
            convergence_rule=self.convergence_rule,
            min_runs=self.min_runs,
        )
