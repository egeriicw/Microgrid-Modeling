"""Request/response shapes for exposed attributes and filter selections, with size and shape limits.

These models only bound sizes and basic types (and reject NaN/infinity and unknown keys); the meaning is always checked
afterwards by the library (``ExposedAttributes.from_dict`` / ``to_filters``), so the two can never disagree silently.
Strict types are used so JSON ``"true"`` or ``"1"`` is not coerced into a boolean or number the library would reject.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StringConstraints

MAX_ATTRIBUTES = 100
MAX_CHOICES = 500
MAX_TEXT = 200
MAX_HELP = 2000
MAX_BUILDING_TYPES = 50
MAX_CATEGORIES = 50
MAX_SELECTIONS_PER_CATEGORY = 50
MAX_VALUES = 500
MAX_NOTE = 500
MAX_VERSION = 2**31 - 1

KEY_PATTERN = r"^[a-z][a-z0-9_]{0,63}$"
# Characteristics column names (e.g. "in.sqft..ft2"): letters, digits, "_", ".", "-". No slashes, so no path shapes.
COLUMN_PATTERN = r"^[A-Za-z0-9_][A-Za-z0-9_.\-]{0,127}$"
NAME_PATTERN = r"^[A-Za-z0-9_][A-Za-z0-9_\-]{0,63}$"

Operator = Literal["equals", "in", "range", "not_null", "is_null", "is_true", "is_false"]
Kind = Literal["numeric", "categorical", "boolean"]
Source = Literal["resstock", "comstock"]

AttributeKey = Annotated[str, StringConstraints(strict=True, pattern=KEY_PATTERN)]
ColumnName = Annotated[str, StringConstraints(strict=True, pattern=COLUMN_PATTERN)]
Name = Annotated[str, StringConstraints(strict=True, pattern=NAME_PATTERN)]
Text = Annotated[str, StringConstraints(strict=True, max_length=MAX_TEXT)]
#: Bound for every number in documents and selections. Far beyond any building characteristic, exactly representable
#: as a float, and small enough that no int/float conversion in the library can overflow (10**400 is a 422, not a 500).
MAX_ABS_NUMBER = 10**15
BoundedInt = Annotated[StrictInt, Field(ge=-MAX_ABS_NUMBER, le=MAX_ABS_NUMBER)]
FiniteFloat = Annotated[float, Field(strict=True, allow_inf_nan=False, ge=-MAX_ABS_NUMBER, le=MAX_ABS_NUMBER)]
Number = BoundedInt | FiniteFloat
Scalar = StrictBool | BoundedInt | FiniteFloat | Text


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExposedAttributeIn(_Strict):
    """One entry of the exposed-attribute document (same fields as ``exposed_attributes.yaml``)."""

    key: AttributeKey
    label: Annotated[str, StringConstraints(strict=True, min_length=1, max_length=MAX_TEXT)]
    help: Annotated[str, StringConstraints(strict=True, max_length=MAX_HELP)] = ""
    kind: Kind
    columns: Annotated[dict[Source, ColumnName], Field(min_length=1, max_length=2)]
    operators: Annotated[list[Operator], Field(min_length=1, max_length=7)]
    choices: Annotated[list[Text], Field(min_length=1, max_length=MAX_CHOICES)] | None = None
    min: Number | None = None
    max: Number | None = None
    building_types: Annotated[list[Name], Field(max_length=MAX_BUILDING_TYPES)] = []
    order: Annotated[StrictInt, Field(ge=-1_000_000, le=1_000_000)] = 100
    active: StrictBool = True


class ExposedAttributesDocument(_Strict):
    """A full configuration document. ``version`` is the document format (always 1), not the stored version number."""

    version: Literal[1]
    attributes: Annotated[list[ExposedAttributeIn], Field(max_length=MAX_ATTRIBUTES)]

    def to_plain(self) -> dict[str, Any]:
        """The document exactly as sent (unset optional fields stay absent, as in the YAML file)."""
        return self.model_dump(mode="json", exclude_unset=True)


class Selection(_Strict):
    """One user selection on an exposed attribute. There is deliberately no ``column`` field."""

    attribute: AttributeKey
    op: Operator
    value: Scalar | None = None
    values: Annotated[list[Scalar], Field(max_length=MAX_VALUES)] | None = None
    min: Number | None = None
    max: Number | None = None
    min_inclusive: StrictBool | None = None
    max_inclusive: StrictBool | None = None


#: {category name: [selection, ...]} where the category is a key of ``composition.categories``.
FilterSelections = Annotated[
    dict[Name, Annotated[list[Selection], Field(max_length=MAX_SELECTIONS_PER_CATEGORY)]],
    Field(max_length=MAX_CATEGORIES),
]


def selections_to_plain(selections: dict[str, list[Selection]]) -> dict[str, list[dict[str, Any]]]:
    return {name: [s.model_dump(mode="json", exclude_unset=True) for s in sels] for name, sels in selections.items()}


# --- requests ------------------------------------------------------------------------------------------------------


class VersionCreate(_Strict):
    base_version: Annotated[StrictInt, Field(ge=0, le=MAX_VERSION)] = Field(
        description="The latest version number you started from (0 when none is stored). A stale value is a 409."
    )
    document: ExposedAttributesDocument
    note: Annotated[str, StringConstraints(strict=True, max_length=MAX_NOTE)] | None = None
    activate: StrictBool = False


class ActivateRequest(_Strict):
    expected_active_version: Annotated[StrictInt, Field(ge=0, le=MAX_VERSION)] | None = Field(
        default=None,
        description="The version you believe is active now (0 = packaged). If another admin activated something else "
        "meanwhile, the request fails with 409 and nothing changes. Omit it for last-writer-wins.",
    )


class PreviewRequest(_Strict):
    """A scenario config WITHOUT a `data` section (the server adds it) plus optional selections per category."""

    config: dict[str, Any]
    filter_selections: FilterSelections | None = None


# --- responses -----------------------------------------------------------------------------------------------------


class OfferedAttribute(BaseModel):
    key: str
    label: str
    help: str
    kind: Kind
    operators: list[Operator]
    choices: list[str] | None
    min: float | None
    max: float | None
    building_types: list[str]
    sources: list[Source]
    order: int


class ActiveConfigurationOut(BaseModel):
    version: int = Field(description="0 = the packaged starter set (nothing stored or activated yet)")
    source: Literal["packaged", "database"]
    activated_at: datetime | None
    attributes: list[OfferedAttribute] = Field(description="Active attributes only, in display order")


class VersionOut(BaseModel):
    version: int
    based_on: int | None
    note: str | None
    created_by: str | None = Field(description="None for version 0 (packaged)")
    created_at: datetime | None
    active: bool


class VersionDetail(VersionOut):
    document: ExposedAttributesDocument


class VersionList(BaseModel):
    items: list[VersionOut]
    total: int
    limit: int
    offset: int
    latest_version: int
    active_version: int


class PoolPreviewOut(BaseModel):
    matched: int
    total: int
    warnings: list[str]


class PreviewOut(BaseModel):
    exposed_attributes_version: int
    min_pool_size: int
    categories: dict[str, PoolPreviewOut] = Field(description='Keyed "resstock/<category>" or "comstock/<category>"')
