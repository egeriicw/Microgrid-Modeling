"""MVP spine tables (architecture-and-stack section 6). Portable types only (JSON, Uuid, Enum as string)."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, Boolean, CheckConstraint, DateTime, Enum, ForeignKey, Integer, String, Uuid, false, func
from sqlalchemy.orm import Mapped, mapped_column

from microgrid_api.db.base import Base


class Role(enum.StrEnum):
    admin = "admin"
    client = "client"
    demo = "demo"


class RunStatus(enum.StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


def _now() -> Any:
    return func.now()


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[Role] = mapped_column(
        Enum(Role, native_enum=False, length=16, create_constraint=True, validate_strings=True, name="role"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now(), nullable=False)


class Composition(Base):
    __tablename__ = "compositions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    owner_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    body: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    is_demo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=_now(), onupdate=_now(), nullable=False
    )


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    composition_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    seeds: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    engine_version: Mapped[str] = mapped_column(String(64), nullable=False)
    data_manifest_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=RunStatus.queued.value)
    progress: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now(), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Added in 0002 (story 1.10). `result` is RunResult.to_dict() and is set only on success; `error_message` is a
    # SAFE summary (never a raw exception message); `cancel_requested` is the cooperative cancel flag.
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(500), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=false())
    # Added in 0003: the API key subject that created the run. NULL for rows created before ownership existed
    # (visible to admins only).
    owner: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    # Added in 0004 (FR-ENG-11 e): what the user picked, by exposed-attribute key, and the configuration version used
    # to turn it into the effective (column-level) filters stored in composition_snapshot. NULL = no selections.
    filter_selections: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class RunEvent(Base):
    __tablename__ = "run_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), nullable=False, index=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now(), nullable=False)
    stage: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(String(1000), nullable=False, default="")
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)


class ExposedAttributeVersion(Base):
    """One immutable version of the exposed-attribute configuration (FR-ENG-11 b). Rows are only ever inserted.

    ``version`` is assigned by the service as latest + 1; the primary key makes two writers that raced from the same
    base version collide (one wins, the other gets a conflict). Version 0 is reserved for the packaged starter set and
    is never stored (CHECK version >= 1).
    """

    __tablename__ = "exposed_attribute_versions"
    __table_args__ = (CheckConstraint("version >= 1", name="version_positive"),)

    version: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    document: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    based_on: Mapped[int | None] = mapped_column(Integer, nullable=True)
    note: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_by: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now(), nullable=False)


class ExposedAttributeActivation(Base):
    """Append-only activation log. The active configuration is the newest row (highest id); ``version`` NULL means
    the packaged starter set. No rows at all also means the packaged starter set. The service sets ``id`` to the
    latest id + 1, so two activations made from the same state collide on the primary key (optimistic lock)."""

    __tablename__ = "exposed_attribute_activations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    version: Mapped[int | None] = mapped_column(
        ForeignKey(
            "exposed_attribute_versions.version", ondelete="RESTRICT", name="fk_exposed_attribute_activations_version"
        ),
        nullable=True,
    )
    activated_by: Mapped[str] = mapped_column(String(64), nullable=False)
    activated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now(), nullable=False)
