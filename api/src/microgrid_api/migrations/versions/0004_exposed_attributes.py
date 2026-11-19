"""exposed-attribute configuration versions and activations; runs.filter_selections (FR-ENG-11 b, e)

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-17

No seed rows: an empty table means "the packaged starter set is active" (fallback in the service), so this
migration never depends on the library's YAML content.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "exposed_attribute_versions",
        sa.Column("version", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("document", sa.JSON(), nullable=False),
        sa.Column("based_on", sa.Integer(), nullable=True),
        sa.Column("note", sa.String(length=500), nullable=True),
        sa.Column("created_by", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("version", name="pk_exposed_attribute_versions"),
        # version 0 is the packaged starter set and is never stored
        sa.CheckConstraint("version >= 1", name="ck_exposed_attribute_versions_version_positive"),
    )
    op.create_table(
        "exposed_attribute_activations",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=True),
        sa.Column("activated_by", sa.String(length=64), nullable=False),
        sa.Column("activated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["version"],
            ["exposed_attribute_versions.version"],
            name="fk_exposed_attribute_activations_version",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_exposed_attribute_activations"),
    )
    with op.batch_alter_table("runs") as batch:
        batch.add_column(sa.Column("filter_selections", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("runs") as batch:
        batch.drop_column("filter_selections")
    op.drop_table("exposed_attribute_activations")
    op.drop_table("exposed_attribute_versions")
