"""runs.owner: the API key subject that created the run (HTTP run endpoints, ownership checks)

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("runs") as batch:
        batch.add_column(sa.Column("owner", sa.String(length=64), nullable=True))
        batch.create_index("ix_runs_owner", ["owner"])


def downgrade() -> None:
    with op.batch_alter_table("runs") as batch:
        batch.drop_index("ix_runs_owner")
        batch.drop_column("owner")
