"""runs: result, error_message, cancel_requested (story 1.10, background worker)

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("runs") as batch:
        batch.add_column(sa.Column("result", sa.JSON(), nullable=True))
        batch.add_column(sa.Column("error_message", sa.String(length=500), nullable=True))
        batch.add_column(
            sa.Column("cancel_requested", sa.Boolean(), server_default=sa.false(), nullable=False),
        )


def downgrade() -> None:
    with op.batch_alter_table("runs") as batch:
        batch.drop_column("cancel_requested")
        batch.drop_column("error_message")
        batch.drop_column("result")
