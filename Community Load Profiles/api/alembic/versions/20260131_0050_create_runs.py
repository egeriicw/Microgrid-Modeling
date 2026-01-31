"""create runs table

Revision ID: 20260131_0050
Revises: 20260131_0036
Create Date: 2026-01-31
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260131_0050"
down_revision = "20260131_0036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "runs",
        sa.Column("run_id", sa.String(length=36), primary_key=True),
        sa.Column("config_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="queued"),
        sa.Column("progress_current", sa.Integer(), nullable=True),
        sa.Column("progress_total", sa.Integer(), nullable=True),
        sa.Column("progress_message", sa.String(length=500), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("started_at", sa.Integer(), nullable=True),
        sa.Column("finished_at", sa.Integer(), nullable=True),
        sa.Column("log_text", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("runs")
