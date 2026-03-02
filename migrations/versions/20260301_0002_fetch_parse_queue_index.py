"""add fetch parse queue index

Revision ID: 20260301_0002
Revises: 20260301_0001
Create Date: 2026-03-01 00:30:00
"""
from __future__ import annotations

from alembic import op


revision = "20260301_0002"
down_revision = "20260301_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_fetches_status_code_parsed_at",
        "fetches",
        ["status_code", "parsed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_fetches_status_code_parsed_at", table_name="fetches")
