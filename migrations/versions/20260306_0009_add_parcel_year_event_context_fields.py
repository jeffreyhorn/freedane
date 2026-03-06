"""add parcel_year_facts permit and appeal context fields

Revision ID: 20260306_0009
Revises: 20260306_0008
Create Date: 2026-03-06 21:15:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260306_0009"
down_revision = "20260306_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("parcel_year_facts") as batch:
        batch.add_column(sa.Column("permit_event_count", sa.Integer(), nullable=True))
        batch.add_column(
            sa.Column(
                "permit_declared_valuation_known_count", sa.Integer(), nullable=True
            )
        )
        batch.add_column(
            sa.Column("permit_declared_valuation_sum", sa.Numeric(14, 2), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_estimated_cost_known_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_estimated_cost_sum", sa.Numeric(14, 2), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_status_applied_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_status_issued_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_status_finaled_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_status_cancelled_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_status_expired_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_status_unknown_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_recent_1y_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("permit_recent_2y_count", sa.Integer(), nullable=True)
        )
        batch.add_column(sa.Column("permit_has_recent_1y", sa.Boolean(), nullable=True))
        batch.add_column(sa.Column("permit_has_recent_2y", sa.Boolean(), nullable=True))
        batch.add_column(sa.Column("appeal_event_count", sa.Integer(), nullable=True))
        batch.add_column(
            sa.Column("appeal_reduction_granted_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("appeal_partial_reduction_count", sa.Integer(), nullable=True)
        )
        batch.add_column(sa.Column("appeal_denied_count", sa.Integer(), nullable=True))
        batch.add_column(
            sa.Column("appeal_withdrawn_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("appeal_dismissed_count", sa.Integer(), nullable=True)
        )
        batch.add_column(sa.Column("appeal_pending_count", sa.Integer(), nullable=True))
        batch.add_column(
            sa.Column("appeal_unknown_outcome_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("appeal_value_change_known_count", sa.Integer(), nullable=True)
        )
        batch.add_column(
            sa.Column("appeal_value_change_total", sa.Numeric(14, 2), nullable=True)
        )
        batch.add_column(
            sa.Column(
                "appeal_value_change_reduction_total",
                sa.Numeric(14, 2),
                nullable=True,
            )
        )
        batch.add_column(
            sa.Column(
                "appeal_value_change_increase_total",
                sa.Numeric(14, 2),
                nullable=True,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("parcel_year_facts") as batch:
        batch.drop_column("appeal_value_change_increase_total")
        batch.drop_column("appeal_value_change_reduction_total")
        batch.drop_column("appeal_value_change_total")
        batch.drop_column("appeal_value_change_known_count")
        batch.drop_column("appeal_unknown_outcome_count")
        batch.drop_column("appeal_pending_count")
        batch.drop_column("appeal_dismissed_count")
        batch.drop_column("appeal_withdrawn_count")
        batch.drop_column("appeal_denied_count")
        batch.drop_column("appeal_partial_reduction_count")
        batch.drop_column("appeal_reduction_granted_count")
        batch.drop_column("appeal_event_count")
        batch.drop_column("permit_has_recent_2y")
        batch.drop_column("permit_has_recent_1y")
        batch.drop_column("permit_recent_2y_count")
        batch.drop_column("permit_recent_1y_count")
        batch.drop_column("permit_status_unknown_count")
        batch.drop_column("permit_status_expired_count")
        batch.drop_column("permit_status_cancelled_count")
        batch.drop_column("permit_status_finaled_count")
        batch.drop_column("permit_status_issued_count")
        batch.drop_column("permit_status_applied_count")
        batch.drop_column("permit_estimated_cost_sum")
        batch.drop_column("permit_estimated_cost_known_count")
        batch.drop_column("permit_declared_valuation_sum")
        batch.drop_column("permit_declared_valuation_known_count")
        batch.drop_column("permit_event_count")
