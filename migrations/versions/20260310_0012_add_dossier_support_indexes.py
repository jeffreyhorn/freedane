"""add dossier support indexes

Revision ID: 20260310_0012
Revises: 20260307_0011
Create Date: 2026-03-10 18:00:00
"""

from __future__ import annotations

from alembic import op

revision = "20260310_0012"
down_revision = "20260307_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_assessments_parcel_id_year_id",
        "assessments",
        ["parcel_id", "year", "id"],
        unique=False,
    )
    op.create_index(
        "ix_sales_parcel_matches_parcel_id_sales_transaction_id",
        "sales_parcel_matches",
        ["parcel_id", "sales_transaction_id"],
        unique=False,
    )
    op.create_index(
        "ix_permit_events_parcel_id_permit_year",
        "permit_events",
        ["parcel_id", "permit_year"],
        unique=False,
    )
    op.create_index(
        "ix_appeal_events_parcel_id_tax_year",
        "appeal_events",
        ["parcel_id", "tax_year"],
        unique=False,
    )
    op.create_index(
        "ix_fraud_flags_score_id_reason_rank",
        "fraud_flags",
        ["score_id", "reason_rank"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_fraud_flags_score_id_reason_rank", table_name="fraud_flags")
    op.drop_index("ix_appeal_events_parcel_id_tax_year", table_name="appeal_events")
    op.drop_index("ix_permit_events_parcel_id_permit_year", table_name="permit_events")
    op.drop_index(
        "ix_sales_parcel_matches_parcel_id_sales_transaction_id",
        table_name="sales_parcel_matches",
    )
    op.drop_index("ix_assessments_parcel_id_year_id", table_name="assessments")
