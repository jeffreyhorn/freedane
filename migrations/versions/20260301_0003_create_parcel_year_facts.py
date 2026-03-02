"""create parcel_year_facts

Revision ID: 20260301_0003
Revises: 20260301_0002
Create Date: 2026-03-01 01:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260301_0003"
down_revision = "20260301_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "parcel_year_facts",
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("parcel_summary_fetch_id", sa.Integer(), nullable=True),
        sa.Column("assessment_fetch_id", sa.Integer(), nullable=True),
        sa.Column("tax_fetch_id", sa.Integer(), nullable=True),
        sa.Column("payment_fetch_id", sa.Integer(), nullable=True),
        sa.Column("municipality_name", sa.String(), nullable=True),
        sa.Column("current_parcel_description", sa.String(), nullable=True),
        sa.Column("current_owner_name", sa.String(), nullable=True),
        sa.Column("current_primary_address", sa.String(), nullable=True),
        sa.Column("current_billing_address", sa.String(), nullable=True),
        sa.Column("assessment_valuation_classification", sa.String(), nullable=True),
        sa.Column("assessment_acres", sa.Numeric(10, 3), nullable=True),
        sa.Column("assessment_land_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("assessment_improved_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("assessment_total_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("assessment_average_assessment_ratio", sa.Numeric(8, 4), nullable=True),
        sa.Column(
            "assessment_estimated_fair_market_value",
            sa.Numeric(14, 2),
            nullable=True,
        ),
        sa.Column("assessment_valuation_date", sa.Date(), nullable=True),
        sa.Column("tax_total_assessed_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("tax_assessed_land_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("tax_assessed_improvement_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("tax_taxes", sa.Numeric(14, 2), nullable=True),
        sa.Column("tax_specials", sa.Numeric(14, 2), nullable=True),
        sa.Column("tax_first_dollar_credit", sa.Numeric(14, 2), nullable=True),
        sa.Column("tax_lottery_credit", sa.Numeric(14, 2), nullable=True),
        sa.Column("tax_amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("payment_event_count", sa.Integer(), nullable=True),
        sa.Column("payment_total_amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("payment_first_date", sa.Date(), nullable=True),
        sa.Column("payment_last_date", sa.Date(), nullable=True),
        sa.Column("payment_has_placeholder_row", sa.Boolean(), nullable=True),
        sa.Column(
            "built_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["assessment_fetch_id"], ["fetches.id"]),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.ForeignKeyConstraint(["parcel_summary_fetch_id"], ["fetches.id"]),
        sa.ForeignKeyConstraint(["payment_fetch_id"], ["fetches.id"]),
        sa.ForeignKeyConstraint(["tax_fetch_id"], ["fetches.id"]),
        sa.PrimaryKeyConstraint("parcel_id", "year"),
    )
    op.create_index(
        "ix_parcel_year_facts_current_owner_name",
        "parcel_year_facts",
        ["current_owner_name"],
        unique=False,
    )
    op.create_index("ix_parcel_year_facts_year", "parcel_year_facts", ["year"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_parcel_year_facts_year", table_name="parcel_year_facts")
    op.drop_index("ix_parcel_year_facts_current_owner_name", table_name="parcel_year_facts")
    op.drop_table("parcel_year_facts")
