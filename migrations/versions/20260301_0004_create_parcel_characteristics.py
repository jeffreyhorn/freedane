"""create parcel_characteristics and parcel_lineage_links

Revision ID: 20260301_0004
Revises: 20260301_0003
Create Date: 2026-03-01 02:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260301_0004"
down_revision = "20260301_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "parcel_characteristics",
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("source_fetch_id", sa.Integer(), nullable=True),
        sa.Column("formatted_parcel_number", sa.String(), nullable=True),
        sa.Column("state_municipality_code", sa.String(), nullable=True),
        sa.Column("township", sa.String(), nullable=True),
        sa.Column("range", sa.String(), nullable=True),
        sa.Column("section", sa.String(), nullable=True),
        sa.Column("quarter_quarter", sa.String(), nullable=True),
        sa.Column("has_dcimap_link", sa.Boolean(), nullable=True),
        sa.Column("has_google_map_link", sa.Boolean(), nullable=True),
        sa.Column("has_bing_map_link", sa.Boolean(), nullable=True),
        sa.Column("current_assessment_year", sa.Integer(), nullable=True),
        sa.Column("current_valuation_classification", sa.String(), nullable=True),
        sa.Column("current_assessment_acres", sa.Numeric(10, 3), nullable=True),
        sa.Column("current_assessment_ratio", sa.Numeric(8, 4), nullable=True),
        sa.Column(
            "current_estimated_fair_market_value",
            sa.Numeric(14, 2),
            nullable=True,
        ),
        sa.Column("current_tax_info_available", sa.Boolean(), nullable=True),
        sa.Column("current_payment_history_available", sa.Boolean(), nullable=True),
        sa.Column("tax_jurisdiction_count", sa.Integer(), nullable=True),
        sa.Column("is_exempt_style_page", sa.Boolean(), nullable=True),
        sa.Column("has_empty_valuation_breakout", sa.Boolean(), nullable=True),
        sa.Column("has_empty_tax_section", sa.Boolean(), nullable=True),
        sa.Column(
            "built_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.ForeignKeyConstraint(["source_fetch_id"], ["fetches.id"]),
        sa.PrimaryKeyConstraint("parcel_id"),
    )
    op.create_index(
        "ix_parcel_characteristics_source_fetch_id",
        "parcel_characteristics",
        ["source_fetch_id"],
        unique=False,
    )
    op.create_index(
        "ix_parcel_characteristics_current_valuation_classification",
        "parcel_characteristics",
        ["current_valuation_classification"],
        unique=False,
    )
    op.create_index(
        "ix_parcel_characteristics_state_municipality_code",
        "parcel_characteristics",
        ["state_municipality_code"],
        unique=False,
    )

    op.create_table(
        "parcel_lineage_links",
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("related_parcel_id", sa.String(), nullable=False),
        sa.Column("relationship_type", sa.String(), nullable=False),
        sa.Column("source_fetch_id", sa.Integer(), nullable=True),
        sa.Column("related_parcel_status", sa.String(), nullable=True),
        sa.Column("relationship_note", sa.String(), nullable=True),
        sa.Column(
            "built_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.ForeignKeyConstraint(["source_fetch_id"], ["fetches.id"]),
        sa.PrimaryKeyConstraint("parcel_id", "related_parcel_id", "relationship_type"),
    )
    op.create_index(
        "ix_parcel_lineage_links_related_parcel_id",
        "parcel_lineage_links",
        ["related_parcel_id"],
        unique=False,
    )
    op.create_index(
        "ix_parcel_lineage_links_relationship_type",
        "parcel_lineage_links",
        ["relationship_type"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_parcel_lineage_links_relationship_type",
        table_name="parcel_lineage_links",
    )
    op.drop_index(
        "ix_parcel_lineage_links_related_parcel_id",
        table_name="parcel_lineage_links",
    )
    op.drop_table("parcel_lineage_links")

    op.drop_index(
        "ix_parcel_characteristics_state_municipality_code",
        table_name="parcel_characteristics",
    )
    op.drop_index(
        "ix_parcel_characteristics_current_valuation_classification",
        table_name="parcel_characteristics",
    )
    op.drop_index(
        "ix_parcel_characteristics_source_fetch_id",
        table_name="parcel_characteristics",
    )
    op.drop_table("parcel_characteristics")
