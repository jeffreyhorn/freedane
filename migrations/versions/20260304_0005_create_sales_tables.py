"""create RETR sales tables

Revision ID: 20260304_0005
Revises: 20260301_0004
Create Date: 2026-03-04 00:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260304_0005"
down_revision = "20260301_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sales_transactions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_system", sa.String(), nullable=False),
        sa.Column("source_file_name", sa.String(), nullable=False),
        sa.Column("source_file_sha256", sa.String(), nullable=False),
        sa.Column("source_row_number", sa.Integer(), nullable=False),
        sa.Column("source_headers", sa.JSON(), nullable=False),
        sa.Column("raw_row", sa.JSON(), nullable=False),
        sa.Column("import_status", sa.String(), nullable=False),
        sa.Column("import_error", sa.Text(), nullable=True),
        sa.Column(
            "loaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("revenue_object_id", sa.String(), nullable=True),
        sa.Column("document_number", sa.String(), nullable=True),
        sa.Column("county_name", sa.String(), nullable=True),
        sa.Column("municipality_name", sa.String(), nullable=True),
        sa.Column("transfer_date", sa.Date(), nullable=True),
        sa.Column("recording_date", sa.Date(), nullable=True),
        sa.Column("consideration_amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("property_type", sa.String(), nullable=True),
        sa.Column("conveyance_type", sa.String(), nullable=True),
        sa.Column("deed_type", sa.String(), nullable=True),
        sa.Column("grantor_name", sa.String(), nullable=True),
        sa.Column("grantee_name", sa.String(), nullable=True),
        sa.Column("official_parcel_number_raw", sa.String(), nullable=True),
        sa.Column("official_parcel_number_norm", sa.String(), nullable=True),
        sa.Column("property_address_raw", sa.String(), nullable=True),
        sa.Column("property_address_norm", sa.String(), nullable=True),
        sa.Column("legal_description_raw", sa.Text(), nullable=True),
        sa.Column("school_district_name", sa.String(), nullable=True),
        sa.Column("arms_length_indicator_raw", sa.String(), nullable=True),
        sa.Column("arms_length_indicator_norm", sa.Boolean(), nullable=True),
        sa.Column("usable_sale_indicator_raw", sa.String(), nullable=True),
        sa.Column("usable_sale_indicator_norm", sa.Boolean(), nullable=True),
        sa.Column(
            "review_status",
            sa.String(),
            server_default=sa.text("'unreviewed'"),
            nullable=False,
        ),
        sa.Column("review_notes", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_sales_transactions_source_file_sha256",
        "sales_transactions",
        ["source_file_sha256"],
        unique=False,
    )
    op.create_index(
        "ux_sales_transactions_source_file_sha256_source_row_number",
        "sales_transactions",
        ["source_file_sha256", "source_row_number"],
        unique=True,
    )
    op.create_index(
        "ix_sales_transactions_transfer_date",
        "sales_transactions",
        ["transfer_date"],
        unique=False,
    )
    op.create_index(
        "ix_sales_transactions_official_parcel_number_norm",
        "sales_transactions",
        ["official_parcel_number_norm"],
        unique=False,
    )
    op.create_index(
        "ix_sales_transactions_property_address_norm",
        "sales_transactions",
        ["property_address_norm"],
        unique=False,
    )
    op.create_index(
        "ix_sales_transactions_import_status",
        "sales_transactions",
        ["import_status"],
        unique=False,
    )

    op.create_table(
        "sales_parcel_matches",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("sales_transaction_id", sa.Integer(), nullable=False),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("match_method", sa.String(), nullable=False),
        sa.Column("confidence_score", sa.Numeric(5, 4), nullable=True),
        sa.Column("match_rank", sa.Integer(), nullable=True),
        sa.Column(
            "is_primary",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("match_review_status", sa.String(), nullable=False),
        sa.Column("matched_value", sa.String(), nullable=True),
        sa.Column("matcher_version", sa.String(), nullable=True),
        sa.Column(
            "matched_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["sales_transaction_id"],
            ["sales_transactions.id"],
        ),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_sales_parcel_matches_sales_transaction_id_parcel_id_match_method",
        "sales_parcel_matches",
        ["sales_transaction_id", "parcel_id", "match_method"],
        unique=True,
    )
    op.create_index(
        "ux_sales_parcel_matches_primary_per_transaction",
        "sales_parcel_matches",
        ["sales_transaction_id"],
        unique=True,
        sqlite_where=sa.text("is_primary = 1"),
        postgresql_where=sa.text("is_primary = TRUE"),
    )
    op.create_index(
        "ix_sales_parcel_matches_parcel_id",
        "sales_parcel_matches",
        ["parcel_id"],
        unique=False,
    )
    op.create_index(
        "ix_sales_parcel_matches_match_review_status",
        "sales_parcel_matches",
        ["match_review_status"],
        unique=False,
    )

    op.create_table(
        "sales_exclusions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("sales_transaction_id", sa.Integer(), nullable=False),
        sa.Column("exclusion_code", sa.String(), nullable=False),
        sa.Column("exclusion_reason", sa.Text(), nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column("excluded_by_rule", sa.String(), nullable=True),
        sa.Column(
            "excluded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["sales_transaction_id"],
            ["sales_transactions.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_sales_exclusions_sales_transaction_id_exclusion_code",
        "sales_exclusions",
        ["sales_transaction_id", "exclusion_code"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ux_sales_exclusions_sales_transaction_id_exclusion_code",
        table_name="sales_exclusions",
    )
    op.drop_table("sales_exclusions")

    op.drop_index(
        "ix_sales_parcel_matches_match_review_status",
        table_name="sales_parcel_matches",
    )
    op.drop_index(
        "ix_sales_parcel_matches_parcel_id",
        table_name="sales_parcel_matches",
    )
    op.drop_index(
        "ux_sales_parcel_matches_primary_per_transaction",
        table_name="sales_parcel_matches",
    )
    op.drop_index(
        "ux_sales_parcel_matches_sales_transaction_id_parcel_id_match_method",
        table_name="sales_parcel_matches",
    )
    op.drop_table("sales_parcel_matches")

    op.drop_index(
        "ix_sales_transactions_import_status",
        table_name="sales_transactions",
    )
    op.drop_index(
        "ix_sales_transactions_property_address_norm",
        table_name="sales_transactions",
    )
    op.drop_index(
        "ix_sales_transactions_official_parcel_number_norm",
        table_name="sales_transactions",
    )
    op.drop_index(
        "ix_sales_transactions_transfer_date",
        table_name="sales_transactions",
    )
    op.drop_index(
        "ux_sales_transactions_source_file_sha256_source_row_number",
        table_name="sales_transactions",
    )
    op.drop_index(
        "ix_sales_transactions_source_file_sha256",
        table_name="sales_transactions",
    )
    op.drop_table("sales_transactions")
