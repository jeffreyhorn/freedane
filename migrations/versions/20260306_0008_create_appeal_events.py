"""create appeal events table

Revision ID: 20260306_0008
Revises: 20260306_0007
Create Date: 2026-03-06 00:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260306_0008"
down_revision = "20260306_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "appeal_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_system", sa.String(), nullable=False),
        sa.Column("source_file_name", sa.String(), nullable=False),
        sa.Column("source_file_sha256", sa.String(), nullable=False),
        sa.Column("source_row_number", sa.Integer(), nullable=False),
        sa.Column("source_headers", sa.JSON(), nullable=False),
        sa.Column("raw_row", sa.JSON(), nullable=False),
        sa.Column("import_status", sa.String(), nullable=False),
        sa.Column("import_error", sa.Text(), nullable=True),
        sa.Column("import_warnings", sa.JSON(), nullable=True),
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
        sa.Column("parcel_number_raw", sa.String(), nullable=True),
        sa.Column("parcel_number_norm", sa.String(), nullable=True),
        sa.Column("site_address_raw", sa.String(), nullable=True),
        sa.Column("site_address_norm", sa.String(), nullable=True),
        sa.Column("owner_name_raw", sa.String(), nullable=True),
        sa.Column("owner_name_norm", sa.String(), nullable=True),
        sa.Column("parcel_id", sa.String(), nullable=True),
        sa.Column("parcel_link_method", sa.String(), nullable=True),
        sa.Column("parcel_link_confidence", sa.Numeric(5, 4), nullable=True),
        sa.Column("appeal_number", sa.String(), nullable=True),
        sa.Column("docket_number", sa.String(), nullable=True),
        sa.Column("appeal_level_raw", sa.String(), nullable=True),
        sa.Column("appeal_level_norm", sa.String(), nullable=True),
        sa.Column("appeal_reason_raw", sa.Text(), nullable=True),
        sa.Column("appeal_reason_norm", sa.String(), nullable=True),
        sa.Column("filing_date", sa.Date(), nullable=True),
        sa.Column("hearing_date", sa.Date(), nullable=True),
        sa.Column("decision_date", sa.Date(), nullable=True),
        sa.Column("tax_year", sa.Integer(), nullable=True),
        sa.Column("outcome_raw", sa.String(), nullable=True),
        sa.Column("outcome_norm", sa.String(), nullable=True),
        sa.Column("assessed_value_before", sa.Numeric(14, 2), nullable=True),
        sa.Column("requested_assessed_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("decided_assessed_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("value_change_amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("representative_name", sa.String(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_index(
        "ix_appeal_events_source_file_sha256",
        "appeal_events",
        ["source_file_sha256"],
        unique=False,
    )
    op.create_index(
        "ux_appeal_events_source_file_sha256_source_row_number",
        "appeal_events",
        ["source_file_sha256", "source_row_number"],
        unique=True,
    )
    op.create_index(
        "ix_appeal_events_import_status",
        "appeal_events",
        ["import_status"],
        unique=False,
    )
    op.create_index(
        "ix_appeal_events_parcel_number_norm",
        "appeal_events",
        ["parcel_number_norm"],
        unique=False,
    )
    op.create_index(
        "ix_appeal_events_site_address_norm",
        "appeal_events",
        ["site_address_norm"],
        unique=False,
    )
    op.create_index(
        "ix_appeal_events_parcel_id",
        "appeal_events",
        ["parcel_id"],
        unique=False,
    )
    op.create_index(
        "ix_appeal_events_tax_year",
        "appeal_events",
        ["tax_year"],
        unique=False,
    )
    op.create_index(
        "ix_appeal_events_outcome_norm",
        "appeal_events",
        ["outcome_norm"],
        unique=False,
    )
    op.create_index(
        "ix_appeal_events_hearing_date",
        "appeal_events",
        ["hearing_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_appeal_events_hearing_date", table_name="appeal_events")
    op.drop_index("ix_appeal_events_outcome_norm", table_name="appeal_events")
    op.drop_index("ix_appeal_events_tax_year", table_name="appeal_events")
    op.drop_index("ix_appeal_events_parcel_id", table_name="appeal_events")
    op.drop_index("ix_appeal_events_site_address_norm", table_name="appeal_events")
    op.drop_index(
        "ix_appeal_events_parcel_number_norm",
        table_name="appeal_events",
    )
    op.drop_index("ix_appeal_events_import_status", table_name="appeal_events")
    op.drop_index(
        "ux_appeal_events_source_file_sha256_source_row_number",
        table_name="appeal_events",
    )
    op.drop_index("ix_appeal_events_source_file_sha256", table_name="appeal_events")
    op.drop_table("appeal_events")
