"""create permit events table

Revision ID: 20260306_0007
Revises: 20260304_0006
Create Date: 2026-03-06 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260306_0007"
down_revision = "20260304_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "permit_events",
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
        sa.Column("parcel_id", sa.String(), nullable=True),
        sa.Column("parcel_link_method", sa.String(), nullable=True),
        sa.Column("parcel_link_confidence", sa.Numeric(5, 4), nullable=True),
        sa.Column("permit_number", sa.String(), nullable=True),
        sa.Column("issuing_jurisdiction", sa.String(), nullable=True),
        sa.Column("permit_type", sa.String(), nullable=True),
        sa.Column("permit_subtype", sa.String(), nullable=True),
        sa.Column("work_class", sa.String(), nullable=True),
        sa.Column("permit_status_raw", sa.String(), nullable=True),
        sa.Column("permit_status_norm", sa.String(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("owner_name", sa.String(), nullable=True),
        sa.Column("contractor_name", sa.String(), nullable=True),
        sa.Column("applied_date", sa.Date(), nullable=True),
        sa.Column("issued_date", sa.Date(), nullable=True),
        sa.Column("finaled_date", sa.Date(), nullable=True),
        sa.Column("status_date", sa.Date(), nullable=True),
        sa.Column("declared_valuation", sa.Numeric(14, 2), nullable=True),
        sa.Column("estimated_cost", sa.Numeric(14, 2), nullable=True),
        sa.Column("permit_year", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_permit_events_source_file_sha256",
        "permit_events",
        ["source_file_sha256"],
        unique=False,
    )
    op.create_index(
        "ux_permit_events_source_file_sha256_source_row_number",
        "permit_events",
        ["source_file_sha256", "source_row_number"],
        unique=True,
    )
    op.create_index(
        "ix_permit_events_import_status",
        "permit_events",
        ["import_status"],
        unique=False,
    )
    op.create_index(
        "ix_permit_events_parcel_number_norm",
        "permit_events",
        ["parcel_number_norm"],
        unique=False,
    )
    op.create_index(
        "ix_permit_events_site_address_norm",
        "permit_events",
        ["site_address_norm"],
        unique=False,
    )
    op.create_index(
        "ix_permit_events_parcel_id",
        "permit_events",
        ["parcel_id"],
        unique=False,
    )
    op.create_index(
        "ix_permit_events_permit_year",
        "permit_events",
        ["permit_year"],
        unique=False,
    )
    op.create_index(
        "ix_permit_events_permit_status_norm",
        "permit_events",
        ["permit_status_norm"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_permit_events_permit_status_norm", table_name="permit_events")
    op.drop_index("ix_permit_events_permit_year", table_name="permit_events")
    op.drop_index("ix_permit_events_parcel_id", table_name="permit_events")
    op.drop_index("ix_permit_events_site_address_norm", table_name="permit_events")
    op.drop_index("ix_permit_events_parcel_number_norm", table_name="permit_events")
    op.drop_index("ix_permit_events_import_status", table_name="permit_events")
    op.drop_index(
        "ux_permit_events_source_file_sha256_source_row_number",
        table_name="permit_events",
    )
    op.drop_index("ix_permit_events_source_file_sha256", table_name="permit_events")
    op.drop_table("permit_events")
