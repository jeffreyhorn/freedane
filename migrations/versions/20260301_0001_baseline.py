"""baseline schema

Revision ID: 20260301_0001
Revises:
Create Date: 2026-03-01 00:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260301_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "parcels",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("trs_code", sa.String(), nullable=True),
        sa.Column("section", sa.Integer(), nullable=True),
        sa.Column("subsection", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_parcels_trs_code"), "parcels", ["trs_code"], unique=False)
    op.create_index(op.f("ix_parcels_section"), "parcels", ["section"], unique=False)

    op.create_table(
        "fetches",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("url", sa.String(), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("raw_path", sa.String(), nullable=True),
        sa.Column("raw_sha256", sa.String(), nullable=True),
        sa.Column("raw_size", sa.Integer(), nullable=True),
        sa.Column("parsed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("parse_error", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_fetches_parcel_id"), "fetches", ["parcel_id"], unique=False)
    op.create_index(op.f("ix_fetches_fetched_at"), "fetches", ["fetched_at"], unique=False)
    op.create_index(op.f("ix_fetches_raw_sha256"), "fetches", ["raw_sha256"], unique=False)

    op.create_table(
        "assessments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("fetch_id", sa.Integer(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("valuation_classification", sa.String(), nullable=True),
        sa.Column("assessment_acres", sa.Numeric(10, 3), nullable=True),
        sa.Column("land_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("improved_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("total_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("average_assessment_ratio", sa.Numeric(8, 4), nullable=True),
        sa.Column("estimated_fair_market_value", sa.Numeric(14, 2), nullable=True),
        sa.Column("valuation_date", sa.Date(), nullable=True),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["fetch_id"], ["fetches.id"]),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_assessments_parcel_id"), "assessments", ["parcel_id"], unique=False
    )
    op.create_index(op.f("ix_assessments_fetch_id"), "assessments", ["fetch_id"], unique=False)

    op.create_table(
        "taxes",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("fetch_id", sa.Integer(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["fetch_id"], ["fetches.id"]),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_taxes_parcel_id"), "taxes", ["parcel_id"], unique=False)
    op.create_index(op.f("ix_taxes_fetch_id"), "taxes", ["fetch_id"], unique=False)

    op.create_table(
        "payments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("fetch_id", sa.Integer(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["fetch_id"], ["fetches.id"]),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_payments_parcel_id"), "payments", ["parcel_id"], unique=False)
    op.create_index(op.f("ix_payments_fetch_id"), "payments", ["fetch_id"], unique=False)

    op.create_table(
        "parcel_summaries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("fetch_id", sa.Integer(), nullable=False),
        sa.Column("municipality_name", sa.String(), nullable=True),
        sa.Column("parcel_description", sa.String(), nullable=True),
        sa.Column("owner_name", sa.String(), nullable=True),
        sa.Column("primary_address", sa.String(), nullable=True),
        sa.Column("billing_address", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["fetch_id"], ["fetches.id"]),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("parcel_id", name="uq_parcel_summaries_parcel_id"),
    )
    op.create_index(
        op.f("ix_parcel_summaries_fetch_id"), "parcel_summaries", ["fetch_id"], unique=False
    )
    op.create_index(
        op.f("ix_parcel_summaries_parcel_id"), "parcel_summaries", ["parcel_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_parcel_summaries_parcel_id"), table_name="parcel_summaries")
    op.drop_index(op.f("ix_parcel_summaries_fetch_id"), table_name="parcel_summaries")
    op.drop_table("parcel_summaries")

    op.drop_index(op.f("ix_payments_fetch_id"), table_name="payments")
    op.drop_index(op.f("ix_payments_parcel_id"), table_name="payments")
    op.drop_table("payments")

    op.drop_index(op.f("ix_taxes_fetch_id"), table_name="taxes")
    op.drop_index(op.f("ix_taxes_parcel_id"), table_name="taxes")
    op.drop_table("taxes")

    op.drop_index(op.f("ix_assessments_fetch_id"), table_name="assessments")
    op.drop_index(op.f("ix_assessments_parcel_id"), table_name="assessments")
    op.drop_table("assessments")

    op.drop_index(op.f("ix_fetches_raw_sha256"), table_name="fetches")
    op.drop_index(op.f("ix_fetches_fetched_at"), table_name="fetches")
    op.drop_index(op.f("ix_fetches_parcel_id"), table_name="fetches")
    op.drop_table("fetches")

    op.drop_index(op.f("ix_parcels_section"), table_name="parcels")
    op.drop_index(op.f("ix_parcels_trs_code"), table_name="parcels")
    op.drop_table("parcels")
