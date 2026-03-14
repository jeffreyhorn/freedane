"""create case reviews table

Revision ID: 20260312_0013
Revises: 20260310_0012
Create Date: 2026-03-12 11:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260312_0013"
down_revision = "20260310_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "case_reviews",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("score_id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("feature_version", sa.String(), nullable=False),
        sa.Column("ruleset_version", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("disposition", sa.String(), nullable=True),
        sa.Column("reviewer", sa.String(), nullable=True),
        sa.Column("assigned_reviewer", sa.String(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column(
            "evidence_links_json",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'"),
        ),
        sa.Column(
            "created_at",
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
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'in_review', 'resolved', 'closed')",
            name="ck_case_reviews_status",
        ),
        sa.CheckConstraint(
            "disposition IS NULL OR disposition IN ("
            "'confirmed_issue', "
            "'false_positive', "
            "'inconclusive', "
            "'needs_field_review', "
            "'duplicate_case'"
            ")",
            name="ck_case_reviews_disposition",
        ),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.ForeignKeyConstraint(["score_id"], ["fraud_scores.id"]),
        sa.ForeignKeyConstraint(["run_id"], ["scoring_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_case_reviews_score_id_feature_version_ruleset_version",
        "case_reviews",
        ["score_id", "feature_version", "ruleset_version"],
        unique=True,
    )
    op.create_index(
        "ix_case_reviews_status_updated_at",
        "case_reviews",
        ["status", "updated_at"],
        unique=False,
    )
    op.create_index(
        "ix_case_reviews_disposition_reviewed_at",
        "case_reviews",
        ["disposition", "reviewed_at"],
        unique=False,
    )
    op.create_index(
        "ix_case_reviews_assigned_reviewer_status_updated_at",
        "case_reviews",
        ["assigned_reviewer", "status", "updated_at"],
        unique=False,
    )
    op.create_index(
        "ix_case_reviews_parcel_id_year",
        "case_reviews",
        ["parcel_id", "year"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_case_reviews_parcel_id_year", table_name="case_reviews")
    op.drop_index(
        "ix_case_reviews_assigned_reviewer_status_updated_at",
        table_name="case_reviews",
    )
    op.drop_index(
        "ix_case_reviews_disposition_reviewed_at",
        table_name="case_reviews",
    )
    op.drop_index("ix_case_reviews_status_updated_at", table_name="case_reviews")
    op.drop_index(
        "ux_case_reviews_score_id_feature_version_ruleset_version",
        table_name="case_reviews",
    )
    op.drop_table("case_reviews")
