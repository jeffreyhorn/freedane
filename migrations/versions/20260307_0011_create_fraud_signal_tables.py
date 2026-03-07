"""create fraud signal tables

Revision ID: 20260307_0011
Revises: 20260307_0010
Create Date: 2026-03-07 03:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260307_0011"
down_revision = "20260307_0010"
branch_labels = None
depends_on = None


def _create_fraud_flag_consistency_triggers() -> None:
    dialect = op.get_bind().dialect.name
    if dialect == "postgresql":
        op.execute(
            """
            CREATE FUNCTION enforce_fraud_flag_score_consistency()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM fraud_scores s
                    WHERE s.id = NEW.score_id
                      AND s.parcel_id = NEW.parcel_id
                      AND s.year = NEW.year
                      AND s.ruleset_version = NEW.ruleset_version
                ) THEN
                    RAISE EXCEPTION
                        'fraud_flags row must match parent fraud_scores parcel_id/year/ruleset_version';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        )
        op.execute(
            """
            CREATE TRIGGER trg_fraud_flags_score_consistency
            BEFORE INSERT OR UPDATE OF score_id, parcel_id, year, ruleset_version
            ON fraud_flags
            FOR EACH ROW
            EXECUTE FUNCTION enforce_fraud_flag_score_consistency();
            """
        )
        return

    op.execute(
        """
        CREATE TRIGGER trg_fraud_flags_consistency_insert
        BEFORE INSERT ON fraud_flags
        FOR EACH ROW
        BEGIN
            SELECT
                CASE
                    WHEN NOT EXISTS (
                        SELECT 1
                        FROM fraud_scores s
                        WHERE s.id = NEW.score_id
                          AND s.parcel_id = NEW.parcel_id
                          AND s.year = NEW.year
                          AND s.ruleset_version = NEW.ruleset_version
                    ) THEN
                        RAISE(
                            ABORT,
                            'fraud_flags row must match parent fraud_scores parcel_id/year/ruleset_version'
                        )
                END;
        END;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_fraud_flags_consistency_update
        BEFORE UPDATE OF score_id, parcel_id, year, ruleset_version ON fraud_flags
        FOR EACH ROW
        BEGIN
            SELECT
                CASE
                    WHEN NOT EXISTS (
                        SELECT 1
                        FROM fraud_scores s
                        WHERE s.id = NEW.score_id
                          AND s.parcel_id = NEW.parcel_id
                          AND s.year = NEW.year
                          AND s.ruleset_version = NEW.ruleset_version
                    ) THEN
                        RAISE(
                            ABORT,
                            'fraud_flags row must match parent fraud_scores parcel_id/year/ruleset_version'
                        )
                END;
        END;
        """
    )


def _drop_fraud_flag_consistency_triggers() -> None:
    dialect = op.get_bind().dialect.name
    if dialect == "postgresql":
        op.execute(
            "DROP TRIGGER IF EXISTS trg_fraud_flags_score_consistency ON fraud_flags"
        )
        op.execute("DROP FUNCTION IF EXISTS enforce_fraud_flag_score_consistency()")
        return

    op.execute("DROP TRIGGER IF EXISTS trg_fraud_flags_consistency_insert")
    op.execute("DROP TRIGGER IF EXISTS trg_fraud_flags_consistency_update")


def upgrade() -> None:
    op.create_table(
        "scoring_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_type", sa.String(), nullable=False),
        sa.Column(
            "status",
            sa.String(),
            server_default=sa.text("'running'"),
            nullable=False,
        ),
        sa.Column("version_tag", sa.String(), nullable=False),
        sa.Column("scope_hash", sa.String(), nullable=True),
        sa.Column("scope_json", sa.JSON(), nullable=False),
        sa.Column("config_json", sa.JSON(), nullable=False),
        sa.Column("input_summary_json", sa.JSON(), nullable=True),
        sa.Column("output_summary_json", sa.JSON(), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("parent_run_id", sa.Integer(), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "run_type IN ('sales_ratio_study', 'build_features', 'score_fraud')",
            name="ck_scoring_runs_run_type",
        ),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'failed')",
            name="ck_scoring_runs_status",
        ),
        sa.ForeignKeyConstraint(["parent_run_id"], ["scoring_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_scoring_runs_run_type_started_at",
        "scoring_runs",
        ["run_type", "started_at"],
        unique=False,
    )
    op.create_index(
        "ix_scoring_runs_status",
        "scoring_runs",
        ["status"],
        unique=False,
    )
    op.create_index(
        "ix_scoring_runs_version_tag",
        "scoring_runs",
        ["version_tag"],
        unique=False,
    )
    op.create_index(
        "ix_scoring_runs_run_type_version_tag_scope_hash",
        "scoring_runs",
        ["run_type", "version_tag", "scope_hash"],
        unique=False,
    )
    op.create_index(
        "ix_scoring_runs_parent_run_id",
        "scoring_runs",
        ["parent_run_id"],
        unique=False,
    )

    op.create_table(
        "parcel_features",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("feature_version", sa.String(), nullable=False),
        sa.Column("assessment_to_sale_ratio", sa.Numeric(10, 6), nullable=True),
        sa.Column("peer_percentile", sa.Numeric(6, 4), nullable=True),
        sa.Column("yoy_assessment_change_pct", sa.Numeric(10, 6), nullable=True),
        sa.Column("permit_adjusted_expected_change", sa.Numeric(14, 2), nullable=True),
        sa.Column("permit_adjusted_gap", sa.Numeric(14, 2), nullable=True),
        sa.Column("appeal_value_delta_3y", sa.Numeric(14, 2), nullable=True),
        sa.Column("appeal_success_rate_3y", sa.Numeric(6, 4), nullable=True),
        sa.Column("lineage_value_reset_delta", sa.Numeric(14, 2), nullable=True),
        sa.Column("feature_quality_flags", sa.JSON(), nullable=True),
        sa.Column("source_refs_json", sa.JSON(), nullable=False),
        sa.Column(
            "built_at",
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
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.ForeignKeyConstraint(["run_id"], ["scoring_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_parcel_features_parcel_id_year_feature_version",
        "parcel_features",
        ["parcel_id", "year", "feature_version"],
        unique=True,
    )
    op.create_index(
        "ix_parcel_features_feature_version_year",
        "parcel_features",
        ["feature_version", "year"],
        unique=False,
    )
    op.create_index(
        "ix_parcel_features_run_id",
        "parcel_features",
        ["run_id"],
        unique=False,
    )

    op.create_table(
        "fraud_scores",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("feature_run_id", sa.Integer(), nullable=True),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("ruleset_version", sa.String(), nullable=False),
        sa.Column("feature_version", sa.String(), nullable=False),
        sa.Column("score_value", sa.Numeric(6, 2), nullable=False),
        sa.Column("risk_band", sa.String(), nullable=False),
        sa.Column(
            "requires_review",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "reason_code_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("score_summary_json", sa.JSON(), nullable=False),
        sa.Column(
            "scored_at",
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
        sa.CheckConstraint(
            "risk_band IN ('high', 'medium', 'low')",
            name="ck_fraud_scores_risk_band",
        ),
        sa.CheckConstraint(
            "score_value >= 0 AND score_value <= 100",
            name="ck_fraud_scores_score_value_range",
        ),
        sa.ForeignKeyConstraint(["feature_run_id"], ["scoring_runs.id"]),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.ForeignKeyConstraint(["run_id"], ["scoring_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_fraud_scores_parcel_id_year_ruleset_version_feature_version",
        "fraud_scores",
        ["parcel_id", "year", "ruleset_version", "feature_version"],
        unique=True,
    )
    op.create_index(
        "ix_fraud_scores_ruleset_version_feature_version_score_value",
        "fraud_scores",
        ["ruleset_version", "feature_version", "score_value"],
        unique=False,
    )
    op.create_index(
        "ix_fraud_scores_requires_review_risk_band",
        "fraud_scores",
        ["requires_review", "risk_band"],
        unique=False,
    )
    op.create_index(
        "ix_fraud_scores_run_id",
        "fraud_scores",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "ix_fraud_scores_feature_run_id",
        "fraud_scores",
        ["feature_run_id"],
        unique=False,
    )

    op.create_table(
        "fraud_flags",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("score_id", sa.Integer(), nullable=False),
        sa.Column("parcel_id", sa.String(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("ruleset_version", sa.String(), nullable=False),
        sa.Column("reason_code", sa.String(), nullable=False),
        sa.Column("reason_rank", sa.Integer(), nullable=False),
        sa.Column("severity_weight", sa.Numeric(8, 4), nullable=True),
        sa.Column("metric_name", sa.String(), nullable=False),
        sa.Column("metric_value", sa.Text(), nullable=False),
        sa.Column("threshold_value", sa.Text(), nullable=False),
        sa.Column("comparison_operator", sa.String(), nullable=False),
        sa.Column("explanation", sa.Text(), nullable=False),
        sa.Column("source_refs_json", sa.JSON(), nullable=False),
        sa.Column(
            "flagged_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["parcel_id"], ["parcels.id"]),
        sa.ForeignKeyConstraint(["run_id"], ["scoring_runs.id"]),
        sa.ForeignKeyConstraint(["score_id"], ["fraud_scores.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ux_fraud_flags_score_id_reason_code",
        "fraud_flags",
        ["score_id", "reason_code"],
        unique=True,
    )
    op.create_index(
        "ix_fraud_flags_reason_code_ruleset_version",
        "fraud_flags",
        ["reason_code", "ruleset_version"],
        unique=False,
    )
    op.create_index(
        "ix_fraud_flags_parcel_id_year",
        "fraud_flags",
        ["parcel_id", "year"],
        unique=False,
    )
    op.create_index(
        "ix_fraud_flags_run_id",
        "fraud_flags",
        ["run_id"],
        unique=False,
    )
    _create_fraud_flag_consistency_triggers()


def downgrade() -> None:
    _drop_fraud_flag_consistency_triggers()

    op.drop_index("ix_fraud_flags_run_id", table_name="fraud_flags")
    op.drop_index("ix_fraud_flags_parcel_id_year", table_name="fraud_flags")
    op.drop_index(
        "ix_fraud_flags_reason_code_ruleset_version",
        table_name="fraud_flags",
    )
    op.drop_index("ux_fraud_flags_score_id_reason_code", table_name="fraud_flags")
    op.drop_table("fraud_flags")

    op.drop_index("ix_fraud_scores_feature_run_id", table_name="fraud_scores")
    op.drop_index("ix_fraud_scores_run_id", table_name="fraud_scores")
    op.drop_index(
        "ix_fraud_scores_requires_review_risk_band",
        table_name="fraud_scores",
    )
    op.drop_index(
        "ix_fraud_scores_ruleset_version_feature_version_score_value",
        table_name="fraud_scores",
    )
    op.drop_index(
        "ux_fraud_scores_parcel_id_year_ruleset_version_feature_version",
        table_name="fraud_scores",
    )
    op.drop_table("fraud_scores")

    op.drop_index("ix_parcel_features_run_id", table_name="parcel_features")
    op.drop_index(
        "ix_parcel_features_feature_version_year",
        table_name="parcel_features",
    )
    op.drop_index(
        "ux_parcel_features_parcel_id_year_feature_version",
        table_name="parcel_features",
    )
    op.drop_table("parcel_features")

    op.drop_index("ix_scoring_runs_parent_run_id", table_name="scoring_runs")
    op.drop_index(
        "ix_scoring_runs_run_type_version_tag_scope_hash",
        table_name="scoring_runs",
    )
    op.drop_index("ix_scoring_runs_version_tag", table_name="scoring_runs")
    op.drop_index("ix_scoring_runs_status", table_name="scoring_runs")
    op.drop_index(
        "ix_scoring_runs_run_type_started_at",
        table_name="scoring_runs",
    )
    op.drop_table("scoring_runs")
