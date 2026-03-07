from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError

from accessdane_audit.db import BASELINE_REVISION, HEAD_REVISION, init_db


def _db_url(path: Path) -> str:
    return f"sqlite:///{path}"


def _alembic_config(database_url: str) -> Config:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _create_legacy_baseline_schema(database_url: str) -> None:
    config = _alembic_config(database_url)
    command.upgrade(config, BASELINE_REVISION)

    engine = create_engine(database_url, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE alembic_version"))
    finally:
        engine.dispose()


def test_init_db_applies_migrations_to_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.sqlite"

    init_db(_db_url(db_path))

    engine = create_engine(_db_url(db_path), future=True)
    try:
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert "alembic_version" in tables
        assert {
            "parcels",
            "fetches",
            "assessments",
            "taxes",
            "payments",
            "parcel_summaries",
            "parcel_year_facts",
            "parcel_characteristics",
            "parcel_lineage_links",
            "sales_transactions",
            "sales_parcel_matches",
            "sales_exclusions",
            "permit_events",
            "appeal_events",
            "scoring_runs",
            "parcel_features",
            "fraud_scores",
            "fraud_flags",
        }.issubset(tables)

        with engine.connect() as conn:
            revision = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert revision == HEAD_REVISION

        fetch_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("fetches")
        }
        assert fetch_indexes["ix_fetches_status_code_parsed_at"] == (
            "status_code",
            "parsed_at",
        )
        parcel_year_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("parcel_year_facts")
        }
        assert parcel_year_indexes["ix_parcel_year_facts_year"] == ("year",)
        assert parcel_year_indexes["ix_parcel_year_facts_current_owner_name"] == (
            "current_owner_name",
        )
        parcel_year_columns = {
            column["name"] for column in inspector.get_columns("parcel_year_facts")
        }
        assert {
            "permit_event_count",
            "permit_declared_valuation_known_count",
            "permit_declared_valuation_sum",
            "permit_estimated_cost_known_count",
            "permit_estimated_cost_sum",
            "permit_status_applied_count",
            "permit_status_issued_count",
            "permit_status_finaled_count",
            "permit_status_cancelled_count",
            "permit_status_expired_count",
            "permit_status_unknown_count",
            "permit_recent_1y_count",
            "permit_recent_2y_count",
            "permit_has_recent_1y",
            "permit_has_recent_2y",
            "appeal_event_count",
            "appeal_reduction_granted_count",
            "appeal_partial_reduction_count",
            "appeal_denied_count",
            "appeal_withdrawn_count",
            "appeal_dismissed_count",
            "appeal_pending_count",
            "appeal_unknown_outcome_count",
            "appeal_value_change_known_count",
            "appeal_value_change_total",
            "appeal_value_change_reduction_total",
            "appeal_value_change_increase_total",
        }.issubset(parcel_year_columns)
        parcel_characteristic_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("parcel_characteristics")
        }
        assert parcel_characteristic_indexes[
            "ix_parcel_characteristics_source_fetch_id"
        ] == ("source_fetch_id",)
        assert parcel_characteristic_indexes[
            "ix_parcel_characteristics_current_valuation_classification"
        ] == ("current_valuation_classification",)
        assert parcel_characteristic_indexes[
            "ix_parcel_characteristics_state_municipality_code"
        ] == ("state_municipality_code",)
        parcel_characteristic_columns = {
            column["name"] for column in inspector.get_columns("parcel_characteristics")
        }
        assert {
            "centroid_latitude",
            "centroid_longitude",
            "geometry_wkt",
        }.issubset(parcel_characteristic_columns)
        parcel_lineage_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("parcel_lineage_links")
        }
        assert parcel_lineage_indexes["ix_parcel_lineage_links_related_parcel_id"] == (
            "related_parcel_id",
        )
        assert parcel_lineage_indexes["ix_parcel_lineage_links_relationship_type"] == (
            "relationship_type",
        )
        sales_transaction_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("sales_transactions")
        }
        assert sales_transaction_indexes[
            "ix_sales_transactions_source_file_sha256"
        ] == ("source_file_sha256",)
        assert sales_transaction_indexes[
            "ux_sales_transactions_source_file_sha256_source_row_number"
        ] == ("source_file_sha256", "source_row_number")
        assert sales_transaction_indexes["ix_sales_transactions_transfer_date"] == (
            "transfer_date",
        )
        assert sales_transaction_indexes[
            "ix_sales_transactions_official_parcel_number_norm"
        ] == ("official_parcel_number_norm",)
        assert sales_transaction_indexes[
            "ix_sales_transactions_property_address_norm"
        ] == ("property_address_norm",)
        assert sales_transaction_indexes["ix_sales_transactions_import_status"] == (
            "import_status",
        )
        sales_parcel_match_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("sales_parcel_matches")
        }
        assert sales_parcel_match_indexes[
            "ux_sales_parcel_matches_txn_parcel_method"
        ] == ("sales_transaction_id", "parcel_id", "match_method")
        assert sales_parcel_match_indexes[
            "ux_sales_parcel_matches_primary_per_transaction"
        ] == ("sales_transaction_id",)
        assert sales_parcel_match_indexes["ix_sales_parcel_matches_parcel_id"] == (
            "parcel_id",
        )
        assert sales_parcel_match_indexes[
            "ix_sales_parcel_matches_match_review_status"
        ] == ("match_review_status",)
        sales_exclusion_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("sales_exclusions")
        }
        assert sales_exclusion_indexes[
            "ux_sales_exclusions_sales_transaction_id_exclusion_code"
        ] == ("sales_transaction_id", "exclusion_code")
        permit_event_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("permit_events")
        }
        assert permit_event_indexes["ix_permit_events_source_file_sha256"] == (
            "source_file_sha256",
        )
        assert permit_event_indexes[
            "ux_permit_events_source_file_sha256_source_row_number"
        ] == ("source_file_sha256", "source_row_number")
        assert permit_event_indexes["ix_permit_events_import_status"] == (
            "import_status",
        )
        assert permit_event_indexes["ix_permit_events_parcel_number_norm"] == (
            "parcel_number_norm",
        )
        assert permit_event_indexes["ix_permit_events_site_address_norm"] == (
            "site_address_norm",
        )
        assert permit_event_indexes["ix_permit_events_parcel_id"] == ("parcel_id",)
        assert permit_event_indexes["ix_permit_events_permit_year"] == ("permit_year",)
        assert permit_event_indexes["ix_permit_events_permit_status_norm"] == (
            "permit_status_norm",
        )
        appeal_event_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("appeal_events")
        }
        assert appeal_event_indexes["ix_appeal_events_source_file_sha256"] == (
            "source_file_sha256",
        )
        assert appeal_event_indexes[
            "ux_appeal_events_source_file_sha256_source_row_number"
        ] == ("source_file_sha256", "source_row_number")
        assert appeal_event_indexes["ix_appeal_events_import_status"] == (
            "import_status",
        )
        assert appeal_event_indexes["ix_appeal_events_parcel_number_norm"] == (
            "parcel_number_norm",
        )
        assert appeal_event_indexes["ix_appeal_events_site_address_norm"] == (
            "site_address_norm",
        )
        assert appeal_event_indexes["ix_appeal_events_parcel_id"] == ("parcel_id",)
        assert appeal_event_indexes["ix_appeal_events_tax_year"] == ("tax_year",)
        assert appeal_event_indexes["ix_appeal_events_outcome_norm"] == (
            "outcome_norm",
        )
        assert appeal_event_indexes["ix_appeal_events_hearing_date"] == (
            "hearing_date",
        )
        scoring_run_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("scoring_runs")
        }
        assert scoring_run_indexes["ix_scoring_runs_run_type_started_at"] == (
            "run_type",
            "started_at",
        )
        assert scoring_run_indexes["ix_scoring_runs_status"] == ("status",)
        assert scoring_run_indexes["ix_scoring_runs_version_tag"] == ("version_tag",)
        assert scoring_run_indexes[
            "ix_scoring_runs_run_type_version_tag_scope_hash"
        ] == ("run_type", "version_tag", "scope_hash")
        assert scoring_run_indexes["ix_scoring_runs_parent_run_id"] == (
            "parent_run_id",
        )
        scoring_run_columns = {
            column["name"] for column in inspector.get_columns("scoring_runs")
        }
        assert {
            "run_type",
            "status",
            "version_tag",
            "scope_hash",
            "scope_json",
            "config_json",
            "input_summary_json",
            "output_summary_json",
            "error_summary",
            "parent_run_id",
            "started_at",
            "completed_at",
            "created_at",
        }.issubset(scoring_run_columns)
        parcel_feature_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("parcel_features")
        }
        assert parcel_feature_indexes[
            "ux_parcel_features_parcel_id_year_feature_version"
        ] == ("parcel_id", "year", "feature_version")
        assert parcel_feature_indexes["ix_parcel_features_feature_version_year"] == (
            "feature_version",
            "year",
        )
        assert parcel_feature_indexes["ix_parcel_features_run_id"] == ("run_id",)
        parcel_feature_columns = {
            column["name"] for column in inspector.get_columns("parcel_features")
        }
        assert {
            "run_id",
            "parcel_id",
            "year",
            "feature_version",
            "assessment_to_sale_ratio",
            "peer_percentile",
            "yoy_assessment_change_pct",
            "permit_adjusted_expected_change",
            "permit_adjusted_gap",
            "appeal_value_delta_3y",
            "appeal_success_rate_3y",
            "lineage_value_reset_delta",
            "feature_quality_flags",
            "source_refs_json",
            "built_at",
            "updated_at",
        }.issubset(parcel_feature_columns)
        fraud_score_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("fraud_scores")
        }
        assert fraud_score_indexes[
            "ux_fraud_scores_parcel_id_year_ruleset_version_feature_version"
        ] == ("parcel_id", "year", "ruleset_version", "feature_version")
        assert fraud_score_indexes[
            "ix_fraud_scores_ruleset_version_feature_version_score_value"
        ] == ("ruleset_version", "feature_version", "score_value")
        assert fraud_score_indexes["ix_fraud_scores_requires_review_risk_band"] == (
            "requires_review",
            "risk_band",
        )
        assert fraud_score_indexes["ix_fraud_scores_run_id"] == ("run_id",)
        assert fraud_score_indexes["ix_fraud_scores_feature_run_id"] == (
            "feature_run_id",
        )
        fraud_score_columns = {
            column["name"] for column in inspector.get_columns("fraud_scores")
        }
        assert {
            "run_id",
            "feature_run_id",
            "parcel_id",
            "year",
            "ruleset_version",
            "feature_version",
            "score_value",
            "risk_band",
            "requires_review",
            "reason_code_count",
            "score_summary_json",
            "scored_at",
            "updated_at",
        }.issubset(fraud_score_columns)
        fraud_flag_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("fraud_flags")
        }
        assert fraud_flag_indexes["ux_fraud_flags_score_id_reason_code"] == (
            "score_id",
            "reason_code",
        )
        assert fraud_flag_indexes["ix_fraud_flags_reason_code_ruleset_version"] == (
            "reason_code",
            "ruleset_version",
        )
        assert fraud_flag_indexes["ix_fraud_flags_parcel_id_year"] == (
            "parcel_id",
            "year",
        )
        assert fraud_flag_indexes["ix_fraud_flags_run_id"] == ("run_id",)
        fraud_flag_columns = {
            column["name"] for column in inspector.get_columns("fraud_flags")
        }
        assert {
            "run_id",
            "score_id",
            "parcel_id",
            "year",
            "ruleset_version",
            "reason_code",
            "reason_rank",
            "severity_weight",
            "metric_name",
            "metric_value",
            "threshold_value",
            "comparison_operator",
            "explanation",
            "source_refs_json",
            "flagged_at",
        }.issubset(fraud_flag_columns)
        with engine.connect() as conn:
            partial_index_sql = conn.execute(
                text(
                    "SELECT sql FROM sqlite_master "
                    "WHERE type = 'index' "
                    "AND name = 'ux_sales_parcel_matches_primary_per_transaction'"
                )
            ).scalar_one()
        assert partial_index_sql is not None
        assert "WHERE is_primary = 1" in partial_index_sql
        with engine.connect() as conn:
            trigger_names = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT name FROM sqlite_master "
                        "WHERE type = 'trigger' AND tbl_name = 'fraud_flags'"
                    )
                )
            }
        assert {
            "trg_fraud_flags_consistency_insert",
            "trg_fraud_flags_consistency_update",
        }.issubset(trigger_names)
    finally:
        engine.dispose()


def test_init_db_stamps_compatible_legacy_schema_before_upgrading(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy.sqlite"
    database_url = _db_url(db_path)

    _create_legacy_baseline_schema(database_url)

    init_db(database_url)

    engine = create_engine(database_url, future=True)
    try:
        inspector = inspect(engine)
        assert "alembic_version" in set(inspector.get_table_names())
        with engine.connect() as conn:
            revision = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert revision == HEAD_REVISION
        fetch_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("fetches")
        }
        assert fetch_indexes["ix_fetches_status_code_parsed_at"] == (
            "status_code",
            "parsed_at",
        )
        assert "parcel_year_facts" in set(inspector.get_table_names())
        assert "parcel_characteristics" in set(inspector.get_table_names())
        assert "parcel_lineage_links" in set(inspector.get_table_names())
        assert "sales_transactions" in set(inspector.get_table_names())
        assert "sales_parcel_matches" in set(inspector.get_table_names())
        assert "sales_exclusions" in set(inspector.get_table_names())
        assert "permit_events" in set(inspector.get_table_names())
        assert "appeal_events" in set(inspector.get_table_names())
        assert "scoring_runs" in set(inspector.get_table_names())
        assert "parcel_features" in set(inspector.get_table_names())
        assert "fraud_scores" in set(inspector.get_table_names())
        assert "fraud_flags" in set(inspector.get_table_names())
    finally:
        engine.dispose()


def test_init_db_rejects_incompatible_preexisting_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "partial.sqlite"
    database_url = _db_url(db_path)

    engine = create_engine(database_url, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE parcels (id VARCHAR PRIMARY KEY)"))
    finally:
        engine.dispose()

    with pytest.raises(RuntimeError, match="does not match the expected legacy schema"):
        init_db(database_url)


def test_init_db_is_idempotent_on_already_versioned_database(tmp_path: Path) -> None:
    db_path = tmp_path / "versioned.sqlite"
    database_url = _db_url(db_path)

    init_db(database_url)
    init_db(database_url)

    engine = create_engine(database_url, future=True)
    try:
        inspector = inspect(engine)
        assert "parcel_year_facts" in set(inspector.get_table_names())
        assert "parcel_characteristics" in set(inspector.get_table_names())
        assert "parcel_lineage_links" in set(inspector.get_table_names())
        assert "sales_transactions" in set(inspector.get_table_names())
        assert "sales_parcel_matches" in set(inspector.get_table_names())
        assert "sales_exclusions" in set(inspector.get_table_names())
        assert "permit_events" in set(inspector.get_table_names())
        assert "appeal_events" in set(inspector.get_table_names())
        assert "scoring_runs" in set(inspector.get_table_names())
        assert "parcel_features" in set(inspector.get_table_names())
        assert "fraud_scores" in set(inspector.get_table_names())
        assert "fraud_flags" in set(inspector.get_table_names())
        with engine.connect() as conn:
            revision = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert revision == HEAD_REVISION
    finally:
        engine.dispose()


def test_fraud_flags_trigger_enforces_parent_score_denormalized_fields(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fraud_flags_trigger.sqlite"
    database_url = _db_url(db_path)
    init_db(database_url)

    engine = create_engine(database_url, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO scoring_runs "
                    "(id, run_type, status, version_tag, scope_json, config_json) "
                    "VALUES (1, 'score_fraud', 'succeeded', 'ruleset-v1', '{}', '{}')"
                )
            )
            conn.execute(text("INSERT INTO parcels (id) VALUES ('P1'), ('P2')"))
            conn.execute(
                text(
                    "INSERT INTO fraud_scores "
                    "(run_id, feature_run_id, parcel_id, year, ruleset_version, "
                    "feature_version, score_value, risk_band, score_summary_json) "
                    "VALUES (1, NULL, 'P1', 2025, 'ruleset-v1', 'features-v1', "
                    "42.00, 'medium', '{}')"
                )
            )
            score_id = conn.execute(
                text(
                    "SELECT id FROM fraud_scores "
                    "WHERE parcel_id = 'P1' "
                    "AND year = 2025 "
                    "AND ruleset_version = 'ruleset-v1'"
                )
            ).scalar_one()

            with pytest.raises(
                IntegrityError,
                match="fraud_flags row must match parent fraud_scores",
            ):
                conn.execute(
                    text(
                        "INSERT INTO fraud_flags "
                        "(run_id, score_id, parcel_id, year, ruleset_version, "
                        "reason_code, reason_rank, severity_weight, metric_name, "
                        "metric_value, threshold_value, comparison_operator, "
                        "explanation, source_refs_json) "
                        "VALUES "
                        "(:run_id, :score_id, 'P2', 2025, 'ruleset-v1', "
                        "'ratio_outlier', 1, 0.75, 'assessment_to_sale_ratio', "
                        "'1.42', '1.20', 'gt', "
                        "'Mismatch should be blocked by trigger', '{}')"
                    ),
                    {"run_id": 1, "score_id": score_id},
                )

            conn.execute(
                text(
                    "INSERT INTO fraud_flags "
                    "(run_id, score_id, parcel_id, year, ruleset_version, "
                    "reason_code, reason_rank, severity_weight, metric_name, "
                    "metric_value, threshold_value, comparison_operator, "
                    "explanation, source_refs_json) "
                    "VALUES "
                    "(:run_id, :score_id, 'P1', 2025, 'ruleset-v1', "
                    "'ratio_outlier', 1, 0.75, 'assessment_to_sale_ratio', "
                    "'1.42', '1.20', 'gt', "
                    "'Consistent denormalized fields should pass', '{}')"
                ),
                {"run_id": 1, "score_id": score_id},
            )
    finally:
        engine.dispose()


def test_reconcile_sales_match_index_name_migration(tmp_path: Path) -> None:
    db_path = tmp_path / "sales_match_index_reconcile.sqlite"
    database_url = _db_url(db_path)
    config = _alembic_config(database_url)

    command.upgrade(config, "20260304_0005")

    engine = create_engine(database_url, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP INDEX ux_sales_parcel_matches_txn_parcel_method"))
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX "
                    "ux_sales_parcel_matches_"
                    "sales_transaction_id_parcel_id_match_method "
                    "ON sales_parcel_matches "
                    "(sales_transaction_id, parcel_id, match_method)"
                )
            )
    finally:
        engine.dispose()

    command.upgrade(config, HEAD_REVISION)

    engine = create_engine(database_url, future=True)
    try:
        inspector = inspect(engine)
        index_names = {
            index["name"] for index in inspector.get_indexes("sales_parcel_matches")
        }
        assert "ux_sales_parcel_matches_txn_parcel_method" in index_names
        assert (
            "ux_sales_parcel_matches_sales_transaction_id_parcel_id_match_method"
            not in index_names
        )
        with engine.connect() as conn:
            revision = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
        assert revision == HEAD_REVISION
    finally:
        engine.dispose()
