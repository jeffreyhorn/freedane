from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.exc import InvalidRequestError, PendingRollbackError
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit import sales_ratio_study as sales_ratio_study_module
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    Parcel,
    ParcelYearFact,
    SalesExclusion,
    SalesParcelMatch,
    SalesTransaction,
    ScoringRun,
)
from accessdane_audit.sales_ratio_study import build_sales_ratio_study


def test_build_sales_ratio_study_computes_group_metrics_and_persists_run(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "sales_ratio_study.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_sales_ratio_fixture(session)

    with session_scope(database_url) as session:
        payload = build_sales_ratio_study(session, version_tag="ratio-v1")

    assert payload["run"]["status"] == "succeeded"
    assert payload["run"]["run_type"] == "sales_ratio_study"
    assert payload["run"]["run_persisted"] is True
    assert payload["run"]["run_id"] is not None
    assert payload["summary"]["candidate_sales_count"] == 4
    assert payload["summary"]["included_sales_count"] == 3
    assert payload["summary"]["excluded_sales_count"] == 1
    assert payload["summary"]["skipped_scope_filter_count"] == 0
    assert payload["summary"]["group_count"] == 2

    groups = {
        (group["municipality_name"], group["valuation_classification"]): group
        for group in payload["groups"]
    }
    mcfarland_group = groups[("McFarland", "residential")]
    madison_group = groups[("Madison", "commercial")]

    assert mcfarland_group["year"] == 2025
    assert mcfarland_group["area_key"] == "McFarland"
    assert mcfarland_group["sale_count"] == 2
    assert mcfarland_group["median_ratio"] == 0.75
    assert mcfarland_group["cod"] == 33.3333
    assert mcfarland_group["prd"] == 1.0
    assert mcfarland_group["outlier_low_count"] == 0
    assert mcfarland_group["outlier_high_count"] == 0
    assert mcfarland_group["excluded_count"] == 1

    assert madison_group["year"] == 2025
    assert madison_group["area_key"] == "Madison"
    assert madison_group["sale_count"] == 1
    assert madison_group["median_ratio"] == 1.5
    assert madison_group["cod"] == 0.0
    assert madison_group["prd"] == 1.0
    assert madison_group["outlier_low_count"] == 0
    assert madison_group["outlier_high_count"] == 0
    assert madison_group["excluded_count"] == 0

    with session_scope(database_url) as session:
        run_row = session.execute(
            select(ScoringRun).where(ScoringRun.version_tag == "ratio-v1")
        ).scalar_one()

    assert run_row.run_type == "sales_ratio_study"
    assert run_row.status == "succeeded"
    assert run_row.scope_hash is not None
    assert run_row.output_summary_json == payload["summary"]


def test_sales_ratio_study_cli_supports_scope_and_output_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "sales_ratio_study_cli.sqlite"
    database_url = f"sqlite:///{db_path}"
    output_path = tmp_path / "sales_ratio_study.json"
    ids_path = tmp_path / "parcel_ids.txt"
    ids_path.write_text("parcel-res-1\nparcel-res-2\n", encoding="utf-8")
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_sales_ratio_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "sales-ratio-study",
            "--ids",
            str(ids_path),
            "--year",
            "2025",
            "--municipality",
            "mcfarland",
            "--class",
            "residential",
            "--version-tag",
            "ratio-v1-scope",
            "--out",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["run"]["status"] == "succeeded"
    assert payload["run"]["run_persisted"] is True
    assert payload["run"]["run_id"] is not None
    assert payload["scope"] == {
        "parcel_ids": ["parcel-res-1", "parcel-res-2"],
        "years": [2025],
        "municipality": "mcfarland",
        "valuation_classification": "residential",
    }
    assert payload["summary"]["candidate_sales_count"] == 2
    assert payload["summary"]["included_sales_count"] == 1
    assert payload["summary"]["excluded_sales_count"] == 1
    assert payload["summary"]["skipped_scope_filter_count"] == 0
    assert payload["summary"]["group_count"] == 1
    assert payload["groups"] == [
        {
            "year": 2025,
            "municipality_name": "McFarland",
            "valuation_classification": "residential",
            "area_key": "McFarland",
            "sale_count": 1,
            "median_ratio": 1.0,
            "cod": 0.0,
            "prd": 1.0,
            "outlier_low_count": 0,
            "outlier_high_count": 0,
            "excluded_count": 1,
        }
    ]

    with session_scope(database_url) as session:
        run_row = session.execute(
            select(ScoringRun).where(ScoringRun.version_tag == "ratio-v1-scope")
        ).scalar_one()

    assert run_row.scope_json == payload["scope"]
    assert run_row.status == "succeeded"


def test_build_sales_ratio_study_counts_skipped_missing_fact_and_assessment_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "sales_ratio_study_missing_inputs.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_sales_ratio_fixture(session)
        session.add_all(
            [
                Parcel(id="parcel-missing-fact"),
                Parcel(id="parcel-missing-assessment"),
                ParcelYearFact(
                    parcel_id="parcel-missing-assessment",
                    year=2025,
                    municipality_name="Sun Prairie",
                    assessment_valuation_classification="residential",
                    assessment_total_value=None,
                ),
            ]
        )
        missing_sales = [
            SalesTransaction(
                source_system="wisconsin_dor_retr",
                source_file_name="test.csv",
                source_file_sha256="sha-test-1",
                source_row_number=5,
                source_headers=["Transfer Date", "Consideration"],
                raw_row={"Transfer Date": "2025-04-01", "Consideration": "50000"},
                import_status="loaded",
                transfer_date=date(2025, 4, 1),
                consideration_amount=Decimal("50000.00"),
            ),
            SalesTransaction(
                source_system="wisconsin_dor_retr",
                source_file_name="test.csv",
                source_file_sha256="sha-test-1",
                source_row_number=6,
                source_headers=["Transfer Date", "Consideration"],
                raw_row={"Transfer Date": "2025-04-05", "Consideration": "70000"},
                import_status="loaded",
                transfer_date=date(2025, 4, 5),
                consideration_amount=Decimal("70000.00"),
            ),
        ]
        session.add_all(missing_sales)
        session.flush()
        session.add_all(
            [
                SalesParcelMatch(
                    sales_transaction_id=missing_sales[0].id,
                    parcel_id="parcel-missing-fact",
                    match_method="exact_parcel_number",
                    confidence_score=Decimal("1.0000"),
                    match_rank=1,
                    is_primary=True,
                    match_review_status="auto_accepted",
                    matched_value="parcel-missing-fact",
                    matcher_version="test",
                ),
                SalesParcelMatch(
                    sales_transaction_id=missing_sales[1].id,
                    parcel_id="parcel-missing-assessment",
                    match_method="exact_parcel_number",
                    confidence_score=Decimal("1.0000"),
                    match_rank=1,
                    is_primary=True,
                    match_review_status="auto_accepted",
                    matched_value="parcel-missing-assessment",
                    matcher_version="test",
                ),
            ]
        )

    with session_scope(database_url) as session:
        payload = build_sales_ratio_study(session, version_tag="ratio-v1-missing-cases")

    assert payload["run"]["status"] == "succeeded"
    assert payload["summary"]["candidate_sales_count"] == 6
    assert payload["summary"]["included_sales_count"] == 3
    assert payload["summary"]["excluded_sales_count"] == 1
    assert payload["summary"]["skipped_scope_filter_count"] == 0
    assert payload["summary"]["skipped_missing_parcel_year_fact_count"] == 1
    assert payload["summary"]["skipped_missing_assessment_count"] == 1
    assert payload["summary"]["group_count"] == 2
    assert all(
        group["municipality_name"] != "Sun Prairie" for group in payload["groups"]
    )


def test_build_sales_ratio_study_counts_scope_filter_skips(tmp_path: Path) -> None:
    db_path = tmp_path / "sales_ratio_study_scope_filter.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_sales_ratio_fixture(session)

    with session_scope(database_url) as session:
        payload = build_sales_ratio_study(
            session,
            version_tag="ratio-v1-scope-filter",
            municipality="mcfarland",
            valuation_classification="residential",
        )

    assert payload["run"]["status"] == "succeeded"
    assert payload["summary"]["candidate_sales_count"] == 4
    assert payload["summary"]["included_sales_count"] == 2
    assert payload["summary"]["excluded_sales_count"] == 1
    assert payload["summary"]["skipped_scope_filter_count"] == 1
    assert payload["summary"]["group_count"] == 1


def test_build_sales_ratio_study_normalizes_scope_text_for_hash(tmp_path: Path) -> None:
    db_path = tmp_path / "sales_ratio_study_scope_normalization.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_sales_ratio_fixture(session)

    with session_scope(database_url) as session:
        payload_mixed_case = build_sales_ratio_study(
            session,
            version_tag="ratio-v1-scope-mixed-case",
            municipality=" McFarland ",
            valuation_classification=" Residential ",
        )
        payload_lower_case = build_sales_ratio_study(
            session,
            version_tag="ratio-v1-scope-lower-case",
            municipality="mcfarland",
            valuation_classification="residential",
        )

    assert payload_mixed_case["scope"]["municipality"] == "mcfarland"
    assert payload_mixed_case["scope"]["valuation_classification"] == "residential"
    assert payload_lower_case["scope"]["municipality"] == "mcfarland"
    assert payload_lower_case["scope"]["valuation_classification"] == "residential"

    with session_scope(database_url) as session:
        mixed_case_run = session.execute(
            select(ScoringRun).where(
                ScoringRun.version_tag == "ratio-v1-scope-mixed-case"
            )
        ).scalar_one()
        lower_case_run = session.execute(
            select(ScoringRun).where(
                ScoringRun.version_tag == "ratio-v1-scope-lower-case"
            )
        ).scalar_one()

    assert mixed_case_run.scope_json == lower_case_run.scope_json
    assert mixed_case_run.scope_hash == lower_case_run.scope_hash


def test_build_sales_ratio_study_batches_in_clause_filters(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "sales_ratio_study_batched_in_clause.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_sales_ratio_fixture(session)

    monkeypatch.setattr(sales_ratio_study_module, "IN_CLAUSE_BATCH_SIZE", 1)
    with session_scope(database_url) as session:
        payload = build_sales_ratio_study(
            session,
            version_tag="ratio-v1-batched-in-clause",
            parcel_ids=["parcel-res-1", "parcel-res-2", "parcel-res-3"],
            years=[2025],
        )

    assert payload["run"]["status"] == "succeeded"
    assert payload["summary"]["candidate_sales_count"] == 3
    assert payload["summary"]["included_sales_count"] == 2
    assert payload["summary"]["excluded_sales_count"] == 1
    assert payload["summary"]["group_count"] == 1


def test_sales_ratio_study_cli_rejects_empty_ids_scope(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "sales_ratio_study_cli_empty_ids.sqlite"
    database_url = f"sqlite:///{db_path}"
    ids_path = tmp_path / "empty_ids.txt"
    ids_path.write_text(" \n\t\n", encoding="utf-8")
    init_db(database_url)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["sales-ratio-study", "--ids", str(ids_path)],
    )

    assert result.exit_code != 0
    assert "At least one parcel ID must be provided via --id or --ids." in result.output


def test_sales_ratio_study_cli_rejects_whitespace_id_option(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "sales_ratio_study_cli_whitespace_id.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["sales-ratio-study", "--id", "  ", "--id", "\t"],
    )

    assert result.exit_code != 0
    assert "At least one parcel ID must be provided via --id or --ids." in result.output


def test_sales_ratio_study_cli_rejects_non_positive_year(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "sales_ratio_study_cli_invalid_year.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["sales-ratio-study", "--year", "0"],
    )

    assert result.exit_code != 0
    assert "Invalid value for '--year'" in result.output


def test_build_sales_ratio_study_persists_failure_summary(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "sales_ratio_study_failure_summary.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    def _raise_runtime_error(*_args, **_kwargs):
        raise RuntimeError("forced study failure")

    monkeypatch.setattr(
        sales_ratio_study_module,
        "_load_candidate_rows",
        _raise_runtime_error,
    )
    with session_scope(database_url) as session:
        payload = build_sales_ratio_study(
            session,
            version_tag="ratio-v1-forced-failure",
        )

    assert payload["run"]["status"] == "failed"
    assert payload["run"]["run_persisted"] is True
    assert payload["run"]["run_id"] is not None
    assert payload["summary"] == {
        "candidate_sales_count": 0,
        "included_sales_count": 0,
        "excluded_sales_count": 0,
        "skipped_scope_filter_count": 0,
        "skipped_missing_parcel_year_fact_count": 0,
        "skipped_missing_assessment_count": 0,
        "group_count": 0,
    }

    with session_scope(database_url) as session:
        run_row = session.execute(
            select(ScoringRun).where(
                ScoringRun.version_tag == "ratio-v1-forced-failure"
            )
        ).scalar_one()

    assert run_row.status == "failed"
    assert run_row.output_summary_json == payload["summary"]


def test_build_sales_ratio_study_handles_pending_rollback_on_failure_flush(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "sales_ratio_study_pending_rollback.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    def _raise_runtime_error(*_args, **_kwargs):
        raise RuntimeError("forced study failure")

    monkeypatch.setattr(
        sales_ratio_study_module,
        "_load_candidate_rows",
        _raise_runtime_error,
    )

    with session_scope(database_url) as session:
        original_flush = session.flush
        flush_call_count = {"count": 0}

        def _flush_with_pending_rollback(*_args, **_kwargs):
            flush_call_count["count"] += 1
            if flush_call_count["count"] == 1:
                return original_flush()
            raise PendingRollbackError("forced pending rollback")

        monkeypatch.setattr(session, "flush", _flush_with_pending_rollback)
        payload = build_sales_ratio_study(
            session,
            version_tag="ratio-v1-pending-rollback",
        )

    assert payload["run"]["status"] == "failed"
    assert payload["run"]["run_persisted"] is False
    assert payload["run"]["run_id"] is None
    assert payload["summary"]["group_count"] == 0
    assert payload["error"] == "forced study failure"


def test_build_sales_ratio_study_handles_initial_run_flush_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "sales_ratio_study_initial_flush_failure.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:

        def _flush_always_fails(*_args, **_kwargs):
            raise InvalidRequestError("forced initial run flush failure")

        monkeypatch.setattr(session, "flush", _flush_always_fails)
        payload = build_sales_ratio_study(
            session,
            version_tag="ratio-v1-initial-flush-failure",
        )

    assert payload["run"]["status"] == "failed"
    assert payload["run"]["run_persisted"] is False
    assert payload["run"]["run_id"] is None
    assert payload["summary"]["group_count"] == 0
    assert payload["error"] == "forced initial run flush failure"


def _seed_sales_ratio_fixture(session) -> None:
    session.add_all(
        [
            Parcel(id="parcel-res-1"),
            Parcel(id="parcel-res-2"),
            Parcel(id="parcel-res-3"),
            Parcel(id="parcel-com-1"),
        ]
    )
    session.add_all(
        [
            ParcelYearFact(
                parcel_id="parcel-res-1",
                year=2025,
                municipality_name="McFarland",
                assessment_valuation_classification="residential",
                assessment_total_value=Decimal("100000.00"),
            ),
            ParcelYearFact(
                parcel_id="parcel-res-2",
                year=2025,
                municipality_name="McFarland",
                assessment_valuation_classification="residential",
                assessment_total_value=Decimal("160000.00"),
            ),
            ParcelYearFact(
                parcel_id="parcel-res-3",
                year=2025,
                municipality_name="McFarland",
                assessment_valuation_classification="residential",
                assessment_total_value=Decimal("50000.00"),
            ),
            ParcelYearFact(
                parcel_id="parcel-com-1",
                year=2025,
                municipality_name="Madison",
                assessment_valuation_classification="commercial",
                assessment_total_value=Decimal("300000.00"),
            ),
        ]
    )

    sales_rows = [
        SalesTransaction(
            source_system="wisconsin_dor_retr",
            source_file_name="test.csv",
            source_file_sha256="sha-test-1",
            source_row_number=1,
            source_headers=["Transfer Date", "Consideration"],
            raw_row={"Transfer Date": "2025-01-15", "Consideration": "100000"},
            import_status="loaded",
            transfer_date=date(2025, 1, 15),
            consideration_amount=Decimal("100000.00"),
        ),
        SalesTransaction(
            source_system="wisconsin_dor_retr",
            source_file_name="test.csv",
            source_file_sha256="sha-test-1",
            source_row_number=2,
            source_headers=["Transfer Date", "Consideration"],
            raw_row={"Transfer Date": "2025-02-05", "Consideration": "100000"},
            import_status="loaded",
            transfer_date=date(2025, 2, 5),
            consideration_amount=Decimal("100000.00"),
        ),
        SalesTransaction(
            source_system="wisconsin_dor_retr",
            source_file_name="test.csv",
            source_file_sha256="sha-test-1",
            source_row_number=3,
            source_headers=["Transfer Date", "Consideration"],
            raw_row={"Transfer Date": "2025-03-01", "Consideration": "100000"},
            import_status="loaded",
            transfer_date=date(2025, 3, 1),
            consideration_amount=Decimal("100000.00"),
        ),
        SalesTransaction(
            source_system="wisconsin_dor_retr",
            source_file_name="test.csv",
            source_file_sha256="sha-test-1",
            source_row_number=4,
            source_headers=["Transfer Date", "Consideration"],
            raw_row={"Transfer Date": "2025-03-10", "Consideration": "200000"},
            import_status="loaded",
            transfer_date=date(2025, 3, 10),
            consideration_amount=Decimal("200000.00"),
        ),
    ]
    session.add_all(sales_rows)
    session.flush()

    session.add_all(
        [
            SalesParcelMatch(
                sales_transaction_id=sales_rows[0].id,
                parcel_id="parcel-res-1",
                match_method="exact_parcel_number",
                confidence_score=Decimal("1.0000"),
                match_rank=1,
                is_primary=True,
                match_review_status="auto_accepted",
                matched_value="parcel-res-1",
                matcher_version="test",
            ),
            SalesParcelMatch(
                sales_transaction_id=sales_rows[1].id,
                parcel_id="parcel-res-2",
                match_method="exact_parcel_number",
                confidence_score=Decimal("1.0000"),
                match_rank=1,
                is_primary=True,
                match_review_status="auto_accepted",
                matched_value="parcel-res-2",
                matcher_version="test",
            ),
            SalesParcelMatch(
                sales_transaction_id=sales_rows[2].id,
                parcel_id="parcel-res-3",
                match_method="exact_parcel_number",
                confidence_score=Decimal("1.0000"),
                match_rank=1,
                is_primary=True,
                match_review_status="auto_accepted",
                matched_value="parcel-res-3",
                matcher_version="test",
            ),
            SalesParcelMatch(
                sales_transaction_id=sales_rows[3].id,
                parcel_id="parcel-com-1",
                match_method="exact_parcel_number",
                confidence_score=Decimal("1.0000"),
                match_rank=1,
                is_primary=True,
                match_review_status="auto_accepted",
                matched_value="parcel-com-1",
                matcher_version="test",
            ),
        ]
    )
    session.add(
        SalesExclusion(
            sales_transaction_id=sales_rows[1].id,
            exclusion_code="non_arms_length",
            exclusion_reason="Not arm's-length",
            is_active=True,
            excluded_by_rule="test_rule",
        )
    )
