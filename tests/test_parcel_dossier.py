from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    AppealEvent,
    AssessmentRecord,
    FraudFlag,
    FraudScore,
    Parcel,
    ParcelFeature,
    ParcelYearFact,
    PermitEvent,
    SalesParcelMatch,
    SalesTransaction,
    ScoringRun,
)


def test_parcel_dossier_cli_builds_full_chain_and_is_deterministic(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "parcel_dossier_full.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_full_dossier_fixture(session, parcel_id="P-1")

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result_a = runner.invoke(cli.app, ["parcel-dossier", "--id", "P-1"])
    result_b = runner.invoke(cli.app, ["parcel-dossier", "--id", "P-1"])

    assert result_a.exit_code == 0, result_a.stdout
    assert result_b.exit_code == 0, result_b.stdout
    assert result_a.stdout == result_b.stdout

    payload = json.loads(result_a.stdout)
    assert list(payload.keys()) == [
        "run",
        "request",
        "parcel",
        "section_order",
        "sections",
        "timeline",
        "diagnostics",
        "error",
    ]
    assert payload["run"]["status"] == "succeeded"
    assert payload["request"] == {
        "parcel_id": "P-1",
        "years": [],
        "feature_version": "feature_v1",
        "ruleset_version": "scoring_rules_v1",
    }
    assert payload["section_order"] == [
        "assessment_history",
        "matched_sales",
        "peer_context",
        "permit_events",
        "appeal_events",
        "reason_code_evidence",
    ]
    assert all(
        payload["sections"][section]["status"] == "populated"
        for section in payload["section_order"]
    )
    assert (
        payload["sections"]["assessment_history"]["rows"][0]["total_value"]
        == "300000.00"
    )
    assert set(row["event_type"] for row in payload["timeline"]["rows"]) == {
        "assessment",
        "sale",
        "permit",
        "appeal",
        "score",
    }
    assert payload["error"] is None


def test_parcel_dossier_cli_marks_sparse_sections_empty(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "parcel_dossier_sparse.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="P-2", trs_code="TRS-2"))
        session.add(
            ParcelYearFact(
                parcel_id="P-2",
                year=2024,
                municipality_name="Sparse Town",
                current_owner_name="Sparse Owner",
            )
        )
        session.add(
            AssessmentRecord(
                parcel_id="P-2",
                fetch_id=1,
                year=2024,
                valuation_classification="residential",
                total_value=Decimal("210000.00"),
                data={},
            )
        )

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["parcel-dossier", "--id", "P-2"])
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)

    assert payload["sections"]["assessment_history"]["status"] == "populated"
    assert payload["sections"]["matched_sales"]["status"] == "empty"
    assert payload["sections"]["peer_context"]["status"] == "empty"
    assert payload["sections"]["permit_events"]["status"] == "empty"
    assert payload["sections"]["appeal_events"]["status"] == "empty"
    assert payload["sections"]["reason_code_evidence"]["status"] == "empty"
    assert payload["diagnostics"]["unavailable_sections"] == []


def test_parcel_dossier_cli_marks_reason_codes_unavailable_for_version_mismatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "parcel_dossier_version_mismatch.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="P-3", trs_code="TRS-3"))
        session.add(
            ParcelYearFact(
                parcel_id="P-3",
                year=2025,
                municipality_name="Mismatch Town",
            )
        )
        feature_run = ScoringRun(
            run_type="build_features",
            status="succeeded",
            version_tag="feature_v2",
            scope_json={},
            config_json={},
        )
        score_run = ScoringRun(
            run_type="score_fraud",
            status="succeeded",
            version_tag="scoring_rules_v1",
            scope_json={},
            config_json={},
        )
        session.add_all([feature_run, score_run])
        session.flush()
        session.add(
            ParcelFeature(
                run_id=feature_run.id,
                parcel_id="P-3",
                year=2025,
                feature_version="feature_v2",
                assessment_to_sale_ratio=Decimal("0.820000"),
                peer_percentile=Decimal("0.2200"),
                source_refs_json={},
            )
        )
        score_row = FraudScore(
            run_id=score_run.id,
            feature_run_id=feature_run.id,
            parcel_id="P-3",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("58.00"),
            risk_band="medium",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
            scored_at=datetime(2025, 8, 1, 15, 0, tzinfo=timezone.utc),
        )
        session.add(score_row)
        session.flush()
        session.add(
            FraudFlag(
                run_id=score_run.id,
                score_id=score_row.id,
                parcel_id="P-3",
                year=2025,
                ruleset_version="scoring_rules_v1",
                reason_code="R1",
                reason_rank=1,
                severity_weight=Decimal("5.0000"),
                metric_name="assessment_to_sale_ratio",
                metric_value="0.820000",
                threshold_value="0.900000",
                comparison_operator="lt",
                explanation="ratio below threshold",
                source_refs_json={},
            )
        )

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["parcel-dossier", "--id", "P-3", "--feature-version", "feature_v2"],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)

    assert payload["sections"]["peer_context"]["status"] == "populated"
    assert payload["sections"]["reason_code_evidence"]["status"] == "unavailable"
    assert payload["sections"]["reason_code_evidence"]["message"] == (
        "missing_fraud_scores_for_requested_versions"
    )
    assert payload["diagnostics"]["unavailable_sections"] == ["reason_code_evidence"]


def test_parcel_dossier_cli_applies_year_filter_and_deduplicates_years(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "parcel_dossier_year_scope.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="P-4", trs_code="TRS-4"))
        session.add_all(
            [
                ParcelYearFact(
                    parcel_id="P-4",
                    year=2023,
                    municipality_name="Y-2023",
                    current_owner_name="Owner-2023",
                ),
                ParcelYearFact(
                    parcel_id="P-4",
                    year=2024,
                    municipality_name="Y-2024",
                    current_owner_name="Owner-2024",
                ),
            ]
        )
        session.add_all(
            [
                AssessmentRecord(
                    parcel_id="P-4",
                    fetch_id=11,
                    year=2023,
                    valuation_classification="residential",
                    total_value=Decimal("190000.00"),
                    data={},
                ),
                AssessmentRecord(
                    parcel_id="P-4",
                    fetch_id=12,
                    year=2024,
                    valuation_classification="residential",
                    total_value=Decimal("205000.00"),
                    data={},
                ),
            ]
        )

        sale_2023 = SalesTransaction(
            source_system="retr",
            source_file_name="retr.csv",
            source_file_sha256="sha-year-scope",
            source_row_number=1,
            source_headers=[],
            raw_row={},
            import_status="loaded",
            transfer_date=date(2023, 4, 15),
            consideration_amount=Decimal("240000.00"),
        )
        sale_2024 = SalesTransaction(
            source_system="retr",
            source_file_name="retr.csv",
            source_file_sha256="sha-year-scope",
            source_row_number=2,
            source_headers=[],
            raw_row={},
            import_status="loaded",
            transfer_date=date(2024, 4, 15),
            consideration_amount=Decimal("250000.00"),
        )
        session.add_all([sale_2023, sale_2024])
        session.flush()
        session.add_all(
            [
                SalesParcelMatch(
                    sales_transaction_id=sale_2023.id,
                    parcel_id="P-4",
                    match_method="parcel_number",
                    confidence_score=Decimal("0.9500"),
                    match_rank=1,
                    is_primary=True,
                    match_review_status="reviewed",
                ),
                SalesParcelMatch(
                    sales_transaction_id=sale_2024.id,
                    parcel_id="P-4",
                    match_method="parcel_number",
                    confidence_score=Decimal("0.9800"),
                    match_rank=1,
                    is_primary=True,
                    match_review_status="reviewed",
                ),
            ]
        )

        feature_run = ScoringRun(
            run_type="build_features",
            status="succeeded",
            version_tag="feature_v1",
            scope_json={},
            config_json={},
        )
        score_run = ScoringRun(
            run_type="score_fraud",
            status="succeeded",
            version_tag="scoring_rules_v1",
            scope_json={},
            config_json={},
        )
        session.add_all([feature_run, score_run])
        session.flush()
        session.add_all(
            [
                ParcelFeature(
                    run_id=feature_run.id,
                    parcel_id="P-4",
                    year=2023,
                    feature_version="feature_v1",
                    assessment_to_sale_ratio=Decimal("0.780000"),
                    source_refs_json={},
                ),
                ParcelFeature(
                    run_id=feature_run.id,
                    parcel_id="P-4",
                    year=2024,
                    feature_version="feature_v1",
                    assessment_to_sale_ratio=Decimal("0.810000"),
                    source_refs_json={},
                ),
            ]
        )
        score_2023 = FraudScore(
            run_id=score_run.id,
            feature_run_id=feature_run.id,
            parcel_id="P-4",
            year=2023,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("44.00"),
            risk_band="medium",
            requires_review=True,
            reason_code_count=0,
            score_summary_json={},
        )
        score_2024 = FraudScore(
            run_id=score_run.id,
            feature_run_id=feature_run.id,
            parcel_id="P-4",
            year=2024,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("62.00"),
            risk_band="medium",
            requires_review=True,
            reason_code_count=0,
            score_summary_json={},
        )
        session.add_all([score_2023, score_2024])

        session.add_all(
            [
                PermitEvent(
                    source_system="permits",
                    source_file_name="permits.csv",
                    source_file_sha256="permits-year-scope",
                    source_row_number=1,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-4",
                    permit_year=2023,
                    issued_date=date(2023, 7, 1),
                    permit_status_norm="issued",
                ),
                PermitEvent(
                    source_system="permits",
                    source_file_name="permits.csv",
                    source_file_sha256="permits-year-scope",
                    source_row_number=2,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-4",
                    permit_year=2024,
                    issued_date=date(2024, 7, 1),
                    permit_status_norm="issued",
                ),
            ]
        )
        session.add_all(
            [
                AppealEvent(
                    source_system="appeals",
                    source_file_name="appeals.csv",
                    source_file_sha256="appeals-year-scope",
                    source_row_number=1,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-4",
                    tax_year=2023,
                    decision_date=date(2023, 9, 1),
                    outcome_norm="denied",
                ),
                AppealEvent(
                    source_system="appeals",
                    source_file_name="appeals.csv",
                    source_file_sha256="appeals-year-scope",
                    source_row_number=2,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-4",
                    tax_year=2024,
                    decision_date=date(2024, 9, 1),
                    outcome_norm="denied",
                ),
            ]
        )

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["parcel-dossier", "--id", "P-4", "--year", "2024", "--year", "2024"],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)

    assert payload["request"]["years"] == [2024]
    assert payload["parcel"]["municipality_name"] == "Y-2024"
    assert {
        row["year"] for row in payload["sections"]["assessment_history"]["rows"]
    } == {2024}
    assert {
        row["transfer_date"][:4]
        for row in payload["sections"]["matched_sales"]["rows"]
        if row["transfer_date"] is not None
    } == {"2024"}
    assert {row["year"] for row in payload["sections"]["peer_context"]["rows"]} == {
        2024
    }
    assert {
        row["permit_year"] for row in payload["sections"]["permit_events"]["rows"]
    } == {2024}
    assert {
        row["tax_year"] for row in payload["sections"]["appeal_events"]["rows"]
    } == {2024}
    assert {
        row["year"] for row in payload["sections"]["reason_code_evidence"]["rows"]
    } == {2024}
    assert 2023 not in {
        row["year"] for row in payload["timeline"]["rows"] if row["year"] is not None
    }


def test_parcel_dossier_cli_returns_failed_payload_for_missing_parcel(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "parcel_dossier_missing_parcel.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["parcel-dossier", "--parcel-id", "missing"])
    assert result.exit_code == 1, result.stdout
    payload = json.loads(result.stdout)

    assert payload["run"]["status"] == "failed"
    assert payload["error"]["code"] == "parcel_not_found"
    assert payload["request"]["parcel_id"] == "missing"
    assert "parcel" not in payload
    assert "sections" not in payload
    assert "timeline" not in payload


def test_parcel_dossier_cli_rejects_combined_id_options() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["parcel-dossier", "--id", "P-1", "--parcel-id", "P-1"],
    )
    assert result.exit_code != 0
    assert "Provide exactly one of --id or --parcel-id." in result.output


def _seed_full_dossier_fixture(session, *, parcel_id: str) -> None:
    session.add(Parcel(id=parcel_id, trs_code="TRS-1"))
    session.add(
        ParcelYearFact(
            parcel_id=parcel_id,
            year=2025,
            municipality_name="Town",
            current_owner_name="Owner",
            current_primary_address="123 Main St",
            current_parcel_description="Primary parcel",
        )
    )
    session.add(
        AssessmentRecord(
            parcel_id=parcel_id,
            fetch_id=1,
            year=2025,
            valuation_classification="residential",
            total_value=Decimal("300000.00"),
            data={},
        )
    )

    sale = SalesTransaction(
        source_system="retr",
        source_file_name="retr.csv",
        source_file_sha256="sha-full-fixture",
        source_row_number=1,
        source_headers=[],
        raw_row={},
        import_status="loaded",
        transfer_date=date(2025, 6, 1),
        consideration_amount=Decimal("350000.00"),
        arms_length_indicator_norm=True,
        usable_sale_indicator_norm=True,
    )
    session.add(sale)
    session.flush()
    session.add(
        SalesParcelMatch(
            sales_transaction_id=sale.id,
            parcel_id=parcel_id,
            match_method="parcel_number",
            confidence_score=Decimal("1.0000"),
            match_rank=1,
            is_primary=True,
            match_review_status="reviewed",
        )
    )

    session.add(
        PermitEvent(
            source_system="permits",
            source_file_name="permits.csv",
            source_file_sha256="permits-full-fixture",
            source_row_number=1,
            source_headers=[],
            raw_row={},
            import_status="loaded",
            parcel_id=parcel_id,
            permit_year=2025,
            issued_date=date(2025, 5, 15),
            permit_status_norm="issued",
            permit_type="roof",
        )
    )
    session.add(
        AppealEvent(
            source_system="appeals",
            source_file_name="appeals.csv",
            source_file_sha256="appeals-full-fixture",
            source_row_number=1,
            source_headers=[],
            raw_row={},
            import_status="loaded",
            parcel_id=parcel_id,
            tax_year=2025,
            decision_date=date(2025, 8, 1),
            outcome_norm="reduction_granted",
            value_change_amount=Decimal("-25000.00"),
        )
    )

    feature_run = ScoringRun(
        run_type="build_features",
        status="succeeded",
        version_tag="feature_v1",
        scope_json={},
        config_json={},
    )
    score_run = ScoringRun(
        run_type="score_fraud",
        status="succeeded",
        version_tag="scoring_rules_v1",
        scope_json={},
        config_json={},
    )
    session.add_all([feature_run, score_run])
    session.flush()

    session.add(
        ParcelFeature(
            run_id=feature_run.id,
            parcel_id=parcel_id,
            year=2025,
            feature_version="feature_v1",
            assessment_to_sale_ratio=Decimal("0.857100"),
            peer_percentile=Decimal("0.1500"),
            yoy_assessment_change_pct=Decimal("0.110000"),
            permit_adjusted_gap=Decimal("25000.00"),
            appeal_value_delta_3y=Decimal("-15000.00"),
            lineage_value_reset_delta=Decimal("-30000.00"),
            feature_quality_flags=["permit_gap"],
            source_refs_json={},
        )
    )
    score_row = FraudScore(
        run_id=score_run.id,
        feature_run_id=feature_run.id,
        parcel_id=parcel_id,
        year=2025,
        ruleset_version="scoring_rules_v1",
        feature_version="feature_v1",
        score_value=Decimal("72.00"),
        risk_band="high",
        requires_review=True,
        reason_code_count=1,
        score_summary_json={},
        scored_at=datetime(2025, 9, 1, 12, 0, tzinfo=timezone.utc),
    )
    session.add(score_row)
    session.flush()
    session.add(
        FraudFlag(
            run_id=score_run.id,
            score_id=score_row.id,
            parcel_id=parcel_id,
            year=2025,
            ruleset_version="scoring_rules_v1",
            reason_code="R_PERMIT_GAP",
            reason_rank=1,
            severity_weight=Decimal("20.0000"),
            metric_name="permit_adjusted_gap",
            metric_value="25000.00",
            threshold_value="20000.00",
            comparison_operator="gte",
            explanation="permit gap is elevated",
            source_refs_json={"permits": {"basis": "declared"}},
        )
    )
