from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    AppealEvent,
    FraudFlag,
    FraudScore,
    Parcel,
    ParcelFeature,
    ParcelYearFact,
    PermitEvent,
    SalesExclusion,
    SalesParcelMatch,
    SalesTransaction,
    ScoringRun,
)
from accessdane_audit.parcel_dossier_queries import (
    build_timeline_rows,
    get_parcel_header,
    list_appeal_events,
    list_assessment_history,
    list_matched_sales,
    list_peer_context,
    list_permit_events,
    list_reason_code_evidence,
)


def test_list_assessment_history_includes_fallback_rows_with_deterministic_order(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_dossier_assessment_history.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="P-1"))
        session.add_all(
            [
                ParcelYearFact(
                    parcel_id="P-1",
                    year=2025,
                    assessment_fetch_id=99,
                    assessment_valuation_classification="residential",
                    assessment_total_value=Decimal("330000.00"),
                    assessment_land_value=Decimal("130000.00"),
                    assessment_improved_value=Decimal("200000.00"),
                    assessment_estimated_fair_market_value=Decimal("360000.00"),
                    assessment_average_assessment_ratio=Decimal("0.9100"),
                    assessment_valuation_date=date(2025, 1, 1),
                ),
                ParcelYearFact(
                    parcel_id="P-1",
                    year=2024,
                    assessment_fetch_id=88,
                    assessment_valuation_classification="residential",
                    assessment_total_value=Decimal("320000.00"),
                ),
            ]
        )
        from accessdane_audit.models import AssessmentRecord

        session.add_all(
            [
                AssessmentRecord(
                    parcel_id="P-1",
                    fetch_id=10,
                    year=2024,
                    valuation_classification="residential",
                    total_value=Decimal("315000.00"),
                    data={},
                ),
                AssessmentRecord(
                    parcel_id="P-1",
                    fetch_id=11,
                    year=None,
                    valuation_classification="residential",
                    total_value=Decimal("100000.00"),
                    data={},
                ),
            ]
        )

    with session_scope(database_url) as session:
        rows = list_assessment_history(session, parcel_id="P-1")

    assert [row["year"] for row in rows] == [2025, 2024, None]
    assert rows[0]["assessment_id"] is None
    assert rows[0]["total_value"] == Decimal("330000.00")
    assert rows[0]["source_fetch_id"] == 99
    assert rows[1]["assessment_id"] is not None
    assert rows[1]["total_value"] == Decimal("315000.00")
    assert rows[2]["assessment_id"] is not None


def test_list_matched_sales_selects_one_representative_match_per_transaction(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_dossier_matched_sales.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id="P-1"), Parcel(id="P-2")])
        session.flush()
        txn_1 = SalesTransaction(
            source_system="retr",
            source_file_name="retr.csv",
            source_file_sha256="sha-1",
            source_row_number=1,
            source_headers=[],
            raw_row={},
            import_status="loaded",
            transfer_date=date(2025, 5, 1),
            consideration_amount=Decimal("250000.00"),
            document_number="DOC-1",
            arms_length_indicator_norm=True,
            usable_sale_indicator_norm=True,
        )
        txn_2 = SalesTransaction(
            source_system="retr",
            source_file_name="retr.csv",
            source_file_sha256="sha-1",
            source_row_number=2,
            source_headers=[],
            raw_row={},
            import_status="loaded",
            transfer_date=date(2024, 6, 1),
            consideration_amount=Decimal("300000.00"),
            document_number="DOC-2",
            arms_length_indicator_norm=False,
            usable_sale_indicator_norm=True,
        )
        txn_3 = SalesTransaction(
            source_system="retr",
            source_file_name="retr.csv",
            source_file_sha256="sha-1",
            source_row_number=3,
            source_headers=[],
            raw_row={},
            import_status="loaded",
            transfer_date=None,
            recording_date=date(2026, 1, 15),
            consideration_amount=Decimal("400000.00"),
            document_number="DOC-3",
            arms_length_indicator_norm=True,
            usable_sale_indicator_norm=True,
        )
        session.add_all([txn_1, txn_2, txn_3])
        session.flush()

        session.add_all(
            [
                # Same parcel/transaction: representative should pick
                # the higher-confidence row.
                SalesParcelMatch(
                    sales_transaction_id=txn_1.id,
                    parcel_id="P-1",
                    match_method="address",
                    confidence_score=Decimal("0.7500"),
                    match_rank=2,
                    is_primary=False,
                    match_review_status="reviewed",
                ),
                SalesParcelMatch(
                    sales_transaction_id=txn_1.id,
                    parcel_id="P-1",
                    match_method="parcel_number",
                    confidence_score=Decimal("0.9000"),
                    match_rank=3,
                    is_primary=False,
                    match_review_status="reviewed",
                ),
                # Primary for another parcel should never be selected for P-1.
                SalesParcelMatch(
                    sales_transaction_id=txn_1.id,
                    parcel_id="P-2",
                    match_method="parcel_number",
                    confidence_score=Decimal("1.0000"),
                    match_rank=1,
                    is_primary=True,
                    match_review_status="reviewed",
                ),
                # Same confidence for P-1 transaction 2, lower match_rank wins.
                SalesParcelMatch(
                    sales_transaction_id=txn_2.id,
                    parcel_id="P-1",
                    match_method="address",
                    confidence_score=Decimal("0.8000"),
                    match_rank=5,
                    is_primary=False,
                    match_review_status="reviewed",
                ),
                SalesParcelMatch(
                    sales_transaction_id=txn_2.id,
                    parcel_id="P-1",
                    match_method="parcel_number",
                    confidence_score=Decimal("0.8000"),
                    match_rank=2,
                    is_primary=False,
                    match_review_status="reviewed",
                ),
                SalesParcelMatch(
                    sales_transaction_id=txn_3.id,
                    parcel_id="P-1",
                    match_method="parcel_number",
                    confidence_score=Decimal("0.9500"),
                    match_rank=1,
                    is_primary=False,
                    match_review_status="reviewed",
                ),
            ]
        )
        session.add_all(
            [
                SalesExclusion(
                    sales_transaction_id=txn_1.id,
                    exclusion_code="non_arms_length",
                    exclusion_reason="test",
                    is_active=True,
                ),
                SalesExclusion(
                    sales_transaction_id=txn_1.id,
                    exclusion_code="related_party",
                    exclusion_reason="test",
                    is_active=True,
                ),
                SalesExclusion(
                    sales_transaction_id=txn_2.id,
                    exclusion_code="inactive_example",
                    exclusion_reason="test",
                    is_active=False,
                ),
            ]
        )

    with session_scope(database_url) as session:
        rows = list_matched_sales(session, parcel_id="P-1")

    assert [row["sales_transaction_id"] for row in rows] == [3, 1, 2]
    assert rows[0]["recording_date"] == date(2026, 1, 15)
    assert rows[1]["match_method"] == "parcel_number"
    assert rows[1]["active_exclusion_codes"] == ["non_arms_length", "related_party"]
    assert rows[2]["match_method"] == "parcel_number"
    assert rows[2]["active_exclusion_codes"] == []


def test_list_reason_code_evidence_orders_scores_and_nested_flags(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_dossier_reason_evidence.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="P-1"))
        run = ScoringRun(
            run_type="score_fraud",
            status="succeeded",
            version_tag="scoring_rules_v1",
            scope_json={},
            config_json={},
        )
        session.add(run)
        session.flush()
        score_1 = FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-1",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("70.00"),
            risk_band="high",
            requires_review=True,
            reason_code_count=2,
            score_summary_json={},
        )
        score_2 = FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-1",
            year=2024,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("90.00"),
            risk_band="high",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
        )
        score_3 = FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-1",
            year=2023,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("10.00"),
            risk_band="low",
            requires_review=False,
            reason_code_count=0,
            score_summary_json={},
        )
        session.add_all([score_1, score_2, score_3])
        session.flush()
        session.add_all(
            [
                FraudFlag(
                    run_id=run.id,
                    score_id=score_1.id,
                    parcel_id="P-1",
                    year=2025,
                    ruleset_version="scoring_rules_v1",
                    reason_code="z_reason",
                    reason_rank=2,
                    severity_weight=Decimal("10.0000"),
                    metric_name="m1",
                    metric_value="1",
                    threshold_value="2",
                    comparison_operator="lt",
                    explanation="z",
                    source_refs_json={},
                ),
                FraudFlag(
                    run_id=run.id,
                    score_id=score_1.id,
                    parcel_id="P-1",
                    year=2025,
                    ruleset_version="scoring_rules_v1",
                    reason_code="a_reason",
                    reason_rank=1,
                    severity_weight=Decimal("11.0000"),
                    metric_name="m2",
                    metric_value="1",
                    threshold_value="2",
                    comparison_operator="lt",
                    explanation="a",
                    source_refs_json={},
                ),
            ]
        )

    with session_scope(database_url) as session:
        rows = list_reason_code_evidence(
            session,
            parcel_id="P-1",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
        )

    # Rows are ordered by year desc, then score desc, then score_id desc.
    assert [row["year"] for row in rows] == [2025, 2024, 2023]
    assert [reason["reason_code"] for reason in rows[0]["reason_codes"]] == [
        "a_reason",
        "z_reason",
    ]


def test_build_timeline_rows_applies_precedence_and_excludes_fallback_assessments() -> (
    None
):
    rows = build_timeline_rows(
        assessment_history=[
            {
                "assessment_id": None,
                "year": 2024,
                "valuation_classification": "residential",
                "total_value": Decimal("1"),
                "land_value": None,
                "improved_value": None,
                "estimated_fair_market_value": None,
                "average_assessment_ratio": None,
                "valuation_date": None,
                "source_fetch_id": None,
            },
            {
                "assessment_id": 10,
                "year": 2024,
                "valuation_classification": "residential",
                "total_value": Decimal("2"),
                "land_value": None,
                "improved_value": None,
                "estimated_fair_market_value": None,
                "average_assessment_ratio": None,
                "valuation_date": date(2024, 1, 3),
                "source_fetch_id": 1,
            },
        ],
        matched_sales=[
            {
                "sales_transaction_id": 20,
                "transfer_date": date(2024, 1, 3),
                "recording_date": None,
                "consideration_amount": Decimal("300000.00"),
                "arms_length_indicator": True,
                "usable_sale_indicator": True,
                "document_number": "DOC",
                "match_method": "parcel_number",
                "match_review_status": "reviewed",
                "match_confidence_score": Decimal("1.0000"),
                "is_primary_match": True,
                "active_exclusion_codes": [],
            }
        ],
        permit_events=[
            {
                "permit_event_id": 30,
                "permit_year": 2024,
                "issued_date": date(2024, 1, 3),
                "applied_date": None,
                "finaled_date": None,
                "status_date": None,
                "permit_number": "PERM-1",
                "permit_type": "repair",
                "permit_subtype": None,
                "work_class": None,
                "permit_status": "issued",
                "declared_valuation": None,
                "estimated_cost": None,
                "description": None,
                "parcel_link_method": None,
                "parcel_link_confidence": None,
            },
            {
                "permit_event_id": 29,
                "permit_year": 2024,
                "issued_date": date(2024, 1, 3),
                "applied_date": None,
                "finaled_date": None,
                "status_date": None,
                "permit_number": "PERM-0",
                "permit_type": "repair",
                "permit_subtype": None,
                "work_class": None,
                "permit_status": "issued",
                "declared_valuation": None,
                "estimated_cost": None,
                "description": None,
                "parcel_link_method": None,
                "parcel_link_confidence": None,
            },
        ],
        appeal_events=[
            {
                "appeal_event_id": 40,
                "tax_year": 2024,
                "filing_date": date(2024, 1, 1),
                "hearing_date": date(2024, 1, 2),
                "decision_date": date(2024, 1, 3),
                "appeal_number": "A-1",
                "docket_number": None,
                "appeal_level": "board",
                "outcome": "granted",
                "assessed_value_before": None,
                "requested_assessed_value": None,
                "decided_assessed_value": None,
                "value_change_amount": None,
                "representative_name": None,
                "parcel_link_method": None,
                "parcel_link_confidence": None,
            }
        ],
        reason_code_evidence=[
            {
                "score_id": 50,
                "run_id": 1,
                "feature_run_id": None,
                "year": 2024,
                "score_value": Decimal("95.00"),
                "risk_band": "high",
                "requires_review": True,
                "reason_code_count": 1,
                "scored_at": datetime(2024, 1, 3, 14, 5, tzinfo=timezone.utc),
                "reason_codes": [],
            }
        ],
    )

    assert [row["source"]["row_id"] for row in rows] == [50, 40, 29, 30, 20, 10]
    assert all(
        not (row["event_type"] == "assessment" and row["source"]["row_id"] is None)
        for row in rows
    )


def test_dossier_support_indexes_exist_at_head(tmp_path: Path) -> None:
    db_path = tmp_path / "parcel_dossier_support_indexes.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    from sqlalchemy import inspect

    with session_scope(database_url) as session:
        db_inspector = inspect(session.get_bind())
        assessments_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in db_inspector.get_indexes("assessments")
        }
        assert assessments_indexes["ix_assessments_parcel_id_year_id"] == (
            "parcel_id",
            "year",
            "id",
        )
        sales_match_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in db_inspector.get_indexes("sales_parcel_matches")
        }
        assert sales_match_indexes[
            "ix_sales_parcel_matches_parcel_id_sales_transaction_id"
        ] == ("parcel_id", "sales_transaction_id")
        permit_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in db_inspector.get_indexes("permit_events")
        }
        assert permit_indexes["ix_permit_events_parcel_id_permit_year"] == (
            "parcel_id",
            "permit_year",
        )
        appeal_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in db_inspector.get_indexes("appeal_events")
        }
        assert appeal_indexes["ix_appeal_events_parcel_id_tax_year"] == (
            "parcel_id",
            "tax_year",
        )
        fraud_flag_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in db_inspector.get_indexes("fraud_flags")
        }
        assert fraud_flag_indexes["ix_fraud_flags_score_id_reason_rank"] == (
            "score_id",
            "reason_rank",
        )


def test_reason_code_evidence_year_scope_filters_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "parcel_dossier_reason_year_scope.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="P-1"))
        run = ScoringRun(
            run_type="score_fraud",
            status="succeeded",
            version_tag="scoring_rules_v1",
            scope_json={},
            config_json={},
        )
        session.add(run)
        session.flush()
        session.add_all(
            [
                FraudScore(
                    run_id=run.id,
                    feature_run_id=None,
                    parcel_id="P-1",
                    year=2025,
                    ruleset_version="scoring_rules_v1",
                    feature_version="feature_v1",
                    score_value=Decimal("50.00"),
                    risk_band="medium",
                    requires_review=True,
                    reason_code_count=0,
                    score_summary_json={},
                ),
                FraudScore(
                    run_id=run.id,
                    feature_run_id=None,
                    parcel_id="P-1",
                    year=2024,
                    ruleset_version="scoring_rules_v1",
                    feature_version="feature_v1",
                    score_value=Decimal("30.00"),
                    risk_band="low",
                    requires_review=False,
                    reason_code_count=0,
                    score_summary_json={},
                ),
            ]
        )

    with session_scope(database_url) as session:
        rows = list_reason_code_evidence(
            session,
            parcel_id="P-1",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            years=[2024],
        )

    assert len(rows) == 1
    assert rows[0]["year"] == 2024


def test_get_parcel_header_year_scoping_and_missing_parcel(tmp_path: Path) -> None:
    db_path = tmp_path / "parcel_dossier_header_scope.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="P-1", trs_code="TRS-1"))
        session.add_all(
            [
                ParcelYearFact(
                    parcel_id="P-1",
                    year=2023,
                    municipality_name="Town-2023",
                    current_owner_name="Owner-2023",
                    current_primary_address="Address-2023",
                    current_parcel_description="Desc-2023",
                ),
                ParcelYearFact(
                    parcel_id="P-1",
                    year=2024,
                    municipality_name="Town-2024",
                    current_owner_name="Owner-2024",
                    current_primary_address="Address-2024",
                    current_parcel_description="Desc-2024",
                ),
                ParcelYearFact(
                    parcel_id="P-1",
                    year=2025,
                    municipality_name="Town-2025",
                    current_owner_name="Owner-2025",
                    current_primary_address="Address-2025",
                    current_parcel_description="Desc-2025",
                ),
            ]
        )

    with session_scope(database_url) as session:
        scoped = get_parcel_header(session, parcel_id="P-1", years=[2023, 2024])
        unscoped = get_parcel_header(session, parcel_id="P-1")
        missing = get_parcel_header(session, parcel_id="P-missing")

    assert scoped is not None
    assert scoped["parcel_id"] == "P-1"
    assert scoped["trs_code"] == "TRS-1"
    assert scoped["municipality_name"] == "Town-2024"
    assert scoped["current_owner_name"] == "Owner-2024"
    assert scoped["current_primary_address"] == "Address-2024"
    assert scoped["current_parcel_description"] == "Desc-2024"

    assert unscoped is not None
    assert unscoped["municipality_name"] == "Town-2025"
    assert unscoped["current_owner_name"] == "Owner-2025"
    assert missing is None


def test_list_peer_context_orders_rows_and_normalizes_quality_flags(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_dossier_peer_context.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="P-1"))
        session.add_all(
            [
                ParcelYearFact(
                    parcel_id="P-1",
                    year=2024,
                    municipality_name="Town-2024",
                    assessment_valuation_classification="residential",
                ),
                ParcelYearFact(
                    parcel_id="P-1",
                    year=2025,
                    municipality_name="Town-2025",
                    assessment_valuation_classification="residential",
                ),
            ]
        )
        run_1 = ScoringRun(
            run_type="build_features",
            status="succeeded",
            version_tag="feature_v1",
            scope_json={},
            config_json={},
        )
        run_2 = ScoringRun(
            run_type="build_features",
            status="succeeded",
            version_tag="feature_v1",
            scope_json={},
            config_json={},
        )
        session.add_all([run_1, run_2])
        session.flush()
        session.add_all(
            [
                ParcelFeature(
                    run_id=run_1.id,
                    parcel_id="P-1",
                    year=2024,
                    feature_version="feature_v1",
                    assessment_to_sale_ratio=Decimal("0.950000"),
                    feature_quality_flags=None,
                    source_refs_json={},
                ),
                ParcelFeature(
                    run_id=run_2.id,
                    parcel_id="P-1",
                    year=2025,
                    feature_version="feature_v1",
                    assessment_to_sale_ratio=Decimal("1.050000"),
                    feature_quality_flags=["permit_gap"],
                    source_refs_json={},
                ),
            ]
        )

    with session_scope(database_url) as session:
        rows = list_peer_context(
            session,
            parcel_id="P-1",
            feature_version="feature_v1",
        )

    assert [row["year"] for row in rows] == [2025, 2024]
    assert rows[0]["municipality_name"] == "Town-2025"
    assert rows[0]["feature_quality_flags"] == ["permit_gap"]
    assert rows[1]["feature_quality_flags"] == []


def test_list_permit_events_year_scoping_with_fallback_dates(tmp_path: Path) -> None:
    db_path = tmp_path / "parcel_dossier_permit_events.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id="P-1"), Parcel(id="P-2")])
        session.add_all(
            [
                PermitEvent(
                    source_system="permits",
                    source_file_name="permits.csv",
                    source_file_sha256="permits-sha",
                    source_row_number=1,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-1",
                    permit_year=None,
                    applied_date=date(2024, 6, 1),
                    permit_number="PERM-1",
                    permit_type="roof",
                ),
                PermitEvent(
                    source_system="permits",
                    source_file_name="permits.csv",
                    source_file_sha256="permits-sha",
                    source_row_number=2,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-1",
                    permit_year=None,
                    status_date=date(2024, 12, 31),
                    permit_number="PERM-2",
                    permit_type="plumbing",
                ),
                PermitEvent(
                    source_system="permits",
                    source_file_name="permits.csv",
                    source_file_sha256="permits-sha",
                    source_row_number=3,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-1",
                    permit_year=2024,
                    permit_number="PERM-3",
                    permit_type="electrical",
                ),
                PermitEvent(
                    source_system="permits",
                    source_file_name="permits.csv",
                    source_file_sha256="permits-sha",
                    source_row_number=4,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-1",
                    permit_year=2025,
                    issued_date=date(2025, 1, 15),
                    permit_number="PERM-4",
                    permit_type="addition",
                ),
                PermitEvent(
                    source_system="permits",
                    source_file_name="permits.csv",
                    source_file_sha256="permits-sha",
                    source_row_number=5,
                    source_headers=[],
                    raw_row={},
                    import_status="failed",
                    parcel_id="P-1",
                    permit_year=2024,
                    issued_date=date(2024, 2, 1),
                    permit_number="PERM-5",
                    permit_type="ignored",
                ),
                PermitEvent(
                    source_system="permits",
                    source_file_name="permits.csv",
                    source_file_sha256="permits-sha",
                    source_row_number=6,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-2",
                    permit_year=2024,
                    issued_date=date(2024, 3, 1),
                    permit_number="PERM-6",
                    permit_type="other",
                ),
            ]
        )

    with session_scope(database_url) as session:
        rows = list_permit_events(session, parcel_id="P-1", years=[2024])

    assert [row["permit_event_id"] for row in rows] == [2, 1, 3]
    assert rows[0]["permit_year"] is None
    assert rows[1]["permit_year"] is None
    assert rows[2]["permit_year"] == 2024


def test_list_appeal_events_year_scoping_with_fallback_dates(tmp_path: Path) -> None:
    db_path = tmp_path / "parcel_dossier_appeal_events.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id="P-1"), Parcel(id="P-2")])
        session.add_all(
            [
                AppealEvent(
                    source_system="appeals",
                    source_file_name="appeals.csv",
                    source_file_sha256="appeals-sha",
                    source_row_number=1,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-1",
                    tax_year=None,
                    hearing_date=date(2024, 8, 1),
                    filing_date=date(2024, 7, 1),
                    appeal_number="A-1",
                ),
                AppealEvent(
                    source_system="appeals",
                    source_file_name="appeals.csv",
                    source_file_sha256="appeals-sha",
                    source_row_number=2,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-1",
                    tax_year=2024,
                    appeal_number="A-2",
                ),
                AppealEvent(
                    source_system="appeals",
                    source_file_name="appeals.csv",
                    source_file_sha256="appeals-sha",
                    source_row_number=3,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-1",
                    tax_year=None,
                    decision_date=date(2025, 2, 1),
                    appeal_number="A-3",
                ),
                AppealEvent(
                    source_system="appeals",
                    source_file_name="appeals.csv",
                    source_file_sha256="appeals-sha",
                    source_row_number=4,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-1",
                    tax_year=None,
                    decision_date=date(2024, 9, 1),
                    appeal_number="A-4",
                ),
                AppealEvent(
                    source_system="appeals",
                    source_file_name="appeals.csv",
                    source_file_sha256="appeals-sha",
                    source_row_number=5,
                    source_headers=[],
                    raw_row={},
                    import_status="failed",
                    parcel_id="P-1",
                    tax_year=2024,
                    decision_date=date(2024, 6, 1),
                    appeal_number="A-5",
                ),
                AppealEvent(
                    source_system="appeals",
                    source_file_name="appeals.csv",
                    source_file_sha256="appeals-sha",
                    source_row_number=6,
                    source_headers=[],
                    raw_row={},
                    import_status="loaded",
                    parcel_id="P-2",
                    tax_year=2024,
                    decision_date=date(2024, 6, 1),
                    appeal_number="A-6",
                ),
            ]
        )

    with session_scope(database_url) as session:
        rows = list_appeal_events(session, parcel_id="P-1", years=[2024])

    assert [row["appeal_event_id"] for row in rows] == [4, 1, 2]
    assert rows[0]["tax_year"] is None
    assert rows[1]["tax_year"] is None
    assert rows[2]["tax_year"] == 2024
