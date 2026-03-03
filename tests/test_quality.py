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
    AssessmentRecord,
    Fetch,
    Parcel,
    ParcelCharacteristic,
    ParcelLineageLink,
    ParcelYearFact,
    PaymentRecord,
    TaxRecord,
)
from accessdane_audit.quality import quality_report_to_dict, run_data_quality_checks


def test_run_data_quality_checks_flags_expected_issue_categories(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quality.sqlite"
    database_url = f"sqlite:///{db_path}"
    now = datetime.now(timezone.utc)

    init_db(database_url)

    with session_scope(database_url) as session:
        fetch_missing_raw = Fetch(
            parcel_id="p1", url="https://example.test/p1", status_code=200
        )
        fetch_parsed_empty = Fetch(
            parcel_id="p2",
            url="https://example.test/p2",
            status_code=200,
            raw_path="/tmp/p2.html",
            parsed_at=now,
        )
        fetch_bad_status = Fetch(
            parcel_id="p3",
            url="https://example.test/p3",
            status_code=500,
            raw_path="/tmp/p3.html",
            parsed_at=now,
        )
        fetch_bad_values = Fetch(
            parcel_id="p4",
            url="https://example.test/p4",
            status_code=200,
            raw_path="/tmp/p4.html",
            parsed_at=now,
        )
        session.add_all(
            [
                Parcel(id="p1"),
                Parcel(id="p2"),
                Parcel(id="p3"),
                Parcel(id="p4"),
            ]
        )
        session.add_all(
            [
                fetch_missing_raw,
                fetch_parsed_empty,
                fetch_bad_status,
                fetch_bad_values,
            ]
        )
        session.flush()
        session.add(
            AssessmentRecord(
                parcel_id="p4",
                fetch_id=fetch_bad_values.id,
                year=2025,
                valuation_classification="G1",
                assessment_acres=Decimal("1.000"),
                land_value=Decimal("-1.00"),
                improved_value=Decimal("2.00"),
                total_value=Decimal("0.50"),
                average_assessment_ratio=None,
                estimated_fair_market_value=None,
                valuation_date=date(2027, 1, 1),
                data={"source": "detail"},
            )
        )
        session.add(
            TaxRecord(
                parcel_id="p4",
                fetch_id=fetch_bad_values.id,
                year=2025,
                data={"source": "summary", "Amount": "-$5.00"},
            )
        )
        session.add(
            PaymentRecord(
                parcel_id="p4",
                fetch_id=fetch_bad_values.id,
                year=2025,
                data={
                    "Tax Year": "2025",
                    "Date of Payment": "01/15/2025",
                    "Amount": "-$2.00",
                },
            )
        )

    with session_scope(database_url) as session:
        report = run_data_quality_checks(session)

    payload = quality_report_to_dict(report)
    checks = {check["code"]: check for check in payload["checks"]}

    assert payload["passed"] is False
    assert checks["duplicate_parcel_summaries"]["passed"] is True
    assert checks["suspicious_assessment_dates"]["issue_count"] == 1
    assert checks["impossible_numeric_values"]["issue_count"] >= 4
    assert checks["fetch_parse_consistency"]["issue_count"] >= 4

    numeric_issue_codes = {
        issue["code"] for issue in checks["impossible_numeric_values"]["issues"]
    }
    assert "negative_assessment_value" in numeric_issue_codes
    assert "assessment_total_less_than_components" in numeric_issue_codes
    assert "negative_tax_amount" in numeric_issue_codes
    assert "negative_payment_amount" in numeric_issue_codes

    consistency_issue_codes = {
        issue["code"] for issue in checks["fetch_parse_consistency"]["issues"]
    }
    assert "missing_raw_path" in consistency_issue_codes
    assert "unparsed_successful_fetch" in consistency_issue_codes
    assert "parsed_without_records" in consistency_issue_codes
    assert "parsed_non_200_fetch" in consistency_issue_codes


def test_check_data_quality_cli_emits_json_and_can_fail_on_issues(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "quality_cli.sqlite"
    database_url = f"sqlite:///{db_path}"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="p1"))
        session.add(
            Fetch(parcel_id="p1", url="https://example.test/p1", status_code=200)
        )

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["check-data-quality", "--fail-on-issues"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["passed"] is False
    checks = {check["code"]: check for check in payload["checks"]}
    assert checks["fetch_parse_consistency"]["issue_count"] >= 1


def test_run_data_quality_checks_suppresses_carry_forward_but_flags_stale_dates(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quality_stale_dates.sqlite"
    database_url = f"sqlite:///{db_path}"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id="p1"), Parcel(id="p2")])
        fetch_one = Fetch(
            parcel_id="p1",
            url="https://example.test/p1",
            status_code=200,
            raw_path="/tmp/p1.html",
            parsed_at=datetime.now(timezone.utc),
        )
        fetch_two = Fetch(
            parcel_id="p2",
            url="https://example.test/p2",
            status_code=200,
            raw_path="/tmp/p2.html",
            parsed_at=datetime.now(timezone.utc),
        )
        session.add_all([fetch_one, fetch_two])
        session.flush()
        session.add_all(
            [
                AssessmentRecord(
                    parcel_id="p1",
                    fetch_id=fetch_one.id,
                    year=2025,
                    valuation_classification="G1",
                    valuation_date=date(2021, 1, 1),
                    total_value=Decimal("100.00"),
                    data={"source": "detail"},
                ),
                AssessmentRecord(
                    parcel_id="p1",
                    fetch_id=fetch_one.id,
                    year=2025,
                    valuation_classification="G1",
                    valuation_date=date(2020, 1, 1),
                    total_value=Decimal("200.00"),
                    data={"source": "detail"},
                ),
            ]
        )
        session.add_all(
            [
                AssessmentRecord(
                    parcel_id="p2",
                    fetch_id=fetch_two.id,
                    year=year,
                    valuation_classification="G1",
                    valuation_date=date(2020, 1, 1),
                    total_value=Decimal("300.00"),
                    data={"source": "detail"},
                )
                for year in range(2020, 2026)
            ]
        )

    with session_scope(database_url) as session:
        payload = quality_report_to_dict(run_data_quality_checks(session))

    checks = {check["code"]: check for check in payload["checks"]}
    issues = checks["suspicious_assessment_dates"]["issues"]

    assert [issue["code"] for issue in issues] == ["stale_assessment_date"]
    assert issues[0]["parcel_id"] == "p1"
    assert issues[0]["year"] == 2025
    assert issues[0]["details"]["valuation_date"] == "2020-01-01"


def test_run_data_quality_checks_can_filter_to_selected_parcels(tmp_path: Path) -> None:
    db_path = tmp_path / "quality_filtered.sqlite"
    database_url = f"sqlite:///{db_path}"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id="p1"), Parcel(id="p2")])
        fetch_p1 = Fetch(
            parcel_id="p1",
            url="https://example.test/p1",
            status_code=200,
        )
        fetch_p2 = Fetch(
            parcel_id="p2",
            url="https://example.test/p2",
            status_code=200,
            raw_path="/tmp/p2.html",
            parsed_at=datetime.now(timezone.utc),
        )
        session.add_all([fetch_p1, fetch_p2])
        session.flush()
        session.add(
            ParcelCharacteristic(
                parcel_id="p2",
                source_fetch_id=fetch_p2.id,
                current_valuation_classification="X1",
                is_exempt_style_page=False,
                current_payment_history_available=False,
            )
        )
        session.add(
            ParcelLineageLink(
                parcel_id="p2",
                related_parcel_id="p2",
                relationship_type="parent",
                source_fetch_id=fetch_p2.id,
            )
        )
        session.add(
            PaymentRecord(
                parcel_id="p2",
                fetch_id=fetch_p2.id,
                year=2025,
                data={
                    "Date of Payment": "01/15/2025",
                    "Amount": "$10.00",
                },
            )
        )
        session.add(
            ParcelYearFact(
                parcel_id="p2",
                year=2025,
                payment_fetch_id=fetch_p2.id,
                payment_event_count=1,
                payment_total_amount=Decimal("10.00"),
                payment_first_date=date(2025, 1, 15),
                payment_last_date=date(2025, 1, 15),
                payment_has_placeholder_row=True,
            )
        )

    with session_scope(database_url) as session:
        report = run_data_quality_checks(session, parcel_ids=["p1"])

    payload = quality_report_to_dict(report)
    checks = {check["code"]: check for check in payload["checks"]}
    issues = checks["fetch_parse_consistency"]["issues"]

    assert issues
    assert {issue["parcel_id"] for issue in issues} == {"p1"}
    assert checks["parcel_characteristics_consistency"]["passed"] is True
    assert checks["lineage_consistency"]["passed"] is True
    assert checks["payment_history_semantics"]["passed"] is True


def test_run_data_quality_checks_flags_new_extraction_layer_issues(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quality_extraction_layers.sqlite"
    database_url = f"sqlite:///{db_path}"
    now = datetime.now(timezone.utc)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all(
            [
                Parcel(id="p1"),
                Parcel(id="p2"),
                Parcel(id="p3"),
                Parcel(id="p4"),
            ]
        )
        fetch_exempt = Fetch(
            parcel_id="p1",
            url="https://example.test/p1",
            status_code=200,
            raw_path="/tmp/p1.html",
            parsed_at=now,
        )
        fetch_payment = Fetch(
            parcel_id="p2",
            url="https://example.test/p2",
            status_code=200,
            raw_path="/tmp/p2.html",
            parsed_at=now,
        )
        fetch_lineage = Fetch(
            parcel_id="p3",
            url="https://example.test/p3",
            status_code=200,
            raw_path="/tmp/p3.html",
            parsed_at=now,
        )
        fetch_conflict = Fetch(
            parcel_id="p4",
            url="https://example.test/p4",
            status_code=200,
            raw_path="/tmp/p4.html",
            parsed_at=now,
        )
        session.add_all([fetch_exempt, fetch_payment, fetch_lineage, fetch_conflict])
        session.flush()

        session.add(
            ParcelCharacteristic(
                parcel_id="p1",
                source_fetch_id=fetch_exempt.id,
                current_valuation_classification="X1",
                is_exempt_style_page=False,
            )
        )
        session.add(
            ParcelCharacteristic(
                parcel_id="p2",
                source_fetch_id=fetch_payment.id,
                current_payment_history_available=False,
            )
        )
        session.add(
            PaymentRecord(
                parcel_id="p2",
                fetch_id=fetch_payment.id,
                year=2025,
                data={
                    "Date of Payment": "01/15/2025",
                    "Amount": "$2.00",
                },
            )
        )
        session.add(
            ParcelLineageLink(
                parcel_id="p3",
                related_parcel_id="p3",
                relationship_type="parent",
                source_fetch_id=fetch_lineage.id,
            )
        )
        session.add_all(
            [
                ParcelLineageLink(
                    parcel_id="p4",
                    related_parcel_id="p5",
                    relationship_type="parent",
                    source_fetch_id=fetch_conflict.id,
                ),
                ParcelLineageLink(
                    parcel_id="p4",
                    related_parcel_id="p5",
                    relationship_type="child",
                    source_fetch_id=fetch_conflict.id,
                ),
            ]
        )
        session.add(
            ParcelYearFact(
                parcel_id="p2",
                year=2025,
                payment_fetch_id=fetch_payment.id,
                payment_event_count=1,
                payment_total_amount=Decimal("2.00"),
                payment_first_date=date(2025, 1, 15),
                payment_last_date=date(2025, 1, 15),
                payment_has_placeholder_row=True,
            )
        )

    with session_scope(database_url) as session:
        payload = quality_report_to_dict(run_data_quality_checks(session))

    checks = {check["code"]: check for check in payload["checks"]}

    characteristic_issue_codes = {
        issue["code"]
        for issue in checks["parcel_characteristics_consistency"]["issues"]
    }
    assert "missing_exempt_style_flag" in characteristic_issue_codes

    lineage_issue_codes = {
        issue["code"] for issue in checks["lineage_consistency"]["issues"]
    }
    assert "self_referential_lineage_link" in lineage_issue_codes
    assert "conflicting_lineage_relationship" in lineage_issue_codes

    payment_issue_codes = {
        issue["code"] for issue in checks["payment_history_semantics"]["issues"]
    }
    assert "payment_history_flag_mismatch" in payment_issue_codes
    assert "payment_placeholder_flag_mismatch" in payment_issue_codes
    assert "placeholder_payment_rollup_conflict" in payment_issue_codes


def test_run_data_quality_checks_treats_col_2_placeholder_rows_as_placeholders(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quality_placeholder_col2.sqlite"
    database_url = f"sqlite:///{db_path}"
    now = datetime.now(timezone.utc)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="p1"))
        fetch = Fetch(
            parcel_id="p1",
            url="https://example.test/p1",
            status_code=200,
            raw_path="/tmp/p1.html",
            parsed_at=now,
        )
        session.add(fetch)
        session.flush()
        session.add(
            ParcelCharacteristic(
                parcel_id="p1",
                source_fetch_id=fetch.id,
                current_payment_history_available=True,
            )
        )
        session.add(
            PaymentRecord(
                parcel_id="p1",
                fetch_id=fetch.id,
                year=2025,
                data={
                    "col_1": "2025",
                    "col_2": "No payments found.",
                    "Amount": "",
                },
            )
        )
        session.add(
            ParcelYearFact(
                parcel_id="p1",
                year=2025,
                payment_fetch_id=fetch.id,
                payment_event_count=1,
                payment_total_amount=Decimal("1.00"),
                payment_first_date=date(2025, 1, 15),
                payment_last_date=date(2025, 1, 15),
                payment_has_placeholder_row=False,
            )
        )

    with session_scope(database_url) as session:
        payload = quality_report_to_dict(run_data_quality_checks(session))

    checks = {check["code"]: check for check in payload["checks"]}
    issues_by_code = {
        issue["code"]: issue for issue in checks["payment_history_semantics"]["issues"]
    }

    assert set(issues_by_code) == {
        "payment_history_flag_mismatch",
        "payment_placeholder_flag_mismatch",
        "placeholder_payment_rollup_conflict",
    }
    assert issues_by_code["payment_history_flag_mismatch"]["parcel_id"] == "p1"
    assert issues_by_code["payment_history_flag_mismatch"]["details"] == {
        "expected_current_payment_history_available": False,
        "actual_current_payment_history_available": True,
    }
    assert issues_by_code["payment_placeholder_flag_mismatch"]["details"] == {
        "expected_payment_has_placeholder_row": True,
        "actual_payment_has_placeholder_row": False,
    }
