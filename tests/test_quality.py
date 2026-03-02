from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import AssessmentRecord, Fetch, Parcel, PaymentRecord, TaxRecord
from accessdane_audit.quality import quality_report_to_dict, run_data_quality_checks


def test_run_data_quality_checks_flags_expected_issue_categories(tmp_path: Path) -> None:
    db_path = tmp_path / "quality.sqlite"
    database_url = f"sqlite:///{db_path}"
    now = datetime.now(timezone.utc)

    init_db(database_url)

    with session_scope(database_url) as session:
        fetch_missing_raw = Fetch(parcel_id="p1", url="https://example.test/p1", status_code=200)
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
                data={"Tax Year": "2025", "Date of Payment": "01/15/2025", "Amount": "-$2.00"},
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


def test_check_data_quality_cli_emits_json_and_can_fail_on_issues(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "quality_cli.sqlite"
    database_url = f"sqlite:///{db_path}"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="p1"))
        session.add(Fetch(parcel_id="p1", url="https://example.test/p1", status_code=200))

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


def test_run_data_quality_checks_allows_typical_carry_forward_but_flags_truly_stale_dates(
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
        session.add_all(
            [
                Fetch(parcel_id="p1", url="https://example.test/p1", status_code=200),
                Fetch(parcel_id="p2", url="https://example.test/p2", status_code=200),
            ]
        )

    with session_scope(database_url) as session:
        report = run_data_quality_checks(session, parcel_ids=["p1"])

    payload = quality_report_to_dict(report)
    checks = {check["code"]: check for check in payload["checks"]}
    issues = checks["fetch_parse_consistency"]["issues"]

    assert issues
    assert {issue["parcel_id"] for issue in issues} == {"p1"}
