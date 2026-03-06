from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.appeals import AppealImportFileError, ingest_appeals_csv
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import AppealEvent


def test_ingest_appeals_imports_and_normalizes_loaded_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "appeal_import.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals.csv"
    csv_path.write_text(
        (
            "Appeal Number,Docket Number,Parcel Number,Address,Owner Name,"
            "Filing Date,Hearing Date,Decision Date,Tax Year,Outcome,"
            "Appeal Reason,Requested Value,Final Value,Assessed Value Before,"
            "Representative Name,Notes\n"
            "A-1,D-1,06-10-0139-151-1,4519 and 4521 Field Ave #2,Jane Doe,"
            "01/15/2025,2025-02-01,02-10-2025,2025,Granted,"
            "Valuation error,$200000,$180000,$190000,Agent Smith,Needs review\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-appeals", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "Appeal import summary: total=1 loaded=1 rejected=0 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(AppealEvent)).scalar_one()

    assert row.source_system == "manual_appeal_csv"
    assert row.import_status == "loaded"
    assert row.import_error is None
    assert row.import_warnings is None
    assert row.parcel_number_norm == "061001391511"
    assert row.site_address_norm == "4519 AND 4521 FIELD AVE 2"
    assert row.owner_name_norm == "JANE DOE"
    assert row.appeal_number == "A-1"
    assert row.docket_number == "D-1"
    assert row.appeal_reason_norm == "valuation"
    assert row.outcome_norm == "reduction_granted"
    assert row.filing_date and row.filing_date.isoformat() == "2025-01-15"
    assert row.hearing_date and row.hearing_date.isoformat() == "2025-02-01"
    assert row.decision_date and row.decision_date.isoformat() == "2025-02-10"
    assert row.tax_year == 2025
    assert str(row.assessed_value_before) == "190000.00"
    assert str(row.requested_assessed_value) == "200000.00"
    assert str(row.decided_assessed_value) == "180000.00"
    assert str(row.value_change_amount) == "-10000.00"
    assert row.parcel_id is None


def test_ingest_appeals_rejects_row_without_appeal_signal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "appeal_import_missing_signal.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_missing_signal.csv"
    csv_path.write_text(
        "Parcel Number,Filing Date,Outcome,Requested Value,Final Value\n"
        "06-10-0139-151-1,01/15/2025,,,\n",
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-appeals", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "Appeal import summary: total=1 loaded=0 rejected=1 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(AppealEvent)).scalar_one()

    assert row.import_status == "rejected"
    assert (
        row.import_error
        == "At least one appeal signal is required: appeal_number, docket_number, "
        "outcome_raw, requested_assessed_value, or decided_assessed_value."
    )


def test_ingest_appeals_rejects_row_without_parcel_locator(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "appeal_import_missing_parcel.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_missing_parcel.csv"
    csv_path.write_text(
        (
            "Parcel Number,Filing Date,Outcome,Requested Value,Final Value\n"
            ",01/15/2025,denial,100000,90000\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-appeals", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "Appeal import summary: total=1 loaded=0 rejected=1 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(AppealEvent)).scalar_one()

    assert row.import_status == "rejected"
    assert (
        row.import_error
        == "At least one parcel locator is required: parcel number or site address."
    )


def test_ingest_appeals_outcome_mapping_prioritizes_denial_over_partial_keyword(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "appeal_import_outcome_priority.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_outcome_priority.csv"
    csv_path.write_text(
        (
            "Appeal Number,Parcel Number,Filing Date,Outcome\n"
            "A-1,06-10-0139-151-1,01/15/2025,partial denial\n"
            "A-2,06-10-0139-151-1,01/15/2025,partial reduction\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        summary = ingest_appeals_csv(session, csv_path)

    assert summary.total_rows == 2
    assert summary.loaded_rows == 2
    assert summary.rejected_rows == 0

    with session_scope(database_url) as session:
        rows = session.execute(
            select(AppealEvent).order_by(AppealEvent.source_row_number)
        ).scalars()
        first_row, second_row = list(rows)

    assert first_row.outcome_norm == "denied"
    assert second_row.outcome_norm == "partial_reduction"


def test_ingest_appeals_records_year_anchor_mismatch_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "appeal_import_year_anchor_mismatch.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_year_anchor_mismatch.csv"
    csv_path.write_text(
        (
            "Appeal Number,Parcel Number,Filing Date,Tax Year\n"
            "A-1,06-10-0139-151-1,01/15/2025,2024\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-appeals", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert "Appeal warning counts:" in result.stdout
    assert "  tax_year_anchor_mismatch=1" in result.stdout

    with session_scope(database_url) as session:
        row = session.execute(select(AppealEvent)).scalar_one()

    assert row.import_status == "loaded"
    assert row.tax_year == 2024
    assert row.import_warnings == ["tax_year_anchor_mismatch"]


def test_ingest_appeals_records_outcome_value_direction_mismatch_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "appeal_import_outcome_direction_mismatch.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_outcome_direction_mismatch.csv"
    csv_path.write_text(
        (
            "Appeal Number,Parcel Number,Filing Date,Outcome,"
            "Assessed Value Before,Final Value\n"
            "A-1,06-10-0139-151-1,01/15/2025,reduction granted,100000,110000\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-appeals", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert "Appeal warning counts:" in result.stdout
    assert "  outcome_value_direction_mismatch=1" in result.stdout

    with session_scope(database_url) as session:
        row = session.execute(select(AppealEvent)).scalar_one()

    assert row.import_status == "loaded"
    assert row.outcome_norm == "reduction_granted"
    assert str(row.value_change_amount) == "10000.00"
    assert row.import_warnings == ["outcome_value_direction_mismatch"]


def test_ingest_appeals_treats_common_null_tokens_as_missing_values(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "appeal_import_null_tokens.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_null_tokens.csv"
    csv_path.write_text(
        (
            "Appeal Number,Parcel Number,Filing Date,"
            "Hearing Date,Decision Date,Outcome\n"
            "A-1,06-10-0139-151-1,01/15/2025,N/A,NULL,TBD\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-appeals", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "Appeal import summary: total=1 loaded=1 rejected=0 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(AppealEvent)).scalar_one()

    assert row.import_status == "loaded"
    assert row.hearing_date is None
    assert row.decision_date is None
    assert row.outcome_raw is None
    assert row.outcome_norm is None
    assert row.import_warnings is None


def test_ingest_appeals_rejects_file_without_appeal_signal_headers(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "appeal_import_missing_signal_headers.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_missing_signal_headers.csv"
    csv_path.write_text(
        "Parcel Number,Filing Date,Notes\n06-10-0139-151-1,01/15/2025,abc\n",
        encoding="utf-8",
    )

    init_db(database_url)
    with pytest.raises(
        AppealImportFileError,
        match="minimum appeal-shape requirements",
    ):
        with session_scope(database_url) as session:
            ingest_appeals_csv(session, csv_path)


def test_ingest_appeals_rejects_duplicate_headers_after_normalization(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "appeal_import_duplicate_headers.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_duplicate_headers.csv"
    csv_path.write_text(
        (
            "Appeal Number,Appeal-Number,Parcel Number,Filing Date\n"
            "A-1,A-1,06-10-0139-151-1,01/15/2025\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    with pytest.raises(
        AppealImportFileError,
        match="duplicate headers after normalization",
    ):
        with session_scope(database_url) as session:
            ingest_appeals_csv(session, csv_path)


def test_ingest_appeals_rejects_blank_header_name(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "appeal_import_blank_header.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_blank_header.csv"
    csv_path.write_text(
        ",Parcel Number,Filing Date,Appeal Number\nx,06-10-0139-151-1,01/15/2025,A-1\n",
        encoding="utf-8",
    )

    init_db(database_url)
    with pytest.raises(AppealImportFileError, match="blank header names"):
        with session_scope(database_url) as session:
            ingest_appeals_csv(session, csv_path)


def test_ingest_appeals_parses_parenthesized_amount_with_internal_spaces(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "appeal_import_parenthesized_amount.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_parenthesized_amount.csv"
    csv_path.write_text(
        (
            "Appeal Number,Parcel Number,Filing Date,Requested Value\n"
            "A-1,06-10-0139-151-1,01/15/2025,( 10 000 )\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        summary = ingest_appeals_csv(session, csv_path)

    assert summary.total_rows == 1
    assert summary.loaded_rows == 1
    assert summary.rejected_rows == 0
    assert summary.warning_counts == {}

    with session_scope(database_url) as session:
        row = session.execute(select(AppealEvent)).scalar_one()

    assert row.import_status == "loaded"
    assert str(row.requested_assessed_value) == "-10000.00"


def test_ingest_appeals_upserts_rows_when_reimporting_same_file(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "appeal_import_upsert.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_upsert.csv"
    csv_path.write_text(
        "Appeal Number,Parcel Number,Filing Date\nA-1,06-10-0139-151-1,01/15/2025\n",
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        first_summary = ingest_appeals_csv(session, csv_path)
    with session_scope(database_url) as session:
        second_summary = ingest_appeals_csv(session, csv_path)

    assert first_summary.inserted_rows == 1
    assert first_summary.updated_rows == 0
    assert second_summary.inserted_rows == 0
    assert second_summary.updated_rows == 1

    with session_scope(database_url) as session:
        count = session.execute(
            select(func.count()).select_from(AppealEvent)
        ).scalar_one()
    assert count == 1


def test_ingest_appeals_cli_prints_rejection_and_warning_breakdowns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "appeal_import_breakdown.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "appeals_breakdown.csv"
    csv_path.write_text(
        (
            "Appeal Number,Parcel Number,Filing Date\n"
            "A-1,06-10-0139-151-1,\n"
            "A-2,06-10-0139-151-1,2025-01-20,EXTRA\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-appeals", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "Appeal import summary: total=2 loaded=1 rejected=1 inserted=2 updated=0"
        in result.stdout
    )
    assert "Appeal rejection counts:" in result.stdout
    assert (
        "  At least one temporal anchor is required: filing_date, hearing_date, "
        "decision_date, or tax_year.=1"
    ) in result.stdout
    assert "Appeal warning counts:" in result.stdout
    assert "  row_has_extra_columns=1" in result.stdout
