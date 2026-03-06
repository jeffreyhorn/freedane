from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    Fetch,
    Parcel,
    ParcelCharacteristic,
    ParcelSummary,
    PermitEvent,
)
from accessdane_audit.permits import PermitImportFileError, ingest_permits_csv


def test_ingest_permits_imports_and_normalizes_loaded_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits.csv"
    csv_path.write_text(
        (
            "Permit #,Parcel Number,Address,Applied Date,Issued Date,Permit Status,"
            "Declared Valuation\n"
            'P-1," 06-10-0139-151-1 ","4519 and 4521 Field Ave #2",'
            '01/15/2025,2025-01-20," Issued ","$12,345.50"\n'
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
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "Permit import summary: total=1 loaded=1 rejected=0 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.source_system == "manual_permit_csv"
    assert row.import_status == "loaded"
    assert row.import_error is None
    assert row.import_warnings is None
    assert row.parcel_number_raw == " 06-10-0139-151-1 "
    assert row.parcel_number_norm == "061001391511"
    assert row.site_address_raw == "4519 and 4521 Field Ave #2"
    assert row.site_address_norm == "4519 AND 4521 FIELD AVE 2"
    assert row.permit_status_raw == " Issued "
    assert row.permit_status_norm == "issued"
    assert row.applied_date.isoformat() == "2025-01-15"
    assert row.issued_date.isoformat() == "2025-01-20"
    assert row.permit_year == 2025
    assert str(row.declared_valuation) == "12345.50"
    assert row.estimated_cost is None
    assert row.parcel_id is None


def test_ingest_permits_rejects_row_without_parcel_locator(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_missing_locator.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_missing_locator.csv"
    csv_path.write_text(
        "Permit Number,Parcel Number,Applied Date\nP-1, ,01/15/2025\n",
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "Permit import summary: total=1 loaded=0 rejected=1 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.import_status == "rejected"
    assert (
        row.import_error
        == "At least one parcel locator is required: parcel number or site address."
    )


def test_ingest_permits_keeps_row_loaded_when_other_temporal_anchor_exists(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_unparseable_date.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_unparseable_date.csv"
    csv_path.write_text(
        "Parcel Number,Issued Date,Permit Year\n06-10-0139-151-1,not-a-date,2024\n",
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "Permit import summary: total=1 loaded=1 rejected=0 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.import_status == "loaded"
    assert row.permit_year == 2024
    assert row.issued_date is None
    assert row.import_warnings == ["issued_date_unparseable"]


def test_ingest_permits_parses_common_datetime_date_variants(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "permit_import_datetime_variants.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_datetime_variants.csv"
    csv_path.write_text(
        ("Parcel Number,Issued Date\n06-10-0139-151-1,2025-01-20T14:30:00Z\n"),
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        summary = ingest_permits_csv(session, csv_path)

    assert summary.total_rows == 1
    assert summary.loaded_rows == 1
    assert summary.rejected_rows == 0
    assert summary.rejection_reason_counts == {}
    assert summary.warning_counts == {}

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.import_status == "loaded"
    assert row.issued_date and row.issued_date.isoformat() == "2025-01-20"
    assert row.permit_year == 2025
    assert row.import_warnings is None


def test_ingest_permits_records_year_anchor_mismatch_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_year_mismatch.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_year_mismatch.csv"
    csv_path.write_text(
        "Parcel Number,Issued Date,Permit Year\n06-10-0139-151-1,01/05/2025,2024\n",
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.import_status == "loaded"
    assert row.permit_year == 2024
    assert row.issued_date and row.issued_date.year == 2025
    assert row.import_warnings == ["permit_year_anchor_mismatch"]


def test_ingest_permits_upserts_rows_when_reimporting_same_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_upsert.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_upsert.csv"
    csv_path.write_text(
        "Parcel Number,Issued Date\n06-10-0139-151-1,01/15/2025\n",
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    first_result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])
    second_result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert first_result.exit_code == 0, first_result.stdout
    assert second_result.exit_code == 0, second_result.stdout
    assert (
        "Permit import summary: total=1 loaded=1 rejected=0 inserted=1 updated=0"
        in first_result.stdout
    )
    assert (
        "Permit import summary: total=1 loaded=1 rejected=0 inserted=0 updated=1"
        in second_result.stdout
    )

    with session_scope(database_url) as session:
        count = session.execute(
            select(func.count()).select_from(PermitEvent)
        ).scalar_one()
    assert count == 1


def test_ingest_permits_treats_common_null_tokens_as_missing_values(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "permit_import_null_tokens.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_null_tokens.csv"
    csv_path.write_text(
        (
            "Parcel Number,Address,Issued Date,Permit Year,Permit Status,"
            "Declared Valuation\n"
            "N/A,4519 Field Ave,n/a,2025,N/A,N/A\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        summary = ingest_permits_csv(session, csv_path)

    assert summary.total_rows == 1
    assert summary.loaded_rows == 1
    assert summary.rejected_rows == 0
    assert summary.rejection_reason_counts == {}
    assert summary.warning_counts == {}

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.import_status == "loaded"
    assert row.import_warnings is None
    assert row.parcel_number_raw is None
    assert row.parcel_number_norm is None
    assert row.site_address_norm == "4519 FIELD AVE"
    assert row.issued_date is None
    assert row.permit_year == 2025
    assert row.permit_status_raw is None
    assert row.permit_status_norm is None
    assert row.declared_valuation is None


def test_ingest_permits_records_row_shape_warnings_and_summary_counters(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "permit_import_shape_warnings.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_shape_warnings.csv"
    csv_path.write_text(
        (
            "Parcel Number,Issued Date,Permit Year,Declared Valuation\n"
            "06-10-0139-151-1\n"
            "06-10-0139-151-1,2025-01-20,2025,($12345.67),EXTRA\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        summary = ingest_permits_csv(session, csv_path)

    assert summary.total_rows == 2
    assert summary.loaded_rows == 1
    assert summary.rejected_rows == 1
    assert summary.inserted_rows == 2
    assert summary.updated_rows == 0
    assert summary.rejection_reason_counts == {
        "At least one temporal anchor is required: applied_date, issued_date, "
        "finaled_date, status_date, or permit_year.": 1
    }
    assert summary.warning_counts == {
        "row_has_extra_columns": 1,
        "row_shorter_than_header": 1,
    }

    with session_scope(database_url) as session:
        rows = session.execute(
            select(PermitEvent).order_by(PermitEvent.source_row_number)
        ).scalars()
        rejected_row, loaded_row = list(rows)

    assert rejected_row.import_status == "rejected"
    assert rejected_row.import_warnings == ["row_shorter_than_header"]

    assert loaded_row.import_status == "loaded"
    assert loaded_row.import_warnings == ["row_has_extra_columns"]
    assert loaded_row.raw_row["_extra_columns"] == ["EXTRA"]
    assert str(loaded_row.declared_valuation) == "-12345.67"


def test_ingest_permits_parses_parenthesized_amount_with_internal_spaces(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "permit_import_parenthesized_amount.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_parenthesized_amount.csv"
    csv_path.write_text(
        (
            "Parcel Number,Issued Date,Declared Valuation\n"
            "06-10-0139-151-1,2025-01-20,( $12345.67 )\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        summary = ingest_permits_csv(session, csv_path)

    assert summary.total_rows == 1
    assert summary.loaded_rows == 1
    assert summary.rejected_rows == 0
    assert summary.warning_counts == {}
    assert summary.rejection_reason_counts == {}

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.import_status == "loaded"
    assert row.import_warnings is None
    assert str(row.declared_valuation) == "-12345.67"


def test_ingest_permits_cli_prints_rejection_and_warning_breakdowns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_breakdown_output.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_breakdown_output.csv"
    csv_path.write_text(
        (
            "Parcel Number,Issued Date,Permit Year\n"
            "06-10-0139-151-1\n"
            "06-10-0139-151-1,2025-01-20,2025,EXTRA\n"
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
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "Permit import summary: total=2 loaded=1 rejected=1 inserted=2 updated=0"
        in result.stdout
    )
    assert "Permit rejection counts:" in result.stdout
    assert (
        "  At least one temporal anchor is required: applied_date, issued_date, "
        "finaled_date, status_date, or permit_year.=1"
    ) in result.stdout
    assert "Permit warning counts:" in result.stdout
    assert "  row_has_extra_columns=1" in result.stdout
    assert "  row_shorter_than_header=1" in result.stdout


def test_ingest_permits_rejects_duplicate_headers_after_normalization(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "permit_import_duplicate_headers.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_duplicate_headers.csv"
    csv_path.write_text(
        (
            "Permit-Year,Permit Year,Parcel Number,Issued Date\n"
            "2025,2025,06-10-0139-151-1,01/15/2025\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    with pytest.raises(PermitImportFileError) as exc_info:
        with session_scope(database_url) as session:
            ingest_permits_csv(session, csv_path)

    assert (
        "duplicate headers after normalization: permit year"
        in str(exc_info.value).lower()
    )


def test_ingest_permits_rejects_empty_header_row(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "permit_import_blank_header.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_blank_header.csv"
    csv_path.write_text(
        "\nPermit Number,Parcel Number,Issued Date\nP-1,06-10-0139-151-1,01/15/2025\n",
        encoding="utf-8",
    )

    init_db(database_url)
    with pytest.raises(PermitImportFileError, match="missing a header row"):
        with session_scope(database_url) as session:
            ingest_permits_csv(session, csv_path)


def test_ingest_permits_links_unique_exact_parcel_number_match(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_exact_link.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_exact_link.csv"
    csv_path.write_text(
        "Parcel Number,Issued Date\n06-10-0139-151-1,01/15/2025\n",
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        session.add_all(
            [
                Parcel(id="0601001391511"),
                Fetch(
                    id=1,
                    parcel_id="0601001391511",
                    url="https://example.test/parcel/0601001391511",
                ),
                ParcelCharacteristic(
                    parcel_id="0601001391511",
                    formatted_parcel_number="06-10-0139-151-1",
                ),
            ]
        )
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.parcel_id == "0601001391511"
    assert row.parcel_link_method == "exact_parcel_number"
    assert str(row.parcel_link_confidence) == "1.0000"


def test_ingest_permits_links_unique_crosswalk_parcel_number_match(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_crosswalk_link.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_crosswalk_link.csv"
    csv_path.write_text(
        "Parcel Number,Issued Date\n06-01/060101391511,01/15/2025\n",
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        session.add_all(
            [
                Parcel(id="0601061001391511"),
                Fetch(
                    id=1,
                    parcel_id="0601061001391511",
                    url="https://example.test/parcel/0601061001391511",
                ),
                ParcelCharacteristic(
                    parcel_id="0601061001391511",
                    formatted_parcel_number="06-01/061001391511",
                ),
            ]
        )
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.parcel_id == "0601061001391511"
    assert row.parcel_link_method == "parcel_number_crosswalk"
    assert str(row.parcel_link_confidence) == "0.9500"


def test_ingest_permits_links_unique_normalized_address_match(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_address_link.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_address_link.csv"
    csv_path.write_text(
        "Address,Issued Date\n4519 and 4521 Field Ave #2,01/15/2025\n",
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        session.add_all(
            [
                Parcel(id="0601001391511"),
                Fetch(
                    id=1,
                    parcel_id="0601001391511",
                    url="https://example.test/parcel/0601001391511",
                ),
                ParcelSummary(
                    parcel_id="0601001391511",
                    fetch_id=1,
                    primary_address="4519 and 4521 Field Ave #2",
                ),
            ]
        )
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.parcel_id == "0601001391511"
    assert row.parcel_link_method == "normalized_address"
    assert str(row.parcel_link_confidence) == "0.9000"


def test_ingest_permits_does_not_link_ambiguous_normalized_address_match(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_address_ambiguous.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_address_ambiguous.csv"
    csv_path.write_text(
        "Address,Issued Date\n4519 and 4521 Field Ave #2,01/15/2025\n",
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        session.add_all(
            [
                Parcel(id="0601001391511"),
                Parcel(id="0601001391512"),
                Fetch(
                    id=1,
                    parcel_id="0601001391511",
                    url="https://example.test/parcel/0601001391511",
                ),
                Fetch(
                    id=2,
                    parcel_id="0601001391512",
                    url="https://example.test/parcel/0601001391512",
                ),
                ParcelSummary(
                    parcel_id="0601001391511",
                    fetch_id=1,
                    primary_address="4519 and 4521 Field Ave #2",
                ),
                ParcelSummary(
                    parcel_id="0601001391512",
                    fetch_id=2,
                    primary_address="4519 and 4521 Field Ave #2",
                ),
            ]
        )
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.parcel_id is None
    assert row.parcel_link_method is None
    assert row.parcel_link_confidence is None


def test_ingest_permits_prefers_exact_parcel_number_over_ambiguous_address_match(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "permit_import_exact_precedence.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "permits_exact_precedence.csv"
    csv_path.write_text(
        (
            "Parcel Number,Address,Issued Date\n"
            "06-10-0139-151-1,4519 and 4521 Field Ave #2,01/15/2025\n"
        ),
        encoding="utf-8",
    )

    init_db(database_url)
    with session_scope(database_url) as session:
        session.add_all(
            [
                Parcel(id="0601001391511"),
                Parcel(id="0601001391512"),
                Fetch(
                    id=1,
                    parcel_id="0601001391511",
                    url="https://example.test/parcel/0601001391511",
                ),
                Fetch(
                    id=2,
                    parcel_id="0601001391512",
                    url="https://example.test/parcel/0601001391512",
                ),
                ParcelCharacteristic(
                    parcel_id="0601001391511",
                    formatted_parcel_number="06-10-0139-151-1",
                ),
                ParcelSummary(
                    parcel_id="0601001391511",
                    fetch_id=1,
                    primary_address="4519 and 4521 Field Ave #2",
                ),
                ParcelSummary(
                    parcel_id="0601001391512",
                    fetch_id=2,
                    primary_address="4519 and 4521 Field Ave #2",
                ),
            ]
        )
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-permits", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout

    with session_scope(database_url) as session:
        row = session.execute(select(PermitEvent)).scalar_one()

    assert row.parcel_id == "0601001391511"
    assert row.parcel_link_method == "exact_parcel_number"
    assert str(row.parcel_link_confidence) == "1.0000"


def test_permit_template_headers_are_ingest_compatible(tmp_path: Path) -> None:
    db_path = tmp_path / "permit_template_compat.sqlite"
    database_url = f"sqlite:///{db_path}"
    template_path = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "templates"
        / "permit_events_template.csv"
    )

    assert template_path.exists()

    init_db(database_url)
    with session_scope(database_url) as session:
        summary = ingest_permits_csv(session, template_path)

    assert summary.total_rows == 0
    assert summary.loaded_rows == 0
    assert summary.rejected_rows == 0
    assert summary.inserted_rows == 0
    assert summary.updated_rows == 0
    assert summary.rejection_reason_counts == {}
    assert summary.warning_counts == {}

    with session_scope(database_url) as session:
        count = session.execute(
            select(func.count()).select_from(PermitEvent)
        ).scalar_one()

    assert count == 0
