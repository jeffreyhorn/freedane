from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import Fetch, Parcel, ParcelCharacteristic, PermitEvent
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
