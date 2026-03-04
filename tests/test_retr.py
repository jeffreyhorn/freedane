from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import SalesTransaction


def test_ingest_retr_imports_and_normalizes_loaded_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_import.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "retr.csv"
    csv_path.write_text(
        (
            "Transfer Date,Recording Date,Consideration,Parcel Number,"
            "Property Address,Grantor,Grantee,Arms Length Indicator,"
            "Usable Sale Indicator\n"
            '01/15/2025,2025-01-20,"$123,456.70",'
            '" 06-10-0139-151-1 "," 123 Main St., Apt #2 ",'
            "Seller One,Buyer One, yes ,0\n"
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
    result = runner.invoke(cli.app, ["ingest-retr", "--file", str(csv_path)])

    assert result.exit_code == 0, result.stdout
    assert (
        "RETR import summary: total=1 loaded=1 rejected=0 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(SalesTransaction)).scalar_one()

    assert row.source_system == "wisconsin_dor_retr"
    assert row.import_status == "loaded"
    assert row.import_error is None
    assert row.transfer_date.isoformat() == "2025-01-15"
    assert row.recording_date.isoformat() == "2025-01-20"
    assert str(row.consideration_amount) == "123456.70"
    assert row.official_parcel_number_raw == " 06-10-0139-151-1 "
    assert row.official_parcel_number_norm == "061001391511"
    assert row.property_address_raw == " 123 Main St., Apt #2 "
    assert row.property_address_norm == "123 MAIN ST APT 2"
    assert row.grantor_name == "Seller One"
    assert row.grantee_name == "Buyer One"
    assert row.arms_length_indicator_raw == " yes "
    assert row.arms_length_indicator_norm is True
    assert row.usable_sale_indicator_raw == "0"
    assert row.usable_sale_indicator_norm is False


def test_ingest_retr_records_rejected_rows_and_reuses_existing_same_file_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_rejected.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "retr_rejected.csv"
    csv_path.write_text(
        (
            "Transfer Date,Consideration,Property Address\n"
            "2025/01/15,100000,123 Main St\n"
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
    first_result = runner.invoke(cli.app, ["ingest-retr", "--file", str(csv_path)])

    assert first_result.exit_code == 0, first_result.stdout
    assert (
        "RETR import summary: total=1 loaded=0 rejected=1 inserted=1 updated=0"
        in first_result.stdout
    )

    with session_scope(database_url) as session:
        rows = session.execute(select(SalesTransaction)).scalars().all()

    assert len(rows) == 1
    assert rows[0].import_status == "rejected"
    assert rows[0].import_error == "transfer_date could not be parsed."
    first_row_id = rows[0].id

    second_result = runner.invoke(cli.app, ["ingest-retr", "--file", str(csv_path)])

    assert second_result.exit_code == 0, second_result.stdout
    assert (
        "RETR import summary: total=1 loaded=0 rejected=1 inserted=0 updated=1"
        in second_result.stdout
    )

    with session_scope(database_url) as session:
        rows = session.execute(select(SalesTransaction)).scalars().all()

    assert len(rows) == 1
    assert rows[0].id == first_row_id
    assert rows[0].import_status == "rejected"
    assert rows[0].import_error == "transfer_date could not be parsed."
