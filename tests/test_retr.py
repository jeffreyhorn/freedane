from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit import retr as retr_module
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import SalesExclusion, SalesTransaction


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


def test_ingest_retr_populates_initial_sales_exclusions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_exclusions.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "retr_exclusions.csv"
    csv_path.write_text(
        (
            "Transfer Date,Consideration,Property Address,Conveyance Type,Deed "
            "Type,Grantor,Arms Length Indicator,Usable Sale Indicator\n"
            "01/15/2025,100000,123 Main St,Family Transfer,,Seller One,no,0\n"
            "01/16/2025,200000,456 Main St,,Corrective Deed,County of Dane,yes,1\n"
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
        "RETR import summary: total=2 loaded=2 rejected=0 inserted=2 updated=0"
        in first_result.stdout
    )

    with session_scope(database_url) as session:
        transactions = (
            session.execute(
                select(SalesTransaction).order_by(SalesTransaction.source_row_number)
            )
            .scalars()
            .all()
        )
        exclusions = (
            session.execute(
                select(SalesExclusion).order_by(SalesExclusion.exclusion_code)
            )
            .scalars()
            .all()
        )

    assert len(transactions) == 2
    assert [transaction.import_status for transaction in transactions] == [
        "loaded",
        "loaded",
    ]
    assert [exclusion.exclusion_code for exclusion in exclusions] == [
        "corrective_deed",
        "family_transfer",
        "government_transfer",
        "non_arms_length",
        "non_usable_sale",
    ]
    assert all(exclusion.is_active for exclusion in exclusions)
    assert {
        exclusion.exclusion_code: exclusion.excluded_by_rule for exclusion in exclusions
    } == {
        "corrective_deed": "v1_corrective_deed_keywords",
        "family_transfer": "v1_family_transfer_keywords",
        "government_transfer": "v1_government_transfer_keywords",
        "non_arms_length": "v1_non_arms_length_indicator",
        "non_usable_sale": "v1_usable_sale_indicator",
    }
    first_exclusion_ids = {
        exclusion.exclusion_code: exclusion.id for exclusion in exclusions
    }

    second_result = runner.invoke(cli.app, ["ingest-retr", "--file", str(csv_path)])

    assert second_result.exit_code == 0, second_result.stdout
    assert (
        "RETR import summary: total=2 loaded=2 rejected=0 inserted=0 updated=2"
        in second_result.stdout
    )

    with session_scope(database_url) as session:
        reloaded_exclusions = (
            session.execute(
                select(SalesExclusion).order_by(SalesExclusion.exclusion_code)
            )
            .scalars()
            .all()
        )

    assert len(reloaded_exclusions) == 5
    assert {
        exclusion.exclusion_code: exclusion.id for exclusion in reloaded_exclusions
    } == first_exclusion_ids
    assert all(exclusion.is_active for exclusion in reloaded_exclusions)


def test_ingest_retr_rejects_file_without_header_row(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_no_header.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("", encoding="utf-8")

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-retr", "--file", str(csv_path)])

    assert result.exit_code != 0
    assert "CSV file is missing a header row." in result.stderr


def test_ingest_retr_rejects_duplicate_or_blank_header_names(
    tmp_path: Path,
    monkeypatch,
) -> None:
    duplicate_db_path = tmp_path / "retr_duplicate_headers.sqlite"
    duplicate_database_url = f"sqlite:///{duplicate_db_path}"
    duplicate_csv_path = tmp_path / "duplicate_headers.csv"
    duplicate_csv_path.write_text(
        "Transfer Date,Transfer Date,Consideration\n01/15/2025,01/15/2025,100000\n",
        encoding="utf-8",
    )

    init_db(duplicate_database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=duplicate_database_url),
    )

    runner = CliRunner()
    duplicate_result = runner.invoke(
        cli.app,
        ["ingest-retr", "--file", str(duplicate_csv_path)],
    )

    assert duplicate_result.exit_code != 0
    assert "CSV file contains duplicate header names:" in duplicate_result.stderr
    assert "Transfer" in duplicate_result.stderr
    assert "Date" in duplicate_result.stderr

    blank_db_path = tmp_path / "retr_blank_header.sqlite"
    blank_database_url = f"sqlite:///{blank_db_path}"
    blank_csv_path = tmp_path / "blank_header.csv"
    blank_csv_path.write_text(
        "Transfer Date,,Consideration\n01/15/2025,foo,100000\n",
        encoding="utf-8",
    )

    init_db(blank_database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=blank_database_url),
    )

    blank_result = runner.invoke(
        cli.app,
        ["ingest-retr", "--file", str(blank_csv_path)],
    )

    assert blank_result.exit_code != 0
    assert "CSV file contains blank header names." in blank_result.stderr


def test_ingest_retr_rejects_files_without_recognizable_retr_headers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_unrecognized_headers.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "unrecognized_headers.csv"
    csv_path.write_text("Foo,Bar\nx,y\n", encoding="utf-8")

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-retr", "--file", str(csv_path)])

    assert result.exit_code != 0
    assert "CSV file does not contain recognizable RETR" in result.stderr
    assert "headers." in result.stderr


def test_ingest_retr_rejects_malformed_csv_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_malformed.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "malformed.csv"
    csv_path.write_text(
        "Transfer Date,Consideration,Property Address\n01/15/2025,100000,123 Main St\n",
        encoding="utf-8",
    )

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    class MalformedDictReader:
        fieldnames = ["Transfer Date", "Consideration", "Property Address"]

        def __iter__(self) -> "MalformedDictReader":
            return self

        def __next__(self) -> dict[str, str]:
            raise retr_module.csv.Error("unexpected end of data")

    monkeypatch.setattr(
        retr_module.csv,
        "DictReader",
        lambda *args, **kwargs: MalformedDictReader(),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ingest-retr", "--file", str(csv_path)])

    assert result.exit_code != 0
    assert "CSV file is malformed: unexpected end of data" in result.stderr


def test_ingest_retr_marks_rows_with_extra_columns_as_rejected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_extra_columns.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "extra_columns.csv"
    csv_path.write_text(
        (
            "Transfer Date,Consideration,Property Address\n"
            "01/15/2025,100000,123 Main St,EXTRA\n"
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
        "RETR import summary: total=1 loaded=0 rejected=1 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(SalesTransaction)).scalar_one()

    assert row.import_status == "rejected"
    assert row.import_error == "Row has extra columns beyond the header definition."


def test_ingest_retr_rejects_non_empty_unparseable_optional_recording_date(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_bad_recording_date.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "bad_recording_date.csv"
    csv_path.write_text(
        (
            "Transfer Date,Recording Date,Consideration,Property Address\n"
            "01/15/2025,2025/99/99,100000,123 Main St\n"
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
        "RETR import summary: total=1 loaded=0 rejected=1 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(SalesTransaction)).scalar_one()

    assert row.import_status == "rejected"
    assert row.import_error == "recording_date could not be parsed."
    assert row.recording_date is None


def test_ingest_retr_treats_whitespace_only_rows_as_blank(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_whitespace_blank.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "whitespace_blank.csv"
    csv_path.write_text(
        ("Transfer Date,Consideration,Property Address\n" "   ,   ,   \n"),
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
        "RETR import summary: total=1 loaded=0 rejected=1 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(SalesTransaction)).scalar_one()

    assert row.import_status == "rejected"
    assert row.import_error == "Row is blank."


def test_ingest_retr_rejects_rows_without_any_identifying_inputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "retr_missing_identifiers.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_path = tmp_path / "missing_identifiers.csv"
    csv_path.write_text(
        (
            "Transfer Date,Consideration,Parcel Number,Property Address,Legal "
            "Description\n"
            "01/15/2025,100000, , , \n"
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
        "RETR import summary: total=1 loaded=0 rejected=1 inserted=1 updated=0"
        in result.stdout
    )

    with session_scope(database_url) as session:
        row = session.execute(select(SalesTransaction)).scalar_one()

    assert row.import_status == "rejected"
    assert row.import_error == (
        "At least one identifying input is required: official parcel number, "
        "property address, or legal description."
    )
