from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import Fetch, Parcel, ParcelCharacteristic


def _write_raw_html(tmp_path: Path, name: str, html: str) -> Path:
    path = tmp_path / f"{name}.html"
    path.write_text(html, encoding="utf-8")
    return path


def _minimal_characteristic_html(parcel_number: str = "0999-000-0000-0") -> str:
    return f"""
    <html>
      <body>
        <h1 id="parcel_detail_heading">Parcel Number - {parcel_number}</h1>
      </body>
    </html>
    """


def test_rebuild_parcel_characteristics_full_rebuild_removes_stale_rows(
    load_raw_html,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "rebuild_characteristics_full.sqlite"
    database_url = f"sqlite:///{db_path}"
    active_parcel_id = "061001391511"
    stale_parcel_id = "stale-parcel"
    raw_path = _write_raw_html(tmp_path, active_parcel_id, load_raw_html(active_parcel_id))

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id=active_parcel_id), Parcel(id=stale_parcel_id)])
        fetch = Fetch(
            parcel_id=active_parcel_id,
            url=f"https://example.test/{active_parcel_id}",
            status_code=200,
            raw_path=str(raw_path),
            fetched_at=datetime.now(timezone.utc),
            parsed_at=datetime.now(timezone.utc),
        )
        session.add(fetch)
        session.add(
            ParcelCharacteristic(
                parcel_id=stale_parcel_id,
                formatted_parcel_number="STALE",
                built_at=datetime.now(timezone.utc),
            )
        )

    summary = cli.rebuild_parcel_characteristics(database_url)

    with session_scope(database_url) as session:
        rows = session.execute(
            select(ParcelCharacteristic).order_by(ParcelCharacteristic.parcel_id)
        ).scalars().all()

    assert summary.selected_parcels == 2
    assert summary.eligible_fetches_scanned == 1
    assert summary.rows_deleted == 1
    assert summary.rows_written == 1
    assert summary.skipped_fetches == 0
    assert summary.parcel_failures == 0
    assert [row.parcel_id for row in rows] == [active_parcel_id]
    assert rows[0].source_fetch_id == fetch.id
    assert rows[0].formatted_parcel_number is not None


def test_rebuild_parcel_characteristics_scoped_rebuild_prefers_more_complete_fetch(
    load_raw_html,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "rebuild_characteristics_scoped.sqlite"
    database_url = f"sqlite:///{db_path}"
    selected_parcel_id = "selected-parcel"
    other_parcel_id = "other-parcel"
    rich_raw_path = _write_raw_html(tmp_path, "rich", load_raw_html("061003330128"))
    poor_raw_path = _write_raw_html(tmp_path, "poor", _minimal_characteristic_html())
    now = datetime.now(timezone.utc)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id=selected_parcel_id), Parcel(id=other_parcel_id)])
        rich_fetch = Fetch(
            parcel_id=selected_parcel_id,
            url=f"https://example.test/{selected_parcel_id}/rich",
            status_code=200,
            raw_path=str(rich_raw_path),
            fetched_at=now - timedelta(days=1),
            parsed_at=now - timedelta(days=1),
        )
        poor_fetch = Fetch(
            parcel_id=selected_parcel_id,
            url=f"https://example.test/{selected_parcel_id}/poor",
            status_code=200,
            raw_path=str(poor_raw_path),
            fetched_at=now,
            parsed_at=now,
        )
        session.add_all([rich_fetch, poor_fetch])
        session.add(
            ParcelCharacteristic(
                parcel_id=other_parcel_id,
                formatted_parcel_number="UNCHANGED",
                built_at=now,
            )
        )

    summary = cli.rebuild_parcel_characteristics(
        database_url,
        parcel_ids=[selected_parcel_id],
    )

    with session_scope(database_url) as session:
        selected_row = session.get(ParcelCharacteristic, selected_parcel_id)
        other_row = session.get(ParcelCharacteristic, other_parcel_id)

    assert summary.selected_parcels == 1
    assert summary.eligible_fetches_scanned == 2
    assert summary.rows_deleted == 0
    assert summary.rows_written == 1
    assert summary.parcel_failures == 0
    assert selected_row is not None
    assert selected_row.source_fetch_id == rich_fetch.id
    assert selected_row.current_assessment_year == 2023
    assert other_row is not None
    assert other_row.formatted_parcel_number == "UNCHANGED"


def test_rebuild_parcel_characteristics_cli_builds_rows(
    load_raw_html,
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "rebuild_characteristics_cli.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "061001391511"
    raw_path = _write_raw_html(tmp_path, parcel_id, load_raw_html(parcel_id))

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        session.add(
            Fetch(
                parcel_id=parcel_id,
                url=f"https://example.test/{parcel_id}",
                status_code=200,
                raw_path=str(raw_path),
                fetched_at=datetime.now(timezone.utc),
                parsed_at=datetime.now(timezone.utc),
            )
        )

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["rebuild-parcel-characteristics"])

    assert result.exit_code == 0, result.stdout
    assert "Selected parcels: 1" in result.stdout
    assert "Rows written: 1" in result.stdout

    with session_scope(database_url) as session:
        row = session.get(ParcelCharacteristic, parcel_id)

    assert row is not None
    assert row.current_assessment_year == 2025
