from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select, text
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import Fetch, Parcel, ParcelYearFact


def test_run_all_can_optionally_build_parcel_year_facts(
    load_raw_html,
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "run_all.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "061001391511"
    raw_path = tmp_path / f"{parcel_id}.html"
    raw_path.write_text(load_raw_html(parcel_id), encoding="utf-8")

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        session.add(
            Fetch(
                parcel_id=parcel_id,
                url=f"https://example.test/{parcel_id}",
                status_code=200,
                raw_path=str(raw_path),
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
        ["run-all", "--parse-only", "--reparse", "--build-parcel-year-facts"],
    )

    assert result.exit_code == 0, result.stdout

    with session_scope(database_url) as session:
        rows = session.execute(
            select(ParcelYearFact)
            .where(ParcelYearFact.parcel_id == parcel_id)
            .order_by(ParcelYearFact.year.desc())
        ).scalars().all()

    assert [row.year for row in rows] == [2025, 2024, 2023, 2022, 2021]


def test_run_all_scopes_downstream_steps_when_parse_ids_are_provided(
    load_raw_html,
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "run_all_scoped.sqlite"
    database_url = f"sqlite:///{db_path}"
    scoped_parcel_id = "061001391511"
    other_parcel_id = "061002275801"
    scoped_raw_path = tmp_path / f"{scoped_parcel_id}.html"
    other_raw_path = tmp_path / f"{other_parcel_id}.html"
    scoped_raw_path.write_text(load_raw_html(scoped_parcel_id), encoding="utf-8")
    other_raw_path.write_text(load_raw_html(other_parcel_id), encoding="utf-8")
    parse_ids_path = tmp_path / "parse_ids.txt"
    parse_ids_path.write_text(f"{scoped_parcel_id}\n", encoding="utf-8")
    anomalies_path = tmp_path / "anomalies.json"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id=other_parcel_id), Parcel(id=scoped_parcel_id)])
        other_fetch = Fetch(
            parcel_id=other_parcel_id,
            url=f"https://example.test/{other_parcel_id}",
            status_code=200,
            raw_path=str(other_raw_path),
        )
        scoped_fetch = Fetch(
            parcel_id=scoped_parcel_id,
            url=f"https://example.test/{scoped_parcel_id}",
            status_code=200,
            raw_path=str(scoped_raw_path),
        )
        session.add_all([other_fetch, scoped_fetch])
        session.flush()

        # Seed parsed rows for the non-scoped parcel so a global downstream rebuild would touch it.
        parsed = cli.parse_page(load_raw_html(other_parcel_id))
        cli._store_parsed(session, other_fetch, parsed)
        other_fetch.parsed_at = datetime.now(timezone.utc)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "run-all",
            "--parse-only",
            "--reparse",
            "--parse-ids",
            str(parse_ids_path),
            "--build-parcel-year-facts",
            "--anomalies-out",
            str(anomalies_path),
        ],
    )

    assert result.exit_code == 0, result.stdout

    with session_scope(database_url) as session:
        scoped_rows = session.execute(
            select(ParcelYearFact)
            .where(ParcelYearFact.parcel_id == scoped_parcel_id)
            .order_by(ParcelYearFact.year.desc())
        ).scalars().all()
        other_rows = session.execute(
            select(ParcelYearFact)
            .where(ParcelYearFact.parcel_id == other_parcel_id)
            .order_by(ParcelYearFact.year.desc())
        ).scalars().all()

    assert [row.year for row in scoped_rows] == [2025, 2024, 2023, 2022, 2021]
    assert other_rows == []

    anomalies = json.loads(anomalies_path.read_text(encoding="utf-8"))
    assert anomalies == []


def test_run_all_can_skip_anomalies(
    load_raw_html,
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "run_all_skip_anomalies.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "061001391511"
    raw_path = tmp_path / f"{parcel_id}.html"
    raw_path.write_text(load_raw_html(parcel_id), encoding="utf-8")
    anomalies_path = tmp_path / "anomalies.json"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        session.add(
            Fetch(
                parcel_id=parcel_id,
                url=f"https://example.test/{parcel_id}",
                status_code=200,
                raw_path=str(raw_path),
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
        [
            "run-all",
            "--parse-only",
            "--reparse",
            "--skip-anomalies",
            "--anomalies-out",
            str(anomalies_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert not anomalies_path.exists()


def test_parse_reparse_commits_successful_fetches_even_if_a_later_fetch_hits_db_error(
    load_raw_html,
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "parse_isolated.sqlite"
    database_url = f"sqlite:///{db_path}"
    failing_parcel_id = "061002275801"
    succeeding_parcel_id = "061001391511"
    failing_raw_path = tmp_path / f"{failing_parcel_id}.html"
    succeeding_raw_path = tmp_path / f"{succeeding_parcel_id}.html"
    failing_raw_path.write_text(load_raw_html(failing_parcel_id), encoding="utf-8")
    succeeding_raw_path.write_text(load_raw_html(succeeding_parcel_id), encoding="utf-8")

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id=failing_parcel_id), Parcel(id=succeeding_parcel_id)])
        session.add_all(
            [
                Fetch(
                    parcel_id=failing_parcel_id,
                    url=f"https://example.test/{failing_parcel_id}",
                    status_code=200,
                    raw_path=str(failing_raw_path),
                ),
                Fetch(
                    parcel_id=succeeding_parcel_id,
                    url=f"https://example.test/{succeeding_parcel_id}",
                    status_code=200,
                    raw_path=str(succeeding_raw_path),
                ),
            ]
        )

    original_store_parsed = cli._store_parsed

    def failing_store_parsed(session, fetch, parsed, *, raw_html=None):
        if fetch.parcel_id == failing_parcel_id:
            session.execute(text("SELECT * FROM parcel_characteristics_missing"))
        return original_store_parsed(session, fetch, parsed, raw_html=raw_html)

    monkeypatch.setattr(cli, "_store_parsed", failing_store_parsed)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["parse", "--reparse"])

    assert result.exit_code == 0, result.stdout

    with session_scope(database_url) as session:
        fetches = {
            fetch.parcel_id: fetch
            for fetch in session.execute(select(Fetch).order_by(Fetch.id)).scalars()
        }

    assert "parcel_characteristics_missing" in (fetches[failing_parcel_id].parse_error or "")
    assert fetches[failing_parcel_id].parsed_at is None
    assert fetches[succeeding_parcel_id].parse_error is None
    assert fetches[succeeding_parcel_id].parsed_at is not None
