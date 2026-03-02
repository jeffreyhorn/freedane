from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.cli import _store_parsed
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import Fetch, Parcel
from accessdane_audit.parcel_year_facts import rebuild_parcel_year_facts
from accessdane_audit.parse import parse_page
from accessdane_audit.profiling import build_data_profile


def test_build_data_profile_reports_counts_and_coverage_for_seeded_parcel(
    load_raw_html,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "profile.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "061003330128"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parse_page(load_raw_html(parcel_id)))
        fetch.parsed_at = fetch.fetched_at

    with session_scope(database_url) as session:
        rebuild_parcel_year_facts(session)

    with session_scope(database_url) as session:
        payload = build_data_profile(session)

    assert payload["counts"] == {
        "parcels": 1,
        "fetches": 1,
        "successful_fetches": 1,
        "parsed_fetches": 1,
        "parse_errors": 0,
        "assessments": 27,
        "taxes": 50,
        "payments": 277,
        "parcel_summaries": 1,
        "parcel_year_facts": 27,
        "parcel_year_fact_parcels": 1,
        "source_parcel_years": 27,
    }
    assert payload["missing_sections"] == {
        "assessment_fetches": 0,
        "tax_fetches": 0,
        "payment_fetches": 0,
        "current_parcel_summary_parcels": 0,
    }
    assert payload["coverage"]["successful_fetch_rate"] == 1.0
    assert payload["coverage"]["parsed_successful_fetch_rate"] == 1.0
    assert payload["coverage"]["parse_error_successful_fetch_rate"] == 0.0
    assert payload["coverage"]["parcel_summary_parcel_rate"] == 1.0
    assert payload["coverage"]["parcel_year_fact_parcel_rate"] == 1.0
    assert payload["coverage"]["parcel_year_fact_source_year_rate"] == 1.0


def test_build_data_profile_reports_parse_errors_and_missing_sections(tmp_path: Path) -> None:
    db_path = tmp_path / "profile_missing.sqlite"
    database_url = f"sqlite:///{db_path}"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id="p1"), Parcel(id="p2")])
        session.add_all(
            [
                Fetch(parcel_id="p1", url="https://example.test/p1", status_code=200),
                Fetch(
                    parcel_id="p2",
                    url="https://example.test/p2",
                    status_code=200,
                    parse_error="boom",
                ),
            ]
        )

    with session_scope(database_url) as session:
        payload = build_data_profile(session)

    assert payload["counts"]["successful_fetches"] == 2
    assert payload["counts"]["parsed_fetches"] == 0
    assert payload["counts"]["parse_errors"] == 1
    assert payload["missing_sections"]["assessment_fetches"] == 2
    assert payload["missing_sections"]["tax_fetches"] == 2
    assert payload["missing_sections"]["payment_fetches"] == 2
    assert payload["coverage"]["parsed_successful_fetch_rate"] == 0.0
    assert payload["coverage"]["parse_error_successful_fetch_rate"] == 0.5


def test_profile_data_cli_emits_json(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "profile_cli.sqlite"
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
    result = runner.invoke(cli.app, ["profile-data"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["counts"]["parcels"] == 1
    assert payload["counts"]["fetches"] == 1
    assert payload["missing_sections"]["assessment_fetches"] == 1


def test_build_data_profile_can_filter_scope_and_has_stable_output_shape(tmp_path: Path) -> None:
    db_path = tmp_path / "profile_filtered.sqlite"
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
        payload = build_data_profile(session, parcel_ids=["p1"])

    assert set(payload) == {"generated_at", "scope", "counts", "missing_sections", "coverage"}
    assert payload["scope"] == {
        "filtered": True,
        "parcel_filter_count": 1,
    }
    assert set(payload["counts"]) == {
        "parcels",
        "fetches",
        "successful_fetches",
        "parsed_fetches",
        "parse_errors",
        "assessments",
        "taxes",
        "payments",
        "parcel_summaries",
        "parcel_year_facts",
        "parcel_year_fact_parcels",
        "source_parcel_years",
    }
    assert set(payload["missing_sections"]) == {
        "assessment_fetches",
        "tax_fetches",
        "payment_fetches",
        "current_parcel_summary_parcels",
    }
    assert set(payload["coverage"]) == {
        "successful_fetch_rate",
        "parsed_successful_fetch_rate",
        "parse_error_successful_fetch_rate",
        "parcel_summary_parcel_rate",
        "parcel_year_fact_parcel_rate",
        "parcel_year_fact_source_year_rate",
    }
    assert payload["counts"]["parcels"] == 1
    assert payload["counts"]["fetches"] == 1
    assert payload["coverage"]["successful_fetch_rate"] == 1.0
