from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from accessdane_audit.cli import _store_parsed
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import Fetch, Parcel, ParcelYearFact
from accessdane_audit.parcel_year_facts import rebuild_parcel_year_facts
from accessdane_audit.parse import parse_page


def _seed_and_build_fact_rows(
    database_url: str,
    parcel_id: str,
    html: str,
) -> int:
    parsed = parse_page(html)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(
            parcel_id=parcel_id,
            url=f"https://example.test/{parcel_id}",
            status_code=200,
        )
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed)

    with session_scope(database_url) as session:
        return rebuild_parcel_year_facts(session)


def test_rebuild_parcel_year_facts_prefers_detail_assessment_and_rolls_up_payments(
    load_raw_html,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_year_facts_full.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "061003330128"

    init_db(database_url)
    row_count = _seed_and_build_fact_rows(
        database_url, parcel_id, load_raw_html(parcel_id)
    )

    with session_scope(database_url) as session:
        row = session.execute(
            select(ParcelYearFact).where(
                ParcelYearFact.parcel_id == parcel_id,
                ParcelYearFact.year == 2023,
            )
        ).scalar_one()

    assert row_count == 27
    assert row.assessment_valuation_classification == "G1"
    assert row.assessment_average_assessment_ratio == Decimal("0.9580")
    assert row.assessment_estimated_fair_market_value == Decimal("245304.00")
    assert row.tax_amount == Decimal("3632.76")
    assert row.payment_event_count == 2
    assert row.payment_total_amount == Decimal("3632.76")
    assert row.payment_first_date == date(2024, 1, 31)
    assert row.payment_last_date == date(2024, 6, 24)
    assert row.payment_has_placeholder_row is False
    assert row.current_owner_name is None


def test_rebuild_parcel_year_facts_preserves_owner_names_and_placeholder_payments(
    load_raw_html,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_year_facts_owner_names.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "061002122981"

    init_db(database_url)
    _seed_and_build_fact_rows(database_url, parcel_id, load_raw_html(parcel_id))

    with session_scope(database_url) as session:
        row = session.execute(
            select(ParcelYearFact).where(
                ParcelYearFact.parcel_id == parcel_id,
                ParcelYearFact.year == 2025,
            )
        ).scalar_one()

    assert row.current_owner_name == "KEVIN SINGER\nERIN SINGER"
    assert row.current_primary_address == "6100 SHOOTING STAR TRL"
    assert row.assessment_valuation_classification == "G1"
    assert row.tax_amount == Decimal("10521.20")
    assert row.payment_event_count == 0
    assert row.payment_total_amount is None
    assert row.payment_first_date is None
    assert row.payment_last_date is None
    assert row.payment_has_placeholder_row is True


def test_rebuild_parcel_year_facts_can_limit_rebuild_to_selected_parcels(
    load_raw_html,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_year_facts_filtered.sqlite"
    database_url = f"sqlite:///{db_path}"
    first_parcel_id = "061003330128"
    second_parcel_id = "061001391511"

    init_db(database_url)
    _seed_and_build_fact_rows(
        database_url, first_parcel_id, load_raw_html(first_parcel_id)
    )

    with session_scope(database_url) as session:
        session.add(Parcel(id=second_parcel_id))
        fetch = Fetch(
            parcel_id=second_parcel_id,
            url=f"https://example.test/{second_parcel_id}",
            status_code=200,
        )
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parse_page(load_raw_html(second_parcel_id)))

    with session_scope(database_url) as session:
        row_count = rebuild_parcel_year_facts(session, parcel_ids=[second_parcel_id])

    with session_scope(database_url) as session:
        rows = session.execute(
            select(ParcelYearFact.parcel_id, ParcelYearFact.year).order_by(
                ParcelYearFact.parcel_id,
                ParcelYearFact.year.desc(),
            )
        ).all()

    assert row_count == 5
    assert sum(1 for parcel_id, _ in rows if parcel_id == first_parcel_id) == 27
    assert sum(1 for parcel_id, _ in rows if parcel_id == second_parcel_id) == 5
