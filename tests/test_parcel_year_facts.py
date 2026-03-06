from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from accessdane_audit.cli import _store_parsed
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    AppealEvent,
    Fetch,
    Parcel,
    ParcelCharacteristic,
    ParcelYearFact,
    PaymentRecord,
    PermitEvent,
)
from accessdane_audit.parcel_year_facts import (
    PaymentRollupCandidate,
    rebuild_parcel_year_facts,
)
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


def _make_permit_event(
    *,
    source_row_number: int,
    parcel_id: str | None,
    parcel_number_norm: str | None,
    permit_year: int | None,
    permit_status_norm: str | None = None,
    declared_valuation: Decimal | None = None,
    estimated_cost: Decimal | None = None,
) -> PermitEvent:
    return PermitEvent(
        source_system="test",
        source_file_name="permits.csv",
        source_file_sha256=f"permit-sha-{source_row_number}",
        source_row_number=source_row_number,
        source_headers=[],
        raw_row={},
        import_status="loaded",
        import_error=None,
        import_warnings=None,
        parcel_number_raw=None,
        parcel_number_norm=parcel_number_norm,
        site_address_raw=None,
        site_address_norm=None,
        parcel_id=parcel_id,
        parcel_link_method=None,
        parcel_link_confidence=None,
        permit_number=None,
        issuing_jurisdiction=None,
        permit_type=None,
        permit_subtype=None,
        work_class=None,
        permit_status_raw=None,
        permit_status_norm=permit_status_norm,
        description=None,
        owner_name=None,
        contractor_name=None,
        applied_date=None,
        issued_date=None,
        finaled_date=None,
        status_date=None,
        declared_valuation=declared_valuation,
        estimated_cost=estimated_cost,
        permit_year=permit_year,
    )


def _make_appeal_event(
    *,
    source_row_number: int,
    parcel_id: str | None,
    parcel_number_norm: str | None,
    tax_year: int | None,
    outcome_norm: str | None = None,
    value_change_amount: Decimal | None = None,
) -> AppealEvent:
    return AppealEvent(
        source_system="test",
        source_file_name="appeals.csv",
        source_file_sha256=f"appeal-sha-{source_row_number}",
        source_row_number=source_row_number,
        source_headers=[],
        raw_row={},
        import_status="loaded",
        import_error=None,
        import_warnings=None,
        parcel_number_raw=None,
        parcel_number_norm=parcel_number_norm,
        site_address_raw=None,
        site_address_norm=None,
        owner_name_raw=None,
        owner_name_norm=None,
        parcel_id=parcel_id,
        parcel_link_method=None,
        parcel_link_confidence=None,
        appeal_number=None,
        docket_number=None,
        appeal_level_raw=None,
        appeal_level_norm=None,
        appeal_reason_raw=None,
        appeal_reason_norm=None,
        filing_date=None,
        hearing_date=None,
        decision_date=None,
        tax_year=tax_year,
        outcome_raw=None,
        outcome_norm=outcome_norm,
        assessed_value_before=None,
        requested_assessed_value=None,
        decided_assessed_value=None,
        value_change_amount=value_change_amount,
        representative_name=None,
        notes=None,
    )


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


def test_payment_rollup_candidate_stores_usable_rows_as_immutable_tuple() -> None:
    usable_rows = [(date(2024, 1, 31), Decimal("100.00"))]

    candidate = PaymentRollupCandidate(
        fetch_id=1,
        usable_rows=usable_rows,
        has_placeholder=False,
    )
    usable_rows.append((date(2024, 6, 24), Decimal("200.00")))

    assert candidate.usable_rows == ((date(2024, 1, 31), Decimal("100.00")),)
    assert isinstance(candidate.usable_rows, tuple)


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


def test_rebuild_parcel_year_facts_treats_col_2_placeholder_rows_as_placeholders(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_year_facts_placeholder_col2.sqlite"
    database_url = f"sqlite:///{db_path}"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id="p1"))
        fetch = Fetch(
            parcel_id="p1",
            url="https://example.test/p1",
            status_code=200,
        )
        session.add(fetch)
        session.flush()
        session.add(
            PaymentRecord(
                parcel_id="p1",
                fetch_id=fetch.id,
                year=2025,
                data={
                    "col_1": "2025",
                    "col_2": "No payments found.",
                    "Amount": "",
                },
            )
        )

    with session_scope(database_url) as session:
        row_count = rebuild_parcel_year_facts(session)

    with session_scope(database_url) as session:
        row = session.execute(
            select(ParcelYearFact).where(
                ParcelYearFact.parcel_id == "p1",
                ParcelYearFact.year == 2025,
            )
        ).scalar_one()

    assert row_count == 1
    assert row.payment_event_count == 0
    assert row.payment_total_amount is None
    assert row.payment_first_date is None
    assert row.payment_last_date is None
    assert row.payment_has_placeholder_row is True


def test_rebuild_parcel_year_facts_includes_permit_and_appeal_year_only_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_year_facts_event_only.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "P-001"
    parcel_number_norm = "061001391511"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        session.add(
            ParcelCharacteristic(
                parcel_id=parcel_id,
                source_fetch_id=None,
                formatted_parcel_number="06-10-0139-151-1",
            )
        )
        session.add(
            _make_permit_event(
                source_row_number=1,
                parcel_id=None,
                parcel_number_norm=parcel_number_norm,
                permit_year=2024,
                permit_status_norm="issued",
                declared_valuation=Decimal("1200.00"),
                estimated_cost=Decimal("2000.00"),
            )
        )
        session.add(
            _make_appeal_event(
                source_row_number=1,
                parcel_id=None,
                parcel_number_norm=parcel_number_norm,
                tax_year=2024,
                outcome_norm="reduction_granted",
                value_change_amount=Decimal("-300.00"),
            )
        )

    with session_scope(database_url) as session:
        row_count = rebuild_parcel_year_facts(session)

    with session_scope(database_url) as session:
        row = session.execute(
            select(ParcelYearFact).where(
                ParcelYearFact.parcel_id == parcel_id,
                ParcelYearFact.year == 2024,
            )
        ).scalar_one()

    assert row_count == 1
    assert row.assessment_fetch_id is None
    assert row.tax_fetch_id is None
    assert row.payment_fetch_id is None
    assert row.permit_event_count == 1
    assert row.permit_status_issued_count == 1
    assert row.appeal_event_count == 1
    assert row.appeal_reduction_granted_count == 1
    assert row.appeal_value_change_total == Decimal("-300.00")


def test_rebuild_parcel_year_facts_rolls_recent_permits_and_appeal_outcome_metrics(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_year_facts_permit_appeal_rollups.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "P-ROLLUP-1"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        session.add(
            _make_permit_event(
                source_row_number=1,
                parcel_id=parcel_id,
                parcel_number_norm=None,
                permit_year=2023,
                permit_status_norm="issued",
                declared_valuation=Decimal("10.00"),
                estimated_cost=Decimal("20.00"),
            )
        )
        session.add(
            _make_appeal_event(
                source_row_number=1,
                parcel_id=parcel_id,
                parcel_number_norm=None,
                tax_year=2024,
                outcome_norm="reduction_granted",
                value_change_amount=Decimal("-100.00"),
            )
        )
        session.add(
            _make_appeal_event(
                source_row_number=2,
                parcel_id=parcel_id,
                parcel_number_norm=None,
                tax_year=2024,
                outcome_norm="partial_reduction",
                value_change_amount=Decimal("50.00"),
            )
        )
        session.add(
            _make_appeal_event(
                source_row_number=3,
                parcel_id=parcel_id,
                parcel_number_norm=None,
                tax_year=2024,
                outcome_norm=None,
                value_change_amount=None,
            )
        )
        session.add(
            _make_appeal_event(
                source_row_number=4,
                parcel_id=parcel_id,
                parcel_number_norm=None,
                tax_year=2025,
                outcome_norm="denied",
                value_change_amount=Decimal("20.00"),
            )
        )

    with session_scope(database_url) as session:
        row_count = rebuild_parcel_year_facts(session)

    with session_scope(database_url) as session:
        year_2024 = session.execute(
            select(ParcelYearFact).where(
                ParcelYearFact.parcel_id == parcel_id,
                ParcelYearFact.year == 2024,
            )
        ).scalar_one()
        year_2025 = session.execute(
            select(ParcelYearFact).where(
                ParcelYearFact.parcel_id == parcel_id,
                ParcelYearFact.year == 2025,
            )
        ).scalar_one()

    assert row_count == 3
    assert year_2024.permit_event_count is None
    assert year_2024.permit_recent_1y_count == 1
    assert year_2024.permit_has_recent_1y is True
    assert year_2024.permit_recent_2y_count == 1
    assert year_2024.permit_has_recent_2y is True

    assert year_2024.appeal_event_count == 3
    assert year_2024.appeal_reduction_granted_count == 1
    assert year_2024.appeal_partial_reduction_count == 1
    assert year_2024.appeal_denied_count == 0
    assert year_2024.appeal_withdrawn_count == 0
    assert year_2024.appeal_dismissed_count == 0
    assert year_2024.appeal_pending_count == 0
    assert year_2024.appeal_unknown_outcome_count == 1
    assert year_2024.appeal_value_change_known_count == 2
    assert year_2024.appeal_value_change_total == Decimal("-50.00")
    assert year_2024.appeal_value_change_reduction_total == Decimal("-100.00")
    assert year_2024.appeal_value_change_increase_total == Decimal("50.00")

    assert year_2025.appeal_event_count == 1
    assert year_2025.appeal_value_change_known_count == 1
    assert year_2025.appeal_value_change_total == Decimal("20.00")
    assert year_2025.appeal_value_change_reduction_total == Decimal("0.00")
    assert year_2025.appeal_value_change_increase_total == Decimal("20.00")


def test_rebuild_parcel_year_facts_scoped_rebuild_resolves_null_parcel_id_events_first(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parcel_year_facts_scoped_effective_parcel.sqlite"
    database_url = f"sqlite:///{db_path}"
    selected_parcel_id = "P-SELECTED"
    other_parcel_id = "P-OTHER"
    selected_parcel_number_norm = "061001391511"

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id=selected_parcel_id), Parcel(id=other_parcel_id)])
        session.add(
            ParcelCharacteristic(
                parcel_id=selected_parcel_id,
                source_fetch_id=None,
                formatted_parcel_number="06-10-0139-151-1",
            )
        )
        session.add(
            _make_appeal_event(
                source_row_number=1,
                parcel_id=None,
                parcel_number_norm=selected_parcel_number_norm,
                tax_year=2024,
                outcome_norm="denied",
                value_change_amount=Decimal("0.00"),
            )
        )
        session.add(
            _make_appeal_event(
                source_row_number=2,
                parcel_id=other_parcel_id,
                parcel_number_norm=None,
                tax_year=2024,
                outcome_norm="denied",
                value_change_amount=Decimal("0.00"),
            )
        )

    with session_scope(database_url) as session:
        initial_count = rebuild_parcel_year_facts(session)

    with session_scope(database_url) as session:
        scoped_count = rebuild_parcel_year_facts(
            session, parcel_ids=[selected_parcel_id]
        )

    with session_scope(database_url) as session:
        rows = session.execute(
            select(ParcelYearFact.parcel_id, ParcelYearFact.year).order_by(
                ParcelYearFact.parcel_id,
                ParcelYearFact.year,
            )
        ).all()
        selected_row = session.execute(
            select(ParcelYearFact).where(
                ParcelYearFact.parcel_id == selected_parcel_id,
                ParcelYearFact.year == 2024,
            )
        ).scalar_one()

    assert initial_count == 2
    assert scoped_count == 1
    assert rows == [(other_parcel_id, 2024), (selected_parcel_id, 2024)]
    assert selected_row.appeal_event_count == 1
