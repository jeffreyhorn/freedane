from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable, Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from .models import (
    AssessmentRecord,
    Fetch,
    ParcelSummary,
    ParcelYearFact,
    PaymentRecord,
    TaxRecord,
)


def rebuild_parcel_year_facts(
    session: Session,
    *,
    parcel_ids: Optional[Iterable[str]] = None,
) -> int:
    parcel_filter = set(parcel_ids) if parcel_ids else None

    fetches = _load_records(session, Fetch, parcel_filter=parcel_filter)
    fetch_by_id = {fetch.id: fetch for fetch in fetches}

    summaries = _load_records(session, ParcelSummary, parcel_filter=parcel_filter)
    assessments = _load_records(session, AssessmentRecord, parcel_filter=parcel_filter)
    taxes = _load_records(session, TaxRecord, parcel_filter=parcel_filter)
    payments = _load_records(session, PaymentRecord, parcel_filter=parcel_filter)

    session.execute(_delete_facts_query(parcel_filter))

    latest_summary_by_parcel = _build_latest_summary_map(summaries, fetch_by_id)
    assessment_by_key = _group_by_parcel_year(assessments)
    tax_by_key = _group_by_parcel_year(taxes)
    payment_by_key = _group_by_parcel_year(payments)
    years_by_parcel = _build_year_universe(assessments, taxes, payments)

    rows: list[ParcelYearFact] = []
    built_at = datetime.now(timezone.utc)

    for parcel_id in sorted(years_by_parcel):
        summary = latest_summary_by_parcel.get(parcel_id)
        for year in sorted(years_by_parcel[parcel_id], reverse=True):
            chosen_assessment = _choose_assessment(
                assessment_by_key.get((parcel_id, year), []),
                fetch_by_id,
            )
            chosen_tax = _choose_tax(
                tax_by_key.get((parcel_id, year), []),
                fetch_by_id,
            )
            chosen_payment = _choose_payment(
                payment_by_key.get((parcel_id, year), []),
                fetch_by_id,
            )
            rows.append(
                _build_fact_row(
                    parcel_id=parcel_id,
                    year=year,
                    built_at=built_at,
                    summary=summary,
                    assessment=chosen_assessment,
                    tax=chosen_tax,
                    payment=chosen_payment,
                )
            )

    session.add_all(rows)
    return len(rows)


def _load_records(session: Session, model, *, parcel_filter: Optional[set[str]]) -> list:
    query = select(model)
    if parcel_filter:
        query = query.where(model.parcel_id.in_(parcel_filter))
    return session.execute(query).scalars().all()


def _delete_facts_query(parcel_filter: Optional[set[str]]):
    query = delete(ParcelYearFact)
    if parcel_filter:
        query = query.where(ParcelYearFact.parcel_id.in_(parcel_filter))
    return query


def _group_by_parcel_year(records: Iterable) -> dict[tuple[str, int], list]:
    grouped: dict[tuple[str, int], list] = defaultdict(list)
    for record in records:
        if record.year is None:
            continue
        grouped[(record.parcel_id, record.year)].append(record)
    return grouped


def _build_year_universe(
    assessments: Iterable[AssessmentRecord],
    taxes: Iterable[TaxRecord],
    payments: Iterable[PaymentRecord],
) -> dict[str, set[int]]:
    years_by_parcel: dict[str, set[int]] = defaultdict(set)
    for collection in (assessments, taxes, payments):
        for record in collection:
            if record.year is None:
                continue
            years_by_parcel[record.parcel_id].add(record.year)
    return years_by_parcel


def _build_latest_summary_map(
    summaries: Iterable[ParcelSummary],
    fetch_by_id: dict[int, Fetch],
) -> dict[str, ParcelSummary]:
    latest: dict[str, ParcelSummary] = {}
    for summary in summaries:
        current = latest.get(summary.parcel_id)
        if current is None or _fetch_rank(summary.fetch_id, fetch_by_id) > _fetch_rank(
            current.fetch_id, fetch_by_id
        ):
            latest[summary.parcel_id] = summary
    return latest


def _choose_assessment(
    records: list[AssessmentRecord],
    fetch_by_id: dict[int, Fetch],
) -> Optional[AssessmentRecord]:
    candidates = [
        record
        for record in records
        if str(record.data.get("source") or "") != "valuation_breakout"
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda record: (
            _assessment_source_priority(record),
            _assessment_completeness(record),
            _fetch_rank(record.fetch_id, fetch_by_id),
            record.id,
        ),
    )


def _assessment_source_priority(record: AssessmentRecord) -> int:
    source = str(record.data.get("source") or "")
    if source == "detail":
        return 3
    if source == "summary":
        return 2
    return 1


def _assessment_completeness(record: AssessmentRecord) -> int:
    values = [
        record.valuation_classification,
        record.assessment_acres,
        record.land_value,
        record.improved_value,
        record.total_value,
        record.average_assessment_ratio,
        record.estimated_fair_market_value,
        record.valuation_date,
    ]
    return sum(value is not None for value in values)


def _choose_tax(records: list[TaxRecord], fetch_by_id: dict[int, Fetch]) -> Optional[TaxRecord]:
    candidates = [
        record for record in records if str(record.data.get("source") or "") == "summary"
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda record: (
            _tax_completeness(record),
            _fetch_rank(record.fetch_id, fetch_by_id),
            record.id,
        ),
    )


def _tax_completeness(record: TaxRecord) -> int:
    keys = (
        "Total Assessed Value",
        "Assessed Land Value",
        "Assessed Improvement Value",
        "Taxes",
        "Specials(+)",
        "First Dollar Credit(-)",
        "Lottery Credit(-)",
        "Amount",
    )
    return sum(_parse_money(record.data.get(key)) is not None for key in keys)


def _choose_payment(
    records: list[PaymentRecord],
    fetch_by_id: dict[int, Fetch],
) -> Optional[dict[str, object]]:
    summary_records = [
        record
        for record in records
        if str(record.data.get("source") or "") != "tax_detail_payments"
    ]
    if not summary_records:
        return None

    rows_by_fetch: dict[int, list[PaymentRecord]] = defaultdict(list)
    for record in summary_records:
        rows_by_fetch[record.fetch_id].append(record)

    candidates = []
    for fetch_id, fetch_rows in rows_by_fetch.items():
        usable_rows = []
        for row in fetch_rows:
            payment_date = _parse_date(row.data.get("Date of Payment"))
            amount = _parse_money(row.data.get("Amount"))
            if payment_date is not None and amount is not None:
                usable_rows.append((payment_date, amount))
        has_placeholder = any(
            str(row.data.get("Date of Payment") or "").strip() == "No payments found."
            for row in fetch_rows
        )
        if not usable_rows and not has_placeholder:
            continue
        candidates.append(
            {
                "fetch_id": fetch_id,
                "usable_rows": usable_rows,
                "has_placeholder": has_placeholder,
            }
        )

    if not candidates:
        return None

    chosen = max(
        candidates,
        key=lambda item: (
            len(item["usable_rows"]),
            _fetch_rank(item["fetch_id"], fetch_by_id),
            item["fetch_id"],
        ),
    )

    usable_rows = chosen["usable_rows"]
    if usable_rows:
        dates = [payment_date for payment_date, _ in usable_rows]
        amounts = [amount for _, amount in usable_rows]
        return {
            "payment_fetch_id": chosen["fetch_id"],
            "payment_event_count": len(usable_rows),
            "payment_total_amount": sum(amounts, Decimal("0.00")),
            "payment_first_date": min(dates),
            "payment_last_date": max(dates),
            "payment_has_placeholder_row": bool(chosen["has_placeholder"]),
        }

    return {
        "payment_fetch_id": chosen["fetch_id"],
        "payment_event_count": 0,
        "payment_total_amount": None,
        "payment_first_date": None,
        "payment_last_date": None,
        "payment_has_placeholder_row": True if chosen["has_placeholder"] else False,
    }


def _build_fact_row(
    *,
    parcel_id: str,
    year: int,
    built_at: datetime,
    summary: Optional[ParcelSummary],
    assessment: Optional[AssessmentRecord],
    tax: Optional[TaxRecord],
    payment: Optional[dict[str, object]],
) -> ParcelYearFact:
    payment = payment or {}
    return ParcelYearFact(
        parcel_id=parcel_id,
        year=year,
        parcel_summary_fetch_id=summary.fetch_id if summary else None,
        assessment_fetch_id=assessment.fetch_id if assessment else None,
        tax_fetch_id=tax.fetch_id if tax else None,
        payment_fetch_id=payment.get("payment_fetch_id"),
        municipality_name=summary.municipality_name if summary else None,
        current_parcel_description=summary.parcel_description if summary else None,
        current_owner_name=summary.owner_name if summary else None,
        current_primary_address=summary.primary_address if summary else None,
        current_billing_address=summary.billing_address if summary else None,
        assessment_valuation_classification=(
            assessment.valuation_classification if assessment else None
        ),
        assessment_acres=assessment.assessment_acres if assessment else None,
        assessment_land_value=assessment.land_value if assessment else None,
        assessment_improved_value=assessment.improved_value if assessment else None,
        assessment_total_value=assessment.total_value if assessment else None,
        assessment_average_assessment_ratio=(
            assessment.average_assessment_ratio if assessment else None
        ),
        assessment_estimated_fair_market_value=(
            assessment.estimated_fair_market_value if assessment else None
        ),
        assessment_valuation_date=assessment.valuation_date if assessment else None,
        tax_total_assessed_value=_parse_money(
            tax.data.get("Total Assessed Value") if tax else None
        ),
        tax_assessed_land_value=_parse_money(
            tax.data.get("Assessed Land Value") if tax else None
        ),
        tax_assessed_improvement_value=_parse_money(
            tax.data.get("Assessed Improvement Value") if tax else None
        ),
        tax_taxes=_parse_money(tax.data.get("Taxes") if tax else None),
        tax_specials=_parse_money(tax.data.get("Specials(+)") if tax else None),
        tax_first_dollar_credit=_parse_money(
            tax.data.get("First Dollar Credit(-)") if tax else None
        ),
        tax_lottery_credit=_parse_money(
            tax.data.get("Lottery Credit(-)") if tax else None
        ),
        tax_amount=_parse_money(tax.data.get("Amount") if tax else None),
        payment_event_count=payment.get("payment_event_count"),
        payment_total_amount=payment.get("payment_total_amount"),
        payment_first_date=payment.get("payment_first_date"),
        payment_last_date=payment.get("payment_last_date"),
        payment_has_placeholder_row=payment.get("payment_has_placeholder_row"),
        built_at=built_at,
    )


def _fetch_rank(fetch_id: Optional[int], fetch_by_id: dict[int, Fetch]) -> tuple[datetime, int]:
    if fetch_id is None:
        return (datetime.min, -1)
    fetch = fetch_by_id.get(fetch_id)
    if fetch is None or fetch.fetched_at is None:
        return (datetime.min, fetch_id)
    fetched_at = fetch.fetched_at
    if fetched_at.tzinfo is not None:
        fetched_at = fetched_at.astimezone(timezone.utc).replace(tzinfo=None)
    return (fetched_at, fetch_id)


def _parse_money(value: object) -> Optional[Decimal]:
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"))
    if isinstance(value, (int, float)):
        return Decimal(str(value)).quantize(Decimal("0.01"))
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text.upper() in {"N/A", "NA"}:
        return None
    cleaned = text.replace(",", "").replace("$", "")
    if cleaned in {"", "-", "--"}:
        return None
    try:
        return Decimal(cleaned).quantize(Decimal("0.01"))
    except Exception:
        return None


def _parse_date(value: object) -> Optional[date]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text.upper() in {"N/A", "NA"}:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None
