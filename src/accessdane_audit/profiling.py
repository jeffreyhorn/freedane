from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .models import (
    AssessmentRecord,
    Fetch,
    Parcel,
    ParcelSummary,
    ParcelYearFact,
    PaymentRecord,
    TaxRecord,
)


def build_data_profile(
    session: Session,
    *,
    parcel_ids: Optional[Iterable[str]] = None,
) -> dict[str, object]:
    parcel_filter = set(parcel_ids) if parcel_ids else None

    parcel_count = _count_rows(session, Parcel, parcel_filter)
    fetch_count = _count_rows(session, Fetch, parcel_filter)
    successful_fetch_count = _count_rows(
        session,
        Fetch,
        parcel_filter,
        extra_where=(Fetch.status_code == 200,),
    )
    parsed_fetch_count = _count_rows(
        session,
        Fetch,
        parcel_filter,
        extra_where=(Fetch.parsed_at.is_not(None),),
    )
    parse_error_count = _count_rows(
        session,
        Fetch,
        parcel_filter,
        extra_where=(Fetch.parse_error.is_not(None),),
    )

    assessment_count = _count_rows(session, AssessmentRecord, parcel_filter)
    tax_count = _count_rows(session, TaxRecord, parcel_filter)
    payment_count = _count_rows(session, PaymentRecord, parcel_filter)
    parcel_summary_count = _count_rows(session, ParcelSummary, parcel_filter)
    parcel_year_fact_count = _count_rows(session, ParcelYearFact, parcel_filter)
    parcel_year_fact_parcel_count = _count_distinct(session, ParcelYearFact, "parcel_id", parcel_filter)

    assessment_fetch_ids = _fetch_ids_with_rows(session, AssessmentRecord, parcel_filter)
    tax_fetch_ids = _fetch_ids_with_rows(session, TaxRecord, parcel_filter)
    payment_fetch_ids = _fetch_ids_with_rows(session, PaymentRecord, parcel_filter)

    successful_fetch_ids = _successful_fetch_ids(session, parcel_filter)
    missing_assessment_fetch_count = len(successful_fetch_ids - assessment_fetch_ids)
    missing_tax_fetch_count = len(successful_fetch_ids - tax_fetch_ids)
    missing_payment_fetch_count = len(successful_fetch_ids - payment_fetch_ids)

    source_parcel_year_count = len(_source_parcel_year_keys(session, parcel_filter))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "filtered": parcel_filter is not None,
            "parcel_filter_count": len(parcel_filter) if parcel_filter is not None else None,
        },
        "counts": {
            "parcels": parcel_count,
            "fetches": fetch_count,
            "successful_fetches": successful_fetch_count,
            "parsed_fetches": parsed_fetch_count,
            "parse_errors": parse_error_count,
            "assessments": assessment_count,
            "taxes": tax_count,
            "payments": payment_count,
            "parcel_summaries": parcel_summary_count,
            "parcel_year_facts": parcel_year_fact_count,
            "parcel_year_fact_parcels": parcel_year_fact_parcel_count,
            "source_parcel_years": source_parcel_year_count,
        },
        "missing_sections": {
            "assessment_fetches": missing_assessment_fetch_count,
            "tax_fetches": missing_tax_fetch_count,
            "payment_fetches": missing_payment_fetch_count,
            "current_parcel_summary_parcels": max(parcel_count - parcel_summary_count, 0),
        },
        "coverage": {
            "successful_fetch_rate": _ratio(successful_fetch_count, fetch_count),
            "parsed_successful_fetch_rate": _ratio(parsed_fetch_count, successful_fetch_count),
            "parse_error_successful_fetch_rate": _ratio(parse_error_count, successful_fetch_count),
            "parcel_summary_parcel_rate": _ratio(parcel_summary_count, parcel_count),
            "parcel_year_fact_parcel_rate": _ratio(parcel_year_fact_parcel_count, parcel_count),
            "parcel_year_fact_source_year_rate": _ratio(
                parcel_year_fact_count,
                source_parcel_year_count,
            ),
        },
    }


def _count_rows(session: Session, model, parcel_filter: Optional[set[str]], extra_where=()) -> int:
    query = select(func.count())
    query = query.select_from(model)
    query = _apply_parcel_filter(query, model, parcel_filter)
    for clause in extra_where:
        query = query.where(clause)
    return int(session.execute(query).scalar_one())


def _count_distinct(
    session: Session,
    model,
    field_name: str,
    parcel_filter: Optional[set[str]],
) -> int:
    field = getattr(model, field_name)
    query = select(func.count(func.distinct(field))).select_from(model)
    query = _apply_parcel_filter(query, model, parcel_filter)
    return int(session.execute(query).scalar_one())


def _successful_fetch_ids(session: Session, parcel_filter: Optional[set[str]]) -> set[int]:
    query = select(Fetch.id).where(Fetch.status_code == 200)
    query = _apply_parcel_filter(query, Fetch, parcel_filter)
    return set(session.execute(query).scalars())


def _fetch_ids_with_rows(session: Session, model, parcel_filter: Optional[set[str]]) -> set[int]:
    query = select(model.fetch_id).distinct()
    query = _apply_parcel_filter(query, model, parcel_filter)
    return set(session.execute(query).scalars())


def _source_parcel_year_keys(session: Session, parcel_filter: Optional[set[str]]) -> set[tuple[str, int]]:
    keys: set[tuple[str, int]] = set()
    for model in (AssessmentRecord, TaxRecord, PaymentRecord):
        query = select(model.parcel_id, model.year).where(model.year.is_not(None))
        query = _apply_parcel_filter(query, model, parcel_filter)
        keys.update(session.execute(query).all())
    return keys


def _apply_parcel_filter(query, model, parcel_filter: Optional[set[str]]):
    if parcel_filter:
        if hasattr(model, "parcel_id"):
            parcel_column = model.parcel_id
        else:
            parcel_column = model.id
        query = query.where(parcel_column.in_(parcel_filter))
    return query


def _ratio(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)
