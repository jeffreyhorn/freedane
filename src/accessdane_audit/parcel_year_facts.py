from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable, Optional

from sqlalchemy import delete, or_, select
from sqlalchemy.orm import Session

from .extraction_signals import is_placeholder_payment_row
from .models import (
    AppealEvent,
    AssessmentRecord,
    Fetch,
    Parcel,
    ParcelCharacteristic,
    ParcelSummary,
    ParcelYearFact,
    PaymentRecord,
    PermitEvent,
    TaxRecord,
)

_PARCEL_NUMBER_SEPARATORS = (" ", ".", "/", "_", "-")
_PERMIT_STATUS_BUCKETS = (
    "applied",
    "issued",
    "finaled",
    "cancelled",
    "expired",
    "unknown",
)
_EXPLICIT_APPEAL_OUTCOME_BUCKETS = (
    "reduction_granted",
    "partial_reduction",
    "denied",
    "withdrawn",
    "dismissed",
    "pending",
)


@dataclass(frozen=True)
class PaymentRollupCandidate:
    fetch_id: int
    usable_rows: tuple[tuple[date, Decimal], ...]
    has_placeholder: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "usable_rows", tuple(self.usable_rows))


@dataclass(frozen=True)
class PaymentRollup:
    payment_fetch_id: int
    payment_event_count: int
    payment_total_amount: Optional[Decimal]
    payment_first_date: Optional[date]
    payment_last_date: Optional[date]
    payment_has_placeholder_row: bool


@dataclass(frozen=True)
class ResolvedPermitEvent:
    parcel_id: str
    year: int
    permit_status_norm: Optional[str]
    declared_valuation: Optional[Decimal]
    estimated_cost: Optional[Decimal]


@dataclass(frozen=True)
class ResolvedAppealEvent:
    parcel_id: str
    year: int
    outcome_norm: Optional[str]
    value_change_amount: Optional[Decimal]


@dataclass(frozen=True)
class PermitRollup:
    permit_event_count: Optional[int]
    permit_declared_valuation_known_count: Optional[int]
    permit_declared_valuation_sum: Optional[Decimal]
    permit_estimated_cost_known_count: Optional[int]
    permit_estimated_cost_sum: Optional[Decimal]
    permit_status_applied_count: Optional[int]
    permit_status_issued_count: Optional[int]
    permit_status_finaled_count: Optional[int]
    permit_status_cancelled_count: Optional[int]
    permit_status_expired_count: Optional[int]
    permit_status_unknown_count: Optional[int]
    permit_recent_1y_count: Optional[int]
    permit_recent_2y_count: Optional[int]
    permit_has_recent_1y: Optional[bool]
    permit_has_recent_2y: Optional[bool]


@dataclass(frozen=True)
class AppealRollup:
    appeal_event_count: Optional[int]
    appeal_reduction_granted_count: Optional[int]
    appeal_partial_reduction_count: Optional[int]
    appeal_denied_count: Optional[int]
    appeal_withdrawn_count: Optional[int]
    appeal_dismissed_count: Optional[int]
    appeal_pending_count: Optional[int]
    appeal_unknown_outcome_count: Optional[int]
    appeal_value_change_known_count: Optional[int]
    appeal_value_change_total: Optional[Decimal]
    appeal_value_change_reduction_total: Optional[Decimal]
    appeal_value_change_increase_total: Optional[Decimal]


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

    permit_events = _load_permit_events(session, parcel_filter=parcel_filter)
    appeal_events = _load_appeal_events(session, parcel_filter=parcel_filter)
    parcel_id_index, characteristic_index = _build_parcel_number_indexes(session)
    resolved_permit_events = _resolve_permit_events(
        permit_events,
        parcel_filter=parcel_filter,
        parcel_id_index=parcel_id_index,
        characteristic_index=characteristic_index,
    )
    resolved_appeal_events = _resolve_appeal_events(
        appeal_events,
        parcel_filter=parcel_filter,
        parcel_id_index=parcel_id_index,
        characteristic_index=characteristic_index,
    )

    session.execute(_delete_facts_query(parcel_filter))

    latest_summary_by_parcel = _build_latest_summary_map(summaries, fetch_by_id)
    assessment_by_key = _group_by_parcel_year(assessments)
    tax_by_key = _group_by_parcel_year(taxes)
    payment_by_key = _group_by_parcel_year(payments)
    permit_by_key = _group_by_parcel_year(resolved_permit_events)
    appeal_by_key = _group_by_parcel_year(resolved_appeal_events)
    permit_by_parcel = _group_by_parcel(resolved_permit_events)
    years_by_parcel = _build_year_universe(
        assessments,
        taxes,
        payments,
        resolved_permit_events,
        resolved_appeal_events,
    )

    rows: list[ParcelYearFact] = []
    built_at = datetime.now(timezone.utc)

    for parcel_id in sorted(years_by_parcel):
        summary = latest_summary_by_parcel.get(parcel_id)
        parcel_permits = permit_by_parcel.get(parcel_id, [])
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
            permit_rollup = _rollup_permit_metrics(
                annual_events=permit_by_key.get((parcel_id, year), []),
                parcel_events=parcel_permits,
                year=year,
            )
            appeal_rollup = _rollup_appeal_metrics(
                appeal_by_key.get((parcel_id, year), [])
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
                    permit_rollup=permit_rollup,
                    appeal_rollup=appeal_rollup,
                )
            )

    session.add_all(rows)
    return len(rows)


def _load_records(
    session: Session, model, *, parcel_filter: Optional[set[str]]
) -> list:
    query = select(model)
    if parcel_filter:
        query = query.where(model.parcel_id.in_(parcel_filter))
    return list(session.execute(query).scalars().all())


def _load_permit_events(
    session: Session,
    *,
    parcel_filter: Optional[set[str]],
) -> list[PermitEvent]:
    query = select(PermitEvent).where(PermitEvent.import_status == "loaded")
    if parcel_filter:
        query = query.where(
            or_(
                PermitEvent.parcel_id.in_(parcel_filter),
                PermitEvent.parcel_id.is_(None),
            )
        )
    return list(session.execute(query).scalars().all())


def _load_appeal_events(
    session: Session,
    *,
    parcel_filter: Optional[set[str]],
) -> list[AppealEvent]:
    query = select(AppealEvent).where(AppealEvent.import_status == "loaded")
    if parcel_filter:
        query = query.where(
            or_(
                AppealEvent.parcel_id.in_(parcel_filter),
                AppealEvent.parcel_id.is_(None),
            )
        )
    return list(session.execute(query).scalars().all())


def _build_parcel_number_indexes(
    session: Session,
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    parcel_id_index: dict[str, set[str]] = defaultdict(set)
    characteristic_index: dict[str, set[str]] = defaultdict(set)

    for parcel_id in session.execute(select(Parcel.id)).scalars().all():
        normalized_parcel_id = _normalize_parcel_number_key(parcel_id)
        if normalized_parcel_id is None:
            continue
        parcel_id_index[normalized_parcel_id].add(parcel_id)

    for parcel_id, formatted_parcel_number in session.execute(
        select(
            ParcelCharacteristic.parcel_id,
            ParcelCharacteristic.formatted_parcel_number,
        )
    ).all():
        normalized_formatted = _normalize_parcel_number_key(formatted_parcel_number)
        if normalized_formatted is None:
            continue
        characteristic_index[normalized_formatted].add(parcel_id)

    return parcel_id_index, characteristic_index


def _resolve_permit_events(
    permit_events: Iterable[PermitEvent],
    *,
    parcel_filter: Optional[set[str]],
    parcel_id_index: dict[str, set[str]],
    characteristic_index: dict[str, set[str]],
) -> list[ResolvedPermitEvent]:
    resolved: list[ResolvedPermitEvent] = []
    for event in permit_events:
        if event.permit_year is None:
            continue
        effective_parcel_id = _resolve_effective_parcel_id(
            parcel_id=event.parcel_id,
            parcel_number_norm=event.parcel_number_norm,
            parcel_id_index=parcel_id_index,
            characteristic_index=characteristic_index,
        )
        if effective_parcel_id is None:
            continue
        if parcel_filter and effective_parcel_id not in parcel_filter:
            continue
        resolved.append(
            ResolvedPermitEvent(
                parcel_id=effective_parcel_id,
                year=event.permit_year,
                permit_status_norm=event.permit_status_norm,
                declared_valuation=event.declared_valuation,
                estimated_cost=event.estimated_cost,
            )
        )
    return resolved


def _resolve_appeal_events(
    appeal_events: Iterable[AppealEvent],
    *,
    parcel_filter: Optional[set[str]],
    parcel_id_index: dict[str, set[str]],
    characteristic_index: dict[str, set[str]],
) -> list[ResolvedAppealEvent]:
    resolved: list[ResolvedAppealEvent] = []
    for event in appeal_events:
        if event.tax_year is None:
            continue
        effective_parcel_id = _resolve_effective_parcel_id(
            parcel_id=event.parcel_id,
            parcel_number_norm=event.parcel_number_norm,
            parcel_id_index=parcel_id_index,
            characteristic_index=characteristic_index,
        )
        if effective_parcel_id is None:
            continue
        if parcel_filter and effective_parcel_id not in parcel_filter:
            continue
        resolved.append(
            ResolvedAppealEvent(
                parcel_id=effective_parcel_id,
                year=event.tax_year,
                outcome_norm=event.outcome_norm,
                value_change_amount=event.value_change_amount,
            )
        )
    return resolved


def _resolve_effective_parcel_id(
    *,
    parcel_id: Optional[str],
    parcel_number_norm: Optional[str],
    parcel_id_index: dict[str, set[str]],
    characteristic_index: dict[str, set[str]],
) -> Optional[str]:
    if parcel_id is not None:
        return parcel_id

    normalized_key = _normalize_parcel_number_key(parcel_number_norm)
    if normalized_key is None:
        return None

    direct_matches = parcel_id_index.get(normalized_key, set())
    if len(direct_matches) == 1:
        return next(iter(direct_matches))
    if direct_matches:
        return None

    formatted_matches = characteristic_index.get(normalized_key, set())
    if len(formatted_matches) == 1:
        return next(iter(formatted_matches))

    return None


def _normalize_parcel_number_key(value: Optional[str]) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if not normalized:
        return None
    for separator in _PARCEL_NUMBER_SEPARATORS:
        normalized = normalized.replace(separator, "")
    return normalized or None


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


def _group_by_parcel(records: Iterable) -> dict[str, list]:
    grouped: dict[str, list] = defaultdict(list)
    for record in records:
        grouped[record.parcel_id].append(record)
    return grouped


def _build_year_universe(
    assessments: Iterable[AssessmentRecord],
    taxes: Iterable[TaxRecord],
    payments: Iterable[PaymentRecord],
    permit_events: Iterable[ResolvedPermitEvent],
    appeal_events: Iterable[ResolvedAppealEvent],
) -> dict[str, set[int]]:
    years_by_parcel: dict[str, set[int]] = defaultdict(set)
    for collection in (assessments, taxes, payments, permit_events, appeal_events):
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


def _choose_tax(
    records: list[TaxRecord], fetch_by_id: dict[int, Fetch]
) -> Optional[TaxRecord]:
    candidates = [
        record
        for record in records
        if str(record.data.get("source") or "") == "summary"
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
) -> Optional[PaymentRollup]:
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

    candidates: list[PaymentRollupCandidate] = []
    for fetch_id, fetch_rows in rows_by_fetch.items():
        usable_payment_rows: list[tuple[date, Decimal]] = []
        for row in fetch_rows:
            payment_date = _parse_date(row.data.get("Date of Payment"))
            amount = _parse_money(row.data.get("Amount"))
            if payment_date is not None and amount is not None:
                usable_payment_rows.append((payment_date, amount))
        has_placeholder = any(
            is_placeholder_payment_row(row.data) for row in fetch_rows
        )
        if not usable_payment_rows and not has_placeholder:
            continue
        candidates.append(
            PaymentRollupCandidate(
                fetch_id=fetch_id,
                usable_rows=tuple(usable_payment_rows),
                has_placeholder=has_placeholder,
            )
        )

    if not candidates:
        return None

    chosen = max(
        candidates,
        key=lambda item: (
            len(item.usable_rows),
            _fetch_rank(item.fetch_id, fetch_by_id),
            item.fetch_id,
        ),
    )

    selected_rows = chosen.usable_rows
    if selected_rows:
        dates = [payment_date for payment_date, _ in selected_rows]
        amounts = [amount for _, amount in selected_rows]
        return PaymentRollup(
            payment_fetch_id=chosen.fetch_id,
            payment_event_count=len(selected_rows),
            payment_total_amount=sum(amounts, Decimal("0.00")),
            payment_first_date=min(dates),
            payment_last_date=max(dates),
            payment_has_placeholder_row=chosen.has_placeholder,
        )

    return PaymentRollup(
        payment_fetch_id=chosen.fetch_id,
        payment_event_count=0,
        payment_total_amount=None,
        payment_first_date=None,
        payment_last_date=None,
        payment_has_placeholder_row=chosen.has_placeholder,
    )


def _rollup_permit_metrics(
    *,
    annual_events: list[ResolvedPermitEvent],
    parcel_events: list[ResolvedPermitEvent],
    year: int,
) -> PermitRollup:
    permit_event_count: Optional[int]
    permit_declared_valuation_known_count: Optional[int]
    permit_declared_valuation_sum: Optional[Decimal]
    permit_estimated_cost_known_count: Optional[int]
    permit_estimated_cost_sum: Optional[Decimal]
    permit_status_counts: dict[str, Optional[int]] = {
        status: None for status in _PERMIT_STATUS_BUCKETS
    }

    if annual_events:
        permit_event_count = len(annual_events)

        declared_values = [
            event.declared_valuation
            for event in annual_events
            if event.declared_valuation is not None
        ]
        permit_declared_valuation_known_count = len(declared_values)
        permit_declared_valuation_sum = (
            sum(declared_values, Decimal("0.00")) if declared_values else None
        )

        estimated_cost_values = [
            event.estimated_cost
            for event in annual_events
            if event.estimated_cost is not None
        ]
        permit_estimated_cost_known_count = len(estimated_cost_values)
        permit_estimated_cost_sum = (
            sum(estimated_cost_values, Decimal("0.00"))
            if estimated_cost_values
            else None
        )

        status_eligible_events = [
            event for event in annual_events if event.permit_status_norm is not None
        ]
        if status_eligible_events:
            status_counts: dict[str, Optional[int]] = {
                status: 0 for status in _PERMIT_STATUS_BUCKETS
            }
            for event in status_eligible_events:
                status_value = event.permit_status_norm
                if status_value in status_counts:
                    current_count = status_counts[status_value]
                    status_counts[status_value] = (
                        1 if current_count is None else current_count + 1
                    )
            permit_status_counts = status_counts
    else:
        permit_event_count = None
        permit_declared_valuation_known_count = None
        permit_declared_valuation_sum = None
        permit_estimated_cost_known_count = None
        permit_estimated_cost_sum = None

    recent_1y_events = [
        event for event in parcel_events if event.year in {year, year - 1}
    ]
    permit_recent_1y_count = len(recent_1y_events) if recent_1y_events else None
    permit_has_recent_1y = bool(recent_1y_events) if recent_1y_events else None

    recent_2y_events = [
        event for event in parcel_events if event.year in {year, year - 1, year - 2}
    ]
    permit_recent_2y_count = len(recent_2y_events) if recent_2y_events else None
    permit_has_recent_2y = bool(recent_2y_events) if recent_2y_events else None

    return PermitRollup(
        permit_event_count=permit_event_count,
        permit_declared_valuation_known_count=permit_declared_valuation_known_count,
        permit_declared_valuation_sum=permit_declared_valuation_sum,
        permit_estimated_cost_known_count=permit_estimated_cost_known_count,
        permit_estimated_cost_sum=permit_estimated_cost_sum,
        permit_status_applied_count=permit_status_counts["applied"],
        permit_status_issued_count=permit_status_counts["issued"],
        permit_status_finaled_count=permit_status_counts["finaled"],
        permit_status_cancelled_count=permit_status_counts["cancelled"],
        permit_status_expired_count=permit_status_counts["expired"],
        permit_status_unknown_count=permit_status_counts["unknown"],
        permit_recent_1y_count=permit_recent_1y_count,
        permit_recent_2y_count=permit_recent_2y_count,
        permit_has_recent_1y=permit_has_recent_1y,
        permit_has_recent_2y=permit_has_recent_2y,
    )


def _rollup_appeal_metrics(events: list[ResolvedAppealEvent]) -> AppealRollup:
    if not events:
        return AppealRollup(
            appeal_event_count=None,
            appeal_reduction_granted_count=None,
            appeal_partial_reduction_count=None,
            appeal_denied_count=None,
            appeal_withdrawn_count=None,
            appeal_dismissed_count=None,
            appeal_pending_count=None,
            appeal_unknown_outcome_count=None,
            appeal_value_change_known_count=None,
            appeal_value_change_total=None,
            appeal_value_change_reduction_total=None,
            appeal_value_change_increase_total=None,
        )

    appeal_event_count = len(events)
    explicit_outcome_eligible = [
        event
        for event in events
        if event.outcome_norm in _EXPLICIT_APPEAL_OUTCOME_BUCKETS
    ]

    if explicit_outcome_eligible:
        explicit_counts: dict[str, Optional[int]] = {
            outcome: 0 for outcome in _EXPLICIT_APPEAL_OUTCOME_BUCKETS
        }
        for event in explicit_outcome_eligible:
            outcome_value = event.outcome_norm
            if outcome_value in explicit_counts:
                current_count = explicit_counts[outcome_value]
                explicit_counts[outcome_value] = (
                    1 if current_count is None else current_count + 1
                )
    else:
        explicit_counts = {
            outcome: None for outcome in _EXPLICIT_APPEAL_OUTCOME_BUCKETS
        }

    appeal_unknown_outcome_count = sum(
        1
        for event in events
        if event.outcome_norm not in _EXPLICIT_APPEAL_OUTCOME_BUCKETS
    )

    known_value_changes = [
        event.value_change_amount
        for event in events
        if event.value_change_amount is not None
    ]
    appeal_value_change_known_count = len(known_value_changes)

    if appeal_value_change_known_count == 0:
        appeal_value_change_total = None
        appeal_value_change_reduction_total = None
        appeal_value_change_increase_total = None
    else:
        appeal_value_change_total = sum(known_value_changes, Decimal("0.00"))
        reduction_values = [value for value in known_value_changes if value < 0]
        increase_values = [value for value in known_value_changes if value > 0]
        appeal_value_change_reduction_total = sum(reduction_values, Decimal("0.00"))
        appeal_value_change_increase_total = sum(increase_values, Decimal("0.00"))

    return AppealRollup(
        appeal_event_count=appeal_event_count,
        appeal_reduction_granted_count=explicit_counts["reduction_granted"],
        appeal_partial_reduction_count=explicit_counts["partial_reduction"],
        appeal_denied_count=explicit_counts["denied"],
        appeal_withdrawn_count=explicit_counts["withdrawn"],
        appeal_dismissed_count=explicit_counts["dismissed"],
        appeal_pending_count=explicit_counts["pending"],
        appeal_unknown_outcome_count=appeal_unknown_outcome_count,
        appeal_value_change_known_count=appeal_value_change_known_count,
        appeal_value_change_total=appeal_value_change_total,
        appeal_value_change_reduction_total=appeal_value_change_reduction_total,
        appeal_value_change_increase_total=appeal_value_change_increase_total,
    )


def _build_fact_row(
    *,
    parcel_id: str,
    year: int,
    built_at: datetime,
    summary: Optional[ParcelSummary],
    assessment: Optional[AssessmentRecord],
    tax: Optional[TaxRecord],
    payment: Optional[PaymentRollup],
    permit_rollup: PermitRollup,
    appeal_rollup: AppealRollup,
) -> ParcelYearFact:
    return ParcelYearFact(
        parcel_id=parcel_id,
        year=year,
        parcel_summary_fetch_id=summary.fetch_id if summary else None,
        assessment_fetch_id=assessment.fetch_id if assessment else None,
        tax_fetch_id=tax.fetch_id if tax else None,
        payment_fetch_id=payment.payment_fetch_id if payment else None,
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
        payment_event_count=payment.payment_event_count if payment else None,
        payment_total_amount=payment.payment_total_amount if payment else None,
        payment_first_date=payment.payment_first_date if payment else None,
        payment_last_date=payment.payment_last_date if payment else None,
        payment_has_placeholder_row=(
            payment.payment_has_placeholder_row if payment else None
        ),
        permit_event_count=permit_rollup.permit_event_count,
        permit_declared_valuation_known_count=(
            permit_rollup.permit_declared_valuation_known_count
        ),
        permit_declared_valuation_sum=permit_rollup.permit_declared_valuation_sum,
        permit_estimated_cost_known_count=permit_rollup.permit_estimated_cost_known_count,
        permit_estimated_cost_sum=permit_rollup.permit_estimated_cost_sum,
        permit_status_applied_count=permit_rollup.permit_status_applied_count,
        permit_status_issued_count=permit_rollup.permit_status_issued_count,
        permit_status_finaled_count=permit_rollup.permit_status_finaled_count,
        permit_status_cancelled_count=permit_rollup.permit_status_cancelled_count,
        permit_status_expired_count=permit_rollup.permit_status_expired_count,
        permit_status_unknown_count=permit_rollup.permit_status_unknown_count,
        permit_recent_1y_count=permit_rollup.permit_recent_1y_count,
        permit_recent_2y_count=permit_rollup.permit_recent_2y_count,
        permit_has_recent_1y=permit_rollup.permit_has_recent_1y,
        permit_has_recent_2y=permit_rollup.permit_has_recent_2y,
        appeal_event_count=appeal_rollup.appeal_event_count,
        appeal_reduction_granted_count=appeal_rollup.appeal_reduction_granted_count,
        appeal_partial_reduction_count=appeal_rollup.appeal_partial_reduction_count,
        appeal_denied_count=appeal_rollup.appeal_denied_count,
        appeal_withdrawn_count=appeal_rollup.appeal_withdrawn_count,
        appeal_dismissed_count=appeal_rollup.appeal_dismissed_count,
        appeal_pending_count=appeal_rollup.appeal_pending_count,
        appeal_unknown_outcome_count=appeal_rollup.appeal_unknown_outcome_count,
        appeal_value_change_known_count=appeal_rollup.appeal_value_change_known_count,
        appeal_value_change_total=appeal_rollup.appeal_value_change_total,
        appeal_value_change_reduction_total=(
            appeal_rollup.appeal_value_change_reduction_total
        ),
        appeal_value_change_increase_total=appeal_rollup.appeal_value_change_increase_total,
        built_at=built_at,
    )


def _fetch_rank(
    fetch_id: Optional[int], fetch_by_id: dict[int, Fetch]
) -> tuple[datetime, int]:
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
