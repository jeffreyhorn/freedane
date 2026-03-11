from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable, Optional, TypedDict

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import (
    AppealEvent,
    AssessmentRecord,
    FraudFlag,
    FraudScore,
    Parcel,
    ParcelFeature,
    ParcelYearFact,
    PermitEvent,
    SalesExclusion,
    SalesParcelMatch,
    SalesTransaction,
)


class ParcelHeader(TypedDict):
    parcel_id: str
    trs_code: Optional[str]
    municipality_name: Optional[str]
    current_owner_name: Optional[str]
    current_primary_address: Optional[str]
    current_parcel_description: Optional[str]


class AssessmentHistoryRow(TypedDict):
    assessment_id: Optional[int]
    year: Optional[int]
    valuation_classification: Optional[str]
    total_value: Optional[Decimal]
    land_value: Optional[Decimal]
    improved_value: Optional[Decimal]
    estimated_fair_market_value: Optional[Decimal]
    average_assessment_ratio: Optional[Decimal]
    valuation_date: Optional[date]
    source_fetch_id: Optional[int]


class MatchedSaleRow(TypedDict):
    sales_transaction_id: int
    transfer_date: Optional[date]
    recording_date: Optional[date]
    consideration_amount: Optional[Decimal]
    arms_length_indicator: Optional[bool]
    usable_sale_indicator: Optional[bool]
    document_number: Optional[str]
    match_method: str
    match_review_status: str
    match_confidence_score: Optional[Decimal]
    is_primary_match: bool
    active_exclusion_codes: list[str]


class PeerContextRow(TypedDict):
    run_id: int
    year: int
    feature_version: str
    municipality_name: Optional[str]
    valuation_classification: Optional[str]
    assessment_to_sale_ratio: Optional[Decimal]
    peer_percentile: Optional[Decimal]
    yoy_assessment_change_pct: Optional[Decimal]
    permit_adjusted_gap: Optional[Decimal]
    appeal_value_delta_3y: Optional[Decimal]
    lineage_value_reset_delta: Optional[Decimal]
    feature_quality_flags: list[str]


class PermitEventRow(TypedDict):
    permit_event_id: int
    permit_year: Optional[int]
    issued_date: Optional[date]
    applied_date: Optional[date]
    finaled_date: Optional[date]
    status_date: Optional[date]
    permit_number: Optional[str]
    permit_type: Optional[str]
    permit_subtype: Optional[str]
    work_class: Optional[str]
    permit_status: Optional[str]
    declared_valuation: Optional[Decimal]
    estimated_cost: Optional[Decimal]
    description: Optional[str]
    parcel_link_method: Optional[str]
    parcel_link_confidence: Optional[Decimal]


class AppealEventRow(TypedDict):
    appeal_event_id: int
    tax_year: Optional[int]
    filing_date: Optional[date]
    hearing_date: Optional[date]
    decision_date: Optional[date]
    appeal_number: Optional[str]
    docket_number: Optional[str]
    appeal_level: Optional[str]
    outcome: Optional[str]
    assessed_value_before: Optional[Decimal]
    requested_assessed_value: Optional[Decimal]
    decided_assessed_value: Optional[Decimal]
    value_change_amount: Optional[Decimal]
    representative_name: Optional[str]
    parcel_link_method: Optional[str]
    parcel_link_confidence: Optional[Decimal]


class ReasonCodeRow(TypedDict):
    reason_code: str
    reason_rank: int
    severity_weight: Optional[Decimal]
    metric_name: str
    metric_value: str
    threshold_value: str
    comparison_operator: str
    explanation: str
    source_refs: dict[str, object]


class ReasonCodeEvidenceRow(TypedDict):
    score_id: int
    run_id: int
    feature_run_id: Optional[int]
    year: int
    score_value: Decimal
    risk_band: str
    requires_review: bool
    reason_code_count: int
    scored_at: Optional[datetime]
    reason_codes: list[ReasonCodeRow]


class TimelineSource(TypedDict):
    table: str
    row_id: Optional[int]
    source_refs: Optional[dict[str, object]]


class TimelineEvent(TypedDict):
    event_date: Optional[date]
    event_type: str
    year: Optional[int]
    title: str
    details: dict[str, object]
    source: TimelineSource


def get_parcel_header(
    session: Session,
    *,
    parcel_id: str,
    years: Optional[Iterable[int]] = None,
) -> Optional[ParcelHeader]:
    parcel = session.get(Parcel, parcel_id)
    if parcel is None:
        return None

    years_list = _normalize_years(years)
    query = select(ParcelYearFact).where(ParcelYearFact.parcel_id == parcel_id)
    if years_list:
        query = query.where(ParcelYearFact.year.in_(years_list))
    facts = session.execute(query).scalars().all()
    selected_fact = max(facts, key=lambda row: row.year) if facts else None

    return {
        "parcel_id": parcel.id,
        "trs_code": parcel.trs_code,
        "municipality_name": (
            selected_fact.municipality_name if selected_fact is not None else None
        ),
        "current_owner_name": (
            selected_fact.current_owner_name if selected_fact is not None else None
        ),
        "current_primary_address": (
            selected_fact.current_primary_address if selected_fact is not None else None
        ),
        "current_parcel_description": (
            selected_fact.current_parcel_description
            if selected_fact is not None
            else None
        ),
    }


def list_assessment_history(
    session: Session,
    *,
    parcel_id: str,
    years: Optional[Iterable[int]] = None,
) -> list[AssessmentHistoryRow]:
    years_list = _normalize_years(years)
    assessments_query = select(AssessmentRecord).where(
        AssessmentRecord.parcel_id == parcel_id
    )
    if years_list:
        assessments_query = assessments_query.where(
            AssessmentRecord.year.in_(years_list)
        )
    assessment_rows = session.execute(assessments_query).scalars().all()

    result: list[AssessmentHistoryRow] = []
    years_with_assessment = {
        row.year for row in assessment_rows if row.year is not None
    }
    for row in assessment_rows:
        result.append(
            {
                "assessment_id": row.id,
                "year": row.year,
                "valuation_classification": row.valuation_classification,
                "total_value": row.total_value,
                "land_value": row.land_value,
                "improved_value": row.improved_value,
                "estimated_fair_market_value": row.estimated_fair_market_value,
                "average_assessment_ratio": row.average_assessment_ratio,
                "valuation_date": row.valuation_date,
                "source_fetch_id": row.fetch_id,
            }
        )

    facts_query = select(ParcelYearFact).where(ParcelYearFact.parcel_id == parcel_id)
    if years_list:
        facts_query = facts_query.where(ParcelYearFact.year.in_(years_list))
    fact_rows = session.execute(facts_query).scalars().all()
    for fact in fact_rows:
        if fact.year in years_with_assessment:
            continue
        result.append(
            {
                "assessment_id": None,
                "year": fact.year,
                "valuation_classification": fact.assessment_valuation_classification,
                "total_value": fact.assessment_total_value,
                "land_value": fact.assessment_land_value,
                "improved_value": fact.assessment_improved_value,
                "estimated_fair_market_value": (
                    fact.assessment_estimated_fair_market_value
                ),
                "average_assessment_ratio": fact.assessment_average_assessment_ratio,
                "valuation_date": fact.assessment_valuation_date,
                "source_fetch_id": fact.assessment_fetch_id,
            }
        )

    result.sort(
        key=lambda row: (
            row["year"] is None,
            -(row["year"] if row["year"] is not None else -1),
            row["assessment_id"] is None,
            -(row["assessment_id"] if row["assessment_id"] is not None else -1),
        )
    )
    return result


def list_matched_sales(
    session: Session,
    *,
    parcel_id: str,
    years: Optional[Iterable[int]] = None,
) -> list[MatchedSaleRow]:
    years_list = _normalize_years(years)
    query = (
        select(SalesParcelMatch, SalesTransaction)
        .join(
            SalesTransaction,
            SalesTransaction.id == SalesParcelMatch.sales_transaction_id,
        )
        .where(SalesParcelMatch.parcel_id == parcel_id)
    )
    match_rows = session.execute(query).all()

    by_transaction: dict[int, tuple[SalesParcelMatch, SalesTransaction]] = {}
    for match_row, transaction in match_rows:
        transaction_year = _coalesced_sale_year(transaction)
        if years_list and transaction_year not in years_list:
            continue
        chosen = by_transaction.get(transaction.id)
        if chosen is None or _match_priority_key(match_row) < _match_priority_key(
            chosen[0]
        ):
            by_transaction[transaction.id] = (match_row, transaction)

    transaction_ids = list(by_transaction)
    exclusion_codes_by_transaction = _active_exclusion_codes_by_transaction(
        session, transaction_ids
    )

    rows: list[MatchedSaleRow] = []
    for transaction_id, (match_row, transaction) in by_transaction.items():
        rows.append(
            {
                "sales_transaction_id": transaction_id,
                "transfer_date": transaction.transfer_date,
                "recording_date": transaction.recording_date,
                "consideration_amount": transaction.consideration_amount,
                "arms_length_indicator": transaction.arms_length_indicator_norm,
                "usable_sale_indicator": transaction.usable_sale_indicator_norm,
                "document_number": transaction.document_number,
                "match_method": match_row.match_method,
                "match_review_status": match_row.match_review_status,
                "match_confidence_score": match_row.confidence_score,
                "is_primary_match": match_row.is_primary,
                "active_exclusion_codes": exclusion_codes_by_transaction.get(
                    transaction_id, []
                ),
            }
        )

    rows.sort(
        key=lambda row: (
            _coalesced_sale_date_from_row(row) is None,
            -_coalesced_sale_ordinal_from_row(row),
            -row["sales_transaction_id"],
        )
    )
    return rows


def list_peer_context(
    session: Session,
    *,
    parcel_id: str,
    feature_version: str,
    years: Optional[Iterable[int]] = None,
) -> list[PeerContextRow]:
    years_list = _normalize_years(years)
    query = (
        select(ParcelFeature, ParcelYearFact)
        .outerjoin(
            ParcelYearFact,
            (ParcelYearFact.parcel_id == ParcelFeature.parcel_id)
            & (ParcelYearFact.year == ParcelFeature.year),
        )
        .where(
            ParcelFeature.parcel_id == parcel_id,
            ParcelFeature.feature_version == feature_version,
        )
    )
    if years_list:
        query = query.where(ParcelFeature.year.in_(years_list))
    rows = session.execute(query).all()

    result: list[PeerContextRow] = []
    for feature, fact in rows:
        result.append(
            {
                "run_id": feature.run_id,
                "year": feature.year,
                "feature_version": feature.feature_version,
                "municipality_name": (
                    fact.municipality_name if fact is not None else None
                ),
                "valuation_classification": (
                    fact.assessment_valuation_classification
                    if fact is not None
                    else None
                ),
                "assessment_to_sale_ratio": feature.assessment_to_sale_ratio,
                "peer_percentile": feature.peer_percentile,
                "yoy_assessment_change_pct": feature.yoy_assessment_change_pct,
                "permit_adjusted_gap": feature.permit_adjusted_gap,
                "appeal_value_delta_3y": feature.appeal_value_delta_3y,
                "lineage_value_reset_delta": feature.lineage_value_reset_delta,
                "feature_quality_flags": list(feature.feature_quality_flags or []),
            }
        )

    result.sort(
        key=lambda row: (
            -row["year"],
            -(row["run_id"] if row["run_id"] is not None else -1),
        )
    )
    return result


def list_permit_events(
    session: Session,
    *,
    parcel_id: str,
    years: Optional[Iterable[int]] = None,
) -> list[PermitEventRow]:
    years_list = _normalize_years(years)
    query = select(PermitEvent).where(
        PermitEvent.parcel_id == parcel_id,
        PermitEvent.import_status == "loaded",
    )
    permits = session.execute(query).scalars().all()

    result: list[PermitEventRow] = []
    for permit in permits:
        if years_list and not _year_in_scope(
            years_list,
            permit.permit_year,
            permit.issued_date,
            permit.applied_date,
            permit.status_date,
        ):
            continue
        result.append(
            {
                "permit_event_id": permit.id,
                "permit_year": permit.permit_year,
                "issued_date": permit.issued_date,
                "applied_date": permit.applied_date,
                "finaled_date": permit.finaled_date,
                "status_date": permit.status_date,
                "permit_number": permit.permit_number,
                "permit_type": permit.permit_type,
                "permit_subtype": permit.permit_subtype,
                "work_class": permit.work_class,
                "permit_status": permit.permit_status_norm,
                "declared_valuation": permit.declared_valuation,
                "estimated_cost": permit.estimated_cost,
                "description": permit.description,
                "parcel_link_method": permit.parcel_link_method,
                "parcel_link_confidence": permit.parcel_link_confidence,
            }
        )

    result.sort(
        key=lambda row: (
            _coalesced_permit_date(row) is None,
            -_coalesced_permit_ordinal(row),
            -row["permit_event_id"],
        )
    )
    return result


def list_appeal_events(
    session: Session,
    *,
    parcel_id: str,
    years: Optional[Iterable[int]] = None,
) -> list[AppealEventRow]:
    years_list = _normalize_years(years)
    query = select(AppealEvent).where(
        AppealEvent.parcel_id == parcel_id,
        AppealEvent.import_status == "loaded",
    )
    appeals = session.execute(query).scalars().all()

    result: list[AppealEventRow] = []
    for appeal in appeals:
        if years_list and not _year_in_scope(
            years_list,
            appeal.tax_year,
            appeal.decision_date,
            appeal.hearing_date,
            appeal.filing_date,
        ):
            continue
        result.append(
            {
                "appeal_event_id": appeal.id,
                "tax_year": appeal.tax_year,
                "filing_date": appeal.filing_date,
                "hearing_date": appeal.hearing_date,
                "decision_date": appeal.decision_date,
                "appeal_number": appeal.appeal_number,
                "docket_number": appeal.docket_number,
                "appeal_level": appeal.appeal_level_norm,
                "outcome": appeal.outcome_norm,
                "assessed_value_before": appeal.assessed_value_before,
                "requested_assessed_value": appeal.requested_assessed_value,
                "decided_assessed_value": appeal.decided_assessed_value,
                "value_change_amount": appeal.value_change_amount,
                "representative_name": appeal.representative_name,
                "parcel_link_method": appeal.parcel_link_method,
                "parcel_link_confidence": appeal.parcel_link_confidence,
            }
        )

    result.sort(
        key=lambda row: (
            _coalesced_appeal_date(row) is None,
            -_coalesced_appeal_ordinal(row),
            -row["appeal_event_id"],
        )
    )
    return result


def list_reason_code_evidence(
    session: Session,
    *,
    parcel_id: str,
    feature_version: str,
    ruleset_version: str,
    years: Optional[Iterable[int]] = None,
) -> list[ReasonCodeEvidenceRow]:
    years_list = _normalize_years(years)
    score_query = select(FraudScore).where(
        FraudScore.parcel_id == parcel_id,
        FraudScore.feature_version == feature_version,
        FraudScore.ruleset_version == ruleset_version,
    )
    if years_list:
        score_query = score_query.where(FraudScore.year.in_(years_list))
    scores = session.execute(score_query).scalars().all()
    score_ids = [score.id for score in scores]

    flags_by_score: dict[int, list[FraudFlag]] = defaultdict(list)
    if score_ids:
        flags = (
            session.execute(
                select(FraudFlag)
                .where(FraudFlag.score_id.in_(score_ids))
                .order_by(FraudFlag.score_id.asc())
            )
            .scalars()
            .all()
        )
        for flag in flags:
            flags_by_score[flag.score_id].append(flag)
    for grouped_flags in flags_by_score.values():
        grouped_flags.sort(key=lambda row: (row.reason_rank, row.reason_code))

    rows: list[ReasonCodeEvidenceRow] = []
    for score in scores:
        reason_codes: list[ReasonCodeRow] = []
        for flag in flags_by_score.get(score.id, []):
            reason_codes.append(
                {
                    "reason_code": flag.reason_code,
                    "reason_rank": flag.reason_rank,
                    "severity_weight": flag.severity_weight,
                    "metric_name": flag.metric_name,
                    "metric_value": flag.metric_value,
                    "threshold_value": flag.threshold_value,
                    "comparison_operator": flag.comparison_operator,
                    "explanation": flag.explanation,
                    "source_refs": flag.source_refs_json,
                }
            )
        rows.append(
            {
                "score_id": score.id,
                "run_id": score.run_id,
                "feature_run_id": score.feature_run_id,
                "year": score.year,
                "score_value": score.score_value,
                "risk_band": score.risk_band,
                "requires_review": score.requires_review,
                "reason_code_count": score.reason_code_count,
                "scored_at": score.scored_at,
                "reason_codes": reason_codes,
            }
        )

    rows.sort(
        key=lambda row: (
            -row["year"],
            -(row["score_value"]),
            -row["score_id"],
        )
    )
    return rows


def build_timeline_rows(
    *,
    assessment_history: Iterable[AssessmentHistoryRow],
    matched_sales: Iterable[MatchedSaleRow],
    permit_events: Iterable[PermitEventRow],
    appeal_events: Iterable[AppealEventRow],
    reason_code_evidence: Iterable[ReasonCodeEvidenceRow],
) -> list[TimelineEvent]:
    rows: list[TimelineEvent] = []

    for assessment in assessment_history:
        if assessment["assessment_id"] is None:
            # Contract requirement: fallback-only facts are not timeline events.
            continue
        rows.append(
            {
                "event_date": _assessment_event_date(assessment),
                "event_type": "assessment",
                "year": assessment["year"],
                "title": "Assessment snapshot",
                "details": {
                    "valuation_classification": assessment["valuation_classification"],
                    "total_value": assessment["total_value"],
                },
                "source": {
                    "table": "assessments",
                    "row_id": assessment["assessment_id"],
                    "source_refs": None,
                },
            }
        )

    for sale in matched_sales:
        rows.append(
            {
                "event_date": sale["transfer_date"] or sale["recording_date"],
                "event_type": "sale",
                "year": _coalesced_sale_year_from_row(sale),
                "title": "Matched sale",
                "details": {
                    "consideration_amount": sale["consideration_amount"],
                    "match_method": sale["match_method"],
                },
                "source": {
                    "table": "sales_transactions",
                    "row_id": sale["sales_transaction_id"],
                    "source_refs": None,
                },
            }
        )

    for permit in permit_events:
        rows.append(
            {
                "event_date": _permit_event_date(permit),
                "event_type": "permit",
                "year": permit["permit_year"],
                "title": "Permit event",
                "details": {
                    "permit_type": permit["permit_type"],
                    "permit_status": permit["permit_status"],
                },
                "source": {
                    "table": "permit_events",
                    "row_id": permit["permit_event_id"],
                    "source_refs": None,
                },
            }
        )

    for appeal in appeal_events:
        rows.append(
            {
                "event_date": _appeal_event_date(appeal),
                "event_type": "appeal",
                "year": appeal["tax_year"],
                "title": "Appeal event",
                "details": {
                    "outcome": appeal["outcome"],
                    "value_change_amount": appeal["value_change_amount"],
                },
                "source": {
                    "table": "appeal_events",
                    "row_id": appeal["appeal_event_id"],
                    "source_refs": None,
                },
            }
        )

    for score_row in reason_code_evidence:
        rows.append(
            {
                "event_date": _score_event_date(score_row),
                "event_type": "score",
                "year": score_row["year"],
                "title": "Fraud score",
                "details": {
                    "risk_band": score_row["risk_band"],
                    "score_value": score_row["score_value"],
                    "reason_code_count": score_row["reason_code_count"],
                },
                "source": {
                    "table": "fraud_scores",
                    "row_id": score_row["score_id"],
                    "source_refs": None,
                },
            }
        )

    precedence = {"score": 0, "appeal": 1, "permit": 2, "sale": 3, "assessment": 4}
    rows.sort(
        key=lambda row: (
            row["event_date"] is None,
            -(row["event_date"].toordinal() if row["event_date"] is not None else -1),
            precedence[row["event_type"]],
            row["source"]["table"],
            (
                row["source"]["row_id"]
                if row["source"]["row_id"] is not None
                else 2_147_483_647
            ),
        )
    )
    return rows


def _normalize_years(years: Optional[Iterable[int]]) -> list[int]:
    if years is None:
        return []
    return sorted(set(years))


def _match_priority_key(match_row: SalesParcelMatch) -> tuple[object, ...]:
    confidence = (
        match_row.confidence_score
        if match_row.confidence_score is not None
        else Decimal(0)
    )
    return (
        0 if match_row.is_primary else 1,
        0 if match_row.confidence_score is not None else 1,
        -confidence,
        0 if match_row.match_rank is not None else 1,
        match_row.match_rank if match_row.match_rank is not None else 0,
        match_row.id,
    )


def _active_exclusion_codes_by_transaction(
    session: Session, sales_transaction_ids: list[int]
) -> dict[int, list[str]]:
    if not sales_transaction_ids:
        return {}
    exclusions = (
        session.execute(
            select(SalesExclusion).where(
                SalesExclusion.sales_transaction_id.in_(sales_transaction_ids),
                SalesExclusion.is_active.is_(True),
            )
        )
        .scalars()
        .all()
    )
    grouped: dict[int, list[str]] = defaultdict(list)
    for exclusion in exclusions:
        grouped[exclusion.sales_transaction_id].append(exclusion.exclusion_code)
    for key in list(grouped):
        grouped[key].sort()
    return dict(grouped)


def _coalesced_sale_year(transaction: SalesTransaction) -> Optional[int]:
    if transaction.transfer_date is not None:
        return transaction.transfer_date.year
    if transaction.recording_date is not None:
        return transaction.recording_date.year
    return None


def _coalesced_sale_year_from_row(row: MatchedSaleRow) -> Optional[int]:
    if row["transfer_date"] is not None:
        return row["transfer_date"].year
    if row["recording_date"] is not None:
        return row["recording_date"].year
    return None


def _coalesced_sale_date_from_row(row: MatchedSaleRow) -> Optional[date]:
    return row["transfer_date"] or row["recording_date"]


def _coalesced_sale_ordinal_from_row(row: MatchedSaleRow) -> int:
    coalesced = _coalesced_sale_date_from_row(row)
    return coalesced.toordinal() if coalesced is not None else -1


def _year_in_scope(
    years: list[int],
    primary_year: Optional[int],
    *fallback_dates: Optional[date],
) -> bool:
    if primary_year is not None:
        return primary_year in years
    for value in fallback_dates:
        if value is not None and value.year in years:
            return True
    return False


def _coalesced_permit_date(row: PermitEventRow) -> Optional[date]:
    return row["issued_date"] or row["applied_date"] or row["status_date"]


def _coalesced_permit_ordinal(row: PermitEventRow) -> int:
    coalesced = _coalesced_permit_date(row)
    return coalesced.toordinal() if coalesced is not None else -1


def _coalesced_appeal_date(row: AppealEventRow) -> Optional[date]:
    return row["decision_date"] or row["hearing_date"] or row["filing_date"]


def _coalesced_appeal_ordinal(row: AppealEventRow) -> int:
    coalesced = _coalesced_appeal_date(row)
    return coalesced.toordinal() if coalesced is not None else -1


def _assessment_event_date(row: AssessmentHistoryRow) -> Optional[date]:
    if row["valuation_date"] is not None:
        return row["valuation_date"]
    if row["year"] is not None:
        return date(row["year"], 1, 1)
    return None


def _permit_event_date(row: PermitEventRow) -> Optional[date]:
    direct = _coalesced_permit_date(row)
    if direct is not None:
        return direct
    if row["permit_year"] is not None:
        return date(row["permit_year"], 1, 1)
    return None


def _appeal_event_date(row: AppealEventRow) -> Optional[date]:
    direct = _coalesced_appeal_date(row)
    if direct is not None:
        return direct
    if row["tax_year"] is not None:
        return date(row["tax_year"], 1, 1)
    return None


def _score_event_date(row: ReasonCodeEvidenceRow) -> Optional[date]:
    scored_at = row["scored_at"]
    if scored_at is not None:
        if scored_at.tzinfo is None:
            return scored_at.date()
        return scored_at.astimezone(timezone.utc).date()
    if row["year"] is not None:
        return date(row["year"], 12, 31)
    return None
