from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from statistics import median
from typing import Optional, Sequence, TypedDict

from sqlalchemy import and_, exists, or_, select
from sqlalchemy.exc import InvalidRequestError, PendingRollbackError
from sqlalchemy.orm import Session

from .models import (
    ParcelYearFact,
    SalesExclusion,
    SalesParcelMatch,
    SalesTransaction,
    ScoringRun,
)

RUN_TYPE_SALES_RATIO_STUDY = "sales_ratio_study"
OUTLIER_LOW_RATIO_THRESHOLD = Decimal("0.5000")
OUTLIER_HIGH_RATIO_THRESHOLD = Decimal("1.5000")
IN_CLAUSE_BATCH_SIZE = 800


@dataclass(frozen=True)
class SalesRatioStudyInputRow:
    parcel_id: str
    year: int
    consideration_amount: Decimal
    has_active_exclusion: bool


@dataclass
class _GroupAccumulator:
    included_ratios: list[Decimal] = field(default_factory=list)
    included_ratio_sum: Decimal = Decimal("0")
    included_assessed_sum: Decimal = Decimal("0")
    included_sale_sum: Decimal = Decimal("0")
    excluded_count: int = 0
    outlier_low_count: int = 0
    outlier_high_count: int = 0


class ScopePayload(TypedDict):
    parcel_ids: Optional[list[str]]
    years: Optional[list[int]]
    municipality: Optional[str]
    valuation_classification: Optional[str]


class SalesRatioStudyRun(TypedDict):
    run_id: Optional[int]
    run_persisted: bool
    run_type: str
    version_tag: str
    status: str


class SalesRatioStudySummary(TypedDict):
    candidate_sales_count: int
    included_sales_count: int
    excluded_sales_count: int
    skipped_scope_filter_count: int
    skipped_missing_parcel_year_fact_count: int
    skipped_missing_assessment_count: int
    group_count: int


class SalesRatioStudyPayload(TypedDict, total=False):
    run: SalesRatioStudyRun
    scope: ScopePayload
    summary: SalesRatioStudySummary
    groups: list[dict[str, object]]
    error: str


def build_sales_ratio_study(
    session: Session,
    *,
    version_tag: str,
    parcel_ids: Optional[Sequence[str]] = None,
    years: Optional[Sequence[int]] = None,
    municipality: Optional[str] = None,
    valuation_classification: Optional[str] = None,
) -> SalesRatioStudyPayload:
    resolved_scope = _resolved_scope(
        parcel_ids=parcel_ids,
        years=years,
        municipality=municipality,
        valuation_classification=valuation_classification,
    )
    config_json = {
        "exclude_active_sales_exclusions": True,
        "area_key_mode": "municipality_name",
        "outlier_low_ratio_threshold": str(OUTLIER_LOW_RATIO_THRESHOLD),
        "outlier_high_ratio_threshold": str(OUTLIER_HIGH_RATIO_THRESHOLD),
    }
    run = ScoringRun(
        run_type=RUN_TYPE_SALES_RATIO_STUDY,
        status="running",
        version_tag=version_tag,
        scope_hash=_scope_hash(resolved_scope, config_json),
        scope_json=dict(resolved_scope),
        config_json=config_json,
    )

    try:
        session.add(run)
        session.flush()
        rows = _load_candidate_rows(
            session,
            parcel_ids=resolved_scope["parcel_ids"],
            years=resolved_scope["years"],
        )
        payload = _build_payload(
            rows=rows,
            session=session,
            run=run,
            scope=resolved_scope,
            municipality_filter=resolved_scope["municipality"],
            valuation_class_filter=resolved_scope["valuation_classification"],
        )
        run.status = "succeeded"
        run.error_summary = None
        run.input_summary_json = {
            "candidate_sales_count": payload["summary"]["candidate_sales_count"]
        }
        run.output_summary_json = dict(payload["summary"])
        run.completed_at = datetime.now(timezone.utc)
        session.flush()
        payload["run"] = _run_payload(run)
        return payload
    except Exception as exc:
        run_persisted = True
        failure_summary: SalesRatioStudySummary = {
            "candidate_sales_count": 0,
            "included_sales_count": 0,
            "excluded_sales_count": 0,
            "skipped_scope_filter_count": 0,
            "skipped_missing_parcel_year_fact_count": 0,
            "skipped_missing_assessment_count": 0,
            "group_count": 0,
        }
        run.status = "failed"
        run.error_summary = str(exc)
        run.input_summary_json = None
        run.output_summary_json = dict(failure_summary)
        run.completed_at = datetime.now(timezone.utc)
        try:
            session.flush()
        except (PendingRollbackError, InvalidRequestError):
            # If the original error invalidated the transaction, avoid masking it.
            session.rollback()
            run_persisted = False
        return {
            "run": _run_payload(run, run_persisted=run_persisted),
            "scope": resolved_scope,
            "summary": failure_summary,
            "groups": [],
            "error": str(exc),
        }


def _build_payload(
    *,
    rows: list[SalesRatioStudyInputRow],
    session: Session,
    run: ScoringRun,
    scope: ScopePayload,
    municipality_filter: Optional[str],
    valuation_class_filter: Optional[str],
) -> SalesRatioStudyPayload:
    fact_map = _parcel_year_fact_map(session, rows)
    groups: dict[
        tuple[int, Optional[str], Optional[str], Optional[str]], _GroupAccumulator
    ]
    groups = defaultdict(_GroupAccumulator)

    included_sales_count = 0
    excluded_sales_count = 0
    skipped_scope_filter_count = 0
    skipped_missing_parcel_year_fact_count = 0
    skipped_missing_assessment_count = 0

    for row in rows:
        fact = fact_map.get((row.parcel_id, row.year))
        if fact is None:
            skipped_missing_parcel_year_fact_count += 1
            continue

        municipality_name = fact.municipality_name
        valuation_classification = fact.assessment_valuation_classification
        if not _matches_text_filter(municipality_name, municipality_filter):
            skipped_scope_filter_count += 1
            continue
        if not _matches_text_filter(valuation_classification, valuation_class_filter):
            skipped_scope_filter_count += 1
            continue

        area_key = municipality_name
        group_key = (
            row.year,
            municipality_name,
            valuation_classification,
            area_key,
        )
        if row.has_active_exclusion:
            group = groups[group_key]
            group.excluded_count += 1
            excluded_sales_count += 1
            continue

        if (
            fact.assessment_total_value is None
            or row.consideration_amount <= 0
            or fact.assessment_total_value < 0
        ):
            skipped_missing_assessment_count += 1
            continue

        group = groups[group_key]
        ratio = fact.assessment_total_value / row.consideration_amount
        group.included_ratios.append(ratio)
        group.included_ratio_sum += ratio
        group.included_assessed_sum += fact.assessment_total_value
        group.included_sale_sum += row.consideration_amount
        included_sales_count += 1

        if ratio < OUTLIER_LOW_RATIO_THRESHOLD:
            group.outlier_low_count += 1
        if ratio > OUTLIER_HIGH_RATIO_THRESHOLD:
            group.outlier_high_count += 1

    grouped_payload = [
        _group_payload(group_key, accumulator)
        for group_key, accumulator in sorted(
            groups.items(),
            key=lambda item: (
                item[0][0],
                item[0][1] or "",
                item[0][2] or "",
                item[0][3] or "",
            ),
        )
    ]
    summary: SalesRatioStudySummary = {
        "candidate_sales_count": len(rows),
        "included_sales_count": included_sales_count,
        "excluded_sales_count": excluded_sales_count,
        "skipped_scope_filter_count": skipped_scope_filter_count,
        "skipped_missing_parcel_year_fact_count": (
            skipped_missing_parcel_year_fact_count
        ),
        "skipped_missing_assessment_count": skipped_missing_assessment_count,
        "group_count": len(grouped_payload),
    }
    return {
        "run": _run_payload(run),
        "scope": scope,
        "summary": summary,
        "groups": grouped_payload,
    }


def _group_payload(
    group_key: tuple[int, Optional[str], Optional[str], Optional[str]],
    accumulator: _GroupAccumulator,
) -> dict[str, object]:
    year, municipality_name, valuation_classification, area_key = group_key
    ratios = accumulator.included_ratios
    median_ratio = _median_ratio(ratios)
    cod = _cod(ratios, median_ratio)
    prd = _prd(
        ratio_count=len(ratios),
        ratio_sum=accumulator.included_ratio_sum,
        assessed_sum=accumulator.included_assessed_sum,
        sale_sum=accumulator.included_sale_sum,
    )
    return {
        "year": year,
        "municipality_name": municipality_name,
        "valuation_classification": valuation_classification,
        "area_key": area_key,
        "sale_count": len(ratios),
        "median_ratio": _decimal_as_float(median_ratio, scale=6),
        "cod": _decimal_as_float(cod, scale=4),
        "prd": _decimal_as_float(prd, scale=4),
        "outlier_low_count": accumulator.outlier_low_count,
        "outlier_high_count": accumulator.outlier_high_count,
        "excluded_count": accumulator.excluded_count,
    }


def _load_candidate_rows(
    session: Session,
    *,
    parcel_ids: Optional[list[str]],
    years: Optional[list[int]],
) -> list[SalesRatioStudyInputRow]:
    if parcel_ids is not None and not parcel_ids:
        return []
    if years is not None and not years:
        return []

    has_active_exclusion = exists(
        select(SalesExclusion.id).where(
            and_(
                SalesExclusion.sales_transaction_id == SalesTransaction.id,
                SalesExclusion.is_active.is_(True),
            )
        )
    )
    base_query = (
        select(
            SalesParcelMatch.parcel_id,
            SalesTransaction.transfer_date,
            SalesTransaction.consideration_amount,
            has_active_exclusion.label("has_active_exclusion"),
        )
        .select_from(SalesTransaction)
        .join(
            SalesParcelMatch,
            and_(
                SalesParcelMatch.sales_transaction_id == SalesTransaction.id,
                SalesParcelMatch.is_primary.is_(True),
            ),
        )
        .where(
            SalesTransaction.import_status == "loaded",
            SalesTransaction.transfer_date.is_not(None),
            SalesTransaction.consideration_amount.is_not(None),
            SalesTransaction.consideration_amount > 0,
        )
    )
    if years is not None:
        year_ranges = [
            and_(
                SalesTransaction.transfer_date >= date(year, 1, 1),
                SalesTransaction.transfer_date < date(year + 1, 1, 1),
            )
            for year in years
        ]
        base_query = base_query.where(or_(*year_ranges))

    rows: list[SalesRatioStudyInputRow] = []
    queries = (
        [
            base_query.where(SalesParcelMatch.parcel_id.in_(batch_parcel_ids))
            for batch_parcel_ids in _chunked(parcel_ids, IN_CLAUSE_BATCH_SIZE)
        ]
        if parcel_ids is not None
        else [base_query]
    )
    for query in queries:
        for parcel_id, transfer_date, consideration_amount, excluded in session.execute(
            query
        ):
            rows.append(
                SalesRatioStudyInputRow(
                    parcel_id=parcel_id,
                    year=transfer_date.year,
                    consideration_amount=consideration_amount,
                    has_active_exclusion=bool(excluded),
                )
            )
    return rows


def _parcel_year_fact_map(
    session: Session, rows: Sequence[SalesRatioStudyInputRow]
) -> dict[tuple[str, int], ParcelYearFact]:
    if not rows:
        return {}

    parcel_ids = sorted({row.parcel_id for row in rows})
    years = sorted({row.year for row in rows})
    facts_by_key: dict[tuple[str, int], ParcelYearFact] = {}
    for batch_parcel_ids in _chunked(parcel_ids, IN_CLAUSE_BATCH_SIZE):
        query = select(ParcelYearFact).where(
            ParcelYearFact.parcel_id.in_(batch_parcel_ids),
            ParcelYearFact.year.in_(years),
        )
        for fact in session.execute(query).scalars().all():
            facts_by_key[(fact.parcel_id, fact.year)] = fact
    return facts_by_key


def _resolved_scope(
    *,
    parcel_ids: Optional[Sequence[str]],
    years: Optional[Sequence[int]],
    municipality: Optional[str],
    valuation_classification: Optional[str],
) -> ScopePayload:
    resolved_parcel_ids = (
        sorted({parcel_id.strip() for parcel_id in parcel_ids if parcel_id.strip()})
        if parcel_ids is not None
        else None
    )
    resolved_years = sorted(set(years)) if years is not None else None
    resolved_municipality = _normalized_scope_text(municipality)
    resolved_classification = _normalized_scope_text(valuation_classification)
    return {
        "parcel_ids": resolved_parcel_ids,
        "years": resolved_years,
        "municipality": resolved_municipality,
        "valuation_classification": resolved_classification,
    }


def _scope_hash(scope_json: ScopePayload, config_json: dict[str, object]) -> str:
    digest_input = json.dumps(
        {"scope": scope_json, "config": config_json},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(digest_input).hexdigest()


def _run_payload(run: ScoringRun, *, run_persisted: bool = True) -> SalesRatioStudyRun:
    run_id = run.id if (run_persisted and run.id is not None) else None
    return {
        "run_id": run_id,
        "run_persisted": run_persisted,
        "run_type": run.run_type,
        "version_tag": run.version_tag,
        "status": run.status,
    }


def _matches_text_filter(value: Optional[str], filter_value: Optional[str]) -> bool:
    if filter_value is None:
        return True
    if value is None:
        return False
    return value.casefold() == filter_value.casefold()


def _normalized_scope_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped.casefold()


def _median_ratio(ratios: Sequence[Decimal]) -> Optional[Decimal]:
    if not ratios:
        return None
    return median(ratios)


def _cod(
    ratios: Sequence[Decimal], median_ratio: Optional[Decimal]
) -> Optional[Decimal]:
    if not ratios or median_ratio is None or median_ratio == 0:
        return None
    total_absolute_deviation = sum(abs(ratio - median_ratio) for ratio in ratios)
    cod = (total_absolute_deviation / (Decimal(len(ratios)) * median_ratio)) * Decimal(
        "100"
    )
    return cod


def _prd(
    *,
    ratio_count: int,
    ratio_sum: Decimal,
    assessed_sum: Decimal,
    sale_sum: Decimal,
) -> Optional[Decimal]:
    if ratio_count <= 0 or sale_sum <= 0:
        return None
    mean_ratio = ratio_sum / Decimal(ratio_count)
    weighted_mean_ratio = assessed_sum / sale_sum
    if weighted_mean_ratio == 0:
        return None
    return mean_ratio / weighted_mean_ratio


def _decimal_as_float(value: Optional[Decimal], *, scale: int) -> Optional[float]:
    if value is None:
        return None
    quantized = value.quantize(Decimal(10) ** -scale, rounding=ROUND_HALF_UP)
    return float(quantized)


def _chunked(values: Sequence[str], batch_size: int) -> list[list[str]]:
    return [
        list(values[index : index + batch_size])
        for index in range(0, len(values), batch_size)
    ]
