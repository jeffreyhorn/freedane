from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, TypedDict

from sqlalchemy import case, func, select, tuple_
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

from .models import FraudFlag, FraudScore, ParcelYearFact
from .score_fraud import SUPPORTED_RULESET_VERSIONS

RUN_TYPE_REVIEW_QUEUE = "review_queue"
REVIEW_QUEUE_VERSION_TAG = "review_queue_v1"
REVIEW_QUEUE_SORT_KEY_VERSION = "review_queue_sort_v1"
_SOURCE_QUERY_ERROR_MESSAGE = "Failed to query source data while building review queue."
_TOP_N_DEFAULT = 100
_PAGE_DEFAULT = 1
_PAGE_SIZE_DEFAULT = 100
_SCALAR_IN_CLAUSE_BATCH_SIZE = 800
_COMPOSITE_KEY_IN_CLAUSE_BATCH_SIZE = 400

RISK_BAND_PRECEDENCE: dict[str, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}

CSV_COLUMNS = [
    "queue_rank",
    "score_id",
    "run_id",
    "feature_run_id",
    "parcel_id",
    "year",
    "score_value",
    "risk_band",
    "requires_review",
    "reason_code_count",
    "primary_reason_code",
    "primary_reason_weight",
    "municipality_name",
    "valuation_classification",
    "dossier_parcel_id",
    "dossier_year",
    "dossier_feature_version",
    "dossier_ruleset_version",
]


class ReviewQueueRun(TypedDict):
    run_id: Optional[int]
    run_persisted: bool
    run_type: str
    version_tag: str
    status: str


class ReviewQueueRequest(TypedDict):
    top: Optional[int]
    page: Optional[int]
    page_size: Optional[int]
    parcel_ids: list[str]
    years: list[int]
    feature_version: str
    ruleset_version: str
    risk_bands: list[str]
    requires_review_only: bool


class ReviewQueueSummary(TypedDict):
    candidate_count: int
    filtered_count: int
    skipped_count: int
    returned_count: int
    truncated: bool
    page: Optional[int]
    page_size: Optional[int]
    total_pages: Optional[int]


class ReviewQueueRow(TypedDict):
    queue_rank: int
    score_id: int
    run_id: int
    feature_run_id: Optional[int]
    parcel_id: str
    year: int
    score_value: str
    risk_band: str
    requires_review: bool
    reason_code_count: int
    primary_reason_code: Optional[str]
    primary_reason_weight: Optional[str]
    municipality_name: Optional[str]
    valuation_classification: Optional[str]
    dossier_args: dict[str, object]


class ReviewQueueComparability(TypedDict):
    comparable: bool
    queue_contract_version: str
    comparison_key: dict[str, object]


class ReviewQueueDiagnostics(TypedDict):
    filtered_reason_counts: dict[str, int]
    skipped_row_counts: dict[str, int]
    comparability: ReviewQueueComparability


class ReviewQueueError(TypedDict):
    code: str
    message: str


class ReviewQueuePayload(TypedDict, total=False):
    run: ReviewQueueRun
    request: ReviewQueueRequest
    summary: ReviewQueueSummary
    rows: list[ReviewQueueRow]
    diagnostics: ReviewQueueDiagnostics
    error: Optional[ReviewQueueError]


@dataclass(frozen=True)
class _ResolvedMode:
    slice_mode: str
    top: Optional[int]
    page: Optional[int]
    page_size: Optional[int]


def build_review_queue(
    session: Session,
    *,
    top: Optional[int] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    parcel_ids: Optional[Sequence[str]] = None,
    years: Optional[Sequence[int]] = None,
    feature_version: str = "feature_v1",
    ruleset_version: str = "scoring_rules_v1",
    risk_bands: Optional[Sequence[str]] = None,
    requires_review_only: bool = True,
) -> ReviewQueuePayload:
    normalized_parcel_ids = _normalize_parcel_ids(parcel_ids)
    normalized_years = sorted(set(years or []))
    normalized_risk_bands = _normalize_risk_bands(risk_bands)
    unsupported_risk_bands = _unsupported_risk_bands(risk_bands)
    resolved_mode = _resolve_mode(top=top, page=page, page_size=page_size)

    request: ReviewQueueRequest = {
        "top": resolved_mode.top,
        "page": resolved_mode.page,
        "page_size": resolved_mode.page_size,
        "parcel_ids": normalized_parcel_ids,
        "years": normalized_years,
        "feature_version": feature_version,
        "ruleset_version": ruleset_version,
        "risk_bands": normalized_risk_bands,
        "requires_review_only": requires_review_only,
    }

    if unsupported_risk_bands:
        supported_values = ", ".join(
            sorted(
                RISK_BAND_PRECEDENCE,
                key=lambda risk_band: RISK_BAND_PRECEDENCE[risk_band],
            )
        )
        return _failure_payload(
            request=request,
            code="unsupported_risk_band",
            message=(
                "Unsupported risk_bands: "
                f"{', '.join(unsupported_risk_bands)}. "
                f"Supported values: {supported_values}."
            ),
        )

    if ruleset_version not in SUPPORTED_RULESET_VERSIONS:
        supported_values = ", ".join(SUPPORTED_RULESET_VERSIONS)
        return _failure_payload(
            request=request,
            code="unsupported_version_selector",
            message=(
                f"Unsupported ruleset_version '{ruleset_version}'. "
                f"Supported values: {supported_values}."
            ),
        )

    try:
        base_conditions = [
            FraudScore.feature_version == feature_version,
            FraudScore.ruleset_version == ruleset_version,
        ]
        candidate_count = _count_scores(session, conditions=base_conditions)

        filtered_reason_counts: dict[str, int] = {}
        filtered_conditions = list(base_conditions)
        filtered_total_count = candidate_count

        if normalized_parcel_ids:
            filtered_total_count = _advance_filter_stage(
                session,
                filtered_conditions=filtered_conditions,
                previous_count=filtered_total_count,
                extra_condition=FraudScore.parcel_id.in_(normalized_parcel_ids),
                filtered_reason_counts=filtered_reason_counts,
                reason_key="filtered_by_parcel_id",
            )
        if normalized_years:
            filtered_total_count = _advance_filter_stage(
                session,
                filtered_conditions=filtered_conditions,
                previous_count=filtered_total_count,
                extra_condition=FraudScore.year.in_(normalized_years),
                filtered_reason_counts=filtered_reason_counts,
                reason_key="filtered_by_year",
            )
        if normalized_risk_bands:
            filtered_total_count = _advance_filter_stage(
                session,
                filtered_conditions=filtered_conditions,
                previous_count=filtered_total_count,
                extra_condition=FraudScore.risk_band.in_(normalized_risk_bands),
                filtered_reason_counts=filtered_reason_counts,
                reason_key="filtered_by_risk_band",
            )
        if requires_review_only:
            filtered_total_count = _advance_filter_stage(
                session,
                filtered_conditions=filtered_conditions,
                previous_count=filtered_total_count,
                extra_condition=(FraudScore.requires_review.is_(True)),
                filtered_reason_counts=filtered_reason_counts,
                reason_key="filtered_by_requires_review",
            )

        valid_conditions = [*filtered_conditions, *_required_identifier_conditions()]
        valid_count = _count_scores(session, conditions=valid_conditions)
        invalid_required_identifiers = max(filtered_total_count - valid_count, 0)

        skipped_row_counts = {
            "invalid_required_identifiers": invalid_required_identifiers,
        }

        eligible_total = valid_count
        paged_query = (
            select(FraudScore)
            .where(*valid_conditions)
            .order_by(*_score_order_clauses())
        )
        if resolved_mode.slice_mode == "top":
            assert resolved_mode.top is not None
            paged_query = paged_query.limit(resolved_mode.top)
            queue_rank_start = 1
        else:
            assert resolved_mode.page is not None
            assert resolved_mode.page_size is not None
            offset = max((resolved_mode.page - 1) * resolved_mode.page_size, 0)
            paged_query = paged_query.offset(offset).limit(resolved_mode.page_size)
            queue_rank_start = offset + 1

        scored_rows = _load_base_scores(session, query=paged_query)
        primary_reason_map = _load_primary_reason_map(
            session,
            score_ids=[row.id for row in scored_rows],
        )
        parcel_fact_map = _load_parcel_fact_map(
            session,
            score_rows=scored_rows,
        )

        returned_rows: list[ReviewQueueRow] = []
        for index, score in enumerate(scored_rows, start=queue_rank_start):
            reason_code, reason_weight = primary_reason_map.get(score.id, (None, None))
            primary_reason_weight = _format_primary_reason_weight(
                primary_reason_code=reason_code,
                severity_weight=reason_weight,
            )

            parcel_fact = parcel_fact_map.get((score.parcel_id, score.year))
            municipality_name = (
                parcel_fact.municipality_name if parcel_fact is not None else None
            )
            valuation_classification = (
                parcel_fact.assessment_valuation_classification
                if parcel_fact is not None
                else None
            )

            returned_rows.append(
                {
                    "queue_rank": index,
                    "score_id": score.id,
                    "run_id": score.run_id,
                    "feature_run_id": score.feature_run_id,
                    "parcel_id": score.parcel_id,
                    "year": score.year,
                    "score_value": _decimal_to_str(score.score_value, scale=2),
                    "risk_band": score.risk_band,
                    "requires_review": bool(score.requires_review),
                    "reason_code_count": score.reason_code_count,
                    "primary_reason_code": reason_code,
                    "primary_reason_weight": primary_reason_weight,
                    "municipality_name": municipality_name,
                    "valuation_classification": valuation_classification,
                    "dossier_args": {
                        "parcel_id": score.parcel_id,
                        "year": score.year,
                        "feature_version": feature_version,
                        "ruleset_version": ruleset_version,
                    },
                }
            )

        filtered_count = sum(filtered_reason_counts.values())
        skipped_count = sum(skipped_row_counts.values())
        returned_count = len(returned_rows)

        summary: ReviewQueueSummary = {
            "candidate_count": candidate_count,
            "filtered_count": filtered_count,
            "skipped_count": skipped_count,
            "returned_count": returned_count,
            "truncated": returned_count < eligible_total,
            "page": resolved_mode.page,
            "page_size": resolved_mode.page_size,
            "total_pages": _total_pages(eligible_total, resolved_mode.page_size),
        }

        diagnostics: ReviewQueueDiagnostics = {
            "filtered_reason_counts": {
                key: value for key, value in filtered_reason_counts.items() if value > 0
            },
            "skipped_row_counts": {
                key: value for key, value in skipped_row_counts.items() if value > 0
            },
            "comparability": {
                "comparable": True,
                "queue_contract_version": REVIEW_QUEUE_VERSION_TAG,
                "comparison_key": {
                    "feature_version": feature_version,
                    "ruleset_version": ruleset_version,
                    "requires_review_only": requires_review_only,
                    "risk_bands": normalized_risk_bands,
                    "years": normalized_years,
                    "parcel_ids": normalized_parcel_ids,
                    "sort_key_version": REVIEW_QUEUE_SORT_KEY_VERSION,
                    "slice_mode": resolved_mode.slice_mode,
                    "top": resolved_mode.top,
                    "page": resolved_mode.page,
                    "page_size": resolved_mode.page_size,
                },
            },
        }

        return {
            "run": _run_payload("succeeded"),
            "request": request,
            "summary": summary,
            "rows": returned_rows,
            "diagnostics": diagnostics,
            "error": None,
        }
    except Exception:
        session.rollback()
        return _failure_payload(
            request=request,
            code="source_query_error",
            message=_SOURCE_QUERY_ERROR_MESSAGE,
        )


def write_review_queue_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(CSV_COLUMNS)
        for row in rows:
            dossier_args = row.get("dossier_args")
            if not isinstance(dossier_args, Mapping):
                dossier_args = {}
            ordered_values = [
                row.get("queue_rank"),
                row.get("score_id"),
                row.get("run_id"),
                row.get("feature_run_id"),
                row.get("parcel_id"),
                row.get("year"),
                row.get("score_value"),
                row.get("risk_band"),
                row.get("requires_review"),
                row.get("reason_code_count"),
                row.get("primary_reason_code"),
                row.get("primary_reason_weight"),
                row.get("municipality_name"),
                row.get("valuation_classification"),
                dossier_args.get("parcel_id"),
                dossier_args.get("year"),
                dossier_args.get("feature_version"),
                dossier_args.get("ruleset_version"),
            ]
            writer.writerow(_csv_value(value) for value in ordered_values)


def _run_payload(status: str) -> ReviewQueueRun:
    return {
        "run_id": None,
        "run_persisted": False,
        "run_type": RUN_TYPE_REVIEW_QUEUE,
        "version_tag": REVIEW_QUEUE_VERSION_TAG,
        "status": status,
    }


def _failure_payload(
    *,
    request: ReviewQueueRequest,
    code: str,
    message: str,
) -> ReviewQueuePayload:
    return {
        "run": _run_payload("failed"),
        "request": request,
        "error": {
            "code": code,
            "message": message,
        },
    }


def _resolve_mode(
    *,
    top: Optional[int],
    page: Optional[int],
    page_size: Optional[int],
) -> _ResolvedMode:
    in_pagination_mode = page is not None or page_size is not None
    if in_pagination_mode:
        resolved_page = _PAGE_DEFAULT if page is None else max(page, 1)
        resolved_page_size = (
            _PAGE_SIZE_DEFAULT if page_size is None else max(page_size, 1)
        )
        return _ResolvedMode(
            slice_mode="page",
            top=None,
            page=resolved_page,
            page_size=resolved_page_size,
        )

    resolved_top = _TOP_N_DEFAULT if top is None else max(top, 1)
    return _ResolvedMode(
        slice_mode="top",
        top=resolved_top,
        page=None,
        page_size=None,
    )


def _normalize_parcel_ids(parcel_ids: Optional[Sequence[str]]) -> list[str]:
    if not parcel_ids:
        return []
    return sorted({parcel_id.strip() for parcel_id in parcel_ids if parcel_id.strip()})


def _normalize_risk_bands(risk_bands: Optional[Sequence[str]]) -> list[str]:
    if not risk_bands:
        return []
    normalized = {band.strip().lower() for band in risk_bands if band.strip()}
    supported = {band for band in normalized if band in RISK_BAND_PRECEDENCE}
    return sorted(supported, key=lambda value: RISK_BAND_PRECEDENCE[value])


def _unsupported_risk_bands(risk_bands: Optional[Sequence[str]]) -> list[str]:
    if not risk_bands:
        return []
    normalized = {band.strip().lower() for band in risk_bands if band.strip()}
    return sorted(band for band in normalized if band not in RISK_BAND_PRECEDENCE)


def _count_scores(
    session: Session,
    *,
    conditions: Sequence[ColumnElement[bool]],
) -> int:
    return int(
        session.execute(
            select(func.count(FraudScore.id)).where(*conditions)
        ).scalar_one()
    )


def _advance_filter_stage(
    session: Session,
    *,
    filtered_conditions: list[ColumnElement[bool]],
    previous_count: int,
    extra_condition: ColumnElement[bool],
    filtered_reason_counts: dict[str, int],
    reason_key: str,
) -> int:
    filtered_conditions.append(extra_condition)
    next_count = _count_scores(session, conditions=filtered_conditions)
    excluded_count = max(previous_count - next_count, 0)
    if excluded_count > 0:
        filtered_reason_counts[reason_key] = excluded_count
    return next_count


def _required_identifier_conditions() -> tuple[ColumnElement[bool], ...]:
    return (
        FraudScore.id.is_not(None),
        FraudScore.run_id.is_not(None),
        FraudScore.parcel_id.is_not(None),
        func.length(func.trim(FraudScore.parcel_id)) > 0,
        FraudScore.year >= 1,
    )


def _score_order_clauses() -> tuple[ColumnElement[Any], ...]:
    risk_band_rank = case(RISK_BAND_PRECEDENCE, value=FraudScore.risk_band, else_=99)
    return (
        FraudScore.score_value.desc(),
        FraudScore.reason_code_count.desc(),
        risk_band_rank.asc(),
        FraudScore.year.desc(),
        FraudScore.parcel_id.asc(),
        FraudScore.id.asc(),
    )


def _load_base_scores(
    session: Session,
    *,
    query: Optional[Select[tuple[FraudScore]]] = None,
    feature_version: Optional[str] = None,
    ruleset_version: Optional[str] = None,
) -> list[FraudScore]:
    if query is not None:
        return list(session.execute(query).scalars().all())

    if feature_version is None or ruleset_version is None:
        msg = (
            "feature_version and ruleset_version are required when query is not "
            "provided"
        )
        raise ValueError(msg)

    return list(
        session.execute(
            select(FraudScore)
            .where(
                FraudScore.feature_version == feature_version,
                FraudScore.ruleset_version == ruleset_version,
            )
            .order_by(FraudScore.id.asc())
        )
        .scalars()
        .all()
    )


def _load_primary_reason_map(
    session: Session,
    *,
    score_ids: Sequence[int],
) -> dict[int, tuple[Optional[str], Optional[Decimal]]]:
    if not score_ids:
        return {}

    primary_map: dict[int, tuple[Optional[str], Optional[Decimal]]] = {}
    for batch in _chunked(score_ids, _SCALAR_IN_CLAUSE_BATCH_SIZE):
        flags = (
            session.execute(
                select(FraudFlag)
                .where(FraudFlag.reason_rank == 1, FraudFlag.score_id.in_(batch))
                .order_by(FraudFlag.score_id.asc(), FraudFlag.id.asc())
            )
            .scalars()
            .all()
        )
        for flag in flags:
            if flag.score_id not in primary_map:
                primary_map[flag.score_id] = (flag.reason_code, flag.severity_weight)
    return primary_map


def _load_parcel_fact_map(
    session: Session,
    *,
    score_rows: Sequence[FraudScore],
) -> dict[tuple[str, int], ParcelYearFact]:
    keys = sorted({(row.parcel_id, row.year) for row in score_rows})
    if not keys:
        return {}

    fact_map: dict[tuple[str, int], ParcelYearFact] = {}
    for batch in _chunked(keys, _COMPOSITE_KEY_IN_CLAUSE_BATCH_SIZE):
        rows = (
            session.execute(
                select(ParcelYearFact).where(
                    tuple_(ParcelYearFact.parcel_id, ParcelYearFact.year).in_(batch)
                )
            )
            .scalars()
            .all()
        )
        for row in rows:
            fact_map[(row.parcel_id, row.year)] = row
    return fact_map


def _format_primary_reason_weight(
    *,
    primary_reason_code: Optional[str],
    severity_weight: Optional[Decimal],
) -> Optional[str]:
    if primary_reason_code is None:
        return None
    if severity_weight is None:
        return None
    return _decimal_to_str(severity_weight, scale=4)


def _total_pages(eligible_count: int, page_size: Optional[int]) -> Optional[int]:
    if page_size is None:
        return None
    if eligible_count == 0:
        return 0
    return (eligible_count + page_size - 1) // page_size


def _decimal_to_str(value: Decimal, *, scale: int) -> str:
    quant = Decimal("1").scaleb(-scale)
    quantized = value.quantize(quant, rounding=ROUND_HALF_UP)
    return format(quantized, "f")


def _chunked(items: Sequence, size: int) -> list[Sequence]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _csv_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
