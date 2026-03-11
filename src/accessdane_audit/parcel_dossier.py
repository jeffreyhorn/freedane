from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Callable, Iterable, Mapping, Optional, Sequence, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import FraudScore, ParcelFeature
from .parcel_dossier_queries import (
    AppealEventRow,
    AssessmentHistoryRow,
    MatchedSaleRow,
    PeerContextRow,
    PermitEventRow,
    ReasonCodeEvidenceRow,
    build_timeline_rows,
    get_parcel_header,
    list_appeal_events,
    list_assessment_history,
    list_matched_sales,
    list_peer_context,
    list_permit_events,
    list_reason_code_evidence,
)
from .score_fraud import SUPPORTED_RULESET_VERSIONS

RUN_TYPE_PARCEL_DOSSIER = "parcel_dossier"
PARCEL_DOSSIER_VERSION_TAG = "parcel_dossier_v1"
SECTION_ORDER = [
    "assessment_history",
    "matched_sales",
    "peer_context",
    "permit_events",
    "appeal_events",
    "reason_code_evidence",
]
_PERMIT_OPEN_STATUSES = {"applied", "issued"}
_PERMIT_CLOSED_STATUSES = {"finaled", "expired", "cancelled"}
_APPEAL_SUCCESSFUL_OUTCOMES = {"reduction_granted", "partial_reduction"}
_APPEAL_DENIED_OUTCOMES = {"denied"}
_T = TypeVar("_T")


def build_parcel_dossier(
    session: Session,
    *,
    parcel_id: str,
    years: Optional[Sequence[int]] = None,
    feature_version: str = "feature_v1",
    ruleset_version: str = "scoring_rules_v1",
) -> dict[str, object]:
    request_years = sorted(set(years)) if years is not None else []
    years_filter = request_years if years is not None else None
    request: dict[str, object] = {
        "parcel_id": parcel_id,
        "years": list(request_years),
        "feature_version": feature_version,
        "ruleset_version": ruleset_version,
    }

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
        parcel = get_parcel_header(
            session,
            parcel_id=parcel_id,
            years=years_filter,
        )
    except Exception as exc:
        session.rollback()
        return _failure_payload(
            request=request,
            code="source_query_error",
            message=str(exc),
        )

    if parcel is None:
        return _failure_payload(
            request=request,
            code="parcel_not_found",
            message=f"Parcel '{parcel_id}' was not found.",
        )

    warnings: list[str] = []
    unavailable_sections: list[str] = []

    assessment_rows = _load_rows(
        session,
        section_key="assessment_history",
        warnings=warnings,
        unavailable_sections=unavailable_sections,
        loader=lambda: list_assessment_history(
            session,
            parcel_id=parcel_id,
            years=years_filter,
        ),
    )
    matched_sales_rows = _load_rows(
        session,
        section_key="matched_sales",
        warnings=warnings,
        unavailable_sections=unavailable_sections,
        loader=lambda: list_matched_sales(
            session,
            parcel_id=parcel_id,
            years=years_filter,
        ),
    )
    peer_context_rows = _load_rows(
        session,
        section_key="peer_context",
        warnings=warnings,
        unavailable_sections=unavailable_sections,
        loader=lambda: list_peer_context(
            session,
            parcel_id=parcel_id,
            feature_version=feature_version,
            years=years_filter,
        ),
    )
    permit_event_rows = _load_rows(
        session,
        section_key="permit_events",
        warnings=warnings,
        unavailable_sections=unavailable_sections,
        loader=lambda: list_permit_events(
            session,
            parcel_id=parcel_id,
            years=years_filter,
        ),
    )
    appeal_event_rows = _load_rows(
        session,
        section_key="appeal_events",
        warnings=warnings,
        unavailable_sections=unavailable_sections,
        loader=lambda: list_appeal_events(
            session,
            parcel_id=parcel_id,
            years=years_filter,
        ),
    )
    reason_code_rows = _load_rows(
        session,
        section_key="reason_code_evidence",
        warnings=warnings,
        unavailable_sections=unavailable_sections,
        loader=lambda: list_reason_code_evidence(
            session,
            parcel_id=parcel_id,
            feature_version=feature_version,
            ruleset_version=ruleset_version,
            years=years_filter,
        ),
    )

    sections = {
        "assessment_history": _assessment_section(assessment_rows),
        "matched_sales": _matched_sales_section(matched_sales_rows),
        "peer_context": _peer_context_section(
            session,
            rows=peer_context_rows,
            parcel_id=parcel_id,
            years_filter=years_filter,
            unavailable_sections=unavailable_sections,
        ),
        "permit_events": _permit_events_section(permit_event_rows),
        "appeal_events": _appeal_events_section(appeal_event_rows),
        "reason_code_evidence": _reason_code_section(
            session,
            rows=reason_code_rows,
            parcel_id=parcel_id,
            years_filter=years_filter,
            unavailable_sections=unavailable_sections,
        ),
    }

    timeline_rows = build_timeline_rows(
        assessment_history=assessment_rows,
        matched_sales=matched_sales_rows,
        permit_events=permit_event_rows,
        appeal_events=appeal_event_rows,
        reason_code_evidence=reason_code_rows,
    )

    return {
        "run": _run_payload("succeeded"),
        "request": request,
        "parcel": _jsonify(parcel),
        "section_order": SECTION_ORDER,
        "sections": sections,
        "timeline": {
            "event_count": len(timeline_rows),
            "rows": _jsonify(timeline_rows),
        },
        "diagnostics": {
            "warnings": sorted(set(warnings)),
            "unavailable_sections": _ordered_unavailable_sections(unavailable_sections),
            "query_filters": {
                "years": request_years,
                "feature_version": feature_version,
                "ruleset_version": ruleset_version,
            },
        },
        "error": None,
    }


def _run_payload(status: str) -> dict[str, object]:
    return {
        "run_id": None,
        "run_persisted": False,
        "run_type": RUN_TYPE_PARCEL_DOSSIER,
        "version_tag": PARCEL_DOSSIER_VERSION_TAG,
        "status": status,
    }


def _failure_payload(
    *,
    request: Mapping[str, object],
    code: str,
    message: str,
) -> dict[str, object]:
    return {
        "run": _run_payload("failed"),
        "request": request,
        "error": {
            "code": code,
            "message": message,
        },
    }


def _load_rows(
    session: Session,
    *,
    section_key: str,
    warnings: list[str],
    unavailable_sections: list[str],
    loader: Callable[[], list[_T]],
) -> list[_T]:
    try:
        return list(loader())
    except Exception:
        session.rollback()
        warnings.append(f"{section_key}_query_error")
        unavailable_sections.append(section_key)
        return []


def _assessment_section(rows: list[AssessmentHistoryRow]) -> dict[str, object]:
    years = [row["year"] for row in rows if row["year"] is not None]
    summary: dict[str, object] = {
        "row_count": len(rows),
        "year_min": min(years) if years else None,
        "year_max": max(years) if years else None,
    }
    return _section_payload(
        rows=rows,
        summary=summary,
        empty_message="no_assessments_for_scope",
    )


def _matched_sales_section(rows: list[MatchedSaleRow]) -> dict[str, object]:
    summary: dict[str, object] = {
        "row_count": len(rows),
        "arms_length_true_count": sum(
            1 for row in rows if row["arms_length_indicator"] is True
        ),
        "usable_sale_true_count": sum(
            1 for row in rows if row["usable_sale_indicator"] is True
        ),
        "excluded_sale_count": sum(
            1 for row in rows if len(row["active_exclusion_codes"]) > 0
        ),
    }
    return _section_payload(
        rows=rows,
        summary=summary,
        empty_message="no_sales_for_scope",
    )


def _peer_context_section(
    session: Session,
    *,
    rows: list[PeerContextRow],
    parcel_id: str,
    years_filter: Optional[list[int]],
    unavailable_sections: list[str],
) -> dict[str, object]:
    summary: dict[str, object] = {
        "row_count": len(rows),
        "missing_ratio_count": sum(
            1 for row in rows if row["assessment_to_sale_ratio"] is None
        ),
        "missing_peer_percentile_count": sum(
            1 for row in rows if row["peer_percentile"] is None
        ),
        "quality_flagged_row_count": sum(
            1 for row in rows if len(row["feature_quality_flags"]) > 0
        ),
    }
    if rows:
        return _section_payload(rows=rows, summary=summary, empty_message="")

    has_feature_rows = _has_feature_rows_for_scope(
        session,
        parcel_id=parcel_id,
        years_filter=years_filter,
    )
    if has_feature_rows:
        unavailable_sections.append("peer_context")
        return _section_payload(
            rows=[],
            summary=summary,
            empty_message="missing_parcel_features_for_requested_version",
            unavailable=True,
        )

    return _section_payload(
        rows=[],
        summary=summary,
        empty_message="no_peer_context_for_scope",
    )


def _permit_events_section(rows: list[PermitEventRow]) -> dict[str, object]:
    summary: dict[str, object] = {
        "row_count": len(rows),
        "open_status_count": sum(
            1 for row in rows if row["permit_status"] in _PERMIT_OPEN_STATUSES
        ),
        "closed_status_count": sum(
            1 for row in rows if row["permit_status"] in _PERMIT_CLOSED_STATUSES
        ),
        "declared_valuation_known_count": sum(
            1 for row in rows if row["declared_valuation"] is not None
        ),
    }
    return _section_payload(
        rows=rows,
        summary=summary,
        empty_message="no_permits_for_scope",
    )


def _appeal_events_section(rows: list[AppealEventRow]) -> dict[str, object]:
    summary: dict[str, object] = {
        "row_count": len(rows),
        "successful_reduction_count": sum(
            1 for row in rows if row["outcome"] in _APPEAL_SUCCESSFUL_OUTCOMES
        ),
        "denied_or_no_change_count": sum(
            1 for row in rows if row["outcome"] in _APPEAL_DENIED_OUTCOMES
        ),
        "unknown_outcome_count": sum(
            1 for row in rows if row["outcome"] in (None, "unknown")
        ),
    }
    return _section_payload(
        rows=rows,
        summary=summary,
        empty_message="no_appeals_for_scope",
    )


def _reason_code_section(
    session: Session,
    *,
    rows: list[ReasonCodeEvidenceRow],
    parcel_id: str,
    years_filter: Optional[list[int]],
    unavailable_sections: list[str],
) -> dict[str, object]:
    summary: dict[str, object] = {
        "row_count": len(rows),
        "high_risk_count": sum(1 for row in rows if row["risk_band"] == "high"),
        "medium_risk_count": sum(1 for row in rows if row["risk_band"] == "medium"),
        "review_required_count": sum(1 for row in rows if row["requires_review"]),
    }
    if rows:
        output_rows = [_reason_code_row_for_output(row) for row in rows]
        return _section_payload(rows=output_rows, summary=summary, empty_message="")

    has_any_scores = _has_scores_for_scope(
        session,
        parcel_id=parcel_id,
        years_filter=years_filter,
    )
    if has_any_scores:
        unavailable_sections.append("reason_code_evidence")
        return _section_payload(
            rows=[],
            summary=summary,
            empty_message="missing_fraud_scores_for_requested_versions",
            unavailable=True,
        )

    return _section_payload(
        rows=[],
        summary=summary,
        empty_message="no_scores_for_scope",
    )


def _reason_code_row_for_output(row: ReasonCodeEvidenceRow) -> dict[str, object]:
    return {
        "score_id": row["score_id"],
        "run_id": row["run_id"],
        "feature_run_id": row["feature_run_id"],
        "year": row["year"],
        "score_value": row["score_value"],
        "risk_band": row["risk_band"],
        "requires_review": row["requires_review"],
        "reason_code_count": row["reason_code_count"],
        "reason_codes": row["reason_codes"],
    }


def _section_payload(
    *,
    rows: Sequence[object],
    summary: Mapping[str, object],
    empty_message: str,
    unavailable: bool = False,
) -> dict[str, object]:
    if unavailable:
        status = "unavailable"
        message: Optional[str] = empty_message
    elif rows:
        status = "populated"
        message = None
    else:
        status = "empty"
        message = empty_message
    return {
        "status": status,
        "summary": _jsonify(summary),
        "rows": _jsonify(rows),
        "message": message,
    }


def _has_feature_rows_for_scope(
    session: Session,
    *,
    parcel_id: str,
    years_filter: Optional[Iterable[int]],
) -> bool:
    query = select(ParcelFeature.id).where(ParcelFeature.parcel_id == parcel_id)
    if years_filter is not None:
        query = query.where(ParcelFeature.year.in_(years_filter))
    return session.execute(query.limit(1)).first() is not None


def _has_scores_for_scope(
    session: Session,
    *,
    parcel_id: str,
    years_filter: Optional[Iterable[int]],
) -> bool:
    query = select(FraudScore.id).where(FraudScore.parcel_id == parcel_id)
    if years_filter is not None:
        query = query.where(FraudScore.year.in_(years_filter))
    return session.execute(query.limit(1)).first() is not None


def _ordered_unavailable_sections(unavailable_sections: Sequence[str]) -> list[str]:
    available_set = set(unavailable_sections)
    return [key for key in SECTION_ORDER if key in available_set]


def _jsonify(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonify(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonify(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonify(item) for item in value]
    return value
