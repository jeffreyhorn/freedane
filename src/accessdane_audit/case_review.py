from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping, Optional, Sequence, TypedDict

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .models import CaseReview, FraudScore

RUN_TYPE_CASE_REVIEW = "case_review"
CASE_REVIEW_VERSION_TAG = "case_review_v1"
_SOURCE_QUERY_ERROR_MESSAGE = (
    "Failed to query source data while processing case review."
)

STATUS_PENDING = "pending"
STATUS_IN_REVIEW = "in_review"
STATUS_RESOLVED = "resolved"
STATUS_CLOSED = "closed"

STATUS_VALUES = (
    STATUS_PENDING,
    STATUS_IN_REVIEW,
    STATUS_RESOLVED,
    STATUS_CLOSED,
)

DISPOSITION_CONFIRMED_ISSUE = "confirmed_issue"
DISPOSITION_FALSE_POSITIVE = "false_positive"
DISPOSITION_INCONCLUSIVE = "inconclusive"
DISPOSITION_NEEDS_FIELD_REVIEW = "needs_field_review"
DISPOSITION_DUPLICATE_CASE = "duplicate_case"

DISPOSITION_VALUES = (
    DISPOSITION_CONFIRMED_ISSUE,
    DISPOSITION_FALSE_POSITIVE,
    DISPOSITION_INCONCLUSIVE,
    DISPOSITION_NEEDS_FIELD_REVIEW,
    DISPOSITION_DUPLICATE_CASE,
)

EVIDENCE_KIND_DOSSIER = "dossier"
EVIDENCE_KIND_QUEUE_ROW = "queue_row"
EVIDENCE_KIND_REASON_CODE = "reason_code"
EVIDENCE_KIND_EXTERNAL_DOC = "external_doc"
EVIDENCE_KIND_FIELD_VISIT = "field_visit"

EVIDENCE_KIND_VALUES = {
    EVIDENCE_KIND_DOSSIER,
    EVIDENCE_KIND_QUEUE_ROW,
    EVIDENCE_KIND_REASON_CODE,
    EVIDENCE_KIND_EXTERNAL_DOC,
    EVIDENCE_KIND_FIELD_VISIT,
}

_ALLOWED_TRANSITIONS = {
    (STATUS_PENDING, STATUS_IN_REVIEW),
    (STATUS_PENDING, STATUS_RESOLVED),
    (STATUS_IN_REVIEW, STATUS_PENDING),
    (STATUS_IN_REVIEW, STATUS_RESOLVED),
    (STATUS_RESOLVED, STATUS_IN_REVIEW),
    (STATUS_RESOLVED, STATUS_CLOSED),
    (STATUS_CLOSED, STATUS_IN_REVIEW),
}


class CaseReviewRun(TypedDict):
    run_id: Optional[int]
    run_persisted: bool
    run_type: str
    version_tag: str
    status: str


class CaseReviewError(TypedDict):
    code: str
    message: str


class CaseReviewPayload(TypedDict, total=False):
    run: CaseReviewRun
    request: dict[str, object]
    summary: dict[str, object]
    review: dict[str, object]
    reviews: list[dict[str, object]]
    diagnostics: dict[str, object]
    error: Optional[CaseReviewError]


def create_case_review(
    session: Session,
    *,
    score_id: int,
    status: str = STATUS_PENDING,
    disposition: Optional[str] = None,
    reviewer: Optional[str] = None,
    assigned_reviewer: Optional[str] = None,
    note: Optional[str] = None,
    evidence_links: Optional[Sequence[str]] = None,
) -> CaseReviewPayload:
    try:
        normalized_status = _normalize_status(status)
        normalized_disposition = _normalize_disposition(disposition)
        normalized_reviewer = _normalize_optional_string(reviewer)
        normalized_assigned_reviewer = _normalize_optional_string(assigned_reviewer)
        normalized_note = _normalize_optional_string(note)
        parsed_evidence_links = _normalize_evidence_link_inputs(evidence_links)
    except ValueError as exc:
        message = str(exc)
        code = (
            "invalid_evidence_link"
            if "evidence" in message.lower()
            else "invalid_disposition_for_status"
        )
        return _failure_payload(
            request={
                "score_id": score_id,
                "status": status,
                "disposition": disposition,
                "reviewer": reviewer,
                "assigned_reviewer": assigned_reviewer,
                "note": note,
                "feature_version": None,
                "ruleset_version": None,
                "evidence_links": list(evidence_links or []),
            },
            code=code,
            message=message,
        )

    if not _is_disposition_valid_for_status(
        status_value=normalized_status,
        disposition_value=normalized_disposition,
    ):
        return _failure_payload(
            request={
                "score_id": score_id,
                "status": normalized_status,
                "disposition": normalized_disposition,
                "reviewer": normalized_reviewer,
                "assigned_reviewer": normalized_assigned_reviewer,
                "note": normalized_note,
                "feature_version": None,
                "ruleset_version": None,
                "evidence_links": parsed_evidence_links,
            },
            code="invalid_disposition_for_status",
            message="Disposition is invalid for the requested status.",
        )

    if (
        normalized_status in {STATUS_RESOLVED, STATUS_CLOSED}
        and not parsed_evidence_links
    ):
        return _failure_payload(
            request={
                "score_id": score_id,
                "status": normalized_status,
                "disposition": normalized_disposition,
                "reviewer": normalized_reviewer,
                "assigned_reviewer": normalized_assigned_reviewer,
                "note": normalized_note,
                "feature_version": None,
                "ruleset_version": None,
                "evidence_links": parsed_evidence_links,
            },
            code="evidence_link_required",
            message=(
                "At least one evidence link is required for resolved/closed status."
            ),
        )

    try:
        score = session.execute(
            select(FraudScore).where(FraudScore.id == score_id)
        ).scalar_one_or_none()
    except Exception:
        session.rollback()
        return _failure_payload(
            request={
                "score_id": score_id,
                "status": normalized_status,
                "disposition": normalized_disposition,
                "reviewer": normalized_reviewer,
                "assigned_reviewer": normalized_assigned_reviewer,
                "note": normalized_note,
                "feature_version": None,
                "ruleset_version": None,
                "evidence_links": parsed_evidence_links,
            },
            code="source_query_error",
            message=_SOURCE_QUERY_ERROR_MESSAGE,
        )

    if score is None:
        return _failure_payload(
            request={
                "score_id": score_id,
                "status": normalized_status,
                "disposition": normalized_disposition,
                "reviewer": normalized_reviewer,
                "assigned_reviewer": normalized_assigned_reviewer,
                "note": normalized_note,
                "feature_version": None,
                "ruleset_version": None,
                "evidence_links": parsed_evidence_links,
            },
            code="score_not_found",
            message=f"Score id {score_id} was not found.",
        )

    request = {
        "score_id": score_id,
        "status": normalized_status,
        "disposition": normalized_disposition,
        "reviewer": normalized_reviewer,
        "assigned_reviewer": normalized_assigned_reviewer,
        "note": normalized_note,
        "feature_version": score.feature_version,
        "ruleset_version": score.ruleset_version,
        "evidence_links": parsed_evidence_links,
    }

    try:
        existing = session.execute(
            select(CaseReview).where(
                CaseReview.score_id == score.id,
                CaseReview.feature_version == score.feature_version,
                CaseReview.ruleset_version == score.ruleset_version,
            )
        ).scalar_one_or_none()
    except Exception:
        session.rollback()
        return _failure_payload(
            request=request,
            code="source_query_error",
            message=_SOURCE_QUERY_ERROR_MESSAGE,
        )

    if existing is not None:
        if (
            existing.parcel_id != score.parcel_id
            or existing.year != score.year
            or existing.run_id != score.run_id
        ):
            return _failure_payload(
                request=request,
                code="score_context_mismatch",
                message=(
                    "Existing case review linkage does not match the referenced "
                    "score context."
                ),
            )

        try:
            incoming_canonical = _canonical_client_fields(
                status=normalized_status,
                disposition=normalized_disposition,
                reviewer=normalized_reviewer,
                assigned_reviewer=normalized_assigned_reviewer,
                note=normalized_note,
                evidence_links=parsed_evidence_links,
            )
            existing_canonical = _canonical_client_fields(
                status=existing.status,
                disposition=existing.disposition,
                reviewer=existing.reviewer,
                assigned_reviewer=existing.assigned_reviewer,
                note=existing.note,
                evidence_links=_normalize_evidence_link_objects(
                    existing.evidence_links_json
                ),
            )
        except ValueError:
            return _failure_payload(
                request=request,
                code="source_query_error",
                message=_SOURCE_QUERY_ERROR_MESSAGE,
            )

        if incoming_canonical == existing_canonical:
            return {
                "run": _run_payload("succeeded"),
                "request": request,
                "review": _review_payload(existing, created=False),
                "diagnostics": {
                    "warnings": [],
                    "normalization": {
                        "evidence_links_count": len(parsed_evidence_links),
                    },
                },
                "error": None,
            }

        return _failure_payload(
            request=request,
            code="duplicate_case_review",
            message=(
                "A case review already exists for this score context with different "
                "client-controlled fields."
            ),
        )

    now = datetime.now(timezone.utc)
    reviewed_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    if normalized_status in {STATUS_RESOLVED, STATUS_CLOSED}:
        reviewed_at = now
    if normalized_status == STATUS_CLOSED:
        closed_at = now

    row = CaseReview(
        parcel_id=score.parcel_id,
        year=score.year,
        score_id=score.id,
        run_id=score.run_id,
        feature_version=score.feature_version,
        ruleset_version=score.ruleset_version,
        status=normalized_status,
        disposition=normalized_disposition,
        reviewer=normalized_reviewer,
        assigned_reviewer=normalized_assigned_reviewer,
        note=normalized_note,
        evidence_links_json=parsed_evidence_links,
        reviewed_at=reviewed_at,
        closed_at=closed_at,
    )

    try:
        session.add(row)
        session.flush()
    except Exception:
        session.rollback()
        return _failure_payload(
            request=request,
            code="source_query_error",
            message=_SOURCE_QUERY_ERROR_MESSAGE,
        )

    return {
        "run": _run_payload("succeeded"),
        "request": request,
        "review": _review_payload(row, created=True),
        "diagnostics": {
            "warnings": [],
            "normalization": {
                "evidence_links_count": len(parsed_evidence_links),
            },
        },
        "error": None,
    }


def update_case_review(
    session: Session,
    *,
    case_review_id: int,
    status: Optional[str] = None,
    disposition: Optional[str] = None,
    reviewer: Optional[str] = None,
    assigned_reviewer: Optional[str] = None,
    note: Optional[str] = None,
    set_evidence_links: Optional[Sequence[str]] = None,
    clear_evidence_links: bool = False,
) -> CaseReviewPayload:
    if clear_evidence_links and set_evidence_links is not None:
        return _failure_payload(
            request={"id": case_review_id, "patch": {}},
            code="invalid_evidence_link",
            message=(
                "Cannot combine set_evidence_links with clear_evidence_links in one "
                "patch."
            ),
        )

    try:
        patch: dict[str, object] = {}
        if status is not None:
            patch["status"] = _normalize_status(status)
        if disposition is not None:
            patch["disposition"] = _normalize_disposition(disposition)
        if reviewer is not None:
            patch["reviewer"] = _normalize_optional_string(reviewer)
        if assigned_reviewer is not None:
            patch["assigned_reviewer"] = _normalize_optional_string(assigned_reviewer)
        if note is not None:
            patch["note"] = _normalize_optional_string(note)

        if clear_evidence_links:
            patch["evidence_links"] = []
        elif set_evidence_links is not None:
            patch["evidence_links"] = _normalize_evidence_link_inputs(
                set_evidence_links
            )
    except ValueError as exc:
        message = str(exc)
        code = (
            "invalid_evidence_link"
            if "evidence" in message.lower()
            else "invalid_disposition_for_status"
        )
        return _failure_payload(
            request={"id": case_review_id, "patch": {}},
            code=code,
            message=message,
        )

    request = {
        "id": case_review_id,
        "patch": patch,
    }

    try:
        row = session.execute(
            select(CaseReview).where(CaseReview.id == case_review_id)
        ).scalar_one_or_none()
    except Exception:
        session.rollback()
        return _failure_payload(
            request=request,
            code="source_query_error",
            message=_SOURCE_QUERY_ERROR_MESSAGE,
        )

    if row is None:
        return _failure_payload(
            request=request,
            code="case_review_not_found",
            message=f"Case review id {case_review_id} was not found.",
        )

    try:
        current_evidence_links = _normalize_evidence_link_objects(
            row.evidence_links_json
        )
    except ValueError:
        return _failure_payload(
            request=request,
            code="source_query_error",
            message=_SOURCE_QUERY_ERROR_MESSAGE,
        )
    next_status = patch.get("status", row.status)
    if not isinstance(next_status, str):
        next_status = row.status

    status_changed = next_status != row.status
    if status_changed and (row.status, next_status) not in _ALLOWED_TRANSITIONS:
        return _failure_payload(
            request=request,
            code="invalid_transition",
            message=(
                f"Transition '{row.status}' -> '{next_status}' is not allowed in v1."
            ),
        )

    next_disposition = patch.get("disposition", row.disposition)
    if status_changed and next_status in {STATUS_PENDING, STATUS_IN_REVIEW}:
        # Reopen and in-review transitions clear disposition by contract.
        next_disposition = None

    if not _is_disposition_valid_for_status(
        status_value=next_status,
        disposition_value=(
            str(next_disposition) if next_disposition is not None else None
        ),
    ):
        return _failure_payload(
            request=request,
            code="invalid_disposition_for_status",
            message="Disposition is invalid for the resulting status.",
        )

    if "evidence_links" in patch:
        next_evidence_links = patch["evidence_links"]
        assert isinstance(next_evidence_links, list)
    else:
        next_evidence_links = current_evidence_links

    if next_status in {STATUS_RESOLVED, STATUS_CLOSED} and not next_evidence_links:
        return _failure_payload(
            request=request,
            code="evidence_link_required",
            message=(
                "At least one evidence link is required for resolved/closed status."
            ),
        )

    next_reviewer = patch["reviewer"] if "reviewer" in patch else row.reviewer
    next_assigned_reviewer = (
        patch["assigned_reviewer"]
        if "assigned_reviewer" in patch
        else row.assigned_reviewer
    )
    next_note = patch["note"] if "note" in patch else row.note

    assert next_reviewer is None or isinstance(next_reviewer, str)
    assert next_assigned_reviewer is None or isinstance(next_assigned_reviewer, str)
    assert next_note is None or isinstance(next_note, str)
    assert next_disposition is None or isinstance(next_disposition, str)

    next_reviewed_at = row.reviewed_at
    next_closed_at = row.closed_at
    now = datetime.now(timezone.utc)

    if status_changed:
        if next_status in {STATUS_RESOLVED, STATUS_CLOSED} and next_reviewed_at is None:
            next_reviewed_at = now
        if next_status == STATUS_CLOSED:
            next_closed_at = now
        elif row.status == STATUS_CLOSED and next_status == STATUS_IN_REVIEW:
            next_closed_at = None

    changed = any(
        [
            next_status != row.status,
            next_disposition != row.disposition,
            next_reviewer != row.reviewer,
            next_assigned_reviewer != row.assigned_reviewer,
            next_note != row.note,
            next_evidence_links != current_evidence_links,
            next_reviewed_at != row.reviewed_at,
            next_closed_at != row.closed_at,
        ]
    )

    if not changed:
        return {
            "run": _run_payload("succeeded"),
            "request": request,
            "review": _review_payload(row, updated=False),
            "diagnostics": {
                "warnings": [],
                "normalization": {
                    "evidence_links_count": len(current_evidence_links),
                },
            },
            "error": None,
        }

    row.status = next_status
    row.disposition = next_disposition
    row.reviewer = next_reviewer
    row.assigned_reviewer = next_assigned_reviewer
    row.note = next_note
    row.evidence_links_json = next_evidence_links
    row.reviewed_at = next_reviewed_at
    row.closed_at = next_closed_at

    try:
        session.flush()
    except Exception:
        session.rollback()
        return _failure_payload(
            request=request,
            code="source_query_error",
            message=_SOURCE_QUERY_ERROR_MESSAGE,
        )

    return {
        "run": _run_payload("succeeded"),
        "request": request,
        "review": _review_payload(row, updated=True),
        "diagnostics": {
            "warnings": [],
            "normalization": {
                "evidence_links_count": len(next_evidence_links),
            },
        },
        "error": None,
    }


def list_case_reviews(
    session: Session,
    *,
    statuses: Optional[Sequence[str]] = None,
    dispositions: Optional[Sequence[str]] = None,
    reviewer: Optional[str] = None,
    assigned_reviewer: Optional[str] = None,
    parcel_id: Optional[str] = None,
    years: Optional[Sequence[int]] = None,
    feature_version: str = "feature_v1",
    ruleset_version: str = "scoring_rules_v1",
    limit: int = 100,
    offset: int = 0,
) -> CaseReviewPayload:
    normalized_statuses = _normalize_string_sequence(statuses)
    normalized_dispositions = _normalize_string_sequence(dispositions)
    normalized_years = sorted(set(years or []))
    normalized_reviewer = _normalize_optional_string(reviewer)
    normalized_assigned_reviewer = _normalize_optional_string(assigned_reviewer)
    normalized_parcel_id = _normalize_optional_string(parcel_id)

    normalized_limit = max(1, min(limit, 1000))
    normalized_offset = max(offset, 0)

    request: dict[str, object] = {
        "statuses": normalized_statuses,
        "dispositions": normalized_dispositions,
        "years": normalized_years,
        "reviewer": normalized_reviewer,
        "assigned_reviewer": normalized_assigned_reviewer,
        "parcel_id": normalized_parcel_id,
        "feature_version": feature_version,
        "ruleset_version": ruleset_version,
        "limit": normalized_limit,
        "offset": normalized_offset,
    }

    try:
        query = select(CaseReview).where(
            CaseReview.feature_version == feature_version,
            CaseReview.ruleset_version == ruleset_version,
        )
        if normalized_statuses:
            query = query.where(CaseReview.status.in_(normalized_statuses))
        if normalized_dispositions:
            query = query.where(CaseReview.disposition.in_(normalized_dispositions))
        if normalized_years:
            query = query.where(CaseReview.year.in_(normalized_years))
        if normalized_reviewer is not None:
            query = query.where(CaseReview.reviewer == normalized_reviewer)
        if normalized_assigned_reviewer is not None:
            query = query.where(
                CaseReview.assigned_reviewer == normalized_assigned_reviewer
            )
        if normalized_parcel_id is not None:
            query = query.where(CaseReview.parcel_id == normalized_parcel_id)

        total = session.execute(
            select(func.count()).select_from(query.subquery())
        ).scalar_one()

        rows = (
            session.execute(
                query.order_by(CaseReview.updated_at.desc(), CaseReview.id.desc())
                .offset(normalized_offset)
                .limit(normalized_limit)
            )
            .scalars()
            .all()
        )
    except Exception:
        session.rollback()
        return _failure_payload(
            request=request,
            code="source_query_error",
            message=_SOURCE_QUERY_ERROR_MESSAGE,
        )

    reviews = [_review_payload(row) for row in rows]

    return {
        "run": _run_payload("succeeded"),
        "request": request,
        "summary": {
            "total": int(total),
            "limit": normalized_limit,
            "offset": normalized_offset,
            "returned": len(reviews),
        },
        "reviews": reviews,
        "diagnostics": {
            "warnings": [],
            "normalization": {
                "status_filter_count": len(normalized_statuses),
                "disposition_filter_count": len(normalized_dispositions),
                "year_filter_count": len(normalized_years),
            },
        },
        "error": None,
    }


def _normalize_status(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in STATUS_VALUES:
        raise ValueError(f"Unsupported status '{value}'.")
    return normalized


def _normalize_disposition(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized not in DISPOSITION_VALUES:
        raise ValueError(f"Unsupported disposition '{value}'.")
    return normalized


def _normalize_optional_string(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def _normalize_string_sequence(values: Optional[Sequence[str]]) -> list[str]:
    normalized = sorted(
        {value.strip().lower() for value in values or [] if value.strip()}
    )
    return normalized


def _normalize_evidence_link_inputs(
    values: Optional[Sequence[str]],
) -> list[dict[str, object]]:
    raw_values = values or []
    tuples: set[tuple[str, str, Optional[str]]] = set()
    for raw in raw_values:
        tuples.add(_parse_evidence_link_input(raw))
    return _canonicalize_evidence_tuples(tuples)


def _normalize_evidence_link_objects(values: object) -> list[dict[str, object]]:
    if values is None:
        return []
    if not isinstance(values, list):
        raise ValueError("Evidence links must be a list.")

    tuples: set[tuple[str, str, Optional[str]]] = set()
    for item in values:
        if not isinstance(item, Mapping):
            raise ValueError("Evidence link entries must be objects.")
        kind = _normalize_optional_string(_coerce_str(item.get("kind")))
        ref = _normalize_optional_string(_coerce_str(item.get("ref")))
        label = _normalize_optional_string(_coerce_str(item.get("label")))
        if kind is None or ref is None:
            raise ValueError("Evidence links require non-empty kind and ref.")
        if kind not in EVIDENCE_KIND_VALUES:
            raise ValueError(f"Unsupported evidence link kind '{kind}'.")
        tuples.add((kind, ref, label))

    return _canonicalize_evidence_tuples(tuples)


def _coerce_str(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _parse_evidence_link_input(value: str) -> tuple[str, str, Optional[str]]:
    text = value.strip()
    if not text:
        raise ValueError("Evidence link cannot be empty.")

    pairs: dict[str, str] = {}
    for chunk in text.split(","):
        part = chunk.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError(
                "Evidence link must use kind=...,ref=...,label=... format."
            )
        key, raw_val = part.split("=", 1)
        normalized_key = key.strip().lower()
        if normalized_key not in {"kind", "ref", "label"}:
            raise ValueError(f"Unsupported evidence link key '{normalized_key}'.")
        if normalized_key in pairs:
            raise ValueError(f"Duplicate evidence link key '{normalized_key}'.")
        pairs[normalized_key] = raw_val

    kind = _normalize_optional_string(pairs.get("kind"))
    ref = _normalize_optional_string(pairs.get("ref"))
    label = _normalize_optional_string(pairs.get("label"))

    if kind is None or ref is None:
        raise ValueError("Evidence links require non-empty kind and ref.")
    if kind not in EVIDENCE_KIND_VALUES:
        raise ValueError(f"Unsupported evidence link kind '{kind}'.")

    return (kind, ref, label)


def _canonicalize_evidence_tuples(
    tuples: set[tuple[str, str, Optional[str]]],
) -> list[dict[str, object]]:
    sorted_tuples = sorted(
        tuples,
        key=lambda item: (item[0], item[1], "" if item[2] is None else item[2]),
    )
    return [
        {
            "kind": kind,
            "ref": ref,
            "label": label,
        }
        for (kind, ref, label) in sorted_tuples
    ]


def _is_disposition_valid_for_status(
    *,
    status_value: str,
    disposition_value: Optional[str],
) -> bool:
    if status_value in {STATUS_PENDING, STATUS_IN_REVIEW}:
        return disposition_value is None
    return disposition_value in DISPOSITION_VALUES


def _canonical_client_fields(
    *,
    status: str,
    disposition: Optional[str],
    reviewer: Optional[str],
    assigned_reviewer: Optional[str],
    note: Optional[str],
    evidence_links: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "status": _normalize_status(status),
        "disposition": _normalize_disposition(disposition),
        "reviewer": _normalize_optional_string(reviewer),
        "assigned_reviewer": _normalize_optional_string(assigned_reviewer),
        "note": _normalize_optional_string(note),
        "evidence_links": _normalize_evidence_link_objects(evidence_links),
    }


def _run_payload(status: str) -> CaseReviewRun:
    return {
        "run_id": None,
        "run_persisted": False,
        "run_type": RUN_TYPE_CASE_REVIEW,
        "version_tag": CASE_REVIEW_VERSION_TAG,
        "status": status,
    }


def _failure_payload(
    *,
    request: Mapping[str, object],
    code: str,
    message: str,
) -> CaseReviewPayload:
    return {
        "run": _run_payload("failed"),
        "request": dict(request),
        "error": {
            "code": code,
            "message": message,
        },
    }


def _review_payload(
    row: CaseReview,
    *,
    created: Optional[bool] = None,
    updated: Optional[bool] = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": row.id,
        "parcel_id": row.parcel_id,
        "year": row.year,
        "score_id": row.score_id,
        "run_id": row.run_id,
        "feature_version": row.feature_version,
        "ruleset_version": row.ruleset_version,
        "status": row.status,
        "disposition": row.disposition,
        "reviewer": row.reviewer,
        "assigned_reviewer": row.assigned_reviewer,
        "note": row.note,
        "evidence_links": _normalize_evidence_link_objects(row.evidence_links_json),
        "created_at": _iso_datetime(row.created_at),
        "updated_at": _iso_datetime(row.updated_at),
        "reviewed_at": _iso_datetime(row.reviewed_at),
        "closed_at": _iso_datetime(row.closed_at),
    }
    if created is not None:
        payload["created"] = created
    if updated is not None:
        payload["updated"] = updated
    return payload


def _iso_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()
