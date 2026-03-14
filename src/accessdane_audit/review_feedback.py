from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from typing import Mapping, Optional, Sequence, TypedDict

from sqlalchemy import select
from sqlalchemy.orm import Session

from .case_review import (
    DISPOSITION_FALSE_POSITIVE,
    DISPOSITION_VALUES,
    STATUS_CLOSED,
    STATUS_RESOLVED,
)
from .models import CaseReview, FraudFlag, FraudScore
from .review_queue import RISK_BAND_PRECEDENCE
from .score_fraud import SUPPORTED_RULESET_VERSIONS

RUN_TYPE_REVIEW_FEEDBACK = "review_feedback"
REVIEW_FEEDBACK_VERSION_TAG = "review_feedback_v1"
REVIEW_FEEDBACK_SQL_VERSION = "review_feedback_sql_v1"
_SOURCE_QUERY_ERROR_MESSAGE = (
    "Failed to query source data while building review feedback."
)
_FALSE_POSITIVE_RATE_THRESHOLD = Decimal("0.5000")
_EXCLUSION_CANDIDATE_RATE_THRESHOLD = Decimal("0.7500")
_MIN_REVIEWED_CASES_FOR_RECOMMENDATION = 2
_SCALAR_IN_CLAUSE_BATCH_SIZE = 800


class ReviewFeedbackRun(TypedDict):
    run_id: Optional[int]
    run_persisted: bool
    run_type: str
    version_tag: str
    status: str


class ReviewFeedbackRequest(TypedDict):
    feature_version: str
    ruleset_version: str


class ReviewFeedbackError(TypedDict):
    code: str
    message: str


class ReviewFeedbackPayload(TypedDict, total=False):
    run: ReviewFeedbackRun
    request: ReviewFeedbackRequest
    summary: dict[str, object]
    risk_band_outcomes: list[dict[str, object]]
    reason_code_outcomes: list[dict[str, object]]
    rule_outcome_slices: list[dict[str, object]]
    recommendations: dict[str, object]
    artifacts: dict[str, object]
    diagnostics: dict[str, object]
    error: Optional[ReviewFeedbackError]


@dataclass(frozen=True)
class _ReviewedCase:
    score_id: int
    risk_band: str
    score_value: Decimal
    disposition: str


@dataclass(frozen=True)
class _ReasonFlag:
    reason_code: str
    metric_name: str
    comparison_operator: str
    threshold_value: str


@dataclass
class _OutcomeAccumulator:
    reviewed_case_count: int = 0
    false_positive_count: int = 0
    disposition_counts: Counter[str] = field(default_factory=Counter)
    score_values: list[Decimal] = field(default_factory=list)
    threshold_signatures: set[tuple[str, str, str]] = field(default_factory=set)


def build_review_feedback(
    session: Session,
    *,
    feature_version: str = "feature_v1",
    ruleset_version: str = "scoring_rules_v1",
) -> ReviewFeedbackPayload:
    request: ReviewFeedbackRequest = {
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
        reviewed_cases = _load_reviewed_cases(
            session,
            feature_version=feature_version,
            ruleset_version=ruleset_version,
        )
        flags_by_score = _load_reason_flags_by_score(
            session,
            score_ids=[case.score_id for case in reviewed_cases],
        )
    except Exception:
        session.rollback()
        return _failure_payload(
            request=request,
            code="source_query_error",
            message=_SOURCE_QUERY_ERROR_MESSAGE,
        )

    risk_band_outcomes = _build_risk_band_outcomes(reviewed_cases)
    reason_code_outcomes = _build_reason_code_outcomes(reviewed_cases, flags_by_score)
    rule_outcome_slices = _build_rule_outcome_slices(reviewed_cases, flags_by_score)
    recommendations = _build_recommendations(reason_code_outcomes)
    sql_queries = _build_sql_queries(
        feature_version=feature_version,
        ruleset_version=ruleset_version,
    )

    summary: dict[str, object] = {
        "reviewed_case_count": len(reviewed_cases),
        "false_positive_count": sum(
            1
            for case in reviewed_cases
            if case.disposition == DISPOSITION_FALSE_POSITIVE
        ),
        "risk_band_bucket_count": len(risk_band_outcomes),
        "reason_code_bucket_count": len(reason_code_outcomes),
        "rule_outcome_slice_count": len(rule_outcome_slices),
    }

    return {
        "run": _run_payload("succeeded"),
        "request": request,
        "summary": summary,
        "risk_band_outcomes": risk_band_outcomes,
        "reason_code_outcomes": reason_code_outcomes,
        "rule_outcome_slices": rule_outcome_slices,
        "recommendations": recommendations,
        "artifacts": {
            "sql_contract_version": REVIEW_FEEDBACK_SQL_VERSION,
            "sql_queries": sql_queries,
            "sql_script": _sql_script(sql_queries),
        },
        "diagnostics": {
            "warnings": [],
            "normalization": {
                "dispositions": sorted(DISPOSITION_VALUES),
                "review_statuses": [STATUS_RESOLVED, STATUS_CLOSED],
            },
        },
        "error": None,
    }


def _run_payload(status: str) -> ReviewFeedbackRun:
    return {
        "run_id": None,
        "run_persisted": False,
        "run_type": RUN_TYPE_REVIEW_FEEDBACK,
        "version_tag": REVIEW_FEEDBACK_VERSION_TAG,
        "status": status,
    }


def _failure_payload(
    *,
    request: ReviewFeedbackRequest,
    code: str,
    message: str,
) -> ReviewFeedbackPayload:
    return {
        "run": _run_payload("failed"),
        "request": request,
        "error": {
            "code": code,
            "message": message,
        },
    }


def _load_reviewed_cases(
    session: Session,
    *,
    feature_version: str,
    ruleset_version: str,
) -> list[_ReviewedCase]:
    rows = session.execute(
        select(CaseReview, FraudScore)
        .join(FraudScore, FraudScore.id == CaseReview.score_id)
        .where(
            CaseReview.feature_version == feature_version,
            CaseReview.ruleset_version == ruleset_version,
            FraudScore.feature_version == feature_version,
            FraudScore.ruleset_version == ruleset_version,
            CaseReview.status.in_([STATUS_RESOLVED, STATUS_CLOSED]),
            CaseReview.disposition.is_not(None),
        )
        .order_by(FraudScore.id.asc())
    ).all()

    reviewed_cases: list[_ReviewedCase] = []
    for case_review, score in rows:
        if case_review.disposition is None:
            continue
        reviewed_cases.append(
            _ReviewedCase(
                score_id=score.id,
                risk_band=score.risk_band,
                score_value=score.score_value,
                disposition=case_review.disposition,
            )
        )
    return reviewed_cases


def _load_reason_flags_by_score(
    session: Session,
    *,
    score_ids: Sequence[int],
) -> dict[int, list[_ReasonFlag]]:
    if not score_ids:
        return {}

    flags_by_score: dict[int, list[_ReasonFlag]] = defaultdict(list)
    unique_score_ids = sorted(set(score_ids))
    for batch in _chunked(unique_score_ids, _SCALAR_IN_CLAUSE_BATCH_SIZE):
        flags = (
            session.execute(
                select(FraudFlag)
                .where(FraudFlag.score_id.in_(batch))
                .order_by(
                    FraudFlag.score_id.asc(),
                    FraudFlag.reason_rank.asc(),
                    FraudFlag.reason_code.asc(),
                    FraudFlag.id.asc(),
                )
            )
            .scalars()
            .all()
        )
        for flag in flags:
            flags_by_score[flag.score_id].append(
                _ReasonFlag(
                    reason_code=flag.reason_code,
                    metric_name=flag.metric_name,
                    comparison_operator=flag.comparison_operator,
                    threshold_value=flag.threshold_value,
                )
            )
    return dict(flags_by_score)


def _build_risk_band_outcomes(
    reviewed_cases: Sequence[_ReviewedCase],
) -> list[dict[str, object]]:
    by_band: dict[str, _OutcomeAccumulator] = defaultdict(_OutcomeAccumulator)
    for case in reviewed_cases:
        acc = by_band[case.risk_band]
        acc.reviewed_case_count += 1
        if case.disposition == DISPOSITION_FALSE_POSITIVE:
            acc.false_positive_count += 1
        acc.disposition_counts[case.disposition] += 1
        acc.score_values.append(case.score_value)

    ordered_bands = sorted(
        by_band,
        key=lambda value: (RISK_BAND_PRECEDENCE.get(value, 99), value),
    )
    return [
        {
            "risk_band": band,
            "reviewed_case_count": acc.reviewed_case_count,
            "false_positive_count": acc.false_positive_count,
            "false_positive_rate": _ratio_str(
                acc.false_positive_count,
                acc.reviewed_case_count,
            ),
            "disposition_counts": _ordered_disposition_counts(acc.disposition_counts),
            "median_score_value": _median_decimal_str(acc.score_values, scale=2),
        }
        for band in ordered_bands
        for acc in [by_band[band]]
    ]


def _build_reason_code_outcomes(
    reviewed_cases: Sequence[_ReviewedCase],
    flags_by_score: Mapping[int, Sequence[_ReasonFlag]],
) -> list[dict[str, object]]:
    by_reason: dict[str, _OutcomeAccumulator] = defaultdict(_OutcomeAccumulator)
    by_reason_band: dict[tuple[str, str], _OutcomeAccumulator] = defaultdict(
        _OutcomeAccumulator
    )

    for case in reviewed_cases:
        flags = list(flags_by_score.get(case.score_id, []))
        if not flags:
            continue
        reason_codes = sorted({flag.reason_code for flag in flags})
        for reason_code in reason_codes:
            reason_flags = [flag for flag in flags if flag.reason_code == reason_code]
            _update_outcome_accumulator(
                by_reason[reason_code],
                disposition=case.disposition,
                score_value=case.score_value,
                threshold_signatures=[
                    (
                        reason_flag.metric_name,
                        reason_flag.comparison_operator,
                        reason_flag.threshold_value,
                    )
                    for reason_flag in reason_flags
                ],
            )
            _update_outcome_accumulator(
                by_reason_band[(reason_code, case.risk_band)],
                disposition=case.disposition,
                score_value=case.score_value,
                threshold_signatures=[
                    (
                        reason_flag.metric_name,
                        reason_flag.comparison_operator,
                        reason_flag.threshold_value,
                    )
                    for reason_flag in reason_flags
                ],
            )

    outcomes: list[dict[str, object]] = []
    for reason_code in sorted(
        by_reason,
        key=lambda value: (-by_reason[value].reviewed_case_count, value),
    ):
        acc = by_reason[reason_code]
        risk_breakdown = []
        reason_slices = [
            (risk_band, slice_acc)
            for (slice_reason, risk_band), slice_acc in by_reason_band.items()
            if slice_reason == reason_code
        ]
        reason_slices.sort(
            key=lambda item: (RISK_BAND_PRECEDENCE.get(item[0], 99), item[0])
        )
        for risk_band, slice_acc in reason_slices:
            risk_breakdown.append(
                {
                    "risk_band": risk_band,
                    "reviewed_case_count": slice_acc.reviewed_case_count,
                    "false_positive_count": slice_acc.false_positive_count,
                    "false_positive_rate": _ratio_str(
                        slice_acc.false_positive_count,
                        slice_acc.reviewed_case_count,
                    ),
                }
            )

        outcomes.append(
            {
                "reason_code": reason_code,
                "reviewed_case_count": acc.reviewed_case_count,
                "false_positive_count": acc.false_positive_count,
                "false_positive_rate": _ratio_str(
                    acc.false_positive_count,
                    acc.reviewed_case_count,
                ),
                "disposition_counts": _ordered_disposition_counts(
                    acc.disposition_counts
                ),
                "median_score_value": _median_decimal_str(acc.score_values, scale=2),
                "threshold_signatures": _ordered_threshold_signatures(
                    acc.threshold_signatures
                ),
                "risk_band_breakdown": risk_breakdown,
            }
        )

    return outcomes


def _build_rule_outcome_slices(
    reviewed_cases: Sequence[_ReviewedCase],
    flags_by_score: Mapping[int, Sequence[_ReasonFlag]],
) -> list[dict[str, object]]:
    by_slice: dict[tuple[str, str], _OutcomeAccumulator] = defaultdict(
        _OutcomeAccumulator
    )

    for case in reviewed_cases:
        flags = list(flags_by_score.get(case.score_id, []))
        if not flags:
            continue
        reason_codes = sorted({flag.reason_code for flag in flags})
        for reason_code in reason_codes:
            reason_flags = [flag for flag in flags if flag.reason_code == reason_code]
            _update_outcome_accumulator(
                by_slice[(reason_code, case.risk_band)],
                disposition=case.disposition,
                score_value=case.score_value,
                threshold_signatures=[
                    (
                        reason_flag.metric_name,
                        reason_flag.comparison_operator,
                        reason_flag.threshold_value,
                    )
                    for reason_flag in reason_flags
                ],
            )

    ordered_keys = sorted(
        by_slice,
        key=lambda item: (item[0], RISK_BAND_PRECEDENCE.get(item[1], 99), item[1]),
    )
    return [
        {
            "reason_code": reason_code,
            "risk_band": risk_band,
            "reviewed_case_count": acc.reviewed_case_count,
            "false_positive_count": acc.false_positive_count,
            "false_positive_rate": _ratio_str(
                acc.false_positive_count,
                acc.reviewed_case_count,
            ),
            "disposition_counts": _ordered_disposition_counts(acc.disposition_counts),
            "median_score_value": _median_decimal_str(acc.score_values, scale=2),
            "threshold_signatures": _ordered_threshold_signatures(
                acc.threshold_signatures
            ),
        }
        for reason_code, risk_band in ordered_keys
        for acc in [by_slice[(reason_code, risk_band)]]
    ]


def _build_recommendations(
    reason_code_outcomes: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    threshold_candidates: list[dict[str, object]] = []
    exclusion_candidates: list[dict[str, object]] = []

    for outcome in reason_code_outcomes:
        reviewed_case_count = _coerce_int(outcome.get("reviewed_case_count"), default=0)
        false_positive_rate = Decimal(str(outcome.get("false_positive_rate", "0")))
        disposition_counts = outcome.get("disposition_counts")
        confirmed_issue_count = 0
        if isinstance(disposition_counts, dict):
            confirmed_issue_count = _coerce_int(
                disposition_counts.get("confirmed_issue"), default=0
            )

        if (
            reviewed_case_count >= _MIN_REVIEWED_CASES_FOR_RECOMMENDATION
            and false_positive_rate >= _FALSE_POSITIVE_RATE_THRESHOLD
        ):
            threshold_candidates.append(
                {
                    "reason_code": outcome.get("reason_code"),
                    "reviewed_case_count": reviewed_case_count,
                    "false_positive_rate": str(false_positive_rate),
                    "suggested_action": "tighten_threshold",
                }
            )

        if (
            reviewed_case_count >= _MIN_REVIEWED_CASES_FOR_RECOMMENDATION
            and false_positive_rate >= _EXCLUSION_CANDIDATE_RATE_THRESHOLD
            and confirmed_issue_count == 0
        ):
            exclusion_candidates.append(
                {
                    "reason_code": outcome.get("reason_code"),
                    "reviewed_case_count": reviewed_case_count,
                    "false_positive_rate": str(false_positive_rate),
                    "suggested_action": "evaluate_exclusion_candidate",
                }
            )

    return {
        "threshold_tuning_candidates": threshold_candidates,
        "exclusion_tuning_candidates": exclusion_candidates,
    }


def _coerce_int(value: object, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _chunked(items: Sequence[int], size: int) -> list[Sequence[int]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _update_outcome_accumulator(
    acc: _OutcomeAccumulator,
    *,
    disposition: str,
    score_value: Decimal,
    threshold_signatures: Sequence[tuple[str, str, str]],
) -> None:
    acc.reviewed_case_count += 1
    if disposition == DISPOSITION_FALSE_POSITIVE:
        acc.false_positive_count += 1
    acc.disposition_counts[disposition] += 1
    acc.score_values.append(score_value)
    acc.threshold_signatures.update(threshold_signatures)


def _ordered_disposition_counts(counter: Counter[str]) -> dict[str, int]:
    return {
        disposition: counter[disposition]
        for disposition in sorted(counter)
        if counter[disposition] > 0
    }


def _ordered_threshold_signatures(
    signatures: set[tuple[str, str, str]],
) -> list[dict[str, str]]:
    return [
        {
            "metric_name": metric_name,
            "comparison_operator": comparison_operator,
            "threshold_value": threshold_value,
        }
        for metric_name, comparison_operator, threshold_value in sorted(
            signatures,
            key=lambda item: (item[0], item[1], item[2]),
        )
    ]


def _ratio_str(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0000"
    value = Decimal(numerator) / Decimal(denominator)
    quantized = value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return format(quantized, "f")


def _median_decimal_str(values: Sequence[Decimal], *, scale: int) -> Optional[str]:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        median_value = ordered[mid]
    else:
        median_value = (ordered[mid - 1] + ordered[mid]) / Decimal(2)
    quant = Decimal("1").scaleb(-scale)
    quantized = median_value.quantize(quant, rounding=ROUND_HALF_UP)
    return format(quantized, "f")


def _build_sql_queries(
    *,
    feature_version: str,
    ruleset_version: str,
) -> dict[str, str]:
    fv = _sql_literal(feature_version)
    rv = _sql_literal(ruleset_version)

    base_cte = (
        "WITH reviewed AS (\n"
        "  SELECT cr.score_id, cr.disposition, fs.risk_band\n"
        "  FROM case_reviews cr\n"
        "  JOIN fraud_scores fs ON fs.id = cr.score_id\n"
        f"  WHERE cr.feature_version = {fv}\n"
        f"    AND cr.ruleset_version = {rv}\n"
        "    AND fs.feature_version = cr.feature_version\n"
        "    AND fs.ruleset_version = cr.ruleset_version\n"
        "    AND cr.status IN ('resolved', 'closed')\n"
        "    AND cr.disposition IS NOT NULL\n"
        ")\n"
    )

    risk_band_sql = (
        base_cte
        + "SELECT\n"
        + "  risk_band,\n"
        + "  COUNT(*) AS reviewed_case_count,\n"
        + (
            "  SUM(CASE WHEN disposition = 'false_positive' THEN 1 ELSE 0 END) "
            "AS false_positive_count,\n"
        )
        + "  CASE WHEN COUNT(*) = 0 THEN 0\n"
        + (
            "       ELSE ROUND(1.0 * SUM(CASE WHEN disposition = 'false_positive' "
            "THEN 1 ELSE 0 END) / COUNT(*), 4)\n"
        )
        + "  END AS false_positive_rate\n"
        + "FROM reviewed\n"
        + "GROUP BY risk_band\n"
        + (
            "ORDER BY CASE risk_band WHEN 'high' THEN 0 WHEN 'medium' THEN 1 "
            "WHEN 'low' THEN 2 ELSE 99 END, risk_band;\n"
        )
    )

    reason_sql = (
        base_cte
        + ", reason_rows AS (\n"
        + "  SELECT r.disposition, ff.reason_code\n"
        + "  FROM reviewed r\n"
        + "  JOIN fraud_flags ff ON ff.score_id = r.score_id\n"
        + ")\n"
        + "SELECT\n"
        + "  reason_code,\n"
        + "  COUNT(*) AS reviewed_case_count,\n"
        + (
            "  SUM(CASE WHEN disposition = 'false_positive' THEN 1 ELSE 0 END) "
            "AS false_positive_count,\n"
        )
        + "  CASE WHEN COUNT(*) = 0 THEN 0\n"
        + (
            "       ELSE ROUND(1.0 * SUM(CASE WHEN disposition = 'false_positive' "
            "THEN 1 ELSE 0 END) / COUNT(*), 4)\n"
        )
        + "  END AS false_positive_rate\n"
        + "FROM reason_rows\n"
        + "GROUP BY reason_code\n"
        + "ORDER BY reviewed_case_count DESC, reason_code ASC;\n"
    )

    reason_risk_sql = (
        base_cte
        + ", reason_rows AS (\n"
        + "  SELECT r.disposition, r.risk_band, ff.reason_code\n"
        + "  FROM reviewed r\n"
        + "  JOIN fraud_flags ff ON ff.score_id = r.score_id\n"
        + ")\n"
        + "SELECT\n"
        + "  reason_code,\n"
        + "  risk_band,\n"
        + "  COUNT(*) AS reviewed_case_count,\n"
        + (
            "  SUM(CASE WHEN disposition = 'false_positive' THEN 1 ELSE 0 END) "
            "AS false_positive_count,\n"
        )
        + "  CASE WHEN COUNT(*) = 0 THEN 0\n"
        + (
            "       ELSE ROUND(1.0 * SUM(CASE WHEN disposition = 'false_positive' "
            "THEN 1 ELSE 0 END) / COUNT(*), 4)\n"
        )
        + "  END AS false_positive_rate\n"
        + "FROM reason_rows\n"
        + "GROUP BY reason_code, risk_band\n"
        + (
            "ORDER BY reason_code ASC, CASE risk_band WHEN 'high' THEN 0 "
            "WHEN 'medium' THEN 1 WHEN 'low' THEN 2 ELSE 99 END, risk_band "
            "ASC;\n"
        )
    )

    return {
        "risk_band_outcomes": risk_band_sql,
        "reason_code_outcomes": reason_sql,
        "reason_code_risk_slices": reason_risk_sql,
    }


def _sql_script(sql_queries: Mapping[str, str]) -> str:
    ordered_keys = [
        "risk_band_outcomes",
        "reason_code_outcomes",
        "reason_code_risk_slices",
    ]
    sections = []
    for key in ordered_keys:
        query = sql_queries.get(key)
        if not isinstance(query, str):
            continue
        sections.append(f"-- {key}\n{query.strip()}\n")
    return "\n".join(sections).strip() + "\n"


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
