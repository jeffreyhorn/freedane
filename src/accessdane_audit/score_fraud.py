from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from typing import Callable, Mapping, Optional, Sequence, TypedDict, TypeVar

from sqlalchemy import delete, select, tuple_, update
from sqlalchemy.exc import InvalidRequestError, PendingRollbackError
from sqlalchemy.orm import Session, load_only

from .models import CaseReview, FraudFlag, FraudScore, ParcelFeature, ScoringRun

RUN_TYPE_SCORE_FRAUD = "score_fraud"
IN_CLAUSE_BATCH_SIZE = 800
FEATURE_KEY_IN_CLAUSE_BATCH_SIZE = 400
TOP_FLAGS_LIMIT = 20
TOP_PARCELS_LIMIT = 20
SCORE_INSERT_BATCH_SIZE = 200
SCORE_MIN = Decimal("0.00")
SCORE_MAX = Decimal("100.00")
RISK_BAND_HIGH_MIN = Decimal("70.00")
RISK_BAND_MEDIUM_MIN = Decimal("40.00")
PERMIT_SUPPORT_CUTOFF = Decimal("10000")
SUPPORTED_RULESET_VERSIONS = ("scoring_rules_v1",)


class ScoreFraudScope(TypedDict):
    parcel_ids: Optional[list[str]]
    years: Optional[list[int]]
    feature_run_id: Optional[int]


class ScoreFraudRun(TypedDict):
    run_id: Optional[int]
    run_persisted: bool
    run_type: str
    version_tag: str
    status: str
    parent_run_id: Optional[int]


class ScoreFraudSummary(TypedDict):
    features_considered: int
    scores_inserted: int
    scores_updated: int
    flags_inserted: int
    high_risk_count: int
    medium_risk_count: int
    low_risk_count: int
    skipped_feature_rows: int


class ScoreFraudTopFlag(TypedDict):
    parcel_id: str
    year: int
    score_value: str
    risk_band: str
    reason_code: str
    reason_rank: int
    severity_weight: str
    explanation: str


class ScoreFraudTopParcel(TypedDict):
    parcel_id: str
    year: int
    score_value: str
    risk_band: str
    reason_code_count: int
    primary_reason_code: Optional[str]
    primary_reason_weight: str


class ScoreFraudRiskBandBreakdown(TypedDict):
    risk_band: str
    parcel_count: int


class ScoreFraudReasonCodeBreakdown(TypedDict):
    reason_code: str
    flag_count: int


class ScoreFraudSkippedFeatureBreakdown(TypedDict):
    reason: str
    row_count: int


class ScoreFraudRankings(TypedDict):
    top_parcels: list[ScoreFraudTopParcel]
    risk_band_breakdown: list[ScoreFraudRiskBandBreakdown]
    reason_code_breakdown: list[ScoreFraudReasonCodeBreakdown]
    skipped_feature_breakdown: list[ScoreFraudSkippedFeatureBreakdown]


class ScoreFraudPayload(TypedDict, total=False):
    run: ScoreFraudRun
    scope: ScoreFraudScope
    summary: ScoreFraudSummary
    top_flags: list[ScoreFraudTopFlag]
    rankings: ScoreFraudRankings
    error: str


@dataclass(frozen=True)
class _RuleTrigger:
    reason_code: str
    severity_weight: Decimal
    metric_name: str
    metric_value: Decimal
    threshold_value: str
    comparison_operator: str
    explanation: str
    secondary_evidence: dict[str, object]


@dataclass(frozen=True)
class _RuleSkip:
    reason_code: str
    skip_reason: str
    missing_inputs: list[str]


@dataclass(frozen=True)
class _RuleResult:
    trigger: Optional[_RuleTrigger]
    skip: Optional[_RuleSkip]


@dataclass(frozen=True)
class _PendingScoreBatchItem:
    parcel_id: str
    year: int
    feature_version: str
    feature_run_id: Optional[int]
    feature_sources: dict[str, object]
    score_row: FraudScore
    ranked_triggers: list[_RuleTrigger]


@dataclass(frozen=True)
class _TopFlagCandidate:
    sort_key: tuple[Decimal, int, str, int, str]
    payload: ScoreFraudTopFlag


@dataclass(frozen=True)
class _TopParcelCandidate:
    sort_key: tuple[Decimal, int, str, int]
    payload: ScoreFraudTopParcel


def score_fraud(
    session: Session,
    *,
    ruleset_version: str,
    feature_version: str,
    feature_run_id: Optional[int] = None,
    parcel_ids: Optional[Sequence[str]] = None,
    years: Optional[Sequence[int]] = None,
) -> ScoreFraudPayload:
    if ruleset_version not in SUPPORTED_RULESET_VERSIONS:
        supported_values = ", ".join(SUPPORTED_RULESET_VERSIONS)
        return {
            "run": {
                "run_id": None,
                "run_persisted": False,
                "run_type": RUN_TYPE_SCORE_FRAUD,
                "version_tag": ruleset_version,
                "status": "failed",
                "parent_run_id": None,
            },
            "scope": _resolved_scope(
                parcel_ids=parcel_ids,
                years=years,
                feature_run_id=feature_run_id,
            ),
            "summary": _empty_summary(),
            "top_flags": [],
            "rankings": _empty_rankings(),
            "error": (
                f"Unsupported ruleset_version '{ruleset_version}'. "
                f"Supported values: {supported_values}."
            ),
        }

    raw_scope: dict[str, object] = {
        "requested_parcel_ids": list(parcel_ids) if parcel_ids is not None else None,
        "requested_years": list(years) if years is not None else None,
        "requested_feature_run_id": feature_run_id,
    }
    resolved_scope = _resolved_scope(
        parcel_ids=parcel_ids,
        years=years,
        feature_run_id=feature_run_id,
    )
    config_json: dict[str, object] = {
        "ruleset_version": ruleset_version,
        "feature_version": feature_version,
        "score_min": str(SCORE_MIN),
        "score_max": str(SCORE_MAX),
        "risk_band_high_min": str(RISK_BAND_HIGH_MIN),
        "risk_band_medium_min": str(RISK_BAND_MEDIUM_MIN),
        "rounding_mode": "ROUND_HALF_UP",
        "top_flags_limit": TOP_FLAGS_LIMIT,
        "top_parcels_limit": TOP_PARCELS_LIMIT,
        "tier_resolution": "descending_severity_first_match",
        "top_parcels_ordering": "score_desc_reason_count_desc_parcel_id_year",
        "rule_ids": ["R1", "R2", "R3", "R4", "R5", "R6"],
    }
    run = ScoringRun(
        run_type=RUN_TYPE_SCORE_FRAUD,
        status="running",
        version_tag=ruleset_version,
        scope_hash=_scope_hash(resolved_scope, config_json),
        scope_json=dict(resolved_scope),
        config_json=config_json,
        parent_run_id=None,
    )
    failure_summary = _empty_summary()
    failure_top_flags: list[ScoreFraudTopFlag] = []
    failure_rankings = _empty_rankings()

    try:
        session.add(run)
        session.flush()
        features = _load_candidate_features(
            session,
            feature_version=feature_version,
            parcel_ids=resolved_scope["parcel_ids"],
            years=resolved_scope["years"],
            feature_run_id=resolved_scope["feature_run_id"],
        )
        run.parent_run_id = _resolve_parent_run_id(
            features=features,
            explicit_feature_run_id=resolved_scope["feature_run_id"],
        )

        with session.begin_nested():
            existing_scores_by_key = _delete_existing_scored_rows(
                session,
                ruleset_version=ruleset_version,
                feature_version=feature_version,
                features=features,
            )
            summary, top_flags, rankings = _persist_scores_and_flags(
                session=session,
                run_id=run.id,
                ruleset_version=ruleset_version,
                feature_version=feature_version,
                features=features,
                existing_scores_by_key=existing_scores_by_key,
            )

        run.status = "succeeded"
        run.error_summary = None
        run.input_summary_json = raw_scope
        run.output_summary_json = dict(summary)
        run.completed_at = datetime.now(timezone.utc)
        session.flush()

        return {
            "run": _run_payload(run),
            "scope": resolved_scope,
            "summary": summary,
            "top_flags": top_flags,
            "rankings": rankings,
        }
    except Exception as exc:
        run_persisted = True
        run.status = "failed"
        run.error_summary = str(exc)
        run.input_summary_json = raw_scope
        run.output_summary_json = dict(failure_summary)
        run.completed_at = datetime.now(timezone.utc)
        try:
            session.flush()
        except (PendingRollbackError, InvalidRequestError):
            session.rollback()
            run_persisted = False
        return {
            "run": _run_payload(run, run_persisted=run_persisted),
            "scope": resolved_scope,
            "summary": failure_summary,
            "top_flags": failure_top_flags,
            "rankings": failure_rankings,
            "error": str(exc),
        }


def _load_candidate_features(
    session: Session,
    *,
    feature_version: str,
    parcel_ids: Optional[list[str]],
    years: Optional[list[int]],
    feature_run_id: Optional[int],
) -> list[ParcelFeature]:
    if parcel_ids is not None and not parcel_ids:
        return []
    if years is not None and not years:
        return []

    columns = (
        ParcelFeature.id,
        ParcelFeature.run_id,
        ParcelFeature.parcel_id,
        ParcelFeature.year,
        ParcelFeature.feature_version,
        ParcelFeature.assessment_to_sale_ratio,
        ParcelFeature.peer_percentile,
        ParcelFeature.yoy_assessment_change_pct,
        ParcelFeature.permit_adjusted_expected_change,
        ParcelFeature.permit_adjusted_gap,
        ParcelFeature.appeal_value_delta_3y,
        ParcelFeature.appeal_success_rate_3y,
        ParcelFeature.lineage_value_reset_delta,
        ParcelFeature.feature_quality_flags,
        ParcelFeature.source_refs_json,
    )
    if parcel_ids is None:
        query = (
            select(ParcelFeature)
            .options(load_only(*columns))
            .where(ParcelFeature.feature_version == feature_version)
        )
        if years is not None:
            query = query.where(ParcelFeature.year.in_(years))
        if feature_run_id is not None:
            query = query.where(ParcelFeature.run_id == feature_run_id)
        return list(
            session.execute(
                query.order_by(ParcelFeature.parcel_id.asc(), ParcelFeature.year.asc())
            )
            .scalars()
            .all()
        )

    features: list[ParcelFeature] = []
    for batch_parcel_ids in _chunked(parcel_ids, IN_CLAUSE_BATCH_SIZE):
        query = (
            select(ParcelFeature)
            .options(load_only(*columns))
            .where(
                ParcelFeature.feature_version == feature_version,
                ParcelFeature.parcel_id.in_(batch_parcel_ids),
            )
        )
        if years is not None:
            query = query.where(ParcelFeature.year.in_(years))
        if feature_run_id is not None:
            query = query.where(ParcelFeature.run_id == feature_run_id)
        features.extend(session.execute(query).scalars().all())
    return sorted(features, key=lambda row: (row.parcel_id, row.year))


def _delete_existing_scored_rows(
    session: Session,
    *,
    ruleset_version: str,
    feature_version: str,
    features: Sequence[ParcelFeature],
) -> dict[tuple[str, int], FraudScore]:
    score_ids = _load_existing_score_ids_for_feature_keys(
        session,
        ruleset_version=ruleset_version,
        feature_version=feature_version,
        feature_keys=_feature_keys(features),
    )
    if not score_ids:
        return {}

    score_rows_with_reviews = _load_existing_scores_with_case_reviews(
        session,
        score_ids=score_ids,
    )
    reviewed_score_ids = {
        score_row.id
        for score_row in score_rows_with_reviews
        if score_row.id is not None
    }

    score_ids_to_delete = [
        score_id for score_id in score_ids if score_id not in reviewed_score_ids
    ]
    _delete_flags_for_score_ids(session, score_ids=score_ids_to_delete)

    for batch_score_ids in _chunked(score_ids_to_delete, IN_CLAUSE_BATCH_SIZE):
        session.execute(delete(FraudScore).where(FraudScore.id.in_(batch_score_ids)))

    return {
        (score_row.parcel_id, score_row.year): score_row
        for score_row in score_rows_with_reviews
    }


def _delete_flags_for_score_ids(session: Session, *, score_ids: Sequence[int]) -> None:
    unique_score_ids = sorted(set(score_ids))
    if not unique_score_ids:
        return
    for batch_score_ids in _chunked(unique_score_ids, IN_CLAUSE_BATCH_SIZE):
        session.execute(
            delete(FraudFlag).where(FraudFlag.score_id.in_(batch_score_ids))
        )


def _sync_case_review_run_ids_for_scores(
    session: Session,
    *,
    score_ids: Sequence[int],
    run_id: int,
    feature_version: str,
    ruleset_version: str,
) -> None:
    unique_score_ids = sorted(set(score_ids))
    if not unique_score_ids:
        return
    for batch_score_ids in _chunked(unique_score_ids, IN_CLAUSE_BATCH_SIZE):
        session.execute(
            update(CaseReview)
            .where(
                CaseReview.score_id.in_(batch_score_ids),
                CaseReview.feature_version == feature_version,
                CaseReview.ruleset_version == ruleset_version,
            )
            .values(run_id=run_id)
        )


def _load_existing_scores_with_case_reviews(
    session: Session,
    *,
    score_ids: Sequence[int],
) -> list[FraudScore]:
    if not score_ids:
        return []

    score_rows: list[FraudScore] = []
    for batch_score_ids in _chunked(score_ids, IN_CLAUSE_BATCH_SIZE):
        query = (
            select(FraudScore)
            .join(CaseReview, CaseReview.score_id == FraudScore.id)
            .where(FraudScore.id.in_(batch_score_ids))
            .distinct()
        )
        score_rows.extend(session.execute(query).scalars().all())
    return score_rows


def _feature_keys(features: Sequence[ParcelFeature]) -> list[tuple[str, int]]:
    keys: set[tuple[str, int]] = set()
    for feature in features:
        parcel_id = feature.parcel_id
        year = feature.year
        if not isinstance(parcel_id, str):
            continue
        if not isinstance(year, int) or isinstance(year, bool):
            continue
        keys.add((parcel_id, year))
        stripped_parcel_id = parcel_id.strip()
        if stripped_parcel_id and stripped_parcel_id != parcel_id:
            # Include legacy-normalized key to ensure stale rows from pre-guard
            # normalization are deleted during reruns.
            keys.add((stripped_parcel_id, year))
    return sorted(keys, key=lambda item: (item[0], item[1]))


def _load_existing_score_ids_for_feature_keys(
    session: Session,
    *,
    ruleset_version: str,
    feature_version: str,
    feature_keys: Sequence[tuple[str, int]],
) -> list[int]:
    if not feature_keys:
        return []

    score_ids: list[int] = []
    for batch_feature_keys in _chunked(feature_keys, FEATURE_KEY_IN_CLAUSE_BATCH_SIZE):
        query = select(FraudScore.id).where(
            FraudScore.ruleset_version == ruleset_version,
            FraudScore.feature_version == feature_version,
            tuple_(FraudScore.parcel_id, FraudScore.year).in_(batch_feature_keys),
        )
        score_ids.extend(session.execute(query).scalars().all())
    return sorted(score_ids)


def _persist_scores_and_flags(
    *,
    session: Session,
    run_id: int,
    ruleset_version: str,
    feature_version: str,
    features: Sequence[ParcelFeature],
    existing_scores_by_key: Mapping[tuple[str, int], FraudScore],
) -> tuple[ScoreFraudSummary, list[ScoreFraudTopFlag], ScoreFraudRankings]:
    scores_inserted = 0
    scores_updated = 0
    flags_inserted = 0
    high_risk_count = 0
    medium_risk_count = 0
    low_risk_count = 0
    skipped_feature_rows = 0
    top_flag_candidates: list[_TopFlagCandidate] = []
    top_parcel_candidates: list[_TopParcelCandidate] = []
    pending_scores: list[_PendingScoreBatchItem] = []
    flag_refresh_score_ids: list[int] = []
    case_review_sync_score_ids: list[int] = []
    reason_code_flag_counts: dict[str, int] = {}
    skipped_feature_reason_counts: dict[str, int] = {}

    for feature in features:
        guard_reason = _feature_row_guard_reason(feature)
        if guard_reason is not None:
            skipped_feature_rows += 1
            skipped_feature_reason_counts[guard_reason] = (
                skipped_feature_reason_counts.get(guard_reason, 0) + 1
            )
            continue

        assert isinstance(feature.parcel_id, str)
        assert feature.year is not None
        parcel_id = feature.parcel_id
        year = int(feature.year)
        feature_sources, invalid_source_refs = _normalize_feature_sources(
            feature.source_refs_json
        )

        rule_results = [
            _evaluate_r1(feature),
            _evaluate_r2(feature),
            _evaluate_r3(feature),
            _evaluate_r4(feature),
            _evaluate_r5(feature),
            _evaluate_r6(feature),
        ]
        triggers = [
            result.trigger for result in rule_results if result.trigger is not None
        ]
        skips = [result.skip for result in rule_results if result.skip is not None]
        ranked_triggers = sorted(
            triggers,
            key=lambda item: (
                Decimal("0.0000") - item.severity_weight,
                item.reason_code,
            ),
        )

        raw_score = _quantize_decimal(
            sum((trigger.severity_weight for trigger in ranked_triggers), Decimal("0")),
            scale=4,
        )
        score_value = _quantize_decimal(
            min(SCORE_MAX, max(SCORE_MIN, raw_score)),
            scale=2,
        )
        risk_band = _risk_band(score_value)
        requires_review = risk_band in {"high", "medium"}
        reason_codes = sorted({trigger.reason_code for trigger in ranked_triggers})
        quality_flags = _normalized_feature_quality_flags(feature.feature_quality_flags)
        if invalid_source_refs:
            quality_flags = sorted({*quality_flags, "source_refs_json_invalid_shape"})

        score_summary_json: dict[str, object] = {
            "ruleset_version": ruleset_version,
            "feature_version": feature_version,
            "raw_score": _decimal_to_str(raw_score),
            "score_value": _decimal_to_str(score_value),
            "risk_band": risk_band,
            "quality_flags": quality_flags,
            "triggered_reason_codes": reason_codes,
            "skipped_rules": [
                {
                    "reason_code": skip.reason_code,
                    "skip_reason": skip.skip_reason,
                    "missing_inputs": skip.missing_inputs,
                }
                for skip in sorted(skips, key=lambda item: item.reason_code)
            ],
        }
        score_key = (parcel_id, year)
        score_row = existing_scores_by_key.get(score_key)
        if score_row is None:
            score_row = FraudScore(
                run_id=run_id,
                feature_run_id=feature.run_id,
                parcel_id=parcel_id,
                year=year,
                ruleset_version=ruleset_version,
                feature_version=feature_version,
                score_value=score_value,
                risk_band=risk_band,
                requires_review=requires_review,
                reason_code_count=len(ranked_triggers),
                score_summary_json=score_summary_json,
            )
            session.add(score_row)
            scores_inserted += 1
        else:
            score_id = score_row.id
            assert score_id is not None
            flag_refresh_score_ids.append(score_id)
            case_review_sync_score_ids.append(score_id)
            score_row.run_id = run_id
            score_row.feature_run_id = feature.run_id
            score_row.ruleset_version = ruleset_version
            score_row.feature_version = feature_version
            score_row.score_value = score_value
            score_row.risk_band = risk_band
            score_row.requires_review = requires_review
            score_row.reason_code_count = len(ranked_triggers)
            score_row.score_summary_json = score_summary_json
            scores_updated += 1

        if risk_band == "high":
            high_risk_count += 1
        elif risk_band == "medium":
            medium_risk_count += 1
        else:
            low_risk_count += 1

        pending_scores.append(
            _PendingScoreBatchItem(
                parcel_id=parcel_id,
                year=year,
                feature_version=feature.feature_version,
                feature_run_id=feature.run_id,
                feature_sources=feature_sources,
                score_row=score_row,
                ranked_triggers=ranked_triggers,
            )
        )

        for reason_rank, trigger in enumerate(ranked_triggers, start=1):
            reason_code_flag_counts[trigger.reason_code] = (
                reason_code_flag_counts.get(trigger.reason_code, 0) + 1
            )
            top_flag_payload: ScoreFraudTopFlag = {
                "parcel_id": parcel_id,
                "year": year,
                "score_value": _decimal_to_str(score_value),
                "risk_band": risk_band,
                "reason_code": trigger.reason_code,
                "reason_rank": reason_rank,
                "severity_weight": _decimal_to_str(
                    _quantize_decimal(trigger.severity_weight, scale=4)
                ),
                "explanation": trigger.explanation,
            }
            _consider_top_flag_candidate(
                top_flag_candidates=top_flag_candidates,
                candidate=_TopFlagCandidate(
                    sort_key=_top_flag_sort_key(
                        score_value=score_value,
                        reason_rank=reason_rank,
                        parcel_id=parcel_id,
                        year=year,
                        reason_code=trigger.reason_code,
                    ),
                    payload=top_flag_payload,
                ),
            )

        primary_reason_code = (
            ranked_triggers[0].reason_code if ranked_triggers else None
        )
        primary_reason_weight = (
            _decimal_to_str(
                _quantize_decimal(ranked_triggers[0].severity_weight, scale=4)
            )
            if ranked_triggers
            else "0.0000"
        )
        _consider_top_parcel_candidate(
            top_parcel_candidates=top_parcel_candidates,
            candidate=_TopParcelCandidate(
                sort_key=_top_parcel_sort_key(
                    score_value=score_value,
                    reason_code_count=len(ranked_triggers),
                    parcel_id=parcel_id,
                    year=year,
                ),
                payload={
                    "parcel_id": parcel_id,
                    "year": year,
                    "score_value": _decimal_to_str(score_value),
                    "risk_band": risk_band,
                    "reason_code_count": len(ranked_triggers),
                    "primary_reason_code": primary_reason_code,
                    "primary_reason_weight": primary_reason_weight,
                },
            ),
        )

        if len(pending_scores) >= SCORE_INSERT_BATCH_SIZE:
            _delete_flags_for_score_ids(session, score_ids=flag_refresh_score_ids)
            flag_refresh_score_ids.clear()
            _sync_case_review_run_ids_for_scores(
                session,
                score_ids=case_review_sync_score_ids,
                run_id=run_id,
                feature_version=feature_version,
                ruleset_version=ruleset_version,
            )
            case_review_sync_score_ids.clear()
            flags_inserted += _flush_score_batch(
                session=session,
                pending_scores=pending_scores,
                run_id=run_id,
                ruleset_version=ruleset_version,
            )
            pending_scores.clear()

    _delete_flags_for_score_ids(session, score_ids=flag_refresh_score_ids)
    _sync_case_review_run_ids_for_scores(
        session,
        score_ids=case_review_sync_score_ids,
        run_id=run_id,
        feature_version=feature_version,
        ruleset_version=ruleset_version,
    )
    flags_inserted += _flush_score_batch(
        session=session,
        pending_scores=pending_scores,
        run_id=run_id,
        ruleset_version=ruleset_version,
    )
    ordered_top_flags = [item.payload for item in top_flag_candidates]
    ordered_top_parcels = [item.payload for item in top_parcel_candidates]
    summary: ScoreFraudSummary = {
        "features_considered": len(features),
        "scores_inserted": scores_inserted,
        "scores_updated": scores_updated,
        "flags_inserted": flags_inserted,
        "high_risk_count": high_risk_count,
        "medium_risk_count": medium_risk_count,
        "low_risk_count": low_risk_count,
        "skipped_feature_rows": skipped_feature_rows,
    }
    rankings: ScoreFraudRankings = {
        "top_parcels": ordered_top_parcels,
        "risk_band_breakdown": [
            {"risk_band": "high", "parcel_count": high_risk_count},
            {"risk_band": "medium", "parcel_count": medium_risk_count},
            {"risk_band": "low", "parcel_count": low_risk_count},
        ],
        "reason_code_breakdown": [
            {"reason_code": reason_code, "flag_count": flag_count}
            for reason_code, flag_count in sorted(
                reason_code_flag_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ],
        "skipped_feature_breakdown": [
            {"reason": reason, "row_count": row_count}
            for reason, row_count in sorted(
                skipped_feature_reason_counts.items(),
                key=lambda item: item[0],
            )
        ],
    }
    return summary, ordered_top_flags, rankings


def _consider_top_flag_candidate(
    *,
    top_flag_candidates: list[_TopFlagCandidate],
    candidate: _TopFlagCandidate,
) -> None:
    _consider_top_candidate(
        candidates=top_flag_candidates,
        candidate=candidate,
        limit=TOP_FLAGS_LIMIT,
        key_fn=lambda item: item.sort_key,
    )


def _consider_top_parcel_candidate(
    *,
    top_parcel_candidates: list[_TopParcelCandidate],
    candidate: _TopParcelCandidate,
) -> None:
    _consider_top_candidate(
        candidates=top_parcel_candidates,
        candidate=candidate,
        limit=TOP_PARCELS_LIMIT,
        key_fn=lambda item: item.sort_key,
    )


_TopCandidateT = TypeVar("_TopCandidateT")


def _consider_top_candidate(
    *,
    candidates: list[_TopCandidateT],
    candidate: _TopCandidateT,
    limit: int,
    key_fn: Callable[[_TopCandidateT], tuple[object, ...]],
) -> None:
    if len(candidates) < limit:
        candidates.append(candidate)
        candidates.sort(key=key_fn)
        return
    if key_fn(candidate) < key_fn(candidates[-1]):
        candidates[-1] = candidate
        candidates.sort(key=key_fn)


def _top_flag_sort_key(
    *,
    score_value: Decimal,
    reason_rank: int,
    parcel_id: str,
    year: int,
    reason_code: str,
) -> tuple[Decimal, int, str, int, str]:
    return (
        Decimal("0.00") - score_value,
        reason_rank,
        parcel_id,
        year,
        reason_code,
    )


def _top_parcel_sort_key(
    *,
    score_value: Decimal,
    reason_code_count: int,
    parcel_id: str,
    year: int,
) -> tuple[Decimal, int, str, int]:
    return (Decimal("0.00") - score_value, -reason_code_count, parcel_id, year)


def _feature_row_guard_reason(feature: ParcelFeature) -> Optional[str]:
    parcel_id = feature.parcel_id
    if not isinstance(parcel_id, str) or not parcel_id.strip():
        return "missing_parcel_id"
    if parcel_id.strip() != parcel_id:
        return "parcel_id_has_surrounding_whitespace"
    year = feature.year
    if not isinstance(year, int) or isinstance(year, bool) or year < 1:
        return "invalid_year"
    return None


def _normalize_feature_sources(
    source_refs_json: object,
) -> tuple[dict[str, object], bool]:
    if isinstance(source_refs_json, dict):
        return source_refs_json, False
    return {"invalid_source_refs_json_type": type(source_refs_json).__name__}, True


def _normalized_feature_quality_flags(raw_flags: object) -> list[str]:
    if not isinstance(raw_flags, Sequence) or isinstance(
        raw_flags, (str, bytes, bytearray)
    ):
        return []
    return sorted({flag for flag in raw_flags if isinstance(flag, str)})


def _flush_score_batch(
    *,
    session: Session,
    pending_scores: list[_PendingScoreBatchItem],
    run_id: int,
    ruleset_version: str,
) -> int:
    if not pending_scores:
        return 0

    session.flush()
    inserted_flags = 0
    inserted_flag_rows: list[FraudFlag] = []

    for batch_item in pending_scores:
        score_id = batch_item.score_row.id
        assert score_id is not None
        for reason_rank, trigger in enumerate(batch_item.ranked_triggers, start=1):
            source_refs_json: dict[str, object] = {
                "feature_row": {
                    "parcel_id": batch_item.parcel_id,
                    "year": batch_item.year,
                    "feature_version": batch_item.feature_version,
                },
                "feature_run_id": batch_item.feature_run_id,
                "feature_sources": batch_item.feature_sources,
            }
            if trigger.secondary_evidence:
                source_refs_json["secondary_evidence"] = trigger.secondary_evidence
            flag_row = FraudFlag(
                run_id=run_id,
                score_id=score_id,
                parcel_id=batch_item.parcel_id,
                year=batch_item.year,
                ruleset_version=ruleset_version,
                reason_code=trigger.reason_code,
                reason_rank=reason_rank,
                severity_weight=_quantize_decimal(trigger.severity_weight, scale=4),
                metric_name=trigger.metric_name,
                metric_value=_decimal_to_str(trigger.metric_value),
                threshold_value=trigger.threshold_value,
                comparison_operator=trigger.comparison_operator,
                explanation=trigger.explanation,
                source_refs_json=source_refs_json,
            )
            session.add(flag_row)
            inserted_flag_rows.append(flag_row)
            inserted_flags += 1

    session.flush()
    for flag_row in inserted_flag_rows:
        session.expunge(flag_row)
    for batch_item in pending_scores:
        session.expunge(batch_item.score_row)
    return inserted_flags


def _empty_summary() -> ScoreFraudSummary:
    return {
        "features_considered": 0,
        "scores_inserted": 0,
        "scores_updated": 0,
        "flags_inserted": 0,
        "high_risk_count": 0,
        "medium_risk_count": 0,
        "low_risk_count": 0,
        "skipped_feature_rows": 0,
    }


def _empty_rankings() -> ScoreFraudRankings:
    return {
        "top_parcels": [],
        "risk_band_breakdown": [
            {"risk_band": "high", "parcel_count": 0},
            {"risk_band": "medium", "parcel_count": 0},
            {"risk_band": "low", "parcel_count": 0},
        ],
        "reason_code_breakdown": [],
        "skipped_feature_breakdown": [],
    }


def _evaluate_r1(feature: ParcelFeature) -> _RuleResult:
    ratio = feature.assessment_to_sale_ratio
    if ratio is None:
        return _missing_required_input(
            reason_code="ratio__assessment_to_sale_below_floor",
            missing_inputs=["assessment_to_sale_ratio"],
        )
    if ratio < Decimal("0.55"):
        return _triggered_rule(
            reason_code="ratio__assessment_to_sale_below_floor",
            severity_weight=Decimal("35.0000"),
            metric_name="assessment_to_sale_ratio",
            metric_value=ratio,
            threshold_value="0.55",
            comparison_operator="<",
            explanation=(
                f"Assessment-to-sale ratio {_decimal_to_str(ratio)} is below "
                "threshold 0.55."
            ),
        )
    if ratio < Decimal("0.70"):
        return _triggered_rule(
            reason_code="ratio__assessment_to_sale_below_floor",
            severity_weight=Decimal("20.0000"),
            metric_name="assessment_to_sale_ratio",
            metric_value=ratio,
            threshold_value="0.70",
            comparison_operator="<",
            explanation=(
                f"Assessment-to-sale ratio {_decimal_to_str(ratio)} is below "
                "threshold 0.70."
            ),
        )
    return _no_trigger()


def _evaluate_r2(feature: ParcelFeature) -> _RuleResult:
    peer_percentile = feature.peer_percentile
    if peer_percentile is None:
        return _missing_required_input(
            reason_code="peer__assessment_ratio_bottom_peer_percentile",
            missing_inputs=["peer_percentile"],
        )
    if peer_percentile <= Decimal("0.05"):
        return _triggered_rule(
            reason_code="peer__assessment_ratio_bottom_peer_percentile",
            severity_weight=Decimal("20.0000"),
            metric_name="peer_percentile",
            metric_value=peer_percentile,
            threshold_value="0.05",
            comparison_operator="<=",
            explanation=(
                f"Peer percentile {_decimal_to_str(peer_percentile)} is at or below "
                "threshold 0.05 for the parcel peer group."
            ),
        )
    if peer_percentile <= Decimal("0.10"):
        return _triggered_rule(
            reason_code="peer__assessment_ratio_bottom_peer_percentile",
            severity_weight=Decimal("12.0000"),
            metric_name="peer_percentile",
            metric_value=peer_percentile,
            threshold_value="0.10",
            comparison_operator="<=",
            explanation=(
                f"Peer percentile {_decimal_to_str(peer_percentile)} is at or below "
                "threshold 0.10 for the parcel peer group."
            ),
        )
    return _no_trigger()


def _evaluate_r3(feature: ParcelFeature) -> _RuleResult:
    permit_gap = feature.permit_adjusted_gap
    if permit_gap is None:
        return _missing_required_input(
            reason_code="permit_gap__assessment_increase_unexplained_by_permits",
            missing_inputs=["permit_adjusted_gap"],
        )
    if permit_gap >= Decimal("50000"):
        return _triggered_rule(
            reason_code="permit_gap__assessment_increase_unexplained_by_permits",
            severity_weight=Decimal("20.0000"),
            metric_name="permit_adjusted_gap",
            metric_value=permit_gap,
            threshold_value="50000",
            comparison_operator=">=",
            explanation=(
                f"Permit-adjusted gap {_decimal_to_str(permit_gap)} exceeds "
                "threshold 50000, indicating unexplained assessed-value increase."
            ),
        )
    if permit_gap >= Decimal("20000"):
        return _triggered_rule(
            reason_code="permit_gap__assessment_increase_unexplained_by_permits",
            severity_weight=Decimal("12.0000"),
            metric_name="permit_adjusted_gap",
            metric_value=permit_gap,
            threshold_value="20000",
            comparison_operator=">=",
            explanation=(
                f"Permit-adjusted gap {_decimal_to_str(permit_gap)} exceeds "
                "threshold 20000, indicating unexplained assessed-value increase."
            ),
        )
    return _no_trigger()


def _evaluate_r4(feature: ParcelFeature) -> _RuleResult:
    yoy_change = feature.yoy_assessment_change_pct
    if yoy_change is None:
        return _missing_required_input(
            reason_code="yoy__assessment_spike_without_support",
            missing_inputs=["yoy_assessment_change_pct"],
        )
    permit_expected_change = feature.permit_adjusted_expected_change
    if permit_expected_change is None:
        condition = "is_null"
    elif permit_expected_change <= PERMIT_SUPPORT_CUTOFF:
        condition = "<=_cutoff"
    else:
        return _no_trigger()

    if yoy_change >= Decimal("0.35"):
        threshold = "0.35"
        weight = Decimal("16.0000")
    elif yoy_change >= Decimal("0.20"):
        threshold = "0.20"
        weight = Decimal("10.0000")
    else:
        return _no_trigger()
    permit_support_cutoff = _decimal_to_str(PERMIT_SUPPORT_CUTOFF)

    secondary_evidence: dict[str, object] = {
        "permit_adjusted_expected_change": (
            _decimal_to_str(permit_expected_change)
            if permit_expected_change is not None
            else None
        ),
        "permit_adjusted_expected_change_condition": condition,
        "permit_adjusted_expected_change_cutoff": permit_support_cutoff,
        "permit_basis": _permit_basis_from_feature_sources(feature.source_refs_json),
    }
    return _triggered_rule(
        reason_code="yoy__assessment_spike_without_support",
        severity_weight=weight,
        metric_name="yoy_assessment_change_pct",
        metric_value=yoy_change,
        threshold_value=threshold,
        comparison_operator=">=",
        explanation=(
            "Year-over-year assessment change "
            f"{_decimal_to_str(yoy_change)} exceeds threshold {threshold} without "
            f"strong permit support ({condition} vs cutoff {permit_support_cutoff})."
        ),
        secondary_evidence=secondary_evidence,
    )


def _evaluate_r5(feature: ParcelFeature) -> _RuleResult:
    appeal_success_rate = feature.appeal_success_rate_3y
    appeal_value_delta = feature.appeal_value_delta_3y
    missing_inputs: list[str] = []
    if appeal_success_rate is None:
        missing_inputs.append("appeal_success_rate_3y")
    if appeal_value_delta is None:
        missing_inputs.append("appeal_value_delta_3y")
    if missing_inputs:
        return _missing_required_input(
            reason_code="appeal__recurring_successful_reductions",
            missing_inputs=missing_inputs,
        )
    assert appeal_success_rate is not None
    assert appeal_value_delta is not None
    if appeal_success_rate < Decimal("0.60") or appeal_value_delta > Decimal("-5000"):
        return _no_trigger()

    if appeal_success_rate >= Decimal("0.75") and appeal_value_delta <= Decimal(
        "-15000"
    ):
        threshold = "0.75"
        value_delta_threshold = "-15000"
        weight = Decimal("12.0000")
    else:
        threshold = "0.60"
        value_delta_threshold = "-5000"
        weight = Decimal("8.0000")

    return _triggered_rule(
        reason_code="appeal__recurring_successful_reductions",
        severity_weight=weight,
        metric_name="appeal_success_rate_3y",
        metric_value=appeal_success_rate,
        threshold_value=threshold,
        comparison_operator=">=",
        explanation=(
            f"Appeal success rate {_decimal_to_str(appeal_success_rate)} meets "
            f"threshold {threshold} with net negative appeal value change over 3 "
            "years."
        ),
        secondary_evidence={
            "appeal_value_delta_3y": _decimal_to_str(appeal_value_delta),
            "appeal_value_delta_threshold": value_delta_threshold,
        },
    )


def _evaluate_r6(feature: ParcelFeature) -> _RuleResult:
    lineage_delta = feature.lineage_value_reset_delta
    if lineage_delta is None:
        return _missing_required_input(
            reason_code="lineage__post_lineage_value_drop",
            missing_inputs=["lineage_value_reset_delta"],
        )
    if lineage_delta <= Decimal("-100000"):
        threshold = "-100000"
        weight = Decimal("18.0000")
    elif lineage_delta <= Decimal("-50000"):
        threshold = "-50000"
        weight = Decimal("10.0000")
    else:
        return _no_trigger()
    return _triggered_rule(
        reason_code="lineage__post_lineage_value_drop",
        severity_weight=weight,
        metric_name="lineage_value_reset_delta",
        metric_value=lineage_delta,
        threshold_value=threshold,
        comparison_operator="<=",
        explanation=(
            f"Lineage reset delta {_decimal_to_str(lineage_delta)} is at or below "
            f"threshold {threshold}, indicating a post-lineage value drop."
        ),
    )


def _permit_basis_from_feature_sources(
    source_refs_json: object,
) -> Optional[str]:
    if not isinstance(source_refs_json, dict):
        return None
    permits = source_refs_json.get("permits")
    if not isinstance(permits, dict):
        return None
    basis = permits.get("basis")
    return basis if isinstance(basis, str) else None


def _missing_required_input(reason_code: str, missing_inputs: list[str]) -> _RuleResult:
    return _RuleResult(
        trigger=None,
        skip=_RuleSkip(
            reason_code=reason_code,
            skip_reason="missing_required_input",
            missing_inputs=sorted(missing_inputs),
        ),
    )


def _triggered_rule(
    *,
    reason_code: str,
    severity_weight: Decimal,
    metric_name: str,
    metric_value: Decimal,
    threshold_value: str,
    comparison_operator: str,
    explanation: str,
    secondary_evidence: Optional[dict[str, object]] = None,
) -> _RuleResult:
    return _RuleResult(
        trigger=_RuleTrigger(
            reason_code=reason_code,
            severity_weight=_quantize_decimal(severity_weight, scale=4),
            metric_name=metric_name,
            metric_value=metric_value,
            threshold_value=threshold_value,
            comparison_operator=comparison_operator,
            explanation=explanation,
            secondary_evidence=secondary_evidence or {},
        ),
        skip=None,
    )


def _no_trigger() -> _RuleResult:
    return _RuleResult(trigger=None, skip=None)


def _risk_band(score_value: Decimal) -> str:
    if score_value >= RISK_BAND_HIGH_MIN:
        return "high"
    if score_value >= RISK_BAND_MEDIUM_MIN:
        return "medium"
    return "low"


def _quantize_decimal(value: Decimal, *, scale: int) -> Decimal:
    return value.quantize(Decimal(10) ** -scale, rounding=ROUND_HALF_UP)


def _decimal_to_str(value: Decimal) -> str:
    return format(value, "f")


def _resolve_parent_run_id(
    *,
    features: Sequence[ParcelFeature],
    explicit_feature_run_id: Optional[int],
) -> Optional[int]:
    if explicit_feature_run_id is not None:
        return explicit_feature_run_id
    run_ids = {feature.run_id for feature in features}
    if len(run_ids) == 1:
        return next(iter(run_ids))
    return None


def _resolved_scope(
    *,
    parcel_ids: Optional[Sequence[str]],
    years: Optional[Sequence[int]],
    feature_run_id: Optional[int],
) -> ScoreFraudScope:
    resolved_parcel_ids = (
        sorted({parcel_id.strip() for parcel_id in parcel_ids if parcel_id.strip()})
        if parcel_ids is not None
        else None
    )
    resolved_years = sorted(set(years)) if years is not None else None
    return {
        "parcel_ids": resolved_parcel_ids,
        "years": resolved_years,
        "feature_run_id": feature_run_id,
    }


def _scope_hash(scope_json: ScoreFraudScope, config_json: dict[str, object]) -> str:
    digest_input = json.dumps(
        {"scope": scope_json, "config": config_json},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(digest_input).hexdigest()


def _run_payload(run: ScoringRun, *, run_persisted: bool = True) -> ScoreFraudRun:
    run_id = run.id if (run_persisted and run.id is not None) else None
    parent_run_id = run.parent_run_id if run_persisted else None
    return {
        "run_id": run_id,
        "run_persisted": run_persisted,
        "run_type": run.run_type,
        "version_tag": run.version_tag,
        "status": run.status,
        "parent_run_id": parent_run_id,
    }


_ChunkValueT = TypeVar("_ChunkValueT")


def _chunked(values: Sequence[_ChunkValueT], size: int) -> list[list[_ChunkValueT]]:
    chunks: list[list[_ChunkValueT]] = []
    for index in range(0, len(values), size):
        chunks.append(list(values[index : index + size]))
    return chunks
