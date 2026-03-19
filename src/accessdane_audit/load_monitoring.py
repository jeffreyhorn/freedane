from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Mapping, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .models import CaseReview

RUN_TYPE_LOAD_MONITORING = "load_monitoring"
LOAD_MONITORING_VERSION_TAG = "load_monitoring_v1"

WINDOW_ORDER: tuple[tuple[str, int], ...] = (
    ("window_1d", 24),
    ("window_7d", 168),
    ("window_14d", 336),
    ("window_30d", 720),
)

_METRIC_KEYS: tuple[tuple[str, str], ...] = (
    ("duration.total_seconds", "duration"),
    ("duration.stage.analysis_artifacts.seconds", "duration"),
    ("duration.stage.build_context.seconds", "duration"),
    ("duration.stage.health_summary.seconds", "duration"),
    ("duration.stage.ingest_context.seconds", "duration"),
    ("duration.stage.investigation_artifacts.seconds", "duration"),
    ("duration.stage.score_pipeline.seconds", "duration"),
    ("failure_rate.run.failed_fraction_7d", "failure_rate"),
    ("failure_rate.stage.blocked_fraction_7d", "failure_rate"),
    ("failure_rate.stage.failed_fraction_7d", "failure_rate"),
    ("freshness.hours_since_last_successful_daily_refresh", "freshness"),
    ("freshness.hours_since_last_successful_review_feedback", "freshness"),
    ("freshness.hours_since_last_successful_score_pipeline", "freshness"),
    ("queue_size.case_review.in_review_count", "queue_size"),
    ("queue_size.review_queue.high_risk_count", "queue_size"),
    ("queue_size.review_queue.unreviewed_count", "queue_size"),
    ("volume.review_feedback.recommendation_count", "volume"),
    ("volume.review_feedback.reviewed_case_count", "volume"),
    ("volume.review_queue.returned_count", "volume"),
)


@dataclass(frozen=True)
class _HistorySample:
    path: Path
    run_id: str
    profile_name: str
    run_status: str
    run_started_at: Optional[datetime]
    run_finished_at: Optional[datetime]
    sample_event_at: Optional[datetime]
    duration_total_seconds: Optional[float]
    stage_status_by_id: dict[str, str]
    stage_duration_seconds_by_id: dict[str, Optional[float]]
    stage_started_at_by_id: dict[str, Optional[datetime]]
    stage_finished_at_by_id: dict[str, Optional[datetime]]
    review_feedback_command_status_by_stage: dict[str, str]
    review_queue_returned_count: Optional[float]
    review_queue_high_risk_count: Optional[float]
    review_queue_unreviewed_count: Optional[float]
    review_feedback_reviewed_case_count: Optional[float]
    review_feedback_recommendation_count: Optional[float]
    feature_version: Optional[str]
    ruleset_version: Optional[str]
    run_date: Optional[str]
    skip_reason_by_stage_id: dict[str, str]


@dataclass(frozen=True)
class _SubjectContext:
    sample: _HistorySample
    refresh_payload_path: Path


def build_load_diagnostics(
    session: Session,
    *,
    artifact_base_dir: Path,
    profile_name: str,
    feature_version: str,
    ruleset_version: str,
    subject_run_id: Optional[str] = None,
    subject_refresh_payload_path: Optional[Path] = None,
    monitor_run_id: Optional[str] = None,
    run_persisted: bool = False,
) -> dict[str, Any]:
    started_dt = _now_utc()
    source_artifacts: set[str] = set()
    warnings: list[str] = []

    try:
        subject_context = _resolve_subject_context(
            artifact_base_dir=artifact_base_dir,
            profile_name=profile_name,
            feature_version=feature_version,
            ruleset_version=ruleset_version,
            subject_run_id=subject_run_id,
            subject_refresh_payload_path=subject_refresh_payload_path,
            source_artifacts=source_artifacts,
        )
        subject_feature_version = (
            subject_context.sample.feature_version or feature_version
        )
        subject_ruleset_version = (
            subject_context.sample.ruleset_version or ruleset_version
        )

        history_samples = _load_history_samples(
            artifact_base_dir=artifact_base_dir,
            profile_name=subject_context.sample.profile_name,
            source_artifacts=source_artifacts,
            warnings=warnings,
        )
        version_filtered_samples = [
            sample
            for sample in history_samples
            if sample.feature_version == subject_feature_version
            and sample.ruleset_version == subject_ruleset_version
        ]
        history_by_run_id = {
            sample.run_id: sample for sample in version_filtered_samples
        }
        history_by_run_id[subject_context.sample.run_id] = subject_context.sample
        history = sorted(
            history_by_run_id.values(),
            key=lambda sample: (
                sample.sample_event_at or datetime.min.replace(tzinfo=timezone.utc),
                sample.run_id,
                str(sample.path),
            ),
        )

        finished_dt = _now_utc()
        monitor_run_id_value = monitor_run_id or _default_run_id(
            prefix="load_monitor",
            run_date=subject_context.sample.run_date or _utc_today(),
            generated_at=finished_dt,
        )

        window_bounds = _build_window_bounds(finished_dt)
        rollups = _build_rollups(history=history, window_bounds=window_bounds)
        signal_result = _build_signals(
            session=session,
            subject=subject_context.sample,
            history=history,
            window_bounds=window_bounds,
            fallback_feature_version=subject_feature_version,
            fallback_ruleset_version=subject_ruleset_version,
        )

        alerts = _build_alert_summaries(
            run_status="succeeded",
            subject_run_id=subject_context.sample.run_id,
            overall_severity=signal_result["overall_severity"],
            signals=signal_result["signals"],
        )
        return {
            "run": {
                "run_type": RUN_TYPE_LOAD_MONITORING,
                "version_tag": LOAD_MONITORING_VERSION_TAG,
                "run_id": monitor_run_id_value,
                "status": "succeeded",
                "run_persisted": run_persisted,
                "started_at": _iso_utc(started_dt),
                "finished_at": _iso_utc(finished_dt),
            },
            "subject": {
                "run_id": subject_context.sample.run_id,
                "profile_name": subject_context.sample.profile_name,
                "run_date": subject_context.sample.run_date,
                "feature_version": subject_feature_version,
                "ruleset_version": subject_ruleset_version,
                "refresh_payload_path": str(subject_context.refresh_payload_path),
                "refresh_status": _normalized_run_status(
                    subject_context.sample.run_status
                ),
                "refresh_finished_at": _iso_utc_or_none(
                    subject_context.sample.run_finished_at
                ),
            },
            "summary": {
                "overall_severity": signal_result["overall_severity"],
                "signal_count": len(signal_result["signals"]),
                "signal_ok_count": signal_result["signal_ok_count"],
                "signal_warn_count": signal_result["signal_warn_count"],
                "signal_critical_count": signal_result["signal_critical_count"],
                "evaluated_metric_count": signal_result["evaluated_metric_count"],
                "ignored_metric_count": signal_result["ignored_metric_count"],
            },
            "signals": signal_result["signals"],
            "rollups": rollups,
            "alerts": alerts,
            "diagnostics": {
                "warnings": sorted(warnings),
                "source_artifacts": sorted(source_artifacts),
                "history": {
                    "profile_name": subject_context.sample.profile_name,
                    "sample_count_total": len(history),
                    "window_bounds": _window_bounds_to_payload(window_bounds),
                },
                "threshold_overrides": {},
            },
            "error": None,
        }
    except Exception as exc:
        finished_dt = _now_utc()
        run_date = _utc_today()
        run_id_value = monitor_run_id or _default_run_id(
            prefix="load_monitor",
            run_date=run_date,
            generated_at=finished_dt,
        )
        return build_failed_load_diagnostics(
            profile_name=profile_name,
            run_id=run_id_value,
            started_at=started_dt,
            finished_at=finished_dt,
            message=str(exc),
            run_persisted=run_persisted,
        )


def build_failed_load_diagnostics(
    *,
    profile_name: str,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    message: str,
    run_persisted: bool = False,
) -> dict[str, Any]:
    window_bounds = {
        window_key: {"start_at": None, "end_at": None} for window_key, _ in WINDOW_ORDER
    }
    return {
        "run": {
            "run_type": RUN_TYPE_LOAD_MONITORING,
            "version_tag": LOAD_MONITORING_VERSION_TAG,
            "run_id": run_id,
            "status": "failed",
            "run_persisted": run_persisted,
            "started_at": _iso_utc(started_at),
            "finished_at": _iso_utc(finished_at),
        },
        "subject": {
            "run_id": None,
            "profile_name": profile_name,
            "run_date": None,
            "feature_version": None,
            "ruleset_version": None,
            "refresh_payload_path": None,
            "refresh_status": "unknown",
            "refresh_finished_at": None,
        },
        "summary": {
            "overall_severity": "critical",
            "signal_count": 0,
            "signal_ok_count": 0,
            "signal_warn_count": 0,
            "signal_critical_count": 0,
            "evaluated_metric_count": 0,
            "ignored_metric_count": 0,
        },
        "signals": [],
        "rollups": [],
        "alerts": [],
        "diagnostics": {
            "warnings": [],
            "source_artifacts": [],
            "history": {
                "profile_name": profile_name,
                "sample_count_total": 0,
                "window_bounds": window_bounds,
            },
            "threshold_overrides": {},
        },
        "error": {
            "code": "load_monitoring_failed",
            "message": message,
        },
    }


def build_alert_payload_from_diagnostics(
    diagnostics_payload: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    run_payload = _as_dict(diagnostics_payload.get("run"))
    if _as_str(run_payload.get("status")) != "succeeded":
        return None

    summary = _as_dict(diagnostics_payload.get("summary"))
    severity = _as_str(summary.get("overall_severity"))
    if severity not in {"warn", "critical"}:
        return None

    alerts = _as_list(diagnostics_payload.get("alerts"))
    first_alert = _as_dict(alerts[0]) if alerts else {}

    subject = _as_dict(diagnostics_payload.get("subject"))
    signals = _as_list(diagnostics_payload.get("signals"))
    impacted_signals = [
        signal
        for signal in signals
        if _as_str(_as_dict(signal).get("severity")) == severity
        and not _as_bool(_as_dict(signal).get("ignored"))
    ]
    impacted_signals.sort(
        key=lambda signal: (
            _as_str(_as_dict(signal).get("metric_key")) or "",
            _as_str(_as_dict(signal).get("family")) or "",
            _as_str(_as_dict(signal).get("signal_id")) or "",
        )
    )
    reason_codes = sorted(
        {
            reason_code
            for signal in impacted_signals
            for reason_code in [_as_str(_as_dict(signal).get("reason_code"))]
            if reason_code is not None
        }
    )

    refresh_payload_path = _as_str(subject.get("refresh_payload_path"))
    action_paths = [path for path in [refresh_payload_path] if path]
    operator_actions = _build_operator_actions(
        severity=severity,
        action_paths=action_paths,
    )
    subject_run_id = _as_str(subject.get("run_id"))
    alert_id = _as_str(first_alert.get("alert_id")) or (
        f"{subject_run_id}.{severity}" if subject_run_id is not None else None
    )

    return {
        "run": run_payload,
        "alert": {
            "alert_id": alert_id,
            "alert_type": "load_monitoring",
            "severity": severity,
            "generated_at": _utc_now(),
            "subject_run_id": subject_run_id,
            "profile_name": _as_str(subject.get("profile_name")),
            "reason_codes": reason_codes,
            "title": (
                f"Load monitor {severity}: "
                f"{_as_str(subject.get('profile_name')) or 'unknown_profile'}"
            ),
            "summary": (
                f"{len(impacted_signals)} signal(s) at severity {severity} "
                f"for subject run {subject_run_id or 'unknown_run'}."
            ),
        },
        "impacted_signals": impacted_signals,
        "operator_actions": operator_actions,
        "error": None,
    }


def _build_operator_actions(
    *,
    severity: str,
    action_paths: list[str],
) -> list[dict[str, Any]]:
    if severity == "critical":
        definitions = [
            (
                "run_immediate_daily_refresh",
                "Run immediate daily refresh",
                (
                    "Run a daily refresh retry from the failed boundary and verify "
                    "all stages complete."
                ),
                ".venv/bin/accessdane refresh-runner --profile-name daily_refresh",
            ),
            (
                "inspect_ingest_and_drift",
                "Inspect ingest and drift",
                (
                    "Inspect source-file availability and the latest parser drift "
                    "outputs for upstream breakage."
                ),
                "ls data/refresh_runs/latest",
            ),
            (
                "validate_regenerated_artifacts",
                "Validate regenerated artifacts",
                (
                    "Validate score and report artifacts were regenerated for the "
                    "latest refresh run."
                ),
                ".venv/bin/accessdane review-queue --top 10",
            ),
            (
                "open_ops_incident_note",
                "Open ops incident note",
                (
                    "If critical freshness persists for two runs, record an incident "
                    "note in operations logs."
                ),
                None,
            ),
        ]
    else:
        definitions = [
            (
                "review_refresh_payload",
                "Review refresh payload",
                (
                    "Review refresh payload and stage statuses for the subject run "
                    "before taking corrective actions."
                ),
                None,
            ),
            (
                "inspect_parser_drift_diff",
                "Inspect parser drift",
                "Inspect latest parser drift diff artifact when available.",
                "ls data/refresh_runs/*/daily_refresh/*/parser_drift*",
            ),
            (
                "rerun_analysis_only",
                "Rerun analysis-only profile",
                (
                    "If freshness is acceptable but reports look stale, rerun the "
                    "analysis-only profile."
                ),
                ".venv/bin/accessdane refresh-runner --profile-name analysis_only",
            ),
        ]

    actions: list[dict[str, Any]] = []
    for rank, (action_id, title, description, command) in enumerate(
        definitions, start=1
    ):
        actions.append(
            {
                "action_id": action_id,
                "rank": rank,
                "severity": severity,
                "title": title,
                "description": description,
                "message": description,
                "command": command,
                "required_artifact_paths": list(action_paths),
                "artifact_paths": list(action_paths),
                "automatable": False,
            }
        )
    return actions


def _build_signals(
    *,
    session: Session,
    subject: _HistorySample,
    history: list[_HistorySample],
    window_bounds: Mapping[str, Mapping[str, Optional[datetime]]],
    fallback_feature_version: str,
    fallback_ruleset_version: str,
) -> dict[str, Any]:
    subject_feature_version = subject.feature_version or fallback_feature_version
    subject_ruleset_version = subject.ruleset_version or fallback_ruleset_version
    in_review_count = session.execute(
        select(func.count(CaseReview.id)).where(
            CaseReview.feature_version == subject_feature_version,
            CaseReview.ruleset_version == subject_ruleset_version,
            CaseReview.status == "in_review",
        )
    ).scalar_one()
    subject_metrics = _subject_metrics(
        subject=subject,
        case_review_in_review_count=float(in_review_count),
    )

    bounds_30d = window_bounds.get("window_30d", {})
    baseline_candidates = [
        sample
        for sample in history
        if sample.run_status == "succeeded"
        and sample.run_id != subject.run_id
        and _in_window(
            sample.sample_event_at,
            start_at=_as_datetime(bounds_30d.get("start_at")),
            end_at=_as_datetime(bounds_30d.get("end_at")),
        )
    ]
    baseline_values_by_metric = _baseline_values_by_metric(baseline_candidates)

    bounds_7d = window_bounds.get("window_7d", {})
    window_7d_samples = [
        sample
        for sample in history
        if _in_window(
            sample.sample_event_at,
            start_at=_as_datetime(bounds_7d.get("start_at")),
            end_at=_as_datetime(bounds_7d.get("end_at")),
        )
    ]

    freshness_values = _freshness_metrics(
        history=history,
        window_bounds=window_bounds,
        now_utc=_as_datetime(window_bounds["window_1d"]["end_at"]) or _now_utc(),
    )

    signals: list[dict[str, Any]] = []
    for metric_key, family in _METRIC_KEYS:
        subject_value = subject_metrics.get(metric_key)
        sample_size = 1
        ignored = False
        ignore_reason: Optional[str] = None
        baseline_value: Optional[float] = None
        delta_absolute: Optional[float] = None
        delta_relative: Optional[float] = None
        severity = "ok"

        unavailable_for_profile = _metric_unavailable_for_profile(
            metric_key=metric_key,
            subject=subject,
        )
        if unavailable_for_profile:
            ignored = True
            ignore_reason = "metric_unavailable_for_profile"
            sample_size = 0
        elif metric_key.startswith("failure_rate."):
            denominator = len(window_7d_samples)
            sample_size = denominator
            subject_value = _failure_rate_metric_value(metric_key, window_7d_samples)
            if denominator == 0:
                subject_value = None
                ignored = True
                ignore_reason = "insufficient_history"
                sample_size = 0
            elif denominator < 3:
                ignored = True
                ignore_reason = "insufficient_history"
            if not ignored and subject_value is not None:
                severity = _severity_for_failure_rate(metric_key, subject_value)
        elif metric_key.startswith("freshness."):
            freshness_payload = freshness_values.get(metric_key)
            raw_freshness_value = _as_dict(freshness_payload).get("subject_value")
            subject_value = _as_half_up_float(raw_freshness_value, scale=2)
            sample_size = _as_int(_as_dict(freshness_payload).get("sample_size"))
            if subject_value is None:
                ignored = True
                ignore_reason = "insufficient_history"
                sample_size = 0
            else:
                severity = _severity_for_freshness(metric_key, subject_value)
        elif metric_key == "queue_size.case_review.in_review_count":
            baseline_value = None
            delta_absolute = None
            delta_relative = None
            if subject_value is None:
                ignored = True
                ignore_reason = "missing_subject_value"
                sample_size = 0
            else:
                severity = _severity_for_case_review_in_review_count(subject_value)
        else:
            baseline_series = baseline_values_by_metric.get(metric_key, [])
            baseline_value = _nearest_rank_percentile(baseline_series, percentile=50)
            if subject_value is None:
                ignored = True
                ignore_reason = "missing_subject_value"
                sample_size = 0
            elif len(baseline_series) < 3 or baseline_value is None:
                ignored = True
                ignore_reason = "insufficient_history"
            elif baseline_value == 0:
                ignored = True
                ignore_reason = "invalid_baseline"
            else:
                delta_absolute = _as_float(subject_value - baseline_value)
                delta_relative = _as_float(
                    (subject_value - baseline_value) / baseline_value
                )
                severity = _severity_for_relative_delta(
                    family=family,
                    metric_key=metric_key,
                    delta_relative=delta_relative,
                )

        if ignored:
            severity = "ok"

        reason_token = _reason_token(
            severity=severity,
            family=family,
            ignore_reason=ignore_reason,
        )
        reason_code = f"{family}.{reason_token}.{metric_key}"
        if not ignored and metric_key.startswith(("failure_rate.", "freshness.")):
            baseline_value = None
            delta_absolute = None
            delta_relative = None
        if ignored and subject_value is None:
            sample_size = 0

        signals.append(
            {
                "signal_id": f"{family}:{metric_key}",
                "metric_key": metric_key,
                "family": family,
                "subject_value": subject_value,
                "baseline_value": baseline_value,
                "delta_absolute": delta_absolute,
                "delta_relative": delta_relative,
                "sample_size": sample_size,
                "severity": severity,
                "reason_code": reason_code,
                "ignored": ignored,
                "ignore_reason": ignore_reason,
            }
        )

    signals.sort(
        key=lambda signal: (
            signal["metric_key"],
            signal["family"],
            signal["signal_id"],
        )
    )

    evaluated_signals = [
        signal for signal in signals if not _as_bool(signal["ignored"])
    ]
    overall_severity = "ok"
    if any(signal["severity"] == "critical" for signal in evaluated_signals):
        overall_severity = "critical"
    elif any(signal["severity"] == "warn" for signal in evaluated_signals):
        overall_severity = "warn"

    return {
        "signals": signals,
        "overall_severity": overall_severity,
        "signal_ok_count": sum(1 for signal in signals if signal["severity"] == "ok"),
        "signal_warn_count": sum(
            1 for signal in signals if signal["severity"] == "warn"
        ),
        "signal_critical_count": sum(
            1 for signal in signals if signal["severity"] == "critical"
        ),
        "evaluated_metric_count": len(evaluated_signals),
        "ignored_metric_count": len(signals) - len(evaluated_signals),
    }


def _build_alert_summaries(
    *,
    run_status: str,
    subject_run_id: str,
    overall_severity: str,
    signals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if run_status != "succeeded":
        return []
    if overall_severity not in {"warn", "critical"}:
        return []

    impacted_signals = [
        signal
        for signal in signals
        if signal["severity"] == overall_severity and not _as_bool(signal["ignored"])
    ]
    reason_codes = sorted(
        reason_code
        for reason_code in (
            _as_str(signal["reason_code"]) for signal in impacted_signals
        )
        if reason_code is not None
    )
    return [
        {
            "alert_id": f"{subject_run_id}.{overall_severity}",
            "alert_type": "load_monitoring",
            "severity": overall_severity,
            "generated_at": _utc_now(),
            "reason_codes": reason_codes,
            "signal_count": len(impacted_signals),
        }
    ]


def _build_rollups(
    *,
    history: list[_HistorySample],
    window_bounds: Mapping[str, Mapping[str, Optional[datetime]]],
) -> list[dict[str, Any]]:
    rollups: list[dict[str, Any]] = []
    for window_key, window_hours in WINDOW_ORDER:
        bounds = _as_dict(window_bounds.get(window_key))
        start_at = _as_datetime(bounds.get("start_at"))
        end_at = _as_datetime(bounds.get("end_at"))
        samples = [
            sample
            for sample in history
            if _in_window(sample.sample_event_at, start_at=start_at, end_at=end_at)
        ]
        sample_count = len(samples)
        successful_run_count = sum(
            1 for sample in samples if sample.run_status == "succeeded"
        )
        failed_run_count = max(sample_count - successful_run_count, 0)
        duration_values = [
            value
            for value in [sample.duration_total_seconds for sample in samples]
            if value is not None
        ]
        review_queue_values = [
            value
            for value in [sample.review_queue_returned_count for sample in samples]
            if value is not None
        ]
        rollups.append(
            {
                "window_key": window_key,
                "window_hours": window_hours,
                "sample_count": sample_count,
                "successful_run_count": successful_run_count,
                "failed_run_count": failed_run_count,
                "duration_total_p50_seconds": _percentile_with_minimum(
                    duration_values, percentile=50
                ),
                "duration_total_p95_seconds": _percentile_with_minimum(
                    duration_values, percentile=95
                ),
                "review_queue_returned_p50": _percentile_with_minimum(
                    review_queue_values, percentile=50
                ),
                "review_queue_returned_p95": _percentile_with_minimum(
                    review_queue_values, percentile=95
                ),
            }
        )
    return rollups


def _subject_metrics(
    *,
    subject: _HistorySample,
    case_review_in_review_count: float,
) -> dict[str, Optional[float]]:
    metrics: dict[str, Optional[float]] = {
        "duration.total_seconds": subject.duration_total_seconds,
        "duration.stage.ingest_context.seconds": _stage_duration_metric(
            subject, "ingest_context"
        ),
        "duration.stage.build_context.seconds": _stage_duration_metric(
            subject, "build_context"
        ),
        "duration.stage.score_pipeline.seconds": _stage_duration_metric(
            subject, "score_pipeline"
        ),
        "duration.stage.analysis_artifacts.seconds": _stage_duration_metric(
            subject, "analysis_artifacts"
        ),
        "duration.stage.investigation_artifacts.seconds": _stage_duration_metric(
            subject, "investigation_artifacts"
        ),
        "duration.stage.health_summary.seconds": _stage_duration_metric(
            subject, "health_summary"
        ),
        "volume.review_queue.returned_count": subject.review_queue_returned_count,
        "volume.review_feedback.reviewed_case_count": (
            subject.review_feedback_reviewed_case_count
        ),
        "volume.review_feedback.recommendation_count": (
            subject.review_feedback_recommendation_count
        ),
        "queue_size.review_queue.high_risk_count": subject.review_queue_high_risk_count,
        "queue_size.review_queue.unreviewed_count": (
            subject.review_queue_unreviewed_count
        ),
        "queue_size.case_review.in_review_count": case_review_in_review_count,
    }
    return {metric_key: _as_float(value) for metric_key, value in metrics.items()}


def _baseline_values_by_metric(
    baseline_candidates: list[_HistorySample],
) -> dict[str, list[float]]:
    values: dict[str, list[float]] = {}
    for metric_key, _ in _METRIC_KEYS:
        if metric_key.startswith(("failure_rate.", "freshness.")):
            continue
        if metric_key == "queue_size.case_review.in_review_count":
            continue
        for sample in baseline_candidates:
            sample_metrics = _subject_metrics(
                subject=sample, case_review_in_review_count=0.0
            )
            metric_value = sample_metrics.get(metric_key)
            if metric_value is None:
                continue
            values.setdefault(metric_key, []).append(metric_value)
    return values


def _failure_rate_metric_value(
    metric_key: str,
    samples: list[_HistorySample],
) -> Optional[float]:
    denominator = len(samples)
    if denominator == 0:
        return None
    if metric_key == "failure_rate.run.failed_fraction_7d":
        numerator = sum(1 for sample in samples if sample.run_status == "failed")
    elif metric_key == "failure_rate.stage.failed_fraction_7d":
        numerator = sum(
            1
            for sample in samples
            if any(status == "failed" for status in sample.stage_status_by_id.values())
        )
    else:
        numerator = sum(
            1
            for sample in samples
            if any(status == "blocked" for status in sample.stage_status_by_id.values())
        )
    return _as_float(numerator / denominator)


def _freshness_metrics(
    *,
    history: list[_HistorySample],
    window_bounds: Mapping[str, Mapping[str, Optional[datetime]]],
    now_utc: datetime,
) -> dict[str, dict[str, object]]:
    bounds = _as_dict(window_bounds.get("window_30d"))
    start_at = _as_datetime(bounds.get("start_at"))
    end_at = _as_datetime(bounds.get("end_at"))
    samples = [
        sample
        for sample in history
        if _in_window(sample.sample_event_at, start_at=start_at, end_at=end_at)
    ]

    def _daily_success(sample: _HistorySample) -> Optional[datetime]:
        if sample.run_status != "succeeded":
            return None
        return sample.run_finished_at

    def _score_success(sample: _HistorySample) -> Optional[datetime]:
        if sample.stage_status_by_id.get("score_pipeline") != "succeeded":
            return None
        return sample.stage_finished_at_by_id.get("score_pipeline")

    def _feedback_success(sample: _HistorySample) -> Optional[datetime]:
        status = sample.review_feedback_command_status_by_stage.get(
            "analysis_artifacts"
        )
        if status != "succeeded":
            return None
        return sample.stage_finished_at_by_id.get("analysis_artifacts")

    metric_map = {
        "freshness.hours_since_last_successful_daily_refresh": _daily_success,
        "freshness.hours_since_last_successful_score_pipeline": _score_success,
        "freshness.hours_since_last_successful_review_feedback": _feedback_success,
    }
    result: dict[str, dict[str, object]] = {}
    for metric_key, resolver in metric_map.items():
        timestamps = [
            value
            for value in (resolver(sample) for sample in samples)
            if value is not None
        ]
        if not timestamps:
            result[metric_key] = {"subject_value": None, "sample_size": 0}
            continue
        latest_success = max(timestamps)
        hours = max((now_utc - latest_success).total_seconds() / 3600.0, 0.0)
        result[metric_key] = {
            "subject_value": _round_half_up(hours, scale=2),
            "sample_size": len(timestamps),
        }
    return result


def _severity_for_relative_delta(
    *,
    family: str,
    metric_key: str,
    delta_relative: Optional[float],
) -> str:
    if delta_relative is None:
        return "ok"
    if family == "duration":
        if delta_relative >= 1.0:
            return "critical"
        if delta_relative >= 0.5:
            return "warn"
        return "ok"
    if family in {"volume", "queue_size"}:
        absolute_relative = abs(delta_relative)
        if absolute_relative >= 0.60:
            return "critical"
        if absolute_relative >= 0.35:
            return "warn"
        return "ok"
    return "ok"


def _severity_for_failure_rate(metric_key: str, subject_value: float) -> str:
    if subject_value >= 0.20:
        return "critical"
    if subject_value >= 0.10:
        return "warn"
    return "ok"


def _severity_for_freshness(metric_key: str, subject_value: float) -> str:
    if metric_key == "freshness.hours_since_last_successful_daily_refresh":
        if subject_value >= 48:
            return "critical"
        if subject_value >= 30:
            return "warn"
        return "ok"
    if metric_key == "freshness.hours_since_last_successful_score_pipeline":
        if subject_value >= 60:
            return "critical"
        if subject_value >= 36:
            return "warn"
        return "ok"
    if subject_value >= 72:
        return "critical"
    if subject_value >= 48:
        return "warn"
    return "ok"


def _severity_for_case_review_in_review_count(subject_value: float) -> str:
    if subject_value >= 50:
        return "critical"
    if subject_value >= 25:
        return "warn"
    return "ok"


def _reason_token(
    *,
    severity: str,
    family: str,
    ignore_reason: Optional[str],
) -> str:
    if ignore_reason == "metric_unavailable_for_profile":
        return "ignored_unavailable_profile"
    if ignore_reason == "insufficient_history":
        return "ignored_insufficient_history"
    if ignore_reason == "missing_subject_value":
        return "ignored_missing_subject_value"
    if ignore_reason == "invalid_baseline":
        return "ignored_invalid_baseline"
    if family == "freshness" and severity == "critical":
        return "critical_freshness"
    if family == "freshness" and severity == "warn":
        return "warn_freshness"
    if family == "failure_rate" and severity == "critical":
        return "critical_failure_rate"
    if family == "failure_rate" and severity == "warn":
        return "warn_failure_rate"
    if severity == "critical":
        return "critical_relative_delta"
    if severity == "warn":
        return "warn_relative_delta"
    return "ok_within_threshold"


def _metric_unavailable_for_profile(
    *,
    metric_key: str,
    subject: _HistorySample,
) -> bool:
    if not metric_key.startswith("duration.stage."):
        return False
    stage_id = metric_key.split(".")[2]
    stage_status = subject.stage_status_by_id.get(stage_id)
    if stage_status != "skipped":
        return False
    skip_reason = subject.skip_reason_by_stage_id.get(stage_id)
    return bool(skip_reason and skip_reason.startswith("profile_skip:"))


def _stage_duration_metric(subject: _HistorySample, stage_id: str) -> Optional[float]:
    stage_duration = subject.stage_duration_seconds_by_id.get(stage_id)
    if stage_duration is not None:
        return _as_float(stage_duration)

    # Fallback to timestamp deltas when duration_seconds is unavailable.
    stage_started = subject.stage_started_at_by_id.get(stage_id)
    stage_finished = subject.stage_finished_at_by_id.get(stage_id)
    if stage_started is None or stage_finished is None:
        return None
    return _as_float(max((stage_finished - stage_started).total_seconds(), 0.0))


def _resolve_subject_context(
    *,
    artifact_base_dir: Path,
    profile_name: str,
    feature_version: str,
    ruleset_version: str,
    subject_run_id: Optional[str],
    subject_refresh_payload_path: Optional[Path],
    source_artifacts: set[str],
) -> _SubjectContext:
    if subject_refresh_payload_path is not None:
        sample = _load_refresh_sample(subject_refresh_payload_path, source_artifacts)
        return _SubjectContext(
            sample=sample, refresh_payload_path=subject_refresh_payload_path
        )

    if subject_run_id:
        for path in sorted(
            artifact_base_dir.glob(
                f"*/{profile_name}/{subject_run_id}/health_summary/refresh_run_payload.json"
            )
        ):
            sample = _load_refresh_sample(path, source_artifacts)
            if sample.run_id == subject_run_id:
                return _SubjectContext(sample=sample, refresh_payload_path=path)
        raise ValueError(f"Unable to resolve subject run_id '{subject_run_id}'.")

    latest_pointer = (
        artifact_base_dir
        / "latest"
        / profile_name
        / feature_version
        / ruleset_version
        / "latest_run.json"
    )
    if not latest_pointer.exists():
        raise ValueError(
            f"Unable to resolve latest pointer for profile '{profile_name}' at "
            f"{latest_pointer}."
        )
    source_artifacts.add(str(latest_pointer))
    latest_payload = _read_json(latest_pointer)
    root_path = _as_str(_as_dict(latest_payload).get("root_path"))
    if root_path is None:
        raise ValueError("latest_run.json is missing root_path.")
    subject_path = Path(root_path) / "health_summary" / "refresh_run_payload.json"
    sample = _load_refresh_sample(subject_path, source_artifacts)
    return _SubjectContext(sample=sample, refresh_payload_path=subject_path)


def _load_history_samples(
    *,
    artifact_base_dir: Path,
    profile_name: str,
    source_artifacts: set[str],
    warnings: list[str],
) -> list[_HistorySample]:
    samples: list[_HistorySample] = []
    for path in sorted(
        artifact_base_dir.glob(
            "*/" + profile_name + "/*/health_summary/refresh_run_payload.json"
        )
    ):
        try:
            samples.append(_load_refresh_sample(path, source_artifacts))
        except (json.JSONDecodeError, OSError, ValueError, TypeError) as exc:
            warnings.append(
                "history_sample_skipped:"
                f"{path}:{type(exc).__name__}:"
                f"{str(exc).replace(chr(10), ' ')}"
            )
            continue
    return samples


def _load_refresh_sample(path: Path, source_artifacts: set[str]) -> _HistorySample:
    source_artifacts.add(str(path))
    payload = _read_json(path)
    run = _as_dict(payload.get("run"))
    request = _as_dict(payload.get("request"))
    summary = _as_dict(payload.get("summary"))
    stages = _as_list(payload.get("stages"))
    diagnostics = _as_dict(payload.get("diagnostics"))

    run_id = _as_str(run.get("run_id"))
    if run_id is None:
        raise ValueError(f"Refresh payload at {path} is missing run.run_id.")

    run_status = _normalized_run_status(_as_str(run.get("status")) or "unknown")
    run_started_at = _as_datetime(run.get("started_at"))
    run_finished_at = _as_datetime(run.get("finished_at"))
    sample_event_at = run_finished_at or run_started_at

    stage_status_by_id: dict[str, str] = {}
    stage_duration_seconds_by_id: dict[str, Optional[float]] = {}
    stage_started_at_by_id: dict[str, Optional[datetime]] = {}
    stage_finished_at_by_id: dict[str, Optional[datetime]] = {}
    review_feedback_command_status_by_stage: dict[str, str] = {}
    for stage in stages:
        stage_payload = _as_dict(stage)
        stage_id = _as_str(stage_payload.get("stage_id"))
        if stage_id is None:
            continue
        stage_status_by_id[stage_id] = _as_str(stage_payload.get("status")) or "unknown"
        stage_duration_seconds_by_id[stage_id] = _as_float(
            stage_payload.get("duration_seconds")
        )
        stage_started_at_by_id[stage_id] = _as_datetime(stage_payload.get("started_at"))
        stage_finished_at_by_id[stage_id] = _as_datetime(
            stage_payload.get("finished_at")
        )
        command_results = _as_list(stage_payload.get("command_results"))
        for result in command_results:
            result_payload = _as_dict(result)
            command_id = _as_str(result_payload.get("command_id"))
            if command_id != "review_feedback":
                continue
            review_feedback_command_status_by_stage[stage_id] = (
                _as_str(result_payload.get("status")) or "unknown"
            )

    skip_reason_by_stage_id: dict[str, str] = {}
    for skip in _as_list(diagnostics.get("skip_reasons")):
        skip_payload = _as_dict(skip)
        stage_id = _as_str(skip_payload.get("stage_id"))
        reason = _as_str(skip_payload.get("reason"))
        if stage_id is None or reason is None:
            continue
        skip_reason_by_stage_id[stage_id] = reason

    root_path = _as_str(_as_dict(payload.get("artifacts")).get("root_path"))
    if root_path is None:
        root_path = str(path.parent.parent)
    analysis_dir = Path(root_path) / "analysis_artifacts"
    review_queue_path = analysis_dir / "review_queue.json"
    review_feedback_path = analysis_dir / "review_feedback.json"
    review_queue_payload: dict[str, Any] = {}
    review_feedback_payload: dict[str, Any] = {}
    if review_queue_path.exists():
        source_artifacts.add(str(review_queue_path))
        review_queue_payload = _read_json(review_queue_path)
    if review_feedback_path.exists():
        source_artifacts.add(str(review_feedback_path))
        review_feedback_payload = _read_json(review_feedback_path)

    review_queue_summary = _as_dict(review_queue_payload.get("summary"))
    review_feedback_summary = _as_dict(review_feedback_payload.get("summary"))
    review_queue_rows = _as_list(review_queue_payload.get("rows"))
    high_risk_count = sum(
        1
        for row in review_queue_rows
        if _as_str(_as_dict(row).get("risk_band")) == "high"
    )
    unreviewed_count = sum(
        1
        for row in review_queue_rows
        if _as_str(_as_dict(row).get("review_status")) == "unreviewed"
    )

    recommendations = _as_dict(review_feedback_payload.get("recommendations"))
    threshold_candidates = _as_list(recommendations.get("threshold_tuning_candidates"))
    exclusion_candidates = _as_list(recommendations.get("exclusion_tuning_candidates"))

    return _HistorySample(
        path=path,
        run_id=run_id,
        profile_name=_as_str(run.get("profile_name")) or "unknown",
        run_status=run_status,
        run_started_at=run_started_at,
        run_finished_at=run_finished_at,
        sample_event_at=sample_event_at,
        duration_total_seconds=_as_float(summary.get("duration_seconds_total")),
        stage_status_by_id=stage_status_by_id,
        stage_duration_seconds_by_id=stage_duration_seconds_by_id,
        stage_started_at_by_id=stage_started_at_by_id,
        stage_finished_at_by_id=stage_finished_at_by_id,
        review_feedback_command_status_by_stage=review_feedback_command_status_by_stage,
        review_queue_returned_count=_as_float(
            review_queue_summary.get("returned_count")
        ),
        review_queue_high_risk_count=_as_float(high_risk_count),
        review_queue_unreviewed_count=_as_float(unreviewed_count),
        review_feedback_reviewed_case_count=_as_float(
            review_feedback_summary.get("reviewed_case_count")
        ),
        review_feedback_recommendation_count=_as_float(
            len(threshold_candidates) + len(exclusion_candidates)
        ),
        feature_version=_as_str(request.get("feature_version")),
        ruleset_version=_as_str(request.get("ruleset_version")),
        run_date=_as_str(request.get("run_date")),
        skip_reason_by_stage_id=skip_reason_by_stage_id,
    )


def _build_window_bounds(
    end_at: datetime,
) -> dict[str, dict[str, Optional[datetime]]]:
    result: dict[str, dict[str, Optional[datetime]]] = {}
    for window_key, window_hours in WINDOW_ORDER:
        result[window_key] = {
            "start_at": end_at - timedelta(hours=window_hours),
            "end_at": end_at,
        }
    return result


def _window_bounds_to_payload(
    bounds: Mapping[str, Mapping[str, Optional[datetime]]],
) -> dict[str, dict[str, Optional[str]]]:
    payload: dict[str, dict[str, Optional[str]]] = {}
    for window_key, _ in WINDOW_ORDER:
        window = _as_dict(bounds.get(window_key))
        payload[window_key] = {
            "start_at": _iso_utc_or_none(_as_datetime(window.get("start_at"))),
            "end_at": _iso_utc_or_none(_as_datetime(window.get("end_at"))),
        }
    return payload


def _in_window(
    value: Optional[datetime],
    *,
    start_at: Optional[datetime],
    end_at: Optional[datetime],
) -> bool:
    if value is None or start_at is None or end_at is None:
        return False
    return value > start_at and value <= end_at


def _read_json(path: Path) -> dict[str, Any]:
    return _as_dict(json.loads(path.read_text(encoding="utf-8")))


def _percentile_with_minimum(
    values: list[float],
    *,
    percentile: int,
) -> Optional[float]:
    if len(values) < 5:
        return None
    return _nearest_rank_percentile(values, percentile=percentile)


def _nearest_rank_percentile(
    values: list[float],
    *,
    percentile: int,
) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(int(math.ceil((percentile / 100) * len(ordered))), 1)
    return _as_float(ordered[rank - 1])


def _normalized_run_status(value: str) -> str:
    if value in {"succeeded", "failed"}:
        return value
    return "unknown"


def _round_half_up(value: float, *, scale: int) -> float:
    quant = Decimal("1").scaleb(-scale)
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _utc_today() -> str:
    return _now_utc().strftime("%Y%m%d")


def _utc_now() -> str:
    return _iso_utc(_now_utc())


def _iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _iso_utc_or_none(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return _iso_utc(value)


def _default_run_id(*, prefix: str, run_date: str, generated_at: datetime) -> str:
    return f"{run_date}_{prefix}_{generated_at.strftime('%Y%m%d_%H%M%S')}"


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0
        try:
            return int(float(stripped))
        except ValueError:
            return 0
    return 0


def _as_float(value: Any, *, scale: int = 4) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return round(float(value), scale)
    if isinstance(value, (int, float)):
        return round(float(value), scale)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return round(float(stripped), scale)
        except ValueError:
            return None
    return None


def _as_half_up_float(value: Any, *, scale: int) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return _round_half_up(float(value), scale=scale)
    if isinstance(value, (int, float)):
        return _round_half_up(float(value), scale=scale)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return _round_half_up(float(stripped), scale=scale)
        except ValueError:
            return None
    return None


def _as_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        normalized = stripped.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None
