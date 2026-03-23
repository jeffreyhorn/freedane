from __future__ import annotations

import json
import os
import re
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from .case_review import (
    DISPOSITION_CONFIRMED_ISSUE,
    DISPOSITION_DUPLICATE_CASE,
    DISPOSITION_FALSE_POSITIVE,
    DISPOSITION_INCONCLUSIVE,
    DISPOSITION_NEEDS_FIELD_REVIEW,
)
from .models import ParcelCharacteristic
from .review_queue import build_review_queue

RUN_TYPE_BENCHMARK_PACK = "benchmark_pack"
BENCHMARK_PACK_VERSION_TAG = "benchmark_pack_v1"
_SAFE_ARTIFACT_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_SAFE_SEGMENT_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_SEGMENT_COMPONENT_CLEANUP_RE = re.compile(r"[^A-Za-z0-9_-]+")
_ROUTING_COMPONENT_CLEANUP_RE = re.compile(r"[^A-Za-z0-9_.-]+")

_RISK_BANDS: tuple[str, ...] = ("low", "medium", "high")
_REVIEW_DISPOSITIONS: tuple[str, ...] = (
    DISPOSITION_CONFIRMED_ISSUE,
    DISPOSITION_FALSE_POSITIVE,
    DISPOSITION_INCONCLUSIVE,
    DISPOSITION_NEEDS_FIELD_REVIEW,
    DISPOSITION_DUPLICATE_CASE,
)
_DISPOSITION_KEYS: tuple[str, ...] = _REVIEW_DISPOSITIONS + ("unreviewed",)

_RUN_SIGNAL_METRIC_KEYS: tuple[str, ...] = (
    "coverage.review_rate",
    "risk_band_mix.high.rate",
    "risk_band_mix.medium.rate",
    "disposition_mix.false_positive.rate",
    "disposition_mix.confirmed_issue.rate",
)


def build_benchmark_pack(
    session: Session,
    *,
    profile_name: str,
    run_date: str,
    feature_version: str,
    ruleset_version: str,
    top_n: int,
    artifact_base_dir: Path = Path("data/benchmark_packs"),
    source_artifacts: Optional[Iterable[str]] = None,
    benchmark_run_id: Optional[str] = None,
    baseline_path: Optional[Path] = None,
    period_start: Optional[datetime] = None,
    period_end: Optional[datetime] = None,
    source_run_id: Optional[str] = None,
    run_persisted: bool = False,
) -> dict[str, Any]:
    started_at = _now_utc()
    resolved_run_date = _validate_run_date(run_date)
    resolved_profile_name = _validate_artifact_component(
        profile_name, name="profile_name"
    )
    resolved_feature_version = _validate_artifact_component(
        feature_version, name="feature_version"
    )
    resolved_ruleset_version = _validate_artifact_component(
        ruleset_version, name="ruleset_version"
    )
    resolved_top_n = _validate_top_n(top_n)
    resolved_run_id = _validate_artifact_component(
        benchmark_run_id
        or _default_benchmark_run_id(
            run_date=resolved_run_date,
            profile_name=resolved_profile_name,
            feature_version=resolved_feature_version,
            ruleset_version=resolved_ruleset_version,
            generated_at=started_at,
        ),
        name="benchmark_run_id",
    )
    resolved_period_end = (period_end or started_at).astimezone(timezone.utc)
    resolved_period_start = (
        period_start or (resolved_period_end - timedelta(days=6))
    ).astimezone(timezone.utc)
    if resolved_period_start > resolved_period_end:
        raise ValueError("period_start must be less than or equal to period_end.")
    resolved_source_artifacts = sorted({str(path) for path in source_artifacts or []})

    try:
        review_queue_payload = build_review_queue(
            session,
            top=resolved_top_n,
            feature_version=resolved_feature_version,
            ruleset_version=resolved_ruleset_version,
            requires_review_only=True,
        )
        run_payload = _as_dict(review_queue_payload.get("run"))
        if _as_str(run_payload.get("status")) != "succeeded":
            review_queue_error = _as_dict(review_queue_payload.get("error"))
            message = _as_str(review_queue_error.get("message"))
            raise ValueError(message or "Failed to build review queue for benchmark.")

        queue_rows = [
            _as_dict(row) for row in _as_list(review_queue_payload.get("rows"))
        ]
        parcel_ids = [
            parcel_id
            for parcel_id in (_as_str(row.get("parcel_id")) for row in queue_rows)
            if parcel_id is not None
        ]
        characteristics = _load_characteristics_by_parcel_id(session, parcel_ids)
        summary, segments = _build_summary_and_segments(queue_rows, characteristics)

        (
            baseline_payload,
            baseline_path_used,
            baseline_non_comparable_reasons,
        ) = _resolve_baseline_payload(
            artifact_base_dir=artifact_base_dir,
            profile_name=resolved_profile_name,
            feature_version=resolved_feature_version,
            ruleset_version=resolved_ruleset_version,
            baseline_path=baseline_path,
        )
        comparison = _build_comparison(
            baseline_payload=baseline_payload,
            summary=summary,
            segments=segments,
            profile_name=resolved_profile_name,
            feature_version=resolved_feature_version,
            ruleset_version=resolved_ruleset_version,
            top_n=resolved_top_n,
            period_length_days=_period_length_days(
                period_start=resolved_period_start,
                period_end=resolved_period_end,
            ),
            baseline_non_comparable_reasons=baseline_non_comparable_reasons,
        )
        finished_at = _now_utc()
        alerts = _build_alerts(
            run_id=resolved_run_id,
            finished_at=finished_at,
            signals=_as_list(comparison.get("signals")),
        )
        if _as_str(comparison.get("overall_severity")) == "ok":
            alerts = []

        run_source_run_id = source_run_id or _derive_source_run_id(queue_rows)
        diagnostics_source_artifacts = sorted(
            set(
                resolved_source_artifacts
                + [str(path) for path in [baseline_path_used] if path is not None]
            )
        )
        return {
            "run": {
                "run_type": RUN_TYPE_BENCHMARK_PACK,
                "version_tag": BENCHMARK_PACK_VERSION_TAG,
                "run_id": resolved_run_id,
                "benchmark_run_id": resolved_run_id,
                "status": "succeeded",
                "run_persisted": run_persisted,
                "started_at": _iso_utc(started_at),
                "finished_at": _iso_utc(finished_at),
            },
            "scope": {
                "profile_name": resolved_profile_name,
                "run_date": resolved_run_date,
                "feature_version": resolved_feature_version,
                "ruleset_version": resolved_ruleset_version,
                "source_run_id": run_source_run_id,
                "source_artifacts": resolved_source_artifacts,
                "period_start": _iso_utc(resolved_period_start),
                "period_end": _iso_utc(resolved_period_end),
                "period_length_days": _period_length_days(
                    period_start=resolved_period_start,
                    period_end=resolved_period_end,
                ),
                "top_n": resolved_top_n,
            },
            "summary": summary,
            "segments": segments,
            "comparison": comparison,
            "alerts": alerts,
            "diagnostics": {
                "schema_version": BENCHMARK_PACK_VERSION_TAG,
                "generator_name": "accessdane_audit.benchmark_pack",
                "generator_version": "v1",
                "input_artifacts": diagnostics_source_artifacts,
                "runtime": {
                    "duration_ms": max(
                        int((finished_at - started_at).total_seconds() * 1000), 0
                    ),
                    "cpu_seconds": 0.0,
                },
                "counters": {
                    "total_segments": len(segments),
                    "skipped_segments": sum(
                        1
                        for segment in segments
                        if _as_int(_as_dict(segment).get("queue_parcel_count")) < 25
                    ),
                },
                "failure": None,
            },
            "error": None,
        }
    except Exception as exc:
        finished_at = _now_utc()
        return build_failed_benchmark_pack(
            run_id=resolved_run_id,
            profile_name=resolved_profile_name,
            run_date=resolved_run_date,
            feature_version=resolved_feature_version,
            ruleset_version=resolved_ruleset_version,
            source_run_id=source_run_id,
            source_artifacts=resolved_source_artifacts,
            period_start=resolved_period_start,
            period_end=resolved_period_end,
            top_n=resolved_top_n,
            started_at=started_at,
            finished_at=finished_at,
            message=str(exc),
            run_persisted=run_persisted,
        )


def build_failed_benchmark_pack(
    *,
    run_id: str,
    profile_name: str,
    run_date: str,
    feature_version: str,
    ruleset_version: str,
    source_run_id: Optional[str],
    source_artifacts: list[str],
    period_start: datetime,
    period_end: datetime,
    top_n: int,
    started_at: datetime,
    finished_at: datetime,
    message: str,
    run_persisted: bool = False,
) -> dict[str, Any]:
    return {
        "run": {
            "run_type": RUN_TYPE_BENCHMARK_PACK,
            "version_tag": BENCHMARK_PACK_VERSION_TAG,
            "run_id": run_id,
            "benchmark_run_id": run_id,
            "status": "failed",
            "run_persisted": run_persisted,
            "started_at": _iso_utc(started_at),
            "finished_at": _iso_utc(finished_at),
        },
        "scope": {
            "profile_name": profile_name,
            "run_date": run_date,
            "feature_version": feature_version,
            "ruleset_version": ruleset_version,
            "source_run_id": source_run_id,
            "source_artifacts": list(source_artifacts),
            "period_start": _iso_utc(period_start),
            "period_end": _iso_utc(period_end),
            "period_length_days": _period_length_days(
                period_start=period_start,
                period_end=period_end,
            ),
            "top_n": top_n,
        },
        "summary": _empty_summary(),
        "segments": [],
        "comparison": {
            "baseline_reference": None,
            "comparable": False,
            "non_comparable_reasons": ["run_failed"],
            "signals": [],
            "overall_severity": "ok",
        },
        "alerts": [],
        "diagnostics": {
            "schema_version": BENCHMARK_PACK_VERSION_TAG,
            "generator_name": "accessdane_audit.benchmark_pack",
            "generator_version": "v1",
            "input_artifacts": sorted(source_artifacts),
            "runtime": {
                "duration_ms": max(
                    int((finished_at - started_at).total_seconds() * 1000), 0
                ),
                "cpu_seconds": 0.0,
            },
            "counters": {
                "total_segments": 0,
                "skipped_segments": 0,
            },
            "failure": {
                "stage": "compute_benchmark",
                "class": "unexpected",
                "retryable": False,
                "details": {},
            },
        },
        "error": {
            "code": "benchmark_pack_failed",
            "message": message,
        },
    }


def build_benchmark_trend_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    run = _as_dict(payload.get("run"))
    scope = _as_dict(payload.get("scope"))
    comparison = _as_dict(payload.get("comparison"))
    signals = [_as_dict(signal) for signal in _as_list(comparison.get("signals"))]
    signals.sort(
        key=lambda signal: (
            _as_str(signal.get("metric_key")) or "",
            _as_str(signal.get("family")) or "",
            _as_str(signal.get("signal_id")) or "",
        )
    )

    return {
        "generated_at": _as_str(run.get("finished_at")),
        "run_id": _as_str(run.get("run_id")),
        "profile_name": _as_str(scope.get("profile_name")),
        "feature_version": _as_str(scope.get("feature_version")),
        "ruleset_version": _as_str(scope.get("ruleset_version")),
        "overall_severity": _as_str(comparison.get("overall_severity")) or "ok",
        "series": [
            {
                "metric_key": _as_str(signal.get("metric_key")),
                "family": _as_str(signal.get("family")),
                "baseline_value": _as_float_or_none(signal.get("baseline_value")),
                "current_value": _as_float_or_none(signal.get("current_value")),
                "delta_absolute": _as_float_or_none(signal.get("delta_absolute")),
                "severity": _as_str(signal.get("severity")) or "ok",
                "ignored": _as_bool(signal.get("ignored")),
            }
            for signal in signals
        ],
    }


def build_alert_payload_from_benchmark_pack(
    payload: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    run = _as_dict(payload.get("run"))
    if _as_str(run.get("status")) != "succeeded":
        return None

    comparison = _as_dict(payload.get("comparison"))
    overall_severity = _as_str(comparison.get("overall_severity"))
    if overall_severity not in {"warn", "critical"}:
        return None

    scope = _as_dict(payload.get("scope"))
    source_alerts = [
        _as_dict(alert)
        for alert in _as_list(payload.get("alerts"))
        if _as_str(_as_dict(alert).get("level")) in {"warn", "critical"}
    ]
    if not source_alerts:
        return None

    profile_name = _sanitize_routing_component(
        _as_str(scope.get("profile_name")) or "unknown_profile"
    )
    feature_version = _sanitize_routing_component(
        _as_str(scope.get("feature_version")) or "unknown_feature"
    )
    ruleset_version = _sanitize_routing_component(
        _as_str(scope.get("ruleset_version")) or "unknown_ruleset"
    )

    alerts = []
    for source_alert in source_alerts:
        original_code = _as_str(source_alert.get("code")) or "benchmark_signal"
        code = _sanitize_alert_code(original_code)
        alerts.append(
            {
                "alert_id": _as_str(source_alert.get("id")),
                "alert_type": "benchmark_pack",
                "severity": _as_str(source_alert.get("level")),
                "reason_codes": [code],
                "summary": _as_str(source_alert.get("message")),
                "generated_at": _as_str(source_alert.get("created_at")),
                "routing_key": (
                    f"{profile_name}:{feature_version}:{ruleset_version}:{code}"
                ),
                "context": {
                    "scope": _as_str(source_alert.get("scope")),
                    "segment_id": _as_str(source_alert.get("segment_id")),
                    "signal_id": _as_str(source_alert.get("signal_id")),
                    "original_code": original_code,
                },
            }
        )

    return {
        "generated_at": _as_str(run.get("finished_at")),
        "alert_count": len(alerts),
        "alerts": alerts,
    }


def persist_benchmark_artifacts(
    payload: Mapping[str, Any],
    *,
    artifact_base_dir: Path,
    trend_payload: Mapping[str, Any],
    alert_payload: Optional[Mapping[str, Any]],
) -> dict[str, str]:
    run = _as_dict(payload.get("run"))
    scope = _as_dict(payload.get("scope"))
    run_status = _as_str(run.get("status"))
    run_date = _validate_run_date(_as_str(scope.get("run_date")) or "")
    profile_name = _validate_artifact_component(
        _as_str(scope.get("profile_name")) or "",
        name="scope.profile_name",
    )
    feature_version = _validate_artifact_component(
        _as_str(scope.get("feature_version")) or "",
        name="scope.feature_version",
    )
    ruleset_version = _validate_artifact_component(
        _as_str(scope.get("ruleset_version")) or "",
        name="scope.ruleset_version",
    )
    run_id = _validate_artifact_component(
        _as_str(run.get("run_id")) or "",
        name="run.run_id",
    )

    root_path = artifact_base_dir / run_date / profile_name / run_id
    root_path.mkdir(parents=True, exist_ok=True)

    benchmark_path = root_path / "benchmark_pack.json"
    trend_path = root_path / "benchmark_pack_trend.json"
    alert_path = root_path / "benchmark_pack_alert.json"

    _write_json_atomic(benchmark_path, payload)
    _write_json_atomic(trend_path, trend_payload)
    if alert_payload is not None:
        _write_json_atomic(alert_path, alert_payload)
    elif alert_path.exists():
        alert_path.unlink()

    latest_benchmark_path: Optional[Path] = None
    latest_alert_path: Optional[Path] = None
    if run_status == "succeeded":
        latest_path = (
            artifact_base_dir
            / "latest"
            / profile_name
            / feature_version
            / ruleset_version
        )
        latest_path.mkdir(parents=True, exist_ok=True)
        latest_benchmark_path = latest_path / "latest_benchmark_pack.json"
        _write_json_atomic(latest_benchmark_path, payload)

        latest_alert_path = latest_path / "latest_benchmark_pack_alert.json"
        if alert_payload is not None:
            _write_json_atomic(latest_alert_path, alert_payload)
        elif latest_alert_path.exists():
            latest_alert_path.unlink()

    return {
        "root_path": str(root_path),
        "benchmark_pack_path": str(benchmark_path),
        "benchmark_trend_path": str(trend_path),
        "benchmark_alert_path": str(alert_path) if alert_payload is not None else "",
        "latest_benchmark_pack_path": (
            str(latest_benchmark_path) if latest_benchmark_path is not None else ""
        ),
        "latest_benchmark_alert_path": (
            str(latest_alert_path)
            if alert_payload is not None and latest_alert_path is not None
            else ""
        ),
    }


def _resolve_baseline_payload(
    *,
    artifact_base_dir: Path,
    profile_name: str,
    feature_version: str,
    ruleset_version: str,
    baseline_path: Optional[Path],
) -> tuple[Optional[dict[str, Any]], Optional[Path], list[str]]:
    resolved_baseline_path = baseline_path
    if resolved_baseline_path is None:
        candidate = (
            artifact_base_dir
            / "latest"
            / profile_name
            / feature_version
            / ruleset_version
            / "latest_benchmark_pack.json"
        )
        if candidate.exists():
            resolved_baseline_path = candidate
    if resolved_baseline_path is None:
        return None, None, ["no_comparable_baseline_found"]
    baseline_payload = json.loads(resolved_baseline_path.read_text(encoding="utf-8"))
    if not isinstance(baseline_payload, dict):
        raise ValueError(
            f"Baseline file '{resolved_baseline_path}' is not a JSON object."
        )
    run_info = _as_dict(baseline_payload.get("run"))
    if _as_str(run_info.get("status")) != "succeeded":
        return None, resolved_baseline_path, ["baseline_run_failed"]
    return baseline_payload, resolved_baseline_path, []


def _build_summary_and_segments(
    queue_rows: list[dict[str, Any]],
    characteristics_by_parcel_id: Mapping[str, tuple[str, str]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    coverage_scored = len(queue_rows)
    coverage_queue = len(queue_rows)
    coverage_requires_review = 0
    coverage_reviewed = 0
    risk_counts: Counter[str] = Counter({risk_band: 0 for risk_band in _RISK_BANDS})
    disposition_counts: Counter[str] = Counter({key: 0 for key in _DISPOSITION_KEYS})

    segment_accumulators: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        _new_segment_accumulator
    )

    for row in queue_rows:
        if _as_bool(row.get("requires_review")):
            coverage_requires_review += 1

        risk_band = _as_str(row.get("risk_band"))
        if risk_band in _RISK_BANDS:
            risk_counts[risk_band] += 1

        review_status = _as_str(row.get("review_status"))
        review_disposition = _as_str(row.get("review_disposition"))
        reviewed = _is_reviewed_row(
            review_status=review_status,
            review_disposition=review_disposition,
        )
        if reviewed:
            coverage_reviewed += 1
            if review_disposition in _REVIEW_DISPOSITIONS:
                disposition_counts[review_disposition] += 1
            else:
                disposition_counts["unreviewed"] += 1
        else:
            disposition_counts["unreviewed"] += 1

        parcel_id = _as_str(row.get("parcel_id"))
        geography_key, class_key = characteristics_by_parcel_id.get(
            parcel_id or "",
            ("unknown_geography", "unknown_class"),
        )
        segment = segment_accumulators[(geography_key, class_key)]
        segment["queue_parcel_count"] += 1
        if reviewed:
            segment["reviewed_case_count"] += 1
        if risk_band in _RISK_BANDS:
            segment["risk_counts"][risk_band] += 1
        if reviewed:
            if review_disposition in _REVIEW_DISPOSITIONS:
                segment["disposition_counts"][review_disposition] += 1
            else:
                segment["disposition_counts"]["unreviewed"] += 1
        else:
            segment["disposition_counts"]["unreviewed"] += 1

    summary = {
        "coverage": {
            "scored_parcel_count": coverage_scored,
            "queue_parcel_count": coverage_queue,
            "reviewed_case_count": coverage_reviewed,
            "requires_review_count": coverage_requires_review,
            "review_rate": _rate(coverage_reviewed, coverage_queue),
        },
        "risk_band_mix": {
            risk_band: {
                "count": int(risk_counts[risk_band]),
                "rate": _rate(risk_counts[risk_band], coverage_queue),
            }
            for risk_band in _RISK_BANDS
        },
        "disposition_mix": {
            key: {
                "count": int(disposition_counts[key]),
                "rate": (
                    _rate(disposition_counts[key], coverage_queue)
                    if key == "unreviewed"
                    else _rate(disposition_counts[key], coverage_reviewed)
                ),
            }
            for key in _DISPOSITION_KEYS
        },
    }

    segments = []
    for geography_key, class_key in sorted(segment_accumulators):
        segment = segment_accumulators[(geography_key, class_key)]
        queue_parcel_count = int(segment["queue_parcel_count"])
        reviewed_case_count = int(segment["reviewed_case_count"])
        risk_counter = segment["risk_counts"]
        disposition_counter = segment["disposition_counts"]
        segment_id = f"{geography_key}::{class_key}"
        segments.append(
            {
                "segment_id": segment_id,
                "geography_key": geography_key,
                "class_key": class_key,
                "queue_parcel_count": queue_parcel_count,
                "reviewed_case_count": reviewed_case_count,
                "risk_band_mix": {
                    risk_band: {
                        "count": int(risk_counter[risk_band]),
                        "rate": _rate(risk_counter[risk_band], queue_parcel_count),
                    }
                    for risk_band in _RISK_BANDS
                },
                "disposition_mix": {
                    key: {
                        "count": int(disposition_counter[key]),
                        "rate": (
                            _rate(disposition_counter[key], queue_parcel_count)
                            if key == "unreviewed"
                            else _rate(disposition_counter[key], reviewed_case_count)
                        ),
                    }
                    for key in _DISPOSITION_KEYS
                },
            }
        )
    return summary, segments


def _build_comparison(
    *,
    baseline_payload: Optional[dict[str, Any]],
    summary: Mapping[str, Any],
    segments: list[dict[str, Any]],
    profile_name: str,
    feature_version: str,
    ruleset_version: str,
    top_n: int,
    period_length_days: int,
    baseline_non_comparable_reasons: list[str],
) -> dict[str, Any]:
    if baseline_payload is None:
        return {
            "baseline_reference": None,
            "comparable": False,
            "non_comparable_reasons": (
                baseline_non_comparable_reasons or ["no_comparable_baseline_found"]
            ),
            "signals": [],
            "overall_severity": "ok",
        }

    baseline_run = _as_dict(baseline_payload.get("run"))
    baseline_scope = _as_dict(baseline_payload.get("scope"))
    reasons: list[str] = []

    if _as_str(baseline_run.get("version_tag")) != BENCHMARK_PACK_VERSION_TAG:
        reasons.append("baseline_version_tag_mismatch")
    if _as_str(baseline_scope.get("profile_name")) != profile_name:
        reasons.append("profile_name_mismatch")
    if _as_str(baseline_scope.get("feature_version")) != feature_version:
        reasons.append("feature_version_mismatch")
    if _as_str(baseline_scope.get("ruleset_version")) != ruleset_version:
        reasons.append("ruleset_version_mismatch")
    if _as_int(baseline_scope.get("top_n")) != top_n:
        reasons.append("top_n_mismatch")
    if _as_int(baseline_scope.get("period_length_days")) != period_length_days:
        reasons.append("period_length_days_mismatch")

    if reasons:
        return {
            "baseline_reference": None,
            "comparable": False,
            "non_comparable_reasons": sorted(set(reasons)),
            "signals": [],
            "overall_severity": "ok",
        }

    baseline_summary = _as_dict(baseline_payload.get("summary"))
    baseline_segments = [
        _as_dict(segment) for segment in _as_list(baseline_payload.get("segments"))
    ]
    signals = _build_signals(
        baseline_summary=baseline_summary,
        current_summary=summary,
        baseline_segments=baseline_segments,
        current_segments=segments,
    )
    overall_severity = _overall_severity(signals)

    return {
        "baseline_reference": {
            "benchmark_run_id": _as_str(baseline_run.get("run_id")),
            "run_date": _as_str(baseline_scope.get("run_date")),
            "profile_name": _as_str(baseline_scope.get("profile_name")),
            "feature_version": _as_str(baseline_scope.get("feature_version")),
            "ruleset_version": _as_str(baseline_scope.get("ruleset_version")),
            "top_n": _as_int(baseline_scope.get("top_n")),
            "period_length_days": _as_int(baseline_scope.get("period_length_days")),
        },
        "comparable": True,
        "non_comparable_reasons": [],
        "signals": signals,
        "overall_severity": overall_severity,
    }


def _build_signals(
    *,
    baseline_summary: Mapping[str, Any],
    current_summary: Mapping[str, Any],
    baseline_segments: list[dict[str, Any]],
    current_segments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_segment_by_id = {
        _as_str(segment.get("segment_id")): segment for segment in baseline_segments
    }
    current_segment_by_id = {
        _as_str(segment.get("segment_id")): segment for segment in current_segments
    }

    metric_keys = list(_RUN_SIGNAL_METRIC_KEYS)
    segment_ids = sorted(
        {
            segment_id
            for segment_id in (
                list(baseline_segment_by_id.keys()) + list(current_segment_by_id.keys())
            )
            if segment_id is not None
        }
    )
    for segment_id in segment_ids:
        metric_keys.append(
            f"segment.{segment_id}.disposition_mix.false_positive.rate_delta_abs"
        )
        metric_keys.append(f"segment.{segment_id}.risk_band_mix.high.rate_delta_abs")

    signals = []
    for metric_key in sorted(metric_keys):
        family = _metric_family(metric_key)
        baseline_value, current_value, delta_absolute, delta_relative = (
            _comparison_values_for_metric(
                metric_key=metric_key,
                baseline_summary=baseline_summary,
                current_summary=current_summary,
                baseline_segment_by_id=baseline_segment_by_id,
                current_segment_by_id=current_segment_by_id,
            )
        )
        sample_size = _sample_size_for_metric(
            metric_key=metric_key,
            current_summary=current_summary,
            current_segment_by_id=current_segment_by_id,
        )
        ignored, ignore_reason = _ignore_state_for_signal(
            metric_key=metric_key,
            baseline_value=baseline_value,
            current_value=current_value,
            sample_size=sample_size,
            current_segment_by_id=current_segment_by_id,
        )
        severity = _severity_for_signal(
            metric_key=metric_key,
            delta_absolute=delta_absolute,
            ignored=ignored,
        )
        reason_token = _reason_token(
            ignored=ignored,
            ignore_reason=ignore_reason,
            baseline_value=baseline_value,
            current_value=current_value,
            delta_absolute=delta_absolute,
            severity=severity,
        )
        signals.append(
            {
                "signal_id": metric_key,
                "family": family,
                "metric_key": metric_key,
                "baseline_value": baseline_value,
                "current_value": current_value,
                "delta_absolute": delta_absolute,
                "delta_relative": delta_relative,
                "sample_size": sample_size,
                "severity": severity,
                "reason_code": f"{family}.{reason_token}.{metric_key}",
                "ignored": ignored,
                "ignore_reason": ignore_reason,
            }
        )

    signals.sort(
        key=lambda signal: (
            _as_str(signal.get("metric_key")) or "",
            _as_str(signal.get("family")) or "",
            _as_str(signal.get("signal_id")) or "",
        )
    )
    return signals


def _comparison_values_for_metric(
    *,
    metric_key: str,
    baseline_summary: Mapping[str, Any],
    current_summary: Mapping[str, Any],
    baseline_segment_by_id: Mapping[Optional[str], dict[str, Any]],
    current_segment_by_id: Mapping[Optional[str], dict[str, Any]],
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if metric_key.startswith("segment."):
        segment_id = _segment_id_from_metric_key(metric_key)
        baseline_segment = baseline_segment_by_id.get(segment_id)
        current_segment = current_segment_by_id.get(segment_id)
        metric_path = _segment_underlying_metric_path(metric_key)
        baseline_value = _metric_value_from_segment(baseline_segment, metric_path)
        current_value = _metric_value_from_segment(current_segment, metric_path)
    else:
        baseline_value = _metric_value_from_summary(baseline_summary, metric_key)
        current_value = _metric_value_from_summary(current_summary, metric_key)

    if baseline_value is None or current_value is None:
        return baseline_value, current_value, None, None

    if metric_key.endswith("rate_delta_abs"):
        return (
            baseline_value,
            current_value,
            _round_fraction(abs(current_value - baseline_value)),
            None,
        )

    delta_absolute = _round_fraction(current_value - baseline_value)
    delta_relative = (
        _round_fraction((current_value - baseline_value) / baseline_value)
        if baseline_value != 0
        else None
    )
    return baseline_value, current_value, delta_absolute, delta_relative


def _sample_size_for_metric(
    *,
    metric_key: str,
    current_summary: Mapping[str, Any],
    current_segment_by_id: Mapping[Optional[str], dict[str, Any]],
) -> int:
    coverage = _as_dict(current_summary.get("coverage"))
    queue_parcel_count = _as_int(coverage.get("queue_parcel_count"))
    reviewed_case_count = _as_int(coverage.get("reviewed_case_count"))

    if metric_key.startswith("coverage."):
        return queue_parcel_count
    if metric_key.startswith("risk_band_mix."):
        return queue_parcel_count
    if metric_key == "disposition_mix.unreviewed.rate":
        return queue_parcel_count
    if metric_key.startswith("disposition_mix."):
        return reviewed_case_count
    if metric_key.startswith("segment."):
        segment_id = _segment_id_from_metric_key(metric_key)
        segment = current_segment_by_id.get(segment_id)
        if segment is None:
            return 0
        queue_count = _as_int(_as_dict(segment).get("queue_parcel_count"))
        reviewed_count = _as_int(_as_dict(segment).get("reviewed_case_count"))
        if ".disposition_mix.unreviewed." in metric_key:
            return queue_count
        if ".disposition_mix." in metric_key:
            return reviewed_count
        return queue_count
    return 0


def _ignore_state_for_signal(
    *,
    metric_key: str,
    baseline_value: Optional[float],
    current_value: Optional[float],
    sample_size: int,
    current_segment_by_id: Mapping[Optional[str], dict[str, Any]],
) -> tuple[bool, Optional[str]]:
    if baseline_value is None and current_value is not None:
        return True, "missing_baseline_metric"
    if current_value is None and baseline_value is not None:
        return True, "missing_current_metric"
    if baseline_value is None and current_value is None:
        return True, "missing_current_metric"

    if metric_key.startswith("segment."):
        segment_id = _segment_id_from_metric_key(metric_key)
        segment = current_segment_by_id.get(segment_id)
        queue_count = _as_int(_as_dict(segment).get("queue_parcel_count"))
        reviewed_count = _as_int(_as_dict(segment).get("reviewed_case_count"))
        if queue_count < 25:
            return True, "insufficient_sample_size"
        if ".disposition_mix." in metric_key and ".unreviewed." not in metric_key:
            if reviewed_count < 20:
                return True, "insufficient_sample_size"
        return False, None

    if (
        metric_key.startswith("disposition_mix.")
        and metric_key != "disposition_mix.unreviewed.rate"
    ):
        if sample_size < 20:
            return True, "insufficient_sample_size"
    if metric_key == "disposition_mix.unreviewed.rate" and sample_size < 20:
        return True, "insufficient_sample_size"
    return False, None


def _severity_for_signal(
    *,
    metric_key: str,
    delta_absolute: Optional[float],
    ignored: bool,
) -> str:
    if ignored or delta_absolute is None:
        return "ok"

    if metric_key == "coverage.review_rate":
        if delta_absolute <= -0.20:
            return "critical"
        if delta_absolute <= -0.10:
            return "warn"
        return "ok"
    if metric_key == "risk_band_mix.high.rate":
        if delta_absolute >= 0.08:
            return "critical"
        if delta_absolute >= 0.04:
            return "warn"
        return "ok"
    if metric_key == "risk_band_mix.medium.rate":
        magnitude = abs(delta_absolute)
        if magnitude >= 0.10:
            return "critical"
        if magnitude >= 0.06:
            return "warn"
        return "ok"
    if metric_key == "disposition_mix.false_positive.rate":
        if delta_absolute >= 0.06:
            return "critical"
        if delta_absolute >= 0.03:
            return "warn"
        return "ok"
    if metric_key == "disposition_mix.confirmed_issue.rate":
        if delta_absolute <= -0.06:
            return "critical"
        if delta_absolute <= -0.03:
            return "warn"
        return "ok"
    if metric_key.endswith("rate_delta_abs"):
        if delta_absolute >= 0.12:
            return "critical"
        if delta_absolute >= 0.07:
            return "warn"
        return "ok"
    return "ok"


def _reason_token(
    *,
    ignored: bool,
    ignore_reason: Optional[str],
    baseline_value: Optional[float],
    current_value: Optional[float],
    delta_absolute: Optional[float],
    severity: str,
) -> str:
    if ignored and ignore_reason == "insufficient_sample_size":
        return "low_sample_size"
    if baseline_value is None and current_value is not None:
        return "baseline_missing"
    if current_value is None and baseline_value is not None:
        return "current_missing"
    if baseline_value is None and current_value is None:
        return "non_comparable"
    if delta_absolute is not None and _is_zero(delta_absolute):
        return "no_change"
    if severity in {"warn", "critical"}:
        return "beyond_tolerance"
    if severity == "ok":
        return "within_tolerance"
    return "non_comparable"


def _build_alerts(
    *,
    run_id: str,
    finished_at: datetime,
    signals: list[Any],
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for index, signal_raw in enumerate(signals, start=1):
        signal = _as_dict(signal_raw)
        severity = _as_str(signal.get("severity"))
        if severity not in {"warn", "critical"}:
            continue
        if _as_bool(signal.get("ignored")):
            continue
        metric_key = _as_str(signal.get("metric_key")) or "unknown_metric"
        segment_id = _segment_id_from_metric_key(metric_key)
        scope = "segment" if segment_id is not None else "comparison"
        alerts.append(
            {
                "id": f"{run_id}.alert.{index:03d}",
                "level": severity,
                "code": _as_str(signal.get("reason_code")) or "benchmark_signal",
                "message": f"Benchmark change beyond tolerance on {metric_key}.",
                "scope": scope,
                "created_at": _iso_utc(finished_at),
                "segment_id": segment_id,
                "signal_id": _as_str(signal.get("signal_id")),
                "metadata": {
                    "metric_key": metric_key,
                    "delta_absolute": _as_float_or_none(signal.get("delta_absolute")),
                },
            }
        )
    return alerts


def _overall_severity(signals: list[dict[str, Any]]) -> str:
    severity_rank = {"ok": 0, "warn": 1, "critical": 2}
    max_rank = 0
    for signal in signals:
        if _as_bool(signal.get("ignored")):
            continue
        signal_severity = _as_str(signal.get("severity")) or "ok"
        max_rank = max(max_rank, severity_rank.get(signal_severity, 0))
    if max_rank >= 2:
        return "critical"
    if max_rank == 1:
        return "warn"
    return "ok"


def _segment_id_from_metric_key(metric_key: str) -> Optional[str]:
    parts = metric_key.split(".")
    if len(parts) < 3 or parts[0] != "segment":
        return None
    return parts[1]


def _segment_underlying_metric_path(metric_key: str) -> str:
    suffix = ".rate_delta_abs"
    if metric_key.endswith(suffix):
        return metric_key.split(".", 2)[2][: -len(suffix)] + ".rate"
    return metric_key


def _metric_family(metric_key: str) -> str:
    if metric_key.startswith("segment."):
        return "segment_mix"
    if metric_key.startswith("coverage."):
        return "coverage"
    if metric_key.startswith("risk_band_mix."):
        return "risk_band_mix"
    if metric_key.startswith("disposition_mix."):
        return "disposition_mix"
    return "coverage"


def _metric_value_from_summary(
    summary: Mapping[str, Any], metric_key: str
) -> Optional[float]:
    if metric_key == "coverage.review_rate":
        return _as_float_or_none(_as_dict(summary.get("coverage")).get("review_rate"))
    if metric_key == "risk_band_mix.high.rate":
        return _as_float_or_none(
            _as_dict(_as_dict(summary.get("risk_band_mix")).get("high")).get("rate")
        )
    if metric_key == "risk_band_mix.medium.rate":
        return _as_float_or_none(
            _as_dict(_as_dict(summary.get("risk_band_mix")).get("medium")).get("rate")
        )
    if metric_key == "disposition_mix.false_positive.rate":
        return _as_float_or_none(
            _as_dict(
                _as_dict(summary.get("disposition_mix")).get("false_positive")
            ).get("rate")
        )
    if metric_key == "disposition_mix.confirmed_issue.rate":
        return _as_float_or_none(
            _as_dict(
                _as_dict(summary.get("disposition_mix")).get("confirmed_issue")
            ).get("rate")
        )
    if metric_key == "disposition_mix.unreviewed.rate":
        return _as_float_or_none(
            _as_dict(_as_dict(summary.get("disposition_mix")).get("unreviewed")).get(
                "rate"
            )
        )
    return None


def _metric_value_from_segment(
    segment: Optional[Mapping[str, Any]],
    metric_path: str,
) -> Optional[float]:
    if segment is None:
        return None
    if metric_path == "risk_band_mix.high.rate":
        return _as_float_or_none(
            _as_dict(_as_dict(segment.get("risk_band_mix")).get("high")).get("rate")
        )
    if metric_path == "disposition_mix.false_positive.rate":
        return _as_float_or_none(
            _as_dict(
                _as_dict(segment.get("disposition_mix")).get("false_positive")
            ).get("rate")
        )
    if metric_path == "disposition_mix.unreviewed.rate":
        return _as_float_or_none(
            _as_dict(_as_dict(segment.get("disposition_mix")).get("unreviewed")).get(
                "rate"
            )
        )
    return None


def _load_characteristics_by_parcel_id(
    session: Session,
    parcel_ids: list[str],
) -> dict[str, tuple[str, str]]:
    if not parcel_ids:
        return {}

    characteristic_by_id: dict[str, tuple[str, str]] = {}
    chunk_size = 500
    for start in range(0, len(parcel_ids), chunk_size):
        chunk = parcel_ids[start : start + chunk_size]
        rows = session.execute(
            select(
                ParcelCharacteristic.parcel_id,
                ParcelCharacteristic.state_municipality_code,
                ParcelCharacteristic.current_valuation_classification,
            ).where(ParcelCharacteristic.parcel_id.in_(chunk))
        ).all()
        for parcel_id, municipality_code, valuation_class in rows:
            geography_key = _normalize_segment_component(
                _as_str(municipality_code), fallback="unknown_geography"
            )
            class_key = _normalize_segment_component(
                _as_str(valuation_class), fallback="unknown_class"
            )
            characteristic_by_id[parcel_id] = (geography_key, class_key)

    return characteristic_by_id


def _normalize_segment_component(value: Optional[str], *, fallback: str) -> str:
    if value is None or value.strip() == "":
        return fallback
    normalized = _SEGMENT_COMPONENT_CLEANUP_RE.sub("_", value.strip())
    normalized = normalized.strip("_")
    if not normalized:
        return fallback
    if not _SAFE_SEGMENT_COMPONENT_RE.fullmatch(normalized):
        return fallback
    return normalized


def _derive_source_run_id(queue_rows: list[dict[str, Any]]) -> Optional[str]:
    run_ids = [_as_optional_int(row.get("run_id")) for row in queue_rows]
    filtered = [run_id for run_id in run_ids if run_id is not None]
    if not filtered:
        return None
    return str(max(filtered))


def _is_reviewed_row(
    *, review_status: Optional[str], review_disposition: Optional[str]
) -> bool:
    if review_disposition is not None:
        return True
    return review_status in {"resolved", "closed"}


def _as_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _period_length_days(*, period_start: datetime, period_end: datetime) -> int:
    delta_seconds = (period_end - period_start).total_seconds()
    return 1 + int(delta_seconds // 86400)


def _new_segment_accumulator() -> dict[str, Any]:
    return {
        "queue_parcel_count": 0,
        "reviewed_case_count": 0,
        "risk_counts": Counter({risk_band: 0 for risk_band in _RISK_BANDS}),
        "disposition_counts": Counter({key: 0 for key in _DISPOSITION_KEYS}),
    }


def _empty_summary() -> dict[str, Any]:
    return {
        "coverage": {
            "scored_parcel_count": 0,
            "queue_parcel_count": 0,
            "reviewed_case_count": 0,
            "requires_review_count": 0,
            "review_rate": 0.0,
        },
        "risk_band_mix": {
            risk_band: {"count": 0, "rate": 0.0} for risk_band in _RISK_BANDS
        },
        "disposition_mix": {
            key: {"count": 0, "rate": 0.0} for key in _DISPOSITION_KEYS
        },
    }


def _default_benchmark_run_id(
    *,
    run_date: str,
    profile_name: str,
    feature_version: str,
    ruleset_version: str,
    generated_at: datetime,
) -> str:
    timestamp = generated_at.astimezone(timezone.utc).strftime("%H%M%S")
    return f"{run_date}_{profile_name}_{feature_version}_{ruleset_version}_{timestamp}"


def _validate_run_date(run_date: str) -> str:
    if not re.fullmatch(r"\d{8}", run_date):
        raise ValueError(f"Invalid run_date {run_date!r}; expected YYYYMMDD.")
    return run_date


def _validate_top_n(top_n: int) -> int:
    if top_n <= 0:
        raise ValueError("top_n must be a positive integer.")
    return top_n


def _validate_artifact_component(value: str, *, name: str) -> str:
    if not _SAFE_ARTIFACT_COMPONENT_RE.fullmatch(value):
        raise ValueError(
            f"Invalid {name}: {value!r}. Use only letters, digits, '.', '_' or '-'."
        )
    if ".." in value:
        raise ValueError(
            f"Invalid {name}: path traversal sequence '..' is not allowed."
        )
    return value


def _write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    json_payload = json.dumps(payload, indent=2)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f"{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(json_payload)
        tmp_name = handle.name
    os.replace(tmp_name, path)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _round_fraction(value: float) -> float:
    return float(
        Decimal(str(value)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    )


def _rate(numerator: int, denominator: int) -> float:
    return _round_fraction(numerator / max(denominator, 1))


def _is_zero(value: float) -> bool:
    return abs(value) < 1e-12


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _as_str(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value
    return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return False


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return 0
    return 0


def _as_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _sanitize_alert_code(value: str) -> str:
    sanitized = _ROUTING_COMPONENT_CLEANUP_RE.sub("_", value.replace(":", "_"))
    sanitized = sanitized.strip("._-")
    if sanitized == "":
        return "benchmark_signal"
    return sanitized


def _sanitize_routing_component(value: str) -> str:
    sanitized = _ROUTING_COMPONENT_CLEANUP_RE.sub("_", value.replace(":", "_"))
    sanitized = sanitized.strip("._-")
    if sanitized == "":
        return "unknown"
    return sanitized
