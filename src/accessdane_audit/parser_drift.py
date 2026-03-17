from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from sqlalchemy.orm import Session

from .profiling import PROFILED_TAX_DETAIL_FIELDS, build_data_profile

PARSER_DRIFT_VERSION_TAG = "parser_drift_v1"
RUN_TYPE_PARSER_DRIFT_SNAPSHOT = "parser_drift_snapshot"
RUN_TYPE_PARSER_DRIFT_DIFF = "parser_drift_diff"

FIELD_COVERAGE_METRIC_KEYS: tuple[str, ...] = (
    "coverage.parsed_successful_fetch_rate",
    "coverage.parse_error_successful_fetch_rate",
    "coverage.parcel_summary_parcel_rate",
    "coverage.parcel_year_fact_parcel_rate",
    "coverage.parcel_year_fact_source_year_rate",
    "coverage.parcel_characteristic_parcel_rate",
    "coverage.parcel_lineage_parcel_rate",
)

SELECTOR_MISS_METRIC_KEYS: tuple[str, ...] = (
    "selector_miss.assessment_fetch_rate",
    "selector_miss.tax_fetch_rate",
    "selector_miss.payment_fetch_rate",
)

TAX_DETAIL_PRESENCE_METRIC_KEYS: tuple[str, ...] = tuple(
    f"tax_detail_field_presence.{field_name}.rate"
    for field_name in PROFILED_TAX_DETAIL_FIELDS
)


def build_parser_drift_snapshot(
    session: Session,
    *,
    profile_name: str,
    run_date: str,
    feature_version: str,
    ruleset_version: str,
    artifact_root: str,
    source_artifacts: list[str],
    parcel_ids: Optional[list[str]] = None,
    snapshot_id: Optional[str] = None,
    run_persisted: bool = False,
) -> dict[str, Any]:
    profile = build_data_profile(session, parcel_ids=parcel_ids)
    generated_at = _utc_now()
    snapshot_id_value = snapshot_id or _default_run_id(
        prefix="snapshot", run_date=run_date, generated_at=generated_at
    )

    counts = _as_dict(profile.get("counts"))
    coverage = _as_dict(profile.get("coverage"))
    missing_sections = _as_dict(profile.get("missing_sections"))
    detail_presence = _as_dict(profile.get("tax_detail_field_presence"))

    successful_fetches = _as_int(counts.get("successful_fetches"))
    detail_tax_records = _as_int(counts.get("detail_tax_records"))

    selector_miss = {
        "assessment_fetch_rate": _ratio(
            _as_int(missing_sections.get("assessment_fetches")), successful_fetches
        ),
        "tax_fetch_rate": _ratio(
            _as_int(missing_sections.get("tax_fetches")), successful_fetches
        ),
        "payment_fetch_rate": _ratio(
            _as_int(missing_sections.get("payment_fetches")), successful_fetches
        ),
    }

    tax_detail_presence = {
        field_name: {
            "rate": _as_float(_as_dict(detail_presence.get(field_name)).get("rate")),
            "count": _as_int(_as_dict(detail_presence.get(field_name)).get("count")),
        }
        for field_name in PROFILED_TAX_DETAIL_FIELDS
    }

    extraction_null_rate = {
        field_name: {
            "rate": _ratio(
                max(detail_tax_records - _as_int(field_payload["count"]), 0),
                detail_tax_records,
            ),
            "null_count": max(detail_tax_records - _as_int(field_payload["count"]), 0),
            "eligible_record_count": detail_tax_records,
        }
        for field_name, field_payload in tax_detail_presence.items()
    }

    return {
        "run": {
            "run_type": RUN_TYPE_PARSER_DRIFT_SNAPSHOT,
            "version_tag": PARSER_DRIFT_VERSION_TAG,
            "run_id": snapshot_id_value,
            "snapshot_id": snapshot_id_value,
            "status": "succeeded",
            "run_persisted": run_persisted,
            "generated_at": generated_at,
        },
        "scope": {
            "profile_name": profile_name,
            "run_date": run_date,
            "feature_version": feature_version,
            "ruleset_version": ruleset_version,
            "artifact_root": artifact_root,
            "parcel_filter_count": _as_optional_int(
                _as_dict(profile.get("scope")).get("parcel_filter_count")
            ),
        },
        "metrics": {
            "counts": {
                "successful_fetches": successful_fetches,
                "parcels": _as_int(counts.get("parcels")),
                "source_parcel_years": _as_int(counts.get("source_parcel_years")),
                "detail_tax_records": detail_tax_records,
            },
            "field_coverage": {
                "parsed_successful_fetch_rate": _as_float(
                    coverage.get("parsed_successful_fetch_rate")
                ),
                "parse_error_successful_fetch_rate": _as_float(
                    coverage.get("parse_error_successful_fetch_rate")
                ),
                "parcel_summary_parcel_rate": _as_float(
                    coverage.get("parcel_summary_parcel_rate")
                ),
                "parcel_year_fact_parcel_rate": _as_float(
                    coverage.get("parcel_year_fact_parcel_rate")
                ),
                "parcel_year_fact_source_year_rate": _as_float(
                    coverage.get("parcel_year_fact_source_year_rate")
                ),
                "parcel_characteristic_parcel_rate": _as_float(
                    coverage.get("parcel_characteristic_parcel_rate")
                ),
                "parcel_lineage_parcel_rate": _as_float(
                    coverage.get("parcel_lineage_parcel_rate")
                ),
            },
            "selector_miss": selector_miss,
            "tax_detail_field_presence": tax_detail_presence,
            "extraction_null_rate": extraction_null_rate,
        },
        "diagnostics": {
            "warnings": [],
            "source_artifacts": source_artifacts,
        },
        "error": None,
    }


def build_failed_snapshot_payload(
    *,
    profile_name: str,
    run_date: str,
    feature_version: str,
    ruleset_version: str,
    artifact_root: str,
    source_artifacts: list[str],
    message: str,
    snapshot_id: Optional[str] = None,
    run_persisted: bool = False,
) -> dict[str, Any]:
    generated_at = _utc_now()
    snapshot_id_value = snapshot_id or _default_run_id(
        prefix="snapshot", run_date=run_date, generated_at=generated_at
    )
    return {
        "run": {
            "run_type": RUN_TYPE_PARSER_DRIFT_SNAPSHOT,
            "version_tag": PARSER_DRIFT_VERSION_TAG,
            "run_id": snapshot_id_value,
            "snapshot_id": snapshot_id_value,
            "status": "failed",
            "run_persisted": run_persisted,
            "generated_at": generated_at,
        },
        "scope": {
            "profile_name": profile_name,
            "run_date": run_date,
            "feature_version": feature_version,
            "ruleset_version": ruleset_version,
            "artifact_root": artifact_root,
            "parcel_filter_count": None,
        },
        "metrics": {
            "counts": {},
            "field_coverage": {},
            "selector_miss": {},
            "tax_detail_field_presence": {},
            "extraction_null_rate": {},
        },
        "diagnostics": {
            "warnings": [],
            "source_artifacts": source_artifacts,
        },
        "error": {
            "code": "snapshot_build_failed",
            "message": message,
        },
    }


def build_parser_drift_diff(
    *,
    baseline_snapshot: Mapping[str, Any],
    current_snapshot: Mapping[str, Any],
    baseline_artifact_path: str,
    current_artifact_path: str,
    diff_id: Optional[str] = None,
    run_persisted: bool = False,
) -> dict[str, Any]:
    generated_at = _utc_now()
    diff_id_value = diff_id or _default_run_id(
        prefix="diff",
        run_date=_as_str(_as_dict(current_snapshot.get("scope")).get("run_date"))
        or datetime.now(timezone.utc).strftime("%Y%m%d"),
        generated_at=generated_at,
    )
    baseline_metrics = _metric_values_from_snapshot(baseline_snapshot)
    current_metrics = _metric_values_from_snapshot(current_snapshot)
    metric_keys = sorted(set(baseline_metrics) | set(current_metrics))

    signals: list[dict[str, Any]] = []
    for metric_key in metric_keys:
        baseline_value, baseline_size = baseline_metrics.get(metric_key, (None, 0))
        current_value, current_size = current_metrics.get(metric_key, (None, 0))
        family = _metric_family(metric_key)

        ignored = False
        ignore_reason: Optional[str] = None
        severity = "ok"
        delta_absolute: Optional[float]
        if baseline_value is None or current_value is None:
            delta_absolute = None
        else:
            delta_absolute = round(current_value - baseline_value, 4)

        if baseline_size == 0 or current_size == 0:
            ignored = True
            ignore_reason = "insufficient_denominator"
        elif baseline_value is None or current_value is None:
            ignored = True
            ignore_reason = "missing_metric_value"
        else:
            severity = _threshold_severity(
                metric_key, round(current_value - baseline_value, 4)
            )
            if current_size < 25 and severity == "error":
                severity = "warn"

        if baseline_value is None or baseline_value == 0 or current_value is None:
            delta_relative = None
        else:
            delta_relative = round((current_value - baseline_value) / baseline_value, 4)

        reason_token = _reason_token(
            severity=severity,
            ignored=ignored,
            ignore_reason=ignore_reason,
        )
        signals.append(
            {
                "signal_id": f"{family}:{metric_key}",
                "family": family,
                "metric_key": metric_key,
                "baseline_value": baseline_value,
                "current_value": current_value,
                "delta_absolute": delta_absolute,
                "delta_relative": delta_relative,
                "sample_size_baseline": baseline_size,
                "sample_size_current": current_size,
                "severity": severity,
                "reason_code": f"{family}.{reason_token}.{metric_key}",
                "ignored": ignored,
                "ignore_reason": ignore_reason,
            }
        )

    signals.sort(
        key=lambda signal: (signal["metric_key"], signal["family"], signal["signal_id"])
    )

    ignored_count = sum(1 for signal in signals if signal["ignored"])
    signal_ok_count = sum(
        1
        for signal in signals
        if (not signal["ignored"] and signal["severity"] == "ok")
    )
    signal_warn_count = sum(
        1
        for signal in signals
        if (not signal["ignored"] and signal["severity"] == "warn")
    )
    signal_error_count = sum(
        1
        for signal in signals
        if (not signal["ignored"] and signal["severity"] == "error")
    )
    overall_severity = "ok"
    if signal_error_count > 0:
        overall_severity = "error"
    elif signal_warn_count > 0:
        overall_severity = "warn"

    alerts: list[dict[str, Any]] = []
    if overall_severity != "ok":
        reason_codes_set: set[str] = set()
        for signal in signals:
            if _as_bool(signal.get("ignored")):
                continue
            if _as_str(signal.get("severity")) != overall_severity:
                continue
            reason_code = _as_str(signal.get("reason_code"))
            if reason_code is not None:
                reason_codes_set.add(reason_code)
        reason_codes = sorted(reason_codes_set)
        alerts.append(
            {
                "alert_id": f"{diff_id_value}.{overall_severity}",
                "alert_type": "parser_drift",
                "severity": overall_severity,
                "routing_key": (
                    "ops.parser_drift.error"
                    if overall_severity == "error"
                    else "ops.parser_drift.warn"
                ),
                "reason_codes": reason_codes,
            }
        )

    baseline_run = _as_dict(baseline_snapshot.get("run"))
    current_run = _as_dict(current_snapshot.get("run"))

    return {
        "run": {
            "run_type": RUN_TYPE_PARSER_DRIFT_DIFF,
            "version_tag": PARSER_DRIFT_VERSION_TAG,
            "run_id": diff_id_value,
            "diff_id": diff_id_value,
            "baseline_snapshot_id": _as_str(baseline_run.get("snapshot_id")),
            "current_snapshot_id": _as_str(current_run.get("snapshot_id")),
            "run_persisted": run_persisted,
            "generated_at": generated_at,
            "status": "succeeded",
        },
        "baseline": {
            "snapshot_id": _as_str(baseline_run.get("snapshot_id")),
            "generated_at": _as_str(baseline_run.get("generated_at")),
            "artifact_path": baseline_artifact_path,
        },
        "current": {
            "snapshot_id": _as_str(current_run.get("snapshot_id")),
            "generated_at": _as_str(current_run.get("generated_at")),
            "artifact_path": current_artifact_path,
        },
        "summary": {
            "signal_count": len(signals),
            "signal_ok_count": signal_ok_count,
            "signal_warn_count": signal_warn_count,
            "signal_error_count": signal_error_count,
            "ignored_signal_count": ignored_count,
            "overall_severity": overall_severity,
        },
        "signals": signals,
        "alerts": alerts,
        "diagnostics": {
            "warnings": [],
            "source_artifacts": [baseline_artifact_path, current_artifact_path],
        },
        "error": None,
    }


def build_failed_diff_payload(
    *,
    baseline_artifact_path: Optional[str],
    current_artifact_path: Optional[str],
    baseline_snapshot_id: Optional[str],
    current_snapshot_id: Optional[str],
    run_date: str,
    message: str,
    diff_id: Optional[str] = None,
    run_persisted: bool = False,
) -> dict[str, Any]:
    generated_at = _utc_now()
    diff_id_value = diff_id or _default_run_id(
        prefix="diff", run_date=run_date, generated_at=generated_at
    )
    return {
        "run": {
            "run_type": RUN_TYPE_PARSER_DRIFT_DIFF,
            "version_tag": PARSER_DRIFT_VERSION_TAG,
            "run_id": diff_id_value,
            "diff_id": diff_id_value,
            "baseline_snapshot_id": baseline_snapshot_id,
            "current_snapshot_id": current_snapshot_id,
            "run_persisted": run_persisted,
            "generated_at": generated_at,
            "status": "failed",
        },
        "baseline": {
            "snapshot_id": baseline_snapshot_id,
            "generated_at": None,
            "artifact_path": baseline_artifact_path,
        },
        "current": {
            "snapshot_id": current_snapshot_id,
            "generated_at": None,
            "artifact_path": current_artifact_path,
        },
        "summary": {
            "signal_count": 0,
            "signal_ok_count": 0,
            "signal_warn_count": 0,
            "signal_error_count": 0,
            "ignored_signal_count": 0,
            "overall_severity": "ok",
        },
        "signals": [],
        "alerts": [],
        "diagnostics": {
            "warnings": [],
            "source_artifacts": [
                path for path in [baseline_artifact_path, current_artifact_path] if path
            ],
        },
        "error": {
            "code": "diff_build_failed",
            "message": message,
        },
    }


def build_alert_payload_from_diff(
    diff_payload: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    summary = _as_dict(diff_payload.get("summary"))
    overall_severity = _as_str(summary.get("overall_severity"))
    if overall_severity not in {"warn", "error"}:
        return None

    run_payload = _as_dict(diff_payload.get("run"))
    alerts = _as_list(diff_payload.get("alerts"))
    if not alerts:
        return None
    alert = _as_dict(alerts[0])
    signals = _as_list(diff_payload.get("signals"))
    impacted_signals = [
        signal
        for signal in signals
        if (
            _as_str(_as_dict(signal).get("severity")) == overall_severity
            and not _as_bool(_as_dict(signal).get("ignored"))
        )
    ]
    derived_reason_codes = sorted(
        {
            reason_code
            for signal in impacted_signals
            for reason_code in [_as_str(_as_dict(signal).get("reason_code"))]
            if reason_code is not None
        }
    )

    baseline_artifact_path = _as_str(
        _as_dict(diff_payload.get("baseline")).get("artifact_path")
    )
    current_artifact_path = _as_str(
        _as_dict(diff_payload.get("current")).get("artifact_path")
    )

    return {
        "run": run_payload,
        "alert": {
            "alert_id": _as_str(alert.get("alert_id")),
            "alert_type": "parser_drift",
            "severity": overall_severity,
            "reason_codes": derived_reason_codes,
            "routing_key": _as_str(alert.get("routing_key")),
            "generated_at": _utc_now(),
        },
        "impacted_signals": impacted_signals,
        "operator_actions": [
            {
                "action_id": "review_current_vs_baseline_artifacts",
                "message": (
                    "Compare current artifacts against baseline for impacted metrics."
                ),
                "artifact_paths": [
                    path
                    for path in [baseline_artifact_path, current_artifact_path]
                    if path
                ],
            },
            {
                "action_id": "triage_parser_changes",
                "message": (
                    "Validate parser selector/extraction behavior and open "
                    "remediation work if drift is confirmed."
                ),
                "artifact_paths": [path for path in [current_artifact_path] if path],
            },
        ],
        "error": None,
    }


def read_snapshot_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _metric_values_from_snapshot(
    snapshot: Mapping[str, Any],
) -> dict[str, tuple[Optional[float], int]]:
    metrics = _as_dict(snapshot.get("metrics"))
    counts = _as_dict(metrics.get("counts"))
    field_coverage = _as_dict(metrics.get("field_coverage"))
    selector_miss = _as_dict(metrics.get("selector_miss"))
    tax_detail_presence = _as_dict(metrics.get("tax_detail_field_presence"))
    extraction_null = _as_dict(metrics.get("extraction_null_rate"))

    values: dict[str, tuple[Optional[float], int]] = {}

    for metric_key in FIELD_COVERAGE_METRIC_KEYS:
        leaf = metric_key.split(".", 1)[1]
        sample_size = _coverage_sample_size(metric_key=metric_key, counts=counts)
        values[metric_key] = (_as_float(field_coverage.get(leaf)), sample_size)

    for metric_key in SELECTOR_MISS_METRIC_KEYS:
        leaf = metric_key.split(".", 1)[1]
        values[metric_key] = (
            _as_float(selector_miss.get(leaf)),
            _as_int(counts.get("successful_fetches")),
        )

    for metric_key in TAX_DETAIL_PRESENCE_METRIC_KEYS:
        leaf = metric_key.split(".", 2)[1]
        payload = _as_dict(tax_detail_presence.get(leaf))
        values[metric_key] = (
            _as_float(payload.get("rate")),
            _as_int(counts.get("detail_tax_records")),
        )

    for field_name, payload in extraction_null.items():
        field_payload = _as_dict(payload)
        values[f"extraction_null_rate.{field_name}"] = (
            _as_float(field_payload.get("rate")),
            _as_int(field_payload.get("eligible_record_count")),
        )

    return values


def _coverage_sample_size(*, metric_key: str, counts: Mapping[str, Any]) -> int:
    if metric_key in (
        "coverage.parsed_successful_fetch_rate",
        "coverage.parse_error_successful_fetch_rate",
    ):
        return _as_int(counts.get("successful_fetches"))
    if metric_key == "coverage.parcel_year_fact_source_year_rate":
        return _as_int(counts.get("source_parcel_years"))
    return _as_int(counts.get("parcels"))


def _metric_family(metric_key: str) -> str:
    if metric_key.startswith("coverage."):
        return "field_coverage_shift"
    if metric_key.startswith("extraction_null_rate."):
        return "extraction_null_rate_shift"
    return "selector_miss_shift"


def _threshold_severity(metric_key: str, delta_absolute: float) -> str:
    if metric_key == "coverage.parse_error_successful_fetch_rate":
        return _severity_for_increase(
            delta_absolute, warn_threshold=0.01, error_threshold=0.03
        )
    if metric_key.startswith("selector_miss."):
        return _severity_for_increase(
            delta_absolute, warn_threshold=0.02, error_threshold=0.05
        )
    if metric_key.startswith("tax_detail_field_presence."):
        return _severity_for_decrease(
            delta_absolute, warn_threshold=0.02, error_threshold=0.05
        )
    if metric_key.startswith("extraction_null_rate."):
        return _severity_for_increase(
            delta_absolute, warn_threshold=0.03, error_threshold=0.08
        )
    return _severity_for_decrease(
        delta_absolute, warn_threshold=0.02, error_threshold=0.05
    )


def _severity_for_increase(
    delta_absolute: float, *, warn_threshold: float, error_threshold: float
) -> str:
    if delta_absolute >= error_threshold:
        return "error"
    if delta_absolute >= warn_threshold:
        return "warn"
    return "ok"


def _severity_for_decrease(
    delta_absolute: float, *, warn_threshold: float, error_threshold: float
) -> str:
    if delta_absolute <= -error_threshold:
        return "error"
    if delta_absolute <= -warn_threshold:
        return "warn"
    return "ok"


def _reason_token(*, severity: str, ignored: bool, ignore_reason: Optional[str]) -> str:
    if ignored and ignore_reason == "insufficient_denominator":
        return "insufficient_denominator"
    if ignored and ignore_reason == "missing_metric_value":
        return "missing_metric_value"
    if severity == "error":
        return "error_threshold"
    if severity == "warn":
        return "warn_threshold"
    return "no_drift"


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(float(stripped))
            except ValueError:
                return 0
    return 0


def _as_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    return _as_int(value)


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return round(float(value), 4)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return round(float(stripped), 4)
        except ValueError:
            return None
    return None


def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _ratio(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _default_run_id(*, prefix: str, run_date: str, generated_at: str) -> str:
    compact_time = generated_at.replace("-", "").replace(":", "").replace("T", "_")
    compact_time = compact_time.replace("Z", "")
    return f"{run_date}_{prefix}_{compact_time}"
