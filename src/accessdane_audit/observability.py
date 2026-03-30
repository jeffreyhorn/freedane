from __future__ import annotations

import csv
import json
import os
import re
import tempfile
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Optional, Sequence

SAFE_RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")

WINDOW_LABELS: tuple[str, ...] = ("1h", "6h", "24h", "28d", "365d")
WINDOW_TO_DELTA: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "28d": timedelta(days=28),
    "365d": timedelta(days=365),
}


class ObservabilityError(ValueError):
    pass


def _validate_observability_run_id(observability_run_id: str) -> None:
    if not SAFE_RUN_ID_RE.fullmatch(observability_run_id):
        raise ObservabilityError(
            "Invalid observability_run_id; only letters, digits, '.', '_' and '-' "
            "are allowed."
        )
    if observability_run_id in {".", ".."}:
        raise ObservabilityError(
            "Invalid observability_run_id; '.' and '..' are not allowed."
        )


@dataclass(frozen=True)
class _SliSpec:
    sli_id: str
    domain: str
    target: float
    measurement_window: str
    panel_id: str
    profile_name: Optional[str] = None


@dataclass(frozen=True)
class _SliEvent:
    observed_at: datetime
    passed: bool
    profile_name: Optional[str]


@dataclass(frozen=True)
class _SliResult:
    spec: _SliSpec
    numerator: int
    denominator: int
    observed: Optional[float]
    error_budget_remaining: float
    status: str
    burn_rate_1h: Optional[float]
    burn_rate_6h: Optional[float]
    burn_rate_24h: Optional[float]
    insufficient_sample_size: bool


_SLI_SPECS: tuple[_SliSpec, ...] = (
    _SliSpec(
        sli_id="refresh.success_ratio.daily_refresh",
        domain="refresh",
        target=0.98,
        measurement_window="28d",
        panel_id="refresh_reliability",
        profile_name="daily_refresh",
    ),
    _SliSpec(
        sli_id="refresh.success_ratio.analysis_only",
        domain="refresh",
        target=0.99,
        measurement_window="28d",
        panel_id="refresh_reliability",
        profile_name="analysis_only",
    ),
    _SliSpec(
        sli_id="refresh.success_ratio.annual_refresh",
        domain="refresh",
        target=0.95,
        measurement_window="365d",
        panel_id="refresh_reliability",
        profile_name="annual_refresh",
    ),
    _SliSpec(
        sli_id="refresh.latency_compliance.daily_refresh",
        domain="refresh",
        target=0.95,
        measurement_window="28d",
        panel_id="refresh_latency",
        profile_name="daily_refresh",
    ),
    _SliSpec(
        sli_id="refresh.latency_compliance.analysis_only",
        domain="refresh",
        target=0.95,
        measurement_window="28d",
        panel_id="refresh_latency",
        profile_name="analysis_only",
    ),
    _SliSpec(
        sli_id="refresh.latency_compliance.annual_refresh",
        domain="refresh",
        target=0.95,
        measurement_window="365d",
        panel_id="refresh_latency",
        profile_name="annual_refresh",
    ),
    _SliSpec(
        sli_id="parser_drift.error_free_ratio",
        domain="parser_drift",
        target=0.99,
        measurement_window="28d",
        panel_id="parser_drift_health",
    ),
    _SliSpec(
        sli_id="parser_drift.artifact_coverage",
        domain="parser_drift",
        target=0.995,
        measurement_window="28d",
        panel_id="parser_drift_health",
    ),
    _SliSpec(
        sli_id="load_monitoring.critical_free_ratio",
        domain="load_monitoring",
        target=0.985,
        measurement_window="28d",
        panel_id="load_monitor_health",
    ),
    _SliSpec(
        sli_id="load_monitoring.timeliness",
        domain="load_monitoring",
        target=0.99,
        measurement_window="28d",
        panel_id="load_monitor_health",
    ),
    _SliSpec(
        sli_id="annual_refresh.signoff_success_ratio",
        domain="annual_refresh",
        target=0.95,
        measurement_window="365d",
        panel_id="annual_refresh_readiness",
        profile_name="annual_refresh",
    ),
    _SliSpec(
        sli_id="annual_refresh.required_stage_completion_ratio",
        domain="annual_refresh",
        target=0.97,
        measurement_window="365d",
        panel_id="annual_refresh_readiness",
        profile_name="annual_refresh",
    ),
    _SliSpec(
        sli_id="benchmark.generation_success_ratio",
        domain="benchmark_pack",
        target=0.99,
        measurement_window="28d",
        panel_id="benchmark_freshness",
    ),
    _SliSpec(
        sli_id="benchmark.freshness_compliance",
        domain="benchmark_pack",
        target=0.99,
        measurement_window="28d",
        panel_id="benchmark_freshness",
    ),
)


def discover_observability_input_files(
    *,
    refresh_artifact_base_dir: Path,
    benchmark_artifact_base_dir: Path,
    startup_artifact_base_dir: Path,
    refresh_files: Sequence[Path] = (),
    scheduler_files: Sequence[Path] = (),
    parser_drift_files: Sequence[Path] = (),
    load_monitor_files: Sequence[Path] = (),
    annual_signoff_files: Sequence[Path] = (),
    benchmark_files: Sequence[Path] = (),
    run_date: Optional[str] = None,
) -> dict[str, list[Path]]:
    refresh_root = _expand_and_resolve(refresh_artifact_base_dir)
    benchmark_root = _expand_and_resolve(benchmark_artifact_base_dir)
    startup_root = _expand_and_resolve(startup_artifact_base_dir)

    if run_date is not None and not re.fullmatch(r"\d{8}", run_date):
        raise ObservabilityError(f"Invalid run_date {run_date!r}; expected YYYYMMDD.")

    # Discovery intentionally scans the full artifact roots to support rolling
    # window metrics (28d/365d), independent of the output run_date label.
    refresh_search_root = refresh_root
    benchmark_search_root = benchmark_root
    startup_search_root = startup_root

    discovered_refresh = _discover_files(
        explicit_paths=refresh_files,
        roots_and_patterns=(
            (refresh_search_root, "*/*/*/health_summary/refresh_run_payload.json"),
        ),
    )
    discovered_scheduler = _discover_files(
        explicit_paths=scheduler_files,
        roots_and_patterns=(
            (refresh_root / "scheduler_logs", "*.json"),
            (refresh_root / "logs", "*.json"),
            (refresh_search_root, "*/*/*/scheduler_logs/*.json"),
            (refresh_search_root, "*/*/*/logs/*.json"),
        ),
    )
    discovered_parser = _discover_files(
        explicit_paths=parser_drift_files,
        roots_and_patterns=(
            (refresh_search_root, "*/*/*/parser_drift_diff.json"),
            (startup_search_root, "*/*/*/startup_parser_drift_diff.json"),
        ),
    )
    discovered_load = _discover_files(
        explicit_paths=load_monitor_files,
        roots_and_patterns=(
            (refresh_search_root, "*/*/*/load_monitor.json"),
            (startup_search_root, "*/*/*/startup_load_monitor.json"),
        ),
    )
    discovered_annual = _discover_files(
        explicit_paths=annual_signoff_files,
        roots_and_patterns=(
            (refresh_search_root, "*/*/*/annual_signoff/annual_signoff.json"),
        ),
    )
    discovered_benchmark = _discover_files(
        explicit_paths=benchmark_files,
        roots_and_patterns=(
            (benchmark_search_root, "*/*/*/benchmark_pack.json"),
            (startup_search_root, "*/*/*/startup_benchmark_pack.json"),
        ),
    )

    return {
        "refresh": discovered_refresh,
        "scheduler": discovered_scheduler,
        "parser_drift": discovered_parser,
        "load_monitoring": discovered_load,
        "annual_signoff": discovered_annual,
        "benchmark": discovered_benchmark,
    }


def build_observability_outputs(
    *,
    environment_name: str,
    alert_route_group: str,
    run_date: str,
    observability_run_id: str,
    refresh_payload_files: Sequence[Path],
    scheduler_payload_files: Sequence[Path],
    parser_drift_files: Sequence[Path],
    load_monitor_files: Sequence[Path],
    annual_signoff_files: Sequence[Path],
    benchmark_files: Sequence[Path],
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, object]:
    if not re.fullmatch(r"\d{8}", run_date):
        raise ObservabilityError(f"Invalid run_date {run_date!r}; expected YYYYMMDD.")
    _validate_observability_run_id(observability_run_id)

    now_dt = now_fn().astimezone(timezone.utc)

    normalized_inputs = {
        "refresh_payload_files": _sorted_paths(refresh_payload_files),
        "scheduler_payload_files": _sorted_paths(scheduler_payload_files),
        "parser_drift_files": _sorted_paths(parser_drift_files),
        "load_monitor_files": _sorted_paths(load_monitor_files),
        "annual_signoff_files": _sorted_paths(annual_signoff_files),
        "benchmark_files": _sorted_paths(benchmark_files),
    }

    events_by_sli: dict[str, list[_SliEvent]] = {spec.sli_id: [] for spec in _SLI_SPECS}
    errors: list[dict[str, object]] = []
    benchmark_finished_ats: list[datetime] = []

    refresh_event_keys = _record_refresh_events(
        refresh_payload_files=normalized_inputs["refresh_payload_files"],
        events_by_sli=events_by_sli,
        errors=errors,
        now_dt=now_dt,
    )
    _record_scheduler_events(
        scheduler_payload_files=normalized_inputs["scheduler_payload_files"],
        events_by_sli=events_by_sli,
        errors=errors,
        now_dt=now_dt,
        seen_refresh_event_keys=refresh_event_keys,
    )
    _record_parser_drift_events(
        parser_drift_files=normalized_inputs["parser_drift_files"],
        events_by_sli=events_by_sli,
        errors=errors,
        now_dt=now_dt,
    )
    _record_load_monitor_events(
        load_monitor_files=normalized_inputs["load_monitor_files"],
        events_by_sli=events_by_sli,
        errors=errors,
        now_dt=now_dt,
    )
    _record_annual_signoff_events(
        annual_signoff_files=normalized_inputs["annual_signoff_files"],
        events_by_sli=events_by_sli,
        errors=errors,
        now_dt=now_dt,
    )
    _record_benchmark_events(
        benchmark_files=normalized_inputs["benchmark_files"],
        events_by_sli=events_by_sli,
        benchmark_finished_ats=benchmark_finished_ats,
        errors=errors,
        now_dt=now_dt,
    )

    if benchmark_finished_ats:
        _record_benchmark_freshness_events(
            benchmark_finished_ats=benchmark_finished_ats,
            events_by_sli=events_by_sli,
            now_dt=now_dt,
        )

    _append_missing_artifact_errors(
        errors=errors,
        domain_to_files={
            "refresh": (
                tuple(normalized_inputs["refresh_payload_files"])
                + tuple(normalized_inputs["scheduler_payload_files"])
            ),
            "parser_drift": normalized_inputs["parser_drift_files"],
            "load_monitoring": normalized_inputs["load_monitor_files"],
            "benchmark_pack": normalized_inputs["benchmark_files"],
        },
    )
    _append_annual_refresh_artifact_errors(
        errors=errors,
        has_annual_refresh_payload=bool(
            events_by_sli["annual_refresh.required_stage_completion_ratio"]
        ),
        annual_signoff_files=normalized_inputs["annual_signoff_files"],
    )

    sli_results, burn_alerts, non_computable = _compute_sli_results(
        events_by_sli=events_by_sli,
        now_dt=now_dt,
        alert_route_group=alert_route_group,
    )
    metrics = _build_metric_records(
        events_by_sli=events_by_sli,
        sli_results=sli_results,
        environment_name=environment_name,
        now_dt=now_dt,
    )
    timeseries_rows = _build_timeseries_rows(metrics)

    run_payload = {
        "observability_run_id": observability_run_id,
        "run_date": run_date,
        "environment": environment_name,
        "generated_at_utc": _iso_utc(now_dt),
        "window_start_utc": _iso_utc(now_dt - timedelta(days=365)),
        "window_end_utc": _iso_utc(now_dt),
    }

    rollup = {
        "run": run_payload,
        "inputs": {
            key: [_portable_path(path) for path in value]
            for key, value in normalized_inputs.items()
        },
        "metrics": metrics,
        "slo_status": [
            {
                "sli_id": result.spec.sli_id,
                "target": result.spec.target,
                "observed": result.observed,
                "compliant": (
                    result.observed is not None
                    and result.observed >= result.spec.target
                ),
                "error_budget_remaining": result.error_budget_remaining,
                "status": result.status,
                "measurement_window": result.spec.measurement_window,
                "insufficient_sample_size": result.insufficient_sample_size,
            }
            for result in sli_results
        ],
        "burn_alerts": burn_alerts,
        "errors": errors,
    }

    measurement_windows = {
        result.spec.measurement_window
        for result in sli_results
        if result.spec.measurement_window
    }
    if len(measurement_windows) == 1:
        evaluation_measurement_window = next(iter(measurement_windows))
    else:
        evaluation_measurement_window = "mixed"

    slo_evaluation = {
        "evaluation": {
            "contract_version": "observability_slo_v1",
            "environment": environment_name,
            "generated_at_utc": _iso_utc(now_dt),
            "measurement_window": evaluation_measurement_window,
        },
        "sli_results": [
            {
                "sli_id": result.spec.sli_id,
                "numerator": result.numerator,
                "denominator": result.denominator,
                "observed": result.observed,
                "target": result.spec.target,
                "error_budget_burn_rate": _round_float(result.burn_rate_24h, 6),
                "status": result.status,
                "measurement_window": result.spec.measurement_window,
                "insufficient_sample_size": result.insufficient_sample_size,
            }
            for result in sli_results
        ],
        "non_computable": non_computable,
    }

    dashboard = {
        "snapshot": {
            "contract_version": "observability_slo_v1",
            "generated_at_utc": _iso_utc(now_dt),
            "environment": environment_name,
            "run_date": run_date,
            "observability_run_id": observability_run_id,
        },
        "panels": _build_panels(sli_results=sli_results),
        "alerts": burn_alerts,
    }

    return {
        "rollup": rollup,
        "slo_evaluation": slo_evaluation,
        "dashboard_snapshot": dashboard,
        "timeseries_rows": timeseries_rows,
    }


def persist_observability_outputs(
    *,
    artifact_base_dir: Path,
    run_date: str,
    observability_run_id: str,
    outputs: Mapping[str, object],
) -> dict[str, Path]:
    if not re.fullmatch(r"\d{8}", run_date):
        raise ObservabilityError(f"Invalid run_date {run_date!r}; expected YYYYMMDD.")
    _validate_observability_run_id(observability_run_id)
    if "rollup" not in outputs or not isinstance(outputs["rollup"], Mapping):
        raise ObservabilityError(
            "outputs must include a mapping value for key 'rollup'."
        )
    if "slo_evaluation" not in outputs or not isinstance(
        outputs["slo_evaluation"], Mapping
    ):
        raise ObservabilityError(
            "outputs must include a mapping value for key 'slo_evaluation'."
        )
    if "dashboard_snapshot" not in outputs or not isinstance(
        outputs["dashboard_snapshot"], Mapping
    ):
        raise ObservabilityError(
            "outputs must include a mapping value for key 'dashboard_snapshot'."
        )
    if "timeseries_rows" not in outputs or not isinstance(
        outputs["timeseries_rows"], list
    ):
        raise ObservabilityError(
            "outputs must include a list value for key 'timeseries_rows'."
        )

    output_base = _expand_and_resolve(artifact_base_dir)
    run_root = output_base / "ops_observability" / run_date / observability_run_id
    run_root.mkdir(parents=True, exist_ok=True)

    rollup_path = run_root / "observability_rollup.json"
    evaluation_path = run_root / "observability_slo_evaluation.json"
    snapshot_path = run_root / "observability_dashboard_snapshot.json"
    timeseries_path = run_root / "observability_metric_timeseries.csv"

    _write_json_atomic(rollup_path, outputs["rollup"])
    _write_json_atomic(evaluation_path, outputs["slo_evaluation"])
    _write_json_atomic(snapshot_path, outputs["dashboard_snapshot"])
    _write_timeseries_csv(
        timeseries_path,
        rows=_as_mapping_list(outputs.get("timeseries_rows")),
    )

    return {
        "run_root": run_root,
        "rollup_path": rollup_path,
        "slo_evaluation_path": evaluation_path,
        "dashboard_snapshot_path": snapshot_path,
        "timeseries_path": timeseries_path,
    }


def _record_refresh_events(
    *,
    refresh_payload_files: Sequence[Path],
    events_by_sli: dict[str, list[_SliEvent]],
    errors: list[dict[str, object]],
    now_dt: datetime,
) -> set[str]:
    event_keys: set[str] = set()
    for path in refresh_payload_files:
        payload = _load_json_or_error(path=path, domain="refresh", errors=errors)
        if payload is None:
            continue

        run = _as_dict(payload.get("run"))
        profile_name = _as_str(run.get("profile_name"))
        if profile_name not in {"daily_refresh", "analysis_only", "annual_refresh"}:
            continue

        status = _as_str(run.get("status"))
        if status == "cancelled":
            continue
        run_id = _as_str(run.get("run_id")) or path.name
        event_key = _refresh_event_key(profile_name=profile_name, run_id=run_id)
        event_keys.add(event_key)

        finished_at = _parse_iso(_as_str(run.get("finished_at")))
        started_at = _parse_iso(_as_str(run.get("started_at")))
        observed_at = finished_at or started_at or now_dt
        succeeded = status == "succeeded"

        events_by_sli[f"refresh.success_ratio.{profile_name}"].append(
            _SliEvent(
                observed_at=observed_at,
                passed=succeeded,
                profile_name=profile_name,
            )
        )

        duration_seconds: Optional[float] = None
        if started_at is not None and finished_at is not None:
            duration_seconds = (finished_at - started_at).total_seconds()

        latency_threshold = {
            "daily_refresh": 90 * 60,
            "analysis_only": 30 * 60,
            "annual_refresh": 8 * 60 * 60,
        }[profile_name]
        passed_latency = bool(
            succeeded
            and duration_seconds is not None
            and duration_seconds <= float(latency_threshold)
        )
        events_by_sli[f"refresh.latency_compliance.{profile_name}"].append(
            _SliEvent(
                observed_at=observed_at,
                passed=passed_latency,
                profile_name=profile_name,
            )
        )

        if profile_name == "annual_refresh":
            stage_payloads = [
                _as_dict(stage)
                for stage in _as_list(payload.get("stages"))
                if isinstance(stage, dict)
            ]
            all_succeeded = bool(stage_payloads) and all(
                _as_str(stage.get("status")) == "succeeded" for stage in stage_payloads
            )
            events_by_sli["annual_refresh.required_stage_completion_ratio"].append(
                _SliEvent(
                    observed_at=observed_at,
                    passed=all_succeeded,
                    profile_name="annual_refresh",
                )
            )
    return event_keys


def _record_scheduler_events(
    *,
    scheduler_payload_files: Sequence[Path],
    events_by_sli: dict[str, list[_SliEvent]],
    errors: list[dict[str, object]],
    now_dt: datetime,
    seen_refresh_event_keys: set[str],
) -> None:
    for path in scheduler_payload_files:
        payload = _load_json_or_error(path=path, domain="refresh", errors=errors)
        if payload is None:
            continue

        scheduler_run = _as_dict(payload.get("scheduler_run"))
        result = _as_dict(payload.get("result"))
        profile_name = _as_str(scheduler_run.get("profile_name"))
        if profile_name not in {"daily_refresh", "analysis_only", "annual_refresh"}:
            continue

        result_status = _as_str(result.get("status"))
        if result_status in {None, "cancelled"}:
            continue
        scheduler_run_id = _as_str(scheduler_run.get("scheduler_run_id")) or path.stem
        refresh_run_id = _as_str(scheduler_run.get("refresh_run_id"))
        event_key = _refresh_event_key(
            profile_name=profile_name,
            run_id=refresh_run_id or scheduler_run_id,
        )
        if event_key in seen_refresh_event_keys:
            continue
        seen_refresh_event_keys.add(event_key)

        observed_at = (
            _parse_iso(_as_str(result.get("last_attempt_finished_at_utc")))
            or _parse_iso(_as_str(scheduler_run.get("updated_at_utc")))
            or now_dt
        )
        events_by_sli[f"refresh.success_ratio.{profile_name}"].append(
            _SliEvent(
                observed_at=observed_at,
                passed=result_status == "succeeded",
                profile_name=profile_name,
            )
        )

        first_started, last_finished = _scheduler_attempt_bounds(
            _as_list(payload.get("attempts"))
        )
        duration_seconds: Optional[float] = None
        if first_started is not None and last_finished is not None:
            duration_seconds = (last_finished - first_started).total_seconds()

        latency_threshold = {
            "daily_refresh": 90 * 60,
            "analysis_only": 30 * 60,
            "annual_refresh": 8 * 60 * 60,
        }[profile_name]
        passed_latency = bool(
            result_status == "succeeded"
            and duration_seconds is not None
            and duration_seconds <= float(latency_threshold)
        )
        events_by_sli[f"refresh.latency_compliance.{profile_name}"].append(
            _SliEvent(
                observed_at=observed_at,
                passed=passed_latency,
                profile_name=profile_name,
            )
        )


def _scheduler_attempt_bounds(
    attempts: Sequence[object],
) -> tuple[Optional[datetime], Optional[datetime]]:
    started_values: list[datetime] = []
    finished_values: list[datetime] = []
    for raw_attempt in attempts:
        attempt = _as_dict(raw_attempt)
        started_at = _parse_iso(_as_str(attempt.get("started_at_utc")))
        finished_at = _parse_iso(_as_str(attempt.get("finished_at_utc")))
        if started_at is not None:
            started_values.append(started_at)
        if finished_at is not None:
            finished_values.append(finished_at)
    first_started = min(started_values) if started_values else None
    last_finished = max(finished_values) if finished_values else None
    return first_started, last_finished


def _record_parser_drift_events(
    *,
    parser_drift_files: Sequence[Path],
    events_by_sli: dict[str, list[_SliEvent]],
    errors: list[dict[str, object]],
    now_dt: datetime,
) -> None:
    for path in parser_drift_files:
        payload = _load_json_or_error(path=path, domain="parser_drift", errors=errors)
        if payload is None:
            continue

        run = _as_dict(payload.get("run"))
        run_type = _as_str(run.get("run_type"))
        if run_type != "parser_drift_diff":
            continue

        status = _as_str(run.get("status"))
        generated_at = _parse_iso(_as_str(run.get("generated_at"))) or now_dt
        alerts = [_as_dict(alert) for alert in _as_list(payload.get("alerts"))]
        has_error_alert = any(
            _as_str(alert.get("severity")) == "error" for alert in alerts
        )

        events_by_sli["parser_drift.error_free_ratio"].append(
            _SliEvent(
                observed_at=generated_at,
                passed=bool(status == "succeeded" and not has_error_alert),
                profile_name=None,
            )
        )

        baseline = _as_dict(payload.get("baseline"))
        current = _as_dict(payload.get("current"))
        has_paths = bool(_as_str(baseline.get("artifact_path"))) and bool(
            _as_str(current.get("artifact_path"))
        )
        events_by_sli["parser_drift.artifact_coverage"].append(
            _SliEvent(
                observed_at=generated_at,
                passed=bool(status == "succeeded" and has_paths),
                profile_name=None,
            )
        )


def _record_load_monitor_events(
    *,
    load_monitor_files: Sequence[Path],
    events_by_sli: dict[str, list[_SliEvent]],
    errors: list[dict[str, object]],
    now_dt: datetime,
) -> None:
    for path in load_monitor_files:
        payload = _load_json_or_error(
            path=path, domain="load_monitoring", errors=errors
        )
        if payload is None:
            continue

        run = _as_dict(payload.get("run"))
        status = _as_str(run.get("status"))
        started_at = _parse_iso(_as_str(run.get("started_at")))
        finished_at = _parse_iso(_as_str(run.get("finished_at")))
        observed_at = finished_at or started_at or now_dt

        summary = _as_dict(payload.get("summary"))
        severity = _as_str(summary.get("overall_severity"))
        passed_critical_free = bool(status == "succeeded" and severity != "critical")
        events_by_sli["load_monitoring.critical_free_ratio"].append(
            _SliEvent(
                observed_at=observed_at,
                passed=passed_critical_free,
                profile_name=None,
            )
        )

        duration_seconds: Optional[float] = None
        if started_at is not None and finished_at is not None:
            duration_seconds = (finished_at - started_at).total_seconds()

        passed_timeliness = bool(
            status == "succeeded"
            and duration_seconds is not None
            and duration_seconds <= 10 * 60
        )
        events_by_sli["load_monitoring.timeliness"].append(
            _SliEvent(
                observed_at=observed_at,
                passed=passed_timeliness,
                profile_name=None,
            )
        )


def _record_annual_signoff_events(
    *,
    annual_signoff_files: Sequence[Path],
    events_by_sli: dict[str, list[_SliEvent]],
    errors: list[dict[str, object]],
    now_dt: datetime,
) -> None:
    for path in annual_signoff_files:
        payload = _load_json_or_error(path=path, domain="annual_refresh", errors=errors)
        if payload is None:
            continue

        run = _as_dict(payload.get("run"))
        status = _as_str(run.get("status"))
        if status not in {"approved", "pending_signoff", "rejected"}:
            continue

        observed_at = (
            _parse_iso(_as_str(run.get("updated_at")))
            or _parse_iso(_as_str(run.get("created_at")))
            or now_dt
        )
        events_by_sli["annual_refresh.signoff_success_ratio"].append(
            _SliEvent(
                observed_at=observed_at,
                passed=status == "approved",
                profile_name="annual_refresh",
            )
        )


def _record_benchmark_events(
    *,
    benchmark_files: Sequence[Path],
    events_by_sli: dict[str, list[_SliEvent]],
    benchmark_finished_ats: list[datetime],
    errors: list[dict[str, object]],
    now_dt: datetime,
) -> None:
    for path in benchmark_files:
        payload = _load_json_or_error(path=path, domain="benchmark_pack", errors=errors)
        if payload is None:
            continue

        run = _as_dict(payload.get("run"))
        status = _as_str(run.get("status"))
        finished_at = _parse_iso(_as_str(run.get("finished_at")))
        generated_at = _parse_iso(_as_str(payload.get("generated_at")))
        observed_at = finished_at or generated_at or now_dt
        if finished_at is not None:
            benchmark_finished_ats.append(finished_at)
        elif generated_at is not None:
            benchmark_finished_ats.append(generated_at)

        events_by_sli["benchmark.generation_success_ratio"].append(
            _SliEvent(
                observed_at=observed_at,
                passed=status == "succeeded",
                profile_name=None,
            )
        )


def _record_benchmark_freshness_events(
    *,
    benchmark_finished_ats: Sequence[datetime],
    events_by_sli: dict[str, list[_SliEvent]],
    now_dt: datetime,
) -> None:
    # Emit one synthetic hourly freshness check across the benchmark SLI window
    # so denominator semantics are "hours checked", not rollup executions.
    sorted_finished = sorted(benchmark_finished_ats)
    total_hours = int(WINDOW_TO_DELTA["28d"].total_seconds() // 3600)
    freshness_threshold = timedelta(hours=24)

    for hours_ago in range(total_hours):
        check_time = now_dt - timedelta(hours=hours_ago)
        idx = bisect_right(sorted_finished, check_time) - 1
        latest_for_check = sorted_finished[idx] if idx >= 0 else None
        freshness_passed = bool(
            latest_for_check is not None
            and (check_time - latest_for_check) <= freshness_threshold
        )
        events_by_sli["benchmark.freshness_compliance"].append(
            _SliEvent(
                observed_at=check_time,
                passed=freshness_passed,
                profile_name=None,
            )
        )


def _compute_sli_results(
    *,
    events_by_sli: Mapping[str, list[_SliEvent]],
    now_dt: datetime,
    alert_route_group: str,
) -> tuple[list[_SliResult], list[dict[str, object]], list[dict[str, object]]]:
    results: list[_SliResult] = []
    burn_alerts: list[dict[str, object]] = []
    non_computable: list[dict[str, object]] = []

    for spec in _SLI_SPECS:
        all_events = sorted(
            events_by_sli.get(spec.sli_id, []),
            key=lambda item: (item.observed_at, item.profile_name or ""),
        )
        measurement_delta = WINDOW_TO_DELTA[spec.measurement_window]
        measured_events = [
            event
            for event in all_events
            if event.observed_at >= now_dt - measurement_delta
        ]

        numerator = sum(1 for event in measured_events if event.passed)
        denominator = len(measured_events)
        observed = (numerator / denominator) if denominator > 0 else None
        burn_1h, denom_1h = _burn_rate_for_window(
            events=all_events,
            now_dt=now_dt,
            target=spec.target,
            window="1h",
        )
        burn_6h, denom_6h = _burn_rate_for_window(
            events=all_events,
            now_dt=now_dt,
            target=spec.target,
            window="6h",
        )
        burn_24h, denom_24h = _burn_rate_for_window(
            events=all_events,
            now_dt=now_dt,
            target=spec.target,
            window="24h",
        )
        burn_1h_value = 0.0 if burn_1h is None else burn_1h
        burn_6h_value = 0.0 if burn_6h is None else burn_6h
        burn_24h_value = 0.0 if burn_24h is None else burn_24h

        insufficient_sample_size = False
        burn_severity: Optional[str] = None
        if denominator > 0 and denom_6h < 20 and denom_24h < 20:
            insufficient_sample_size = True
            non_computable.append(
                {
                    "sli_id": spec.sli_id,
                    "reason": "insufficient_sample_size",
                    "window": "24h",
                    "denominator": denom_24h,
                }
            )
        else:
            if denom_6h >= 20 and burn_1h_value >= 14 and burn_6h_value >= 7:
                burn_severity = "critical"
            elif denom_24h >= 20 and burn_6h_value >= 4 and burn_24h_value >= 2:
                burn_severity = "warn"
            else:
                measurement_days = 365 if spec.measurement_window == "365d" else 28
                projected_days = (
                    float("inf")
                    if burn_24h_value <= 0
                    else measurement_days / burn_24h_value
                )
                if denom_24h >= 20 and burn_24h_value >= 1 and projected_days < 21:
                    burn_severity = "info"

        status = "ok"
        if observed is None:
            non_computable.append(
                {
                    "sli_id": spec.sli_id,
                    "reason": "no_events",
                    "window": spec.measurement_window,
                    "denominator": 0,
                }
            )
        elif observed < spec.target:
            status = "breached"
        elif burn_severity is not None:
            status = "burning"

        error_budget_remaining = _error_budget_remaining(
            observed=observed,
            target=spec.target,
        )

        result = _SliResult(
            spec=spec,
            numerator=numerator,
            denominator=denominator,
            observed=observed,
            error_budget_remaining=error_budget_remaining,
            status=status,
            burn_rate_1h=burn_1h,
            burn_rate_6h=burn_6h,
            burn_rate_24h=burn_24h,
            insufficient_sample_size=insufficient_sample_size,
        )
        results.append(result)

        if burn_severity is not None:
            burn_alerts.append(
                {
                    "sli_id": spec.sli_id,
                    "severity": burn_severity,
                    "burn_rate_1h": _round_float(burn_1h_value, 6),
                    "burn_rate_6h": _round_float(burn_6h_value, 6),
                    "burn_rate_24h": _round_float(burn_24h_value, 6),
                    "triggered_at_utc": _iso_utc(now_dt),
                    "routing_key": f"{alert_route_group}.{spec.domain}.{burn_severity}",
                }
            )

    return results, burn_alerts, non_computable


def _build_metric_records(
    *,
    events_by_sli: Mapping[str, list[_SliEvent]],
    sli_results: Sequence[_SliResult],
    environment_name: str,
    now_dt: datetime,
) -> list[dict[str, object]]:
    spec_by_id = {spec.sli_id: spec for spec in _SLI_SPECS}
    rows: list[dict[str, object]] = []

    for sli_id, events in events_by_sli.items():
        spec = spec_by_id[sli_id]
        for event in sorted(
            events, key=lambda item: (item.observed_at, item.profile_name or "")
        ):
            rows.append(
                {
                    "metric_id": f"{sli_id}.event",
                    "domain": spec.domain,
                    "environment": environment_name,
                    "profile_name": event.profile_name,
                    "window": "event",
                    "observed_at_utc": _iso_utc(event.observed_at),
                    "value": 1.0 if event.passed else 0.0,
                    "numerator": 1 if event.passed else 0,
                    "denominator": 1,
                }
            )

        for window in WINDOW_LABELS:
            numerator, denominator = _window_fraction(
                events=events,
                now_dt=now_dt,
                window=window,
            )
            value = (numerator / denominator) if denominator > 0 else None
            rows.append(
                {
                    "metric_id": f"{sli_id}.compliance",
                    "domain": spec.domain,
                    "environment": environment_name,
                    "profile_name": spec.profile_name,
                    "window": window,
                    "observed_at_utc": _iso_utc(now_dt),
                    "value": _round_float(value, 6),
                    "numerator": numerator,
                    "denominator": denominator,
                }
            )

    for result in sli_results:
        rows.append(
            {
                "metric_id": f"{result.spec.sli_id}.error_budget_remaining",
                "domain": result.spec.domain,
                "environment": environment_name,
                "profile_name": result.spec.profile_name,
                "window": result.spec.measurement_window,
                "observed_at_utc": _iso_utc(now_dt),
                "value": _round_float(result.error_budget_remaining, 6),
                "numerator": None,
                "denominator": None,
            }
        )

    rows.sort(
        key=lambda row: (
            _as_str(row.get("metric_id")) or "",
            _as_str(row.get("window")) or "",
            _as_str(row.get("profile_name")) or "",
            _as_str(row.get("observed_at_utc")) or "",
        )
    )
    return rows


def _build_timeseries_rows(
    metrics: Sequence[Mapping[str, object]],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for metric in metrics:
        rows.append(
            {
                "metric_id": _as_str(metric.get("metric_id")) or "",
                "domain": _as_str(metric.get("domain")) or "",
                "environment": _as_str(metric.get("environment")) or "",
                "profile_name": _as_str(metric.get("profile_name")) or "",
                "window": _as_str(metric.get("window")) or "",
                "observed_at_utc": _as_str(metric.get("observed_at_utc")) or "",
                "value": "" if metric.get("value") is None else f"{metric['value']}",
                "numerator": (
                    "" if metric.get("numerator") is None else f"{metric['numerator']}"
                ),
                "denominator": (
                    ""
                    if metric.get("denominator") is None
                    else f"{metric['denominator']}"
                ),
            }
        )
    return rows


def _build_panels(*, sli_results: Sequence[_SliResult]) -> list[dict[str, object]]:
    panel_ids = [
        "refresh_reliability",
        "refresh_latency",
        "parser_drift_health",
        "load_monitor_health",
        "annual_refresh_readiness",
        "benchmark_freshness",
        "slo_burn_overview",
    ]

    panel_map: dict[str, list[_SliResult]] = {panel_id: [] for panel_id in panel_ids}
    for result in sli_results:
        panel_map[result.spec.panel_id].append(result)
        panel_map["slo_burn_overview"].append(result)

    panels: list[dict[str, object]] = []
    for panel_id in panel_ids:
        panel_results = panel_map[panel_id]
        if not panel_results:
            status = "no_data"
        elif all(result.denominator == 0 for result in panel_results):
            status = "no_data"
        elif any(result.status == "breached" for result in panel_results):
            status = "critical"
        elif any(result.status == "burning" for result in panel_results):
            status = "warn"
        else:
            status = "ok"

        series = [
            {
                "label": result.spec.sli_id,
                "value": _round_float(result.observed, 6),
                "target": result.spec.target,
                "status": result.status,
                "insufficient_sample_size": result.insufficient_sample_size,
            }
            for result in panel_results
        ]
        series.sort(key=lambda item: _as_str(item.get("label")) or "")

        panel_window = "28d"
        if panel_id == "annual_refresh_readiness":
            panel_window = "365d"
        elif panel_id == "slo_burn_overview":
            panel_window = "mixed"

        panels.append(
            {
                "panel_id": panel_id,
                "title": panel_id.replace("_", " ").title(),
                "window": panel_window,
                "series": series,
                "status": status,
            }
        )

    return panels


def _append_missing_artifact_errors(
    *, errors: list[dict[str, object]], domain_to_files: Mapping[str, Sequence[Path]]
) -> None:
    for domain, files in domain_to_files.items():
        if files:
            continue
        errors.append(
            {
                "code": "artifact_missing",
                "domain": domain,
                "message": f"No source artifacts discovered for domain '{domain}'.",
            }
        )


def _append_annual_refresh_artifact_errors(
    *,
    errors: list[dict[str, object]],
    has_annual_refresh_payload: bool,
    annual_signoff_files: Sequence[Path],
) -> None:
    if not has_annual_refresh_payload:
        errors.append(
            {
                "code": "artifact_missing",
                "domain": "annual_refresh",
                "artifact_type": "annual_refresh_payload",
                "message": (
                    "No annual_refresh profile run payload artifacts discovered for "
                    "domain 'annual_refresh'."
                ),
            }
        )
    if not annual_signoff_files:
        errors.append(
            {
                "code": "artifact_missing",
                "domain": "annual_refresh",
                "artifact_type": "annual_signoff",
                "message": (
                    "No annual signoff artifacts discovered for domain "
                    "'annual_refresh'."
                ),
            }
        )


def _refresh_event_key(*, profile_name: str, run_id: str) -> str:
    return f"{profile_name}:{run_id}"


def _discover_files(
    *, explicit_paths: Sequence[Path], roots_and_patterns: Sequence[tuple[Path, str]]
) -> list[Path]:
    discovered: list[Path] = []
    for path in explicit_paths:
        discovered.append(_expand_and_resolve(path))
    if discovered:
        return _dedupe_sorted(discovered)

    for root, pattern in roots_and_patterns:
        resolved_root = _expand_and_resolve(root)
        if not resolved_root.exists():
            continue
        for path in resolved_root.glob(pattern):
            if path.is_file():
                discovered.append(path.resolve())
    return _dedupe_sorted(discovered)


def _load_json_or_error(
    *, path: Path, domain: str, errors: list[dict[str, object]]
) -> Optional[dict[str, object]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        errors.append(
            {
                "code": "artifact_read_error",
                "domain": domain,
                "path": _portable_path(path),
                "message": str(exc),
            }
        )
        return None
    except json.JSONDecodeError as exc:
        errors.append(
            {
                "code": "invalid_json",
                "domain": domain,
                "path": _portable_path(path),
                "message": str(exc),
            }
        )
        return None

    if not isinstance(payload, dict):
        errors.append(
            {
                "code": "invalid_payload_shape",
                "domain": domain,
                "path": _portable_path(path),
                "message": "Top-level JSON value must be an object.",
            }
        )
        return None
    return payload


def _burn_rate_for_window(
    *,
    events: Sequence[_SliEvent],
    now_dt: datetime,
    target: float,
    window: str,
) -> tuple[Optional[float], int]:
    numerator, denominator = _window_fraction(
        events=events, now_dt=now_dt, window=window
    )
    if denominator == 0:
        return None, 0
    bad = denominator - numerator
    observed_error_rate = bad / denominator
    error_budget = 1 - target
    if error_budget <= 0:
        if observed_error_rate <= 0:
            return 0.0, denominator
        return 1_000_000.0, denominator
    return observed_error_rate / error_budget, denominator


def _error_budget_remaining(*, observed: Optional[float], target: float) -> float:
    if observed is None:
        return 1.0
    observed_error_rate = 1 - observed
    error_budget = 1 - target
    if error_budget <= 0:
        if observed_error_rate <= 0:
            return 1.0
        return 0.0
    remaining = 1 - (observed_error_rate / error_budget)
    return max(0.0, min(1.0, remaining))


def _window_fraction(
    *, events: Sequence[_SliEvent], now_dt: datetime, window: str
) -> tuple[int, int]:
    cutoff = now_dt - WINDOW_TO_DELTA[window]
    filtered = [event for event in events if event.observed_at >= cutoff]
    numerator = sum(1 for event in filtered if event.passed)
    denominator = len(filtered)
    return numerator, denominator


def _write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _write_timeseries_csv(path: Path, *, rows: Sequence[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "metric_id",
                    "domain",
                    "environment",
                    "profile_name",
                    "window",
                    "observed_at_utc",
                    "value",
                    "numerator",
                    "denominator",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "metric_id": row.get("metric_id"),
                        "domain": row.get("domain"),
                        "environment": row.get("environment"),
                        "profile_name": row.get("profile_name"),
                        "window": row.get("window"),
                        "observed_at_utc": row.get("observed_at_utc"),
                        "value": row.get("value"),
                        "numerator": row.get("numerator"),
                        "denominator": row.get("denominator"),
                    }
                )
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _dedupe_sorted(paths: Sequence[Path]) -> list[Path]:
    unique: dict[str, Path] = {}
    for path in paths:
        resolved = _expand_and_resolve(path)
        unique[str(resolved)] = resolved
    return [unique[key] for key in sorted(unique)]


def _sorted_paths(paths: Sequence[Path]) -> list[Path]:
    return _dedupe_sorted(paths)


def _expand_and_resolve(path: Path) -> Path:
    return path.expanduser().resolve()


def _portable_path(path: Path) -> str:
    resolved = _expand_and_resolve(path)
    return resolved.as_posix()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _round_float(value: Optional[float], places: int) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), places)


def _as_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    return {}


def _as_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    return []


def _as_mapping_list(value: object) -> list[Mapping[str, object]]:
    if not isinstance(value, list):
        return []
    rows: list[Mapping[str, object]] = []
    for item in value:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _as_str(value: object) -> Optional[str]:
    if isinstance(value, str):
        return value
    return None
