from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit import observability as observability_module
from accessdane_audit.observability import (
    ObservabilityError,
    build_observability_outputs,
    discover_observability_input_files,
    persist_observability_outputs,
)


def _fixed_now() -> datetime:
    return datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _set_stage_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = tmp_path / "data" / "environments" / "stage"
    monkeypatch.delenv("environment_name", raising=False)
    monkeypatch.setenv("ACCESSDANE_ENVIRONMENT", "stage")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("ACCESSDANE_BASE_URL", "https://accessdane.danecounty.gov")
    monkeypatch.setenv("ACCESSDANE_RAW_DIR", str(root / "raw"))
    monkeypatch.setenv("ACCESSDANE_USER_AGENT", "AccessDaneAudit/0.1")
    monkeypatch.setenv("ACCESSDANE_TIMEOUT", "30")
    monkeypatch.setenv("ACCESSDANE_RETRIES", "3")
    monkeypatch.setenv("ACCESSDANE_BACKOFF", "1.5")
    monkeypatch.setenv("ACCESSDANE_REFRESH_PROFILE", "analysis_only")
    monkeypatch.setenv("ACCESSDANE_FEATURE_VERSION", "feature_stage_v2")
    monkeypatch.setenv("ACCESSDANE_RULESET_VERSION", "rules_stage_v2")
    monkeypatch.setenv("ACCESSDANE_SALES_RATIO_BASE", "sales_stage_v2")
    monkeypatch.setenv("ACCESSDANE_REFRESH_TOP", "25")
    monkeypatch.setenv("ACCESSDANE_ARTIFACT_BASE_DIR", str(root / "refresh_runs"))
    monkeypatch.setenv(
        "ACCESSDANE_REFRESH_LOG_DIR",
        str(root / "refresh_runs" / "logs"),
    )
    monkeypatch.setenv(
        "ACCESSDANE_BENCHMARK_BASE_DIR",
        str(root / "benchmark_packs"),
    )
    monkeypatch.setenv("ALERT_ROUTE_GROUP", "ops-alerts")
    monkeypatch.setenv("PROMOTION_APPROVER_GROUP", "release-approvers")
    monkeypatch.setenv(
        "PROMOTION_FREEZE_FILE",
        str(root / "promotion_freeze.json"),
    )


def _refresh_payload(
    *,
    run_id: str,
    profile_name: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    stage_status: str = "succeeded",
) -> dict[str, object]:
    return {
        "run": {
            "run_id": run_id,
            "profile_name": profile_name,
            "status": status,
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
        },
        "stages": [
            {"stage_id": "ingest_context", "status": stage_status},
            {"stage_id": "build_context", "status": stage_status},
            {"stage_id": "score_pipeline", "status": stage_status},
            {"stage_id": "analysis_artifacts", "status": stage_status},
            {"stage_id": "investigation_artifacts", "status": stage_status},
            {"stage_id": "health_summary", "status": stage_status},
        ],
    }


def _parser_drift_diff_payload(
    *, run_id: str, generated_at: datetime
) -> dict[str, object]:
    return {
        "run": {
            "run_type": "parser_drift_diff",
            "run_id": run_id,
            "status": "succeeded",
            "generated_at": _iso(generated_at),
        },
        "baseline": {"artifact_path": "data/baseline_snapshot.json"},
        "current": {"artifact_path": "data/current_snapshot.json"},
        "alerts": [],
    }


def _load_monitor_payload(
    *, run_id: str, started_at: datetime, finished_at: datetime
) -> dict[str, object]:
    return {
        "run": {
            "run_id": run_id,
            "status": "succeeded",
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
        },
        "summary": {"overall_severity": "warn"},
    }


def _annual_signoff_payload(
    *, run_id: str, status: str, updated_at: datetime
) -> dict[str, object]:
    return {
        "run": {
            "run_id": run_id,
            "profile_name": "annual_refresh",
            "status": status,
            "created_at": _iso(updated_at),
            "updated_at": _iso(updated_at),
        }
    }


def _benchmark_payload(*, run_id: str, finished_at: datetime) -> dict[str, object]:
    return {
        "run": {
            "run_id": run_id,
            "status": "succeeded",
            "finished_at": _iso(finished_at),
        },
        "comparison": {"overall_severity": "warn"},
    }


def _scheduler_payload(
    *,
    scheduler_run_id: str,
    profile_name: str,
    refresh_run_id: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
) -> dict[str, object]:
    return {
        "scheduler_run": {
            "scheduler_run_id": scheduler_run_id,
            "profile_name": profile_name,
            "refresh_run_id": refresh_run_id,
            "updated_at_utc": _iso(finished_at),
        },
        "attempts": [
            {
                "attempt_index": 1,
                "started_at_utc": _iso(started_at),
                "finished_at_utc": _iso(finished_at),
            }
        ],
        "result": {
            "status": status,
            "last_attempt_finished_at_utc": _iso(finished_at),
        },
    }


def test_build_observability_outputs_is_deterministic(tmp_path: Path) -> None:
    now_dt = _fixed_now()

    refresh_path = (
        tmp_path
        / "refresh_runs"
        / "20260329"
        / "daily_refresh"
        / "refresh_run_001"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    _write_json(
        refresh_path,
        _refresh_payload(
            run_id="refresh_run_001",
            profile_name="daily_refresh",
            status="succeeded",
            started_at=now_dt - timedelta(minutes=40),
            finished_at=now_dt - timedelta(minutes=5),
        ),
    )

    parser_path = tmp_path / "parser" / "parser_drift_diff.json"
    _write_json(
        parser_path,
        _parser_drift_diff_payload(
            run_id="parser_diff_001",
            generated_at=now_dt - timedelta(minutes=10),
        ),
    )

    load_monitor_path = tmp_path / "monitor" / "load_monitor.json"
    _write_json(
        load_monitor_path,
        _load_monitor_payload(
            run_id="load_monitor_001",
            started_at=now_dt - timedelta(minutes=12),
            finished_at=now_dt - timedelta(minutes=3),
        ),
    )

    annual_signoff_path = tmp_path / "annual" / "annual_signoff.json"
    _write_json(
        annual_signoff_path,
        _annual_signoff_payload(
            run_id="annual_refresh_001",
            status="approved",
            updated_at=now_dt - timedelta(minutes=2),
        ),
    )

    benchmark_path = tmp_path / "benchmark" / "benchmark_pack.json"
    _write_json(
        benchmark_path,
        _benchmark_payload(
            run_id="benchmark_001",
            finished_at=now_dt - timedelta(hours=1),
        ),
    )

    outputs_one = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="observability_20260329_120000",
        refresh_payload_files=[refresh_path],
        scheduler_payload_files=[],
        parser_drift_files=[parser_path],
        load_monitor_files=[load_monitor_path],
        annual_signoff_files=[annual_signoff_path],
        benchmark_files=[benchmark_path],
        now_fn=_fixed_now,
    )
    outputs_two = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="observability_20260329_120000",
        refresh_payload_files=[refresh_path],
        scheduler_payload_files=[],
        parser_drift_files=[parser_path],
        load_monitor_files=[load_monitor_path],
        annual_signoff_files=[annual_signoff_path],
        benchmark_files=[benchmark_path],
        now_fn=_fixed_now,
    )

    assert outputs_one == outputs_two
    assert (
        outputs_one["rollup"]["run"]["observability_run_id"]
        == "observability_20260329_120000"
    )
    assert outputs_one["slo_evaluation"]["evaluation"]["measurement_window"] == "mixed"
    daily_refresh = next(
        row
        for row in outputs_one["slo_evaluation"]["sli_results"]
        if row["sli_id"] == "refresh.success_ratio.daily_refresh"
    )
    assert daily_refresh["insufficient_sample_size"] is True
    analysis_only = next(
        row
        for row in outputs_one["slo_evaluation"]["sli_results"]
        if row["sli_id"] == "refresh.success_ratio.analysis_only"
    )
    assert analysis_only["error_budget_burn_rate"] is None

    refresh_panel = next(
        panel
        for panel in outputs_one["dashboard_snapshot"]["panels"]
        if panel["panel_id"] == "refresh_reliability"
    )
    refresh_series = next(
        row
        for row in refresh_panel["series"]
        if row["label"] == "refresh.success_ratio.daily_refresh"
    )
    assert refresh_series["insufficient_sample_size"] is True


def test_observability_burn_threshold_classification_marks_critical(
    tmp_path: Path,
) -> None:
    now_dt = _fixed_now()
    refresh_files: list[Path] = []
    for idx in range(30):
        run_id = f"refresh_fail_{idx:03d}"
        payload_path = (
            tmp_path
            / "refresh_runs"
            / "20260329"
            / "daily_refresh"
            / run_id
            / "health_summary"
            / "refresh_run_payload.json"
        )
        _write_json(
            payload_path,
            _refresh_payload(
                run_id=run_id,
                profile_name="daily_refresh",
                status="failed",
                started_at=now_dt - timedelta(minutes=50),
                finished_at=now_dt - timedelta(minutes=20),
                stage_status="failed",
            ),
        )
        refresh_files.append(payload_path)

    outputs = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="observability_20260329_critical",
        refresh_payload_files=refresh_files,
        scheduler_payload_files=[],
        parser_drift_files=[],
        load_monitor_files=[],
        annual_signoff_files=[],
        benchmark_files=[],
        now_fn=_fixed_now,
    )

    burn_alerts = outputs["rollup"]["burn_alerts"]
    assert any(
        alert["sli_id"] == "refresh.success_ratio.daily_refresh"
        and alert["severity"] == "critical"
        for alert in burn_alerts
    )
    refresh_burn_alert = next(
        alert
        for alert in burn_alerts
        if alert["sli_id"] == "refresh.success_ratio.daily_refresh"
    )
    assert refresh_burn_alert["routing_key"] == "ops-alerts.refresh.critical"

    sli_results = outputs["slo_evaluation"]["sli_results"]
    refresh_sli = next(
        row
        for row in sli_results
        if row["sli_id"] == "refresh.success_ratio.daily_refresh"
    )
    assert refresh_sli["status"] == "breached"
    assert refresh_sli["denominator"] == 30


def test_observability_rollup_cli_writes_required_artifacts(tmp_path: Path) -> None:
    runner = CliRunner()

    refresh_root = tmp_path / "refresh_runs"
    benchmark_root = tmp_path / "benchmark_packs"
    startup_root = tmp_path / "startup_runs"
    output_root = tmp_path / "observability_output"
    summary_out = tmp_path / "observability_summary.json"

    now_dt = _fixed_now()
    refresh_path = (
        refresh_root
        / "20260329"
        / "daily_refresh"
        / "refresh_run_001"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    _write_json(
        refresh_path,
        _refresh_payload(
            run_id="refresh_run_001",
            profile_name="daily_refresh",
            status="succeeded",
            started_at=now_dt - timedelta(minutes=30),
            finished_at=now_dt - timedelta(minutes=10),
        ),
    )

    benchmark_path = (
        benchmark_root
        / "20260329"
        / "daily_refresh"
        / "benchmark_001"
        / "benchmark_pack.json"
    )
    _write_json(
        benchmark_path,
        _benchmark_payload(
            run_id="benchmark_001",
            finished_at=now_dt - timedelta(hours=3),
        ),
    )

    result = runner.invoke(
        cli.app,
        [
            "observability-rollup",
            "--refresh-artifact-base-dir",
            str(refresh_root),
            "--benchmark-artifact-base-dir",
            str(benchmark_root),
            "--startup-artifact-base-dir",
            str(startup_root),
            "--artifact-base-dir",
            str(output_root),
            "--run-date",
            "20260329",
            "--observability-run-id",
            "obs_day10_001",
            "--out",
            str(summary_out),
        ],
    )

    assert result.exit_code == 0, result.output
    summary_payload = json.loads(summary_out.read_text(encoding="utf-8"))

    artifact_paths = summary_payload["artifact_paths"]
    rollup_path = Path(artifact_paths["rollup_path"])
    eval_path = Path(artifact_paths["slo_evaluation_path"])
    dashboard_path = Path(artifact_paths["dashboard_snapshot_path"])
    timeseries_path = Path(artifact_paths["timeseries_path"])

    assert rollup_path.exists()
    assert eval_path.exists()
    assert dashboard_path.exists()
    assert timeseries_path.exists()

    rollup_payload = json.loads(rollup_path.read_text(encoding="utf-8"))
    assert sorted(rollup_payload.keys()) == [
        "burn_alerts",
        "errors",
        "inputs",
        "metrics",
        "run",
        "slo_status",
    ]


def test_observability_rollup_cli_reports_specific_override_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_stage_env(monkeypatch, tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "observability-rollup",
            "--benchmark-artifact-base-dir",
            str(tmp_path / "data" / "environments" / "prod" / "benchmark_packs"),
        ],
    )

    assert result.exit_code == 2
    stderr = getattr(result, "stderr", "")
    combined_output = f"{result.output}{stderr}"
    assert "--benchmark-artifact-base-dir" in combined_output


def test_observability_rollup_cli_reports_startup_override_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_stage_env(monkeypatch, tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "observability-rollup",
            "--startup-artifact-base-dir",
            str(tmp_path / "data" / "environments" / "prod" / "startup_runs"),
        ],
    )

    assert result.exit_code == 2
    stderr = getattr(result, "stderr", "")
    combined_output = f"{result.output}{stderr}"
    assert "--startup-artifact-base-dir" in combined_output


def test_observability_rollup_cli_reports_observability_run_id_hint(
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "observability-rollup",
            "--refresh-artifact-base-dir",
            str(tmp_path / "refresh_runs"),
            "--benchmark-artifact-base-dir",
            str(tmp_path / "benchmark_packs"),
            "--startup-artifact-base-dir",
            str(tmp_path / "startup_runs"),
            "--artifact-base-dir",
            str(tmp_path / "out"),
            "--run-date",
            "20260329",
            "--observability-run-id",
            ".",
        ],
    )

    assert result.exit_code == 2
    stderr = getattr(result, "stderr", "")
    combined_output = f"{result.output}{stderr}"
    assert "--observability-run-id" in combined_output


def test_observability_uses_scheduler_inputs_for_refresh_slis(tmp_path: Path) -> None:
    now_dt = _fixed_now()
    scheduler_path = tmp_path / "refresh_runs" / "scheduler_logs" / "sched_001.json"
    _write_json(
        scheduler_path,
        _scheduler_payload(
            scheduler_run_id="sched_001",
            profile_name="daily_refresh",
            refresh_run_id="refresh_run_from_scheduler",
            started_at=now_dt - timedelta(minutes=35),
            finished_at=now_dt - timedelta(minutes=5),
            status="succeeded",
        ),
    )

    outputs = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="obs_scheduler_only",
        refresh_payload_files=[],
        scheduler_payload_files=[scheduler_path],
        parser_drift_files=[],
        load_monitor_files=[],
        annual_signoff_files=[],
        benchmark_files=[],
        now_fn=_fixed_now,
    )

    refresh_sli = next(
        row
        for row in outputs["slo_evaluation"]["sli_results"]
        if row["sli_id"] == "refresh.success_ratio.daily_refresh"
    )
    assert refresh_sli["denominator"] == 1
    assert refresh_sli["numerator"] == 1

    latency_sli = next(
        row
        for row in outputs["slo_evaluation"]["sli_results"]
        if row["sli_id"] == "refresh.latency_compliance.daily_refresh"
    )
    assert latency_sli["denominator"] == 1
    assert latency_sli["numerator"] == 1


def test_benchmark_freshness_hourly_checks_cover_window(tmp_path: Path) -> None:
    now_dt = _fixed_now()
    benchmark_path = tmp_path / "benchmark" / "benchmark_pack.json"
    _write_json(
        benchmark_path,
        _benchmark_payload(
            run_id="benchmark_001",
            finished_at=now_dt - timedelta(hours=1),
        ),
    )

    outputs = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="obs_benchmark_hourly_window",
        refresh_payload_files=[],
        scheduler_payload_files=[],
        parser_drift_files=[],
        load_monitor_files=[],
        annual_signoff_files=[],
        benchmark_files=[benchmark_path],
        now_fn=_fixed_now,
    )

    freshness_sli = next(
        row
        for row in outputs["slo_evaluation"]["sli_results"]
        if row["sli_id"] == "benchmark.freshness_compliance"
    )
    assert freshness_sli["denominator"] == 28 * 24
    assert freshness_sli["numerator"] == 2


def test_persist_observability_outputs_validates_inputs(tmp_path: Path) -> None:
    outputs = {
        "rollup": {"run": {}},
        "slo_evaluation": {"evaluation": {}, "sli_results": [], "non_computable": []},
        "dashboard_snapshot": {"snapshot": {}, "panels": [], "alerts": []},
        "timeseries_rows": [],
    }

    with pytest.raises(ObservabilityError):
        persist_observability_outputs(
            artifact_base_dir=tmp_path,
            run_date="2026-03-29",
            observability_run_id="obs_ok",
            outputs=outputs,
        )
    with pytest.raises(ObservabilityError):
        persist_observability_outputs(
            artifact_base_dir=tmp_path,
            run_date="20260329",
            observability_run_id="../obs_bad",
            outputs=outputs,
        )
    with pytest.raises(ObservabilityError):
        persist_observability_outputs(
            artifact_base_dir=tmp_path,
            run_date="20260329",
            observability_run_id="obs_ok",
            outputs={
                "rollup": {},
                "slo_evaluation": {},
                "dashboard_snapshot": {},
            },
        )


def test_observability_run_id_rejects_dot_segments(tmp_path: Path) -> None:
    with pytest.raises(ObservabilityError):
        build_observability_outputs(
            environment_name="dev",
            alert_route_group="ops-alerts",
            run_date="20260329",
            observability_run_id=".",
            refresh_payload_files=[],
            scheduler_payload_files=[],
            parser_drift_files=[],
            load_monitor_files=[],
            annual_signoff_files=[],
            benchmark_files=[],
            now_fn=_fixed_now,
        )

    outputs = {
        "rollup": {"run": {}},
        "slo_evaluation": {"evaluation": {}, "sli_results": [], "non_computable": []},
        "dashboard_snapshot": {"snapshot": {}, "panels": [], "alerts": []},
        "timeseries_rows": [],
    }
    with pytest.raises(ObservabilityError):
        persist_observability_outputs(
            artifact_base_dir=tmp_path,
            run_date="20260329",
            observability_run_id="..",
            outputs=outputs,
        )


def test_no_events_do_not_set_insufficient_sample_size() -> None:
    outputs = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="obs_no_events",
        refresh_payload_files=[],
        scheduler_payload_files=[],
        parser_drift_files=[],
        load_monitor_files=[],
        annual_signoff_files=[],
        benchmark_files=[],
        now_fn=_fixed_now,
    )

    daily_refresh = next(
        row
        for row in outputs["slo_evaluation"]["sli_results"]
        if row["sli_id"] == "refresh.success_ratio.daily_refresh"
    )
    assert daily_refresh["denominator"] == 0
    assert daily_refresh["insufficient_sample_size"] is False

    non_computable = outputs["slo_evaluation"]["non_computable"]
    assert any(
        row["sli_id"] == "refresh.success_ratio.daily_refresh"
        and row["reason"] == "no_events"
        for row in non_computable
    )
    assert not any(
        row["sli_id"] == "refresh.success_ratio.daily_refresh"
        and row["reason"] == "insufficient_sample_size"
        for row in non_computable
    )

    compliance_metric = next(
        row
        for row in outputs["rollup"]["metrics"]
        if row["metric_id"] == "refresh.success_ratio.daily_refresh.compliance"
        and row["window"] == "24h"
    )
    assert compliance_metric["value"] is None
    assert compliance_metric["numerator"] == 0
    assert compliance_metric["denominator"] == 0

    timeseries_row = next(
        row
        for row in outputs["timeseries_rows"]
        if row["metric_id"] == "refresh.success_ratio.daily_refresh.compliance"
        and row["window"] == "24h"
    )
    assert timeseries_row["value"] == ""
    assert timeseries_row["numerator"] == "0"
    assert timeseries_row["denominator"] == "0"


def test_annual_refresh_domain_requires_signoff_and_refresh_payload(
    tmp_path: Path,
) -> None:
    now_dt = _fixed_now()
    annual_refresh_payload = (
        tmp_path
        / "refresh_runs"
        / "20260329"
        / "annual_refresh"
        / "annual_refresh_001"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    _write_json(
        annual_refresh_payload,
        _refresh_payload(
            run_id="annual_refresh_001",
            profile_name="annual_refresh",
            status="succeeded",
            started_at=now_dt - timedelta(hours=3),
            finished_at=now_dt - timedelta(hours=1),
        ),
    )

    outputs = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="obs_annual_refresh_payload_only",
        refresh_payload_files=[annual_refresh_payload],
        scheduler_payload_files=[],
        parser_drift_files=[],
        load_monitor_files=[],
        annual_signoff_files=[],
        benchmark_files=[],
        now_fn=_fixed_now,
    )

    errors = outputs["rollup"]["errors"]
    assert any(
        row.get("code") == "artifact_missing"
        and row.get("domain") == "annual_refresh"
        and row.get("artifact_type") == "annual_signoff"
        for row in errors
    )


def test_annual_refresh_domain_not_missing_when_both_artifact_types_present(
    tmp_path: Path,
) -> None:
    now_dt = _fixed_now()
    annual_refresh_payload = (
        tmp_path
        / "refresh_runs"
        / "20260329"
        / "annual_refresh"
        / "annual_refresh_001"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    annual_signoff_path = tmp_path / "annual" / "annual_signoff.json"
    _write_json(
        annual_refresh_payload,
        _refresh_payload(
            run_id="annual_refresh_001",
            profile_name="annual_refresh",
            status="succeeded",
            started_at=now_dt - timedelta(hours=3),
            finished_at=now_dt - timedelta(hours=1),
        ),
    )
    _write_json(
        annual_signoff_path,
        _annual_signoff_payload(
            run_id="annual_refresh_signoff_001",
            status="approved",
            updated_at=now_dt - timedelta(minutes=5),
        ),
    )

    outputs = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="obs_annual_refresh_complete_inputs",
        refresh_payload_files=[annual_refresh_payload],
        scheduler_payload_files=[],
        parser_drift_files=[],
        load_monitor_files=[],
        annual_signoff_files=[annual_signoff_path],
        benchmark_files=[],
        now_fn=_fixed_now,
    )

    errors = outputs["rollup"]["errors"]
    assert not any(
        row.get("code") == "artifact_missing" and row.get("domain") == "annual_refresh"
        for row in errors
    )


def test_annual_refresh_domain_requires_annual_refresh_payload_profile(
    tmp_path: Path,
) -> None:
    now_dt = _fixed_now()
    daily_refresh_payload = (
        tmp_path
        / "refresh_runs"
        / "20260329"
        / "daily_refresh"
        / "daily_refresh_001"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    annual_signoff_path = tmp_path / "annual" / "annual_signoff.json"
    _write_json(
        daily_refresh_payload,
        _refresh_payload(
            run_id="daily_refresh_001",
            profile_name="daily_refresh",
            status="succeeded",
            started_at=now_dt - timedelta(hours=3),
            finished_at=now_dt - timedelta(hours=1),
        ),
    )
    _write_json(
        annual_signoff_path,
        _annual_signoff_payload(
            run_id="annual_refresh_signoff_001",
            status="approved",
            updated_at=now_dt - timedelta(minutes=5),
        ),
    )

    outputs = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="obs_annual_refresh_missing_profile_payload",
        refresh_payload_files=[daily_refresh_payload],
        scheduler_payload_files=[],
        parser_drift_files=[],
        load_monitor_files=[],
        annual_signoff_files=[annual_signoff_path],
        benchmark_files=[],
        now_fn=_fixed_now,
    )

    errors = outputs["rollup"]["errors"]
    assert any(
        row.get("code") == "artifact_missing"
        and row.get("domain") == "annual_refresh"
        and row.get("artifact_type") == "annual_refresh_payload"
        for row in errors
    )
    assert not any(
        row.get("code") == "artifact_missing"
        and row.get("domain") == "annual_refresh"
        and row.get("artifact_type") == "annual_signoff"
        for row in errors
    )


def test_rollup_inputs_emit_absolute_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_dt = _fixed_now()
    refresh_payload = (
        tmp_path
        / "refresh_runs"
        / "20260329"
        / "daily_refresh"
        / "refresh_run_001"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    _write_json(
        refresh_payload,
        _refresh_payload(
            run_id="refresh_run_001",
            profile_name="daily_refresh",
            status="succeeded",
            started_at=now_dt - timedelta(minutes=25),
            finished_at=now_dt - timedelta(minutes=10),
        ),
    )

    monkeypatch.chdir(tmp_path)
    outputs = build_observability_outputs(
        environment_name="dev",
        alert_route_group="ops-alerts",
        run_date="20260329",
        observability_run_id="obs_absolute_inputs",
        refresh_payload_files=[refresh_payload],
        scheduler_payload_files=[],
        parser_drift_files=[],
        load_monitor_files=[],
        annual_signoff_files=[],
        benchmark_files=[],
        now_fn=_fixed_now,
    )

    assert outputs["rollup"]["inputs"]["refresh_payload_files"] == [
        refresh_payload.resolve().as_posix()
    ]


def test_atomic_writes_use_unique_temp_files(tmp_path: Path) -> None:
    json_path = tmp_path / "observability_rollup.json"
    legacy_json_tmp = json_path.with_suffix(f"{json_path.suffix}.tmp")
    legacy_json_tmp.write_text("do-not-touch", encoding="utf-8")
    legacy_json_tmp.chmod(0o444)

    csv_path = tmp_path / "observability_metric_timeseries.csv"
    legacy_csv_tmp = csv_path.with_suffix(f"{csv_path.suffix}.tmp")
    legacy_csv_tmp.write_text("do-not-touch", encoding="utf-8")
    legacy_csv_tmp.chmod(0o444)

    try:
        observability_module._write_json_atomic(json_path, {"ok": True})
        observability_module._write_timeseries_csv(
            csv_path,
            rows=[
                {
                    "metric_id": "refresh.success_ratio.daily_refresh.compliance",
                    "domain": "refresh",
                    "environment": "dev",
                    "profile_name": "daily_refresh",
                    "window": "24h",
                    "observed_at_utc": "2026-03-29T12:00:00Z",
                    "value": "1.0",
                    "numerator": "1",
                    "denominator": "1",
                }
            ],
        )
    finally:
        legacy_json_tmp.chmod(0o644)
        legacy_csv_tmp.chmod(0o644)

    assert json.loads(json_path.read_text(encoding="utf-8")) == {"ok": True}
    assert "metric_id,domain,environment" in csv_path.read_text(encoding="utf-8")
    assert legacy_json_tmp.read_text(encoding="utf-8") == "do-not-touch"
    assert legacy_csv_tmp.read_text(encoding="utf-8") == "do-not-touch"


def test_discovery_scans_full_history_for_rolling_windows(tmp_path: Path) -> None:
    refresh_root = tmp_path / "refresh_runs"
    benchmark_root = tmp_path / "benchmark_packs"
    startup_root = tmp_path / "startup_runs"

    refresh_old = (
        refresh_root
        / "20260301"
        / "daily_refresh"
        / "refresh_old_001"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    benchmark_old = (
        benchmark_root
        / "20260301"
        / "daily_refresh"
        / "benchmark_old_001"
        / "benchmark_pack.json"
    )
    _write_json(
        refresh_old,
        _refresh_payload(
            run_id="refresh_old_001",
            profile_name="daily_refresh",
            status="succeeded",
            started_at=_fixed_now() - timedelta(days=1, minutes=25),
            finished_at=_fixed_now() - timedelta(days=1, minutes=5),
        ),
    )
    _write_json(
        benchmark_old,
        _benchmark_payload(
            run_id="benchmark_old_001",
            finished_at=_fixed_now() - timedelta(days=1, minutes=2),
        ),
    )

    discovered = discover_observability_input_files(
        refresh_artifact_base_dir=refresh_root,
        benchmark_artifact_base_dir=benchmark_root,
        startup_artifact_base_dir=startup_root,
        run_date="20260329",
    )

    assert refresh_old.resolve() in discovered["refresh"]
    assert benchmark_old.resolve() in discovered["benchmark"]


def test_discovery_prefers_fixed_depth_patterns(tmp_path: Path) -> None:
    refresh_root = tmp_path / "refresh_runs"
    benchmark_root = tmp_path / "benchmark_packs"
    startup_root = tmp_path / "startup_runs"

    fixed_depth_refresh = (
        refresh_root
        / "20260329"
        / "daily_refresh"
        / "run_001"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    deep_refresh = (
        refresh_root
        / "20260329"
        / "daily_refresh"
        / "run_002"
        / "nested"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    fixed_depth_benchmark = (
        benchmark_root
        / "20260329"
        / "daily_refresh"
        / "bench_001"
        / "benchmark_pack.json"
    )
    deep_benchmark = (
        benchmark_root
        / "20260329"
        / "daily_refresh"
        / "bench_002"
        / "nested"
        / "benchmark_pack.json"
    )

    _write_json(
        fixed_depth_refresh,
        _refresh_payload(
            run_id="run_001",
            profile_name="daily_refresh",
            status="succeeded",
            started_at=_fixed_now() - timedelta(minutes=20),
            finished_at=_fixed_now() - timedelta(minutes=10),
        ),
    )
    _write_json(
        deep_refresh,
        _refresh_payload(
            run_id="run_002",
            profile_name="daily_refresh",
            status="succeeded",
            started_at=_fixed_now() - timedelta(minutes=40),
            finished_at=_fixed_now() - timedelta(minutes=30),
        ),
    )
    _write_json(
        fixed_depth_benchmark,
        _benchmark_payload(
            run_id="bench_001",
            finished_at=_fixed_now() - timedelta(hours=1),
        ),
    )
    _write_json(
        deep_benchmark,
        _benchmark_payload(
            run_id="bench_002",
            finished_at=_fixed_now() - timedelta(hours=2),
        ),
    )

    discovered = discover_observability_input_files(
        refresh_artifact_base_dir=refresh_root,
        benchmark_artifact_base_dir=benchmark_root,
        startup_artifact_base_dir=startup_root,
        run_date="20260329",
    )

    assert fixed_depth_refresh.resolve() in discovered["refresh"]
    assert deep_refresh.resolve() not in discovered["refresh"]
    assert fixed_depth_benchmark.resolve() in discovered["benchmark"]
    assert deep_benchmark.resolve() not in discovered["benchmark"]


def test_observability_rollup_cli_default_run_id_includes_subseconds(
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    summary_out = tmp_path / "summary.json"

    result = runner.invoke(
        cli.app,
        [
            "observability-rollup",
            "--refresh-artifact-base-dir",
            str(tmp_path / "refresh_runs"),
            "--benchmark-artifact-base-dir",
            str(tmp_path / "benchmark_packs"),
            "--startup-artifact-base-dir",
            str(tmp_path / "startup_runs"),
            "--artifact-base-dir",
            str(tmp_path / "out"),
            "--run-date",
            "20260329",
            "--out",
            str(summary_out),
        ],
    )

    assert result.exit_code == 0, result.output
    summary_payload = json.loads(summary_out.read_text(encoding="utf-8"))
    run_id = summary_payload["run"]["observability_run_id"]
    assert re.fullmatch(r"observability_20260329_\d{6}_\d{6}", run_id)
