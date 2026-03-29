from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from accessdane_audit import cli
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
    assert outputs_one["rollup"]["burn_alerts"] == []
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
