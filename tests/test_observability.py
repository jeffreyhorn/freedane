from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.observability import build_observability_outputs


def _fixed_now() -> datetime:
    return datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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
