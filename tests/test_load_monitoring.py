from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli, load_monitoring
from accessdane_audit.db import init_db, session_scope

_STAGES = (
    "ingest_context",
    "build_context",
    "score_pipeline",
    "analysis_artifacts",
    "investigation_artifacts",
    "health_summary",
)


def test_load_monitoring_ok_warn_critical_and_deterministic_payload(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    baseline_paths: list[Path] = []
    for day_offset, duration_seconds in ((3, 100.0), (2, 100.0), (1, 100.0)):
        run_finished = fixed_now - timedelta(days=day_offset)
        baseline_paths.append(
            _write_refresh_run(
                artifact_base_dir=artifact_base_dir,
                run_id=f"2026031{day_offset}_daily_refresh_feature_v1_scoring_rules_v1",
                run_finished_at=run_finished,
                duration_seconds=duration_seconds,
                review_queue_rows=_queue_rows(high=3, medium=2, low=1, unreviewed=2),
                reviewed_case_count=4,
                threshold_candidate_count=1,
                exclusion_candidate_count=0,
            )
        )

    subject_ok_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_ok",
        run_finished_at=fixed_now - timedelta(minutes=15),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=3, medium=2, low=1, unreviewed=2),
        reviewed_case_count=4,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_warn_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_warn",
        run_finished_at=fixed_now - timedelta(minutes=10),
        duration_seconds=160.0,
        review_queue_rows=_queue_rows(high=3, medium=2, low=1, unreviewed=2),
        reviewed_case_count=4,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_critical_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_critical",
        run_finished_at=fixed_now - timedelta(minutes=5),
        duration_seconds=220.0,
        review_queue_rows=_queue_rows(high=3, medium=2, low=1, unreviewed=2),
        reviewed_case_count=4,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )

    with session_scope(database_url) as session:
        payload_ok_a = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_ok_path,
            run_persisted=True,
        )
        payload_ok_b = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_ok_path,
            run_persisted=True,
        )
        payload_warn = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_warn_path,
        )
        payload_critical = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_critical_path,
        )

    assert payload_ok_a == payload_ok_b
    assert payload_ok_a["summary"]["overall_severity"] == "ok"
    assert payload_warn["summary"]["overall_severity"] == "warn"
    assert payload_critical["summary"]["overall_severity"] == "critical"
    assert payload_ok_a["alerts"] == []
    assert payload_warn["alerts"][0]["severity"] == "warn"
    assert payload_critical["alerts"][0]["severity"] == "critical"

    signals = payload_ok_a["signals"]
    assert signals == sorted(
        signals,
        key=lambda signal: (
            signal["metric_key"],
            signal["family"],
            signal["signal_id"],
        ),
    )
    duration_warn = _signal_by_metric(payload_warn, "duration.total_seconds")
    duration_critical = _signal_by_metric(payload_critical, "duration.total_seconds")
    assert duration_warn["severity"] == "warn"
    assert duration_warn["reason_code"] == (
        "duration.warn_relative_delta.duration.total_seconds"
    )
    assert duration_critical["severity"] == "critical"
    assert duration_critical["reason_code"] == (
        "duration.critical_relative_delta.duration.total_seconds"
    )


def test_build_alert_payload_from_diagnostics_uses_matching_impacted_signals(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_alert.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    for day_offset in (3, 2, 1):
        _write_refresh_run(
            artifact_base_dir=artifact_base_dir,
            run_id=f"2026031{day_offset}_daily_refresh_feature_v1_scoring_rules_v1",
            run_finished_at=fixed_now - timedelta(days=day_offset),
            duration_seconds=100.0,
            review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=3),
            reviewed_case_count=2,
            threshold_candidate_count=1,
            exclusion_candidate_count=0,
        )

    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_warn_alert",
        run_finished_at=fixed_now - timedelta(minutes=2),
        duration_seconds=160.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=3),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )

    with session_scope(database_url) as session:
        payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_path,
        )

    alert_payload = load_monitoring.build_alert_payload_from_diagnostics(payload)
    assert alert_payload is not None
    assert alert_payload["alert"]["severity"] == "warn"
    assert alert_payload["error"] is None
    assert alert_payload["impacted_signals"]
    assert all(
        signal["severity"] == "warn" and signal["ignored"] is False
        for signal in alert_payload["impacted_signals"]
    )
    for action in alert_payload["operator_actions"]:
        assert action["message"] == action["description"]
        assert action["artifact_paths"] == action["required_artifact_paths"]
        assert action["severity"] == alert_payload["alert"]["severity"]


def test_load_monitor_cli_writes_diagnostics_and_alert_payload(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_cli.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    for day_offset in (3, 2, 1):
        _write_refresh_run(
            artifact_base_dir=artifact_base_dir,
            run_id=f"2026031{day_offset}_daily_refresh_feature_v1_scoring_rules_v1",
            run_finished_at=fixed_now - timedelta(days=day_offset),
            duration_seconds=100.0,
            review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
            reviewed_case_count=2,
            threshold_candidate_count=1,
            exclusion_candidate_count=0,
        )

    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_cli",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=160.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )

    diagnostics_out = tmp_path / "load_monitor_diagnostics.json"
    alert_out = tmp_path / "load_monitor_alert.json"
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "load-monitor",
            "--artifact-base-dir",
            str(artifact_base_dir),
            "--subject-refresh-payload",
            str(subject_path),
            "--out",
            str(diagnostics_out),
            "--alert-out",
            str(alert_out),
        ],
    )

    assert result.exit_code == 0, result.stdout
    diagnostics_payload = json.loads(diagnostics_out.read_text(encoding="utf-8"))
    alert_payload = json.loads(alert_out.read_text(encoding="utf-8"))
    assert diagnostics_payload["summary"]["overall_severity"] == "warn"
    assert alert_payload["alert"]["severity"] == "warn"


def _queue_rows(
    *, high: int, medium: int, low: int, unreviewed: int
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    idx = 0
    for risk_band, count in (("high", high), ("medium", medium), ("low", low)):
        for _ in range(count):
            idx += 1
            rows.append(
                {
                    "queue_rank": idx,
                    "score_id": idx,
                    "run_id": 1,
                    "feature_run_id": 1,
                    "parcel_id": f"P-{idx}",
                    "year": 2025,
                    "score_value": "75.00",
                    "risk_band": risk_band,
                    "requires_review": True,
                    "review_status": "unreviewed" if idx <= unreviewed else "reviewed",
                }
            )
    return rows


def _write_refresh_run(
    *,
    artifact_base_dir: Path,
    run_id: str,
    run_finished_at: datetime,
    duration_seconds: float,
    review_queue_rows: list[dict[str, object]],
    reviewed_case_count: int,
    threshold_candidate_count: int,
    exclusion_candidate_count: int,
) -> Path:
    run_date = run_finished_at.strftime("%Y%m%d")
    profile_name = "daily_refresh"
    feature_version = "feature_v1"
    ruleset_version = "scoring_rules_v1"
    root_path = artifact_base_dir / run_date / profile_name / run_id
    health_dir = root_path / "health_summary"
    analysis_dir = root_path / "analysis_artifacts"
    health_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)

    started_at = run_finished_at - timedelta(seconds=duration_seconds)
    stage_payloads: list[dict[str, object]] = []
    per_stage_duration = round(duration_seconds / len(_STAGES), 3)
    stage_start = started_at
    for stage_id in _STAGES:
        stage_finish = stage_start + timedelta(seconds=per_stage_duration)
        command_results: list[dict[str, object]] = []
        if stage_id == "analysis_artifacts":
            command_results = [
                {
                    "command_id": "review_queue",
                    "status": "succeeded",
                    "exit_code": 0,
                    "artifact_paths": [str(analysis_dir / "review_queue.json")],
                    "error_code": None,
                },
                {
                    "command_id": "review_feedback",
                    "status": "succeeded",
                    "exit_code": 0,
                    "artifact_paths": [str(analysis_dir / "review_feedback.json")],
                    "error_code": None,
                },
            ]
        stage_payloads.append(
            {
                "stage_id": stage_id,
                "status": "succeeded",
                "started_at": _iso(stage_start),
                "finished_at": _iso(stage_finish),
                "duration_seconds": per_stage_duration,
                "attempt": 1,
                "command_results": command_results,
            }
        )
        stage_start = stage_finish

    refresh_payload = {
        "run": {
            "run_type": "refresh_automation",
            "version_tag": "refresh_automation_v1",
            "run_id": run_id,
            "profile_name": profile_name,
            "status": "succeeded",
            "run_persisted": True,
            "started_at": _iso(started_at),
            "finished_at": _iso(run_finished_at),
        },
        "request": {
            "profile_name": profile_name,
            "run_date": run_date,
            "feature_version": feature_version,
            "ruleset_version": ruleset_version,
            "sales_ratio_base": "sales_ratio_v1",
            "top": 100,
            "source_files": {
                "retr": None,
                "permits": None,
                "appeals": None,
            },
        },
        "summary": {
            "stage_count": len(_STAGES),
            "stage_succeeded_count": len(_STAGES),
            "stage_failed_count": 0,
            "stage_blocked_count": 0,
            "stage_skipped_count": 0,
            "duration_seconds_total": duration_seconds,
        },
        "stages": stage_payloads,
        "artifacts": {
            "root_path": str(root_path),
            "latest_pointer_path": str(
                artifact_base_dir
                / "latest"
                / profile_name
                / feature_version
                / ruleset_version
            ),
            "stage_artifacts": {
                "analysis_artifacts": [
                    str(analysis_dir / "review_queue.json"),
                    str(analysis_dir / "review_feedback.json"),
                ]
            },
        },
        "diagnostics": {
            "warnings": [],
            "skip_reasons": [],
            "retry": {"attempt_count": 1, "retried_from_stage_id": None},
        },
        "error": None,
    }
    review_queue_payload = {
        "run": {"status": "succeeded"},
        "summary": {
            "returned_count": len(review_queue_rows),
        },
        "rows": review_queue_rows,
    }
    review_feedback_payload = {
        "run": {"status": "succeeded"},
        "summary": {"reviewed_case_count": reviewed_case_count},
        "recommendations": {
            "threshold_tuning_candidates": [
                {"reason_code": f"R{idx}"} for idx in range(threshold_candidate_count)
            ],
            "exclusion_tuning_candidates": [
                {"reason_code": f"E{idx}"} for idx in range(exclusion_candidate_count)
            ],
        },
    }

    refresh_path = health_dir / "refresh_run_payload.json"
    refresh_path.write_text(json.dumps(refresh_payload, indent=2), encoding="utf-8")
    (analysis_dir / "review_queue.json").write_text(
        json.dumps(review_queue_payload, indent=2), encoding="utf-8"
    )
    (analysis_dir / "review_feedback.json").write_text(
        json.dumps(review_feedback_payload, indent=2), encoding="utf-8"
    )
    return refresh_path


def _signal_by_metric(payload: dict[str, object], metric_key: str) -> dict[str, object]:
    for signal in payload["signals"]:  # type: ignore[index]
        if signal["metric_key"] == metric_key:
            return signal
    raise AssertionError(f"Missing metric signal {metric_key}")


def _iso(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
