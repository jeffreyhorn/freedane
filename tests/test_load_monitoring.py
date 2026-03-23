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

    for day_offset, duration_seconds in ((3, 100.0), (2, 100.0), (1, 100.0)):
        run_finished = fixed_now - timedelta(days=day_offset)
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


def test_load_monitoring_anchors_window_end_at_to_run_finished_at(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_window_anchor.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    subject_finished_at = datetime(2026, 3, 18, 11, 59, tzinfo=timezone.utc)

    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260317_daily_refresh_feature_v1_scoring_rules_v1_history",
        run_finished_at=subject_finished_at - timedelta(days=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_window_anchor",
        run_finished_at=subject_finished_at,
        duration_seconds=120.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )

    now_values = [
        datetime(2026, 3, 18, 12, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 18, 12, 0, 5, tzinfo=timezone.utc),
    ]

    def _next_now() -> datetime:
        if not now_values:
            raise AssertionError("Unexpected extra _now_utc call.")
        return now_values.pop(0)

    monkeypatch.setattr(load_monitoring, "_now_utc", _next_now)

    with session_scope(database_url) as session:
        payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_path,
        )

    assert payload["run"]["status"] == "succeeded"
    assert (
        payload["diagnostics"]["history"]["window_bounds"]["window_1d"]["end_at"]
        == payload["run"]["finished_at"]
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
    assert alert_payload["operator_actions"][1]["command"] == (
        f"ls {artifact_base_dir}/*/daily_refresh/parser_drift_runtime/*.json "
        f"{artifact_base_dir}/*/daily_refresh/parser_drift_diff/*.json"
    )


def test_build_alert_payload_uses_profile_and_artifact_base_for_actions(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "custom_runs"
    subject_path = (
        artifact_base_dir
        / "20260318"
        / "county_refresh"
        / "20260318_county_refresh_feature_v1_scoring_rules_v1"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    payload = {
        "run": {
            "run_type": "load_monitoring",
            "version_tag": "load_monitoring_v1",
            "run_id": "monitor_run",
            "status": "succeeded",
            "run_persisted": False,
            "started_at": "2026-03-18T11:59:00Z",
            "finished_at": "2026-03-18T12:00:00Z",
        },
        "subject": {
            "run_id": "subject_run",
            "profile_name": "county_refresh",
            "run_date": "20260318",
            "feature_version": "feature_v1",
            "ruleset_version": "scoring_rules_v1",
            "refresh_payload_path": str(subject_path),
            "refresh_status": "succeeded",
            "refresh_finished_at": "2026-03-18T11:58:00Z",
        },
        "summary": {"overall_severity": "critical"},
        "signals": [
            {
                "signal_id": "duration.total_seconds",
                "family": "duration",
                "metric_key": "duration.total_seconds",
                "severity": "critical",
                "ignored": False,
                "reason_code": (
                    "duration.critical_relative_delta.duration.total_seconds"
                ),
            }
        ],
        "rollups": [],
        "alerts": [{"alert_id": "subject_run.critical"}],
        "diagnostics": {"warnings": [], "source_artifacts": []},
        "error": None,
    }

    alert_payload = load_monitoring.build_alert_payload_from_diagnostics(payload)

    assert alert_payload is not None
    action_by_id = {
        action["action_id"]: action for action in alert_payload["operator_actions"]
    }
    assert action_by_id["run_immediate_daily_refresh"]["command"] == (
        ".venv/bin/accessdane refresh-runner --profile-name county_refresh"
    )
    assert action_by_id["inspect_ingest_and_drift"]["command"] == (
        f"ls {artifact_base_dir}/latest/county_refresh"
    )


def test_build_alert_payload_from_diagnostics_derives_alert_id_without_alerts_array(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_alert_fallback.sqlite"
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
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_warn_no_alerts",
        run_finished_at=fixed_now - timedelta(minutes=2),
        duration_seconds=160.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
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

    payload["alerts"] = []
    alert_payload = load_monitoring.build_alert_payload_from_diagnostics(payload)
    assert alert_payload is not None
    assert alert_payload["alert"]["severity"] == "warn"
    assert alert_payload["alert"]["alert_id"] == f"{payload['subject']['run_id']}.warn"


def test_build_alert_payload_from_diagnostics_returns_error_when_alert_id_missing(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_alert_missing_id.sqlite"
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
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_warn_missing_id",
        run_finished_at=fixed_now - timedelta(minutes=2),
        duration_seconds=160.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
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

    payload["alerts"] = []
    payload["subject"]["run_id"] = None
    alert_payload = load_monitoring.build_alert_payload_from_diagnostics(payload)
    assert alert_payload is not None
    assert alert_payload["alert"] is None
    assert alert_payload["error"]["code"] == "missing_alert_id"
    assert "cannot construct stable alert_id" in alert_payload["error"]["message"]


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
    assert diagnostics_payload["run"]["run_persisted"] is True
    assert alert_payload["alert"]["severity"] == "warn"


def test_load_monitor_cli_skips_alert_out_for_error_payload(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_cli_alert_error.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260317_daily_refresh_feature_v1_scoring_rules_v1_history",
        run_finished_at=fixed_now - timedelta(days=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_cli_alert_error",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=160.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )

    monkeypatch.setattr(
        cli,
        "build_alert_payload_from_diagnostics",
        lambda _payload: {
            "run": {"run_type": "load_monitoring"},
            "alert": None,
            "impacted_signals": [],
            "operator_actions": [],
            "error": {
                "code": "missing_alert_id",
                "message": "cannot construct stable alert_id",
            },
        },
    )
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    diagnostics_out = tmp_path / "load_monitor_diagnostics_error_alert.json"
    alert_out = tmp_path / "load_monitor_alert_error.json"
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
    assert diagnostics_out.exists()
    assert not alert_out.exists()


def test_load_monitor_cli_removes_stale_alert_out_when_no_alert(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_cli_stale_alert.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_cli_stale_alert",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=90.0,
        review_queue_rows=_queue_rows(high=1, medium=1, low=1, unreviewed=1),
        reviewed_case_count=1,
        threshold_candidate_count=0,
        exclusion_candidate_count=0,
    )

    monkeypatch.setattr(cli, "build_alert_payload_from_diagnostics", lambda _p: None)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    diagnostics_out = tmp_path / "load_monitor_diagnostics_stale_alert.json"
    alert_out = tmp_path / "load_monitor_alert_stale.json"
    alert_out.write_text('{"stale": true}\n', encoding="utf-8")

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
    assert diagnostics_out.exists()
    assert not alert_out.exists()


def test_load_monitoring_resolves_subject_by_run_id_and_reports_not_found(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_subject_run_id.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260315_daily_refresh_feature_v1_scoring_rules_v1",
        run_finished_at=fixed_now - timedelta(days=3),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_run_id = "20260318_daily_refresh_feature_v1_scoring_rules_v1_subject_run_id"
    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id=subject_run_id,
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )

    with session_scope(database_url) as session:
        resolved_payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_run_id=subject_run_id,
        )
        missing_payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_run_id="missing-run-id",
        )

    assert resolved_payload["run"]["status"] == "succeeded"
    assert resolved_payload["subject"]["run_id"] == subject_run_id
    assert missing_payload["run"]["status"] == "failed"
    assert "Unable to resolve subject run_id 'missing-run-id'" in (
        missing_payload["error"]["message"]
    )


def test_load_monitoring_resolves_latest_pointer_and_reports_missing_pointer(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_latest_pointer.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    subject_run_id = "20260318_daily_refresh_feature_v1_scoring_rules_v1_latest"
    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id=subject_run_id,
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    latest_pointer = (
        artifact_base_dir
        / "latest"
        / "daily_refresh"
        / "feature_v1"
        / "scoring_rules_v1"
        / "latest_run.json"
    )
    latest_pointer.parent.mkdir(parents=True, exist_ok=True)
    latest_pointer.write_text(
        json.dumps({"root_path": str(subject_path.parent.parent)}, indent=2),
        encoding="utf-8",
    )

    with session_scope(database_url) as session:
        resolved_payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
        )

    missing_base_dir = tmp_path / "refresh_runs_missing_pointer"
    with session_scope(database_url) as session:
        missing_payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=missing_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
        )

    assert resolved_payload["run"]["status"] == "succeeded"
    assert resolved_payload["subject"]["run_id"] == subject_run_id
    assert missing_payload["run"]["status"] == "failed"
    assert "Unable to resolve latest pointer for profile 'daily_refresh'" in (
        missing_payload["error"]["message"]
    )


def test_load_monitoring_rejects_unsafe_path_components(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_invalid_components.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    invalid_cases = (
        (
            "profile_name",
            {
                "profile_name": "daily_refresh/*",
                "feature_version": "feature_v1",
                "ruleset_version": "scoring_rules_v1",
            },
        ),
        (
            "feature_version",
            {
                "profile_name": "daily_refresh",
                "feature_version": "../feature_v1",
                "ruleset_version": "scoring_rules_v1",
            },
        ),
        (
            "ruleset_version",
            {
                "profile_name": "daily_refresh",
                "feature_version": "feature_v1",
                "ruleset_version": "scoring_rules_v1/../x",
            },
        ),
        (
            "subject_run_id",
            {
                "profile_name": "daily_refresh",
                "feature_version": "feature_v1",
                "ruleset_version": "scoring_rules_v1",
                "subject_run_id": "../subject",
            },
        ),
    )

    with session_scope(database_url) as session:
        for field_name, kwargs in invalid_cases:
            payload = load_monitoring.build_load_diagnostics(
                session,
                artifact_base_dir=artifact_base_dir,
                **kwargs,
            )
            assert payload["run"]["status"] == "failed"
            assert f"Invalid {field_name}" in payload["error"]["message"]


def test_load_monitoring_rejects_unsafe_profile_name_from_subject_payload(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_invalid_subject_profile.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_bad_profile",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_payload = json.loads(subject_path.read_text(encoding="utf-8"))
    subject_payload["run"]["profile_name"] = "../unsafe_profile"
    subject_payload["request"]["profile_name"] = "../unsafe_profile"
    subject_path.write_text(json.dumps(subject_payload, indent=2), encoding="utf-8")

    with session_scope(database_url) as session:
        payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_path,
        )

    assert payload["run"]["status"] == "failed"
    assert "Invalid profile_name '../unsafe_profile'" in payload["error"]["message"]


def test_load_monitoring_rejects_latest_pointer_root_path_outside_artifact_base(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_latest_pointer_escape.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    latest_pointer = (
        artifact_base_dir
        / "latest"
        / "daily_refresh"
        / "feature_v1"
        / "scoring_rules_v1"
        / "latest_run.json"
    )
    latest_pointer.parent.mkdir(parents=True, exist_ok=True)
    latest_pointer.write_text(
        json.dumps({"root_path": str(tmp_path / "outside_root")}, indent=2),
        encoding="utf-8",
    )

    with session_scope(database_url) as session:
        payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
        )

    assert payload["run"]["status"] == "failed"
    assert "latest_run.json root_path" in payload["error"]["message"]
    assert "escapes artifact_base_dir" in payload["error"]["message"]


def test_load_monitoring_ignores_unsafe_artifacts_root_path_in_subject_payload(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_unsafe_artifacts_root.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260317_daily_refresh_feature_v1_scoring_rules_v1_history",
        run_finished_at=fixed_now - timedelta(days=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=1, medium=1, low=1, unreviewed=1),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_rows = _queue_rows(high=2, medium=1, low=1, unreviewed=2)
    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_unsafe_artifacts",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=subject_rows,
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )

    external_root = tmp_path / "external_run_root"
    external_analysis_dir = external_root / "analysis_artifacts"
    external_analysis_dir.mkdir(parents=True, exist_ok=True)
    (external_analysis_dir / "review_queue.json").write_text(
        json.dumps({"summary": {"returned_count": 999}, "rows": []}, indent=2),
        encoding="utf-8",
    )
    (external_analysis_dir / "review_feedback.json").write_text(
        json.dumps(
            {
                "summary": {"reviewed_case_count": 999},
                "recommendations": {
                    "threshold_tuning_candidates": [{"reason_code": "R"}] * 999,
                    "exclusion_tuning_candidates": [],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    subject_payload = json.loads(subject_path.read_text(encoding="utf-8"))
    subject_payload["artifacts"]["root_path"] = str(external_root)
    subject_path.write_text(json.dumps(subject_payload, indent=2), encoding="utf-8")

    with session_scope(database_url) as session:
        payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_path,
        )

    assert payload["run"]["status"] == "succeeded"
    queue_volume_signal = _signal_by_metric(
        payload, "volume.review_queue.returned_count"
    )
    assert queue_volume_signal["subject_value"] == float(len(subject_rows))
    source_artifacts = payload["diagnostics"]["source_artifacts"]
    assert all(str(external_root) not in artifact for artifact in source_artifacts)


def test_load_monitoring_rejects_subject_refresh_payload_outside_artifact_base(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_subject_payload_outside_base.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    external_artifact_dir = tmp_path / "external_refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    subject_path = _write_refresh_run(
        artifact_base_dir=external_artifact_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_external_subject",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
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

    assert payload["run"]["status"] == "failed"
    assert "subject_refresh_payload_path" in payload["error"]["message"]
    assert "escapes artifact_base_dir" in payload["error"]["message"]


def test_load_monitoring_failure_payload_carries_warnings_and_source_artifacts(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_failure_context.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_failure_context",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    corrupt_history_path = (
        artifact_base_dir
        / "20260317"
        / "daily_refresh"
        / "corrupt_history"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    corrupt_history_path.parent.mkdir(parents=True, exist_ok=True)
    corrupt_history_path.write_text("{invalid json", encoding="utf-8")

    def _raise_signal_error(**_: object) -> dict[str, object]:
        raise RuntimeError("forced signal failure")

    monkeypatch.setattr(load_monitoring, "_build_signals", _raise_signal_error)

    with session_scope(database_url) as session:
        payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_path,
        )

    assert payload["run"]["status"] == "failed"
    assert payload["error"]["message"] == "forced signal failure"
    assert any(
        warning.startswith("history_sample_skipped:")
        for warning in payload["diagnostics"]["warnings"]
    )
    assert str(subject_path) in payload["diagnostics"]["source_artifacts"]


def test_load_monitoring_baseline_uses_prior_runs_only_for_subject_run_id(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_prior_baseline.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260314_daily_refresh_feature_v1_scoring_rules_v1_prior",
        run_finished_at=fixed_now - timedelta(days=4),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_run_id = "20260315_daily_refresh_feature_v1_scoring_rules_v1_subject"
    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id=subject_run_id,
        run_finished_at=fixed_now - timedelta(days=3),
        duration_seconds=220.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260316_daily_refresh_feature_v1_scoring_rules_v1_future_a",
        run_finished_at=fixed_now - timedelta(days=2),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260317_daily_refresh_feature_v1_scoring_rules_v1_future_b",
        run_finished_at=fixed_now - timedelta(days=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
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
            subject_run_id=subject_run_id,
        )

    duration_signal = _signal_by_metric(payload, "duration.total_seconds")
    assert duration_signal["ignored"] is True
    assert duration_signal["ignore_reason"] == "insufficient_history"


def test_load_monitoring_filters_history_by_feature_and_ruleset_version(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_history_filter.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    for day_offset in (3, 2):
        _write_refresh_run(
            artifact_base_dir=artifact_base_dir,
            run_id=f"2026031{day_offset}_daily_refresh_feature_v1_scoring_rules_v1",
            run_finished_at=fixed_now - timedelta(days=day_offset),
            duration_seconds=100.0,
            review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
            reviewed_case_count=2,
            threshold_candidate_count=1,
            exclusion_candidate_count=0,
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
        )

    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260315_daily_refresh_feature_v2_scoring_rules_v2",
        run_finished_at=fixed_now - timedelta(days=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
        feature_version="feature_v2",
        ruleset_version="scoring_rules_v2",
    )

    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_filter_subject",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=220.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
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

    duration_signal = _signal_by_metric(payload, "duration.total_seconds")
    assert duration_signal["ignored"] is True
    assert duration_signal["ignore_reason"] == "insufficient_history"


def test_load_monitoring_records_warning_for_skipped_corrupt_history_sample(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_history_warning.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260315_daily_refresh_feature_v1_scoring_rules_v1",
        run_finished_at=fixed_now - timedelta(days=3),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_warning_subject",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )

    corrupt_history_path = (
        artifact_base_dir
        / "20260316"
        / "daily_refresh"
        / "20260316_daily_refresh_feature_v1_scoring_rules_v1_corrupt"
        / "health_summary"
        / "refresh_run_payload.json"
    )
    corrupt_history_path.parent.mkdir(parents=True, exist_ok=True)
    corrupt_history_path.write_text("{this is invalid json", encoding="utf-8")

    with session_scope(database_url) as session:
        payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_path,
        )

    warnings = payload["diagnostics"]["warnings"]
    assert any("history_sample_skipped:" in warning for warning in warnings)
    assert any(str(corrupt_history_path) in warning for warning in warnings)


def test_load_monitoring_records_warning_for_invalid_history_run_status(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_history_invalid_status.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260315_daily_refresh_feature_v1_scoring_rules_v1",
        run_finished_at=fixed_now - timedelta(days=3),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    subject_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_invalid_status_subject",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    invalid_history_path = _write_refresh_run(
        artifact_base_dir=artifact_base_dir,
        run_id="20260316_daily_refresh_feature_v1_scoring_rules_v1_invalid_status",
        run_finished_at=fixed_now - timedelta(days=2),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    invalid_payload = json.loads(invalid_history_path.read_text(encoding="utf-8"))
    invalid_payload["run"]["status"] = "partial_success"
    invalid_history_path.write_text(
        json.dumps(invalid_payload, indent=2), encoding="utf-8"
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

    warnings = payload["diagnostics"]["warnings"]
    assert any("history_sample_skipped:" in warning for warning in warnings)
    assert any(
        "invalid run.status='partial_success'" in warning for warning in warnings
    )


def test_load_monitoring_treats_missing_review_queue_artifact_as_missing_subject_value(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_missing_review_queue.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    for day_offset in (4, 3, 2):
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
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_missing_queue",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    (subject_path.parent.parent / "analysis_artifacts" / "review_queue.json").unlink()

    with session_scope(database_url) as session:
        payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_path,
        )

    for metric_key in (
        "volume.review_queue.returned_count",
        "queue_size.review_queue.high_risk_count",
        "queue_size.review_queue.unreviewed_count",
    ):
        signal = _signal_by_metric(payload, metric_key)
        assert signal["ignored"] is True
        assert signal["ignore_reason"] == "missing_subject_value"


def test_missing_review_feedback_artifact_sets_missing_subject_value(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "load_monitor_missing_review_feedback.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    artifact_base_dir = tmp_path / "refresh_runs"
    fixed_now = datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(load_monitoring, "_now_utc", lambda: fixed_now)

    for day_offset in (4, 3, 2):
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
        run_id="20260318_daily_refresh_feature_v1_scoring_rules_v1_missing_feedback",
        run_finished_at=fixed_now - timedelta(minutes=1),
        duration_seconds=100.0,
        review_queue_rows=_queue_rows(high=2, medium=2, low=2, unreviewed=2),
        reviewed_case_count=2,
        threshold_candidate_count=1,
        exclusion_candidate_count=0,
    )
    (
        subject_path.parent.parent / "analysis_artifacts" / "review_feedback.json"
    ).unlink()

    with session_scope(database_url) as session:
        payload = load_monitoring.build_load_diagnostics(
            session,
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            subject_refresh_payload_path=subject_path,
        )

    for metric_key in (
        "volume.review_feedback.reviewed_case_count",
        "volume.review_feedback.recommendation_count",
    ):
        signal = _signal_by_metric(payload, metric_key)
        assert signal["ignored"] is True
        assert signal["ignore_reason"] == "missing_subject_value"


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
    feature_version: str = "feature_v1",
    ruleset_version: str = "scoring_rules_v1",
) -> Path:
    run_date = run_finished_at.strftime("%Y%m%d")
    profile_name = "daily_refresh"
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


def test_default_run_id_includes_microseconds_for_uniqueness() -> None:
    base = datetime(2026, 3, 18, 12, 0, 1, 123456, tzinfo=timezone.utc)
    later = datetime(2026, 3, 18, 12, 0, 1, 654321, tzinfo=timezone.utc)
    run_id_a = load_monitoring._default_run_id(  # noqa: SLF001
        prefix="load_monitor",
        run_date="20260318",
        generated_at=base,
    )
    run_id_b = load_monitoring._default_run_id(  # noqa: SLF001
        prefix="load_monitor",
        run_date="20260318",
        generated_at=later,
    )

    assert run_id_a.endswith("_123456")
    assert run_id_b.endswith("_654321")
    assert run_id_a != run_id_b
