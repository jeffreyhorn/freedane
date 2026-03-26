from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.scheduler_integration import run_managed_scheduler_execution


def _iso(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def test_scheduler_integration_retries_retryable_failure_then_succeeds(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"
    calls: list[dict[str, object]] = []

    def _runner(**kwargs):
        calls.append(kwargs)
        run_id = str(kwargs["run_id"])
        root_path = (
            artifact_base_dir / kwargs["run_date"] / kwargs["profile_name"] / run_id
        )
        if len(calls) == 1:
            return {
                "run": {
                    "status": "failed",
                    "started_at": _iso(
                        datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)
                    ),
                    "finished_at": _iso(
                        datetime(2026, 3, 26, 12, 1, tzinfo=timezone.utc)
                    ),
                },
                "error": {
                    "code": "stage_failure",
                    "message": "stage failed",
                    "failed_stage_id": "build_context",
                },
                "artifacts": {"root_path": str(root_path)},
            }
        return {
            "run": {
                "status": "succeeded",
                "started_at": _iso(datetime(2026, 3, 26, 12, 2, tzinfo=timezone.utc)),
                "finished_at": _iso(datetime(2026, 3, 26, 12, 3, tzinfo=timezone.utc)),
            },
            "error": None,
            "artifacts": {"root_path": str(root_path)},
        }

    payload = run_managed_scheduler_execution(
        trigger_type="scheduled",
        profile_name="daily_refresh",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=10,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        refresh_log_dir=refresh_log_dir,
        accessdane_bin="accessdane",
        scheduler_run_id="sched_retry_success",
        max_attempts=3,
        refresh_runner=_runner,
        backoff_base_seconds=0,
        backoff_cap_seconds=0,
        backoff_jitter_seconds=0,
    )

    assert payload["scheduler_run"]["state"] == "succeeded"
    assert payload["result"]["status"] == "succeeded"
    assert payload["scheduler_run"]["attempt_count"] == 2
    assert [attempt["state"] for attempt in payload["attempts"]] == [
        "failed",
        "succeeded",
    ]
    assert len(calls) == 2
    persisted = refresh_log_dir / "sched_retry_success.json"
    assert persisted.exists()
    persisted_payload = json.loads(persisted.read_text(encoding="utf-8"))
    assert persisted_payload["result"]["status"] == "succeeded"


def test_scheduler_integration_overlap_exhaustion_routes_to_dead_letter(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"
    lock_path = artifact_base_dir / "locks" / "daily_refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("existing lock\n", encoding="utf-8")
    call_count = 0

    def _runner(**kwargs):
        nonlocal call_count
        call_count += 1
        return kwargs

    payload = run_managed_scheduler_execution(
        trigger_type="scheduled",
        profile_name="daily_refresh",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=10,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        refresh_log_dir=refresh_log_dir,
        accessdane_bin="accessdane",
        scheduler_run_id="sched_overlap_dead_letter",
        max_attempts=2,
        refresh_runner=_runner,
        backoff_base_seconds=0,
        backoff_cap_seconds=0,
        backoff_jitter_seconds=0,
    )

    assert call_count == 0
    assert payload["scheduler_run"]["state"] == "dead_lettered"
    assert payload["result"]["status"] == "dead_lettered"
    assert payload["result"]["failure_code"] == "overlapping_run"
    assert payload["result"]["failure_class"] == "exhausted_retries"
    assert [attempt["state"] for attempt in payload["attempts"]] == [
        "overlap_blocked",
        "overlap_blocked",
    ]
    dead_letter_path = Path(payload["result"]["dead_letter_path"] or "")
    assert dead_letter_path.exists()


def test_scheduler_integration_non_retryable_failure_dead_letters_without_retry(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"
    call_count = 0

    def _runner(**kwargs):
        nonlocal call_count
        call_count += 1
        run_id = str(kwargs["run_id"])
        root_path = (
            artifact_base_dir / kwargs["run_date"] / kwargs["profile_name"] / run_id
        )
        return {
            "run": {
                "status": "failed",
                "started_at": _iso(datetime(2026, 3, 26, 12, 5, tzinfo=timezone.utc)),
                "finished_at": _iso(datetime(2026, 3, 26, 12, 6, tzinfo=timezone.utc)),
            },
            "error": {
                "code": "invalid_run_context",
                "message": "invalid context",
                "failed_stage_id": None,
            },
            "artifacts": {"root_path": str(root_path)},
        }

    payload = run_managed_scheduler_execution(
        trigger_type="manual_retry",
        profile_name="daily_refresh",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=10,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        refresh_log_dir=refresh_log_dir,
        accessdane_bin="accessdane",
        scheduler_run_id="sched_non_retryable_dead_letter",
        max_attempts=3,
        refresh_runner=_runner,
    )

    assert call_count == 1
    assert payload["scheduler_run"]["state"] == "dead_lettered"
    assert payload["result"]["status"] == "dead_lettered"
    assert payload["result"]["failure_class"] == "non_retryable"
    assert len(payload["attempts"]) == 1
    assert payload["attempts"][0]["state"] == "failed"


def test_scheduler_runner_cli_writes_output_and_uses_trigger_option(
    tmp_path: Path, monkeypatch
) -> None:
    output_path = tmp_path / "scheduler_payload.json"
    captured: dict[str, object] = {}

    payload = {
        "scheduler_run": {
            "scheduler_run_id": "sched_cli",
            "state": "succeeded",
            "profile_name": "daily_refresh",
            "run_date": "20260326",
            "refresh_run_id": "sched_cli_a01",
            "attempt_count": 1,
            "max_attempts": 3,
            "created_at_utc": "2026-03-26T12:00:00Z",
            "updated_at_utc": "2026-03-26T12:00:01Z",
        },
        "trigger": {
            "trigger_id": "trigger_sched_cli",
            "trigger_type": "catch_up",
            "profile_name": "daily_refresh",
            "scheduled_for_utc": "2026-03-26T12:00:00Z",
            "created_at_utc": "2026-03-26T12:00:00Z",
            "requested_by": "system",
            "run_context": {},
        },
        "attempts": [],
        "result": {
            "status": "succeeded",
            "failure_class": None,
            "failure_code": None,
            "failure_message": None,
            "failed_stage_id": None,
            "attempt_count": 1,
            "max_attempts": 3,
            "last_attempt_finished_at_utc": "2026-03-26T12:00:01Z",
            "dead_letter_path": None,
            "recommended_operator_action_summary": None,
        },
        "incident": None,
    }

    def _fake_scheduler(**kwargs):
        captured.update(kwargs)
        return payload

    monkeypatch.setattr(cli, "run_managed_scheduler_execution", _fake_scheduler)
    monkeypatch.delenv("ACCESSDANE_ENVIRONMENT", raising=False)
    monkeypatch.delenv("environment_name", raising=False)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "scheduler-runner",
            "--trigger-type",
            "catch_up",
            "--scheduled-for-utc",
            "2026-03-26T12:00:00Z",
            "--out",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert output_path.exists()
    persisted = json.loads(output_path.read_text(encoding="utf-8"))
    assert persisted["scheduler_run"]["scheduler_run_id"] == "sched_cli"
    assert captured["trigger_type"] == "catch_up"
