from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from accessdane_audit import cli, scheduler_integration
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
    for attempt in payload["attempts"]:
        assert attempt["started_at_utc"] is None
        assert attempt["finished_at_utc"] is None
        assert attempt["duration_seconds"] is None
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
    assert payload["incident"]["severity"] == "warn"


def test_scheduler_integration_refresh_overlap_failure_keeps_traceability(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"

    def _runner(**kwargs):
        run_id = str(kwargs["run_id"])
        root_path = (
            artifact_base_dir
            / str(kwargs["run_date"])
            / str(kwargs["profile_name"])
            / run_id
        )
        return {
            "run": {
                "status": "failed",
                "started_at": _iso(datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)),
                "finished_at": _iso(datetime(2026, 3, 26, 12, 1, tzinfo=timezone.utc)),
            },
            "error": {
                "code": "overlapping_run",
                "message": "lock overlap reported by refresh-runner",
                "failed_stage_id": None,
            },
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
        scheduler_run_id="sched_refresh_overlap",
        max_attempts=1,
        refresh_runner=_runner,
    )

    assert payload["result"]["status"] == "dead_lettered"
    assert payload["result"]["failure_code"] == "overlapping_run"
    assert len(payload["attempts"]) == 1
    attempt = payload["attempts"][0]
    assert attempt["state"] == "failed"
    assert attempt["refresh_run_id"] == "sched_refresh_overlap_a01"
    assert attempt["started_at_utc"] == "2026-03-26T12:00:00Z"
    assert attempt["finished_at_utc"] == "2026-03-26T12:01:00Z"
    assert attempt["duration_seconds"] == 60
    assert payload["scheduler_run"]["refresh_run_id"] == "sched_refresh_overlap_a01"


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("scheduler_run_id", "../unsafe_run"),
        ("profile_name", "daily/refresh"),
        ("feature_version", "feature/v1"),
        ("ruleset_version", "ruleset/v1"),
    ],
)
def test_scheduler_integration_rejects_unsafe_path_segments(
    tmp_path: Path,
    field_name: str,
    value: str,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"

    kwargs: dict[str, object] = {
        "trigger_type": "scheduled",
        "profile_name": "daily_refresh",
        "feature_version": "feature_v1",
        "ruleset_version": "scoring_rules_v1",
        "sales_ratio_base": "sales_ratio_v1",
        "top": 10,
        "retr_file": None,
        "permits_file": None,
        "appeals_file": None,
        "artifact_base_dir": artifact_base_dir,
        "refresh_log_dir": refresh_log_dir,
        "accessdane_bin": "accessdane",
        "scheduler_run_id": "sched_safe",
    }
    kwargs[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        run_managed_scheduler_execution(**kwargs)
    assert not refresh_log_dir.exists()


def test_scheduler_integration_dispatch_error_attempt_uses_run_level_timing(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"

    timeline = iter(
        (
            datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 26, 12, 0, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 26, 12, 0, 2, tzinfo=timezone.utc),
            datetime(2026, 3, 26, 12, 0, 9, tzinfo=timezone.utc),
            datetime(2026, 3, 26, 12, 0, 12, tzinfo=timezone.utc),
            datetime(2026, 3, 26, 12, 0, 13, tzinfo=timezone.utc),
            datetime(2026, 3, 26, 12, 0, 14, tzinfo=timezone.utc),
            datetime(2026, 3, 26, 12, 0, 15, tzinfo=timezone.utc),
        )
    )

    def _now() -> datetime:
        return next(timeline)

    def _runner(**kwargs):
        raise RuntimeError("boom")

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
        scheduler_run_id="sched_dispatch_error",
        max_attempts=1,
        now_fn=_now,
        refresh_runner=_runner,
    )

    assert payload["result"]["status"] == "dead_lettered"
    assert len(payload["attempts"]) == 1
    attempt = payload["attempts"][0]
    assert attempt["state"] == "dispatch_error"
    assert attempt["started_at_utc"] is None
    assert attempt["finished_at_utc"] is None
    assert attempt["duration_seconds"] is None
    assert payload["scheduler_run"]["refresh_run_id"] is None
    assert payload["result"]["last_attempt_finished_at_utc"] == "2026-03-26T12:00:12Z"


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


def test_scheduler_runner_cli_rejects_naive_scheduled_for_utc(
    monkeypatch,
) -> None:
    monkeypatch.delenv("ACCESSDANE_ENVIRONMENT", raising=False)
    monkeypatch.delenv("environment_name", raising=False)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "scheduler-runner",
            "--scheduled-for-utc",
            "2026-03-26T12:00:00",
        ],
    )

    assert result.exit_code != 0
    exception_text = str(result.exception) if result.exception is not None else ""
    output_text = result.stdout if result.stdout else result.output
    assert (
        "--scheduled-for-utc must include an explicit UTC offset or 'Z' designator."
        in exception_text
        or "--scheduled-for-utc" in output_text
    )


def test_scheduler_runner_cli_expands_user_in_artifact_base_dir(
    tmp_path: Path, monkeypatch
) -> None:
    output_path = tmp_path / "scheduler_payload_tilde.json"
    captured: dict[str, object] = {}

    payload = {
        "scheduler_run": {
            "scheduler_run_id": "sched_tilde",
            "state": "succeeded",
            "profile_name": "daily_refresh",
            "run_date": "20260326",
            "refresh_run_id": "sched_tilde_a01",
            "attempt_count": 1,
            "max_attempts": 3,
            "created_at_utc": "2026-03-26T12:00:00Z",
            "updated_at_utc": "2026-03-26T12:00:01Z",
        },
        "trigger": {
            "trigger_id": "trigger_sched_tilde",
            "trigger_type": "scheduled",
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
    monkeypatch.setenv("HOME", str(tmp_path))

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "scheduler-runner",
            "--artifact-base-dir",
            "~/refresh_runs",
            "--refresh-log-dir",
            "~/refresh_runs/scheduler_logs",
            "--out",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert output_path.exists()
    assert captured["artifact_base_dir"] == (tmp_path / "refresh_runs").resolve()
    assert (
        captured["refresh_log_dir"]
        == (tmp_path / "refresh_runs" / "scheduler_logs").resolve()
    )


def test_scheduler_integration_transitions_to_running_before_dispatch(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"
    observed: dict[str, object] = {}

    def _runner(**kwargs):
        payload_path = refresh_log_dir / "sched_running_state.json"
        persisted = json.loads(payload_path.read_text(encoding="utf-8"))
        observed["state"] = persisted["scheduler_run"]["state"]
        observed["refresh_run_id"] = persisted["scheduler_run"]["refresh_run_id"]
        run_id = str(kwargs["run_id"])
        root_path = (
            artifact_base_dir
            / str(kwargs["run_date"])
            / str(kwargs["profile_name"])
            / run_id
        )
        return {
            "run": {
                "status": "succeeded",
                "started_at": _iso(datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)),
                "finished_at": _iso(datetime(2026, 3, 26, 12, 1, tzinfo=timezone.utc)),
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
        scheduler_run_id="sched_running_state",
        max_attempts=1,
        refresh_runner=_runner,
    )

    assert payload["scheduler_run"]["state"] == "succeeded"
    assert observed["state"] == "running"
    assert observed["refresh_run_id"] is None


def test_scheduler_integration_rejects_naive_scheduled_for_utc(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"

    with pytest.raises(ValueError, match="timezone-aware"):
        run_managed_scheduler_execution(
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
            scheduler_run_id="sched_naive_time",
            scheduled_for_utc=datetime(2026, 3, 26, 12, 0, 0),
        )
    assert not refresh_log_dir.exists()


def test_scheduler_integration_rejects_refresh_log_dir_outside_artifact_base(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = tmp_path / "outside_logs"

    with pytest.raises(
        ValueError, match="refresh_log_dir must be within artifact_base_dir"
    ):
        run_managed_scheduler_execution(
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
            scheduler_run_id="sched_outside_logs",
        )
    assert not refresh_log_dir.exists()


def test_scheduler_integration_atomic_json_writes_leave_no_tmp_files(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"

    def _runner(**kwargs):
        run_id = str(kwargs["run_id"])
        root_path = (
            artifact_base_dir
            / str(kwargs["run_date"])
            / str(kwargs["profile_name"])
            / run_id
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
        scheduler_run_id="sched_atomic_dead_letter",
        max_attempts=1,
        refresh_runner=_runner,
    )

    dead_letter_path = Path(payload["result"]["dead_letter_path"] or "")
    assert dead_letter_path.exists()
    assert not list(refresh_log_dir.glob("*.tmp"))
    assert not list(dead_letter_path.parent.glob("*.tmp"))


def test_scheduler_integration_persists_failed_pending_dead_letter_state(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    refresh_log_dir = artifact_base_dir / "scheduler_logs"
    payload_path = refresh_log_dir / "sched_failed_pending_snapshot.json"
    persisted_states: list[tuple[str, str]] = []

    original_write_json_atomic = scheduler_integration._write_json_atomic

    def _capturing_write(path: Path, payload: object) -> None:
        if path == payload_path:
            run_payload = payload
            assert isinstance(run_payload, dict)
            scheduler_run = run_payload.get("scheduler_run", {})
            result = run_payload.get("result", {})
            if isinstance(scheduler_run, dict) and isinstance(result, dict):
                state = scheduler_run.get("state")
                status = result.get("status")
                if isinstance(state, str) and isinstance(status, str):
                    persisted_states.append((state, status))
        original_write_json_atomic(path, payload)

    monkeypatch.setattr(scheduler_integration, "_write_json_atomic", _capturing_write)

    def _runner(**kwargs):
        run_id = str(kwargs["run_id"])
        root_path = (
            artifact_base_dir
            / str(kwargs["run_date"])
            / str(kwargs["profile_name"])
            / run_id
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
        scheduler_run_id="sched_failed_pending_snapshot",
        max_attempts=1,
        refresh_runner=_runner,
    )

    assert payload["scheduler_run"]["state"] == "dead_lettered"
    assert ("failed_pending_dead_letter", "pending") in persisted_states
    assert ("dead_lettered", "dead_lettered") in persisted_states
    failed_pending_index = persisted_states.index(
        ("failed_pending_dead_letter", "pending")
    )
    dead_lettered_index = persisted_states.index(("dead_lettered", "dead_lettered"))
    assert failed_pending_index < dead_lettered_index
