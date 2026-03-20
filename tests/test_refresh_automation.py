from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from accessdane_audit import cli, refresh_automation
from accessdane_audit.refresh_automation import run_scheduled_refresh


def test_run_scheduled_refresh_daily_profile_executes_deterministic_command_order(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    retr_file = tmp_path / "retr.csv"
    permits_file = tmp_path / "permits.csv"
    appeals_file = tmp_path / "appeals.csv"
    retr_file.write_text("header\n", encoding="utf-8")
    permits_file.write_text("header\n", encoding="utf-8")
    appeals_file.write_text("header\n", encoding="utf-8")

    executed_commands: list[list[str]] = []

    def _executor(command: list[str]) -> int:
        executed_commands.append(command)
        return 0

    payload = run_scheduled_refresh(
        profile_name="daily_refresh",
        run_date="20260316",
        run_id="20260316_daily_refresh_feature_v1_scoring_rules_v1_010101",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=25,
        retr_file=retr_file,
        permits_file=permits_file,
        appeals_file=appeals_file,
        artifact_base_dir=artifact_base_dir,
        accessdane_bin="accessdane",
        command_executor=_executor,
    )

    assert payload["run"]["status"] == "succeeded"
    assert [stage["stage_id"] for stage in payload["stages"]] == [
        "ingest_context",
        "build_context",
        "score_pipeline",
        "analysis_artifacts",
        "investigation_artifacts",
        "health_summary",
    ]
    assert [stage["status"] for stage in payload["stages"]] == [
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
    ]
    assert [command[1] for command in executed_commands] == [
        "ingest-retr",
        "match-sales",
        "ingest-permits",
        "ingest-appeals",
        "build-parcel-year-facts",
        "sales-ratio-study",
        "build-features",
        "score-fraud",
        "review-queue",
        "review-feedback",
        "investigation-report",
    ]
    assert payload["artifacts"]["latest_pointer_path"] == str(
        artifact_base_dir
        / "latest"
        / "daily_refresh"
        / "feature_v1"
        / "scoring_rules_v1"
    )
    assert payload["run"]["run_persisted"] is True
    root_path = Path(payload["artifacts"]["root_path"])
    refresh_payload_path = root_path / "health_summary" / "refresh_run_payload.json"
    assert refresh_payload_path.exists()
    assert (root_path / "run_manifest.json").exists()
    persisted_payload = json.loads(refresh_payload_path.read_text(encoding="utf-8"))
    assert persisted_payload["run"]["run_persisted"] is True
    health_stage_artifacts = payload["artifacts"]["stage_artifacts"]["health_summary"]
    assert str(refresh_payload_path) in health_stage_artifacts
    latest_run = (
        artifact_base_dir
        / "latest"
        / "daily_refresh"
        / "feature_v1"
        / "scoring_rules_v1"
        / "latest_run.json"
    )
    assert latest_run.exists()
    latest_payload = json.loads(latest_run.read_text(encoding="utf-8"))
    assert latest_payload["run_id"] == payload["run"]["run_id"]
    assert latest_payload["root_path"] == payload["artifacts"]["root_path"]
    assert not (artifact_base_dir / "locks" / "daily_refresh.lock").exists()


def test_run_scheduled_refresh_blocks_downstream_stages_after_failure(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    executed_commands: list[list[str]] = []

    def _executor(command: list[str]) -> int:
        executed_commands.append(command)
        if command[1] == "build-parcel-year-facts":
            return 9
        return 0

    payload = run_scheduled_refresh(
        profile_name="daily_refresh",
        run_date="20260316",
        run_id="20260316_daily_refresh_feature_v1_scoring_rules_v1_020202",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=10,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        accessdane_bin="accessdane",
        command_executor=_executor,
    )

    stage_status = {stage["stage_id"]: stage["status"] for stage in payload["stages"]}
    assert payload["run"]["status"] == "failed"
    assert payload["error"] is not None
    assert payload["error"]["failed_stage_id"] == "build_context"
    assert stage_status == {
        "ingest_context": "succeeded",
        "build_context": "failed",
        "score_pipeline": "blocked",
        "analysis_artifacts": "blocked",
        "investigation_artifacts": "blocked",
        "health_summary": "succeeded",
    }
    assert [command[1] for command in executed_commands] == ["build-parcel-year-facts"]
    assert payload["run"]["run_persisted"] is True
    root_path = Path(payload["artifacts"]["root_path"])
    refresh_payload_path = root_path / "health_summary" / "refresh_run_payload.json"
    persisted_payload = json.loads(refresh_payload_path.read_text(encoding="utf-8"))
    assert persisted_payload["run"]["run_persisted"] is True
    failure_artifact = root_path / "health_summary" / "failure_artifact.json"
    assert failure_artifact.exists()
    failure_payload = json.loads(failure_artifact.read_text(encoding="utf-8"))
    assert failure_payload["code"] == "stage_failure"
    health_stage_artifacts = payload["artifacts"]["stage_artifacts"]["health_summary"]
    assert str(refresh_payload_path) in health_stage_artifacts
    assert str(failure_artifact) in health_stage_artifacts


def test_run_scheduled_refresh_analysis_only_profile_skips_upstream_stages(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    executed_commands: list[list[str]] = []

    def _executor(command: list[str]) -> int:
        executed_commands.append(command)
        return 0

    payload = run_scheduled_refresh(
        profile_name="analysis_only",
        run_date="20260316",
        run_id="20260316_analysis_only_feature_v1_scoring_rules_v1_030303",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=15,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        accessdane_bin="accessdane",
        command_executor=_executor,
    )

    stage_status = {stage["stage_id"]: stage["status"] for stage in payload["stages"]}
    assert payload["run"]["status"] == "succeeded"
    assert stage_status == {
        "ingest_context": "skipped",
        "build_context": "skipped",
        "score_pipeline": "skipped",
        "analysis_artifacts": "succeeded",
        "investigation_artifacts": "succeeded",
        "health_summary": "succeeded",
    }
    assert [command[1] for command in executed_commands] == [
        "review-queue",
        "review-feedback",
        "investigation-report",
    ]
    assert payload["diagnostics"]["skip_reasons"] == [
        {
            "stage_id": "ingest_context",
            "command_id": "_stage_profile_selection",
            "reason": "profile_skip:analysis_only",
        },
        {
            "stage_id": "build_context",
            "command_id": "_stage_profile_selection",
            "reason": "profile_skip:analysis_only",
        },
        {
            "stage_id": "score_pipeline",
            "command_id": "_stage_profile_selection",
            "reason": "profile_skip:analysis_only",
        },
    ]


def test_run_scheduled_refresh_retry_run_reexecutes_from_stage_boundary_only(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    executed_commands: list[list[str]] = []

    def _executor(command: list[str]) -> int:
        executed_commands.append(command)
        return 0

    payload = run_scheduled_refresh(
        profile_name="daily_refresh",
        run_date="20260316",
        run_id="20260316_daily_refresh_feature_v1_scoring_rules_v1_040404",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=20,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        accessdane_bin="accessdane",
        attempt_count=2,
        retried_from_stage_id="score_pipeline",
        command_executor=_executor,
    )

    stage_status = {stage["stage_id"]: stage["status"] for stage in payload["stages"]}
    stage_attempts = {
        stage["stage_id"]: stage["attempt"] for stage in payload["stages"]
    }

    assert payload["run"]["status"] == "succeeded"
    assert stage_status == {
        "ingest_context": "skipped",
        "build_context": "skipped",
        "score_pipeline": "succeeded",
        "analysis_artifacts": "succeeded",
        "investigation_artifacts": "succeeded",
        "health_summary": "succeeded",
    }
    assert stage_attempts == {
        "ingest_context": 1,
        "build_context": 1,
        "score_pipeline": 2,
        "analysis_artifacts": 2,
        "investigation_artifacts": 2,
        "health_summary": 2,
    }
    assert [command[1] for command in executed_commands] == [
        "sales-ratio-study",
        "build-features",
        "score-fraud",
        "review-queue",
        "review-feedback",
        "investigation-report",
    ]
    assert payload["diagnostics"]["retry"] == {
        "attempt_count": 2,
        "retried_from_stage_id": "score_pipeline",
    }
    assert {
        (item["stage_id"], item["command_id"], item["reason"])
        for item in payload["diagnostics"]["skip_reasons"]
    } == {
        (
            "ingest_context",
            "_stage_retry_boundary",
            "retry_boundary_before:score_pipeline",
        ),
        (
            "build_context",
            "_stage_retry_boundary",
            "retry_boundary_before:score_pipeline",
        ),
    }


def test_run_scheduled_refresh_rejects_unsupported_profile() -> None:
    payload = run_scheduled_refresh(
        profile_name="county_refresh",
        run_date="20260316",
        run_id="20260316_county_refresh_feature_v1_scoring_rules_v1_050505",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=10,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=Path("data/refresh_runs"),
        accessdane_bin="accessdane",
    )

    assert payload["run"]["status"] == "failed"
    assert payload["error"] == {
        "code": "unsupported_profile",
        "message": "Unsupported profile_name 'county_refresh' for v1 runner.",
        "failed_stage_id": None,
    }
    assert [stage["status"] for stage in payload["stages"]] == [
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
    ]


def test_run_scheduled_refresh_annual_profile_emits_signoff_and_checklist_artifacts(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    retr_file = tmp_path / "retr.csv"
    assessment_manifest_file = tmp_path / "assessment_manifest.json"
    retr_file.write_text("header\n", encoding="utf-8")
    assessment_manifest_file.write_text(
        '{"files": ["roll_2026.csv"]}\n', encoding="utf-8"
    )
    executed_commands: list[list[str]] = []

    def _executor(command: list[str]) -> int:
        executed_commands.append(command)
        return 0

    payload = run_scheduled_refresh(
        profile_name="annual_refresh",
        run_date="20260316",
        run_id="20260316_annual_refresh_feature_v1_scoring_rules_v1_101010",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=25,
        retr_file=retr_file,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        accessdane_bin="accessdane",
        assessment_manifest_file=assessment_manifest_file,
        annual_target_year=2026,
        command_executor=_executor,
    )

    assert payload["run"]["status"] == "succeeded"
    assert [command[1] for command in executed_commands] == [
        "ingest-retr",
        "match-sales",
        "build-parcel-year-facts",
        "sales-ratio-study",
        "build-features",
        "score-fraud",
        "review-queue",
        "review-feedback",
        "investigation-report",
    ]
    annual_artifacts = payload["artifacts"]["stage_artifacts"]["annual_signoff"]
    root_path = Path(payload["artifacts"]["root_path"])
    checklist_path = root_path / "annual_signoff" / "annual_stage_checklist.json"
    signoff_path = root_path / "annual_signoff" / "annual_signoff.json"
    assert checklist_path.exists()
    assert signoff_path.exists()
    assert str(checklist_path) in annual_artifacts
    assert str(signoff_path) in annual_artifacts
    checklist_payload = json.loads(checklist_path.read_text(encoding="utf-8"))
    assert checklist_payload["expected_stage_order"] == [
        "ingest_context",
        "build_context",
        "score_pipeline",
        "analysis_artifacts",
        "investigation_artifacts",
        "health_summary",
    ]
    assert checklist_payload["blocking_failures"] == []
    signoff_payload = json.loads(signoff_path.read_text(encoding="utf-8"))
    assert signoff_payload["run"]["status"] == "pending_signoff"
    assert signoff_payload["annual_context"]["annual_target_year"] == 2026


def test_run_scheduled_refresh_annual_profile_requires_retr_file_preflight(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    assessment_manifest_file = tmp_path / "assessment_manifest.json"
    assessment_manifest_file.write_text(
        '{"files": ["roll_2026.csv"]}\n', encoding="utf-8"
    )
    executed_commands: list[list[str]] = []

    def _executor(command: list[str]) -> int:
        executed_commands.append(command)
        return 0

    payload = run_scheduled_refresh(
        profile_name="annual_refresh",
        run_date="20260316",
        run_id="20260316_annual_refresh_feature_v1_scoring_rules_v1_111111",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=10,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        accessdane_bin="accessdane",
        assessment_manifest_file=assessment_manifest_file,
        annual_target_year=2026,
        command_executor=_executor,
    )

    assert payload["run"]["status"] == "failed"
    assert payload["run"]["run_persisted"] is False
    assert payload["error"] is not None
    assert payload["error"]["code"] == "annual_preflight_failed"
    assert "CP-01_SOURCE_MANIFEST" in payload["error"]["message"]
    assert executed_commands == []
    assert [stage["status"] for stage in payload["stages"]] == [
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
    ]


def test_run_scheduled_refresh_annual_retry_reexecutes_from_stage_boundary_only(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    retr_file = tmp_path / "retr.csv"
    assessment_manifest_file = tmp_path / "assessment_manifest.json"
    retr_file.write_text("header\n", encoding="utf-8")
    assessment_manifest_file.write_text(
        '{"files": ["roll_2026.csv"]}\n', encoding="utf-8"
    )
    executed_commands: list[list[str]] = []

    def _executor(command: list[str]) -> int:
        executed_commands.append(command)
        return 0

    payload = run_scheduled_refresh(
        profile_name="annual_refresh",
        run_date="20260316",
        run_id="20260316_annual_refresh_feature_v1_scoring_rules_v1_121212",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=10,
        retr_file=retr_file,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        accessdane_bin="accessdane",
        assessment_manifest_file=assessment_manifest_file,
        annual_target_year=2026,
        attempt_count=2,
        retried_from_stage_id="score_pipeline",
        command_executor=_executor,
    )

    stage_status = {stage["stage_id"]: stage["status"] for stage in payload["stages"]}
    assert payload["run"]["status"] == "succeeded"
    assert stage_status == {
        "ingest_context": "skipped",
        "build_context": "skipped",
        "score_pipeline": "succeeded",
        "analysis_artifacts": "succeeded",
        "investigation_artifacts": "succeeded",
        "health_summary": "succeeded",
    }
    assert [command[1] for command in executed_commands] == [
        "sales-ratio-study",
        "build-features",
        "score-fraud",
        "review-queue",
        "review-feedback",
        "investigation-report",
    ]
    assert payload["diagnostics"]["retry"] == {
        "attempt_count": 2,
        "retried_from_stage_id": "score_pipeline",
    }


def test_run_scheduled_refresh_rejects_invalid_retry_boundary_without_execution(
    tmp_path: Path,
) -> None:
    executed_commands: list[list[str]] = []

    def _executor(command: list[str]) -> int:
        executed_commands.append(command)
        return 0

    payload = run_scheduled_refresh(
        profile_name="daily_refresh",
        run_date="20260316",
        run_id="20260316_daily_refresh_feature_v1_scoring_rules_v1_060606",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=10,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=tmp_path / "refresh_runs",
        accessdane_bin="accessdane",
        attempt_count=2,
        retried_from_stage_id="not_a_stage",
        command_executor=_executor,
    )

    assert payload["run"]["status"] == "failed"
    assert payload["error"] is not None
    assert payload["error"]["code"] == "invalid_retry_boundary"
    assert payload["error"]["failed_stage_id"] is None
    assert executed_commands == []
    assert [stage["status"] for stage in payload["stages"]] == [
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
    ]


def test_run_scheduled_refresh_rejects_overlapping_profile_lock(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    lock_path = artifact_base_dir / "locks" / "daily_refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("existing lock\n", encoding="utf-8")

    payload = run_scheduled_refresh(
        profile_name="daily_refresh",
        run_date="20260316",
        run_id="20260316_daily_refresh_feature_v1_scoring_rules_v1_070707",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=10,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        accessdane_bin="accessdane",
    )

    assert payload["run"]["status"] == "failed"
    assert payload["run"]["run_persisted"] is False
    assert payload["error"] is not None
    assert payload["error"]["code"] == "overlapping_run"
    assert payload["error"]["failed_stage_id"] is None
    assert [stage["status"] for stage in payload["stages"]] == [
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
    ]


def test_acquire_profile_lock_cleans_up_on_metadata_write_failure(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    lock_path = artifact_base_dir / "locks" / "daily_refresh.lock"

    def _failing_fdopen(*args, **kwargs):
        raise OSError("disk full while writing lock metadata")

    monkeypatch.setattr(refresh_automation.os, "fdopen", _failing_fdopen)

    try:
        refresh_automation._acquire_profile_lock(
            artifact_base_dir=artifact_base_dir,
            profile_name="daily_refresh",
            started_dt=datetime(2026, 3, 16, tzinfo=timezone.utc),
        )
    except OSError:
        pass
    else:
        raise AssertionError("Expected OSError from lock metadata write failure")

    assert not lock_path.exists()


def test_run_scheduled_refresh_rejects_unsafe_run_context_without_executing_commands(
    tmp_path: Path,
) -> None:
    artifact_base_dir = tmp_path / "refresh_runs"
    executed_commands: list[list[str]] = []

    def _executor(command: list[str]) -> int:
        executed_commands.append(command)
        return 0

    payload = run_scheduled_refresh(
        profile_name="daily_refresh",
        run_date="20260316",
        run_id="../escape",
        feature_version="feature_v1",
        ruleset_version="scoring_rules_v1",
        sales_ratio_base="sales_ratio_v1",
        top=5,
        retr_file=None,
        permits_file=None,
        appeals_file=None,
        artifact_base_dir=artifact_base_dir,
        accessdane_bin="accessdane",
        command_executor=_executor,
    )

    assert payload["run"]["status"] == "failed"
    assert payload["error"] == {
        "code": "invalid_run_context",
        "message": (
            "run_id contains unsupported path characters; only letters, digits, "
            "'.', '_' and '-' are allowed."
        ),
        "failed_stage_id": None,
    }
    assert executed_commands == []
    assert [stage["status"] for stage in payload["stages"]] == [
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
        "blocked",
    ]


def test_refresh_runner_cli_writes_json_output(tmp_path: Path, monkeypatch) -> None:
    output_path = tmp_path / "refresh_payload.json"
    payload = {
        "run": {
            "run_type": "refresh_automation",
            "version_tag": "refresh_automation_v1",
            "run_id": "run-1",
            "profile_name": "daily_refresh",
            "status": "succeeded",
            "run_persisted": False,
            "started_at": "2026-03-16T01:00:00Z",
            "finished_at": "2026-03-16T01:00:01Z",
        },
        "request": {},
        "summary": {},
        "stages": [],
        "artifacts": {},
        "diagnostics": {},
        "error": None,
    }

    monkeypatch.setattr(cli, "run_scheduled_refresh", lambda **_: payload)
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "refresh-runner",
            "--run-date",
            "20260316",
            "--run-id",
            "run-1",
            "--out",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert output_path.exists()
    assert '"run_id": "run-1"' in output_path.read_text(encoding="utf-8")


def test_annual_refresh_runner_cli_writes_json_output(
    tmp_path: Path, monkeypatch
) -> None:
    output_path = tmp_path / "annual_refresh_payload.json"
    retr_file = tmp_path / "retr.csv"
    assessment_manifest_file = tmp_path / "assessment_manifest.json"
    retr_file.write_text("header\n", encoding="utf-8")
    assessment_manifest_file.write_text(
        '{"files": ["roll_2026.csv"]}\n', encoding="utf-8"
    )
    payload = {
        "run": {
            "run_type": "refresh_automation",
            "version_tag": "refresh_automation_v1",
            "run_id": "annual-run-1",
            "profile_name": "annual_refresh",
            "status": "succeeded",
            "run_persisted": False,
            "started_at": "2026-03-16T01:00:00Z",
            "finished_at": "2026-03-16T01:00:01Z",
        },
        "request": {},
        "summary": {},
        "stages": [],
        "artifacts": {},
        "diagnostics": {},
        "error": None,
    }

    monkeypatch.setattr(cli, "run_scheduled_refresh", lambda **_: payload)
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "annual-refresh-runner",
            "--run-date",
            "20260316",
            "--run-id",
            "annual-run-1",
            "--annual-target-year",
            "2026",
            "--assessment-manifest-file",
            str(assessment_manifest_file),
            "--retr-file",
            str(retr_file),
            "--out",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert output_path.exists()
    assert '"run_id": "annual-run-1"' in output_path.read_text(encoding="utf-8")
