from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit import promotion as promotion_module


def _profile_env(tmp_path: Path, *, environment_name: str) -> dict[str, str]:
    root = tmp_path / "data" / "environments" / environment_name
    return {
        "ACCESSDANE_ENVIRONMENT": environment_name,
        "DATABASE_URL": "sqlite:///:memory:",
        "ACCESSDANE_BASE_URL": "https://accessdane.danecounty.gov",
        "ACCESSDANE_RAW_DIR": str(root / "raw"),
        "ACCESSDANE_USER_AGENT": "AccessDaneAudit/0.1",
        "ACCESSDANE_TIMEOUT": "30",
        "ACCESSDANE_RETRIES": "3",
        "ACCESSDANE_BACKOFF": "1.5",
        "ACCESSDANE_REFRESH_PROFILE": "daily_refresh",
        "ACCESSDANE_FEATURE_VERSION": "feature_v1",
        "ACCESSDANE_RULESET_VERSION": "scoring_rules_v1",
        "ACCESSDANE_SALES_RATIO_BASE": "sales_ratio_v1",
        "ACCESSDANE_REFRESH_TOP": "100",
        "ACCESSDANE_ARTIFACT_BASE_DIR": str(root / "refresh_runs"),
        "ACCESSDANE_REFRESH_LOG_DIR": str(root / "refresh_runs" / "logs"),
        "ACCESSDANE_BENCHMARK_BASE_DIR": str(root / "benchmark_packs"),
        "ALERT_ROUTE_GROUP": "ops-alerts",
        "PROMOTION_APPROVER_GROUP": "release-approvers",
        "PROMOTION_FREEZE_FILE": str(root / "promotion_freeze.json"),
    }


def _base_pipeline_manifest(
    *,
    source_environment: str,
    target_environment: str,
    annual_refresh_impact: bool = False,
) -> dict[str, Any]:
    return {
        "promotion_id": "promotion_001",
        "source_environment": source_environment,
        "target_environment": target_environment,
        "requested_by": "requester@example.test",
        "requested_at_utc": "2026-03-25T00:00:00Z",
        "source_run_id": "20260325_daily_refresh_feature_v1_scoring_rules_v1_010101",
        "target_run_id": None,
        "feature_version": "feature_v1",
        "ruleset_version": "scoring_rules_v1",
        "evidence_artifacts": [],
        "approval_state": "approved",
        "approvals": [],
        "activation_state": "not_started",
        "activation_started_at_utc": None,
        "activated_by": None,
        "activated_at_utc": None,
        "rollback_reference": None,
        "freeze_override_note": None,
        "break_glass_used": False,
        "break_glass_incident_id": None,
        "contract_version": "promotion_pipeline_v1",
        "source_commit_sha": "0123456789abcdef0123456789abcdef01234567",
        "source_pr_number": 82,
        "change_summary": "Promote validated refresh artifacts.",
        "flags": {
            "annual_refresh_impact": annual_refresh_impact,
        },
    }


def _write_request_bundle(
    *,
    bundle_dir: Path,
    manifest: dict[str, Any],
    evidence_index: dict[str, Any],
) -> Path:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    (bundle_dir / "evidence_index.json").write_text(
        json.dumps(evidence_index, indent=2),
        encoding="utf-8",
    )
    return bundle_dir


def _write_artifact_file(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, indent=2).encode("utf-8")
    path.write_bytes(encoded)
    return hashlib.sha256(encoded).hexdigest()


def _build_evidence_index(
    *,
    artifact_root: Path,
    promotion_id: str,
    generated_at_utc: str,
    include_types: list[str],
    annual_signoff_status: str = "approved",
    review_feedback_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    artifacts: list[dict[str, Any]] = []
    for artifact_type in include_types:
        artifact_path = artifact_root / "evidence" / f"{artifact_type}.json"
        if artifact_type == "annual_signoff":
            artifact_payload: dict[str, Any] = {
                "run": {"status": annual_signoff_status},
                "generated_at_utc": generated_at_utc,
            }
        elif artifact_type == "review_feedback":
            if review_feedback_payload is not None:
                artifact_payload = review_feedback_payload
            else:
                artifact_payload = {
                    "run": {
                        "run_id": None,
                        "run_persisted": False,
                        "run_type": "review_feedback",
                        "version_tag": "review_feedback_v1",
                        "status": "succeeded",
                    },
                    "request": {
                        "feature_version": "feature_v1",
                        "ruleset_version": "scoring_rules_v1",
                    },
                    "summary": {
                        "reviewed_case_count": 1,
                    },
                }
        else:
            artifact_payload = {
                "artifact_type": artifact_type,
                "generated_at_utc": generated_at_utc,
            }
        artifact_sha = _write_artifact_file(artifact_path, artifact_payload)
        artifacts.append(
            {
                "artifact_type": artifact_type,
                "path": str(artifact_path),
                "sha256": artifact_sha,
                "run_id": "run_001",
                "generated_at_utc": generated_at_utc,
            }
        )
    return {
        "contract_version": "promotion_pipeline_v1",
        "promotion_id": promotion_id,
        "generated_at_utc": generated_at_utc,
        "artifacts": artifacts,
    }


def test_promotion_gate_cli_blocks_missing_required_evidence_types(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    activation_started_at_utc = "2026-03-26T12:00:00Z"
    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=["refresh_payload"],
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            activation_started_at_utc,
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "evidence_missing" for err in payload["errors"])


def test_promotion_gate_cli_missing_request_dir_emits_failed_payload(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    missing_request_dir = tmp_path / "missing_request_bundle"
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(missing_request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "path_safety_violation" for err in payload["errors"])
    gate_result_path = (
        Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"])
        / "promotion_gate_results"
        / "unknown_promotion"
        / f"{payload['gate']['gate_run_id']}.json"
    )
    assert gate_result_path.is_file()


def test_promotion_gate_cli_passes_and_records_approval_provenance(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    activation_started_at_utc = "2026-03-26T12:00:00Z"
    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            activation_started_at_utc,
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "passed"
    stage = next(
        item
        for item in payload["stages"]
        if item["stage_id"] == "approval_policy_validation"
    )
    assert stage["status"] == "passed"
    assert stage["details"]["valid_approval_count"] == 1
    assert stage["details"]["valid_approvers"] == ["owner@example.test"]
    assert payload["errors"] == []
    gate_result_path = (
        Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"])
        / "promotion_gate_results"
        / "promotion_001"
        / f"{payload['gate']['gate_run_id']}.json"
    )
    assert gate_result_path.is_file()


def test_promotion_gate_cli_rejects_review_feedback_missing_request_metadata(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
        review_feedback_payload={
            "run": {"status": "succeeded"},
            "summary": {"reviewed_case_count": 1},
        },
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "review_feedback.request object is required." in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_rejects_review_feedback_non_succeeded_run_status(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
        review_feedback_payload={
            "run": {"status": "failed"},
            "request": {
                "feature_version": "feature_v1",
                "ruleset_version": "scoring_rules_v1",
            },
            "summary": {"reviewed_case_count": 1},
        },
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "review_feedback.run.status must be 'succeeded'." in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_rejects_review_feedback_missing_run_object(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
        review_feedback_payload={
            "request": {
                "feature_version": "feature_v1",
                "ruleset_version": "scoring_rules_v1",
            },
            "summary": {"reviewed_case_count": 1},
        },
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "review_feedback.run object is required." in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_rejects_review_feedback_version_drift(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
        review_feedback_payload={
            "run": {"status": "succeeded"},
            "request": {
                "feature_version": "feature_v1",
                "ruleset_version": "scoring_rules_v2",
            },
            "summary": {"reviewed_case_count": 1},
        },
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "review_feedback.request.ruleset_version must match "
        "manifest.ruleset_version." in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_rejects_review_feedback_feature_version_drift(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
        review_feedback_payload={
            "run": {"status": "succeeded"},
            "request": {
                "feature_version": "feature_v2",
                "ruleset_version": "scoring_rules_v1",
            },
            "summary": {"reviewed_case_count": 1},
        },
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "review_feedback.request.feature_version must match "
        "manifest.feature_version." in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_rejects_review_feedback_missing_feature_version(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
        review_feedback_payload={
            "run": {"status": "succeeded"},
            "request": {
                "ruleset_version": "scoring_rules_v1",
            },
            "summary": {"reviewed_case_count": 1},
        },
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "review_feedback.request.feature_version is required." in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_rejects_review_feedback_missing_ruleset_version(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
        review_feedback_payload={
            "run": {"status": "succeeded"},
            "request": {
                "feature_version": "feature_v1",
                "ruleset_version": "",
            },
            "summary": {"reviewed_case_count": 1},
        },
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "review_feedback.request.ruleset_version is required." in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_handles_manifest_read_oserror_and_emits_payload(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    original_read_text = promotion_module.Path.read_text

    def _read_text_with_manifest_failure(self: Path, *args: Any, **kwargs: Any) -> str:
        if self.name == "manifest.json":
            raise OSError("permission denied")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(
        promotion_module.Path, "read_text", _read_text_with_manifest_failure
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any("failed to read" in err["message"] for err in payload["errors"])


def test_promotion_gate_cli_handles_artifact_hash_read_oserror_and_emits_payload(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    original_hash_file_sha256 = promotion_module._hash_file_sha256

    def _hash_file_sha256_with_read_failure(path: Path) -> str:
        if path.name == "refresh_payload.json":
            raise OSError("permission denied")
        return original_hash_file_sha256(path)

    monkeypatch.setattr(
        promotion_module,
        "_hash_file_sha256",
        _hash_file_sha256_with_read_failure,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "evidence_missing" for err in payload["errors"])
    assert any(
        "failed to read evidence artifact for hashing" in err["message"]
        for err in payload["errors"]
    )
    gate_result_path = (
        Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"])
        / "promotion_gate_results"
        / "promotion_001"
        / f"{payload['gate']['gate_run_id']}.json"
    )
    assert gate_result_path.is_file()


def test_promotion_gate_cli_rejects_mismatched_evidence_index_promotion_id(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_999",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "evidence_index.promotion_id must match manifest.promotion_id."
        in err["message"]
        for err in payload["errors"]
    )
    assert any(
        "evidence_index.promotion_id must match manifest.promotion_id."
        in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_normalizes_dev_stage_path_for_approval_policy(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment=" Dev ",
        target_environment="Stage",
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "passed"
    approval_stage = next(
        stage
        for stage in payload["stages"]
        if stage["stage_id"] == "approval_policy_validation"
    )
    assert approval_stage["status"] == "passed"
    assert approval_stage["details"]["valid_approval_count"] == 1


def test_promotion_gate_cli_rejects_non_approved_annual_signoff_status(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev",
        target_environment="stage",
        annual_refresh_impact=True,
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
        "annual_signoff",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
        annual_signoff_status="pending_signoff",
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "annual_signoff.run.status must be 'approved'" in err["message"]
        for err in payload["errors"]
    )
    assert any(
        "annual_signoff.run.status must be 'approved'" in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_blocks_empty_evidence_artifacts_list(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    evidence_index = {
        "contract_version": "promotion_pipeline_v1",
        "promotion_id": "promotion_001",
        "generated_at_utc": "2026-03-26T10:00:00Z",
        "artifacts": [],
    }
    manifest["evidence_artifacts"] = ["evidence/refresh_payload.json"]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        "missing required evidence artifact_type values" in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_enforces_trailing_z_for_evidence_timestamps(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    evidence_index["generated_at_utc"] = "2026-03-26T10:00:00+00:00"
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "evidence_missing" for err in payload["errors"])
    assert any(
        "evidence_index.generated_at_utc must be a UTC timestamp ending with 'Z'."
        in err["message"]
        for err in payload["errors"]
    )

    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00+00:00",
        include_types=required_types,
    )
    evidence_index["generated_at_utc"] = "2026-03-26T10:00:00Z"
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle_2",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "evidence_missing" for err in payload["errors"])
    assert any(
        "generated_at_utc must be an ISO-8601 UTC timestamp ending with 'Z'."
        in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_allows_exact_staleness_boundary_for_stage_prod(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="prod")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="Stage",
        target_environment="Prod",
    )
    manifest["approvals"] = [
        {
            "approved_by": "analyst@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "analyst_owner",
        },
        {
            "approved_by": "engineer@example.test",
            "approved_at_utc": "2026-03-26T11:01:00Z",
            "approver_role": "engineering_owner",
        },
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-19T12:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "passed"
    evidence_stage = next(
        stage
        for stage in payload["stages"]
        if stage["stage_id"] == "evidence_integrity_validation"
    )
    assert evidence_stage["status"] == "passed"
    assert evidence_stage["details"]["evidence_freshness_window_days"] == 7


def test_promotion_gate_cli_handles_invalid_freeze_file_and_emits_payload(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text("{invalid-json", encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "break_glass_invalid" for err in payload["errors"])
    assert any("Invalid JSON in" in err["message"] for err in payload["errors"])
    gate_result_path = (
        Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"])
        / "promotion_gate_results"
        / "promotion_001"
        / f"{payload['gate']['gate_run_id']}.json"
    )
    assert gate_result_path.is_file()


def test_promotion_gate_cli_invalid_gate_run_id_uses_fallback_and_emits_payload(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
            "--gate-run-id",
            "../invalid-run-id",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert payload["gate"]["gate_run_id"].startswith("gate_")
    assert any(err["code"] == "path_safety_violation" for err in payload["errors"])
    assert any("gate_run_id" in err["message"] for err in payload["errors"])
    gate_result_path = (
        Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"])
        / "promotion_gate_results"
        / "promotion_001"
        / f"{payload['gate']['gate_run_id']}.json"
    )
    assert gate_result_path.is_file()


def test_promotion_gate_cli_handles_freeze_file_read_oserror_and_emits_payload(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    original_read_text = promotion_module.Path.read_text

    def _read_text_with_freeze_failure(self: Path, *args: Any, **kwargs: Any) -> str:
        if self == freeze_path:
            raise OSError("permission denied")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(
        promotion_module.Path, "read_text", _read_text_with_freeze_failure
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "break_glass_invalid" for err in payload["errors"])
    assert any("Failed to read" in err["message"] for err in payload["errors"])
    gate_result_path = (
        Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"])
        / "promotion_gate_results"
        / "promotion_001"
        / f"{payload['gate']['gate_run_id']}.json"
    )
    assert gate_result_path.is_file()


def test_promotion_gate_cli_uses_evidence_scoped_code_for_invalid_evidence_index_json(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )
    (request_dir / "evidence_index.json").write_text("{invalid-json", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "evidence_missing" for err in payload["errors"])
    assert any("invalid JSON in" in err["message"] for err in payload["errors"])


def test_promotion_gate_cli_uses_evidence_scoped_code_for_invalid_annual_signoff_json(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev",
        target_environment="stage",
        annual_refresh_impact=True,
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
        "annual_signoff",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    annual_path = next(
        Path(item["path"])
        for item in evidence_index["artifacts"]
        if item["artifact_type"] == "annual_signoff"
    )
    annual_path.write_text("{invalid-json", encoding="utf-8")
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "evidence_missing" for err in payload["errors"])
    assert any(
        "invalid JSON in" in err["message"] and "annual_signoff.json" in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_uses_evidence_scoped_code_for_non_object_artifacts_entries(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    evidence_index["artifacts"] = ["not-an-object"]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        err["code"] == "evidence_missing"
        and "evidence_index artifacts[] entries must be objects." in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_reports_source_commit_sha_hex_requirement(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["source_commit_sha"] = "not-a-sha"
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(
        "source_commit_sha must be a 40-character hexadecimal SHA-1." in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_rejects_duplicate_gate_run_id_without_overwrite(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    duplicate_gate_run_id = "gate_manual_duplicate"
    first_result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
            "--gate-run-id",
            duplicate_gate_run_id,
        ],
    )
    assert first_result.exit_code == 0
    first_payload = json.loads(first_result.output)
    assert first_payload["gate"]["gate_run_id"] == duplicate_gate_run_id
    existing_path = (
        Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"])
        / "promotion_gate_results"
        / "promotion_001"
        / f"{duplicate_gate_run_id}.json"
    )
    assert existing_path.is_file()
    original_contents = existing_path.read_text(encoding="utf-8")

    second_result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
            "--gate-run-id",
            duplicate_gate_run_id,
        ],
    )
    assert second_result.exit_code == 1
    second_payload = json.loads(second_result.output)
    assert second_payload["gate"]["status"] == "failed"
    assert second_payload["gate"]["gate_run_id"] != duplicate_gate_run_id
    assert any(
        "promotion gate results are append-only" in err["message"]
        for err in second_payload["errors"]
    )
    assert existing_path.read_text(encoding="utf-8") == original_contents
    fallback_path = (
        Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"])
        / "promotion_gate_results"
        / "promotion_001"
        / f"{second_payload['gate']['gate_run_id']}.json"
    )
    assert fallback_path.is_file()


def test_promotion_gate_cli_avoids_default_gate_run_id_collisions(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    monkeypatch.setattr(
        promotion_module,
        "_build_default_gate_run_id",
        lambda _: "gate_collision",
    )

    runner = CliRunner()
    first_result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )
    assert first_result.exit_code == 0
    first_payload = json.loads(first_result.output)
    assert first_payload["gate"]["gate_run_id"] == "gate_collision"

    second_result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )
    assert second_result.exit_code == 0
    second_payload = json.loads(second_result.output)
    assert second_payload["gate"]["gate_run_id"] == "gate_collision_1"
    request_stage = next(
        stage
        for stage in second_payload["stages"]
        if stage["stage_id"] == "request_normalization"
    )
    assert (
        request_stage["details"]["gate_run_id_uniqueness"]
        == "default_collision_avoided"
    )


def test_promotion_gate_cli_maps_evidence_index_contract_errors_to_evidence_missing(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    evidence_index["contract_version"] = "promotion_pipeline_v0"
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "evidence_missing" for err in payload["errors"])
    assert any(
        "evidence_index.contract_version must be 'promotion_pipeline_v1'."
        in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_rejects_evidence_paths_outside_artifact_roots(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    external_artifact_path = (
        tmp_path / "request_bundle" / "evidence" / "outside_artifact_roots.json"
    )
    external_sha = _write_artifact_file(
        external_artifact_path,
        {
            "artifact_type": "refresh_payload",
            "generated_at_utc": "2026-03-26T10:00:00Z",
        },
    )
    evidence_index["artifacts"][0]["path"] = str(external_artifact_path)
    evidence_index["artifacts"][0]["sha256"] = external_sha
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "path_safety_violation" for err in payload["errors"])
    assert any(
        "evidence artifact path must resolve under active environment artifact roots."
        in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_handles_gate_result_write_oserror_and_emits_payload(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    def _raise_write_failure(
        path: Path, payload: dict[str, Any], *, overwrite: bool = True
    ) -> None:
        _ = path
        _ = payload
        _ = overwrite
        raise OSError("disk full")

    monkeypatch.setattr(promotion_module, "_write_json_atomic", _raise_write_failure)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "failed"
    assert any(err["code"] == "manifest_invalid_value" for err in payload["errors"])
    assert any(
        "failed to persist gate result artifact:" in err["message"]
        for err in payload["errors"]
    )


def test_promotion_gate_cli_retries_on_gate_result_write_collision(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_pipeline_manifest(
        source_environment="dev", target_environment="stage"
    )
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-26T11:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    required_types = [
        "refresh_payload",
        "review_feedback",
        "parser_drift_diff",
        "load_monitor",
        "benchmark_pack",
        "observability_rollup",
    ]
    evidence_index = _build_evidence_index(
        artifact_root=Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"]),
        promotion_id="promotion_001",
        generated_at_utc="2026-03-26T10:00:00Z",
        include_types=required_types,
    )
    manifest["evidence_artifacts"] = [
        item["path"] for item in evidence_index["artifacts"]
    ]
    request_dir = _write_request_bundle(
        bundle_dir=tmp_path / "request_bundle",
        manifest=manifest,
        evidence_index=evidence_index,
    )

    monkeypatch.setattr(
        promotion_module,
        "_build_default_gate_run_id",
        lambda _: "gate_collision",
    )
    original_write = promotion_module._write_json_atomic
    write_attempts = {"count": 0}

    def _write_collision_once(
        path: Path, payload: dict[str, Any], *, overwrite: bool = True
    ) -> None:
        write_attempts["count"] += 1
        if write_attempts["count"] == 1:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}", encoding="utf-8")
            raise FileExistsError("simulated write collision")
        original_write(path, payload, overwrite=overwrite)

    monkeypatch.setattr(promotion_module, "_write_json_atomic", _write_collision_once)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "promotion-gate",
            "--request-dir",
            str(request_dir),
            "--activation-started-at-utc",
            "2026-03-26T12:00:00Z",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["gate"]["status"] == "passed"
    request_stage = next(
        stage
        for stage in payload["stages"]
        if stage["stage_id"] == "request_normalization"
    )
    assert request_stage["details"]["gate_result_persistence"] == (
        "written_after_collision_retry"
    )
    assert request_stage["details"]["gate_result_collision_retries"] == 1
    assert write_attempts["count"] >= 2
    assert payload["gate"]["gate_run_id"] == "gate_collision_1"
    persisted_path = (
        Path(env["ACCESSDANE_ARTIFACT_BASE_DIR"])
        / "promotion_gate_results"
        / "promotion_001"
        / f"{payload['gate']['gate_run_id']}.json"
    )
    assert persisted_path.is_file()
