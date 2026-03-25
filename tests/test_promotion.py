from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.environment_profiles import load_environment_profile
from accessdane_audit.promotion import PromotionError, activate_promotion_manifest


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


def _write_manifest(tmp_path: Path, payload: dict[str, Any]) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _base_manifest(
    *, source_environment: str, target_environment: str
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
        "evidence_artifacts": ["refresh_run_payload.json"],
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
    }


def test_promotion_blocks_stage_prod_with_insufficient_approvals(
    tmp_path: Path,
) -> None:
    env = _profile_env(tmp_path, environment_name="prod")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="stage", target_environment="prod")
    manifest["approvals"] = [
        {
            "approved_by": "analyst@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "analyst_owner",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(
        PromotionError, match="stage->prod requires two valid approvals"
    ):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )


def test_promotion_blocks_hard_freeze_without_break_glass(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "hard"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="dev", target_environment="stage")
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(PromotionError, match="hard freeze blocks activation"):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )


def test_promotion_activation_captures_rollback_reference(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    profile.promotion_registry_root.mkdir(parents=True, exist_ok=True)
    active_selector_path = profile.promotion_registry_root / "active_selectors.json"
    active_selector_path.write_text(
        json.dumps(
            {
                "feature_version": "feature_v0",
                "ruleset_version": "scoring_rules_v0",
                "promotion_id": "old_promotion",
            }
        ),
        encoding="utf-8",
    )

    manifest = _base_manifest(source_environment="dev", target_environment="stage")
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    result = activate_promotion_manifest(
        manifest_path=manifest_path,
        profile=profile,
        activated_by="operator@example.test",
        activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
    )

    persisted_manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert persisted_manifest["activation_state"] == "succeeded"
    assert persisted_manifest["rollback_reference"] == {
        "feature_version": "feature_v0",
        "ruleset_version": "scoring_rules_v0",
        "promotion_id": "old_promotion",
    }
    new_active_selectors = json.loads(
        result.active_selector_path.read_text(encoding="utf-8")
    )
    assert new_active_selectors["feature_version"] == "feature_v1"
    assert new_active_selectors["ruleset_version"] == "scoring_rules_v1"


def test_promotion_activate_cli_blocks_unsafe_stage_to_prod_manifest(
    tmp_path: Path, monkeypatch
) -> None:
    env = _profile_env(tmp_path, environment_name="prod")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    manifest = _base_manifest(source_environment="stage", target_environment="prod")
    manifest["approvals"] = [
        {
            "approved_by": "analyst@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "analyst_owner",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["promotion-activate", "--manifest-file", str(manifest_path)],
    )

    assert result.exit_code == 1
    assert "stage->prod requires two valid approvals" in result.output


def test_promotion_manifest_must_be_json_object(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("[]", encoding="utf-8")

    with pytest.raises(PromotionError, match="must be a JSON object"):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )


def test_promotion_surfaces_invalid_manifest_json(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(PromotionError, match="Invalid JSON in"):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )


def test_manifest_requested_by_must_be_non_empty(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="dev", target_environment="stage")
    manifest["requested_by"] = "   "
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(PromotionError, match="requested_by is required"):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )


def test_manifest_requested_at_must_include_timezone(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="dev", target_environment="stage")
    manifest["requested_at_utc"] = "2026-03-25T00:00:00"
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(
        PromotionError, match="requested_at_utc must include timezone information"
    ):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )


def test_manifest_selector_versions_are_normalized_before_persist(
    tmp_path: Path,
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="dev", target_environment="stage")
    manifest["feature_version"] = " feature_v1 "
    manifest["ruleset_version"] = " scoring_rules_v1 "
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    result = activate_promotion_manifest(
        manifest_path=manifest_path,
        profile=profile,
        activated_by="operator@example.test",
        activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
    )

    persisted_manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert persisted_manifest["feature_version"] == "feature_v1"
    assert persisted_manifest["ruleset_version"] == "scoring_rules_v1"

    active_selectors = json.loads(
        result.active_selector_path.read_text(encoding="utf-8")
    )
    assert active_selectors["feature_version"] == "feature_v1"
    assert active_selectors["ruleset_version"] == "scoring_rules_v1"


def test_promotion_rejects_naive_activation_started_at(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="dev", target_environment="stage")
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(
        PromotionError, match="activation_started_at must include timezone information"
    ):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0),
        )


def test_promotion_normalizes_source_target_environment_names(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment=" Dev ", target_environment=" Stage ")
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    result = activate_promotion_manifest(
        manifest_path=manifest_path,
        profile=profile,
        activated_by="operator@example.test",
        activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
    )
    persisted_manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert persisted_manifest["source_environment"] == "dev"
    assert persisted_manifest["target_environment"] == "stage"


def test_hard_freeze_break_glass_requires_override_approvers(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "hard"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="dev", target_environment="stage")
    manifest["break_glass_used"] = True
    manifest["break_glass_incident_id"] = "INC-2026-03-25"
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(PromotionError, match="override_approvers list"):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )


def test_manifest_missing_required_contract_field_fails(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="dev", target_environment="stage")
    manifest.pop("rollback_reference")
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(
        PromotionError, match="missing required fields: rollback_reference"
    ):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )


def test_stage_prod_approval_role_whitespace_is_normalized(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="prod")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="stage", target_environment="prod")
    manifest["approvals"] = [
        {
            "approved_by": "analyst@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "analyst_owner",
        },
        {
            "approved_by": "engineer@example.test",
            "approved_at_utc": "2026-03-25T01:10:00Z",
            "approver_role": "engineering_owner ",
        },
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    result = activate_promotion_manifest(
        manifest_path=manifest_path,
        profile=profile,
        activated_by="operator@example.test",
        activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
    )
    assert result.promotion_id == "promotion_001"


def test_promotion_rejects_future_approval_timestamps(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="prod")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text('{"state": "none"}', encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="stage", target_environment="prod")
    manifest["approvals"] = [
        {
            "approved_by": "engineer@example.test",
            "approved_at_utc": "2026-03-25T03:00:00Z",
            "approver_role": "engineering_owner",
        },
        {
            "approved_by": "analyst@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "analyst_owner",
        },
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(
        PromotionError, match="must not be later than activation_started_at"
    ):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )


def test_promotion_surfaces_invalid_freeze_file_json(tmp_path: Path) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    freeze_path = Path(env["PROMOTION_FREEZE_FILE"])
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text("{not-json", encoding="utf-8")
    profile = load_environment_profile(environ=env)
    assert profile is not None

    manifest = _base_manifest(source_environment="dev", target_environment="stage")
    manifest["approvals"] = [
        {
            "approved_by": "owner@example.test",
            "approved_at_utc": "2026-03-25T01:00:00Z",
            "approver_role": "release_operator",
        }
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(PromotionError, match="Invalid JSON in"):
        activate_promotion_manifest(
            manifest_path=manifest_path,
            profile=profile,
            activated_by="operator@example.test",
            activation_started_at=datetime(2026, 3, 25, 2, 0, tzinfo=timezone.utc),
        )
