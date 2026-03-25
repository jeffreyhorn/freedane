from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from .environment_profiles import EnvironmentProfile

ALLOWED_PROMOTION_PATHS: set[tuple[str, str]] = {
    ("dev", "stage"),
    ("stage", "prod"),
}
ALLOWED_APPROVAL_ROLES: set[str] = {
    "engineering_owner",
    "analyst_owner",
    "release_operator",
}
MANIFEST_REQUIRED_FIELDS: tuple[str, ...] = (
    "promotion_id",
    "source_environment",
    "target_environment",
    "requested_by",
    "requested_at_utc",
    "source_run_id",
    "feature_version",
    "ruleset_version",
    "evidence_artifacts",
    "approval_state",
    "approvals",
    "activation_state",
)


class PromotionError(ValueError):
    pass


@dataclass(frozen=True)
class PromotionActivationResult:
    promotion_id: str
    manifest_path: Path
    approval_log_path: Path
    activation_log_path: Path
    active_selector_path: Path
    rollback_reference: Optional[dict[str, Any]]


def activate_promotion_manifest(
    *,
    manifest_path: Path,
    profile: EnvironmentProfile,
    activated_by: str,
    activation_started_at: Optional[datetime] = None,
) -> PromotionActivationResult:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _validate_manifest_required_fields(manifest)
    _validate_source_target_matrix(manifest=manifest, profile=profile)
    _validate_required_metadata(manifest)

    activation_dt = activation_started_at or datetime.now(timezone.utc)
    activation_started_at_utc = _iso_utc(activation_dt)
    _enforce_freeze_policy(
        manifest=manifest,
        freeze_file_path=profile.promotion_freeze_file,
        activation_started_at_utc=activation_started_at_utc,
    )
    _validate_approvals_for_activation(
        manifest=manifest,
        activation_started_at=activation_dt,
    )

    promotion_id = str(manifest["promotion_id"]).strip()
    registry_root = profile.promotion_registry_root
    promotion_root = registry_root / promotion_id
    promotion_root.mkdir(parents=True, exist_ok=True)

    active_selector_path = registry_root / "active_selectors.json"
    rollback_reference = _read_json_if_exists(active_selector_path)

    manifest["activation_started_at_utc"] = activation_started_at_utc
    manifest["activation_state"] = "succeeded"
    manifest["activated_by"] = activated_by
    manifest["activated_at_utc"] = activation_started_at_utc
    manifest["rollback_reference"] = rollback_reference
    if not manifest.get("target_run_id"):
        manifest["target_run_id"] = manifest["source_run_id"]

    manifest_output_path = promotion_root / "manifest.json"
    approval_log_path = promotion_root / "approval_log.json"
    activation_log_path = promotion_root / "activation_log.json"
    _write_json(manifest_output_path, manifest)
    _write_json(approval_log_path, {"approvals": manifest.get("approvals", [])})
    _write_json(
        activation_log_path,
        {
            "promotion_id": promotion_id,
            "source_environment": manifest["source_environment"],
            "target_environment": manifest["target_environment"],
            "activation_started_at_utc": activation_started_at_utc,
            "activation_state": manifest["activation_state"],
            "activated_by": activated_by,
            "rollback_reference": rollback_reference,
        },
    )
    _write_json(
        active_selector_path,
        {
            "feature_version": manifest["feature_version"],
            "ruleset_version": manifest["ruleset_version"],
            "promotion_id": promotion_id,
            "activated_at_utc": activation_started_at_utc,
        },
    )
    return PromotionActivationResult(
        promotion_id=promotion_id,
        manifest_path=manifest_output_path,
        approval_log_path=approval_log_path,
        activation_log_path=activation_log_path,
        active_selector_path=active_selector_path,
        rollback_reference=rollback_reference,
    )


def _validate_manifest_required_fields(manifest: dict[str, Any]) -> None:
    missing = [field for field in MANIFEST_REQUIRED_FIELDS if field not in manifest]
    if missing:
        raise PromotionError(
            "Promotion manifest missing required fields: " + ", ".join(missing)
        )


def _validate_source_target_matrix(
    *, manifest: dict[str, Any], profile: EnvironmentProfile
) -> None:
    source_environment = str(manifest["source_environment"])
    target_environment = str(manifest["target_environment"])
    if (source_environment, target_environment) not in ALLOWED_PROMOTION_PATHS:
        raise PromotionError(
            "Unsupported promotion path. Allowed paths are dev->stage and stage->prod."
        )
    if target_environment != profile.environment_name:
        raise PromotionError(
            "target_environment must match ACCESSDANE_ENVIRONMENT for activation."
        )


def _validate_required_metadata(manifest: dict[str, Any]) -> None:
    if str(manifest["approval_state"]) != "approved":
        raise PromotionError("approval_state must be 'approved' before activation.")
    if str(manifest["activation_state"]) != "not_started":
        raise PromotionError(
            "activation_state must be 'not_started' before activation."
        )
    if not str(manifest["source_run_id"]).strip():
        raise PromotionError("source_run_id is required.")
    feature_version = str(manifest["feature_version"]).strip()
    ruleset_version = str(manifest["ruleset_version"]).strip()
    if not feature_version or not ruleset_version:
        raise PromotionError("feature_version and ruleset_version are required.")

    evidence = manifest["evidence_artifacts"]
    if not isinstance(evidence, list) or not evidence:
        raise PromotionError("evidence_artifacts must be a non-empty list.")


def _validate_approvals_for_activation(
    *, manifest: dict[str, Any], activation_started_at: datetime
) -> None:
    approvals = manifest.get("approvals")
    if not isinstance(approvals, list):
        raise PromotionError("approvals must be a list of approval records.")
    source_environment = str(manifest["source_environment"])
    target_environment = str(manifest["target_environment"])
    requester = str(manifest["requested_by"])

    valid_approvals: list[dict[str, Any]] = []
    for approval in approvals:
        if not isinstance(approval, dict):
            raise PromotionError("approval record must be an object.")
        approved_by = str(approval.get("approved_by", "")).strip()
        approved_at_raw = str(approval.get("approved_at_utc", "")).strip()
        approver_role = str(approval.get("approver_role", "")).strip()
        if not approved_by or not approved_at_raw or not approver_role:
            raise PromotionError(
                "each approval record must include approved_by, approved_at_utc, "
                "and approver_role."
            )
        if approver_role not in ALLOWED_APPROVAL_ROLES:
            raise PromotionError(
                "approver_role must be one of: "
                + ", ".join(sorted(ALLOWED_APPROVAL_ROLES))
                + "."
            )
        if approved_by == requester:
            raise PromotionError("no self-approval is allowed.")
        approved_at = _parse_iso_utc(approved_at_raw)
        if approved_at + timedelta(hours=24) < activation_started_at:
            continue
        valid_approvals.append(approval)

    if (source_environment, target_environment) == ("dev", "stage"):
        if len(valid_approvals) < 1:
            raise PromotionError("dev->stage requires at least one valid approval.")
        return

    if len(valid_approvals) < 2:
        raise PromotionError("stage->prod requires two valid approvals.")

    valid_roles = {str(item.get("approver_role")) for item in valid_approvals}
    if "analyst_owner" not in valid_roles or "engineering_owner" not in valid_roles:
        raise PromotionError(
            "stage->prod approvals must include one analyst_owner and one "
            "engineering_owner."
        )
    distinct_approvers = {
        str(item.get("approved_by")).strip()
        for item in valid_approvals
        if str(item.get("approved_by", "")).strip()
    }
    if len(distinct_approvers) < 2:
        raise PromotionError(
            "stage->prod approvals must come from distinct approved_by identities."
        )


def _enforce_freeze_policy(
    *,
    manifest: dict[str, Any],
    freeze_file_path: Path,
    activation_started_at_utc: str,
) -> None:
    freeze_payload = _read_json_if_exists(freeze_file_path) or {"state": "none"}
    state = str(freeze_payload.get("state", "none"))
    if state not in {"none", "advisory", "hard"}:
        raise PromotionError("freeze file state must be one of: none, advisory, hard.")

    if state == "advisory":
        if not str(manifest.get("freeze_override_note", "")).strip():
            raise PromotionError(
                "advisory freeze requires freeze_override_note in manifest."
            )
    if state != "hard":
        return

    if manifest.get("break_glass_used") is not True:
        raise PromotionError(
            "hard freeze blocks activation unless break_glass_used is true."
        )
    if not str(manifest.get("break_glass_incident_id", "")).strip():
        raise PromotionError(
            "hard freeze break-glass activation requires break_glass_incident_id."
        )

    # Marker is written into the manifest to make break-glass audit timing explicit.
    manifest["break_glass_validated_at_utc"] = activation_started_at_utc


def _read_json_if_exists(path: Path) -> Optional[dict[str, Any]]:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise PromotionError(f"Expected object payload in {path}.")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parse_iso_utc(value: str) -> datetime:
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(candidate)
    if parsed.tzinfo is None:
        raise PromotionError("approved_at_utc must include timezone information.")
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
