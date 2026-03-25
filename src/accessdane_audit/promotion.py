from __future__ import annotations

import json
import os
import re
import tempfile
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
    "target_run_id",
    "feature_version",
    "ruleset_version",
    "evidence_artifacts",
    "approval_state",
    "approvals",
    "activation_state",
    "activation_started_at_utc",
    "activated_by",
    "activated_at_utc",
    "rollback_reference",
    "freeze_override_note",
    "break_glass_used",
    "break_glass_incident_id",
)
SAFE_PATH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")


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
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PromotionError(f"Invalid JSON in {manifest_path}.") from exc
    if not isinstance(manifest, dict):
        raise PromotionError(
            "Promotion manifest must be a JSON object, but got "
            f"{type(manifest).__name__}."
        )
    _validate_manifest_required_fields(manifest)
    _validate_source_target_matrix(manifest=manifest, profile=profile)
    _validate_required_metadata(manifest)

    if activation_started_at is None:
        activation_dt = datetime.now(timezone.utc)
    elif activation_started_at.tzinfo is None:
        raise PromotionError("activation_started_at must include timezone information.")
    else:
        activation_dt = activation_started_at.astimezone(timezone.utc)
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

    promotion_id = _validate_promotion_id(str(manifest["promotion_id"]).strip())
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
    _write_json_atomic(manifest_output_path, manifest)
    _write_json_atomic(approval_log_path, {"approvals": manifest.get("approvals", [])})
    _write_json_atomic(
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
    _write_json_atomic(
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
    source_environment = str(manifest["source_environment"]).strip().lower()
    target_environment = str(manifest["target_environment"]).strip().lower()
    manifest["source_environment"] = source_environment
    manifest["target_environment"] = target_environment
    if (source_environment, target_environment) not in ALLOWED_PROMOTION_PATHS:
        raise PromotionError(
            "Unsupported promotion path. Allowed paths are dev->stage and stage->prod."
        )
    if target_environment != profile.environment_name.lower():
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
    requested_by = str(manifest["requested_by"]).strip()
    if not requested_by:
        raise PromotionError("requested_by is required.")
    manifest["requested_by"] = requested_by
    requested_at = _parse_iso_utc(
        str(manifest["requested_at_utc"]),
        field_name="requested_at_utc",
    )
    manifest["requested_at_utc"] = _iso_utc(requested_at)
    if not str(manifest["source_run_id"]).strip():
        raise PromotionError("source_run_id is required.")
    feature_version = str(manifest["feature_version"]).strip()
    ruleset_version = str(manifest["ruleset_version"]).strip()
    if not feature_version or not ruleset_version:
        raise PromotionError("feature_version and ruleset_version are required.")
    manifest["feature_version"] = feature_version
    manifest["ruleset_version"] = ruleset_version

    evidence = manifest["evidence_artifacts"]
    if not isinstance(evidence, list) or not evidence:
        raise PromotionError("evidence_artifacts must be a non-empty list.")

    target_run_id = manifest["target_run_id"]
    if target_run_id is not None and not str(target_run_id).strip():
        raise PromotionError("target_run_id must be null or a non-empty string.")

    freeze_override_note = manifest["freeze_override_note"]
    if freeze_override_note is not None and not str(freeze_override_note).strip():
        raise PromotionError("freeze_override_note must be null or a non-empty string.")

    break_glass_used = manifest["break_glass_used"]
    if not isinstance(break_glass_used, bool):
        raise PromotionError("break_glass_used must be a boolean.")
    break_glass_incident_id = manifest["break_glass_incident_id"]
    if break_glass_incident_id is not None and not str(break_glass_incident_id).strip():
        raise PromotionError(
            "break_glass_incident_id must be null or a non-empty string."
        )
    if break_glass_used and break_glass_incident_id is None:
        raise PromotionError(
            "break_glass_incident_id is required when break_glass_used is true."
        )

    for nullable_timestamp in ("activation_started_at_utc", "activated_at_utc"):
        value = manifest[nullable_timestamp]
        if value is not None:
            _parse_iso_utc(str(value), field_name=nullable_timestamp)

    activated_by = manifest["activated_by"]
    if activated_by is not None and not str(activated_by).strip():
        raise PromotionError("activated_by must be null or a non-empty string.")

    rollback_reference = manifest["rollback_reference"]
    if rollback_reference is not None and not isinstance(rollback_reference, dict):
        raise PromotionError("rollback_reference must be null or an object.")


def _validate_approvals_for_activation(
    *, manifest: dict[str, Any], activation_started_at: datetime
) -> None:
    approvals = manifest.get("approvals")
    if not isinstance(approvals, list):
        raise PromotionError("approvals must be a list of approval records.")
    source_environment = str(manifest["source_environment"])
    target_environment = str(manifest["target_environment"])
    requester = str(manifest["requested_by"])

    valid_approvals: list[dict[str, str]] = []
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
        approved_at = _parse_iso_utc(approved_at_raw, field_name="approved_at_utc")
        if approved_at > activation_started_at:
            raise PromotionError(
                "approved_at_utc must not be later than activation_started_at."
            )
        if approved_at + timedelta(hours=24) <= activation_started_at:
            continue
        valid_approvals.append(
            {
                "approved_by": approved_by,
                "approved_at_utc": approved_at_raw,
                "approver_role": approver_role,
            }
        )

    if (source_environment, target_environment) == ("dev", "stage"):
        if len(valid_approvals) < 1:
            raise PromotionError("dev->stage requires at least one valid approval.")
        return

    if len(valid_approvals) < 2:
        raise PromotionError("stage->prod requires two valid approvals.")

    valid_roles = {item["approver_role"] for item in valid_approvals}
    if "analyst_owner" not in valid_roles or "engineering_owner" not in valid_roles:
        raise PromotionError(
            "stage->prod approvals must include one analyst_owner and one "
            "engineering_owner."
        )
    distinct_approvers = {
        item["approved_by"] for item in valid_approvals if item["approved_by"]
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
    override_approvers = freeze_payload.get("override_approvers")
    if not isinstance(override_approvers, list):
        raise PromotionError(
            "hard freeze break-glass activation requires override_approvers list in "
            "freeze file."
        )
    normalized_override_approvers = [
        str(approver).strip()
        for approver in override_approvers
        if str(approver).strip()
    ]
    if len(normalized_override_approvers) < 2:
        raise PromotionError(
            "hard freeze break-glass activation requires at least two override "
            "approvers."
        )
    if len(set(normalized_override_approvers)) < 2:
        raise PromotionError(
            "hard freeze break-glass override approvers must be two distinct "
            "identities."
        )

    override_expires_at_utc = freeze_payload.get("override_expires_at_utc")
    if (
        not isinstance(override_expires_at_utc, str)
        or not override_expires_at_utc.strip()
    ):
        raise PromotionError(
            "hard freeze break-glass activation requires override_expires_at_utc in "
            "freeze file."
        )
    override_expires_at = _parse_iso_utc(
        override_expires_at_utc.strip(), field_name="override_expires_at_utc"
    )
    activation_started = _parse_iso_utc(
        activation_started_at_utc, field_name="activation_started_at_utc"
    )
    if activation_started > override_expires_at:
        raise PromotionError("hard freeze break-glass override approvals have expired.")

    # Marker is written into the manifest to make break-glass audit timing explicit.
    manifest["break_glass_validated_at_utc"] = activation_started_at_utc


def _read_json_if_exists(path: Path) -> Optional[dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PromotionError(f"Invalid JSON in {path}.") from exc
    if not isinstance(payload, dict):
        raise PromotionError(f"Expected object payload in {path}.")
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload_text = json.dumps(payload, indent=2)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload_text)
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass


def _parse_iso_utc(value: str, *, field_name: str = "timestamp") -> datetime:
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise PromotionError(
            f"{field_name} must be a valid ISO-8601 timestamp."
        ) from exc
    if parsed.tzinfo is None:
        raise PromotionError(f"{field_name} must include timezone information.")
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _validate_promotion_id(value: str) -> str:
    if not value:
        raise PromotionError("promotion_id is required.")
    promotion_path = Path(value)
    if promotion_path.is_absolute():
        raise PromotionError("promotion_id must not be an absolute path.")
    if value in {".", ".."} or ".." in value:
        raise PromotionError("promotion_id must not contain path traversal segments.")
    if "/" in value or "\\" in value:
        raise PromotionError("promotion_id must be a single filesystem path component.")
    if not SAFE_PATH_SEGMENT_RE.fullmatch(value):
        raise PromotionError(
            "promotion_id contains unsupported path characters; only letters, "
            "digits, '.', '_' and '-' are allowed."
        )
    return value
