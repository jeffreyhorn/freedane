from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from copy import deepcopy
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
PROMOTION_PIPELINE_CONTRACT_VERSION = "promotion_pipeline_v1"
PROMOTION_PIPELINE_STAGE_IDS: tuple[str, ...] = (
    "request_normalization",
    "manifest_contract_validation",
    "evidence_integrity_validation",
    "approval_policy_validation",
    "freeze_policy_validation",
    "activation_readiness_validation",
)
PIPELINE_REQUIRED_MANIFEST_FIELDS: tuple[str, ...] = (
    "contract_version",
    "source_commit_sha",
    "source_pr_number",
    "change_summary",
    "flags",
)
PIPELINE_REQUIRED_EVIDENCE_INDEX_FIELDS: tuple[str, ...] = (
    "contract_version",
    "promotion_id",
    "generated_at_utc",
    "artifacts",
)
PIPELINE_REQUIRED_EVIDENCE_ARTIFACT_FIELDS: tuple[str, ...] = (
    "artifact_type",
    "path",
    "sha256",
    "run_id",
    "generated_at_utc",
)
PIPELINE_REQUIRED_ARTIFACT_TYPES: tuple[str, ...] = (
    "refresh_payload",
    "review_feedback",
    "parser_drift_diff",
    "load_monitor",
    "benchmark_pack",
    "observability_rollup",
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


@dataclass(frozen=True)
class PromotionGateResult:
    promotion_id: str
    gate_run_id: str
    gate_result_path: Path
    payload: dict[str, Any]


def run_promotion_gate(
    *,
    request_bundle_dir: Path,
    profile: EnvironmentProfile,
    activation_started_at: Optional[datetime] = None,
    gate_run_id: Optional[str] = None,
) -> PromotionGateResult:
    evaluated_at = _parse_gate_reference_time(activation_started_at)
    evaluated_at_utc = _iso_utc(evaluated_at)

    request_dir = request_bundle_dir.expanduser().resolve()
    manifest_path = request_dir / "manifest.json"
    evidence_index_path = request_dir / "evidence_index.json"

    errors: list[dict[str, Any]] = []
    stages: list[dict[str, Any]] = []
    manifest_payload: Optional[dict[str, Any]] = None
    evidence_index_payload: Optional[dict[str, Any]] = None
    normalized_manifest: Optional[dict[str, Any]] = None
    artifact_promotion_id = "unknown_promotion"

    # Stage 1: request_normalization
    stage_id = "request_normalization"
    start_errors = len(errors)
    stage_details: dict[str, Any] = {
        "request_bundle_dir": str(request_dir),
        "manifest_path": str(manifest_path),
        "evidence_index_path": str(evidence_index_path),
    }
    if not request_dir.is_dir():
        _append_gate_error(
            errors,
            code="path_safety_violation",
            message="request bundle directory must exist and be a directory.",
            stage_id=stage_id,
            path=request_dir,
        )
    manifest_payload = _load_gate_json_object(
        path=manifest_path,
        missing_code="manifest_missing_field",
        invalid_code="manifest_invalid_value",
        stage_id=stage_id,
        errors=errors,
    )
    evidence_index_payload = _load_gate_json_object(
        path=evidence_index_path,
        missing_code="evidence_missing",
        invalid_code="manifest_invalid_value",
        stage_id=stage_id,
        errors=errors,
    )
    if manifest_payload is not None:
        try:
            artifact_promotion_id = _coerce_promotion_id_for_artifact(
                str(manifest_payload.get("promotion_id", ""))
            )
        except PromotionError as exc:
            _append_gate_error(
                errors,
                code="path_safety_violation",
                message=str(exc),
                stage_id=stage_id,
                path=manifest_path,
            )
    stages.append(
        {
            "stage_id": stage_id,
            "status": "failed" if len(errors) > start_errors else "passed",
            "checked_at_utc": evaluated_at_utc,
            "details": stage_details,
        }
    )

    # Stage 2: manifest_contract_validation
    stage_id = "manifest_contract_validation"
    start_errors = len(errors)
    stage_details = {}
    if manifest_payload is None:
        _append_gate_error(
            errors,
            code="manifest_missing_field",
            message="manifest.json is required for manifest contract validation.",
            stage_id=stage_id,
            path=manifest_path,
        )
    else:
        normalized_manifest = deepcopy(manifest_payload)
        try:
            _validate_manifest_required_fields(normalized_manifest)
            _validate_required_metadata(normalized_manifest)
            _validate_pipeline_manifest_fields(normalized_manifest)
            artifact_promotion_id = _coerce_promotion_id_for_artifact(
                str(normalized_manifest.get("promotion_id", ""))
            )
        except PromotionError as exc:
            _append_gate_error(
                errors,
                code=_map_manifest_error_code(str(exc)),
                message=str(exc),
                stage_id=stage_id,
                path=manifest_path,
            )
            normalized_manifest = None
        else:
            stage_details = {
                "promotion_id": normalized_manifest.get("promotion_id"),
                "annual_refresh_impact": bool(
                    normalized_manifest.get("flags", {}).get("annual_refresh_impact")
                ),
            }
    stages.append(
        {
            "stage_id": stage_id,
            "status": "failed" if len(errors) > start_errors else "passed",
            "checked_at_utc": evaluated_at_utc,
            "details": stage_details,
        }
    )

    # Stage 3: evidence_integrity_validation
    stage_id = "evidence_integrity_validation"
    start_errors = len(errors)
    stage_details = {}
    if normalized_manifest is None or evidence_index_payload is None:
        _append_gate_error(
            errors,
            code="evidence_missing",
            message="manifest and evidence_index are required for evidence validation.",
            stage_id=stage_id,
            path=evidence_index_path,
        )
    else:
        source_environment = (
            str(normalized_manifest.get("source_environment", "")).strip().lower()
        )
        target_environment = (
            str(normalized_manifest.get("target_environment", "")).strip().lower()
        )
        evidence_window_days = (
            7
            if (source_environment, target_environment)
            == (
                "stage",
                "prod",
            )
            else 14
        )
        try:
            artifacts = _validate_pipeline_evidence_index(evidence_index_payload)
        except PromotionError as exc:
            _append_gate_error(
                errors,
                code=_map_evidence_error_code(str(exc)),
                message=str(exc),
                stage_id=stage_id,
                path=evidence_index_path,
            )
            artifacts = []
        else:
            manifest_promotion_id = str(
                normalized_manifest.get("promotion_id", "")
            ).strip()
            evidence_promotion_id_raw = str(
                evidence_index_payload.get("promotion_id", "")
            ).strip()
            try:
                evidence_promotion_id = _validate_promotion_id(
                    evidence_promotion_id_raw
                )
            except PromotionError as exc:
                _append_gate_error(
                    errors,
                    code="path_safety_violation",
                    message=str(exc),
                    stage_id=stage_id,
                    path=evidence_index_path,
                )
            else:
                if evidence_promotion_id != manifest_promotion_id:
                    _append_gate_error(
                        errors,
                        code="manifest_invalid_value",
                        message=(
                            "evidence_index.promotion_id must match "
                            "manifest.promotion_id."
                        ),
                        stage_id=stage_id,
                        path=evidence_index_path,
                    )
        entry_paths = {
            str(item.get("path", "")).strip()
            for item in artifacts
            if isinstance(item, dict)
        }
        for manifest_ref in normalized_manifest.get("evidence_artifacts", []):
            manifest_ref_str = str(manifest_ref).strip()
            if manifest_ref_str not in entry_paths:
                _append_gate_error(
                    errors,
                    code="evidence_missing",
                    message=(
                        "manifest.evidence_artifacts entry "
                        f"'{manifest_ref_str}' is missing from evidence_index."
                    ),
                    stage_id=stage_id,
                    path=evidence_index_path,
                )

        artifact_types_seen: set[str] = set()
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                _append_gate_error(
                    errors,
                    code="manifest_invalid_value",
                    message="evidence_index artifacts[] entries must be objects.",
                    stage_id=stage_id,
                    path=evidence_index_path,
                )
                continue
            missing_fields = [
                field
                for field in PIPELINE_REQUIRED_EVIDENCE_ARTIFACT_FIELDS
                if field not in artifact
            ]
            if missing_fields:
                _append_gate_error(
                    errors,
                    code="evidence_missing",
                    message=(
                        "evidence artifact missing required fields: "
                        + ", ".join(missing_fields)
                    ),
                    stage_id=stage_id,
                    path=evidence_index_path,
                )
                continue
            artifact_type = str(artifact["artifact_type"]).strip()
            artifact_types_seen.add(artifact_type)
            artifact_generated_at: Optional[datetime]
            try:
                raw_generated_at = str(artifact["generated_at_utc"]).strip()
                if not raw_generated_at.endswith("Z"):
                    _append_gate_error(
                        errors,
                        code="manifest_invalid_value",
                        message=(
                            "generated_at_utc must be an ISO-8601 UTC "
                            "timestamp ending with 'Z'."
                        ),
                        stage_id=stage_id,
                        path=evidence_index_path,
                    )
                    continue
                artifact_generated_at = _parse_iso_utc(
                    raw_generated_at,
                    field_name="generated_at_utc",
                )
            except PromotionError as exc:
                _append_gate_error(
                    errors,
                    code="manifest_invalid_value",
                    message=str(exc),
                    stage_id=stage_id,
                    path=evidence_index_path,
                )
                artifact_generated_at = None
            try:
                resolved_artifact_path = _resolve_evidence_artifact_path(
                    raw_path=str(artifact["path"]),
                    request_bundle_dir=request_dir,
                    profile=profile,
                )
            except PromotionError as exc:
                _append_gate_error(
                    errors,
                    code="path_safety_violation",
                    message=str(exc),
                    stage_id=stage_id,
                    path=evidence_index_path,
                )
                continue
            if not resolved_artifact_path.is_file():
                _append_gate_error(
                    errors,
                    code="evidence_missing",
                    message=(f"evidence artifact is missing: {resolved_artifact_path}"),
                    stage_id=stage_id,
                    path=resolved_artifact_path,
                )
                continue
            expected_sha = str(artifact["sha256"]).strip().lower()
            try:
                observed_sha = _hash_file_sha256(resolved_artifact_path)
            except (OSError, PermissionError) as exc:
                _append_gate_error(
                    errors,
                    code="evidence_missing",
                    message=(
                        "failed to read evidence artifact for hashing: "
                        f"{resolved_artifact_path}: {exc}"
                    ),
                    stage_id=stage_id,
                    path=resolved_artifact_path,
                )
                continue
            if expected_sha != observed_sha:
                _append_gate_error(
                    errors,
                    code="evidence_hash_mismatch",
                    message=(
                        f"sha256 mismatch for {resolved_artifact_path}: expected "
                        f"{expected_sha}, observed {observed_sha}."
                    ),
                    stage_id=stage_id,
                    path=resolved_artifact_path,
                )
            if artifact_generated_at is not None:
                if (
                    artifact_generated_at + timedelta(days=evidence_window_days)
                    < evaluated_at
                ):
                    _append_gate_error(
                        errors,
                        code="evidence_stale",
                        message=(
                            "evidence artifact is stale "
                            f"(>{evidence_window_days} days): "
                            f"{resolved_artifact_path}"
                        ),
                        stage_id=stage_id,
                        path=resolved_artifact_path,
                    )
            if artifact_type == "annual_signoff" and bool(
                normalized_manifest.get("flags", {}).get("annual_refresh_impact")
            ):
                annual_payload = _load_gate_json_object(
                    path=resolved_artifact_path,
                    missing_code="evidence_missing",
                    invalid_code="manifest_invalid_value",
                    stage_id=stage_id,
                    errors=errors,
                )
                if annual_payload is not None:
                    run_payload = annual_payload.get("run")
                    run_status = (
                        str(run_payload.get("status", "")).strip()
                        if isinstance(run_payload, dict)
                        else ""
                    )
                    if run_status != "approved":
                        _append_gate_error(
                            errors,
                            code="manifest_invalid_value",
                            message=(
                                "annual_signoff.run.status must be 'approved' when "
                                "flags.annual_refresh_impact is true."
                            ),
                            stage_id=stage_id,
                            path=resolved_artifact_path,
                        )

        required_types = set(PIPELINE_REQUIRED_ARTIFACT_TYPES)
        if bool(normalized_manifest.get("flags", {}).get("annual_refresh_impact")):
            required_types.add("annual_signoff")
        missing_types = sorted(required_types - artifact_types_seen)
        if missing_types:
            _append_gate_error(
                errors,
                code="evidence_missing",
                message=(
                    "missing required evidence artifact_type values: "
                    + ", ".join(missing_types)
                ),
                stage_id=stage_id,
                path=evidence_index_path,
            )
        stage_details = {
            "required_artifact_types": sorted(required_types),
            "artifact_entries": len(artifacts),
            "evidence_freshness_window_days": evidence_window_days,
        }
    stages.append(
        {
            "stage_id": stage_id,
            "status": "failed" if len(errors) > start_errors else "passed",
            "checked_at_utc": evaluated_at_utc,
            "details": stage_details,
        }
    )

    # Stage 4: approval_policy_validation
    stage_id = "approval_policy_validation"
    start_errors = len(errors)
    stage_details = {}
    if normalized_manifest is None:
        _append_gate_error(
            errors,
            code="manifest_missing_field",
            message=(
                "manifest contract validation must pass before approval validation."
            ),
            stage_id=stage_id,
            path=manifest_path,
        )
    else:
        approval_manifest = deepcopy(normalized_manifest)
        try:
            _validate_approvals_for_activation(
                manifest=approval_manifest,
                activation_started_at=evaluated_at,
            )
        except PromotionError as exc:
            _append_gate_error(
                errors,
                code=_map_approval_error_code(str(exc)),
                message=str(exc),
                stage_id=stage_id,
                path=manifest_path,
            )
        else:
            normalized_manifest["approvals"] = approval_manifest.get("approvals", [])
            valid_approvals = _summarize_valid_approvals(
                approvals=approval_manifest.get("approvals", []),
                activation_started_at=evaluated_at,
            )
            stage_details = {
                "approval_count": len(approval_manifest.get("approvals", [])),
                "valid_approval_count": len(valid_approvals),
                "valid_approvers": sorted(
                    {item["approved_by"] for item in valid_approvals}
                ),
                "valid_roles": sorted(
                    {item["approver_role"] for item in valid_approvals}
                ),
            }
    stages.append(
        {
            "stage_id": stage_id,
            "status": "failed" if len(errors) > start_errors else "passed",
            "checked_at_utc": evaluated_at_utc,
            "details": stage_details,
        }
    )

    # Stage 5: freeze_policy_validation
    stage_id = "freeze_policy_validation"
    start_errors = len(errors)
    stage_details = {}
    if normalized_manifest is None:
        _append_gate_error(
            errors,
            code="manifest_missing_field",
            message="manifest contract validation must pass before freeze validation.",
            stage_id=stage_id,
            path=manifest_path,
        )
    else:
        freeze_manifest = deepcopy(normalized_manifest)
        freeze_state = "none"
        freeze_file_read_ok = True
        freeze_payload: Optional[dict[str, Any]] = None
        try:
            freeze_payload = _read_json_if_exists(profile.promotion_freeze_file)
        except PromotionError as exc:
            _append_gate_error(
                errors,
                code="break_glass_invalid",
                message=str(exc),
                stage_id=stage_id,
                path=profile.promotion_freeze_file,
            )
            freeze_file_read_ok = False
            freeze_state = "invalid"
        else:
            if freeze_payload is not None:
                freeze_state = (
                    str(freeze_payload.get("state", "none")).strip() or "none"
                )
        if freeze_file_read_ok:
            try:
                _enforce_freeze_policy(
                    manifest=freeze_manifest,
                    freeze_file_path=profile.promotion_freeze_file,
                    activation_started_at_utc=evaluated_at_utc,
                    freeze_payload=freeze_payload,
                )
            except PromotionError as exc:
                _append_gate_error(
                    errors,
                    code=_map_freeze_error_code(str(exc)),
                    message=str(exc),
                    stage_id=stage_id,
                    path=profile.promotion_freeze_file,
                )
            else:
                normalized_manifest["break_glass_validated_at_utc"] = (
                    freeze_manifest.get("break_glass_validated_at_utc")
                )
        stage_details = {
            "freeze_state": freeze_state,
            "break_glass_used": bool(normalized_manifest.get("break_glass_used")),
        }
    stages.append(
        {
            "stage_id": stage_id,
            "status": "failed" if len(errors) > start_errors else "passed",
            "checked_at_utc": evaluated_at_utc,
            "details": stage_details,
        }
    )

    # Stage 6: activation_readiness_validation
    stage_id = "activation_readiness_validation"
    start_errors = len(errors)
    stage_details = {}
    if normalized_manifest is None:
        _append_gate_error(
            errors,
            code="manifest_missing_field",
            message=(
                "manifest contract validation must pass before activation readiness "
                "validation."
            ),
            stage_id=stage_id,
            path=manifest_path,
        )
    else:
        readiness_manifest = deepcopy(normalized_manifest)
        try:
            _validate_source_target_matrix(manifest=readiness_manifest, profile=profile)
        except PromotionError as exc:
            _append_gate_error(
                errors,
                code=_map_path_policy_error_code(str(exc)),
                message=str(exc),
                stage_id=stage_id,
                path=manifest_path,
            )
        promotion_id = str(readiness_manifest.get("promotion_id", "")).strip()
        try:
            promotion_id = _validate_promotion_id(promotion_id)
        except PromotionError as exc:
            _append_gate_error(
                errors,
                code="path_safety_violation",
                message=str(exc),
                stage_id=stage_id,
                path=manifest_path,
            )
            promotion_registry_slot_available = False
        else:
            promotion_root = profile.promotion_registry_root / promotion_id
            promotion_registry_slot_available = not promotion_root.exists()
            if promotion_root.exists():
                _append_gate_error(
                    errors,
                    code="manifest_invalid_value",
                    message=(
                        "promotion registry already contains this promotion_id; "
                        "activation is not ready."
                    ),
                    stage_id=stage_id,
                    path=promotion_root,
                )
        stage_details = {
            "promotion_registry_root": str(profile.promotion_registry_root),
            "promotion_registry_slot_available": promotion_registry_slot_available,
        }
    stages.append(
        {
            "stage_id": stage_id,
            "status": "failed" if len(errors) > start_errors else "passed",
            "checked_at_utc": evaluated_at_utc,
            "details": stage_details,
        }
    )

    resolved_gate_run_id = _build_default_gate_run_id(evaluated_at)
    if gate_run_id is not None:
        try:
            resolved_gate_run_id = _validate_safe_path_component(
                gate_run_id, field_name="gate_run_id"
            )
        except PromotionError as exc:
            _append_gate_error(
                errors,
                code="path_safety_violation",
                message=str(exc),
                stage_id="request_normalization",
                path=request_dir,
            )
            stages[0]["status"] = "failed"
            stages[0]["details"]["gate_run_id_validation"] = "invalid_fallback_applied"
    status = (
        "failed" if any(stage["status"] == "failed" for stage in stages) else "passed"
    )

    request_payload = normalized_manifest or manifest_payload or {}
    payload = {
        "gate": {
            "contract_version": PROMOTION_PIPELINE_CONTRACT_VERSION,
            "gate_run_id": resolved_gate_run_id,
            "evaluated_at_utc": evaluated_at_utc,
            "environment": profile.environment_name,
            "status": status,
        },
        "request": {
            "promotion_id": str(request_payload.get("promotion_id", "")).strip(),
            "source_environment": str(
                request_payload.get("source_environment", "")
            ).strip(),
            "target_environment": str(
                request_payload.get("target_environment", "")
            ).strip(),
            "manifest_path": str(manifest_path),
            "activation_started_at_utc": evaluated_at_utc,
        },
        "stages": stages,
        "summary": {
            "total_stages": len(PROMOTION_PIPELINE_STAGE_IDS),
            "failed_stages": sum(1 for stage in stages if stage["status"] == "failed"),
            "blocking_error_count": len(errors),
        },
        "errors": errors,
    }

    gate_result_path = (
        profile.artifact_base_dir
        / "promotion_gate_results"
        / artifact_promotion_id
        / f"{resolved_gate_run_id}.json"
    )
    _write_json_atomic(gate_result_path, payload)

    return PromotionGateResult(
        promotion_id=artifact_promotion_id,
        gate_run_id=resolved_gate_run_id,
        gate_result_path=gate_result_path,
        payload=payload,
    )


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
    activated_by_normalized = str(activated_by).strip()
    if not activated_by_normalized:
        raise PromotionError("activated_by must be a non-empty string.")

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
    if promotion_root.exists():
        raise PromotionError(
            f"promotion_id '{promotion_id}' already exists in promotion registry."
        )
    promotion_root.mkdir(parents=True, exist_ok=False)

    active_selector_path = registry_root / "active_selectors.json"
    rollback_reference = _read_json_if_exists(active_selector_path)

    manifest["activation_started_at_utc"] = activation_started_at_utc
    manifest["activation_state"] = "succeeded"
    manifest["activated_by"] = activated_by_normalized
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
            "activated_by": activated_by_normalized,
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
    if manifest["activation_started_at_utc"] is not None:
        raise PromotionError(
            "activation_started_at_utc must be null while activation_state is "
            "'not_started'."
        )
    if manifest["activated_at_utc"] is not None:
        raise PromotionError(
            "activated_at_utc must be null while activation_state is 'not_started'."
        )
    if manifest["activated_by"] is not None:
        raise PromotionError(
            "activated_by must be null while activation_state is 'not_started'."
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
    source_run_id = _validate_safe_path_component(
        str(manifest["source_run_id"]),
        field_name="source_run_id",
    )
    manifest["source_run_id"] = source_run_id
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
    if target_run_id is not None:
        target_run_id = str(target_run_id).strip()
        if not target_run_id:
            raise PromotionError("target_run_id must be null or a non-empty string.")
        manifest["target_run_id"] = _validate_safe_path_component(
            target_run_id,
            field_name="target_run_id",
        )

    freeze_override_note = manifest["freeze_override_note"]
    if freeze_override_note is not None:
        freeze_override_note = str(freeze_override_note).strip()
        if not freeze_override_note:
            raise PromotionError(
                "freeze_override_note must be null or a non-empty string."
            )
        manifest["freeze_override_note"] = freeze_override_note

    break_glass_used = manifest["break_glass_used"]
    if not isinstance(break_glass_used, bool):
        raise PromotionError("break_glass_used must be a boolean.")
    break_glass_incident_id = manifest["break_glass_incident_id"]
    if break_glass_incident_id is not None:
        break_glass_incident_id = str(break_glass_incident_id).strip()
        if not break_glass_incident_id:
            raise PromotionError(
                "break_glass_incident_id must be null or a non-empty string."
            )
        manifest["break_glass_incident_id"] = break_glass_incident_id
    if break_glass_used and break_glass_incident_id is None:
        raise PromotionError(
            "break_glass_incident_id is required when break_glass_used is true."
        )

    rollback_reference = manifest["rollback_reference"]
    if rollback_reference is not None and not isinstance(rollback_reference, dict):
        raise PromotionError("rollback_reference must be null or an object.")


def _validate_approvals_for_activation(
    *, manifest: dict[str, Any], activation_started_at: datetime
) -> None:
    approvals = manifest.get("approvals")
    if not isinstance(approvals, list):
        raise PromotionError("approvals must be a list of approval records.")
    source_environment = str(manifest["source_environment"]).strip().lower()
    target_environment = str(manifest["target_environment"]).strip().lower()
    manifest["source_environment"] = source_environment
    manifest["target_environment"] = target_environment
    requester = str(manifest["requested_by"])

    normalized_approvals: list[dict[str, str]] = []
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
        normalized_approval = {
            "approved_by": approved_by,
            "approved_at_utc": _iso_utc(approved_at),
            "approver_role": approver_role,
        }
        normalized_approvals.append(normalized_approval)
        if approved_at + timedelta(hours=24) <= activation_started_at:
            continue
        valid_approvals.append(normalized_approval)

    manifest["approvals"] = normalized_approvals

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
    freeze_payload: Optional[dict[str, Any]] = None,
) -> None:
    if freeze_payload is None:
        freeze_payload = _read_json_if_exists(freeze_file_path)
    freeze_payload = freeze_payload or {"state": "none"}
    state = str(freeze_payload.get("state", "none"))
    if state not in {"none", "advisory", "hard"}:
        raise PromotionError("freeze file state must be one of: none, advisory, hard.")

    if state == "advisory":
        if not str(manifest.get("freeze_override_note", "")).strip():
            raise PromotionError(
                "advisory freeze requires freeze_override_note in manifest."
            )
    if state != "hard" and manifest.get("break_glass_used") is True:
        raise PromotionError(
            "break_glass_used may be true only when the freeze file state is hard."
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


def _parse_gate_reference_time(value: Optional[datetime]) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        raise PromotionError("activation_started_at must include timezone information.")
    return value.astimezone(timezone.utc)


def _load_gate_json_object(
    *,
    path: Path,
    missing_code: str,
    invalid_code: str,
    stage_id: str,
    errors: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if not path.is_file():
        _append_gate_error(
            errors,
            code=missing_code,
            message=f"required file is missing: {path}",
            stage_id=stage_id,
            path=path,
        )
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        _append_gate_error(
            errors,
            code=invalid_code,
            message=f"invalid JSON in {path}",
            stage_id=stage_id,
            path=path,
        )
        return None
    except (OSError, UnicodeDecodeError) as exc:
        _append_gate_error(
            errors,
            code=invalid_code,
            message=f"failed to read {path}: {exc}",
            stage_id=stage_id,
            path=path,
        )
        return None
    if not isinstance(payload, dict):
        _append_gate_error(
            errors,
            code=invalid_code,
            message=f"expected JSON object payload in {path}",
            stage_id=stage_id,
            path=path,
        )
        return None
    return payload


def _append_gate_error(
    errors: list[dict[str, Any]],
    *,
    code: str,
    message: str,
    stage_id: str,
    path: Optional[Path] = None,
) -> None:
    errors.append(
        {
            "code": code,
            "message": message,
            "stage_id": stage_id,
            "path": str(path) if path is not None else None,
        }
    )


def _validate_pipeline_manifest_fields(manifest: dict[str, Any]) -> None:
    missing = [key for key in PIPELINE_REQUIRED_MANIFEST_FIELDS if key not in manifest]
    if missing:
        raise PromotionError(
            "Promotion manifest missing required fields: " + ", ".join(missing)
        )

    contract_version = str(manifest.get("contract_version", "")).strip()
    if contract_version != PROMOTION_PIPELINE_CONTRACT_VERSION:
        raise PromotionError(
            "contract_version must be 'promotion_pipeline_v1' for promotion gate v1."
        )
    manifest["contract_version"] = contract_version

    source_commit_sha = str(manifest.get("source_commit_sha", "")).strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", source_commit_sha):
        raise PromotionError("source_commit_sha must be a 40-character lowercase SHA.")
    manifest["source_commit_sha"] = source_commit_sha

    source_pr_number = manifest.get("source_pr_number")
    if not isinstance(source_pr_number, int) or source_pr_number <= 0:
        raise PromotionError("source_pr_number must be a positive integer.")

    change_summary = str(manifest.get("change_summary", "")).strip()
    if not change_summary:
        raise PromotionError("change_summary must be a non-empty string.")
    manifest["change_summary"] = change_summary

    flags = manifest.get("flags")
    if not isinstance(flags, dict):
        raise PromotionError("flags must be an object.")
    annual_refresh_impact = flags.get("annual_refresh_impact")
    if not isinstance(annual_refresh_impact, bool):
        raise PromotionError("flags.annual_refresh_impact must be a boolean.")


def _validate_pipeline_evidence_index(
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    missing = [
        key for key in PIPELINE_REQUIRED_EVIDENCE_INDEX_FIELDS if key not in payload
    ]
    if missing:
        raise PromotionError(
            "evidence_index missing required fields: " + ", ".join(missing)
        )
    contract_version = str(payload.get("contract_version", "")).strip()
    if contract_version != PROMOTION_PIPELINE_CONTRACT_VERSION:
        raise PromotionError(
            "evidence_index.contract_version must be 'promotion_pipeline_v1'."
        )
    generated_at_utc = str(payload.get("generated_at_utc", "")).strip()
    if not generated_at_utc.endswith("Z"):
        raise PromotionError(
            "evidence_index.generated_at_utc must be a UTC timestamp ending with 'Z'."
        )
    _parse_iso_utc(generated_at_utc, field_name="generated_at_utc")
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        raise PromotionError("evidence_index.artifacts must be a list.")
    return artifacts


def _map_manifest_error_code(message: str) -> str:
    lowered = message.lower()
    if "missing required fields" in lowered:
        return "manifest_missing_field"
    if (
        "path traversal" in lowered
        or "absolute path" in lowered
        or "single filesystem path component" in lowered
        or "unsupported path characters" in lowered
    ):
        return "path_safety_violation"
    return "manifest_invalid_value"


def _map_evidence_error_code(message: str) -> str:
    lowered = message.lower()
    if "missing required fields" in lowered or "must be a list" in lowered:
        return "evidence_missing"
    return "manifest_invalid_value"


def _map_approval_error_code(message: str) -> str:
    lowered = message.lower()
    if "self-approval" in lowered:
        return "approval_self_approval"
    if (
        "approver_role must be one of" in lowered
        or "must include one analyst_owner" in lowered
    ):
        return "approval_role_missing"
    return "approval_insufficient"


def _map_freeze_error_code(message: str) -> str:
    lowered = message.lower()
    if (
        "advisory freeze requires" in lowered
        or "hard freeze blocks activation" in lowered
    ):
        return "freeze_override_required"
    return "break_glass_invalid"


def _map_path_policy_error_code(message: str) -> str:
    lowered = message.lower()
    if "unsupported promotion path" in lowered:
        return "promotion_path_not_allowed"
    if "target_environment must match" in lowered:
        return "target_environment_mismatch"
    return "manifest_invalid_value"


def _resolve_evidence_artifact_path(
    *,
    raw_path: str,
    request_bundle_dir: Path,
    profile: EnvironmentProfile,
) -> Path:
    candidate = raw_path.strip()
    if not candidate:
        raise PromotionError("evidence artifact path is required.")
    parsed = Path(candidate)
    if parsed.is_absolute():
        resolved = parsed.resolve()
    else:
        resolved = (profile.artifact_base_dir / parsed).resolve()
    allowed_roots = (
        profile.artifact_base_dir.resolve(),
        profile.refresh_log_dir.resolve(),
        profile.benchmark_base_dir.resolve(),
        profile.promotion_registry_root.resolve(),
        request_bundle_dir.resolve(),
    )
    if not any(
        _path_is_within_root(path=resolved, root=root) for root in allowed_roots
    ):
        raise PromotionError(
            "evidence artifact path must resolve under active environment "
            "artifact roots."
        )
    return resolved


def _path_is_within_root(*, path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _hash_file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _coerce_promotion_id_for_artifact(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return "unknown_promotion"
    return _validate_promotion_id(candidate)


def _build_default_gate_run_id(evaluated_at: datetime) -> str:
    return f"gate_{evaluated_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def _summarize_valid_approvals(
    *, approvals: list[dict[str, Any]], activation_started_at: datetime
) -> list[dict[str, str]]:
    valid_approvals: list[dict[str, str]] = []
    for approval in approvals:
        approved_at_raw = str(approval.get("approved_at_utc", "")).strip()
        try:
            approved_at = _parse_iso_utc(approved_at_raw, field_name="approved_at_utc")
        except PromotionError:
            continue
        if approved_at + timedelta(hours=24) <= activation_started_at:
            continue
        valid_approvals.append(
            {
                "approved_by": str(approval.get("approved_by", "")).strip(),
                "approved_at_utc": _iso_utc(approved_at),
                "approver_role": str(approval.get("approver_role", "")).strip(),
            }
        )
    return valid_approvals


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
    return _validate_safe_path_component(value, field_name="promotion_id")


def _validate_safe_path_component(value: str, *, field_name: str) -> str:
    component = value.strip()
    if not component:
        raise PromotionError(f"{field_name} is required.")
    component_path = Path(component)
    if component_path.is_absolute():
        raise PromotionError(f"{field_name} must not be an absolute path.")
    if component in {".", ".."} or ".." in component:
        raise PromotionError(f"{field_name} must not contain path traversal segments.")
    if "/" in component or "\\" in component:
        raise PromotionError(
            f"{field_name} must be a single filesystem path component."
        )
    if not SAFE_PATH_SEGMENT_RE.fullmatch(component):
        raise PromotionError(
            f"{field_name} contains unsupported path characters; only letters, "
            "digits, '.', '_' and '-' are allowed."
        )
    return component
