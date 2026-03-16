from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, TypedDict

RUN_TYPE_REFRESH_AUTOMATION = "refresh_automation"
REFRESH_AUTOMATION_VERSION_TAG = "refresh_automation_v1"
_SAFE_PATH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")

CANONICAL_STAGES: tuple[str, ...] = (
    "ingest_context",
    "build_context",
    "score_pipeline",
    "analysis_artifacts",
    "investigation_artifacts",
    "health_summary",
)
SUPPORTED_PROFILES: dict[str, tuple[str, ...]] = {
    "daily_refresh": CANONICAL_STAGES,
    "analysis_only": (
        "analysis_artifacts",
        "investigation_artifacts",
        "health_summary",
    ),
}

StageStatus = str
CommandStatus = str
CommandExecutor = Callable[[list[str]], int]


class RefreshRun(TypedDict):
    run_type: str
    version_tag: str
    run_id: str
    profile_name: str
    status: str
    run_persisted: bool
    started_at: str
    finished_at: str


class RefreshRequestSourceFiles(TypedDict):
    retr: Optional[str]
    permits: Optional[str]
    appeals: Optional[str]


class RefreshRequest(TypedDict):
    profile_name: str
    run_date: str
    feature_version: str
    ruleset_version: str
    sales_ratio_base: str
    top: int
    source_files: RefreshRequestSourceFiles


class RefreshSummary(TypedDict):
    stage_count: int
    stage_succeeded_count: int
    stage_failed_count: int
    stage_blocked_count: int
    stage_skipped_count: int
    duration_seconds_total: float


class RefreshCommandResult(TypedDict):
    command_id: str
    status: CommandStatus
    exit_code: Optional[int]
    artifact_paths: list[str]
    error_code: Optional[str]


class RefreshStage(TypedDict):
    stage_id: str
    status: StageStatus
    started_at: Optional[str]
    finished_at: Optional[str]
    duration_seconds: Optional[float]
    attempt: int
    command_results: list[RefreshCommandResult]


class RefreshArtifacts(TypedDict):
    root_path: str
    latest_pointer_path: str
    stage_artifacts: dict[str, list[str]]


class RefreshSkipReason(TypedDict):
    stage_id: str
    command_id: str
    reason: str


class RefreshRetry(TypedDict):
    attempt_count: int
    retried_from_stage_id: Optional[str]


class RefreshDiagnostics(TypedDict):
    warnings: list[str]
    skip_reasons: list[RefreshSkipReason]
    retry: RefreshRetry


class RefreshError(TypedDict):
    code: str
    message: str
    failed_stage_id: Optional[str]


class RefreshPayload(TypedDict):
    run: RefreshRun
    request: RefreshRequest
    summary: RefreshSummary
    stages: list[RefreshStage]
    artifacts: RefreshArtifacts
    diagnostics: RefreshDiagnostics
    error: Optional[RefreshError]


@dataclass(frozen=True)
class _CommandSpec:
    stage_id: str
    command_id: str
    command: list[str]
    artifact_paths: list[str]
    skip_reason: Optional[str] = None


def run_scheduled_refresh(
    *,
    profile_name: str,
    run_date: str,
    run_id: str,
    feature_version: str,
    ruleset_version: str,
    sales_ratio_base: str,
    top: int,
    retr_file: Optional[Path],
    permits_file: Optional[Path],
    appeals_file: Optional[Path],
    artifact_base_dir: Path,
    accessdane_bin: str,
    attempt_count: int = 1,
    retried_from_stage_id: Optional[str] = None,
    command_executor: Optional[CommandExecutor] = None,
) -> RefreshPayload:
    started_dt = _now_utc()
    context_error = _validate_run_context(
        run_date=run_date,
        profile_name=profile_name,
        run_id=run_id,
        artifact_base_dir=artifact_base_dir,
    )
    root_path = artifact_base_dir / run_date / profile_name / run_id
    stage_artifacts: dict[str, list[str]] = {
        stage_id: [] for stage_id in CANONICAL_STAGES
    }
    stages: list[RefreshStage] = []
    diagnostics: RefreshDiagnostics = {
        "warnings": [],
        "skip_reasons": [],
        "retry": {
            "attempt_count": attempt_count,
            "retried_from_stage_id": retried_from_stage_id,
        },
    }
    request: RefreshRequest = {
        "profile_name": profile_name,
        "run_date": run_date,
        "feature_version": feature_version,
        "ruleset_version": ruleset_version,
        "sales_ratio_base": sales_ratio_base,
        "top": top,
        "source_files": {
            "retr": str(retr_file) if retr_file is not None else None,
            "permits": str(permits_file) if permits_file is not None else None,
            "appeals": str(appeals_file) if appeals_file is not None else None,
        },
    }
    if context_error is not None:
        stages = [
            _blocked_stage(stage_id=stage_id, attempt=1)
            for stage_id in CANONICAL_STAGES
        ]
        finished_dt = _now_utc()
        return _build_payload(
            run_id=run_id,
            profile_name=profile_name,
            request=request,
            started_dt=started_dt,
            finished_dt=finished_dt,
            stages=stages,
            stage_artifacts=stage_artifacts,
            diagnostics=diagnostics,
            root_path=root_path,
            artifact_base_dir=artifact_base_dir,
            feature_version=feature_version,
            ruleset_version=ruleset_version,
            run_status="failed",
            error={
                "code": "invalid_run_context",
                "message": context_error,
                "failed_stage_id": None,
            },
        )

    selected_stages = SUPPORTED_PROFILES.get(profile_name)
    if selected_stages is None:
        stages = [
            _blocked_stage(stage_id=stage_id, attempt=1)
            for stage_id in CANONICAL_STAGES
        ]
        finished_dt = _now_utc()
        return _build_payload(
            run_id=run_id,
            profile_name=profile_name,
            request=request,
            started_dt=started_dt,
            finished_dt=finished_dt,
            stages=stages,
            stage_artifacts=stage_artifacts,
            diagnostics=diagnostics,
            root_path=root_path,
            artifact_base_dir=artifact_base_dir,
            feature_version=feature_version,
            ruleset_version=ruleset_version,
            run_status="failed",
            error={
                "code": "unsupported_profile",
                "message": f"Unsupported profile_name '{profile_name}' for v1 runner.",
                "failed_stage_id": None,
            },
        )
    retry_error = _validate_retry_context(
        attempt_count=attempt_count,
        retried_from_stage_id=retried_from_stage_id,
        selected_stages=selected_stages,
    )
    if retry_error is not None:
        stages = [
            _blocked_stage(stage_id=stage_id, attempt=1)
            for stage_id in CANONICAL_STAGES
        ]
        finished_dt = _now_utc()
        return _build_payload(
            run_id=run_id,
            profile_name=profile_name,
            request=request,
            started_dt=started_dt,
            finished_dt=finished_dt,
            stages=stages,
            stage_artifacts=stage_artifacts,
            diagnostics=diagnostics,
            root_path=root_path,
            artifact_base_dir=artifact_base_dir,
            feature_version=feature_version,
            ruleset_version=ruleset_version,
            run_status="failed",
            error={
                "code": "invalid_retry_boundary",
                "message": retry_error,
                "failed_stage_id": None,
            },
        )

    lock_path = _acquire_profile_lock(
        artifact_base_dir=artifact_base_dir,
        profile_name=profile_name,
        started_dt=started_dt,
    )
    if lock_path is None:
        stages = [
            _blocked_stage(stage_id=stage_id, attempt=1)
            for stage_id in CANONICAL_STAGES
        ]
        finished_dt = _now_utc()
        return _build_payload(
            run_id=run_id,
            profile_name=profile_name,
            request=request,
            started_dt=started_dt,
            finished_dt=finished_dt,
            stages=stages,
            stage_artifacts=stage_artifacts,
            diagnostics=diagnostics,
            root_path=root_path,
            artifact_base_dir=artifact_base_dir,
            feature_version=feature_version,
            ruleset_version=ruleset_version,
            run_status="failed",
            error={
                "code": "overlapping_run",
                "message": (
                    "Another refresh run is already active for this profile "
                    f"(lock: {artifact_base_dir / 'locks' / f'{profile_name}.lock'})."
                ),
                "failed_stage_id": None,
            },
        )

    executor = command_executor or _default_command_executor
    in_progress_marker: Optional[Path] = None
    encountered_failure = False
    failed_stage_id: Optional[str] = None
    failure_message = "Refresh automation stage execution failed."

    try:
        root_path.mkdir(parents=True, exist_ok=True)
        in_progress_marker = root_path / ".in_progress"
        in_progress_marker.write_text(_iso_utc(started_dt), encoding="utf-8")

        command_specs = _build_command_specs(
            accessdane_bin=accessdane_bin,
            root_path=root_path,
            run_date=run_date,
            profile_name=profile_name,
            sales_ratio_base=sales_ratio_base,
            feature_version=feature_version,
            ruleset_version=ruleset_version,
            top=top,
            retr_file=retr_file,
            permits_file=permits_file,
            appeals_file=appeals_file,
        )

        latest_pass_stages = _latest_pass_stages(
            attempt_count=attempt_count,
            retried_from_stage_id=retried_from_stage_id,
            selected_stages=selected_stages,
        )

        for stage_id in CANONICAL_STAGES:
            if stage_id not in selected_stages:
                stages.append(_skipped_stage(stage_id=stage_id, attempt=1))
                diagnostics["skip_reasons"].append(
                    {
                        "stage_id": stage_id,
                        "command_id": "_stage_profile_selection",
                        "reason": f"profile_skip:{profile_name}",
                    }
                )
                continue

            if attempt_count > 1 and stage_id not in latest_pass_stages:
                stages.append(_skipped_stage(stage_id=stage_id, attempt=1))
                diagnostics["skip_reasons"].append(
                    {
                        "stage_id": stage_id,
                        "command_id": "_stage_retry_boundary",
                        "reason": f"retry_boundary_before:{retried_from_stage_id}",
                    }
                )
                continue

            if encountered_failure and stage_id != "health_summary":
                stages.append(_blocked_stage(stage_id=stage_id, attempt=1))
                continue

            stage_attempt = attempt_count
            stage_dir = root_path / stage_id
            stage_dir.mkdir(parents=True, exist_ok=True)
            stage_started = _now_utc()
            command_results: list[RefreshCommandResult] = []
            stage_failed = False

            for spec in command_specs.get(stage_id, []):
                if spec.skip_reason is not None:
                    command_results.append(
                        {
                            "command_id": spec.command_id,
                            "status": "skipped",
                            "exit_code": None,
                            "artifact_paths": [],
                            "error_code": None,
                        }
                    )
                    diagnostics["skip_reasons"].append(
                        {
                            "stage_id": spec.stage_id,
                            "command_id": spec.command_id,
                            "reason": spec.skip_reason,
                        }
                    )
                    continue

                exit_code = executor(spec.command)
                if exit_code == 0:
                    command_results.append(
                        {
                            "command_id": spec.command_id,
                            "status": "succeeded",
                            "exit_code": 0,
                            "artifact_paths": list(spec.artifact_paths),
                            "error_code": None,
                        }
                    )
                    stage_artifacts[stage_id].extend(spec.artifact_paths)
                else:
                    command_results.append(
                        {
                            "command_id": spec.command_id,
                            "status": "failed",
                            "exit_code": exit_code,
                            "artifact_paths": [],
                            "error_code": "command_failed",
                        }
                    )
                    stage_failed = True
                    if failed_stage_id is None:
                        failed_stage_id = stage_id
                        failure_message = (
                            f"Command '{spec.command_id}' failed in stage '{stage_id}'."
                        )
                    break

            stage_finished = _now_utc()
            duration_seconds = _duration_seconds(stage_started, stage_finished)
            stage_status: StageStatus = "failed" if stage_failed else "succeeded"
            stages.append(
                {
                    "stage_id": stage_id,
                    "status": stage_status,
                    "started_at": _iso_utc(stage_started),
                    "finished_at": _iso_utc(stage_finished),
                    "duration_seconds": duration_seconds,
                    "attempt": stage_attempt,
                    "command_results": command_results,
                }
            )

            if stage_failed:
                encountered_failure = True

        finished_dt = _now_utc()
        run_status = "failed" if failed_stage_id is not None else "succeeded"
        error: Optional[RefreshError]
        if failed_stage_id is None:
            error = None
        else:
            error = {
                "code": "stage_failure",
                "message": failure_message,
                "failed_stage_id": failed_stage_id,
            }

        payload = _build_payload(
            run_id=run_id,
            profile_name=profile_name,
            request=request,
            started_dt=started_dt,
            finished_dt=finished_dt,
            stages=stages,
            stage_artifacts=stage_artifacts,
            diagnostics=diagnostics,
            root_path=root_path,
            artifact_base_dir=artifact_base_dir,
            feature_version=feature_version,
            ruleset_version=ruleset_version,
            run_status=run_status,
            error=error,
        )
        _persist_run_artifacts(payload)
        return payload
    finally:
        if in_progress_marker is not None:
            _safe_unlink(in_progress_marker)
        if lock_path is not None:
            _release_profile_lock(lock_path)


def _default_command_executor(command: list[str]) -> int:
    completed = subprocess.run(command, check=False)
    return int(completed.returncode)


def _build_command_specs(
    *,
    accessdane_bin: str,
    root_path: Path,
    run_date: str,
    profile_name: str,
    sales_ratio_base: str,
    feature_version: str,
    ruleset_version: str,
    top: int,
    retr_file: Optional[Path],
    permits_file: Optional[Path],
    appeals_file: Optional[Path],
) -> dict[str, list[_CommandSpec]]:
    sales_ratio_version_tag = f"{profile_name}_{run_date}_{sales_ratio_base}"
    analysis_dir = root_path / "analysis_artifacts"
    investigation_dir = root_path / "investigation_artifacts"
    score_dir = root_path / "score_pipeline"

    retr_arg = str(retr_file) if retr_file is not None else None
    permits_arg = str(permits_file) if permits_file is not None else None
    appeals_arg = str(appeals_file) if appeals_file is not None else None

    return {
        "ingest_context": [
            _CommandSpec(
                stage_id="ingest_context",
                command_id="ingest_retr",
                command=[accessdane_bin, "ingest-retr", "--file", retr_arg or ""],
                artifact_paths=[],
                skip_reason=(
                    None if retr_arg is not None else "missing_source_file:retr"
                ),
            ),
            _CommandSpec(
                stage_id="ingest_context",
                command_id="match_sales",
                command=[accessdane_bin, "match-sales"],
                artifact_paths=[],
                skip_reason=(
                    None if retr_arg is not None else "missing_source_file:retr"
                ),
            ),
            _CommandSpec(
                stage_id="ingest_context",
                command_id="ingest_permits",
                command=[accessdane_bin, "ingest-permits", "--file", permits_arg or ""],
                artifact_paths=[],
                skip_reason=(
                    None if permits_arg is not None else "missing_source_file:permits"
                ),
            ),
            _CommandSpec(
                stage_id="ingest_context",
                command_id="ingest_appeals",
                command=[accessdane_bin, "ingest-appeals", "--file", appeals_arg or ""],
                artifact_paths=[],
                skip_reason=(
                    None if appeals_arg is not None else "missing_source_file:appeals"
                ),
            ),
        ],
        "build_context": [
            _CommandSpec(
                stage_id="build_context",
                command_id="build_parcel_year_facts",
                command=[accessdane_bin, "build-parcel-year-facts"],
                artifact_paths=[],
            )
        ],
        "score_pipeline": [
            _CommandSpec(
                stage_id="score_pipeline",
                command_id="sales_ratio_study",
                command=[
                    accessdane_bin,
                    "sales-ratio-study",
                    "--version-tag",
                    sales_ratio_version_tag,
                    "--out",
                    str(score_dir / "sales_ratio_study.json"),
                ],
                artifact_paths=[str(score_dir / "sales_ratio_study.json")],
            ),
            _CommandSpec(
                stage_id="score_pipeline",
                command_id="build_features",
                command=[
                    accessdane_bin,
                    "build-features",
                    "--feature-version",
                    feature_version,
                    "--out",
                    str(score_dir / "build_features.json"),
                ],
                artifact_paths=[str(score_dir / "build_features.json")],
            ),
            _CommandSpec(
                stage_id="score_pipeline",
                command_id="score_fraud",
                command=[
                    accessdane_bin,
                    "score-fraud",
                    "--feature-version",
                    feature_version,
                    "--ruleset-version",
                    ruleset_version,
                    "--out",
                    str(score_dir / "score_fraud.json"),
                ],
                artifact_paths=[str(score_dir / "score_fraud.json")],
            ),
        ],
        "analysis_artifacts": [
            _CommandSpec(
                stage_id="analysis_artifacts",
                command_id="review_queue",
                command=[
                    accessdane_bin,
                    "review-queue",
                    "--top",
                    str(top),
                    "--feature-version",
                    feature_version,
                    "--ruleset-version",
                    ruleset_version,
                    "--out",
                    str(analysis_dir / "review_queue.json"),
                    "--csv-out",
                    str(analysis_dir / "review_queue.csv"),
                ],
                artifact_paths=[
                    str(analysis_dir / "review_queue.json"),
                    str(analysis_dir / "review_queue.csv"),
                ],
            ),
            _CommandSpec(
                stage_id="analysis_artifacts",
                command_id="review_feedback",
                command=[
                    accessdane_bin,
                    "review-feedback",
                    "--feature-version",
                    feature_version,
                    "--ruleset-version",
                    ruleset_version,
                    "--out",
                    str(analysis_dir / "review_feedback.json"),
                    "--sql-out",
                    str(analysis_dir / "review_feedback.sql"),
                ],
                artifact_paths=[
                    str(analysis_dir / "review_feedback.json"),
                    str(analysis_dir / "review_feedback.sql"),
                ],
            ),
        ],
        "investigation_artifacts": [
            _CommandSpec(
                stage_id="investigation_artifacts",
                command_id="investigation_report",
                command=[
                    accessdane_bin,
                    "investigation-report",
                    "--top",
                    str(top),
                    "--feature-version",
                    feature_version,
                    "--ruleset-version",
                    ruleset_version,
                    "--html-out",
                    str(investigation_dir / "investigation_report.html"),
                    "--out",
                    str(investigation_dir / "investigation_report.json"),
                ],
                artifact_paths=[
                    str(investigation_dir / "investigation_report.html"),
                    str(investigation_dir / "investigation_report.json"),
                ],
            )
        ],
        "health_summary": [],
    }


def _build_payload(
    *,
    run_id: str,
    profile_name: str,
    request: RefreshRequest,
    started_dt: datetime,
    finished_dt: datetime,
    stages: list[RefreshStage],
    stage_artifacts: dict[str, list[str]],
    diagnostics: RefreshDiagnostics,
    root_path: Path,
    artifact_base_dir: Path,
    feature_version: str,
    ruleset_version: str,
    run_status: str,
    error: Optional[RefreshError],
) -> RefreshPayload:
    succeeded_count = sum(1 for stage in stages if stage["status"] == "succeeded")
    failed_count = sum(1 for stage in stages if stage["status"] == "failed")
    blocked_count = sum(1 for stage in stages if stage["status"] == "blocked")
    skipped_count = sum(1 for stage in stages if stage["status"] == "skipped")
    summary: RefreshSummary = {
        "stage_count": len(CANONICAL_STAGES),
        "stage_succeeded_count": succeeded_count,
        "stage_failed_count": failed_count,
        "stage_blocked_count": blocked_count,
        "stage_skipped_count": skipped_count,
        "duration_seconds_total": _duration_seconds(started_dt, finished_dt),
    }
    artifacts: RefreshArtifacts = {
        "root_path": str(root_path),
        "latest_pointer_path": str(
            artifact_base_dir
            / "latest"
            / profile_name
            / feature_version
            / ruleset_version
        ),
        "stage_artifacts": stage_artifacts,
    }
    run: RefreshRun = {
        "run_type": RUN_TYPE_REFRESH_AUTOMATION,
        "version_tag": REFRESH_AUTOMATION_VERSION_TAG,
        "run_id": run_id,
        "profile_name": profile_name,
        "status": run_status,
        "run_persisted": False,
        "started_at": _iso_utc(started_dt),
        "finished_at": _iso_utc(finished_dt),
    }
    return {
        "run": run,
        "request": request,
        "summary": summary,
        "stages": stages,
        "artifacts": artifacts,
        "diagnostics": diagnostics,
        "error": error,
    }


def _skipped_stage(*, stage_id: str, attempt: int) -> RefreshStage:
    return {
        "stage_id": stage_id,
        "status": "skipped",
        "started_at": None,
        "finished_at": None,
        "duration_seconds": None,
        "attempt": attempt,
        "command_results": [],
    }


def _blocked_stage(*, stage_id: str, attempt: int) -> RefreshStage:
    return {
        "stage_id": stage_id,
        "status": "blocked",
        "started_at": None,
        "finished_at": None,
        "duration_seconds": None,
        "attempt": attempt,
        "command_results": [],
    }


def _latest_pass_stages(
    *,
    attempt_count: int,
    retried_from_stage_id: Optional[str],
    selected_stages: tuple[str, ...],
) -> set[str]:
    if attempt_count <= 1 or not retried_from_stage_id:
        return set(selected_stages)
    start_index = CANONICAL_STAGES.index(retried_from_stage_id)
    return {
        stage_id
        for stage_id in CANONICAL_STAGES[start_index:]
        if stage_id in selected_stages
    }


def _validate_run_context(
    *,
    run_date: str,
    profile_name: str,
    run_id: str,
    artifact_base_dir: Path,
) -> Optional[str]:
    if not re.fullmatch(r"\d{8}", run_date):
        return "run_date must match YYYYMMDD."
    if not _SAFE_PATH_SEGMENT_RE.fullmatch(profile_name):
        return (
            "profile_name contains unsupported path characters; only letters, "
            "digits, '.', '_' and '-' are allowed."
        )
    if not _SAFE_PATH_SEGMENT_RE.fullmatch(run_id):
        return (
            "run_id contains unsupported path characters; only letters, digits, "
            "'.', '_' and '-' are allowed."
        )
    base_resolved = artifact_base_dir.resolve()
    target_resolved = (artifact_base_dir / run_date / profile_name / run_id).resolve()
    try:
        target_resolved.relative_to(base_resolved)
    except ValueError:
        return "artifact root escapes artifact_base_dir."
    return None


def _validate_retry_context(
    *,
    attempt_count: int,
    retried_from_stage_id: Optional[str],
    selected_stages: tuple[str, ...],
) -> Optional[str]:
    if attempt_count <= 1:
        return None
    if retried_from_stage_id is None:
        return None
    if retried_from_stage_id not in CANONICAL_STAGES:
        return (
            "retried_from_stage_id must be one of the canonical stage ids: "
            + ", ".join(CANONICAL_STAGES)
            + "."
        )
    if retried_from_stage_id not in selected_stages:
        return (
            "retried_from_stage_id must be selected by the active profile; "
            f"got '{retried_from_stage_id}' for profile stages {list(selected_stages)}."
        )
    return None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _duration_seconds(started: datetime, finished: datetime) -> float:
    return round(max((finished - started).total_seconds(), 0.0), 3)


def _acquire_profile_lock(
    *, artifact_base_dir: Path, profile_name: str, started_dt: datetime
) -> Optional[Path]:
    lock_dir = artifact_base_dir / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{profile_name}.lock"
    try:
        fd = os.open(str(lock_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        return None
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "profile_name": profile_name,
                        "pid": os.getpid(),
                        "started_at": _iso_utc(started_dt),
                    },
                    indent=2,
                )
            )
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        _safe_unlink(lock_path)
        raise
    return lock_path


def _release_profile_lock(lock_path: Path) -> None:
    _safe_unlink(lock_path)


def _persist_run_artifacts(payload: RefreshPayload) -> None:
    root_path = Path(payload["artifacts"]["root_path"])
    latest_pointer_path = Path(payload["artifacts"]["latest_pointer_path"])
    health_dir = root_path / "health_summary"
    health_dir.mkdir(parents=True, exist_ok=True)

    health_stage_artifacts = payload["artifacts"]["stage_artifacts"].setdefault(
        "health_summary", []
    )
    refresh_payload_path = health_dir / "refresh_run_payload.json"
    run_manifest_path = root_path / "run_manifest.json"
    failure_artifact_path = health_dir / "failure_artifact.json"

    _write_json_atomic(
        run_manifest_path,
        {
            "run_id": payload["run"]["run_id"],
            "profile_name": payload["run"]["profile_name"],
            "status": payload["run"]["status"],
            "started_at": payload["run"]["started_at"],
            "finished_at": payload["run"]["finished_at"],
            "root_path": str(root_path),
        },
    )

    if payload["error"] is not None:
        _write_json_atomic(failure_artifact_path, payload["error"])
        if str(failure_artifact_path) not in health_stage_artifacts:
            health_stage_artifacts.append(str(failure_artifact_path))

    latest_pointer_path.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(
        latest_pointer_path / "latest_run.json",
        {
            "run_id": payload["run"]["run_id"],
            "profile_name": payload["run"]["profile_name"],
            "status": payload["run"]["status"],
            "root_path": str(root_path),
            "finished_at": payload["run"]["finished_at"],
        },
    )

    payload["run"]["run_persisted"] = True
    _write_json_atomic(refresh_payload_path, payload)
    if str(refresh_payload_path) not in health_stage_artifacts:
        health_stage_artifacts.append(str(refresh_payload_path))


def _write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
