from __future__ import annotations

import json
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Optional, TypedDict

from .refresh_automation import run_scheduled_refresh

TriggerType = Literal["scheduled", "manual_retry", "catch_up"]
SchedulerState = Literal[
    "queued",
    "dispatched",
    "running",
    "retry_pending",
    "succeeded",
    "failed_pending_dead_letter",
    "dead_lettered",
    "cancelled",
]
AttemptState = Literal[
    "succeeded",
    "failed",
    "cancelled",
    "overlap_blocked",
    "dispatch_error",
]
ResultStatus = Literal["pending", "succeeded", "dead_lettered", "cancelled"]
FailureClass = Literal["retryable", "non_retryable", "exhausted_retries"]


class SchedulerRun(TypedDict):
    scheduler_run_id: str
    state: SchedulerState
    profile_name: str
    run_date: str
    refresh_run_id: Optional[str]
    attempt_count: int
    max_attempts: int
    created_at_utc: str
    updated_at_utc: str


class SchedulerTrigger(TypedDict):
    trigger_id: str
    trigger_type: TriggerType
    profile_name: str
    scheduled_for_utc: str
    created_at_utc: str
    requested_by: str
    run_context: dict[str, object]


class SchedulerAttempt(TypedDict):
    attempt_index: int
    refresh_run_id: Optional[str]
    state: AttemptState
    started_at_utc: Optional[str]
    finished_at_utc: Optional[str]
    duration_seconds: Optional[int]
    refresh_payload_path: Optional[str]
    failure_code: Optional[str]
    failure_message: Optional[str]
    failed_stage_id: Optional[str]


class SchedulerResult(TypedDict):
    status: ResultStatus
    failure_class: Optional[FailureClass]
    failure_code: Optional[str]
    failure_message: Optional[str]
    failed_stage_id: Optional[str]
    attempt_count: int
    max_attempts: int
    last_attempt_finished_at_utc: Optional[str]
    dead_letter_path: Optional[str]
    recommended_operator_action_summary: Optional[str]


class SchedulerIncident(TypedDict):
    incident_id: str
    opened_at_utc: str
    severity: Literal["critical", "warn", "info"]
    scheduler_run_id: str
    profile_name: str
    failure_class: FailureClass
    acknowledged_by: Optional[str]
    mitigation_summary: Optional[str]
    resolved_at_utc: Optional[str]


class SchedulerPayload(TypedDict):
    scheduler_run: SchedulerRun
    trigger: SchedulerTrigger
    attempts: list[SchedulerAttempt]
    result: SchedulerResult
    incident: Optional[SchedulerIncident]


_RETRYABLE_ERROR_CODES = {
    "overlapping_run",
    "stage_failure",
    "annual_refresh_failed",
    "dispatch_error",
}

_NON_RETRYABLE_ERROR_CODES = {
    "invalid_run_context",
    "unsupported_profile",
    "invalid_retry_boundary",
    "annual_preflight_failed",
}

_SAFE_PATH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")

RefreshRunner = Callable[..., Mapping[str, Any]]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def run_managed_scheduler_execution(
    *,
    trigger_type: TriggerType,
    profile_name: str,
    feature_version: str,
    ruleset_version: str,
    sales_ratio_base: str,
    top: int,
    retr_file: Optional[Path],
    permits_file: Optional[Path],
    appeals_file: Optional[Path],
    artifact_base_dir: Path,
    accessdane_bin: str,
    requested_by: str = "system",
    scheduled_for_utc: Optional[datetime] = None,
    scheduler_run_id: Optional[str] = None,
    refresh_log_dir: Optional[Path] = None,
    max_attempts: Optional[int] = None,
    backoff_base_seconds: int = 300,
    backoff_cap_seconds: int = 3600,
    backoff_jitter_seconds: int = 60,
    sleep_between_attempts: bool = False,
    now_fn: Callable[[], datetime] = _now_utc,
    sleep_fn: Callable[[float], None] = time.sleep,
    refresh_runner: Optional[RefreshRunner] = None,
    rng: Optional[random.Random] = None,
) -> SchedulerPayload:
    if max_attempts is None:
        max_attempts = _default_max_attempts(trigger_type)
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    random_source = rng or random.Random()
    created_dt = now_fn()
    if scheduled_for_utc is not None and (
        scheduled_for_utc.tzinfo is None or scheduled_for_utc.utcoffset() is None
    ):
        raise ValueError("scheduled_for_utc must be timezone-aware.")
    scheduled_dt = scheduled_for_utc or created_dt
    run_date = _derive_run_date(
        trigger_type=trigger_type, created_dt=created_dt, scheduled_for_utc=scheduled_dt
    )
    run_id = scheduler_run_id or _build_scheduler_run_id(
        run_date=run_date,
        profile_name=profile_name,
        feature_version=feature_version,
        ruleset_version=ruleset_version,
        created_dt=created_dt,
        rng=random_source,
    )
    _validate_scheduler_path_segments(
        profile_name=profile_name,
        feature_version=feature_version,
        ruleset_version=ruleset_version,
        run_id=run_id,
    )
    artifact_base_dir = artifact_base_dir.expanduser().resolve()
    log_dir = (
        refresh_log_dir
        if refresh_log_dir is not None
        else artifact_base_dir / "scheduler_logs"
    )
    log_dir = log_dir.expanduser().resolve()
    _validate_log_dir_within_artifact_base_dir(
        artifact_base_dir=artifact_base_dir,
        log_dir=log_dir,
    )
    payload_path = log_dir / f"{run_id}.json"

    payload: SchedulerPayload = {
        "scheduler_run": {
            "scheduler_run_id": run_id,
            "state": "queued",
            "profile_name": profile_name,
            "run_date": run_date,
            "refresh_run_id": None,
            "attempt_count": 0,
            "max_attempts": max_attempts,
            "created_at_utc": _iso_utc(created_dt),
            "updated_at_utc": _iso_utc(created_dt),
        },
        "trigger": {
            "trigger_id": f"trigger_{run_id}",
            "trigger_type": trigger_type,
            "profile_name": profile_name,
            "scheduled_for_utc": _iso_utc(scheduled_dt),
            "created_at_utc": _iso_utc(created_dt),
            "requested_by": requested_by,
            "run_context": {
                "feature_version": feature_version,
                "ruleset_version": ruleset_version,
                "sales_ratio_base": sales_ratio_base,
                "top": top,
                "retr_file": str(retr_file) if retr_file is not None else None,
                "permits_file": str(permits_file) if permits_file is not None else None,
                "appeals_file": str(appeals_file) if appeals_file is not None else None,
            },
        },
        "attempts": [],
        "result": {
            "status": "pending",
            "failure_class": None,
            "failure_code": None,
            "failure_message": None,
            "failed_stage_id": None,
            "attempt_count": 0,
            "max_attempts": max_attempts,
            "last_attempt_finished_at_utc": None,
            "dead_letter_path": None,
            "recommended_operator_action_summary": None,
        },
        "incident": None,
    }
    _persist_scheduler_payload(payload_path=payload_path, payload=payload)

    dispatch = refresh_runner or run_scheduled_refresh
    for attempt_index in range(1, max_attempts + 1):
        _transition_state(payload=payload, state="dispatched", now_fn=now_fn)
        payload["scheduler_run"]["attempt_count"] = attempt_index
        payload["result"]["attempt_count"] = attempt_index
        _persist_scheduler_payload(payload_path=payload_path, payload=payload)

        lock_path = artifact_base_dir / "locks" / f"{profile_name}.lock"
        if lock_path.exists():
            overlap_dt = now_fn()
            attempt = _build_overlap_attempt(
                attempt_index=attempt_index,
                started_at=overlap_dt,
                finished_at=overlap_dt,
                message=(
                    "Another refresh run is already active for this profile "
                    f"(lock: {lock_path})."
                ),
            )
            payload["attempts"].append(attempt)
            payload["result"]["last_attempt_finished_at_utc"] = attempt[
                "finished_at_utc"
            ]
            if attempt_index < max_attempts:
                _transition_state(payload=payload, state="retry_pending", now_fn=now_fn)
                _persist_scheduler_payload(payload_path=payload_path, payload=payload)
                _wait_for_backoff(
                    attempt_index=attempt_index + 1,
                    base_seconds=backoff_base_seconds,
                    cap_seconds=backoff_cap_seconds,
                    jitter_seconds=backoff_jitter_seconds,
                    random_source=random_source,
                    sleep_between_attempts=sleep_between_attempts,
                    sleep_fn=sleep_fn,
                )
                continue
            _finalize_dead_letter(
                payload=payload,
                payload_path=payload_path,
                artifact_base_dir=artifact_base_dir,
                error_code="overlapping_run",
                error_message=attempt["failure_message"]
                or "Overlapping run exhausted attempt budget.",
                failed_stage_id=None,
                failure_class="exhausted_retries",
                now_fn=now_fn,
            )
            return payload

        refresh_run_id = f"{run_id}_a{attempt_index:02d}"
        payload["scheduler_run"]["refresh_run_id"] = refresh_run_id
        _transition_state(payload=payload, state="running", now_fn=now_fn)
        _persist_scheduler_payload(payload_path=payload_path, payload=payload)
        attempt_started_dt = now_fn()
        try:
            refresh_payload = dispatch(
                profile_name=profile_name,
                run_date=run_date,
                run_id=refresh_run_id,
                feature_version=feature_version,
                ruleset_version=ruleset_version,
                sales_ratio_base=sales_ratio_base,
                top=top,
                retr_file=retr_file,
                permits_file=permits_file,
                appeals_file=appeals_file,
                artifact_base_dir=artifact_base_dir,
                accessdane_bin=accessdane_bin,
            )
        except Exception as exc:
            attempt_finished_dt = now_fn()
            attempt = _build_dispatch_error_attempt(
                attempt_index=attempt_index,
                started_at=attempt_started_dt,
                finished_at=attempt_finished_dt,
                message=f"Dispatch failure while invoking refresh-runner: {exc}",
            )
            payload["attempts"].append(attempt)
            payload["result"]["last_attempt_finished_at_utc"] = _iso_utc(
                attempt_finished_dt
            )
            if attempt_index < max_attempts:
                _transition_state(payload=payload, state="retry_pending", now_fn=now_fn)
                _persist_scheduler_payload(payload_path=payload_path, payload=payload)
                _wait_for_backoff(
                    attempt_index=attempt_index + 1,
                    base_seconds=backoff_base_seconds,
                    cap_seconds=backoff_cap_seconds,
                    jitter_seconds=backoff_jitter_seconds,
                    random_source=random_source,
                    sleep_between_attempts=sleep_between_attempts,
                    sleep_fn=sleep_fn,
                )
                continue
            _finalize_dead_letter(
                payload=payload,
                payload_path=payload_path,
                artifact_base_dir=artifact_base_dir,
                error_code="dispatch_error",
                error_message=attempt["failure_message"]
                or "Refresh-runner dispatch failed and retries were exhausted.",
                failed_stage_id=None,
                failure_class="exhausted_retries",
                now_fn=now_fn,
            )
            return payload

        attempt = _build_attempt_from_refresh_payload(
            attempt_index=attempt_index,
            refresh_run_id=refresh_run_id,
            refresh_payload=refresh_payload,
            fallback_started_dt=attempt_started_dt,
            fallback_finished_dt=now_fn(),
        )
        payload["attempts"].append(attempt)
        payload["scheduler_run"]["refresh_run_id"] = attempt.get("refresh_run_id")
        payload["result"]["last_attempt_finished_at_utc"] = attempt.get(
            "finished_at_utc"
        )

        if attempt["state"] == "succeeded":
            _transition_state(payload=payload, state="succeeded", now_fn=now_fn)
            payload["result"].update(
                {
                    "status": "succeeded",
                    "failure_class": None,
                    "failure_code": None,
                    "failure_message": None,
                    "failed_stage_id": None,
                    "dead_letter_path": None,
                    "recommended_operator_action_summary": None,
                }
            )
            _persist_scheduler_payload(payload_path=payload_path, payload=payload)
            return payload

        error_code = attempt.get("failure_code") or "dispatch_error"
        retryable = error_code in _RETRYABLE_ERROR_CODES
        if retryable and attempt_index < max_attempts:
            _transition_state(payload=payload, state="retry_pending", now_fn=now_fn)
            _persist_scheduler_payload(payload_path=payload_path, payload=payload)
            _wait_for_backoff(
                attempt_index=attempt_index + 1,
                base_seconds=backoff_base_seconds,
                cap_seconds=backoff_cap_seconds,
                jitter_seconds=backoff_jitter_seconds,
                random_source=random_source,
                sleep_between_attempts=sleep_between_attempts,
                sleep_fn=sleep_fn,
            )
            continue

        failure_class = _classify_failure_class(
            error_code=error_code,
            retryable=retryable,
            attempt_index=attempt_index,
            max_attempts=max_attempts,
        )
        _finalize_dead_letter(
            payload=payload,
            payload_path=payload_path,
            artifact_base_dir=artifact_base_dir,
            error_code=error_code,
            error_message=attempt.get("failure_message")
            or "Scheduler execution reached terminal failure.",
            failed_stage_id=attempt.get("failed_stage_id"),
            failure_class=failure_class,
            now_fn=now_fn,
        )
        return payload

    # Defensive terminalization: loop always returns above, but keep a safe fallback.
    _finalize_dead_letter(
        payload=payload,
        payload_path=payload_path,
        artifact_base_dir=artifact_base_dir,
        error_code="dispatch_error",
        error_message="Scheduler run ended unexpectedly before terminal state.",
        failed_stage_id=None,
        failure_class="non_retryable",
        now_fn=now_fn,
    )
    return payload


def _default_max_attempts(trigger_type: TriggerType) -> int:
    if trigger_type == "manual_retry":
        return 2
    return 3


def _build_scheduler_run_id(
    *,
    run_date: str,
    profile_name: str,
    feature_version: str,
    ruleset_version: str,
    created_dt: datetime,
    rng: random.Random,
) -> str:
    suffix = f"{rng.randint(0, 9999):04d}"
    return (
        f"{run_date}_{profile_name}_{feature_version}_{ruleset_version}_"
        f"{created_dt.strftime('%H%M%S')}_{suffix}"
    )


def _derive_run_date(
    *, trigger_type: TriggerType, created_dt: datetime, scheduled_for_utc: datetime
) -> str:
    if trigger_type in {"scheduled", "catch_up"}:
        return scheduled_for_utc.astimezone(timezone.utc).strftime("%Y%m%d")
    return created_dt.astimezone(timezone.utc).strftime("%Y%m%d")


def _iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _duration_seconds(started_at: datetime, finished_at: datetime) -> int:
    delta = finished_at - started_at
    seconds = int(delta.total_seconds())
    return seconds if seconds >= 0 else 0


def _parse_optional_iso(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _refresh_payload_path(refresh_payload: Mapping[str, Any]) -> Optional[str]:
    artifacts = refresh_payload.get("artifacts")
    if not isinstance(artifacts, dict):
        return None
    root_path = artifacts.get("root_path")
    if not isinstance(root_path, str) or not root_path:
        return None
    return str(Path(root_path) / "health_summary" / "refresh_run_payload.json")


def _build_attempt_from_refresh_payload(
    *,
    attempt_index: int,
    refresh_run_id: str,
    refresh_payload: Mapping[str, Any],
    fallback_started_dt: datetime,
    fallback_finished_dt: datetime,
) -> SchedulerAttempt:
    run = refresh_payload.get("run")
    run_started = None
    run_finished = None
    run_status = "failed"
    if isinstance(run, dict):
        run_started = _parse_optional_iso(run.get("started_at"))
        run_finished = _parse_optional_iso(run.get("finished_at"))
        run_status_value = run.get("status")
        if isinstance(run_status_value, str):
            run_status = run_status_value

    if run_started is None:
        run_started = fallback_started_dt
    if run_finished is None:
        run_finished = fallback_finished_dt

    if run_status == "succeeded":
        return {
            "attempt_index": attempt_index,
            "refresh_run_id": refresh_run_id,
            "state": "succeeded",
            "started_at_utc": _iso_utc(run_started),
            "finished_at_utc": _iso_utc(run_finished),
            "duration_seconds": _duration_seconds(run_started, run_finished),
            "refresh_payload_path": _refresh_payload_path(refresh_payload),
            "failure_code": None,
            "failure_message": None,
            "failed_stage_id": None,
        }

    error = refresh_payload.get("error")
    code = "dispatch_error"
    message = "Refresh-runner execution failed."
    failed_stage_id = None
    if isinstance(error, dict):
        if isinstance(error.get("code"), str):
            code = error["code"]
        if isinstance(error.get("message"), str):
            message = error["message"]
        if isinstance(error.get("failed_stage_id"), str):
            failed_stage_id = error["failed_stage_id"]
    if code == "overlapping_run":
        return _build_overlap_attempt(
            attempt_index=attempt_index,
            started_at=run_started,
            finished_at=run_finished,
            message=message,
        )
    return {
        "attempt_index": attempt_index,
        "refresh_run_id": refresh_run_id,
        "state": "failed",
        "started_at_utc": _iso_utc(run_started),
        "finished_at_utc": _iso_utc(run_finished),
        "duration_seconds": _duration_seconds(run_started, run_finished),
        "refresh_payload_path": _refresh_payload_path(refresh_payload),
        "failure_code": code,
        "failure_message": message,
        "failed_stage_id": failed_stage_id,
    }


def _build_overlap_attempt(
    *,
    attempt_index: int,
    started_at: datetime,
    finished_at: datetime,
    message: str,
) -> SchedulerAttempt:
    return {
        "attempt_index": attempt_index,
        "refresh_run_id": None,
        "state": "overlap_blocked",
        "started_at_utc": _iso_utc(started_at),
        "finished_at_utc": _iso_utc(finished_at),
        "duration_seconds": _duration_seconds(started_at, finished_at),
        "refresh_payload_path": None,
        "failure_code": "overlapping_run",
        "failure_message": message,
        "failed_stage_id": None,
    }


def _build_dispatch_error_attempt(
    *,
    attempt_index: int,
    started_at: datetime,
    finished_at: datetime,
    message: str,
) -> SchedulerAttempt:
    return {
        "attempt_index": attempt_index,
        "refresh_run_id": None,
        "state": "dispatch_error",
        "started_at_utc": _iso_utc(started_at),
        "finished_at_utc": _iso_utc(finished_at),
        "duration_seconds": _duration_seconds(started_at, finished_at),
        "refresh_payload_path": None,
        "failure_code": "dispatch_error",
        "failure_message": message,
        "failed_stage_id": None,
    }


def _validate_scheduler_path_segments(
    *,
    profile_name: str,
    feature_version: str,
    ruleset_version: str,
    run_id: str,
) -> None:
    _validate_path_segment(name="profile_name", value=profile_name)
    _validate_path_segment(name="feature_version", value=feature_version)
    _validate_path_segment(name="ruleset_version", value=ruleset_version)
    _validate_path_segment(name="scheduler_run_id", value=run_id)


def _validate_path_segment(*, name: str, value: str) -> None:
    if ".." in value:
        raise ValueError(
            f"Invalid {name} '{value}': path traversal sequence '..' is not allowed."
        )
    if not _SAFE_PATH_SEGMENT_RE.fullmatch(value):
        raise ValueError(
            f"Invalid {name} '{value}': only letters, digits, '.', '_' and '-' "
            "are allowed."
        )


def _classify_failure_class(
    *,
    error_code: str,
    retryable: bool,
    attempt_index: int,
    max_attempts: int,
) -> FailureClass:
    if retryable:
        if attempt_index >= max_attempts:
            return "exhausted_retries"
        return "retryable"
    if error_code in _NON_RETRYABLE_ERROR_CODES:
        return "non_retryable"
    return "non_retryable"


def _recommended_operator_action(
    *, error_code: str, failure_class: FailureClass, profile_name: str
) -> str:
    if error_code == "overlapping_run":
        return (
            f"Inspect active lock holders for profile '{profile_name}', then requeue "
            "the scheduler trigger once overlap is cleared."
        )
    if failure_class == "exhausted_retries":
        return (
            "Inspect the latest refresh payload failure metadata and upstream "
            "dependencies, then trigger a manual retry after remediation."
        )
    return (
        "Review failure metadata in scheduler and refresh payloads, apply corrective "
        "configuration or data input fixes, then rerun."
    )


def _transition_state(
    *, payload: SchedulerPayload, state: SchedulerState, now_fn: Callable[[], datetime]
) -> None:
    payload["scheduler_run"]["state"] = state
    payload["scheduler_run"]["updated_at_utc"] = _iso_utc(now_fn())


def _persist_scheduler_payload(
    *, payload_path: Path, payload: SchedulerPayload
) -> None:
    _write_json_atomic(payload_path, payload)


def _write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _wait_for_backoff(
    *,
    attempt_index: int,
    base_seconds: int,
    cap_seconds: int,
    jitter_seconds: int,
    random_source: random.Random,
    sleep_between_attempts: bool,
    sleep_fn: Callable[[float], None],
) -> None:
    exponent = max(0, attempt_index - 2)
    delay = min(base_seconds * (2**exponent), cap_seconds)
    jitter = random_source.randint(0, max(0, jitter_seconds))
    total = delay + jitter
    if sleep_between_attempts and total > 0:
        sleep_fn(float(total))


def _finalize_dead_letter(
    *,
    payload: SchedulerPayload,
    payload_path: Path,
    artifact_base_dir: Path,
    error_code: str,
    error_message: str,
    failed_stage_id: Optional[str],
    failure_class: FailureClass,
    now_fn: Callable[[], datetime],
) -> None:
    _transition_state(
        payload=payload, state="failed_pending_dead_letter", now_fn=now_fn
    )
    _transition_state(payload=payload, state="dead_lettered", now_fn=now_fn)
    dead_letter_path = (
        artifact_base_dir
        / "dead_letter"
        / payload["scheduler_run"]["profile_name"]
        / payload["scheduler_run"]["run_date"]
        / f"{payload['scheduler_run']['scheduler_run_id']}.json"
    )
    payload["result"].update(
        {
            "status": "dead_lettered",
            "failure_class": failure_class,
            "failure_code": error_code,
            "failure_message": error_message,
            "failed_stage_id": failed_stage_id,
            "dead_letter_path": str(dead_letter_path),
            "recommended_operator_action_summary": _recommended_operator_action(
                error_code=error_code,
                failure_class=failure_class,
                profile_name=payload["scheduler_run"]["profile_name"],
            ),
        }
    )
    payload["incident"] = {
        "incident_id": f"inc_{payload['scheduler_run']['scheduler_run_id']}",
        "opened_at_utc": _iso_utc(now_fn()),
        "severity": "critical",
        "scheduler_run_id": payload["scheduler_run"]["scheduler_run_id"],
        "profile_name": payload["scheduler_run"]["profile_name"],
        "failure_class": failure_class,
        "acknowledged_by": None,
        "mitigation_summary": None,
        "resolved_at_utc": None,
    }
    _persist_scheduler_payload(payload_path=payload_path, payload=payload)
    _write_json_atomic(dead_letter_path, payload)


def _validate_log_dir_within_artifact_base_dir(
    *, artifact_base_dir: Path, log_dir: Path
) -> None:
    try:
        log_dir.relative_to(artifact_base_dir)
    except ValueError as exc:
        raise ValueError(
            "refresh_log_dir must be within artifact_base_dir "
            f"(got {log_dir!s}, base {artifact_base_dir!s})"
        ) from exc
