from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    TypedDict,
)

try:
    import fcntl as _fcntl_module
except ModuleNotFoundError:  # pragma: no cover - exercised on non-POSIX runtimes.
    _fcntl: Any = None
else:
    _fcntl = _fcntl_module

DeliveryStatus = Literal[
    "pending",
    "delivered",
    "failed_retryable",
    "failed_terminal",
    "suppressed_duplicate",
]
Severity = Literal["info", "warn", "critical"]


class TransportError(ValueError):
    pass


class RoutePolicy(TypedDict):
    primary_destinations: list[str]
    escalation_destinations: list[str]
    ack_required: bool
    ack_timeout_seconds: Optional[int]
    escalation_schedule_seconds: list[int]


class DestinationConfig(TypedDict, total=False):
    channel_type: Literal["email", "slack", "pagerduty"]
    channel_target: str
    simulate_outcomes: list[str]


class RouteConfig(TypedDict):
    contract_version: str
    route_group: str
    routes: dict[str, RoutePolicy]
    destinations: dict[str, DestinationConfig]


class CanonicalAlertInstance(TypedDict):
    event_id: str
    source_system: str
    source_payload_type: str
    source_payload_path: str
    source_payload_hash: str
    source_run_id: Optional[str]
    alert_id: str
    alert_type: str
    source_alert_type: Optional[str]
    severity: Severity
    generated_at_utc: str
    summary: str
    reason_codes: list[str]
    operator_actions: list[dict[str, Any]]
    source_routing_key: Optional[str]


class TransportRunSummary(TypedDict):
    event_count: int
    delivered_count: int
    suppressed_duplicate_count: int
    failed_count: int


class TransportRunPayload(TypedDict):
    run: dict[str, Any]
    summary: TransportRunSummary
    artifacts: dict[str, Any]
    events: list[dict[str, Any]]


_SAFE_PATH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_RETRY_ATTEMPTS_BY_SEVERITY: dict[Severity, int] = {
    "critical": 6,
    "warn": 4,
    "info": 2,
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _as_str(value: Any) -> Optional[str]:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _as_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def _parse_utc_timestamp(value: Optional[str], *, fallback_now: datetime) -> datetime:
    if value is None:
        return fallback_now
    parsed = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(parsed)
    except ValueError:
        return fallback_now
    if dt.tzinfo is None or dt.utcoffset() is None:
        return fallback_now
    return dt.astimezone(timezone.utc)


def _portable_source_path(path: Path) -> str:
    resolved = path.resolve()
    lowered_parts = [part.lower() for part in resolved.parts]
    if "data" in lowered_parts:
        data_index = lowered_parts.index("data")
        return Path(*resolved.parts[data_index:]).as_posix()
    return Path(path.name).as_posix()


def _sha256_bytes(payload_bytes: bytes) -> str:
    return hashlib.sha256(payload_bytes).hexdigest()


def _write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _validate_path_component(*, label: str, value: str) -> str:
    text = value.strip()
    if not text:
        raise TransportError(f"{label} must be non-empty.")
    if text in {".", ".."}:
        raise TransportError(f"{label} cannot be '.' or '..'.")
    if not _SAFE_PATH_SEGMENT_RE.fullmatch(text):
        raise TransportError(
            f"{label} contains unsupported characters; allowed: letters, "
            "digits, '.', '_' and '-'."
        )
    return text


def _load_string_map(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, str] = {}
    for key, value in payload.items():
        if isinstance(key, str) and isinstance(value, str):
            result[key] = value
    return result


_LOCK_FALLBACK_WARNING_EMITTED = False


def _merge_save_string_map(path: Path, values: dict[str, str]) -> dict[str, str]:
    global _LOCK_FALLBACK_WARNING_EMITTED

    if _fcntl is None:
        if not _LOCK_FALLBACK_WARNING_EMITTED:
            warnings.warn(
                "fcntl is unavailable; using unlocked map-save fallback with reduced "
                "cross-process safety.",
                RuntimeWarning,
                stacklevel=2,
            )
            _LOCK_FALLBACK_WARNING_EMITTED = True
        current = _load_string_map(path)
        merged = dict(current)
        merged.update(values)
        _write_json_atomic(path, merged)
        return merged

    lock_path = path.with_suffix(f"{path.suffix}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a", encoding="utf-8") as handle:
        _fcntl.flock(handle.fileno(), _fcntl.LOCK_EX)
        current = _load_string_map(path)
        merged = dict(current)
        merged.update(values)
        _write_json_atomic(path, merged)
        _fcntl.flock(handle.fileno(), _fcntl.LOCK_UN)
    return merged


def _build_event_id(
    *, alert_id: str, source_payload_hash: str, source_system: str
) -> str:
    preimage = f"{source_system}|{alert_id}|{source_payload_hash}".encode("utf-8")
    return "evt_" + hashlib.sha256(preimage).hexdigest()[:20]


def _normalize_reason_codes(value: Any) -> list[str]:
    values = []
    for item in _as_list(value):
        text = _as_str(item)
        if text is not None:
            values.append(text)
    return sorted(set(values))


def _normalize_operator_actions(value: Any) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for item in _as_list(value):
        if isinstance(item, dict):
            actions.append(dict(item))
    return actions


def _normalize_severity(
    *, source_system: str, severity: Optional[str]
) -> Optional[Severity]:
    if severity is None:
        return None
    if source_system == "parser_drift" and severity == "error":
        return "critical"
    if severity == "info":
        return "info"
    if severity == "warn":
        return "warn"
    if severity == "critical":
        return "critical"
    return None


def _normalize_single_alert_payload(
    *,
    payload: Mapping[str, Any],
    source_path: Path,
    source_payload_hash: str,
    now_dt: datetime,
) -> list[CanonicalAlertInstance]:
    alert = _as_dict(payload.get("alert"))
    if not alert:
        return []
    source_system = _as_str(alert.get("alert_type"))
    if source_system not in {"parser_drift", "load_monitoring"}:
        return []
    source_system_text = str(source_system)
    severity = _normalize_severity(
        source_system=source_system_text,
        severity=_as_str(alert.get("severity")),
    )
    if severity is None:
        return []

    alert_id = _as_str(alert.get("alert_id"))
    if alert_id is None:
        return []

    generated_at = _parse_utc_timestamp(
        _as_str(alert.get("generated_at")),
        fallback_now=now_dt,
    )
    run = _as_dict(payload.get("run"))

    summary = _as_str(alert.get("summary"))
    if summary is None:
        summary = f"{source_system_text} {severity} alert"

    source_run_id = (
        _as_str(run.get("run_id"))
        or _as_str(alert.get("subject_run_id"))
        or _as_str(run.get("scheduler_run_id"))
    )

    instance: CanonicalAlertInstance = {
        "event_id": _build_event_id(
            alert_id=alert_id,
            source_payload_hash=source_payload_hash,
            source_system=source_system_text,
        ),
        "source_system": source_system_text,
        "source_payload_type": f"{source_system_text}_alert_payload_v1",
        "source_payload_path": _portable_source_path(source_path),
        "source_payload_hash": source_payload_hash,
        "source_run_id": source_run_id,
        "alert_id": alert_id,
        "alert_type": source_system_text,
        "source_alert_type": _as_str(alert.get("alert_type")),
        "severity": severity,
        "generated_at_utc": _iso_utc(generated_at),
        "summary": summary,
        "reason_codes": _normalize_reason_codes(alert.get("reason_codes")),
        "operator_actions": _normalize_operator_actions(
            payload.get("operator_actions")
        ),
        "source_routing_key": _as_str(alert.get("routing_key")),
    }
    return [instance]


def _normalize_benchmark_payload(
    *,
    payload: Mapping[str, Any],
    source_path: Path,
    source_payload_hash: str,
    now_dt: datetime,
) -> list[CanonicalAlertInstance]:
    source_alerts = _as_list(payload.get("alerts"))
    if not source_alerts:
        return []

    results: list[CanonicalAlertInstance] = []
    for idx, item in enumerate(source_alerts):
        alert = _as_dict(item)
        severity = _normalize_severity(
            source_system="benchmark_pack",
            severity=_as_str(alert.get("severity")),
        )
        if severity not in {"warn", "critical"}:
            continue
        alert_id = _as_str(alert.get("alert_id"))
        if alert_id is None:
            alert_id = f"benchmark_pack_alert_{idx:03d}"

        generated_at = _parse_utc_timestamp(
            _as_str(alert.get("generated_at")),
            fallback_now=now_dt,
        )
        summary = _as_str(alert.get("summary")) or "benchmark_pack alert"

        results.append(
            {
                "event_id": _build_event_id(
                    alert_id=alert_id,
                    source_payload_hash=source_payload_hash,
                    source_system="benchmark_pack",
                ),
                "source_system": "benchmark_pack",
                "source_payload_type": "benchmark_pack_alert_payload_v1",
                "source_payload_path": _portable_source_path(source_path),
                "source_payload_hash": source_payload_hash,
                "source_run_id": _extract_benchmark_source_run_id(alert_id),
                "alert_id": alert_id,
                "alert_type": "benchmark_pack",
                "source_alert_type": _as_str(alert.get("alert_type"))
                or "benchmark_pack",
                "severity": severity,
                "generated_at_utc": _iso_utc(generated_at),
                "summary": summary,
                "reason_codes": _normalize_reason_codes(alert.get("reason_codes")),
                "operator_actions": [],
                "source_routing_key": _as_str(alert.get("routing_key")),
            }
        )
    return results


def _extract_benchmark_source_run_id(alert_id: str) -> Optional[str]:
    marker = ".alert."
    if marker in alert_id:
        return alert_id.split(marker, 1)[0]
    return None


def _normalize_scheduler_payload(
    *,
    payload: Mapping[str, Any],
    source_path: Path,
    source_payload_hash: str,
    now_dt: datetime,
) -> list[CanonicalAlertInstance]:
    incident = _as_dict(payload.get("incident"))
    if not incident:
        return []

    severity = _normalize_severity(
        source_system="scheduler",
        severity=_as_str(incident.get("severity")),
    )
    if severity is None:
        return []

    incident_id = _as_str(incident.get("incident_id"))
    if incident_id is None:
        return []

    result = _as_dict(payload.get("result"))
    generated_at = _parse_utc_timestamp(
        _as_str(incident.get("opened_at_utc")),
        fallback_now=now_dt,
    )
    reason_codes = _normalize_reason_codes(
        [result.get("failure_code"), result.get("failure_class")]
    )
    summary = (
        _as_str(result.get("failure_message"))
        or _as_str(result.get("recommended_operator_action_summary"))
        or "scheduler incident"
    )

    return [
        {
            "event_id": _build_event_id(
                alert_id=incident_id,
                source_payload_hash=source_payload_hash,
                source_system="scheduler",
            ),
            "source_system": "scheduler",
            "source_payload_type": "scheduler_payload_v1",
            "source_payload_path": _portable_source_path(source_path),
            "source_payload_hash": source_payload_hash,
            "source_run_id": _as_str(
                _as_dict(payload.get("scheduler_run")).get("scheduler_run_id")
            ),
            "alert_id": incident_id,
            "alert_type": "scheduler",
            "source_alert_type": "scheduler",
            "severity": severity,
            "generated_at_utc": _iso_utc(generated_at),
            "summary": summary,
            "reason_codes": reason_codes,
            "operator_actions": _build_scheduler_operator_actions(result),
            "source_routing_key": None,
        }
    ]


def _build_scheduler_operator_actions(
    result: Mapping[str, Any],
) -> list[dict[str, Any]]:
    action_summary = _as_str(result.get("recommended_operator_action_summary"))
    if action_summary is None:
        return []
    return [
        {
            "action_id": "follow_scheduler_recommendation",
            "message": action_summary,
            "artifact_paths": [],
        }
    ]


def load_canonical_alerts(
    *,
    alert_files: Sequence[Path],
    scheduler_files: Sequence[Path],
    parse_warnings: Optional[list[str]] = None,
    now_fn: Callable[[], datetime] = _now_utc,
) -> list[CanonicalAlertInstance]:
    results: list[CanonicalAlertInstance] = []
    for source_path in [*alert_files, *scheduler_files]:
        source_bytes = source_path.read_bytes()
        source_payload_hash = _sha256_bytes(source_bytes)
        try:
            payload = json.loads(source_bytes)
        except (json.JSONDecodeError, UnicodeDecodeError):
            if parse_warnings is not None:
                parse_warnings.append(
                    f"Skipped malformed JSON file: {source_path.as_posix()}"
                )
            continue
        if not isinstance(payload, dict):
            if parse_warnings is not None:
                parse_warnings.append(
                    f"Skipped non-object JSON payload: {source_path.as_posix()}"
                )
            continue
        now_dt = now_fn()
        results.extend(
            _normalize_single_alert_payload(
                payload=payload,
                source_path=source_path,
                source_payload_hash=source_payload_hash,
                now_dt=now_dt,
            )
        )
        results.extend(
            _normalize_benchmark_payload(
                payload=payload,
                source_path=source_path,
                source_payload_hash=source_payload_hash,
                now_dt=now_dt,
            )
        )
        results.extend(
            _normalize_scheduler_payload(
                payload=payload,
                source_path=source_path,
                source_payload_hash=source_payload_hash,
                now_dt=now_dt,
            )
        )
    return results


def default_route_config(route_group: str) -> RouteConfig:
    return {
        "contract_version": "alert_route_config_v1",
        "route_group": route_group,
        "routes": {
            f"{route_group}.*.critical": {
                "primary_destinations": [
                    "slack.ops-critical",
                    "pagerduty.primary",
                    "email.oncall",
                ],
                "escalation_destinations": ["email.escalation"],
                "ack_required": True,
                "ack_timeout_seconds": 900,
                "escalation_schedule_seconds": [900, 1800],
            },
            f"{route_group}.*.warn": {
                "primary_destinations": ["slack.ops-warn", "email.team"],
                "escalation_destinations": ["email.escalation"],
                "ack_required": True,
                "ack_timeout_seconds": 14400,
                "escalation_schedule_seconds": [14400],
            },
            f"{route_group}.*.info": {
                "primary_destinations": ["slack.ops-info"],
                "escalation_destinations": [],
                "ack_required": False,
                "ack_timeout_seconds": None,
                "escalation_schedule_seconds": [],
            },
        },
        "destinations": {
            "slack.ops-critical": {
                "channel_type": "slack",
                "channel_target": "ops-critical",
            },
            "pagerduty.primary": {
                "channel_type": "pagerduty",
                "channel_target": "primary-service",
            },
            "email.oncall": {
                "channel_type": "email",
                "channel_target": "oncall@accessdane.local",
            },
            "email.escalation": {
                "channel_type": "email",
                "channel_target": "escalation@accessdane.local",
            },
            "slack.ops-warn": {
                "channel_type": "slack",
                "channel_target": "ops-warn",
            },
            "email.team": {
                "channel_type": "email",
                "channel_target": "team@accessdane.local",
            },
            "slack.ops-info": {
                "channel_type": "slack",
                "channel_target": "ops-info",
            },
        },
    }


def load_route_config(*, route_group: str, config_path: Optional[Path]) -> RouteConfig:
    resolved_route_group = _validate_route_group(route_group)
    if config_path is None:
        return default_route_config(resolved_route_group)
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise TransportError(
            f"Route config JSON is invalid: {config_path.as_posix()}"
        ) from exc
    if not isinstance(payload, dict):
        raise TransportError("route config must be a JSON object")
    routes = payload.get("routes")
    destinations = payload.get("destinations")
    if not isinstance(routes, dict) or not isinstance(destinations, dict):
        raise TransportError(
            "route config requires object keys: routes and destinations"
        )
    config_route_group = _as_str(payload.get("route_group")) or resolved_route_group
    if config_route_group != resolved_route_group:
        raise TransportError(
            "route_group mismatch between CLI/environment and route config file."
        )
    return {
        "contract_version": str(
            payload.get("contract_version") or "alert_route_config_v1"
        ),
        "route_group": config_route_group,
        "routes": routes,
        "destinations": destinations,
    }


def _resolve_route_policy(
    *,
    route_group: str,
    alert_type: str,
    severity: Severity,
    config: RouteConfig,
) -> tuple[str, RoutePolicy]:
    candidates = (
        f"{route_group}.{alert_type}.{severity}",
        f"{route_group}.{alert_type}.*",
        f"{route_group}.*.{severity}",
        f"{route_group}.*.*",
    )
    routes = config["routes"]
    for key in candidates:
        policy = routes.get(key)
        if isinstance(policy, dict):
            validated = _validate_route_policy(policy=policy, policy_key=key)
            return key, validated
    raise TransportError(
        f"No routing policy found for {route_group}.{alert_type}.{severity}."
    )


def _validate_route_policy(
    *, policy: Mapping[str, Any], policy_key: str
) -> RoutePolicy:
    primary = [
        destination_id
        for destination_id in (
            _as_str(item) for item in _as_list(policy.get("primary_destinations"))
        )
        if destination_id is not None
    ]
    escalation = [
        destination_id
        for destination_id in (
            _as_str(item) for item in _as_list(policy.get("escalation_destinations"))
        )
        if destination_id is not None
    ]
    if not primary:
        raise TransportError(
            f"Routing policy {policy_key} has no primary_destinations."
        )
    if "ack_required" not in policy:
        raise TransportError(f"Routing policy {policy_key} must include ack_required.")
    ack_required_value = policy.get("ack_required")
    if not isinstance(ack_required_value, bool):
        raise TransportError(
            f"Routing policy {policy_key} must set ack_required to a boolean."
        )
    ack_required = ack_required_value
    if "ack_timeout_seconds" not in policy:
        raise TransportError(
            f"Routing policy {policy_key} must include ack_timeout_seconds."
        )
    raw_ack_timeout = policy.get("ack_timeout_seconds")
    ack_timeout = _as_int(raw_ack_timeout)
    if ack_required and ack_timeout is None:
        raise TransportError(
            "Routing policy "
            f"{policy_key} requires ack_timeout_seconds when ack_required=true."
        )
    if not ack_required and raw_ack_timeout is not None:
        raise TransportError(
            "Routing policy "
            f"{policy_key} must set ack_timeout_seconds to null when "
            "ack_required=false."
        )
    if "escalation_schedule_seconds" not in policy:
        raise TransportError(
            f"Routing policy {policy_key} must include escalation_schedule_seconds."
        )
    schedule = [
        seconds
        for seconds in (
            _as_int(item)
            for item in _as_list(policy.get("escalation_schedule_seconds"))
        )
        if seconds is not None and seconds >= 0
    ]
    return {
        "primary_destinations": primary,
        "escalation_destinations": escalation,
        "ack_required": ack_required,
        "ack_timeout_seconds": ack_timeout,
        "escalation_schedule_seconds": sorted(set(schedule)),
    }


@dataclass(frozen=True)
class DeliveryAttemptOutcome:
    delivered: bool
    retryable: bool
    receipt: dict[str, Any]
    error_code: Optional[str] = None
    error_message: Optional[str] = None


class DeliveryAdapter(Protocol):
    def deliver(
        self,
        *,
        destination_id: str,
        destination: DestinationConfig,
        envelope: Mapping[str, Any],
        attempt_index: int,
    ) -> DeliveryAttemptOutcome: ...


class SimulatedDeliveryAdapter:
    """Deterministic adapter used by default and in tests.

    Destination config may include:
    - simulate_outcomes: list[str], where each item is one of
      delivered|failed_retryable|failed_terminal
    """

    def deliver(
        self,
        *,
        destination_id: str,
        destination: DestinationConfig,
        envelope: Mapping[str, Any],
        attempt_index: int,
    ) -> DeliveryAttemptOutcome:
        outcomes = destination.get("simulate_outcomes")
        selected = "delivered"
        if isinstance(outcomes, list) and outcomes:
            index = min(attempt_index - 1, len(outcomes) - 1)
            item = outcomes[index]
            if isinstance(item, str):
                selected = item
        if selected == "delivered":
            return DeliveryAttemptOutcome(
                delivered=True,
                retryable=False,
                receipt={
                    "adapter": "simulated",
                    "destination_id": destination_id,
                    "attempt_index": attempt_index,
                    "status": "delivered",
                },
            )
        if selected == "failed_retryable":
            return DeliveryAttemptOutcome(
                delivered=False,
                retryable=True,
                error_code="simulated_retryable",
                error_message="simulated transient delivery failure",
                receipt={
                    "adapter": "simulated",
                    "destination_id": destination_id,
                    "attempt_index": attempt_index,
                    "status": "failed_retryable",
                },
            )
        return DeliveryAttemptOutcome(
            delivered=False,
            retryable=False,
            error_code="simulated_terminal",
            error_message="simulated terminal delivery failure",
            receipt={
                "adapter": "simulated",
                "destination_id": destination_id,
                "attempt_index": attempt_index,
                "status": "failed_terminal",
            },
        )


class _IdempotencyIndex:
    def __init__(self, *, path: Path) -> None:
        self.path = path
        self._values = self._load()

    def _load(self) -> dict[str, str]:
        return _load_string_map(self.path)

    def contains(self, key: str) -> bool:
        return key in self._values

    def add(self, key: str, event_id: str) -> None:
        self._values[key] = event_id

    def save(self) -> None:
        self._values = _merge_save_string_map(self.path, self._values)


class _EscalationStepIndex:
    def __init__(self, *, path: Path) -> None:
        self.path = path
        self._values = self._load()

    def _load(self) -> dict[str, str]:
        return _load_string_map(self.path)

    def contains(self, key: str) -> bool:
        return key in self._values

    def add(self, key: str, event_id: str) -> None:
        self._values[key] = event_id

    def save(self) -> None:
        self._values = _merge_save_string_map(self.path, self._values)


def _validate_route_group(route_group: str) -> str:
    return _validate_path_component(label="route_group", value=route_group)


def _idempotency_key(
    *,
    event_id: str,
    destination_id: str,
    canonical_routing_key: str,
    severity: Severity,
) -> str:
    preimage = f"{event_id}|{destination_id}|{canonical_routing_key}|{severity}".encode(
        "utf-8"
    )
    return hashlib.sha256(preimage).hexdigest()


def _escalation_step_key(
    *,
    event_id: str,
    destination_id: str,
    canonical_routing_key: str,
    severity: Severity,
    offset_seconds: int,
) -> str:
    preimage = (
        f"{event_id}|{destination_id}|{canonical_routing_key}|{severity}|"
        f"escalation_offset:{offset_seconds}"
    ).encode("utf-8")
    return hashlib.sha256(preimage).hexdigest()


def _alert_payload_without_routing_fields(
    alert: Mapping[str, Any],
) -> dict[str, Any]:
    alert_payload = dict(alert)
    alert_payload.pop("source_routing_key", None)
    return alert_payload


def _destination_config_or_default(
    *, destination_id: str, config: RouteConfig
) -> DestinationConfig:
    destination = config["destinations"].get(destination_id)
    if isinstance(destination, dict):
        channel_type = _as_str(destination.get("channel_type"))
        target = _as_str(destination.get("channel_target"))
        if channel_type in {"email", "slack", "pagerduty"} and target is not None:
            resolved_channel_type: Literal["email", "slack", "pagerduty"]
            if channel_type == "email":
                resolved_channel_type = "email"
            elif channel_type == "pagerduty":
                resolved_channel_type = "pagerduty"
            else:
                resolved_channel_type = "slack"
            validated: DestinationConfig = {
                "channel_type": resolved_channel_type,
                "channel_target": target,
            }
            outcomes = destination.get("simulate_outcomes")
            if isinstance(outcomes, list):
                validated["simulate_outcomes"] = outcomes
            return validated
    if destination_id.startswith("email."):
        return {"channel_type": "email", "channel_target": destination_id}
    if destination_id.startswith("pagerduty."):
        return {"channel_type": "pagerduty", "channel_target": destination_id}
    return {"channel_type": "slack", "channel_target": destination_id}


def _delivery_overall_status(statuses: Sequence[DeliveryStatus]) -> DeliveryStatus:
    if statuses and all(status == "suppressed_duplicate" for status in statuses):
        return "suppressed_duplicate"
    if any(status == "failed_terminal" for status in statuses):
        return "failed_terminal"
    if any(status == "failed_retryable" for status in statuses):
        return "failed_retryable"
    if any(status == "delivered" for status in statuses):
        return "delivered"
    return "pending"


def _retry_delay_seconds(
    *, next_attempt_index: int, event_id: str, destination_id: str
) -> int:
    if next_attempt_index < 2:
        return 0
    base_seconds = min(30 * (2 ** (next_attempt_index - 2)), 900)
    jitter_seed = hashlib.sha256(
        f"{event_id}|{destination_id}|{next_attempt_index}".encode("utf-8")
    ).hexdigest()
    jitter_seconds = int(jitter_seed[:8], 16) % 31
    return base_seconds + jitter_seconds


def _build_transport_envelope_stub(
    *,
    event_id: str,
    transport_run_id: str,
    emitted_at_utc: str,
    run_date: str,
    environment_name: str,
    route_group: str,
    alert_payload: Mapping[str, Any],
    source_routing_key: Optional[str],
    canonical_routing_key: str,
    policy_id: str,
    policy_version: Optional[str],
) -> dict[str, Any]:
    return {
        "envelope": {
            "contract_version": "alert_transport_v1",
            "event_id": event_id,
            "transport_run_id": transport_run_id,
            "emitted_at_utc": emitted_at_utc,
            "run_date": run_date,
            "environment": environment_name,
            "alert_route_group": route_group,
        },
        "alert": alert_payload,
        "route": {
            "canonical_routing_key": canonical_routing_key,
            "source_routing_key": source_routing_key,
            "policy_id": policy_id,
            "policy_version": policy_version,
        },
    }


def _attempt_destination_delivery(
    *,
    alert: CanonicalAlertInstance,
    destination_id: str,
    destination: DestinationConfig,
    canonical_routing_key: str,
    max_attempts: int,
    envelope_stub: Mapping[str, Any],
    adapter: DeliveryAdapter,
    idempotency_index: "_IdempotencyIndex",
    start_dt: datetime,
    idempotency_enabled: bool = True,
) -> tuple[dict[str, Any], list[dict[str, Any]], Optional[datetime]]:
    idempotency_key = _idempotency_key(
        event_id=alert["event_id"],
        destination_id=destination_id,
        canonical_routing_key=canonical_routing_key,
        severity=alert["severity"],
    )
    if idempotency_enabled and idempotency_index.contains(idempotency_key):
        return (
            {
                "destination_id": destination_id,
                "status": "suppressed_duplicate",
                "attempt_count": 0,
                "max_attempts": max_attempts,
                "last_attempt_at_utc": None,
                "next_attempt_at_utc": None,
            },
            [
                {
                    "destination_id": destination_id,
                    "status": "suppressed_duplicate",
                    "idempotency_key": idempotency_key,
                }
            ],
            None,
        )

    attempt_dt = start_dt
    receipts: list[dict[str, Any]] = []
    last_attempt_at_utc: Optional[str] = None
    last_error_code: Optional[str] = None
    last_error_message: Optional[str] = None

    for attempt_index in range(1, max_attempts + 1):
        last_attempt_at_utc = _iso_utc(attempt_dt)
        outcome = adapter.deliver(
            destination_id=destination_id,
            destination=destination,
            envelope=envelope_stub,
            attempt_index=attempt_index,
        )
        receipt = dict(outcome.receipt)
        receipt["attempt_index"] = attempt_index
        receipt["destination_id"] = destination_id
        receipt["at_utc"] = last_attempt_at_utc
        if outcome.error_code is not None:
            receipt["error_code"] = outcome.error_code
        if outcome.error_message is not None:
            receipt["error_message"] = outcome.error_message

        if outcome.delivered:
            if idempotency_enabled:
                idempotency_index.add(idempotency_key, alert["event_id"])
                idempotency_index.save()
            receipts.append(receipt)
            return (
                {
                    "destination_id": destination_id,
                    "status": "delivered",
                    "attempt_count": attempt_index,
                    "max_attempts": max_attempts,
                    "last_attempt_at_utc": last_attempt_at_utc,
                    "next_attempt_at_utc": None,
                    "last_error_code": None,
                    "last_error_message": None,
                },
                receipts,
                attempt_dt,
            )

        last_error_code = outcome.error_code
        last_error_message = outcome.error_message
        if outcome.retryable and attempt_index < max_attempts:
            delay_seconds = _retry_delay_seconds(
                next_attempt_index=attempt_index + 1,
                event_id=alert["event_id"],
                destination_id=destination_id,
            )
            next_attempt_dt = attempt_dt + timedelta(seconds=delay_seconds)
            receipt["next_attempt_at_utc"] = _iso_utc(next_attempt_dt)
            receipt["backoff_seconds"] = delay_seconds
            receipts.append(receipt)
            attempt_dt = next_attempt_dt
            continue

        receipts.append(receipt)
        final_status: DeliveryStatus = (
            "failed_retryable" if outcome.retryable else "failed_terminal"
        )
        return (
            {
                "destination_id": destination_id,
                "status": final_status,
                "attempt_count": attempt_index,
                "max_attempts": max_attempts,
                "last_attempt_at_utc": last_attempt_at_utc,
                "next_attempt_at_utc": None,
                "last_error_code": last_error_code,
                "last_error_message": last_error_message,
            },
            receipts,
            None,
        )

    return (
        {
            "destination_id": destination_id,
            "status": "failed_terminal",
            "attempt_count": max_attempts,
            "max_attempts": max_attempts,
            "last_attempt_at_utc": last_attempt_at_utc,
            "next_attempt_at_utc": None,
            "last_error_code": last_error_code,
            "last_error_message": last_error_message,
        },
        receipts,
        None,
    )


def run_alert_transport(
    *,
    route_group: str,
    config: RouteConfig,
    alerts: Sequence[CanonicalAlertInstance],
    adapter: DeliveryAdapter,
    artifact_base_dir: Path,
    environment_name: str,
    transport_run_id: str,
    now_fn: Callable[[], datetime] = _now_utc,
) -> TransportRunPayload:
    resolved_group = _validate_route_group(route_group)
    config_route_group = _as_str(config.get("route_group"))
    if config_route_group is not None and config_route_group != resolved_group:
        raise TransportError(
            "route_group mismatch between runtime and loaded route configuration."
        )
    _validate_path_component(label="transport_run_id", value=transport_run_id)

    run_started = now_fn()
    emitted_at = _iso_utc(run_started)
    run_date = run_started.strftime("%Y%m%d")
    run_root = artifact_base_dir / run_date
    run_root.mkdir(parents=True, exist_ok=True)

    idempotency_index = _IdempotencyIndex(
        path=artifact_base_dir / "idempotency_index.json"
    )
    escalation_step_index = _EscalationStepIndex(
        path=artifact_base_dir / "escalation_step_index.json"
    )
    events: list[dict[str, Any]] = []

    for alert in alerts:
        raw_severity = _as_str(alert.get("severity"))
        severity: Severity
        if raw_severity == "info":
            severity = "info"
        elif raw_severity == "warn":
            severity = "warn"
        elif raw_severity == "critical":
            severity = "critical"
        else:
            raise TransportError(
                "Unsupported alert severity "
                f"{alert.get('severity')!r}; expected one of info|warn|critical."
            )
        canonical_routing_key = f"{resolved_group}.{alert['alert_type']}.{severity}"
        policy_id, policy = _resolve_route_policy(
            route_group=resolved_group,
            alert_type=alert["alert_type"],
            severity=severity,
            config=config,
        )

        safe_event_id = _validate_path_component(
            label="event_id",
            value=alert["event_id"],
        )
        if safe_event_id != alert["event_id"]:
            raise TransportError("event_id cannot contain leading/trailing whitespace.")
        alert_payload = _alert_payload_without_routing_fields(alert)
        source_routing_key = _as_str(alert.get("source_routing_key"))

        all_destination_ids = (
            policy["primary_destinations"] + policy["escalation_destinations"]
        )
        destination_configs = {
            destination_id: _destination_config_or_default(
                destination_id=destination_id,
                config=config,
            )
            for destination_id in all_destination_ids
        }

        destination_records: list[dict[str, Any]] = []
        delivery_receipts: list[dict[str, Any]] = []
        status_values: list[DeliveryStatus] = []
        first_delivery_dt: Optional[datetime] = None
        current_dt = run_started

        envelope_stub = _build_transport_envelope_stub(
            event_id=alert["event_id"],
            transport_run_id=transport_run_id,
            emitted_at_utc=emitted_at,
            run_date=run_date,
            environment_name=environment_name,
            route_group=resolved_group,
            alert_payload=alert_payload,
            source_routing_key=source_routing_key,
            canonical_routing_key=canonical_routing_key,
            policy_id=policy_id,
            policy_version=config.get("contract_version"),
        )

        for destination_id in policy["primary_destinations"]:
            destination = destination_configs[destination_id]
            max_attempts = _RETRY_ATTEMPTS_BY_SEVERITY[severity]
            destination_record, receipts, delivered_at = _attempt_destination_delivery(
                alert=alert,
                destination_id=destination_id,
                destination=destination,
                canonical_routing_key=canonical_routing_key,
                max_attempts=max_attempts,
                envelope_stub=envelope_stub,
                adapter=adapter,
                idempotency_index=idempotency_index,
                start_dt=current_dt,
            )
            destination_records.append(destination_record)
            delivery_receipts.extend(receipts)
            status_values.append(destination_record["status"])
            if delivered_at is not None and first_delivery_dt is None:
                first_delivery_dt = delivered_at
            if destination_record["last_attempt_at_utc"]:
                current_dt = _parse_utc_timestamp(
                    destination_record["last_attempt_at_utc"],
                    fallback_now=current_dt,
                )

        ack_required = policy["ack_required"]
        ack_reference_now = now_fn()
        ack_deadline_utc: Optional[str] = None
        ack_state: str
        if not ack_required:
            ack_state = "not_required"
        else:
            ack_state = "pending"
            if (
                first_delivery_dt is not None
                and policy["ack_timeout_seconds"] is not None
            ):
                ack_deadline_dt = first_delivery_dt + timedelta(
                    seconds=policy["ack_timeout_seconds"]
                )
                ack_deadline_utc = _iso_utc(ack_deadline_dt)
                if ack_reference_now >= ack_deadline_dt:
                    ack_state = "expired"

        primary_only_duplicate_suppression = (
            first_delivery_dt is None
            and bool(status_values)
            and all(status == "suppressed_duplicate" for status in status_values)
        )
        for destination_id in policy["escalation_destinations"]:
            destination = destination_configs[destination_id]
            max_attempts = _RETRY_ATTEMPTS_BY_SEVERITY[severity]
            escalation_offsets = policy["escalation_schedule_seconds"]

            def _step_key(offset_seconds: int) -> str:
                return _escalation_step_key(
                    event_id=alert["event_id"],
                    destination_id=destination_id,
                    canonical_routing_key=canonical_routing_key,
                    severity=severity,
                    offset_seconds=offset_seconds,
                )

            if first_delivery_dt is None or ack_state != "pending":
                escalation_status: DeliveryStatus
                if primary_only_duplicate_suppression:
                    escalation_status = "suppressed_duplicate"
                else:
                    escalation_status = "pending"
                destination_records.append(
                    {
                        "destination_id": destination_id,
                        "status": escalation_status,
                        "attempt_count": 0,
                        "max_attempts": max_attempts,
                        "last_attempt_at_utc": None,
                        "next_attempt_at_utc": None,
                    }
                )
                status_values.append(escalation_status)
                continue

            due_offsets = [
                offset
                for offset in escalation_offsets
                if ack_reference_now >= first_delivery_dt + timedelta(seconds=offset)
            ]
            due_unfired_offsets = [
                offset
                for offset in due_offsets
                if not escalation_step_index.contains(_step_key(offset))
            ]
            pending_offsets = [
                offset
                for offset in escalation_offsets
                if offset not in due_offsets
                and not escalation_step_index.contains(_step_key(offset))
            ]
            next_due_utc: Optional[str] = None
            if pending_offsets:
                next_due_dt = first_delivery_dt + timedelta(
                    seconds=min(pending_offsets)
                )
                next_due_utc = _iso_utc(next_due_dt)
            if not due_unfired_offsets:
                status: DeliveryStatus = (
                    "suppressed_duplicate" if due_offsets else "pending"
                )
                destination_records.append(
                    {
                        "destination_id": destination_id,
                        "status": status,
                        "attempt_count": 0,
                        "max_attempts": max_attempts,
                        "last_attempt_at_utc": None,
                        "next_attempt_at_utc": next_due_utc,
                        "last_error_code": None,
                        "last_error_message": None,
                    }
                )
                status_values.append(status)
                continue

            attempted_offsets: list[int] = []
            attempted_statuses: list[DeliveryStatus] = []
            attempt_count_total = 0
            last_attempt_at_utc: Optional[str] = None
            last_error_code: Optional[str] = None
            last_error_message: Optional[str] = None
            for due_offset in due_unfired_offsets:
                step_record, receipts, _ = _attempt_destination_delivery(
                    alert=alert,
                    destination_id=destination_id,
                    destination=destination,
                    canonical_routing_key=canonical_routing_key,
                    max_attempts=max_attempts,
                    envelope_stub=envelope_stub,
                    adapter=adapter,
                    idempotency_index=idempotency_index,
                    start_dt=max(
                        ack_reference_now,
                        first_delivery_dt + timedelta(seconds=due_offset),
                    ),
                    idempotency_enabled=False,
                )
                for receipt in receipts:
                    receipt["escalation_offset_seconds"] = due_offset
                delivery_receipts.extend(receipts)
                attempted_offsets.append(due_offset)
                attempted_statuses.append(step_record["status"])
                attempt_count_total += int(step_record["attempt_count"])
                if step_record["last_attempt_at_utc"]:
                    last_attempt_at_utc = str(step_record["last_attempt_at_utc"])
                    current_dt = _parse_utc_timestamp(
                        step_record["last_attempt_at_utc"],
                        fallback_now=current_dt,
                    )
                if step_record.get("last_error_code") is not None:
                    last_error_code = _as_str(step_record.get("last_error_code"))
                if step_record.get("last_error_message") is not None:
                    last_error_message = _as_str(step_record.get("last_error_message"))
                if step_record["status"] in {"delivered", "failed_terminal"}:
                    escalation_step_index.add(_step_key(due_offset), alert["event_id"])
                    escalation_step_index.save()

            destination_status = _delivery_overall_status(attempted_statuses)
            destination_record = {
                "destination_id": destination_id,
                "status": destination_status,
                "attempt_count": attempt_count_total,
                "max_attempts": max_attempts * len(due_unfired_offsets),
                "last_attempt_at_utc": last_attempt_at_utc,
                "next_attempt_at_utc": next_due_utc,
                "last_error_code": last_error_code,
                "last_error_message": last_error_message,
                "escalation_offsets_attempted": attempted_offsets,
            }
            destination_records.append(destination_record)
            status_values.append(destination_status)

        overall_status = _delivery_overall_status(status_values)

        event_payload = {
            "envelope": {
                "contract_version": "alert_transport_v1",
                "event_id": alert["event_id"],
                "transport_run_id": transport_run_id,
                "emitted_at_utc": emitted_at,
                "run_date": run_date,
                "environment": environment_name,
                "alert_route_group": resolved_group,
            },
            "alert": alert_payload,
            "route": {
                "canonical_routing_key": canonical_routing_key,
                "source_routing_key": source_routing_key,
                "policy_id": policy_id,
                "policy_version": config.get("contract_version"),
            },
            "destinations": [
                {
                    "destination_id": destination_id,
                    "channel_type": destination_configs[destination_id]["channel_type"],
                    "channel_target": destination_configs[destination_id][
                        "channel_target"
                    ],
                    "is_primary": destination_id in policy["primary_destinations"],
                    "is_escalation": destination_id
                    in policy["escalation_destinations"],
                }
                for destination_id in all_destination_ids
            ],
            "delivery": {
                "overall_status": overall_status,
                "first_delivery_at_utc": (
                    _iso_utc(first_delivery_dt)
                    if first_delivery_dt is not None
                    else None
                ),
                "delivery_receipts": delivery_receipts,
                "deliveries": destination_records,
            },
            "acknowledgment": {
                "ack_required": ack_required,
                "ack_state": ack_state,
                "ack_deadline_utc": ack_deadline_utc,
                "acked_at_utc": None,
                "acked_by": None,
                "incident_id": None,
            },
            "error": None,
        }
        events.append(event_payload)

        event_root = run_root / safe_event_id
        event_root.mkdir(parents=True, exist_ok=True)
        existing_status_path = event_root / "delivery_status.json"
        preserve_existing_delivered = False
        if existing_status_path.exists():
            existing_status = _load_string_map(existing_status_path)
            preserve_existing_delivered = (
                existing_status.get("overall_status") == "delivered"
                and overall_status != "delivered"
            )
        if not preserve_existing_delivered:
            _write_json_atomic(
                event_root / "alert_transport_envelope.json",
                event_payload,
            )
        attempts_path = event_root / "delivery_attempts.jsonl"
        with attempts_path.open("a", encoding="utf-8") as handle:
            for receipt in delivery_receipts:
                handle.write(json.dumps(receipt) + "\n")
        if not preserve_existing_delivered:
            _write_json_atomic(
                event_root / "delivery_status.json",
                {
                    "event_id": alert["event_id"],
                    "overall_status": overall_status,
                    "delivery_count": len(destination_records),
                },
            )

    delivered_count = sum(
        1 for event in events if event["delivery"]["overall_status"] == "delivered"
    )
    suppressed_duplicate_count = sum(
        1
        for event in events
        if event["delivery"]["overall_status"] == "suppressed_duplicate"
    )
    failed_count = sum(
        1
        for event in events
        if event["delivery"]["overall_status"]
        in {"failed_retryable", "failed_terminal"}
    )

    run_finished = now_fn()
    return {
        "run": {
            "run_type": "alert_transport",
            "version_tag": "alert_transport_v1",
            "transport_run_id": transport_run_id,
            "status": "failed" if failed_count else "succeeded",
            "started_at_utc": _iso_utc(run_started),
            "finished_at_utc": _iso_utc(run_finished),
            "route_group": resolved_group,
            "environment": environment_name,
        },
        "summary": {
            "event_count": len(events),
            "delivered_count": delivered_count,
            "suppressed_duplicate_count": suppressed_duplicate_count,
            "failed_count": failed_count,
        },
        "artifacts": {
            "artifact_base_dir": str(artifact_base_dir),
            "run_root": str(run_root),
            "idempotency_index": str(artifact_base_dir / "idempotency_index.json"),
        },
        "events": events,
    }
