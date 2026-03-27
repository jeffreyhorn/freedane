from __future__ import annotations

import hashlib
import json
import re
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
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _sha256_bytes(payload_bytes: bytes) -> str:
    return hashlib.sha256(payload_bytes).hexdigest()


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
    now_fn: Callable[[], datetime] = _now_utc,
) -> list[CanonicalAlertInstance]:
    results: list[CanonicalAlertInstance] = []
    for source_path in [*alert_files, *scheduler_files]:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        source_payload_hash = _sha256_bytes(source_path.read_bytes())
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
    if config_path is None:
        return default_route_config(route_group)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TransportError("route config must be a JSON object")
    routes = payload.get("routes")
    destinations = payload.get("destinations")
    if not isinstance(routes, dict) or not isinstance(destinations, dict):
        raise TransportError(
            "route config requires object keys: routes and destinations"
        )
    return {
        "contract_version": str(
            payload.get("contract_version") or "alert_route_config_v1"
        ),
        "route_group": str(payload.get("route_group") or route_group),
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
    ack_required = bool(policy.get("ack_required"))
    ack_timeout = _as_int(policy.get("ack_timeout_seconds"))
    if ack_required and ack_timeout is None:
        raise TransportError(
            "Routing policy "
            f"{policy_key} requires ack_timeout_seconds when ack_required=true."
        )
    if not ack_required and policy.get("ack_timeout_seconds") is not None:
        if ack_timeout is not None:
            raise TransportError(
                "Routing policy "
                f"{policy_key} must set ack_timeout_seconds to null when "
                "ack_required=false."
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
        if not self.path.exists():
            return {}
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {}
        result: dict[str, str] = {}
        for key, value in payload.items():
            if isinstance(key, str) and isinstance(value, str):
                result[key] = value
        return result

    def contains(self, key: str) -> bool:
        return key in self._values

    def add(self, key: str, event_id: str) -> None:
        self._values[key] = event_id

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._values, indent=2), encoding="utf-8")


def _validate_route_group(route_group: str) -> str:
    text = route_group.strip()
    if not text:
        raise TransportError("route_group must be non-empty")
    if not _SAFE_PATH_SEGMENT_RE.fullmatch(text):
        raise TransportError(
            "route_group contains unsupported characters; allowed: letters, "
            "digits, '.', '_' and '-'."
        )
    return text


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
    run_started = now_fn()
    emitted_at = _iso_utc(run_started)
    run_date = run_started.strftime("%Y%m%d")
    run_root = artifact_base_dir / run_date / transport_run_id
    run_root.mkdir(parents=True, exist_ok=True)

    idempotency_index = _IdempotencyIndex(
        path=artifact_base_dir / "idempotency_index.json"
    )
    events: list[dict[str, Any]] = []

    for alert in alerts:
        canonical_routing_key = (
            f"{resolved_group}.{alert['alert_type']}.{alert['severity']}"
        )
        policy_id, policy = _resolve_route_policy(
            route_group=resolved_group,
            alert_type=alert["alert_type"],
            severity=alert["severity"],
            config=config,
        )

        destination_records: list[dict[str, Any]] = []
        delivery_receipts: list[dict[str, Any]] = []
        status_values: list[DeliveryStatus] = []
        first_delivery_dt: Optional[datetime] = None

        for destination_id in (
            policy["primary_destinations"] + policy["escalation_destinations"]
        ):
            is_primary = destination_id in policy["primary_destinations"]
            destination = _destination_config_or_default(
                destination_id=destination_id,
                config=config,
            )
            if not is_primary:
                destination_records.append(
                    {
                        "destination_id": destination_id,
                        "status": "pending",
                        "attempt_count": 0,
                        "max_attempts": _RETRY_ATTEMPTS_BY_SEVERITY[alert["severity"]],
                        "last_attempt_at_utc": None,
                        "next_attempt_at_utc": None,
                    }
                )
                status_values.append("pending")
                continue

            max_attempts = _RETRY_ATTEMPTS_BY_SEVERITY[alert["severity"]]
            idempotency_key = _idempotency_key(
                event_id=alert["event_id"],
                destination_id=destination_id,
                canonical_routing_key=canonical_routing_key,
                severity=alert["severity"],
            )
            if idempotency_index.contains(idempotency_key):
                destination_records.append(
                    {
                        "destination_id": destination_id,
                        "status": "suppressed_duplicate",
                        "attempt_count": 0,
                        "max_attempts": max_attempts,
                        "last_attempt_at_utc": None,
                        "next_attempt_at_utc": None,
                    }
                )
                status_values.append("suppressed_duplicate")
                delivery_receipts.append(
                    {
                        "destination_id": destination_id,
                        "status": "suppressed_duplicate",
                        "idempotency_key": idempotency_key,
                    }
                )
                continue

            final_status: DeliveryStatus = "failed_terminal"
            last_attempt_at_utc: Optional[str] = None
            attempt_count = 0
            for attempt_index in range(1, max_attempts + 1):
                attempt_count = attempt_index
                attempt_dt = now_fn()
                last_attempt_at_utc = _iso_utc(attempt_dt)
                envelope = {
                    "envelope": {
                        "contract_version": "alert_transport_v1",
                        "event_id": alert["event_id"],
                        "transport_run_id": transport_run_id,
                        "emitted_at_utc": emitted_at,
                        "run_date": run_date,
                        "environment": environment_name,
                        "alert_route_group": resolved_group,
                    },
                    "alert": alert,
                    "route": {
                        "canonical_routing_key": canonical_routing_key,
                        "source_routing_key": alert["source_routing_key"],
                        "policy_id": policy_id,
                        "policy_version": config.get("contract_version"),
                    },
                }
                outcome = adapter.deliver(
                    destination_id=destination_id,
                    destination=destination,
                    envelope=envelope,
                    attempt_index=attempt_index,
                )
                receipt = dict(outcome.receipt)
                receipt["attempt_index"] = attempt_index
                receipt["destination_id"] = destination_id
                receipt["at_utc"] = last_attempt_at_utc
                delivery_receipts.append(receipt)

                if outcome.delivered:
                    final_status = "delivered"
                    idempotency_index.add(idempotency_key, alert["event_id"])
                    if first_delivery_dt is None:
                        first_delivery_dt = attempt_dt
                    break
                if outcome.retryable and attempt_index < max_attempts:
                    final_status = "failed_retryable"
                    continue
                if outcome.retryable:
                    final_status = "failed_retryable"
                else:
                    final_status = "failed_terminal"
                break

            destination_records.append(
                {
                    "destination_id": destination_id,
                    "status": final_status,
                    "attempt_count": attempt_count,
                    "max_attempts": max_attempts,
                    "last_attempt_at_utc": last_attempt_at_utc,
                    "next_attempt_at_utc": None,
                }
            )
            status_values.append(final_status)

        overall_status = _delivery_overall_status(status_values)
        ack_required = policy["ack_required"]
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
                if now_fn() >= ack_deadline_dt:
                    ack_state = "expired"

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
            "alert": alert,
            "route": {
                "canonical_routing_key": canonical_routing_key,
                "source_routing_key": alert["source_routing_key"],
                "policy_id": policy_id,
                "policy_version": config.get("contract_version"),
            },
            "destinations": [
                {
                    "destination_id": destination_id,
                    "channel_type": _destination_config_or_default(
                        destination_id=destination_id,
                        config=config,
                    )["channel_type"],
                    "channel_target": _destination_config_or_default(
                        destination_id=destination_id,
                        config=config,
                    )["channel_target"],
                    "is_primary": destination_id in policy["primary_destinations"],
                    "is_escalation": destination_id
                    in policy["escalation_destinations"],
                }
                for destination_id in policy["primary_destinations"]
                + policy["escalation_destinations"]
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

        event_root = run_root / alert["event_id"]
        event_root.mkdir(parents=True, exist_ok=True)
        (event_root / "alert_transport_envelope.json").write_text(
            json.dumps(event_payload, indent=2),
            encoding="utf-8",
        )
        attempts_path = event_root / "delivery_attempts.jsonl"
        with attempts_path.open("a", encoding="utf-8") as handle:
            for receipt in delivery_receipts:
                handle.write(json.dumps(receipt) + "\n")
        (event_root / "delivery_status.json").write_text(
            json.dumps(
                {
                    "event_id": alert["event_id"],
                    "overall_status": overall_status,
                    "delivery_count": len(destination_records),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    idempotency_index.save()

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
