from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.alert_transport import (
    SimulatedDeliveryAdapter,
    TransportError,
    default_route_config,
    load_canonical_alerts,
    load_route_config,
    run_alert_transport,
)


def _fixed_now() -> datetime:
    return datetime(2026, 3, 27, 12, 0, 0, tzinfo=timezone.utc)


def _parser_alert_payload() -> dict[str, object]:
    return {
        "run": {"run_id": "parser_run_001"},
        "alert": {
            "alert_id": "parser_run_001.error",
            "alert_type": "parser_drift",
            "severity": "error",
            "routing_key": "ops.parser_drift.error",
            "generated_at": "2026-03-27T11:59:00Z",
            "summary": "Parser drift found",
            "reason_codes": ["selector_miss"],
        },
        "operator_actions": [{"action_id": "triage"}],
        "error": None,
    }


def _benchmark_alert_payload() -> dict[str, object]:
    return {
        "generated_at": "2026-03-27T11:58:00Z",
        "alert_count": 2,
        "alerts": [
            {
                "alert_id": "bench_run.alert.001",
                "alert_type": "benchmark_pack",
                "severity": "warn",
                "summary": "Benchmark drift",
                "generated_at": "2026-03-27T11:58:00Z",
                "reason_codes": ["segment_shift"],
                "routing_key": "ops:feature:ruleset:segment_shift",
            },
            {
                "alert_id": "bench_run.alert.002",
                "alert_type": "benchmark_pack",
                "severity": "info",
                "summary": "Informational only",
                "generated_at": "2026-03-27T11:58:05Z",
            },
        ],
    }


def _scheduler_payload() -> dict[str, object]:
    return {
        "scheduler_run": {"scheduler_run_id": "sched_001"},
        "result": {
            "failure_code": "stage_failure",
            "failure_class": "retryable",
            "failure_message": "Stage failed",
            "recommended_operator_action_summary": "Retry from build_context",
        },
        "incident": {
            "incident_id": "incident_sched_001",
            "opened_at_utc": "2026-03-27T11:57:00Z",
            "severity": "critical",
        },
    }


def test_load_canonical_alerts_normalizes_supported_sources(tmp_path: Path) -> None:
    parser_path = tmp_path / "parser_alert.json"
    parser_path.write_text(
        json.dumps(_parser_alert_payload(), indent=2),
        encoding="utf-8",
    )
    benchmark_path = tmp_path / "benchmark_alert.json"
    benchmark_path.write_text(
        json.dumps(_benchmark_alert_payload(), indent=2),
        encoding="utf-8",
    )
    scheduler_path = tmp_path / "scheduler_payload.json"
    scheduler_path.write_text(
        json.dumps(_scheduler_payload(), indent=2),
        encoding="utf-8",
    )

    alerts = load_canonical_alerts(
        alert_files=[parser_path, benchmark_path],
        scheduler_files=[scheduler_path],
        now_fn=_fixed_now,
    )

    assert len(alerts) == 3
    parser_alert = next(
        alert for alert in alerts if alert["source_system"] == "parser_drift"
    )
    assert parser_alert["severity"] == "critical"
    benchmark_alert = next(
        alert for alert in alerts if alert["source_system"] == "benchmark_pack"
    )
    assert benchmark_alert["severity"] == "warn"
    scheduler_alert = next(
        alert for alert in alerts if alert["source_system"] == "scheduler"
    )
    assert scheduler_alert["severity"] == "critical"


def test_alert_transport_uses_exact_route_match_before_wildcards(
    tmp_path: Path,
) -> None:
    config = default_route_config("ops-alerts")
    config["routes"]["ops-alerts.parser_drift.*"] = {
        "primary_destinations": ["email.team"],
        "escalation_destinations": [],
        "ack_required": True,
        "ack_timeout_seconds": 60,
        "escalation_schedule_seconds": [60],
    }
    config["routes"]["ops-alerts.parser_drift.warn"] = {
        "primary_destinations": ["pagerduty.primary"],
        "escalation_destinations": [],
        "ack_required": True,
        "ack_timeout_seconds": 60,
        "escalation_schedule_seconds": [60],
    }

    alert = {
        "event_id": "evt_test_exact",
        "source_system": "parser_drift",
        "source_payload_type": "parser_drift_alert_payload_v1",
        "source_payload_path": "data/parser_alert.json",
        "source_payload_hash": "abc123",
        "source_run_id": "parser_run_001",
        "alert_id": "parser_run_001.warn",
        "alert_type": "parser_drift",
        "source_alert_type": "parser_drift",
        "severity": "warn",
        "generated_at_utc": "2026-03-27T12:00:00Z",
        "summary": "warn",
        "reason_codes": ["x"],
        "operator_actions": [],
        "source_routing_key": "ops.parser_drift.warn",
    }

    payload = run_alert_transport(
        route_group="ops-alerts",
        config=config,
        alerts=[alert],
        adapter=SimulatedDeliveryAdapter(),
        artifact_base_dir=tmp_path / "alerts",
        environment_name="dev",
        transport_run_id="route_exact_001",
        now_fn=_fixed_now,
    )

    assert payload["summary"]["event_count"] == 1
    assert payload["events"][0]["route"]["policy_id"] == "ops-alerts.parser_drift.warn"


def test_alert_transport_retries_retryable_delivery_and_then_succeeds(
    tmp_path: Path,
) -> None:
    config = default_route_config("ops-alerts")
    config["routes"]["ops-alerts.parser_drift.warn"] = {
        "primary_destinations": ["slack.ops-warn"],
        "escalation_destinations": [],
        "ack_required": True,
        "ack_timeout_seconds": 60,
        "escalation_schedule_seconds": [60],
    }
    config["destinations"]["slack.ops-warn"] = {
        "channel_type": "slack",
        "channel_target": "ops-warn",
        "simulate_outcomes": ["failed_retryable", "delivered"],
    }

    alert = {
        "event_id": "evt_retry_001",
        "source_system": "parser_drift",
        "source_payload_type": "parser_drift_alert_payload_v1",
        "source_payload_path": "data/parser_alert.json",
        "source_payload_hash": "abc123",
        "source_run_id": "parser_run_001",
        "alert_id": "parser_run_001.warn",
        "alert_type": "parser_drift",
        "source_alert_type": "parser_drift",
        "severity": "warn",
        "generated_at_utc": "2026-03-27T12:00:00Z",
        "summary": "warn",
        "reason_codes": ["x"],
        "operator_actions": [],
        "source_routing_key": "ops.parser_drift.warn",
    }

    payload = run_alert_transport(
        route_group="ops-alerts",
        config=config,
        alerts=[alert],
        adapter=SimulatedDeliveryAdapter(),
        artifact_base_dir=tmp_path / "alerts",
        environment_name="dev",
        transport_run_id="retry_001",
        now_fn=_fixed_now,
    )

    delivery = payload["events"][0]["delivery"]
    assert delivery["overall_status"] == "delivered"
    assert delivery["deliveries"][0]["status"] == "delivered"
    assert delivery["deliveries"][0]["attempt_count"] == 2


def test_alert_transport_suppresses_duplicate_delivery_by_idempotency_key(
    tmp_path: Path,
) -> None:
    config = default_route_config("ops-alerts")
    config["routes"]["ops-alerts.parser_drift.warn"] = {
        "primary_destinations": ["slack.ops-warn"],
        "escalation_destinations": [],
        "ack_required": True,
        "ack_timeout_seconds": 60,
        "escalation_schedule_seconds": [60],
    }

    alert = {
        "event_id": "evt_duplicate_001",
        "source_system": "parser_drift",
        "source_payload_type": "parser_drift_alert_payload_v1",
        "source_payload_path": "data/parser_alert.json",
        "source_payload_hash": "abc123",
        "source_run_id": "parser_run_001",
        "alert_id": "parser_run_001.warn",
        "alert_type": "parser_drift",
        "source_alert_type": "parser_drift",
        "severity": "warn",
        "generated_at_utc": "2026-03-27T12:00:00Z",
        "summary": "warn",
        "reason_codes": ["x"],
        "operator_actions": [],
        "source_routing_key": "ops.parser_drift.warn",
    }

    first_payload = run_alert_transport(
        route_group="ops-alerts",
        config=config,
        alerts=[alert],
        adapter=SimulatedDeliveryAdapter(),
        artifact_base_dir=tmp_path / "alerts",
        environment_name="dev",
        transport_run_id="dupe_001",
        now_fn=_fixed_now,
    )
    second_payload = run_alert_transport(
        route_group="ops-alerts",
        config=config,
        alerts=[alert],
        adapter=SimulatedDeliveryAdapter(),
        artifact_base_dir=tmp_path / "alerts",
        environment_name="dev",
        transport_run_id="dupe_002",
        now_fn=_fixed_now,
    )

    assert first_payload["events"][0]["delivery"]["overall_status"] == "delivered"
    duplicate_delivery = second_payload["events"][0]["delivery"]
    assert duplicate_delivery["overall_status"] == "suppressed_duplicate"
    assert duplicate_delivery["deliveries"][0]["attempt_count"] == 0


def test_alert_transport_cli_runs_end_to_end_with_alert_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ACCESSDANE_ENVIRONMENT", raising=False)
    monkeypatch.delenv("environment_name", raising=False)
    alert_path = tmp_path / "parser_alert.json"
    alert_path.write_text(
        json.dumps(_parser_alert_payload(), indent=2),
        encoding="utf-8",
    )
    output_path = tmp_path / "transport_out.json"

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "alert-transport",
            "--alert-file",
            str(alert_path),
            "--route-group",
            "ops-alerts",
            "--artifact-base-dir",
            str(tmp_path / "alerts"),
            "--transport-run-id",
            "cli_run_001",
            "--out",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["run"]["status"] == "succeeded"
    assert payload["summary"]["event_count"] == 1


def test_alert_transport_cli_rejects_out_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ACCESSDANE_ENVIRONMENT", raising=False)
    monkeypatch.delenv("environment_name", raising=False)
    alert_path = tmp_path / "parser_alert.json"
    alert_path.write_text(
        json.dumps(_parser_alert_payload(), indent=2),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "alert-transport",
            "--alert-file",
            str(alert_path),
            "--route-group",
            "ops-alerts",
            "--artifact-base-dir",
            str(tmp_path / "alerts"),
            "--transport-run-id",
            "cli_run_001",
            "--out",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 2
    stderr = getattr(result, "stderr", "")
    combined_output = f"{result.output}{stderr}"
    assert "Invalid value for '--out'" in combined_output


def test_load_canonical_alerts_skips_malformed_json_and_records_warning(
    tmp_path: Path,
) -> None:
    valid_alert_path = tmp_path / "valid_alert.json"
    valid_alert_path.write_text(
        json.dumps(_parser_alert_payload(), indent=2),
        encoding="utf-8",
    )
    malformed_path = tmp_path / "malformed.json"
    malformed_path.write_text("{not-json", encoding="utf-8")

    warnings: list[str] = []
    alerts = load_canonical_alerts(
        alert_files=[valid_alert_path, malformed_path],
        scheduler_files=[],
        parse_warnings=warnings,
        now_fn=_fixed_now,
    )

    assert len(alerts) == 1
    assert len(warnings) == 1
    assert "Skipped malformed JSON file" in warnings[0]


def test_load_route_config_rejects_route_group_mismatch(tmp_path: Path) -> None:
    config_path = tmp_path / "route_config.json"
    config_path.write_text(
        json.dumps(
            {
                "contract_version": "alert_route_config_v1",
                "route_group": "prod-alerts",
                "routes": {},
                "destinations": {},
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(TransportError, match="route_group mismatch"):
        load_route_config(route_group="ops-alerts", config_path=config_path)


def test_alert_transport_rejects_non_null_ack_timeout_when_not_required(
    tmp_path: Path,
) -> None:
    config = default_route_config("ops-alerts")
    config["routes"]["ops-alerts.parser_drift.info"] = {
        "primary_destinations": ["slack.ops-info"],
        "escalation_destinations": [],
        "ack_required": False,
        "ack_timeout_seconds": "invalid-non-null",
        "escalation_schedule_seconds": [],
    }

    alert = {
        "event_id": "evt_info_001",
        "source_system": "parser_drift",
        "source_payload_type": "parser_drift_alert_payload_v1",
        "source_payload_path": "data/parser_alert.json",
        "source_payload_hash": "abc123",
        "source_run_id": "parser_run_001",
        "alert_id": "parser_run_001.info",
        "alert_type": "parser_drift",
        "source_alert_type": "parser_drift",
        "severity": "info",
        "generated_at_utc": "2026-03-27T12:00:00Z",
        "summary": "info",
        "reason_codes": ["x"],
        "operator_actions": [],
        "source_routing_key": "ops.parser_drift.info",
    }

    with pytest.raises(
        TransportError,
        match="must set ack_timeout_seconds to null when ack_required=false",
    ):
        run_alert_transport(
            route_group="ops-alerts",
            config=config,
            alerts=[alert],
            adapter=SimulatedDeliveryAdapter(),
            artifact_base_dir=tmp_path / "alerts",
            environment_name="dev",
            transport_run_id="bad_ack_timeout_001",
            now_fn=_fixed_now,
        )


def test_alert_transport_rejects_missing_ack_required(tmp_path: Path) -> None:
    config = default_route_config("ops-alerts")
    config["routes"]["ops-alerts.parser_drift.warn"] = {
        "primary_destinations": ["slack.ops-warn"],
        "escalation_destinations": [],
        "ack_timeout_seconds": 60,
        "escalation_schedule_seconds": [],
    }

    alert = {
        "event_id": "evt_missing_ack_required_001",
        "source_system": "parser_drift",
        "source_payload_type": "parser_drift_alert_payload_v1",
        "source_payload_path": "data/parser_alert.json",
        "source_payload_hash": "abc123",
        "source_run_id": "parser_run_001",
        "alert_id": "parser_run_001.warn",
        "alert_type": "parser_drift",
        "source_alert_type": "parser_drift",
        "severity": "warn",
        "generated_at_utc": "2026-03-27T12:00:00Z",
        "summary": "warn",
        "reason_codes": ["x"],
        "operator_actions": [],
        "source_routing_key": "ops.parser_drift.warn",
    }

    with pytest.raises(TransportError, match="must include ack_required"):
        run_alert_transport(
            route_group="ops-alerts",
            config=config,
            alerts=[alert],
            adapter=SimulatedDeliveryAdapter(),
            artifact_base_dir=tmp_path / "alerts",
            environment_name="dev",
            transport_run_id="missing_ack_required_001",
            now_fn=_fixed_now,
        )


def test_alert_transport_rejects_missing_ack_timeout_key(tmp_path: Path) -> None:
    config = default_route_config("ops-alerts")
    config["routes"]["ops-alerts.parser_drift.warn"] = {
        "primary_destinations": ["slack.ops-warn"],
        "escalation_destinations": [],
        "ack_required": True,
        "escalation_schedule_seconds": [],
    }

    alert = {
        "event_id": "evt_missing_ack_timeout_001",
        "source_system": "parser_drift",
        "source_payload_type": "parser_drift_alert_payload_v1",
        "source_payload_path": "data/parser_alert.json",
        "source_payload_hash": "abc123",
        "source_run_id": "parser_run_001",
        "alert_id": "parser_run_001.warn",
        "alert_type": "parser_drift",
        "source_alert_type": "parser_drift",
        "severity": "warn",
        "generated_at_utc": "2026-03-27T12:00:00Z",
        "summary": "warn",
        "reason_codes": ["x"],
        "operator_actions": [],
        "source_routing_key": "ops.parser_drift.warn",
    }

    with pytest.raises(TransportError, match="must include ack_timeout_seconds"):
        run_alert_transport(
            route_group="ops-alerts",
            config=config,
            alerts=[alert],
            adapter=SimulatedDeliveryAdapter(),
            artifact_base_dir=tmp_path / "alerts",
            environment_name="dev",
            transport_run_id="missing_ack_timeout_001",
            now_fn=_fixed_now,
        )


def test_alert_transport_executes_all_due_escalation_offsets_once(
    tmp_path: Path,
) -> None:
    config = default_route_config("ops-alerts")
    config["routes"]["ops-alerts.parser_drift.warn"] = {
        "primary_destinations": ["slack.ops-warn"],
        "escalation_destinations": ["email.escalation"],
        "ack_required": True,
        "ack_timeout_seconds": 7200,
        "escalation_schedule_seconds": [900, 1800],
    }

    now_values = [
        datetime(2026, 3, 27, 12, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 27, 12, 40, 0, tzinfo=timezone.utc),
    ]
    index = -1

    def now_fn() -> datetime:
        nonlocal index
        if index < len(now_values) - 1:
            index += 1
        return now_values[index]

    alert = {
        "event_id": "evt_escalation_due_001",
        "source_system": "parser_drift",
        "source_payload_type": "parser_drift_alert_payload_v1",
        "source_payload_path": "data/parser_alert.json",
        "source_payload_hash": "abc123",
        "source_run_id": "parser_run_001",
        "alert_id": "parser_run_001.warn",
        "alert_type": "parser_drift",
        "source_alert_type": "parser_drift",
        "severity": "warn",
        "generated_at_utc": "2026-03-27T12:00:00Z",
        "summary": "warn",
        "reason_codes": ["x"],
        "operator_actions": [],
        "source_routing_key": "ops.parser_drift.warn",
    }

    payload = run_alert_transport(
        route_group="ops-alerts",
        config=config,
        alerts=[alert],
        adapter=SimulatedDeliveryAdapter(),
        artifact_base_dir=tmp_path / "alerts",
        environment_name="dev",
        transport_run_id="escalation_due_001",
        now_fn=now_fn,
    )

    escalation_records = [
        record
        for record in payload["events"][0]["delivery"]["deliveries"]
        if record["destination_id"] == "email.escalation"
    ]
    assert len(escalation_records) == 2
    assert sorted(
        record["escalation_offset_seconds"] for record in escalation_records
    ) == [900, 1800]
    assert all(record["status"] == "delivered" for record in escalation_records)


def test_alert_transport_skips_escalation_when_ack_expired(tmp_path: Path) -> None:
    config = default_route_config("ops-alerts")
    config["routes"]["ops-alerts.parser_drift.warn"] = {
        "primary_destinations": ["slack.ops-warn"],
        "escalation_destinations": ["email.escalation"],
        "ack_required": True,
        "ack_timeout_seconds": 60,
        "escalation_schedule_seconds": [0],
    }

    now_values = [
        datetime(2026, 3, 27, 12, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 27, 12, 10, 0, tzinfo=timezone.utc),
    ]
    index = -1

    def now_fn() -> datetime:
        nonlocal index
        if index < len(now_values) - 1:
            index += 1
        return now_values[index]

    alert = {
        "event_id": "evt_escalation_expired_001",
        "source_system": "parser_drift",
        "source_payload_type": "parser_drift_alert_payload_v1",
        "source_payload_path": "data/parser_alert.json",
        "source_payload_hash": "abc123",
        "source_run_id": "parser_run_001",
        "alert_id": "parser_run_001.warn",
        "alert_type": "parser_drift",
        "source_alert_type": "parser_drift",
        "severity": "warn",
        "generated_at_utc": "2026-03-27T12:00:00Z",
        "summary": "warn",
        "reason_codes": ["x"],
        "operator_actions": [],
        "source_routing_key": "ops.parser_drift.warn",
    }

    payload = run_alert_transport(
        route_group="ops-alerts",
        config=config,
        alerts=[alert],
        adapter=SimulatedDeliveryAdapter(),
        artifact_base_dir=tmp_path / "alerts",
        environment_name="dev",
        transport_run_id="escalation_expired_001",
        now_fn=now_fn,
    )

    event = payload["events"][0]
    assert event["acknowledgment"]["ack_state"] == "expired"
    escalation_records = [
        record
        for record in event["delivery"]["deliveries"]
        if record["destination_id"] == "email.escalation"
    ]
    assert len(escalation_records) == 1
    assert escalation_records[0]["status"] == "pending"
    assert escalation_records[0]["attempt_count"] == 0
