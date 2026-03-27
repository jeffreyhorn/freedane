from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.alert_transport import (
    SimulatedDeliveryAdapter,
    default_route_config,
    load_canonical_alerts,
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


def test_alert_transport_cli_runs_end_to_end_with_alert_file(tmp_path: Path) -> None:
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
