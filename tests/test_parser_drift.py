from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import Fetch, Parcel
from accessdane_audit.parser_drift import (
    FIELD_COVERAGE_METRIC_KEYS,
    SELECTOR_MISS_METRIC_KEYS,
    TAX_DETAIL_PRESENCE_METRIC_KEYS,
    build_alert_payload_from_diff,
    build_parser_drift_diff,
    build_parser_drift_snapshot,
)
from accessdane_audit.profiling import PROFILED_TAX_DETAIL_FIELDS


def _make_snapshot(
    *,
    snapshot_id: str,
    successful_fetches: int = 200,
    parcels: int = 200,
    source_parcel_years: int = 200,
    detail_tax_records: int = 200,
    coverage_override: dict[str, float | None] | None = None,
    selector_override: dict[str, float | None] | None = None,
    tax_presence_override: dict[str, float | None] | None = None,
    extraction_override: dict[str, float | None] | None = None,
) -> dict[str, object]:
    coverage = {
        metric_key.split(".", 1)[1]: 0.95 for metric_key in FIELD_COVERAGE_METRIC_KEYS
    }
    selector = {
        metric_key.split(".", 1)[1]: 0.01 for metric_key in SELECTOR_MISS_METRIC_KEYS
    }
    tax_presence = {field_name: 0.99 for field_name in PROFILED_TAX_DETAIL_FIELDS}
    extraction = {field_name: 0.01 for field_name in PROFILED_TAX_DETAIL_FIELDS}

    if coverage_override:
        coverage.update(coverage_override)
    if selector_override:
        selector.update(selector_override)
    if tax_presence_override:
        tax_presence.update(tax_presence_override)
    if extraction_override:
        extraction.update(extraction_override)

    return {
        "run": {
            "run_type": "parser_drift_snapshot",
            "version_tag": "parser_drift_v1",
            "run_id": snapshot_id,
            "snapshot_id": snapshot_id,
            "status": "succeeded",
            "run_persisted": True,
            "generated_at": "2026-03-17T00:00:00Z",
        },
        "scope": {
            "profile_name": "daily_refresh",
            "run_date": "20260317",
            "feature_version": "feature_v1",
            "ruleset_version": "scoring_rules_v1",
            "artifact_root": "data/refresh_runs",
            "parcel_filter_count": None,
        },
        "metrics": {
            "counts": {
                "successful_fetches": successful_fetches,
                "parcels": parcels,
                "source_parcel_years": source_parcel_years,
                "detail_tax_records": detail_tax_records,
            },
            "field_coverage": coverage,
            "selector_miss": selector,
            "tax_detail_field_presence": {
                field_name: {
                    "rate": tax_presence[field_name],
                    "count": (
                        int(round(tax_presence[field_name] * detail_tax_records))
                        if tax_presence[field_name] is not None
                        else 0
                    ),
                }
                for field_name in PROFILED_TAX_DETAIL_FIELDS
            },
            "extraction_null_rate": {
                field_name: {
                    "rate": extraction[field_name],
                    "null_count": (
                        int(round(extraction[field_name] * detail_tax_records))
                        if extraction[field_name] is not None
                        else 0
                    ),
                    "eligible_record_count": detail_tax_records,
                }
                for field_name in PROFILED_TAX_DETAIL_FIELDS
            },
        },
        "diagnostics": {"warnings": [], "source_artifacts": []},
        "error": None,
    }


def test_parser_drift_diff_stable_snapshot_has_ok_overall_severity() -> None:
    baseline = _make_snapshot(snapshot_id="baseline")
    current = _make_snapshot(snapshot_id="current")

    payload = build_parser_drift_diff(
        baseline_snapshot=baseline,
        current_snapshot=current,
        baseline_artifact_path="baseline.json",
        current_artifact_path="current.json",
        diff_id="diff_stable",
    )

    assert payload["summary"]["overall_severity"] == "ok"
    assert payload["summary"]["signal_error_count"] == 0
    assert payload["alerts"] == []


def test_parser_drift_diff_caps_error_to_warn_for_small_current_sample() -> None:
    baseline = _make_snapshot(
        snapshot_id="baseline",
        successful_fetches=200,
        selector_override={"assessment_fetch_rate": 0.0},
    )
    current = _make_snapshot(
        snapshot_id="current",
        successful_fetches=10,
        selector_override={"assessment_fetch_rate": 0.80},
    )

    payload = build_parser_drift_diff(
        baseline_snapshot=baseline,
        current_snapshot=current,
        baseline_artifact_path="baseline.json",
        current_artifact_path="current.json",
        diff_id="diff_small_sample",
    )
    alert_payload = build_alert_payload_from_diff(payload)

    target_signal = next(
        signal
        for signal in payload["signals"]
        if signal["metric_key"] == "selector_miss.assessment_fetch_rate"
    )
    assert target_signal["severity"] == "warn"
    assert payload["summary"]["overall_severity"] == "warn"
    assert payload["alerts"][0]["routing_key"] == "ops.parser_drift.warn"
    assert alert_payload is not None
    assert alert_payload["alert"]["severity"] == "warn"
    assert alert_payload["impacted_signals"]


def test_parser_drift_diff_selector_regression_produces_error_alert_payload() -> None:
    baseline = _make_snapshot(
        snapshot_id="baseline",
        successful_fetches=200,
        selector_override={"tax_fetch_rate": 0.00},
    )
    current = _make_snapshot(
        snapshot_id="current",
        successful_fetches=200,
        selector_override={"tax_fetch_rate": 0.30},
    )

    diff_payload = build_parser_drift_diff(
        baseline_snapshot=baseline,
        current_snapshot=current,
        baseline_artifact_path="baseline.json",
        current_artifact_path="current.json",
        diff_id="diff_selector_break",
    )
    alert_payload = build_alert_payload_from_diff(diff_payload)

    assert diff_payload["summary"]["overall_severity"] == "error"
    assert diff_payload["alerts"][0]["routing_key"] == "ops.parser_drift.error"
    assert alert_payload is not None
    assert alert_payload["alert"]["severity"] == "error"
    assert alert_payload["impacted_signals"]


def test_parser_drift_diff_threshold_boundary_uses_rounded_delta() -> None:
    baseline = _make_snapshot(
        snapshot_id="baseline",
        coverage_override={"parcel_lineage_parcel_rate": 0.95},
    )
    current = _make_snapshot(
        snapshot_id="current",
        coverage_override={"parcel_lineage_parcel_rate": 0.90},
    )

    payload = build_parser_drift_diff(
        baseline_snapshot=baseline,
        current_snapshot=current,
        baseline_artifact_path="baseline.json",
        current_artifact_path="current.json",
        diff_id="diff_threshold_boundary",
    )

    target_signal = next(
        signal
        for signal in payload["signals"]
        if signal["metric_key"] == "coverage.parcel_lineage_parcel_rate"
    )
    assert target_signal["delta_absolute"] == -0.05
    assert target_signal["severity"] == "error"


def test_build_alert_payload_from_diff_returns_none_for_invalid_overall_severity() -> (
    None
):
    payload = {
        "summary": {"overall_severity": None},
        "run": {"status": "succeeded"},
        "alerts": [{"alert_id": "x"}],
        "signals": [],
    }
    assert build_alert_payload_from_diff(payload) is None


def test_build_parser_drift_snapshot_includes_required_metric_shapes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "parser_drift_snapshot.sqlite"
    database_url = f"sqlite:///{db_path}"

    init_db(database_url)
    with session_scope(database_url) as session:
        session.add(Parcel(id="p1"))
        session.add(
            Fetch(parcel_id="p1", url="https://example.test/p1", status_code=200)
        )

    with session_scope(database_url) as session:
        payload = build_parser_drift_snapshot(
            session,
            profile_name="daily_refresh",
            run_date="20260317",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            artifact_root="data/refresh_runs",
            source_artifacts=[],
        )

    assert payload["run"]["run_type"] == "parser_drift_snapshot"
    assert payload["error"] is None
    assert payload["metrics"]["counts"]["successful_fetches"] == 1
    assert set(payload["metrics"]["selector_miss"].keys()) == {
        "assessment_fetch_rate",
        "tax_fetch_rate",
        "payment_fetch_rate",
    }
    assert set(payload["metrics"]["tax_detail_field_presence"].keys()) == set(
        PROFILED_TAX_DETAIL_FIELDS
    )
    assert set(payload["metrics"]["extraction_null_rate"].keys()) == set(
        PROFILED_TAX_DETAIL_FIELDS
    )


def test_parser_drift_snapshot_cli_writes_payload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "parser_drift_cli_snapshot.sqlite"
    database_url = f"sqlite:///{db_path}"
    out_path = tmp_path / "snapshot.json"

    init_db(database_url)
    with session_scope(database_url) as session:
        session.add(Parcel(id="p1"))
        session.add(
            Fetch(parcel_id="p1", url="https://example.test/p1", status_code=200)
        )

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["parser-drift-snapshot", "--out", str(out_path)])

    assert result.exit_code == 0
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["run"]["run_type"] == "parser_drift_snapshot"
    assert payload["run"]["run_persisted"] is True


def test_parser_drift_diff_cli_writes_diff_and_alert_payload(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "parser_drift_cli_diff.sqlite"
    database_url = f"sqlite:///{db_path}"
    baseline_path = tmp_path / "baseline_snapshot.json"
    current_path = tmp_path / "current_snapshot.json"
    diff_path = tmp_path / "diff.json"
    alert_path = tmp_path / "alert.json"

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    baseline_payload = _make_snapshot(
        snapshot_id="baseline_cli",
        selector_override={"tax_fetch_rate": 0.00},
    )
    current_payload = _make_snapshot(
        snapshot_id="current_cli",
        selector_override={"tax_fetch_rate": 0.35},
    )
    baseline_path.write_text(json.dumps(baseline_payload, indent=2), encoding="utf-8")
    current_path.write_text(json.dumps(current_payload, indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "parser-drift-diff",
            "--baseline",
            str(baseline_path),
            "--current",
            str(current_path),
            "--out",
            str(diff_path),
            "--alert-out",
            str(alert_path),
        ],
    )

    assert result.exit_code == 0
    diff_payload = json.loads(diff_path.read_text(encoding="utf-8"))
    alert_payload = json.loads(alert_path.read_text(encoding="utf-8"))
    assert diff_payload["summary"]["overall_severity"] == "error"
    assert diff_payload["alerts"][0]["routing_key"] == "ops.parser_drift.error"
    assert alert_payload["alert"]["severity"] == "error"


def test_parser_drift_diff_cli_removes_stale_alert_out_when_no_alert(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "parser_drift_cli_diff_stale_alert.sqlite"
    database_url = f"sqlite:///{db_path}"
    baseline_path = tmp_path / "baseline_snapshot_stale_alert.json"
    current_path = tmp_path / "current_snapshot_stale_alert.json"
    diff_path = tmp_path / "diff_stale_alert.json"
    alert_path = tmp_path / "alert_stale.json"

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    baseline_payload = _make_snapshot(snapshot_id="baseline_stale")
    current_payload = _make_snapshot(snapshot_id="current_stale")
    baseline_path.write_text(json.dumps(baseline_payload, indent=2), encoding="utf-8")
    current_path.write_text(json.dumps(current_payload, indent=2), encoding="utf-8")
    alert_path.write_text('{"stale": true}\n', encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "parser-drift-diff",
            "--baseline",
            str(baseline_path),
            "--current",
            str(current_path),
            "--out",
            str(diff_path),
            "--alert-out",
            str(alert_path),
        ],
    )

    assert result.exit_code == 0
    assert diff_path.exists()
    assert not alert_path.exists()


def test_parser_drift_diff_cli_rejects_non_file_alert_out_path(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "parser_drift_cli_diff_alert_dir.sqlite"
    database_url = f"sqlite:///{db_path}"
    baseline_path = tmp_path / "baseline_snapshot_alert_dir.json"
    current_path = tmp_path / "current_snapshot_alert_dir.json"
    diff_path = tmp_path / "diff_alert_dir.json"
    alert_path = tmp_path / "alert_out_dir"

    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    baseline_payload = _make_snapshot(snapshot_id="baseline_alert_dir")
    current_payload = _make_snapshot(snapshot_id="current_alert_dir")
    baseline_path.write_text(json.dumps(baseline_payload, indent=2), encoding="utf-8")
    current_path.write_text(json.dumps(current_payload, indent=2), encoding="utf-8")
    alert_path.mkdir(parents=True, exist_ok=True)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "parser-drift-diff",
            "--baseline",
            str(baseline_path),
            "--current",
            str(current_path),
            "--out",
            str(diff_path),
            "--alert-out",
            str(alert_path),
        ],
    )

    assert result.exit_code != 0
    assert "--alert-out must reference a file path" in result.output


def test_parser_drift_metric_key_sets_are_in_sync() -> None:
    assert set(SELECTOR_MISS_METRIC_KEYS) == {
        "selector_miss.assessment_fetch_rate",
        "selector_miss.tax_fetch_rate",
        "selector_miss.payment_fetch_rate",
    }
    assert set(TAX_DETAIL_PRESENCE_METRIC_KEYS) == {
        f"tax_detail_field_presence.{field_name}.rate"
        for field_name in PROFILED_TAX_DETAIL_FIELDS
    }
