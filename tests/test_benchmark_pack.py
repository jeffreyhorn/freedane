from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import benchmark_pack, cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    CaseReview,
    FraudScore,
    Parcel,
    ParcelCharacteristic,
    ScoringRun,
)


def test_build_benchmark_pack_stable_comparison_and_segment_rollups(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "benchmark_pack.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    fixed_now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(benchmark_pack, "_now_utc", lambda: fixed_now)

    with session_scope(database_url) as session:
        _seed_scores(session)

    with session_scope(database_url) as session:
        payload_a = benchmark_pack.build_benchmark_pack(
            session,
            profile_name="daily_refresh",
            run_date="20260322",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            top_n=30,
            benchmark_run_id="benchmark_run_a",
        )

    assert payload_a["run"]["status"] == "succeeded"
    assert payload_a["comparison"]["comparable"] is False
    assert payload_a["comparison"]["non_comparable_reasons"] == [
        "no_comparable_baseline_found"
    ]
    assert payload_a["summary"]["coverage"]["queue_parcel_count"] == 30
    assert payload_a["summary"]["coverage"]["reviewed_case_count"] == 24
    assert payload_a["summary"]["risk_band_mix"]["high"]["count"] == 12
    assert payload_a["summary"]["risk_band_mix"]["medium"]["count"] == 10
    assert payload_a["summary"]["risk_band_mix"]["low"]["count"] == 8

    segment_ids = [segment["segment_id"] for segment in payload_a["segments"]]
    assert segment_ids == sorted(segment_ids)
    assert payload_a["scope"]["period_length_days"] == 7

    baseline_path = tmp_path / "baseline_benchmark_pack.json"
    baseline_path.write_text(json.dumps(payload_a, indent=2), encoding="utf-8")

    with session_scope(database_url) as session:
        payload_b = benchmark_pack.build_benchmark_pack(
            session,
            profile_name="daily_refresh",
            run_date="20260322",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            top_n=30,
            benchmark_run_id="benchmark_run_b",
            baseline_path=baseline_path,
        )

    assert payload_b["run"]["status"] == "succeeded"
    assert payload_b["comparison"]["comparable"] is True
    baseline_reference = payload_b["comparison"]["baseline_reference"]
    assert baseline_reference["profile_name"] == "daily_refresh"
    assert baseline_reference["feature_version"] == "feature_v1"
    assert baseline_reference["ruleset_version"] == "scoring_rules_v1"
    assert baseline_reference["top_n"] == 30
    assert baseline_reference["period_length_days"] == 7

    signals = payload_b["comparison"]["signals"]
    assert signals == sorted(
        signals,
        key=lambda signal: (
            signal["metric_key"],
            signal["family"],
            signal["signal_id"],
        ),
    )

    trend_payload = benchmark_pack.build_benchmark_trend_payload(payload_b)
    assert trend_payload["run_id"] == "benchmark_run_b"
    assert (
        trend_payload["overall_severity"] == payload_b["comparison"]["overall_severity"]
    )
    assert trend_payload["series"]


def test_build_benchmark_pack_marks_baseline_version_mismatch_non_comparable(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "benchmark_pack_version_mismatch.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    fixed_now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(benchmark_pack, "_now_utc", lambda: fixed_now)

    with session_scope(database_url) as session:
        _seed_scores(session)
        payload = benchmark_pack.build_benchmark_pack(
            session,
            profile_name="daily_refresh",
            run_date="20260322",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            top_n=30,
            benchmark_run_id="benchmark_run_base",
        )

    payload["run"]["version_tag"] = "benchmark_pack_v0"
    baseline_path = tmp_path / "baseline_version_mismatch.json"
    baseline_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with session_scope(database_url) as session:
        current_payload = benchmark_pack.build_benchmark_pack(
            session,
            profile_name="daily_refresh",
            run_date="20260322",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            top_n=30,
            benchmark_run_id="benchmark_run_current",
            baseline_path=baseline_path,
        )

    assert current_payload["comparison"]["comparable"] is False
    assert (
        "baseline_version_tag_mismatch"
        in current_payload["comparison"]["non_comparable_reasons"]
    )
    assert current_payload["comparison"]["signals"] == []


def test_benchmark_pack_cli_persists_canonical_artifacts_and_trend(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "benchmark_pack_cli.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    fixed_now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(benchmark_pack, "_now_utc", lambda: fixed_now)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    with session_scope(database_url) as session:
        _seed_scores(session)

    artifact_base_dir = tmp_path / "benchmark_packs"
    out_path = tmp_path / "benchmark_pack_out.json"
    trend_out_path = tmp_path / "benchmark_trend_out.json"
    alert_out_path = tmp_path / "benchmark_alert_out.json"
    alert_out_path.write_text('{"stale": true}\n', encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "benchmark-pack",
            "--artifact-base-dir",
            str(artifact_base_dir),
            "--run-date",
            "20260322",
            "--run-id",
            "benchmark_run_cli",
            "--top-n",
            "30",
            "--out",
            str(out_path),
            "--trend-out",
            str(trend_out_path),
            "--alert-out",
            str(alert_out_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    out_payload = json.loads(out_path.read_text(encoding="utf-8"))
    trend_payload = json.loads(trend_out_path.read_text(encoding="utf-8"))

    assert out_payload["run"]["run_id"] == "benchmark_run_cli"
    assert out_payload["run"]["status"] == "succeeded"
    assert trend_payload["run_id"] == "benchmark_run_cli"
    assert not alert_out_path.exists()

    benchmark_pack_path = (
        artifact_base_dir
        / "20260322"
        / "daily_refresh"
        / "benchmark_run_cli"
        / "benchmark_pack.json"
    )
    benchmark_trend_path = (
        artifact_base_dir
        / "20260322"
        / "daily_refresh"
        / "benchmark_run_cli"
        / "benchmark_pack_trend.json"
    )
    latest_benchmark_pack_path = (
        artifact_base_dir
        / "latest"
        / "daily_refresh"
        / "feature_v1"
        / "scoring_rules_v1"
        / "latest_benchmark_pack.json"
    )

    assert benchmark_pack_path.exists()
    assert benchmark_trend_path.exists()
    assert latest_benchmark_pack_path.exists()


def test_benchmark_pack_cli_rejects_non_file_alert_out_path(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "benchmark_pack_alert_dir.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    with session_scope(database_url) as session:
        _seed_scores(session)

    alert_out_dir = tmp_path / "benchmark_alert_out_dir"
    alert_out_dir.mkdir(parents=True, exist_ok=True)
    out_path = tmp_path / "benchmark_pack_out.json"

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "benchmark-pack",
            "--run-date",
            "20260322",
            "--run-id",
            "benchmark_run_alert_dir",
            "--top-n",
            "30",
            "--out",
            str(out_path),
            "--alert-out",
            str(alert_out_dir),
        ],
    )

    assert result.exit_code != 0
    assert "--alert-out must reference a file path" in result.output
    assert not out_path.exists()


def test_benchmark_alert_payload_sanitizes_reason_code_for_routing_key() -> None:
    payload = {
        "run": {
            "status": "succeeded",
            "finished_at": "2026-03-22T12:00:00Z",
        },
        "scope": {
            "profile_name": "daily_refresh",
            "feature_version": "feature_v1",
            "ruleset_version": "scoring_rules_v1",
        },
        "comparison": {
            "overall_severity": "warn",
        },
        "alerts": [
            {
                "id": "alert-1",
                "level": "warn",
                "code": (
                    "segment_mix.beyond_tolerance."
                    "segment.55097::res.risk_band_mix.high.rate_delta_abs"
                ),
                "message": (
                    "Benchmark change beyond tolerance on "
                    "segment.55097::res.risk_band_mix.high.rate_delta_abs."
                ),
                "scope": "segment",
                "created_at": "2026-03-22T12:00:00Z",
                "segment_id": "55097::res",
                "signal_id": "signal-1",
            }
        ],
    }

    alert_payload = benchmark_pack.build_alert_payload_from_benchmark_pack(payload)

    assert alert_payload is not None
    alert = alert_payload["alerts"][0]
    code = alert["reason_codes"][0]
    assert ":" not in code
    assert len(alert["routing_key"].split(":")) == 4
    assert alert["routing_key"].split(":")[3] == code
    assert alert["context"]["segment_id"] == "55097::res"


def test_benchmark_pack_cli_invalid_profile_name_returns_bad_parameter(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "benchmark_pack_invalid_profile.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "benchmark-pack",
            "--profile-name",
            "daily:refresh",
            "--run-date",
            "20260322",
        ],
    )

    assert result.exit_code != 0
    assert "Invalid profile_name" in result.output


def test_build_benchmark_pack_marks_failed_baseline_non_comparable(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "benchmark_pack_failed_baseline.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    fixed_now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(benchmark_pack, "_now_utc", lambda: fixed_now)

    with session_scope(database_url) as session:
        _seed_scores(session)
        baseline_payload = benchmark_pack.build_benchmark_pack(
            session,
            profile_name="daily_refresh",
            run_date="20260322",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            top_n=30,
            benchmark_run_id="benchmark_run_baseline",
        )

    baseline_payload["run"]["status"] = "failed"
    baseline_path = tmp_path / "baseline_failed.json"
    baseline_path.write_text(json.dumps(baseline_payload, indent=2), encoding="utf-8")

    with session_scope(database_url) as session:
        current_payload = benchmark_pack.build_benchmark_pack(
            session,
            profile_name="daily_refresh",
            run_date="20260322",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            top_n=30,
            benchmark_run_id="benchmark_run_current",
            baseline_path=baseline_path,
        )

    assert current_payload["run"]["status"] == "succeeded"
    assert current_payload["comparison"]["comparable"] is False
    assert current_payload["comparison"]["signals"] == []
    assert (
        "baseline_run_failed" in current_payload["comparison"]["non_comparable_reasons"]
    )


def test_benchmark_pack_cli_does_not_persist_canonical_artifacts_on_failed_run(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "benchmark_pack_failed_cli.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    artifact_base_dir = tmp_path / "benchmark_packs"
    out_path = tmp_path / "benchmark_pack_failed_out.json"

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "benchmark-pack",
            "--artifact-base-dir",
            str(artifact_base_dir),
            "--run-date",
            "20260322",
            "--run-id",
            "benchmark_run_failed",
            "--ruleset-version",
            "unsupported_ruleset",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 1
    out_payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert out_payload["run"]["status"] == "failed"
    assert out_payload["run"]["run_persisted"] is True
    assert not (
        artifact_base_dir
        / "20260322"
        / "daily_refresh"
        / "benchmark_run_failed"
        / "benchmark_pack.json"
    ).exists()
    assert not (
        artifact_base_dir
        / "latest"
        / "daily_refresh"
        / "feature_v1"
        / "unsupported_ruleset"
        / "latest_benchmark_pack.json"
    ).exists()


def test_benchmark_pack_cli_failed_run_without_outputs_marks_not_persisted(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "benchmark_pack_failed_cli_stdout.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "benchmark-pack",
            "--run-date",
            "20260322",
            "--run-id",
            "benchmark_run_failed_stdout",
            "--ruleset-version",
            "unsupported_ruleset",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["run"]["status"] == "failed"
    assert payload["run"]["run_persisted"] is False


def test_persist_benchmark_artifacts_skips_latest_updates_for_failed_payload(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "benchmark_pack_persist_gating.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)
    fixed_now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(benchmark_pack, "_now_utc", lambda: fixed_now)

    with session_scope(database_url) as session:
        _seed_scores(session)
        success_payload = benchmark_pack.build_benchmark_pack(
            session,
            profile_name="daily_refresh",
            run_date="20260322",
            feature_version="feature_v1",
            ruleset_version="scoring_rules_v1",
            top_n=30,
            benchmark_run_id="benchmark_run_success",
        )

    artifact_base_dir = tmp_path / "benchmark_packs"
    success_trend = benchmark_pack.build_benchmark_trend_payload(success_payload)
    benchmark_pack.persist_benchmark_artifacts(
        success_payload,
        artifact_base_dir=artifact_base_dir,
        trend_payload=success_trend,
        alert_payload=None,
    )

    latest_path = (
        artifact_base_dir
        / "latest"
        / "daily_refresh"
        / "feature_v1"
        / "scoring_rules_v1"
        / "latest_benchmark_pack.json"
    )
    latest_before = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest_before["run"]["run_id"] == "benchmark_run_success"

    failed_payload = copy.deepcopy(success_payload)
    failed_payload["run"]["run_id"] = "benchmark_run_failed_persist"
    failed_payload["run"]["status"] = "failed"
    failed_payload["error"] = {"code": "forced_failure", "message": "forced failure"}
    failed_payload["comparison"] = {
        "baseline_reference": None,
        "comparable": False,
        "non_comparable_reasons": ["run_failed"],
        "signals": [],
        "overall_severity": "ok",
    }
    failed_trend = benchmark_pack.build_benchmark_trend_payload(failed_payload)
    benchmark_pack.persist_benchmark_artifacts(
        failed_payload,
        artifact_base_dir=artifact_base_dir,
        trend_payload=failed_trend,
        alert_payload=None,
    )

    latest_after = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest_after["run"]["run_id"] == "benchmark_run_success"
    assert (
        artifact_base_dir
        / "20260322"
        / "daily_refresh"
        / "benchmark_run_failed_persist"
        / "benchmark_pack.json"
    ).exists()


def test_ignore_state_for_signal_distinguishes_missing_baseline_and_current() -> None:
    ignored, reason = benchmark_pack._ignore_state_for_signal(
        metric_key="coverage.review_rate",
        baseline_value=None,
        current_value=None,
        sample_size=0,
        current_segment_by_id={},
    )

    assert ignored is True
    assert reason == "missing_baseline_and_current_metric"


def _seed_scores(session) -> None:
    run = ScoringRun(
        run_type="score_fraud",
        status="succeeded",
        version_tag="scoring_rules_v1",
        scope_hash=None,
        scope_json={},
        config_json={},
        input_summary_json={},
        output_summary_json={},
    )
    session.add(run)
    session.flush()

    dispositions = [
        "confirmed_issue",
        "false_positive",
        "inconclusive",
        "needs_field_review",
        "duplicate_case",
    ]

    for index in range(30):
        parcel_id = f"parcel_{index:03d}"
        session.add(Parcel(id=parcel_id))

        if index < 12:
            risk_band = "high"
            score_value = Decimal("91.00")
        elif index < 22:
            risk_band = "medium"
            score_value = Decimal("72.00")
        else:
            risk_band = "low"
            score_value = Decimal("48.00")

        score = FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id=parcel_id,
            year=2026,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=score_value,
            risk_band=risk_band,
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
        )
        session.add(score)
        session.flush()

        session.add(
            ParcelCharacteristic(
                parcel_id=parcel_id,
                source_fetch_id=None,
                state_municipality_code="55097" if index % 2 == 0 else "55098",
                current_valuation_classification="res" if index % 3 else "com",
            )
        )

        if index < 24:
            disposition = dispositions[index % len(dispositions)]
            session.add(
                CaseReview(
                    parcel_id=parcel_id,
                    year=2026,
                    score_id=score.id,
                    run_id=run.id,
                    feature_version="feature_v1",
                    ruleset_version="scoring_rules_v1",
                    status="resolved",
                    disposition=disposition,
                    reviewer="analyst_1",
                    assigned_reviewer=None,
                    note=None,
                    evidence_links_json=[],
                )
            )
