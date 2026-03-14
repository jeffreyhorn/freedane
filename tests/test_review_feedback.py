from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    CaseReview,
    FraudFlag,
    FraudScore,
    Parcel,
    ScoringRun,
)


def test_review_feedback_cli_is_deterministic_and_contract_stable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "review_feedback.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_review_feedback_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result_a = runner.invoke(cli.app, ["review-feedback"])
    result_b = runner.invoke(cli.app, ["review-feedback"])

    assert result_a.exit_code == 0, result_a.stdout
    assert result_b.exit_code == 0, result_b.stdout
    assert result_a.stdout == result_b.stdout

    payload = json.loads(result_a.stdout)
    assert list(payload.keys()) == [
        "run",
        "request",
        "summary",
        "risk_band_outcomes",
        "reason_code_outcomes",
        "rule_outcome_slices",
        "recommendations",
        "artifacts",
        "diagnostics",
        "error",
    ]
    assert payload["run"] == {
        "run_id": None,
        "run_persisted": False,
        "run_type": "review_feedback",
        "version_tag": "review_feedback_v1",
        "status": "succeeded",
    }
    assert payload["request"] == {
        "feature_version": "feature_v1",
        "ruleset_version": "scoring_rules_v1",
    }
    assert payload["summary"] == {
        "reviewed_case_count": 3,
        "false_positive_count": 2,
        "risk_band_bucket_count": 2,
        "reason_code_bucket_count": 2,
        "rule_outcome_slice_count": 3,
    }

    assert payload["risk_band_outcomes"] == [
        {
            "risk_band": "high",
            "reviewed_case_count": 2,
            "false_positive_count": 1,
            "false_positive_rate": "0.5000",
            "disposition_counts": {
                "confirmed_issue": 1,
                "false_positive": 1,
            },
            "median_score_value": "77.50",
        },
        {
            "risk_band": "medium",
            "reviewed_case_count": 1,
            "false_positive_count": 1,
            "false_positive_rate": "1.0000",
            "disposition_counts": {
                "false_positive": 1,
            },
            "median_score_value": "60.00",
        },
    ]

    assert payload["reason_code_outcomes"] == [
        {
            "reason_code": "R_PERMIT_GAP",
            "reviewed_case_count": 2,
            "false_positive_count": 2,
            "false_positive_rate": "1.0000",
            "disposition_counts": {
                "false_positive": 2,
            },
            "median_score_value": "72.50",
            "threshold_signatures": [
                {
                    "metric_name": "permit_adjusted_gap",
                    "comparison_operator": "gte",
                    "threshold_value": "20000",
                }
            ],
            "risk_band_breakdown": [
                {
                    "risk_band": "high",
                    "reviewed_case_count": 1,
                    "false_positive_count": 1,
                    "false_positive_rate": "1.0000",
                },
                {
                    "risk_band": "medium",
                    "reviewed_case_count": 1,
                    "false_positive_count": 1,
                    "false_positive_rate": "1.0000",
                },
            ],
        },
        {
            "reason_code": "R_RATIO_LOW",
            "reviewed_case_count": 2,
            "false_positive_count": 1,
            "false_positive_rate": "0.5000",
            "disposition_counts": {
                "confirmed_issue": 1,
                "false_positive": 1,
            },
            "median_score_value": "77.50",
            "threshold_signatures": [
                {
                    "metric_name": "assessment_to_sale_ratio",
                    "comparison_operator": "lt",
                    "threshold_value": "0.55",
                }
            ],
            "risk_band_breakdown": [
                {
                    "risk_band": "high",
                    "reviewed_case_count": 2,
                    "false_positive_count": 1,
                    "false_positive_rate": "0.5000",
                },
            ],
        },
    ]

    assert payload["rule_outcome_slices"] == [
        {
            "reason_code": "R_PERMIT_GAP",
            "risk_band": "high",
            "reviewed_case_count": 1,
            "false_positive_count": 1,
            "false_positive_rate": "1.0000",
            "disposition_counts": {"false_positive": 1},
            "median_score_value": "85.00",
            "threshold_signatures": [
                {
                    "metric_name": "permit_adjusted_gap",
                    "comparison_operator": "gte",
                    "threshold_value": "20000",
                }
            ],
        },
        {
            "reason_code": "R_PERMIT_GAP",
            "risk_band": "medium",
            "reviewed_case_count": 1,
            "false_positive_count": 1,
            "false_positive_rate": "1.0000",
            "disposition_counts": {"false_positive": 1},
            "median_score_value": "60.00",
            "threshold_signatures": [
                {
                    "metric_name": "permit_adjusted_gap",
                    "comparison_operator": "gte",
                    "threshold_value": "20000",
                }
            ],
        },
        {
            "reason_code": "R_RATIO_LOW",
            "risk_band": "high",
            "reviewed_case_count": 2,
            "false_positive_count": 1,
            "false_positive_rate": "0.5000",
            "disposition_counts": {
                "confirmed_issue": 1,
                "false_positive": 1,
            },
            "median_score_value": "77.50",
            "threshold_signatures": [
                {
                    "metric_name": "assessment_to_sale_ratio",
                    "comparison_operator": "lt",
                    "threshold_value": "0.55",
                }
            ],
        },
    ]

    assert payload["recommendations"] == {
        "threshold_tuning_candidates": [
            {
                "reason_code": "R_PERMIT_GAP",
                "reviewed_case_count": 2,
                "false_positive_rate": "1.0000",
                "suggested_action": "tighten_threshold",
            },
            {
                "reason_code": "R_RATIO_LOW",
                "reviewed_case_count": 2,
                "false_positive_rate": "0.5000",
                "suggested_action": "tighten_threshold",
            },
        ],
        "exclusion_tuning_candidates": [
            {
                "reason_code": "R_PERMIT_GAP",
                "reviewed_case_count": 2,
                "false_positive_rate": "1.0000",
                "suggested_action": "evaluate_exclusion_candidate",
            }
        ],
    }

    artifacts = payload["artifacts"]
    assert artifacts["sql_contract_version"] == "review_feedback_sql_v1"
    assert set(artifacts["sql_queries"]) == {
        "risk_band_outcomes",
        "reason_code_outcomes",
        "reason_code_risk_slices",
    }
    assert "feature_v1" in artifacts["sql_queries"]["risk_band_outcomes"]
    assert "ruleset_version" in artifacts["sql_queries"]["reason_code_outcomes"]
    assert "-- risk_band_outcomes" in artifacts["sql_script"]
    assert payload["error"] is None


def test_review_feedback_cli_writes_sql_artifact(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "review_feedback_sql_out.sqlite"
    sql_out = tmp_path / "review_feedback.sql"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_review_feedback_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["review-feedback", "--sql-out", str(sql_out)],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)

    assert payload["run"]["status"] == "succeeded"
    assert payload["artifacts"]["sql_out_path"] == str(sql_out)
    sql_text = sql_out.read_text(encoding="utf-8")
    assert "-- risk_band_outcomes" in sql_text
    assert "-- reason_code_outcomes" in sql_text
    assert "-- reason_code_risk_slices" in sql_text


def test_review_feedback_cli_unsupported_ruleset_returns_exit_one_json(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "review_feedback_ruleset.sqlite"
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
        ["review-feedback", "--ruleset-version", "scoring_rules_v999"],
    )
    assert result.exit_code == 1

    payload = json.loads(result.stdout)
    assert payload["run"]["status"] == "failed"
    assert payload["error"]["code"] == "unsupported_version_selector"


def _seed_review_feedback_fixture(session) -> None:
    run = ScoringRun(
        run_type="score_fraud",
        status="succeeded",
        version_tag="scoring_rules_v1",
        scope_json={},
        config_json={},
    )
    session.add(run)
    session.flush()

    session.add_all(
        [
            Parcel(id="P-1"),
            Parcel(id="P-2"),
            Parcel(id="P-3"),
            Parcel(id="P-4"),
        ]
    )

    score_rows = [
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-1",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("85.00"),
            risk_band="high",
            requires_review=True,
            reason_code_count=2,
            score_summary_json={},
            scored_at=datetime(2025, 8, 1, 12, 0, tzinfo=timezone.utc),
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-2",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("70.00"),
            risk_band="high",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
            scored_at=datetime(2025, 8, 2, 12, 0, tzinfo=timezone.utc),
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-3",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("60.00"),
            risk_band="medium",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
            scored_at=datetime(2025, 8, 3, 12, 0, tzinfo=timezone.utc),
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-4",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("50.00"),
            risk_band="low",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
            scored_at=datetime(2025, 8, 4, 12, 0, tzinfo=timezone.utc),
        ),
    ]
    session.add_all(score_rows)
    session.flush()

    session.add_all(
        [
            FraudFlag(
                run_id=run.id,
                score_id=score_rows[0].id,
                parcel_id="P-1",
                year=2025,
                ruleset_version="scoring_rules_v1",
                reason_code="R_RATIO_LOW",
                reason_rank=1,
                severity_weight=Decimal("10.0000"),
                metric_name="assessment_to_sale_ratio",
                metric_value="0.50",
                threshold_value="0.55",
                comparison_operator="lt",
                explanation="ratio below threshold",
                source_refs_json={},
            ),
            FraudFlag(
                run_id=run.id,
                score_id=score_rows[0].id,
                parcel_id="P-1",
                year=2025,
                ruleset_version="scoring_rules_v1",
                reason_code="R_PERMIT_GAP",
                reason_rank=2,
                severity_weight=Decimal("5.0000"),
                metric_name="permit_adjusted_gap",
                metric_value="25000",
                threshold_value="20000",
                comparison_operator="gte",
                explanation="permit gap above threshold",
                source_refs_json={},
            ),
            FraudFlag(
                run_id=run.id,
                score_id=score_rows[1].id,
                parcel_id="P-2",
                year=2025,
                ruleset_version="scoring_rules_v1",
                reason_code="R_RATIO_LOW",
                reason_rank=1,
                severity_weight=Decimal("8.0000"),
                metric_name="assessment_to_sale_ratio",
                metric_value="0.52",
                threshold_value="0.55",
                comparison_operator="lt",
                explanation="ratio below threshold",
                source_refs_json={},
            ),
            FraudFlag(
                run_id=run.id,
                score_id=score_rows[2].id,
                parcel_id="P-3",
                year=2025,
                ruleset_version="scoring_rules_v1",
                reason_code="R_PERMIT_GAP",
                reason_rank=1,
                severity_weight=Decimal("6.0000"),
                metric_name="permit_adjusted_gap",
                metric_value="22000",
                threshold_value="20000",
                comparison_operator="gte",
                explanation="permit gap above threshold",
                source_refs_json={},
            ),
        ]
    )

    session.add_all(
        [
            CaseReview(
                parcel_id="P-1",
                year=2025,
                score_id=score_rows[0].id,
                run_id=run.id,
                feature_version="feature_v1",
                ruleset_version="scoring_rules_v1",
                status="resolved",
                disposition="false_positive",
                reviewer="Analyst A",
                assigned_reviewer="Analyst A",
                note="False positive",
                evidence_links_json=[],
            ),
            CaseReview(
                parcel_id="P-2",
                year=2025,
                score_id=score_rows[1].id,
                run_id=run.id,
                feature_version="feature_v1",
                ruleset_version="scoring_rules_v1",
                status="closed",
                disposition="confirmed_issue",
                reviewer="Analyst B",
                assigned_reviewer="Analyst B",
                note="Confirmed",
                evidence_links_json=[],
            ),
            CaseReview(
                parcel_id="P-3",
                year=2025,
                score_id=score_rows[2].id,
                run_id=run.id,
                feature_version="feature_v1",
                ruleset_version="scoring_rules_v1",
                status="resolved",
                disposition="false_positive",
                reviewer="Analyst C",
                assigned_reviewer="Analyst C",
                note="False positive",
                evidence_links_json=[],
            ),
            CaseReview(
                parcel_id="P-4",
                year=2025,
                score_id=score_rows[3].id,
                run_id=run.id,
                feature_version="feature_v1",
                ruleset_version="scoring_rules_v1",
                status="in_review",
                disposition=None,
                reviewer="Analyst D",
                assigned_reviewer="Analyst D",
                note="Not resolved yet",
                evidence_links_json=[],
            ),
            # Version-mismatched review should not affect feature_v1 output.
            CaseReview(
                parcel_id="P-2",
                year=2025,
                score_id=score_rows[1].id,
                run_id=run.id,
                feature_version="feature_v2",
                ruleset_version="scoring_rules_v1",
                status="resolved",
                disposition="false_positive",
                reviewer="Analyst X",
                assigned_reviewer="Analyst X",
                note="Different feature version",
                evidence_links_json=[],
            ),
        ]
    )
