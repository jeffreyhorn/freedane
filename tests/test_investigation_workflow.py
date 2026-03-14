from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    FraudFlag,
    FraudScore,
    Parcel,
    ParcelYearFact,
    ScoringRun,
)


def test_investigation_workflow_cli_smoke_sequence(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "investigation_workflow.sqlite"
    database_url = f"sqlite:///{db_path}"
    report_out = tmp_path / "investigation_report.html"
    feedback_sql_out = tmp_path / "review_feedback.sql"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_workflow_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()

    queue_result = runner.invoke(
        cli.app,
        [
            "review-queue",
            "--top",
            "2",
        ],
    )
    assert queue_result.exit_code == 0, queue_result.stdout
    queue_payload = json.loads(queue_result.stdout)
    assert queue_payload["run"]["status"] == "succeeded"
    assert queue_payload["summary"]["returned_count"] == 2
    assert len(queue_payload["rows"]) == 2

    top_row = queue_payload["rows"][0]
    create_result = runner.invoke(
        cli.app,
        [
            "case-review",
            "create",
            "--score-id",
            str(top_row["score_id"]),
            "--status",
            "resolved",
            "--disposition",
            "false_positive",
            "--reviewer",
            "Analyst A",
            "--evidence-link",
            "kind=dossier,ref=CASE-0001",
        ],
    )
    assert create_result.exit_code == 0, create_result.stdout
    create_payload = json.loads(create_result.stdout)
    assert create_payload["run"]["status"] == "succeeded"
    assert create_payload["review"]["status"] == "resolved"
    assert create_payload["review"]["disposition"] == "false_positive"

    list_result = runner.invoke(
        cli.app,
        [
            "case-review",
            "list",
            "--status",
            "resolved",
            "--limit",
            "5",
        ],
    )
    assert list_result.exit_code == 0, list_result.stdout
    list_payload = json.loads(list_result.stdout)
    assert list_payload["run"]["status"] == "succeeded"
    assert list_payload["summary"]["total"] >= 1

    feedback_result = runner.invoke(
        cli.app,
        [
            "review-feedback",
            "--sql-out",
            str(feedback_sql_out),
        ],
    )
    assert feedback_result.exit_code == 0, feedback_result.stdout
    feedback_payload = json.loads(feedback_result.stdout)
    assert feedback_payload["run"]["status"] == "succeeded"
    assert feedback_payload["summary"]["reviewed_case_count"] == 1
    assert feedback_sql_out.exists()

    dossier_result = runner.invoke(
        cli.app,
        [
            "parcel-dossier",
            "--id",
            top_row["parcel_id"],
        ],
    )
    assert dossier_result.exit_code == 0, dossier_result.stdout
    dossier_payload = json.loads(dossier_result.stdout)
    assert dossier_payload["run"]["status"] == "succeeded"
    assert dossier_payload["parcel"]["parcel_id"] == top_row["parcel_id"]

    report_result = runner.invoke(
        cli.app,
        [
            "investigation-report",
            "--top",
            "2",
            "--html-out",
            str(report_out),
        ],
    )
    assert report_result.exit_code == 0, report_result.stdout
    report_payload = json.loads(report_result.stdout)
    assert report_payload["run"]["status"] == "succeeded"
    assert report_payload["summary"]["queue_row_count"] == 2
    assert report_out.exists()
    assert "Risk signals are triage guidance, not proof." in report_out.read_text(
        encoding="utf-8"
    )


def _seed_workflow_fixture(session) -> None:
    run = ScoringRun(
        run_type="score_fraud",
        status="succeeded",
        version_tag="scoring_rules_v1",
        scope_json={},
        config_json={},
    )
    session.add(run)
    session.flush()

    session.add_all([Parcel(id="P-10"), Parcel(id="P-20")])
    session.add_all(
        [
            ParcelYearFact(
                parcel_id="P-10",
                year=2025,
                municipality_name="Town A",
                current_owner_name="Owner A",
            ),
            ParcelYearFact(
                parcel_id="P-20",
                year=2025,
                municipality_name="Town B",
                current_owner_name="Owner B",
            ),
        ]
    )

    scores = [
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-10",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("91.00"),
            risk_band="high",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-20",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("82.00"),
            risk_band="medium",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
        ),
    ]
    session.add_all(scores)
    session.flush()

    session.add_all(
        [
            FraudFlag(
                run_id=run.id,
                score_id=scores[0].id,
                parcel_id="P-10",
                year=2025,
                ruleset_version="scoring_rules_v1",
                reason_code="R_RATIO_LOW",
                reason_rank=1,
                severity_weight=Decimal("9.0000"),
                metric_name="assessment_to_sale_ratio",
                metric_value="0.51",
                threshold_value="0.55",
                comparison_operator="lt",
                explanation="ratio below threshold",
                source_refs_json={},
            ),
            FraudFlag(
                run_id=run.id,
                score_id=scores[1].id,
                parcel_id="P-20",
                year=2025,
                ruleset_version="scoring_rules_v1",
                reason_code="R_PERMIT_GAP",
                reason_rank=1,
                severity_weight=Decimal("7.5000"),
                metric_name="permit_adjusted_gap",
                metric_value="25000",
                threshold_value="20000",
                comparison_operator="gte",
                explanation="permit adjusted gap above threshold",
                source_refs_json={},
            ),
        ]
    )
