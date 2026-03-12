from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit import investigation_report as investigation_report_module
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    FraudFlag,
    FraudScore,
    Parcel,
    ParcelYearFact,
    ScoringRun,
)


def test_investigation_report_cli_generates_html_with_required_sections(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "investigation_report.sqlite"
    database_url = f"sqlite:///{db_path}"
    html_out = tmp_path / "investigation_report.html"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_report_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "investigation-report",
            "--top",
            "2",
            "--html-out",
            str(html_out),
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["run"]["status"] == "succeeded"
    assert payload["summary"]["queue_row_count"] == 2
    assert payload["summary"]["dossier_failure_count"] == 0
    assert payload["artifacts"]["html_path"] == str(html_out)
    timing = payload["diagnostics"]["timing_seconds"]
    assert set(timing.keys()) == {
        "report_build_total",
        "dossier_build_total",
        "dossier_build_avg",
        "dossier_build_max",
    }

    html = html_out.read_text(encoding="utf-8")
    assert "<h1>AccessDane Investigation Report</h1>" in html
    assert "Top Review Queue" in html
    assert "Reason Code Summary" in html
    assert "Parcel Dossier Drill-In" in html
    assert 'href="#dossier-score-' in html
    assert 'id="dossier-score-' in html
    assert "Risk signals are triage guidance, not proof." in html


def test_investigation_report_cli_html_output_is_deterministic(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "investigation_report_deterministic.sqlite"
    database_url = f"sqlite:///{db_path}"
    html_out_a = tmp_path / "report_a.html"
    html_out_b = tmp_path / "report_b.html"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_report_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result_a = runner.invoke(
        cli.app,
        [
            "investigation-report",
            "--top",
            "2",
            "--html-out",
            str(html_out_a),
        ],
    )
    result_b = runner.invoke(
        cli.app,
        [
            "investigation-report",
            "--top",
            "2",
            "--html-out",
            str(html_out_b),
        ],
    )

    assert result_a.exit_code == 0, result_a.stdout
    assert result_b.exit_code == 0, result_b.stdout
    assert html_out_a.read_text(encoding="utf-8") == html_out_b.read_text(
        encoding="utf-8"
    )


def test_investigation_report_normalizes_blank_risk_band_for_table_and_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    html_out = tmp_path / "blank_risk_band_report.html"
    db_path = tmp_path / "blank_risk_band_report.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    def _fake_build_review_queue(*_args, **_kwargs):
        return {
            "run": {"status": "succeeded"},
            "summary": {
                "candidate_count": 1,
                "filtered_count": 0,
                "skipped_count": 0,
                "returned_count": 1,
            },
            "rows": [
                {
                    "queue_rank": 1,
                    "score_id": 10,
                    "parcel_id": "P-BLANK",
                    "year": 2025,
                    "score_value": "10.00",
                    "risk_band": "",
                    "primary_reason_code": "",
                }
            ],
        }

    def _fake_build_parcel_dossier(*_args, **_kwargs):
        return {
            "run": {"status": "succeeded"},
            "parcel": {},
            "sections": {},
            "section_order": [],
            "timeline": {"event_count": 0},
            "error": None,
        }

    monkeypatch.setattr(
        investigation_report_module,
        "build_review_queue",
        _fake_build_review_queue,
    )
    monkeypatch.setattr(
        investigation_report_module,
        "build_parcel_dossier",
        _fake_build_parcel_dossier,
    )

    with session_scope(database_url) as session:
        payload = investigation_report_module.build_investigation_report(
            session,
            html_out=html_out,
            top=1,
        )

    assert payload["run"]["status"] == "succeeded"
    html = html_out.read_text(encoding="utf-8")
    assert "<td>(none)</td>" in html
    assert '<span class="pill">(none): 1</span>' in html


def _seed_report_fixture(session) -> None:
    run = ScoringRun(
        run_type="score_fraud",
        status="succeeded",
        version_tag="scoring_rules_v1",
        scope_json={},
        config_json={},
    )
    session.add(run)
    session.flush()

    session.add_all([Parcel(id="P-1"), Parcel(id="P-2"), Parcel(id="P-3")])
    session.add_all(
        [
            ParcelYearFact(
                parcel_id="P-1",
                year=2025,
                municipality_name="Town A",
                current_owner_name="Owner A",
            ),
            ParcelYearFact(
                parcel_id="P-2",
                year=2025,
                municipality_name="Town B",
                current_owner_name="Owner B",
            ),
            ParcelYearFact(
                parcel_id="P-3",
                year=2025,
                municipality_name="Town C",
                current_owner_name="Owner C",
            ),
        ]
    )

    scores = [
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-1",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("90.00"),
            risk_band="high",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-2",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("80.00"),
            risk_band="medium",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-3",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("70.00"),
            risk_band="low",
            requires_review=False,
            reason_code_count=0,
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
                parcel_id="P-1",
                year=2025,
                ruleset_version="scoring_rules_v1",
                reason_code="ratio__low",
                reason_rank=1,
                severity_weight=Decimal("10.0000"),
                metric_name="m",
                metric_value="1",
                threshold_value="2",
                comparison_operator="<",
                explanation="e",
                source_refs_json={},
            ),
            FraudFlag(
                run_id=run.id,
                score_id=scores[1].id,
                parcel_id="P-2",
                year=2025,
                ruleset_version="scoring_rules_v1",
                reason_code="peer__outlier",
                reason_rank=1,
                severity_weight=Decimal("5.0000"),
                metric_name="m",
                metric_value="1",
                threshold_value="2",
                comparison_operator="<",
                explanation="e",
                source_refs_json={},
            ),
        ]
    )
