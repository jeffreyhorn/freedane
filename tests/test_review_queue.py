from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit import review_queue as review_queue_module
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    FraudFlag,
    FraudScore,
    Parcel,
    ParcelYearFact,
    ScoringRun,
)


def test_review_queue_cli_default_mode_is_deterministic_and_contract_stable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "review_queue_default.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_review_queue_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result_a = runner.invoke(cli.app, ["review-queue"])
    result_b = runner.invoke(cli.app, ["review-queue"])

    assert result_a.exit_code == 0, result_a.stdout
    assert result_b.exit_code == 0, result_b.stdout
    assert result_a.stdout == result_b.stdout

    payload = json.loads(result_a.stdout)
    assert list(payload.keys()) == [
        "run",
        "request",
        "summary",
        "rows",
        "diagnostics",
        "error",
    ]

    assert payload["run"] == {
        "run_id": None,
        "run_persisted": False,
        "run_type": "review_queue",
        "version_tag": "review_queue_v1",
        "status": "succeeded",
    }
    assert payload["request"] == {
        "top": 100,
        "page": None,
        "page_size": None,
        "parcel_ids": [],
        "years": [],
        "feature_version": "feature_v1",
        "ruleset_version": "scoring_rules_v1",
        "risk_bands": [],
        "requires_review_only": True,
    }

    assert [row["parcel_id"] for row in payload["rows"]] == [
        "P-2",
        "P-3",
        "P-1",
        "P-6",
        "P-5",
    ]
    assert [row["queue_rank"] for row in payload["rows"]] == [1, 2, 3, 4, 5]

    p2 = payload["rows"][0]
    p5 = payload["rows"][-1]
    assert p2["primary_reason_code"] == "ratio__low"
    assert p2["primary_reason_weight"] is None
    assert p5["primary_reason_code"] is None
    assert p5["primary_reason_weight"] is None

    assert payload["summary"] == {
        "candidate_count": 7,
        "filtered_count": 2,
        "skipped_count": 0,
        "returned_count": 5,
        "truncated": False,
        "page": None,
        "page_size": None,
        "total_pages": None,
    }
    assert payload["diagnostics"]["filtered_reason_counts"] == {
        "filtered_by_requires_review": 2
    }
    assert payload["diagnostics"]["skipped_row_counts"] == {}
    assert payload["diagnostics"]["comparability"] == {
        "comparable": True,
        "queue_contract_version": "review_queue_v1",
        "comparison_key": {
            "feature_version": "feature_v1",
            "ruleset_version": "scoring_rules_v1",
            "requires_review_only": True,
            "risk_bands": [],
            "years": [],
            "parcel_ids": [],
            "sort_key_version": "review_queue_sort_v1",
            "slice_mode": "top",
            "top": 100,
            "page": None,
            "page_size": None,
        },
    }
    assert payload["error"] is None


def test_review_queue_cli_filters_pagination_and_csv_export(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "review_queue_pagination.sqlite"
    database_url = f"sqlite:///{db_path}"
    csv_out = tmp_path / "queue.csv"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_review_queue_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "review-queue",
            "--all-scores",
            "--id",
            "  P-2  ",
            "--id",
            "P-1",
            "--id",
            "P-1",
            "--year",
            "2025",
            "--risk-band",
            "high",
            "--page",
            "2",
            "--page-size",
            "1",
            "--csv-out",
            str(csv_out),
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)

    assert payload["request"] == {
        "top": None,
        "page": 2,
        "page_size": 1,
        "parcel_ids": ["P-1", "P-2"],
        "years": [2025],
        "feature_version": "feature_v1",
        "ruleset_version": "scoring_rules_v1",
        "risk_bands": ["high"],
        "requires_review_only": False,
    }
    assert payload["summary"] == {
        "candidate_count": 7,
        "filtered_count": 5,
        "skipped_count": 0,
        "returned_count": 1,
        "truncated": True,
        "page": 2,
        "page_size": 1,
        "total_pages": 2,
    }
    assert payload["rows"][0]["parcel_id"] == "P-1"
    assert payload["rows"][0]["queue_rank"] == 2
    assert payload["diagnostics"]["filtered_reason_counts"] == {
        "filtered_by_parcel_id": 5
    }

    csv_text = csv_out.read_text(encoding="utf-8")
    lines = csv_text.splitlines()
    assert "\r" not in csv_text
    assert lines[0].split(",") == review_queue_module.CSV_COLUMNS
    assert len(lines) - 1 == payload["summary"]["returned_count"]
    assert ",true," in lines[1]


def test_review_queue_cli_validation_errors_exit_two_without_json(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "review_queue_validation.sqlite"
    database_url = f"sqlite:///{db_path}"
    ids_path = tmp_path / "ids.txt"
    ids_path.write_text("\n  \n\t\n", encoding="utf-8")
    init_db(database_url)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()

    result_top_page = runner.invoke(
        cli.app,
        ["review-queue", "--top", "10", "--page", "1"],
    )
    assert result_top_page.exit_code == 2
    assert "Cannot combine --top" in (result_top_page.stdout + result_top_page.stderr)

    result_empty_ids = runner.invoke(
        cli.app,
        ["review-queue", "--ids", str(ids_path)],
    )
    assert result_empty_ids.exit_code == 2
    assert "At least one parcel ID must remain" in (
        result_empty_ids.stdout + result_empty_ids.stderr
    )


def test_review_queue_cli_unsupported_ruleset_returns_exit_one_json(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "review_queue_ruleset.sqlite"
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
        ["review-queue", "--ruleset-version", "scoring_rules_v999"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["run"]["status"] == "failed"
    assert payload["error"]["code"] == "unsupported_version_selector"


def test_review_queue_module_invalid_risk_band_returns_specific_failure_payload(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "review_queue_invalid_risk_band.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_review_queue_fixture(session)

    with session_scope(database_url) as session:
        payload = review_queue_module.build_review_queue(
            session,
            risk_bands=["critical"],
        )

    assert payload["run"]["status"] == "failed"
    assert payload["error"]["code"] == "unsupported_risk_band"
    assert "critical" in payload["error"]["message"]
    assert payload["error"]["code"] != "source_query_error"


def test_review_queue_module_normalizes_non_positive_slice_inputs(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "review_queue_slice_normalization.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_review_queue_fixture(session)

    with session_scope(database_url) as session:
        paged_payload = review_queue_module.build_review_queue(
            session,
            page=0,
            page_size=0,
        )
    assert paged_payload["run"]["status"] == "succeeded"
    assert paged_payload["request"]["page"] == 1
    assert paged_payload["request"]["page_size"] == 1
    assert paged_payload["summary"]["page"] == 1
    assert paged_payload["summary"]["page_size"] == 1
    assert paged_payload["summary"]["returned_count"] == 1

    with session_scope(database_url) as session:
        top_payload = review_queue_module.build_review_queue(
            session,
            top=0,
        )
    assert top_payload["run"]["status"] == "succeeded"
    assert top_payload["request"]["top"] == 1
    assert top_payload["summary"]["returned_count"] == 1


def test_review_queue_cli_sanitizes_source_query_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "review_queue_source_error.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    def _raise_query(*_args, **_kwargs):
        raise RuntimeError("db secret leaked /private/path")

    monkeypatch.setattr(review_queue_module, "_load_base_scores", _raise_query)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["review-queue"])
    assert result.exit_code == 1

    payload = json.loads(result.stdout)
    assert payload["run"]["status"] == "failed"
    assert payload["error"]["code"] == "source_query_error"
    assert payload["error"]["message"] == (
        "Failed to query source data while building review queue."
    )
    assert "private/path" not in payload["error"]["message"]


def _seed_review_queue_fixture(session) -> None:
    run = ScoringRun(
        run_type="score_fraud",
        status="succeeded",
        version_tag="scoring_rules_v1",
        scope_json={},
        config_json={},
    )
    session.add(run)
    session.flush()

    for parcel_id in ["P-1", "P-2", "P-3", "P-4", "P-5", "P-6", "P-7"]:
        session.add(Parcel(id=parcel_id))

    session.add_all(
        [
            ParcelYearFact(
                parcel_id="P-1",
                year=2025,
                municipality_name="Town A",
                assessment_valuation_classification="res",
            ),
            ParcelYearFact(
                parcel_id="P-2",
                year=2025,
                municipality_name="Town A",
                assessment_valuation_classification="res",
            ),
            ParcelYearFact(
                parcel_id="P-3",
                year=2024,
                municipality_name="Town B",
                assessment_valuation_classification="com",
            ),
            ParcelYearFact(
                parcel_id="P-4",
                year=2025,
                municipality_name="Town C",
                assessment_valuation_classification="res",
            ),
            ParcelYearFact(
                parcel_id="P-5",
                year=2025,
                municipality_name="Town D",
                assessment_valuation_classification="ag",
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
            reason_code_count=2,
            score_summary_json={},
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-2",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("90.00"),
            risk_band="high",
            requires_review=True,
            reason_code_count=3,
            score_summary_json={},
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-3",
            year=2024,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("90.00"),
            risk_band="medium",
            requires_review=True,
            reason_code_count=3,
            score_summary_json={},
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-4",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("75.00"),
            risk_band="high",
            requires_review=False,
            reason_code_count=1,
            score_summary_json={},
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-5",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("50.00"),
            risk_band="medium",
            requires_review=True,
            reason_code_count=0,
            score_summary_json={},
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-6",
            year=2023,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("50.00"),
            risk_band="low",
            requires_review=True,
            reason_code_count=1,
            score_summary_json={},
        ),
        FraudScore(
            run_id=run.id,
            feature_run_id=None,
            parcel_id="P-7",
            year=2025,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            score_value=Decimal("20.00"),
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
                severity_weight=Decimal("10.5000"),
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
                reason_code="ratio__low",
                reason_rank=1,
                severity_weight=None,
                metric_name="m",
                metric_value="1",
                threshold_value="2",
                comparison_operator="<",
                explanation="e",
                source_refs_json={},
            ),
            FraudFlag(
                run_id=run.id,
                score_id=scores[2].id,
                parcel_id="P-3",
                year=2024,
                ruleset_version="scoring_rules_v1",
                reason_code="ratio__low",
                reason_rank=1,
                severity_weight=Decimal("0.0000"),
                metric_name="m",
                metric_value="1",
                threshold_value="2",
                comparison_operator="<",
                explanation="e",
                source_refs_json={},
            ),
            FraudFlag(
                run_id=run.id,
                score_id=scores[3].id,
                parcel_id="P-4",
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
            FraudFlag(
                run_id=run.id,
                score_id=scores[5].id,
                parcel_id="P-6",
                year=2023,
                ruleset_version="scoring_rules_v1",
                reason_code="permit__gap",
                reason_rank=1,
                severity_weight=Decimal("2.5000"),
                metric_name="m",
                metric_value="1",
                threshold_value="2",
                comparison_operator="<",
                explanation="e",
                source_refs_json={},
            ),
        ]
    )
