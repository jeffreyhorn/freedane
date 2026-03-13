from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    CaseReview,
    FraudFlag,
    FraudScore,
    Parcel,
    ParcelFeature,
    ScoringRun,
)
from accessdane_audit.score_fraud import (
    PERMIT_SUPPORT_CUTOFF,
    _normalized_feature_quality_flags,
    score_fraud,
)


def test_score_fraud_computes_scores_and_persists_flags(tmp_path: Path) -> None:
    db_path = tmp_path / "score_fraud.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        fixture = _seed_score_fraud_fixture(session)

    with session_scope(database_url) as session:
        payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
        )

    assert payload["run"]["status"] == "succeeded"
    assert payload["run"]["run_type"] == "score_fraud"
    assert payload["run"]["run_persisted"] is True
    assert payload["run"]["run_id"] is not None
    assert payload["run"]["parent_run_id"] is None
    assert payload["summary"] == {
        "features_considered": 4,
        "scores_inserted": 4,
        "flags_inserted": 10,
        "high_risk_count": 1,
        "medium_risk_count": 1,
        "low_risk_count": 2,
        "skipped_feature_rows": 0,
    }
    assert payload["top_flags"][0] == {
        "parcel_id": "parcel-1",
        "year": 2025,
        "score_value": "100.00",
        "risk_band": "high",
        "reason_code": "ratio__assessment_to_sale_below_floor",
        "reason_rank": 1,
        "severity_weight": "35.0000",
        "explanation": "Assessment-to-sale ratio 0.500000 is below threshold 0.55.",
    }

    with session_scope(database_url) as session:
        run_row = session.execute(
            select(ScoringRun).where(
                ScoringRun.run_type == "score_fraud",
                ScoringRun.version_tag == "scoring_rules_v1",
            )
        ).scalar_one()
        scores = (
            session.execute(
                select(FraudScore).where(
                    FraudScore.ruleset_version == "scoring_rules_v1"
                )
            )
            .scalars()
            .all()
        )
        p1_flags = (
            session.execute(
                select(FraudFlag)
                .where(
                    FraudFlag.ruleset_version == "scoring_rules_v1",
                    FraudFlag.parcel_id == "parcel-1",
                    FraudFlag.year == 2025,
                )
                .order_by(FraudFlag.reason_rank.asc())
            )
            .scalars()
            .all()
        )

    assert run_row.parent_run_id is None
    assert run_row.output_summary_json == payload["summary"]

    by_key = {(row.parcel_id, row.year): row for row in scores}
    assert len(by_key) == 4

    p1 = by_key[("parcel-1", 2025)]
    assert p1.score_value == Decimal("100.00")
    assert p1.risk_band == "high"
    assert p1.requires_review is True
    assert p1.reason_code_count == 6
    assert p1.feature_run_id == fixture["feature_runs"]["run_1_id"]
    assert p1.score_summary_json["raw_score"] == "121.0000"
    assert p1.score_summary_json["score_value"] == "100.00"
    assert p1.score_summary_json["quality_flags"] == ["flag_a"]
    assert p1.score_summary_json["triggered_reason_codes"] == [
        "appeal__recurring_successful_reductions",
        "lineage__post_lineage_value_drop",
        "peer__assessment_ratio_bottom_peer_percentile",
        "permit_gap__assessment_increase_unexplained_by_permits",
        "ratio__assessment_to_sale_below_floor",
        "yoy__assessment_spike_without_support",
    ]

    p2 = by_key[("parcel-2", 2025)]
    assert p2.score_value == Decimal("0.00")
    assert p2.risk_band == "low"
    assert p2.requires_review is False
    assert p2.reason_code_count == 0
    assert len(p2.score_summary_json["skipped_rules"]) == 6

    p3 = by_key[("parcel-3", 2025)]
    assert p3.score_value == Decimal("50.00")
    assert p3.risk_band == "medium"
    assert p3.requires_review is True
    assert p3.reason_code_count == 4
    assert p3.feature_run_id == fixture["feature_runs"]["run_2_id"]
    assert p3.score_summary_json["quality_flags"] == ["flag_a", "flag_b"]

    assert [flag.reason_code for flag in p1_flags] == [
        "ratio__assessment_to_sale_below_floor",
        "peer__assessment_ratio_bottom_peer_percentile",
        "permit_gap__assessment_increase_unexplained_by_permits",
        "lineage__post_lineage_value_drop",
        "yoy__assessment_spike_without_support",
        "appeal__recurring_successful_reductions",
    ]
    yoy_flag = next(
        flag
        for flag in p1_flags
        if flag.reason_code == "yoy__assessment_spike_without_support"
    )
    assert yoy_flag.source_refs_json["secondary_evidence"] == {
        "permit_adjusted_expected_change": "8000.00",
        "permit_adjusted_expected_change_condition": "<=_cutoff",
        "permit_adjusted_expected_change_cutoff": format(PERMIT_SUPPORT_CUTOFF, "f"),
        "permit_basis": "declared",
    }
    assert (
        yoy_flag.source_refs_json["feature_sources"]["permits"]["basis"] == "declared"
    )
    assert f"vs cutoff {format(PERMIT_SUPPORT_CUTOFF, 'f')}" in yoy_flag.explanation

    assert payload["rankings"]["top_parcels"] == [
        {
            "parcel_id": "parcel-1",
            "year": 2025,
            "score_value": "100.00",
            "risk_band": "high",
            "reason_code_count": 6,
            "primary_reason_code": "ratio__assessment_to_sale_below_floor",
            "primary_reason_weight": "35.0000",
        },
        {
            "parcel_id": "parcel-3",
            "year": 2025,
            "score_value": "50.00",
            "risk_band": "medium",
            "reason_code_count": 4,
            "primary_reason_code": "ratio__assessment_to_sale_below_floor",
            "primary_reason_weight": "20.0000",
        },
        {
            "parcel_id": "parcel-2",
            "year": 2025,
            "score_value": "0.00",
            "risk_band": "low",
            "reason_code_count": 0,
            "primary_reason_code": None,
            "primary_reason_weight": "0.0000",
        },
        {
            "parcel_id": "parcel-4",
            "year": 2024,
            "score_value": "0.00",
            "risk_band": "low",
            "reason_code_count": 0,
            "primary_reason_code": None,
            "primary_reason_weight": "0.0000",
        },
    ]
    assert payload["rankings"]["risk_band_breakdown"] == [
        {"risk_band": "high", "parcel_count": 1},
        {"risk_band": "medium", "parcel_count": 1},
        {"risk_band": "low", "parcel_count": 2},
    ]
    assert payload["rankings"]["reason_code_breakdown"] == [
        {"reason_code": "appeal__recurring_successful_reductions", "flag_count": 2},
        {
            "reason_code": "peer__assessment_ratio_bottom_peer_percentile",
            "flag_count": 2,
        },
        {
            "reason_code": "ratio__assessment_to_sale_below_floor",
            "flag_count": 2,
        },
        {"reason_code": "yoy__assessment_spike_without_support", "flag_count": 2},
        {"reason_code": "lineage__post_lineage_value_drop", "flag_count": 1},
        {
            "reason_code": "permit_gap__assessment_increase_unexplained_by_permits",
            "flag_count": 1,
        },
    ]
    assert payload["rankings"]["skipped_feature_breakdown"] == []

    explanations_by_reason_rank = {
        (flag.reason_code, flag.reason_rank): flag.explanation for flag in p1_flags
    }
    for top_flag in payload["top_flags"]:
        if top_flag["parcel_id"] == "parcel-1" and top_flag["year"] == 2025:
            assert (
                top_flag["explanation"]
                == explanations_by_reason_rank[
                    (top_flag["reason_code"], top_flag["reason_rank"])
                ]
            )


def test_score_fraud_feature_run_filter_replaces_only_filtered_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "score_fraud_filtered_rebuild.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        fixture = _seed_score_fraud_fixture(session)

    with session_scope(database_url) as session:
        first_payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
        )

    first_run_id = first_payload["run"]["run_id"]
    assert first_run_id is not None

    with session_scope(database_url) as session:
        p1_feature = session.execute(
            select(ParcelFeature).where(
                ParcelFeature.parcel_id == "parcel-1",
                ParcelFeature.year == 2025,
                ParcelFeature.feature_version == "feature_v1",
            )
        ).scalar_one()
        p1_feature.assessment_to_sale_ratio = Decimal("0.650000")

    with session_scope(database_url) as session:
        second_payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            feature_run_id=fixture["feature_runs"]["run_1_id"],
        )

    second_run_id = second_payload["run"]["run_id"]
    assert second_run_id is not None
    assert second_run_id != first_run_id
    assert second_payload["run"]["parent_run_id"] == fixture["feature_runs"]["run_1_id"]
    assert second_payload["summary"]["features_considered"] == 2
    assert second_payload["summary"]["scores_inserted"] == 2

    with session_scope(database_url) as session:
        rows = (
            session.execute(
                select(FraudScore).where(
                    FraudScore.ruleset_version == "scoring_rules_v1",
                    FraudScore.feature_version == "feature_v1",
                )
            )
            .scalars()
            .all()
        )
    by_parcel = {row.parcel_id: row for row in rows}
    assert len(rows) == 4
    assert by_parcel["parcel-1"].run_id == second_run_id
    assert by_parcel["parcel-2"].run_id == second_run_id
    assert by_parcel["parcel-3"].run_id == first_run_id
    assert by_parcel["parcel-4"].run_id == first_run_id


def test_score_fraud_is_idempotent_for_same_scope(tmp_path: Path) -> None:
    db_path = tmp_path / "score_fraud_idempotent.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_score_fraud_fixture(session)

    with session_scope(database_url) as session:
        first_payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            years=[2025],
        )
        second_payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            years=[2025],
        )

    assert first_payload["run"]["run_id"] != second_payload["run"]["run_id"]
    assert first_payload["scope"] == second_payload["scope"]
    assert first_payload["summary"] == second_payload["summary"]
    assert first_payload["top_flags"] == second_payload["top_flags"]


def test_score_fraud_rerun_preserves_case_reviews_for_reviewed_scores(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "score_fraud_case_review_fk_cleanup.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_score_fraud_fixture(session)

    with session_scope(database_url) as session:
        first_payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            years=[2025],
        )
        assert first_payload["run"]["status"] == "succeeded"

    with session_scope(database_url) as session:
        score_row = session.execute(
            select(FraudScore).where(
                FraudScore.ruleset_version == "scoring_rules_v1",
                FraudScore.feature_version == "feature_v1",
                FraudScore.parcel_id == "parcel-1",
                FraudScore.year == 2025,
            )
        ).scalar_one()
        original_score_id = score_row.id
        session.add(
            CaseReview(
                parcel_id=score_row.parcel_id,
                year=score_row.year,
                score_id=score_row.id,
                run_id=score_row.run_id,
                feature_version=score_row.feature_version,
                ruleset_version=score_row.ruleset_version,
                status="pending",
            )
        )

    with session_scope(database_url) as session:
        second_payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            years=[2025],
        )
        assert second_payload["run"]["status"] == "succeeded"
        second_run_id = second_payload["run"]["run_id"]
        assert second_run_id is not None

    with session_scope(database_url) as session:
        remaining_case_reviews = session.execute(select(CaseReview)).scalars().all()
        assert len(remaining_case_reviews) == 1
        case_review = remaining_case_reviews[0]
        assert case_review.score_id == original_score_id

        rerun_score = session.execute(
            select(FraudScore).where(FraudScore.id == case_review.score_id)
        ).scalar_one()
        assert rerun_score.run_id == second_run_id


def test_score_fraud_feature_run_filter_replaces_rows_after_feature_run_id_changes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "score_fraud_feature_run_replace.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_score_fraud_fixture(session)

    with session_scope(database_url) as session:
        initial_payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
        )

    initial_run_id = initial_payload["run"]["run_id"]
    assert initial_run_id is not None

    with session_scope(database_url) as session:
        replacement_feature_run = ScoringRun(
            run_type="build_features",
            status="succeeded",
            version_tag="feature_v1-run3",
            scope_json={"parcel_ids": None, "years": None},
            config_json={"feature_version": "feature_v1"},
        )
        session.add(replacement_feature_run)
        session.flush()
        replacement_run_id = replacement_feature_run.id

        p1_feature = session.execute(
            select(ParcelFeature).where(
                ParcelFeature.parcel_id == "parcel-1",
                ParcelFeature.year == 2025,
                ParcelFeature.feature_version == "feature_v1",
            )
        ).scalar_one()
        p1_feature.run_id = replacement_run_id

    with session_scope(database_url) as session:
        filtered_payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
            feature_run_id=replacement_run_id,
        )

    filtered_run_id = filtered_payload["run"]["run_id"]
    assert filtered_run_id is not None
    assert filtered_payload["run"]["status"] == "succeeded"
    assert filtered_payload["summary"]["features_considered"] == 1
    assert filtered_payload["summary"]["scores_inserted"] == 1

    with session_scope(database_url) as session:
        rows = (
            session.execute(
                select(FraudScore).where(
                    FraudScore.ruleset_version == "scoring_rules_v1",
                    FraudScore.feature_version == "feature_v1",
                )
            )
            .scalars()
            .all()
        )
    by_key = {(row.parcel_id, row.year): row for row in rows}
    assert len(rows) == 4
    assert by_key[("parcel-1", 2025)].feature_run_id == replacement_run_id
    assert by_key[("parcel-1", 2025)].run_id == filtered_run_id
    assert by_key[("parcel-3", 2025)].run_id == initial_run_id


def test_score_fraud_skips_invalid_feature_rows_and_records_breakdown(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "score_fraud_skip_invalid_rows.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_score_fraud_fixture(session)
        session.add(Parcel(id="parcel-invalid"))
        session.add(Parcel(id="parcel-space "))
        run_3 = ScoringRun(
            run_type="build_features",
            status="succeeded",
            version_tag="feature_v1-invalid-row",
            scope_json={"parcel_ids": None, "years": None},
            config_json={"feature_version": "feature_v1"},
        )
        session.add(run_3)
        session.flush()
        session.add(
            ParcelFeature(
                run_id=run_3.id,
                parcel_id="parcel-invalid",
                year=0,
                feature_version="feature_v1",
                assessment_to_sale_ratio=Decimal("0.500000"),
                peer_percentile=Decimal("0.0100"),
                yoy_assessment_change_pct=Decimal("0.500000"),
                permit_adjusted_expected_change=Decimal("1000.00"),
                permit_adjusted_gap=Decimal("90000.00"),
                appeal_value_delta_3y=Decimal("-20000.00"),
                appeal_success_rate_3y=Decimal("0.9000"),
                lineage_value_reset_delta=Decimal("-200000.00"),
                feature_quality_flags=None,
                source_refs_json=[],
            )
        )
        session.add(
            ParcelFeature(
                run_id=run_3.id,
                parcel_id="parcel-space ",
                year=2025,
                feature_version="feature_v1",
                assessment_to_sale_ratio=Decimal("0.500000"),
                peer_percentile=Decimal("0.0100"),
                yoy_assessment_change_pct=Decimal("0.500000"),
                permit_adjusted_expected_change=Decimal("1000.00"),
                permit_adjusted_gap=Decimal("90000.00"),
                appeal_value_delta_3y=Decimal("-20000.00"),
                appeal_success_rate_3y=Decimal("0.9000"),
                lineage_value_reset_delta=Decimal("-200000.00"),
                feature_quality_flags=("flag_b", "flag_a"),
                source_refs_json={},
            )
        )
        p3_feature = session.execute(
            select(ParcelFeature).where(
                ParcelFeature.parcel_id == "parcel-3",
                ParcelFeature.year == 2025,
                ParcelFeature.feature_version == "feature_v1",
            )
        ).scalar_one()
        p3_feature.source_refs_json = []

    with session_scope(database_url) as session:
        payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
        )

    assert payload["run"]["status"] == "succeeded"
    assert payload["summary"]["features_considered"] == 6
    assert payload["summary"]["scores_inserted"] == 4
    assert payload["summary"]["skipped_feature_rows"] == 2
    assert payload["rankings"]["skipped_feature_breakdown"] == [
        {"reason": "invalid_year", "row_count": 1},
        {"reason": "parcel_id_has_surrounding_whitespace", "row_count": 1},
    ]
    with session_scope(database_url) as session:
        parcel_3_score = session.execute(
            select(FraudScore).where(
                FraudScore.parcel_id == "parcel-3",
                FraudScore.year == 2025,
                FraudScore.ruleset_version == "scoring_rules_v1",
                FraudScore.feature_version == "feature_v1",
            )
        ).scalar_one()
    assert (
        "source_refs_json_invalid_shape"
        in parcel_3_score.score_summary_json["quality_flags"]
    )


def test_score_fraud_rerun_deletes_stale_rows_for_whitespace_guarded_features(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "score_fraud_whitespace_key_cleanup.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_score_fraud_fixture(session)

    with session_scope(database_url) as session:
        first_payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
        )

    assert first_payload["summary"]["scores_inserted"] == 4

    with session_scope(database_url) as session:
        session.add(Parcel(id="parcel-1 "))
        feature_row = session.execute(
            select(ParcelFeature).where(
                ParcelFeature.parcel_id == "parcel-1",
                ParcelFeature.year == 2025,
                ParcelFeature.feature_version == "feature_v1",
            )
        ).scalar_one()
        feature_row.parcel_id = "parcel-1 "

    with session_scope(database_url) as session:
        payload = score_fraud(
            session,
            ruleset_version="scoring_rules_v1",
            feature_version="feature_v1",
        )

    assert payload["summary"]["features_considered"] == 4
    assert payload["summary"]["scores_inserted"] == 3
    assert payload["summary"]["skipped_feature_rows"] == 1
    assert payload["rankings"]["skipped_feature_breakdown"] == [
        {"reason": "parcel_id_has_surrounding_whitespace", "row_count": 1}
    ]

    with session_scope(database_url) as session:
        score_rows = (
            session.execute(
                select(FraudScore).where(
                    FraudScore.ruleset_version == "scoring_rules_v1",
                    FraudScore.feature_version == "feature_v1",
                )
            )
            .scalars()
            .all()
        )
    assert len(score_rows) == 3
    assert {row.parcel_id for row in score_rows} == {"parcel-2", "parcel-3", "parcel-4"}


def test_normalized_feature_quality_flags_accepts_non_list_sequences() -> None:
    assert _normalized_feature_quality_flags(("flag_b", "flag_a", "flag_b", 1)) == [
        "flag_a",
        "flag_b",
    ]
    assert _normalized_feature_quality_flags("flag_a") == []


def test_score_fraud_cli_supports_scope_filter_and_output_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "score_fraud_cli.sqlite"
    database_url = f"sqlite:///{db_path}"
    out_path = tmp_path / "score_fraud_output.json"
    ids_path = tmp_path / "ids.txt"
    ids_path.write_text("parcel-1\nparcel-2\n", encoding="utf-8")
    init_db(database_url)

    with session_scope(database_url) as session:
        fixture = _seed_score_fraud_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "score-fraud",
            "--ids",
            str(ids_path),
            "--year",
            "2025",
            "--feature-version",
            "feature_v1",
            "--ruleset-version",
            "scoring_rules_v1",
            "--feature-run-id",
            str(fixture["feature_runs"]["run_1_id"]),
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["run"]["status"] == "succeeded"
    assert payload["run"]["run_persisted"] is True
    assert payload["run"]["parent_run_id"] == fixture["feature_runs"]["run_1_id"]
    assert payload["scope"] == {
        "parcel_ids": ["parcel-1", "parcel-2"],
        "years": [2025],
        "feature_run_id": fixture["feature_runs"]["run_1_id"],
    }
    assert payload["summary"] == {
        "features_considered": 2,
        "scores_inserted": 2,
        "flags_inserted": 6,
        "high_risk_count": 1,
        "medium_risk_count": 0,
        "low_risk_count": 1,
        "skipped_feature_rows": 0,
    }


def _seed_score_fraud_fixture(session) -> dict[str, dict[str, int]]:
    session.add_all(
        [
            Parcel(id="parcel-1"),
            Parcel(id="parcel-2"),
            Parcel(id="parcel-3"),
            Parcel(id="parcel-4"),
        ]
    )
    run_1 = ScoringRun(
        run_type="build_features",
        status="succeeded",
        version_tag="feature_v1-run1",
        scope_json={"parcel_ids": None, "years": None},
        config_json={"feature_version": "feature_v1"},
    )
    run_2 = ScoringRun(
        run_type="build_features",
        status="succeeded",
        version_tag="feature_v1-run2",
        scope_json={"parcel_ids": None, "years": None},
        config_json={"feature_version": "feature_v1"},
    )
    session.add_all([run_1, run_2])
    session.flush()

    session.add_all(
        [
            ParcelFeature(
                run_id=run_1.id,
                parcel_id="parcel-1",
                year=2025,
                feature_version="feature_v1",
                assessment_to_sale_ratio=Decimal("0.500000"),
                peer_percentile=Decimal("0.0400"),
                yoy_assessment_change_pct=Decimal("0.400000"),
                permit_adjusted_expected_change=Decimal("8000.00"),
                permit_adjusted_gap=Decimal("60000.00"),
                appeal_value_delta_3y=Decimal("-20000.00"),
                appeal_success_rate_3y=Decimal("0.8000"),
                lineage_value_reset_delta=Decimal("-120000.00"),
                feature_quality_flags=["flag_a"],
                source_refs_json=_source_refs(
                    "parcel-1", 2025, permit_basis="declared"
                ),
            ),
            ParcelFeature(
                run_id=run_1.id,
                parcel_id="parcel-2",
                year=2025,
                feature_version="feature_v1",
                assessment_to_sale_ratio=None,
                peer_percentile=None,
                yoy_assessment_change_pct=None,
                permit_adjusted_expected_change=None,
                permit_adjusted_gap=None,
                appeal_value_delta_3y=None,
                appeal_success_rate_3y=None,
                lineage_value_reset_delta=None,
                feature_quality_flags=None,
                source_refs_json=_source_refs("parcel-2", 2025, permit_basis="unknown"),
            ),
            ParcelFeature(
                run_id=run_2.id,
                parcel_id="parcel-3",
                year=2025,
                feature_version="feature_v1",
                assessment_to_sale_ratio=Decimal("0.680000"),
                peer_percentile=Decimal("0.0900"),
                yoy_assessment_change_pct=Decimal("0.220000"),
                permit_adjusted_expected_change=None,
                permit_adjusted_gap=None,
                appeal_value_delta_3y=Decimal("-6000.00"),
                appeal_success_rate_3y=Decimal("0.6100"),
                lineage_value_reset_delta=Decimal("-40000.00"),
                feature_quality_flags=["flag_b", "flag_a", "flag_b"],
                source_refs_json=_source_refs("parcel-3", 2025, permit_basis="none"),
            ),
            ParcelFeature(
                run_id=run_2.id,
                parcel_id="parcel-4",
                year=2024,
                feature_version="feature_v1",
                assessment_to_sale_ratio=Decimal("1.100000"),
                peer_percentile=Decimal("0.5500"),
                yoy_assessment_change_pct=Decimal("0.010000"),
                permit_adjusted_expected_change=Decimal("25000.00"),
                permit_adjusted_gap=Decimal("-1000.00"),
                appeal_value_delta_3y=Decimal("0.00"),
                appeal_success_rate_3y=Decimal("0.1000"),
                lineage_value_reset_delta=Decimal("1000.00"),
                feature_quality_flags=[],
                source_refs_json=_source_refs(
                    "parcel-4", 2024, permit_basis="estimated"
                ),
            ),
        ]
    )

    return {"feature_runs": {"run_1_id": run_1.id, "run_2_id": run_2.id}}


def _source_refs(parcel_id: str, year: int, *, permit_basis: str) -> dict[str, object]:
    return {
        "assessment": {"parcel_id": parcel_id, "year": year},
        "sales": {"sales_transaction_id": None, "match_id": None},
        "peer_group": {
            "year": year,
            "municipality": "mcfarland",
            "classification": "residential",
            "group_size": 4,
        },
        "permits": {
            "window_years": [year, year],
            "basis": permit_basis,
            "source_parcel_year_keys": [{"parcel_id": parcel_id, "year": year}],
        },
        "appeals": {
            "window_years": [year - 2, year],
            "source_parcel_year_keys": [{"parcel_id": parcel_id, "year": year}],
            "value_detail_status": "known",
            "outcome_detail_status": "known",
        },
        "lineage": {
            "relationship_count": 0,
            "related_parcel_ids": [],
            "reference_year": year - 1,
        },
    }
