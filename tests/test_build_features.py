from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.build_features import build_features
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    Parcel,
    ParcelFeature,
    ParcelYearFact,
    SalesExclusion,
    SalesParcelMatch,
    SalesTransaction,
    ScoringRun,
)


def test_build_features_computes_core_features_and_persists_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "build_features.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        fixture = _seed_build_features_fixture(session)

    with session_scope(database_url) as session:
        payload = build_features(session, feature_version="feature-v1")

    assert payload["run"]["status"] == "succeeded"
    assert payload["run"]["run_type"] == "build_features"
    assert payload["run"]["run_persisted"] is True
    assert payload["summary"] == {
        "selected_parcels": 4,
        "selected_parcel_years": 6,
        "rows_deleted": 0,
        "rows_inserted": 6,
        "rows_skipped": 0,
        "quality_warning_count": 8,
    }

    with session_scope(database_url) as session:
        run_row = session.execute(
            select(ScoringRun).where(ScoringRun.version_tag == "feature-v1")
        ).scalar_one()
        rows = (
            session.execute(
                select(ParcelFeature).order_by(
                    ParcelFeature.parcel_id.asc(),
                    ParcelFeature.year.asc(),
                )
            )
            .scalars()
            .all()
        )

    assert run_row.run_type == "build_features"
    assert run_row.status == "succeeded"
    assert run_row.scope_json == {"parcel_ids": None, "years": None}
    assert run_row.scope_hash is not None
    assert run_row.output_summary_json == payload["summary"]
    assert len(rows) == 6

    by_key = {(row.parcel_id, row.year): row for row in rows}

    p1_2025 = by_key[("parcel-res-1", 2025)]
    assert p1_2025.assessment_to_sale_ratio == Decimal("0.833333")
    assert p1_2025.peer_percentile == Decimal("0.2500")
    assert p1_2025.yoy_assessment_change_pct == Decimal("0.111111")
    assert p1_2025.permit_adjusted_expected_change is None
    assert p1_2025.appeal_value_delta_3y is None
    assert p1_2025.lineage_value_reset_delta is None
    assert p1_2025.feature_quality_flags == []
    assert p1_2025.source_refs_json["sales"] == {
        "sales_transaction_id": fixture["sales"]["p1_selected_sale_id"],
        "match_id": fixture["sales"]["p1_selected_match_id"],
    }

    p2_2025 = by_key[("parcel-res-2", 2025)]
    assert p2_2025.assessment_to_sale_ratio == Decimal("1.600000")
    assert p2_2025.peer_percentile == Decimal("0.7500")
    assert p2_2025.yoy_assessment_change_pct == Decimal("-0.200000")
    assert p2_2025.feature_quality_flags == []
    assert p2_2025.source_refs_json["sales"] == {
        "sales_transaction_id": fixture["sales"]["p2_selected_sale_id"],
        "match_id": fixture["sales"]["p2_selected_match_id"],
    }

    p3_2025 = by_key[("parcel-com-1", 2025)]
    assert p3_2025.assessment_to_sale_ratio == Decimal("1.500000")
    assert p3_2025.peer_percentile is None
    assert p3_2025.yoy_assessment_change_pct is None
    assert p3_2025.feature_quality_flags == [
        "insufficient_peer_group",
        "missing_prior_year_assessment",
    ]
    assert p3_2025.source_refs_json["peer_group"] == {
        "year": 2025,
        "municipality": "madison",
        "classification": "commercial",
        "group_size": 1,
    }

    p4_2025 = by_key[("parcel-missing-sale", 2025)]
    assert p4_2025.assessment_to_sale_ratio is None
    assert p4_2025.peer_percentile is None
    assert p4_2025.yoy_assessment_change_pct is None
    assert p4_2025.feature_quality_flags == [
        "missing_eligible_sale",
        "missing_prior_year_assessment",
    ]
    assert p4_2025.source_refs_json["sales"] == {
        "sales_transaction_id": None,
        "match_id": None,
    }


def test_build_features_scoped_rebuild_replaces_only_in_scope_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "build_features_scoped.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_build_features_fixture(session)

    with session_scope(database_url) as session:
        first_payload = build_features(session, feature_version="feature-v1")
    first_run_id = first_payload["run"]["run_id"]
    assert first_run_id is not None

    with session_scope(database_url) as session:
        p1_2025 = session.execute(
            select(ParcelYearFact).where(
                ParcelYearFact.parcel_id == "parcel-res-1",
                ParcelYearFact.year == 2025,
            )
        ).scalar_one()
        p1_2025.assessment_total_value = Decimal("120000.00")

    with session_scope(database_url) as session:
        second_payload = build_features(
            session,
            feature_version="feature-v1",
            parcel_ids=[" parcel-res-1 "],
            years=[2025],
        )

    second_run_id = second_payload["run"]["run_id"]
    assert second_run_id is not None
    assert second_run_id != first_run_id
    assert second_payload["scope"] == {"parcel_ids": ["parcel-res-1"], "years": [2025]}
    assert second_payload["summary"] == {
        "selected_parcels": 1,
        "selected_parcel_years": 1,
        "rows_deleted": 1,
        "rows_inserted": 1,
        "rows_skipped": 0,
        "quality_warning_count": 1,
    }

    with session_scope(database_url) as session:
        rows = (
            session.execute(
                select(ParcelFeature).where(
                    ParcelFeature.feature_version == "feature-v1"
                )
            )
            .scalars()
            .all()
        )
        by_key = {(row.parcel_id, row.year): row for row in rows}

    assert len(rows) == 6
    assert by_key[("parcel-res-1", 2025)].run_id == second_run_id
    assert by_key[("parcel-res-1", 2025)].assessment_to_sale_ratio == Decimal(
        "1.000000"
    )
    assert by_key[("parcel-res-1", 2025)].peer_percentile is None
    assert by_key[("parcel-res-1", 2025)].feature_quality_flags == [
        "insufficient_peer_group"
    ]
    assert by_key[("parcel-res-2", 2025)].run_id == first_run_id
    assert by_key[("parcel-res-2", 2025)].assessment_to_sale_ratio == Decimal(
        "1.600000"
    )


def test_build_features_cli_supports_scope_and_output_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "build_features_cli.sqlite"
    database_url = f"sqlite:///{db_path}"
    out_path = tmp_path / "build_features_output.json"
    ids_path = tmp_path / "ids.txt"
    ids_path.write_text("parcel-res-1\n", encoding="utf-8")
    init_db(database_url)

    with session_scope(database_url) as session:
        _seed_build_features_fixture(session)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "build-features",
            "--ids",
            str(ids_path),
            "--year",
            "2025",
            "--feature-version",
            "feature-v1-cli",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["run"]["status"] == "succeeded"
    assert payload["scope"] == {"parcel_ids": ["parcel-res-1"], "years": [2025]}
    assert payload["summary"]["selected_parcels"] == 1
    assert payload["summary"]["selected_parcel_years"] == 1
    assert payload["summary"]["rows_inserted"] == 1


def _seed_build_features_fixture(session) -> dict[str, dict[str, int]]:
    session.add_all(
        [
            Parcel(id="parcel-res-1"),
            Parcel(id="parcel-res-2"),
            Parcel(id="parcel-com-1"),
            Parcel(id="parcel-missing-sale"),
        ]
    )
    session.add_all(
        [
            ParcelYearFact(
                parcel_id="parcel-res-1",
                year=2024,
                municipality_name="McFarland",
                assessment_valuation_classification="residential",
                assessment_total_value=Decimal("90000.00"),
            ),
            ParcelYearFact(
                parcel_id="parcel-res-1",
                year=2025,
                municipality_name="McFarland",
                assessment_valuation_classification="residential",
                assessment_total_value=Decimal("100000.00"),
            ),
            ParcelYearFact(
                parcel_id="parcel-res-2",
                year=2024,
                municipality_name="McFarland",
                assessment_valuation_classification="residential",
                assessment_total_value=Decimal("200000.00"),
            ),
            ParcelYearFact(
                parcel_id="parcel-res-2",
                year=2025,
                municipality_name="McFarland",
                assessment_valuation_classification="residential",
                assessment_total_value=Decimal("160000.00"),
            ),
            ParcelYearFact(
                parcel_id="parcel-com-1",
                year=2025,
                municipality_name="Madison",
                assessment_valuation_classification="commercial",
                assessment_total_value=Decimal("300000.00"),
            ),
            ParcelYearFact(
                parcel_id="parcel-missing-sale",
                year=2025,
                municipality_name="Sun Prairie",
                assessment_valuation_classification="residential",
                assessment_total_value=Decimal("70000.00"),
            ),
        ]
    )

    sales_rows = [
        _sale_transaction(
            row_number=1,
            transfer_date=date(2025, 1, 10),
            consideration=Decimal("100000.00"),
        ),
        _sale_transaction(
            row_number=2,
            transfer_date=date(2025, 12, 1),
            consideration=Decimal("120000.00"),
        ),
        _sale_transaction(
            row_number=3,
            transfer_date=date(2025, 6, 1),
            consideration=Decimal("100000.00"),
        ),
        _sale_transaction(
            row_number=4,
            transfer_date=date(2025, 1, 20),
            consideration=Decimal("100000.00"),
        ),
        _sale_transaction(
            row_number=5,
            transfer_date=date(2025, 3, 10),
            consideration=Decimal("200000.00"),
        ),
    ]
    session.add_all(sales_rows)
    session.flush()

    matches = [
        _primary_match(sales_rows[0].id, "parcel-res-1"),
        _primary_match(sales_rows[1].id, "parcel-res-1"),
        _primary_match(sales_rows[2].id, "parcel-res-2"),
        _primary_match(sales_rows[3].id, "parcel-res-2"),
        _primary_match(sales_rows[4].id, "parcel-com-1"),
    ]
    session.add_all(matches)
    session.flush()

    session.add(
        SalesExclusion(
            sales_transaction_id=sales_rows[2].id,
            exclusion_code="non_arms_length",
            exclusion_reason="Not arm's-length",
            is_active=True,
            excluded_by_rule="test_rule",
        )
    )

    return {
        "sales": {
            "p1_selected_sale_id": sales_rows[1].id,
            "p1_selected_match_id": matches[1].id,
            "p2_selected_sale_id": sales_rows[3].id,
            "p2_selected_match_id": matches[3].id,
        }
    }


def _sale_transaction(
    *,
    row_number: int,
    transfer_date: date,
    consideration: Decimal,
) -> SalesTransaction:
    return SalesTransaction(
        source_system="wisconsin_dor_retr",
        source_file_name="build_features_fixture.csv",
        source_file_sha256="sha-build-features-fixture",
        source_row_number=row_number,
        source_headers=["Transfer Date", "Consideration"],
        raw_row={
            "Transfer Date": transfer_date.isoformat(),
            "Consideration": str(consideration),
        },
        import_status="loaded",
        transfer_date=transfer_date,
        consideration_amount=consideration,
    )


def _primary_match(sales_transaction_id: int, parcel_id: str) -> SalesParcelMatch:
    return SalesParcelMatch(
        sales_transaction_id=sales_transaction_id,
        parcel_id=parcel_id,
        match_method="exact_parcel_number",
        confidence_score=Decimal("1.0000"),
        match_rank=1,
        is_primary=True,
        match_review_status="auto_accepted",
        matched_value=parcel_id,
        matcher_version="test",
    )
