from __future__ import annotations

import json
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from typing import Optional, Sequence, TypedDict, cast

from sqlalchemy import and_, delete, exists, or_, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import InvalidRequestError, PendingRollbackError
from sqlalchemy.orm import Session, load_only

from .models import (
    ParcelFeature,
    ParcelYearFact,
    SalesExclusion,
    SalesParcelMatch,
    SalesTransaction,
    ScoringRun,
)

RUN_TYPE_BUILD_FEATURES = "build_features"
IN_CLAUSE_BATCH_SIZE = 800
PEER_PERCENTILE_MIN_GROUP_SIZE = 2
PERMIT_CAPTURE_RATE = Decimal("0.75")
APPEAL_WINDOW_YEARS = 3
LINEAGE_REFERENCE_YEAR_OFFSET = -1


class BuildFeaturesScope(TypedDict):
    parcel_ids: Optional[list[str]]
    years: Optional[list[int]]


class BuildFeaturesRun(TypedDict):
    run_id: Optional[int]
    run_persisted: bool
    run_type: str
    version_tag: str
    status: str


class BuildFeaturesSummary(TypedDict):
    selected_parcels: int
    selected_parcel_years: int
    rows_deleted: int
    rows_inserted: int
    rows_skipped: int
    quality_warning_count: int


class BuildFeaturesPayload(TypedDict, total=False):
    run: BuildFeaturesRun
    scope: BuildFeaturesScope
    summary: BuildFeaturesSummary
    error: str


@dataclass(frozen=True)
class _SelectedSale:
    sales_transaction_id: int
    sales_parcel_match_id: int
    consideration_amount: Decimal
    transfer_date: date


@dataclass
class _FeatureDraft:
    parcel_id: str
    year: int
    assessment_total_value: Optional[Decimal]
    ratio: Optional[Decimal]
    municipality_key: Optional[str]
    classification_key: Optional[str]
    sale: Optional[_SelectedSale]
    flags: list[str]


def build_features(
    session: Session,
    *,
    feature_version: str,
    parcel_ids: Optional[Sequence[str]] = None,
    years: Optional[Sequence[int]] = None,
) -> BuildFeaturesPayload:
    raw_scope: dict[str, object] = {
        "requested_parcel_ids": list(parcel_ids) if parcel_ids is not None else None,
        "requested_years": list(years) if years is not None else None,
    }
    resolved_scope = _resolved_scope(parcel_ids=parcel_ids, years=years)
    config_json: dict[str, object] = {
        "feature_version": feature_version,
        "sale_selection_strategy": "latest_eligible_sale_same_year",
        "peer_percentile_min_group_size": PEER_PERCENTILE_MIN_GROUP_SIZE,
        "permit_capture_rate": str(PERMIT_CAPTURE_RATE),
        "appeal_window_years": APPEAL_WINDOW_YEARS,
        "lineage_reference_year_offset": LINEAGE_REFERENCE_YEAR_OFFSET,
    }
    run = ScoringRun(
        run_type=RUN_TYPE_BUILD_FEATURES,
        status="running",
        version_tag=feature_version,
        scope_hash=_scope_hash(resolved_scope, config_json),
        scope_json=dict(resolved_scope),
        config_json=config_json,
    )
    failure_summary: BuildFeaturesSummary = {
        "selected_parcels": 0,
        "selected_parcel_years": 0,
        "rows_deleted": 0,
        "rows_inserted": 0,
        "rows_skipped": 0,
        "quality_warning_count": 0,
    }

    try:
        session.add(run)
        session.flush()
        facts = _load_candidate_facts(
            session,
            parcel_ids=resolved_scope["parcel_ids"],
            years=resolved_scope["years"],
        )
        rows_deleted = 0
        rows_inserted = 0
        quality_warning_count = 0
        with session.begin_nested():
            rows_deleted = _delete_existing_feature_rows(
                session,
                feature_version=feature_version,
                parcel_ids=resolved_scope["parcel_ids"],
                years=resolved_scope["years"],
            )
            selected_sales = _load_selected_sales(session, facts)
            prior_assessments = _load_prior_assessments(session, facts)
            rows_inserted, quality_warning_count = _build_feature_rows(
                session=session,
                run_id=run.id,
                feature_version=feature_version,
                facts=facts,
                selected_sales=selected_sales,
                prior_assessments=prior_assessments,
            )
        summary: BuildFeaturesSummary = {
            "selected_parcels": len({fact.parcel_id for fact in facts}),
            "selected_parcel_years": len(facts),
            "rows_deleted": rows_deleted,
            "rows_inserted": rows_inserted,
            "rows_skipped": 0,
            "quality_warning_count": quality_warning_count,
        }
        run.status = "succeeded"
        run.error_summary = None
        run.input_summary_json = raw_scope
        run.output_summary_json = dict(summary)
        run.completed_at = datetime.now(timezone.utc)
        session.flush()

        return {
            "run": _run_payload(run),
            "scope": resolved_scope,
            "summary": summary,
        }
    except Exception as exc:
        run_persisted = True
        run.status = "failed"
        run.error_summary = str(exc)
        run.input_summary_json = raw_scope
        run.output_summary_json = dict(failure_summary)
        run.completed_at = datetime.now(timezone.utc)
        try:
            session.flush()
        except (PendingRollbackError, InvalidRequestError):
            session.rollback()
            run_persisted = False

        return {
            "run": _run_payload(run, run_persisted=run_persisted),
            "scope": resolved_scope,
            "summary": failure_summary,
            "error": str(exc),
        }


def _load_candidate_facts(
    session: Session,
    *,
    parcel_ids: Optional[list[str]],
    years: Optional[list[int]],
) -> list[ParcelYearFact]:
    if parcel_ids is not None and not parcel_ids:
        return []
    if years is not None and not years:
        return []

    if parcel_ids is None:
        query = select(ParcelYearFact).options(
            load_only(
                ParcelYearFact.parcel_id,
                ParcelYearFact.year,
                ParcelYearFact.assessment_total_value,
                ParcelYearFact.municipality_name,
                ParcelYearFact.assessment_valuation_classification,
            )
        )
        if years is not None:
            query = query.where(ParcelYearFact.year.in_(years))
        return list(
            session.execute(
                query.order_by(
                    ParcelYearFact.parcel_id.asc(), ParcelYearFact.year.asc()
                )
            )
            .scalars()
            .all()
        )

    facts: list[ParcelYearFact] = []
    for batch_parcel_ids in _chunked(parcel_ids, IN_CLAUSE_BATCH_SIZE):
        query = (
            select(ParcelYearFact)
            .options(
                load_only(
                    ParcelYearFact.parcel_id,
                    ParcelYearFact.year,
                    ParcelYearFact.assessment_total_value,
                    ParcelYearFact.municipality_name,
                    ParcelYearFact.assessment_valuation_classification,
                )
            )
            .where(ParcelYearFact.parcel_id.in_(batch_parcel_ids))
        )
        if years is not None:
            query = query.where(ParcelYearFact.year.in_(years))
        facts.extend(session.execute(query).scalars().all())
    return sorted(facts, key=lambda fact: (fact.parcel_id, fact.year))


def _load_selected_sales(
    session: Session,
    facts: Sequence[ParcelYearFact],
) -> dict[tuple[str, int], _SelectedSale]:
    if not facts:
        return {}

    parcel_ids = sorted({fact.parcel_id for fact in facts})
    years = sorted({fact.year for fact in facts})
    year_ranges = [
        and_(
            SalesTransaction.transfer_date >= date(year, 1, 1),
            SalesTransaction.transfer_date < date(year + 1, 1, 1),
        )
        for year in years
    ]
    has_active_exclusion = exists(
        select(SalesExclusion.id).where(
            and_(
                SalesExclusion.sales_transaction_id == SalesTransaction.id,
                SalesExclusion.is_active.is_(True),
            )
        )
    )
    base_query = (
        select(
            SalesParcelMatch.parcel_id,
            SalesTransaction.transfer_date,
            SalesTransaction.id,
            SalesParcelMatch.id,
            SalesTransaction.consideration_amount,
        )
        .select_from(SalesTransaction)
        .join(
            SalesParcelMatch,
            and_(
                SalesParcelMatch.sales_transaction_id == SalesTransaction.id,
                SalesParcelMatch.is_primary.is_(True),
            ),
        )
        .where(
            SalesTransaction.import_status == "loaded",
            SalesTransaction.transfer_date.is_not(None),
            SalesTransaction.consideration_amount.is_not(None),
            SalesTransaction.consideration_amount > 0,
            ~has_active_exclusion,
            or_(*year_ranges),
        )
    )

    selected: dict[tuple[str, int], _SelectedSale] = {}
    selected_rank: dict[tuple[str, int], tuple[date, int, int]] = {}
    for batch_parcel_ids in _chunked(parcel_ids, IN_CLAUSE_BATCH_SIZE):
        query = base_query.where(
            SalesParcelMatch.parcel_id.in_(batch_parcel_ids)
        ).order_by(
            SalesParcelMatch.parcel_id.asc(),
            SalesTransaction.transfer_date.desc(),
            SalesTransaction.id.desc(),
            SalesParcelMatch.id.desc(),
        )
        for (
            parcel_id,
            transfer_date,
            sales_transaction_id,
            sales_match_id,
            consideration_amount,
        ) in session.execute(query):
            sale_year = transfer_date.year
            key = (parcel_id, sale_year)
            rank = (transfer_date, sales_transaction_id, sales_match_id)
            if key in selected and selected_rank[key] >= rank:
                continue
            selected_rank[key] = rank
            selected[key] = _SelectedSale(
                sales_transaction_id=sales_transaction_id,
                sales_parcel_match_id=sales_match_id,
                consideration_amount=consideration_amount,
                transfer_date=transfer_date,
            )
    return selected


def _load_prior_assessments(
    session: Session, facts: Sequence[ParcelYearFact]
) -> dict[tuple[str, int], Optional[Decimal]]:
    if not facts:
        return {}
    parcel_ids = sorted({fact.parcel_id for fact in facts})
    prior_years = sorted({fact.year - 1 for fact in facts if fact.year > 1})
    if not prior_years:
        return {}

    prior: dict[tuple[str, int], Optional[Decimal]] = {}
    for batch_parcel_ids in _chunked(parcel_ids, IN_CLAUSE_BATCH_SIZE):
        query = select(
            ParcelYearFact.parcel_id,
            ParcelYearFact.year,
            ParcelYearFact.assessment_total_value,
        ).where(
            ParcelYearFact.parcel_id.in_(batch_parcel_ids),
            ParcelYearFact.year.in_(prior_years),
        )
        for parcel_id, year, assessment_total_value in session.execute(query):
            prior[(parcel_id, year)] = assessment_total_value
    return prior


def _build_feature_rows(
    *,
    session: Session,
    run_id: int,
    feature_version: str,
    facts: Sequence[ParcelYearFact],
    selected_sales: dict[tuple[str, int], _SelectedSale],
    prior_assessments: dict[tuple[str, int], Optional[Decimal]],
) -> tuple[int, int]:
    drafts: list[_FeatureDraft] = []
    for fact in facts:
        flags: list[str] = []
        sale = selected_sales.get((fact.parcel_id, fact.year))
        ratio = _assessment_to_sale_ratio(
            assessment_total_value=fact.assessment_total_value,
            sale=sale,
            flags=flags,
        )
        drafts.append(
            _FeatureDraft(
                parcel_id=fact.parcel_id,
                year=fact.year,
                assessment_total_value=fact.assessment_total_value,
                ratio=ratio,
                municipality_key=_canonical_text(fact.municipality_name),
                classification_key=_canonical_text(
                    fact.assessment_valuation_classification
                ),
                sale=sale,
                flags=flags,
            )
        )

    peer_groups: dict[tuple[int, str, str], list[Decimal]] = {}
    for draft in drafts:
        if (
            draft.ratio is None
            or draft.municipality_key is None
            or draft.classification_key is None
        ):
            continue
        key = (draft.year, draft.municipality_key, draft.classification_key)
        peer_groups.setdefault(key, []).append(draft.ratio)
    for ratios in peer_groups.values():
        ratios.sort()

    rows_inserted = 0
    quality_warning_count = 0
    batch_rows: list[ParcelFeature] = []

    def _flush_batch() -> None:
        nonlocal rows_inserted
        if not batch_rows:
            return
        session.add_all(batch_rows)
        session.flush()
        rows_inserted += len(batch_rows)
        for feature_row in batch_rows:
            session.expunge(feature_row)
        batch_rows.clear()

    for draft in drafts:
        peer_group_key = (
            (draft.year, draft.municipality_key, draft.classification_key)
            if draft.municipality_key is not None
            and draft.classification_key is not None
            else None
        )
        peer_group_values = (
            peer_groups.get(peer_group_key, []) if peer_group_key is not None else []
        )
        peer_group_size = len(peer_group_values) if peer_group_key is not None else None

        peer_percentile = _peer_percentile(
            ratio=draft.ratio,
            municipality_key=draft.municipality_key,
            classification_key=draft.classification_key,
            peer_group=peer_group_values,
            flags=draft.flags,
        )
        yoy_change = _yoy_assessment_change_pct(
            current_assessment=draft.assessment_total_value,
            prior_assessment=prior_assessments.get((draft.parcel_id, draft.year - 1)),
            flags=draft.flags,
        )
        appeal_start_year = draft.year - (APPEAL_WINDOW_YEARS - 1)
        lineage_reference_year = draft.year + LINEAGE_REFERENCE_YEAR_OFFSET

        feature_quality_flags = sorted(set(draft.flags))
        quality_warning_count += len(feature_quality_flags)
        row = ParcelFeature(
            run_id=run_id,
            parcel_id=draft.parcel_id,
            year=draft.year,
            feature_version=feature_version,
            assessment_to_sale_ratio=draft.ratio,
            peer_percentile=peer_percentile,
            yoy_assessment_change_pct=yoy_change,
            permit_adjusted_expected_change=None,
            permit_adjusted_gap=None,
            appeal_value_delta_3y=None,
            appeal_success_rate_3y=None,
            lineage_value_reset_delta=None,
            feature_quality_flags=feature_quality_flags,
            source_refs_json={
                "assessment": {"parcel_id": draft.parcel_id, "year": draft.year},
                "sales": {
                    "sales_transaction_id": (
                        draft.sale.sales_transaction_id if draft.sale else None
                    ),
                    "match_id": (
                        draft.sale.sales_parcel_match_id if draft.sale else None
                    ),
                },
                "peer_group": {
                    "year": draft.year,
                    "municipality": draft.municipality_key,
                    "classification": draft.classification_key,
                    "group_size": peer_group_size,
                },
                "permits": {
                    "window_years": [draft.year, draft.year],
                    "basis": "unknown",
                    "source_parcel_year_keys": [],
                },
                "appeals": {
                    "window_years": [appeal_start_year, draft.year],
                    "source_parcel_year_keys": [],
                },
                "lineage": {
                    "relationship_count": 0,
                    "related_parcel_ids": [],
                    "reference_year": lineage_reference_year,
                },
            },
        )
        batch_rows.append(row)
        if len(batch_rows) >= IN_CLAUSE_BATCH_SIZE:
            _flush_batch()

    _flush_batch()
    return rows_inserted, quality_warning_count


def _assessment_to_sale_ratio(
    *,
    assessment_total_value: Optional[Decimal],
    sale: Optional[_SelectedSale],
    flags: list[str],
) -> Optional[Decimal]:
    if assessment_total_value is None:
        flags.append("missing_assessment_total_value")
    elif assessment_total_value < 0:
        flags.append("invalid_assessment_total_value")

    if sale is None:
        flags.append("missing_eligible_sale")
    elif sale.consideration_amount <= 0:
        flags.append("invalid_sale_amount")

    if (
        assessment_total_value is None
        or assessment_total_value < 0
        or sale is None
        or sale.consideration_amount <= 0
    ):
        return None

    raw_value = assessment_total_value / sale.consideration_amount
    return _quantize_numeric(raw_value, precision=10, scale=6, flags=flags)


def _peer_percentile(
    *,
    ratio: Optional[Decimal],
    municipality_key: Optional[str],
    classification_key: Optional[str],
    peer_group: Sequence[Decimal],
    flags: list[str],
) -> Optional[Decimal]:
    if ratio is None:
        return None
    if municipality_key is None or classification_key is None:
        flags.append("missing_peer_group_dimensions")
        return None
    if len(peer_group) < PEER_PERCENTILE_MIN_GROUP_SIZE:
        flags.append("insufficient_peer_group")
        return None

    lower_count = bisect_left(peer_group, ratio)
    equal_count = bisect_right(peer_group, ratio) - lower_count
    numerator = Decimal(lower_count) + (Decimal("0.5") * Decimal(equal_count))
    raw_value = numerator / Decimal(len(peer_group))
    return _quantize_numeric(raw_value, precision=6, scale=4, flags=flags)


def _yoy_assessment_change_pct(
    *,
    current_assessment: Optional[Decimal],
    prior_assessment: Optional[Decimal],
    flags: list[str],
) -> Optional[Decimal]:
    if current_assessment is None:
        flags.append("missing_assessment_total_value")
        return None
    if prior_assessment is None:
        flags.append("missing_prior_year_assessment")
        return None
    if prior_assessment <= 0:
        flags.append("nonpositive_prior_year_assessment")
        return None
    raw_value = (current_assessment - prior_assessment) / prior_assessment
    return _quantize_numeric(raw_value, precision=10, scale=6, flags=flags)


def _quantize_numeric(
    value: Decimal, *, precision: int, scale: int, flags: list[str]
) -> Decimal:
    quantized = value.quantize(Decimal(10) ** -scale, rounding=ROUND_HALF_UP)
    max_abs = (Decimal(10) ** (precision - scale)) - (Decimal(10) ** -scale)
    clamped = quantized
    if quantized > max_abs:
        clamped = max_abs
    elif quantized < -max_abs:
        clamped = -max_abs
    if clamped != quantized:
        flags.append("numeric_precision_clamped")
    return clamped


def _delete_existing_feature_rows(
    session: Session,
    *,
    feature_version: str,
    parcel_ids: Optional[list[str]],
    years: Optional[list[int]],
) -> int:
    if parcel_ids is not None and not parcel_ids:
        return 0
    if years is not None and not years:
        return 0

    bind = session.get_bind()
    supports_sane_rowcount = bool(
        bind is not None and getattr(bind.dialect, "supports_sane_rowcount", False)
    )

    if parcel_ids is None:
        query = delete(ParcelFeature).where(
            ParcelFeature.feature_version == feature_version
        )
        if years is not None:
            query = query.where(ParcelFeature.year.in_(years))
        if supports_sane_rowcount:
            result = cast(CursorResult[object], session.execute(query))
            return int(result.rowcount or 0)
        count_query = select(ParcelFeature.id).where(
            ParcelFeature.feature_version == feature_version
        )
        if years is not None:
            count_query = count_query.where(ParcelFeature.year.in_(years))
        to_delete = len(session.execute(count_query).scalars().all())
        session.execute(query)
        return to_delete

    deleted = 0
    for batch_parcel_ids in _chunked(parcel_ids, IN_CLAUSE_BATCH_SIZE):
        query = delete(ParcelFeature).where(
            ParcelFeature.feature_version == feature_version,
            ParcelFeature.parcel_id.in_(batch_parcel_ids),
        )
        if years is not None:
            query = query.where(ParcelFeature.year.in_(years))
        if supports_sane_rowcount:
            result = cast(CursorResult[object], session.execute(query))
            deleted += int(result.rowcount or 0)
            continue
        count_query = select(ParcelFeature.id).where(
            ParcelFeature.feature_version == feature_version,
            ParcelFeature.parcel_id.in_(batch_parcel_ids),
        )
        if years is not None:
            count_query = count_query.where(ParcelFeature.year.in_(years))
        to_delete = len(session.execute(count_query).scalars().all())
        session.execute(query)
        deleted += to_delete
    return deleted


def _resolved_scope(
    *, parcel_ids: Optional[Sequence[str]], years: Optional[Sequence[int]]
) -> BuildFeaturesScope:
    resolved_parcel_ids = (
        sorted({parcel_id.strip() for parcel_id in parcel_ids if parcel_id.strip()})
        if parcel_ids is not None
        else None
    )
    resolved_years = sorted(set(years)) if years is not None else None
    return {"parcel_ids": resolved_parcel_ids, "years": resolved_years}


def _scope_hash(scope_json: BuildFeaturesScope, config_json: dict[str, object]) -> str:
    digest_input = json.dumps(
        {"scope": scope_json, "config": config_json},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(digest_input).hexdigest()


def _run_payload(run: ScoringRun, *, run_persisted: bool = True) -> BuildFeaturesRun:
    run_id = run.id if (run_persisted and run.id is not None) else None
    return {
        "run_id": run_id,
        "run_persisted": run_persisted,
        "run_type": run.run_type,
        "version_tag": run.version_tag,
        "status": run.status,
    }


def _canonical_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped.casefold()


def _chunked(values: Sequence[str], batch_size: int) -> list[list[str]]:
    return [
        list(values[index : index + batch_size])
        for index in range(0, len(values), batch_size)
    ]
