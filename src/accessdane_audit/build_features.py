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

from .extraction_signals import LINEAGE_PARENT_RELATIONSHIP
from .models import (
    ParcelFeature,
    ParcelLineageLink,
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
    permit_event_count: Optional[int]
    permit_declared_valuation_known_count: Optional[int]
    permit_declared_valuation_sum: Optional[Decimal]
    permit_estimated_cost_known_count: Optional[int]
    permit_estimated_cost_sum: Optional[Decimal]
    sale: Optional[_SelectedSale]
    flags: list[str]


@dataclass(frozen=True)
class _AppealYearContext:
    appeal_event_count: Optional[int]
    appeal_reduction_granted_count: Optional[int]
    appeal_partial_reduction_count: Optional[int]
    appeal_denied_count: Optional[int]
    appeal_withdrawn_count: Optional[int]
    appeal_dismissed_count: Optional[int]
    appeal_pending_count: Optional[int]
    appeal_unknown_outcome_count: Optional[int]
    appeal_value_change_known_count: Optional[int]
    appeal_value_change_total: Optional[Decimal]
    appeal_value_change_reduction_total: Optional[Decimal]
    appeal_value_change_increase_total: Optional[Decimal]

    def has_context(self) -> bool:
        return any(
            value is not None
            for value in (
                self.appeal_event_count,
                self.appeal_reduction_granted_count,
                self.appeal_partial_reduction_count,
                self.appeal_denied_count,
                self.appeal_withdrawn_count,
                self.appeal_dismissed_count,
                self.appeal_pending_count,
                self.appeal_unknown_outcome_count,
                self.appeal_value_change_known_count,
                self.appeal_value_change_total,
                self.appeal_value_change_reduction_total,
                self.appeal_value_change_increase_total,
            )
        )


class _ParcelYearKey(TypedDict):
    parcel_id: str
    year: int


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
            appeal_contexts = _load_appeal_contexts(session, facts)
            lineage_links = _load_lineage_links(session, facts)
            lineage_reference_assessments = _load_lineage_reference_assessments(
                session,
                facts=facts,
                lineage_links=lineage_links,
            )
            rows_inserted, quality_warning_count = _build_feature_rows(
                session=session,
                run_id=run.id,
                feature_version=feature_version,
                facts=facts,
                selected_sales=selected_sales,
                prior_assessments=prior_assessments,
                appeal_contexts=appeal_contexts,
                lineage_links=lineage_links,
                lineage_reference_assessments=lineage_reference_assessments,
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
                ParcelYearFact.permit_event_count,
                ParcelYearFact.permit_declared_valuation_known_count,
                ParcelYearFact.permit_declared_valuation_sum,
                ParcelYearFact.permit_estimated_cost_known_count,
                ParcelYearFact.permit_estimated_cost_sum,
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
                    ParcelYearFact.permit_event_count,
                    ParcelYearFact.permit_declared_valuation_known_count,
                    ParcelYearFact.permit_declared_valuation_sum,
                    ParcelYearFact.permit_estimated_cost_known_count,
                    ParcelYearFact.permit_estimated_cost_sum,
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


def _load_appeal_contexts(
    session: Session, facts: Sequence[ParcelYearFact]
) -> dict[tuple[str, int], _AppealYearContext]:
    if not facts:
        return {}

    parcel_ids = sorted({fact.parcel_id for fact in facts})
    window_years = sorted(
        {
            year
            for fact in facts
            for year in range(fact.year - (APPEAL_WINDOW_YEARS - 1), fact.year + 1)
        }
    )
    if not window_years:
        return {}

    contexts: dict[tuple[str, int], _AppealYearContext] = {}
    for batch_parcel_ids in _chunked(parcel_ids, IN_CLAUSE_BATCH_SIZE):
        query = select(
            ParcelYearFact.parcel_id,
            ParcelYearFact.year,
            ParcelYearFact.appeal_event_count,
            ParcelYearFact.appeal_reduction_granted_count,
            ParcelYearFact.appeal_partial_reduction_count,
            ParcelYearFact.appeal_denied_count,
            ParcelYearFact.appeal_withdrawn_count,
            ParcelYearFact.appeal_dismissed_count,
            ParcelYearFact.appeal_pending_count,
            ParcelYearFact.appeal_unknown_outcome_count,
            ParcelYearFact.appeal_value_change_known_count,
            ParcelYearFact.appeal_value_change_total,
            ParcelYearFact.appeal_value_change_reduction_total,
            ParcelYearFact.appeal_value_change_increase_total,
        ).where(
            ParcelYearFact.parcel_id.in_(batch_parcel_ids),
            ParcelYearFact.year.in_(window_years),
        )
        for (
            parcel_id,
            year,
            appeal_event_count,
            appeal_reduction_granted_count,
            appeal_partial_reduction_count,
            appeal_denied_count,
            appeal_withdrawn_count,
            appeal_dismissed_count,
            appeal_pending_count,
            appeal_unknown_outcome_count,
            appeal_value_change_known_count,
            appeal_value_change_total,
            appeal_value_change_reduction_total,
            appeal_value_change_increase_total,
        ) in session.execute(query):
            contexts[(parcel_id, year)] = _AppealYearContext(
                appeal_event_count=appeal_event_count,
                appeal_reduction_granted_count=appeal_reduction_granted_count,
                appeal_partial_reduction_count=appeal_partial_reduction_count,
                appeal_denied_count=appeal_denied_count,
                appeal_withdrawn_count=appeal_withdrawn_count,
                appeal_dismissed_count=appeal_dismissed_count,
                appeal_pending_count=appeal_pending_count,
                appeal_unknown_outcome_count=appeal_unknown_outcome_count,
                appeal_value_change_known_count=appeal_value_change_known_count,
                appeal_value_change_total=appeal_value_change_total,
                appeal_value_change_reduction_total=appeal_value_change_reduction_total,
                appeal_value_change_increase_total=appeal_value_change_increase_total,
            )
    return contexts


def _load_lineage_links(
    session: Session, facts: Sequence[ParcelYearFact]
) -> dict[str, list[str]]:
    if not facts:
        return {}

    parcel_ids = sorted({fact.parcel_id for fact in facts})
    related_parcels_by_parcel: dict[str, set[str]] = {}
    for batch_parcel_ids in _chunked(parcel_ids, IN_CLAUSE_BATCH_SIZE):
        query = select(
            ParcelLineageLink.parcel_id,
            ParcelLineageLink.related_parcel_id,
        ).where(
            ParcelLineageLink.parcel_id.in_(batch_parcel_ids),
            ParcelLineageLink.relationship_type == LINEAGE_PARENT_RELATIONSHIP,
        )
        for parcel_id, related_parcel_id in session.execute(query):
            related_parcels_by_parcel.setdefault(parcel_id, set()).add(
                related_parcel_id
            )

    return {
        parcel_id: sorted(related_parcel_ids)
        for parcel_id, related_parcel_ids in related_parcels_by_parcel.items()
    }


def _load_lineage_reference_assessments(
    session: Session,
    *,
    facts: Sequence[ParcelYearFact],
    lineage_links: dict[str, list[str]],
) -> dict[tuple[str, int], Optional[Decimal]]:
    if not facts or not lineage_links:
        return {}

    related_parcel_ids = sorted(
        {parcel_id for values in lineage_links.values() for parcel_id in values}
    )
    reference_years = sorted(
        {fact.year + LINEAGE_REFERENCE_YEAR_OFFSET for fact in facts}
    )
    if not related_parcel_ids or not reference_years:
        return {}

    reference_assessments: dict[tuple[str, int], Optional[Decimal]] = {}
    for batch_parcel_ids in _chunked(related_parcel_ids, IN_CLAUSE_BATCH_SIZE):
        query = select(
            ParcelYearFact.parcel_id,
            ParcelYearFact.year,
            ParcelYearFact.assessment_total_value,
        ).where(
            ParcelYearFact.parcel_id.in_(batch_parcel_ids),
            ParcelYearFact.year.in_(reference_years),
        )
        for parcel_id, year, assessment_total_value in session.execute(query):
            reference_assessments[(parcel_id, year)] = assessment_total_value
    return reference_assessments


def _build_feature_rows(
    *,
    session: Session,
    run_id: int,
    feature_version: str,
    facts: Sequence[ParcelYearFact],
    selected_sales: dict[tuple[str, int], _SelectedSale],
    prior_assessments: dict[tuple[str, int], Optional[Decimal]],
    appeal_contexts: dict[tuple[str, int], _AppealYearContext],
    lineage_links: dict[str, list[str]],
    lineage_reference_assessments: dict[tuple[str, int], Optional[Decimal]],
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
                permit_event_count=fact.permit_event_count,
                permit_declared_valuation_known_count=(
                    fact.permit_declared_valuation_known_count
                ),
                permit_declared_valuation_sum=fact.permit_declared_valuation_sum,
                permit_estimated_cost_known_count=fact.permit_estimated_cost_known_count,
                permit_estimated_cost_sum=fact.permit_estimated_cost_sum,
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
        permit_adjusted_expected_change, permit_basis, permit_source_keys = (
            _permit_adjusted_expected_change(
                parcel_id=draft.parcel_id,
                year=draft.year,
                permit_event_count=draft.permit_event_count,
                permit_declared_valuation_known_count=(
                    draft.permit_declared_valuation_known_count
                ),
                permit_declared_valuation_sum=draft.permit_declared_valuation_sum,
                permit_estimated_cost_known_count=draft.permit_estimated_cost_known_count,
                permit_estimated_cost_sum=draft.permit_estimated_cost_sum,
                flags=draft.flags,
            )
        )
        prior_assessment = prior_assessments.get((draft.parcel_id, draft.year - 1))
        permit_adjusted_gap = _permit_adjusted_gap(
            current_assessment=draft.assessment_total_value,
            prior_assessment=prior_assessment,
            permit_adjusted_expected_change=permit_adjusted_expected_change,
            flags=draft.flags,
        )
        (
            appeal_value_delta_3y,
            appeal_success_rate_3y,
            appeal_source_keys,
            appeal_value_detail_status,
            appeal_outcome_detail_status,
        ) = _appeal_window_features(
            parcel_id=draft.parcel_id,
            year=draft.year,
            appeal_contexts=appeal_contexts,
            flags=draft.flags,
        )
        appeal_start_year = draft.year - (APPEAL_WINDOW_YEARS - 1)
        lineage_reference_year = draft.year + LINEAGE_REFERENCE_YEAR_OFFSET
        related_parcel_ids = lineage_links.get(draft.parcel_id, [])
        lineage_value_reset_delta = _lineage_value_reset_delta(
            current_assessment=draft.assessment_total_value,
            related_parcel_ids=related_parcel_ids,
            reference_year=lineage_reference_year,
            lineage_reference_assessments=lineage_reference_assessments,
            flags=draft.flags,
        )

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
            permit_adjusted_expected_change=permit_adjusted_expected_change,
            permit_adjusted_gap=permit_adjusted_gap,
            appeal_value_delta_3y=appeal_value_delta_3y,
            appeal_success_rate_3y=appeal_success_rate_3y,
            lineage_value_reset_delta=lineage_value_reset_delta,
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
                    "basis": permit_basis,
                    "source_parcel_year_keys": permit_source_keys,
                },
                "appeals": {
                    "window_years": [appeal_start_year, draft.year],
                    "source_parcel_year_keys": appeal_source_keys,
                    "value_detail_status": appeal_value_detail_status,
                    "outcome_detail_status": appeal_outcome_detail_status,
                },
                "lineage": {
                    "relationship_count": len(related_parcel_ids),
                    "related_parcel_ids": related_parcel_ids,
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


def _permit_adjusted_expected_change(
    *,
    parcel_id: str,
    year: int,
    permit_event_count: Optional[int],
    permit_declared_valuation_known_count: Optional[int],
    permit_declared_valuation_sum: Optional[Decimal],
    permit_estimated_cost_known_count: Optional[int],
    permit_estimated_cost_sum: Optional[Decimal],
    flags: list[str],
) -> tuple[Optional[Decimal], str, list[_ParcelYearKey]]:
    source_keys = [_source_parcel_year_key(parcel_id=parcel_id, year=year)]
    event_count = permit_event_count
    declared_known_count = permit_declared_valuation_known_count or 0
    estimated_known_count = permit_estimated_cost_known_count or 0

    if event_count is not None and event_count > 0:
        declared_full = declared_known_count == event_count
        estimated_full = estimated_known_count == event_count

        if declared_full and not estimated_full:
            if permit_declared_valuation_sum is None:
                flags.append("missing_permit_context")
                return None, "unknown", source_keys
            raw_value = permit_declared_valuation_sum * PERMIT_CAPTURE_RATE
            return (
                _quantize_numeric(raw_value, precision=14, scale=2, flags=flags),
                "declared",
                source_keys,
            )

        if estimated_full and not declared_full:
            if permit_estimated_cost_sum is None:
                flags.append("missing_permit_context")
                return None, "unknown", source_keys
            raw_value = permit_estimated_cost_sum * PERMIT_CAPTURE_RATE
            return (
                _quantize_numeric(raw_value, precision=14, scale=2, flags=flags),
                "estimated",
                source_keys,
            )

        flags.append("unresolved_permit_basis")
        return None, "unknown", source_keys

    if declared_known_count > 0 or estimated_known_count > 0:
        flags.append("missing_permit_context")
        return None, "unknown", source_keys

    has_unattributed_sum = permit_declared_valuation_sum not in (
        None,
        Decimal("0"),
    ) or permit_estimated_cost_sum not in (None, Decimal("0"))
    if has_unattributed_sum:
        flags.append("missing_permit_context")
        return None, "unknown", source_keys

    if (permit_event_count or 0) > 0:
        flags.append("unresolved_permit_basis")
        return None, "unknown", source_keys

    flags.append("permit_zero_signal_inferred")
    zero_value = _quantize_numeric(Decimal("0"), precision=14, scale=2, flags=flags)
    return zero_value, "none", []


def _permit_adjusted_gap(
    *,
    current_assessment: Optional[Decimal],
    prior_assessment: Optional[Decimal],
    permit_adjusted_expected_change: Optional[Decimal],
    flags: list[str],
) -> Optional[Decimal]:
    has_missing_input = False
    if permit_adjusted_expected_change is None:
        flags.append("missing_permit_adjusted_expected_change")
        has_missing_input = True
    if current_assessment is None or prior_assessment is None:
        flags.append("missing_assessment_for_permit_gap")
        has_missing_input = True
    if has_missing_input:
        return None
    assert permit_adjusted_expected_change is not None
    assert current_assessment is not None
    assert prior_assessment is not None

    raw_value = (
        current_assessment - prior_assessment
    ) - permit_adjusted_expected_change
    return _quantize_numeric(raw_value, precision=14, scale=2, flags=flags)


def _appeal_window_features(
    *,
    parcel_id: str,
    year: int,
    appeal_contexts: dict[tuple[str, int], _AppealYearContext],
    flags: list[str],
) -> tuple[Optional[Decimal], Optional[Decimal], list[_ParcelYearKey], str, str]:
    appeal_start_year = year - (APPEAL_WINDOW_YEARS - 1)
    source_keys: list[_ParcelYearKey] = []
    total_value_delta = Decimal("0")
    success_numerator = 0
    success_denominator = 0
    has_any_appeal_context = False
    missing_value_detail = False
    missing_outcome_detail = False

    for context_year in range(appeal_start_year, year + 1):
        context = appeal_contexts.get((parcel_id, context_year))
        if context is None or not context.has_context():
            continue
        has_any_appeal_context = True
        source_keys.append(
            _source_parcel_year_key(parcel_id=parcel_id, year=context_year)
        )
        value_change_known_count = context.appeal_value_change_known_count
        if (
            value_change_known_count is None
            or value_change_known_count <= 0
            or context.appeal_value_change_total is None
        ):
            missing_value_detail = True
        else:
            total_value_delta += context.appeal_value_change_total

        if (
            context.appeal_unknown_outcome_count is not None
            and context.appeal_unknown_outcome_count > 0
        ):
            missing_outcome_detail = True
        elif (
            context.appeal_event_count is None
            or context.appeal_reduction_granted_count is None
            or context.appeal_partial_reduction_count is None
        ):
            missing_outcome_detail = True
        else:
            success_numerator += context.appeal_reduction_granted_count + (
                context.appeal_partial_reduction_count
            )
            success_denominator += context.appeal_event_count

    if not has_any_appeal_context:
        flags.append("missing_appeal_context_3y")
        return None, None, [], "none", "none"

    appeal_value_delta_3y: Optional[Decimal]
    if missing_value_detail:
        flags.append("missing_appeal_value_detail_3y")
        appeal_value_delta_3y = None
    else:
        appeal_value_delta_3y = _quantize_numeric(
            total_value_delta, precision=14, scale=2, flags=flags
        )

    appeal_success_rate_3y: Optional[Decimal]
    if missing_outcome_detail:
        flags.append("missing_appeal_outcome_detail_3y")
        appeal_success_rate_3y = None
    elif success_denominator <= 0:
        flags.append("no_appeals_in_window")
        appeal_success_rate_3y = None
    else:
        appeal_success_rate_3y = _quantize_numeric(
            Decimal(success_numerator) / Decimal(success_denominator),
            precision=6,
            scale=4,
            flags=flags,
        )

    value_detail_status = "missing" if missing_value_detail else "known"
    outcome_detail_status = "missing" if missing_outcome_detail else "known"
    return (
        appeal_value_delta_3y,
        appeal_success_rate_3y,
        source_keys,
        value_detail_status,
        outcome_detail_status,
    )


def _lineage_value_reset_delta(
    *,
    current_assessment: Optional[Decimal],
    related_parcel_ids: Sequence[str],
    reference_year: int,
    lineage_reference_assessments: dict[tuple[str, int], Optional[Decimal]],
    flags: list[str],
) -> Optional[Decimal]:
    if current_assessment is None:
        flags.append("missing_current_assessment_for_lineage")
        return None

    if not related_parcel_ids:
        return _quantize_numeric(Decimal("0"), precision=14, scale=2, flags=flags)

    related_reference_total = Decimal("0")
    for related_parcel_id in related_parcel_ids:
        related_reference = lineage_reference_assessments.get(
            (related_parcel_id, reference_year)
        )
        if related_reference is None:
            flags.append("missing_lineage_reference_values")
            return None
        related_reference_total += related_reference

    raw_value = current_assessment - related_reference_total
    return _quantize_numeric(raw_value, precision=14, scale=2, flags=flags)


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


def _source_parcel_year_key(*, parcel_id: str, year: int) -> _ParcelYearKey:
    return {"parcel_id": parcel_id, "year": year}


def _chunked(values: Sequence[str], batch_size: int) -> list[list[str]]:
    return [
        list(values[index : index + batch_size])
        for index in range(0, len(values), batch_size)
    ]
