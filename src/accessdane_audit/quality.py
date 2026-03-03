from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .models import (
    AssessmentRecord,
    Fetch,
    ParcelCharacteristic,
    ParcelLineageLink,
    ParcelSummary,
    ParcelYearFact,
    PaymentRecord,
    TaxRecord,
)

ASSESSMENT_STALE_YEAR_THRESHOLD = 4
ASSESSMENT_EXPECTED_CARRY_FORWARD_RUN_LENGTH = 5
VALID_LINEAGE_RELATIONSHIP_TYPES = {"parent", "child"}
PAYMENT_HISTORY_PLACEHOLDER = "No payments found."
UNPARSED_SUCCESSFUL_FETCH_MESSAGE = (
    "Successful fetch has neither parse results nor parse error."
)
PARSED_WITHOUT_RECORDS_MESSAGE = "Fetch was marked parsed but no records were stored."


@dataclass
class QualityIssue:
    code: str
    message: str
    parcel_id: Optional[str] = None
    fetch_id: Optional[int] = None
    year: Optional[int] = None
    details: dict[str, object] = field(default_factory=dict)


@dataclass
class QualityCheckResult:
    code: str
    description: str
    issues: list[QualityIssue]

    @property
    def passed(self) -> bool:
        return not self.issues


@dataclass
class QualityReport:
    generated_at: datetime
    checks: list[QualityCheckResult]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)


@dataclass
class _PaymentHistorySignals:
    has_placeholder_row: bool = False
    has_non_placeholder_row: bool = False


def run_data_quality_checks(
    session: Session,
    *,
    parcel_ids: Optional[Iterable[str]] = None,
) -> QualityReport:
    parcel_filter = set(parcel_ids) if parcel_ids else None
    checks = [
        _check_duplicate_parcel_summaries(session, parcel_filter),
        _check_suspicious_assessment_dates(session, parcel_filter),
        _check_impossible_numeric_values(session, parcel_filter),
        _check_fetch_parse_consistency(session, parcel_filter),
        _check_parcel_characteristics_consistency(session, parcel_filter),
        _check_lineage_consistency(session, parcel_filter),
        _check_payment_history_semantics(session, parcel_filter),
    ]
    return QualityReport(
        generated_at=datetime.now(timezone.utc),
        checks=checks,
    )


def quality_report_to_dict(report: QualityReport) -> dict[str, object]:
    return {
        "generated_at": report.generated_at.isoformat(),
        "passed": report.passed,
        "checks": [
            {
                "code": check.code,
                "description": check.description,
                "passed": check.passed,
                "issue_count": len(check.issues),
                "issues": [
                    {
                        "code": issue.code,
                        "message": issue.message,
                        "parcel_id": issue.parcel_id,
                        "fetch_id": issue.fetch_id,
                        "year": issue.year,
                        "details": issue.details,
                    }
                    for issue in check.issues
                ],
            }
            for check in report.checks
        ],
    }


def _check_duplicate_parcel_summaries(
    session: Session,
    parcel_filter: Optional[set[str]],
) -> QualityCheckResult:
    query = (
        select(ParcelSummary.parcel_id, func.count(ParcelSummary.id))
        .group_by(ParcelSummary.parcel_id)
        .having(func.count(ParcelSummary.id) > 1)
    )
    if parcel_filter:
        query = query.where(ParcelSummary.parcel_id.in_(parcel_filter))

    issues = [
        QualityIssue(
            code="duplicate_parcel_summary",
            message="Multiple parcel_summaries rows exist for one parcel.",
            parcel_id=parcel_id,
            details={"row_count": row_count},
        )
        for parcel_id, row_count in session.execute(query).all()
    ]
    return QualityCheckResult(
        code="duplicate_parcel_summaries",
        description="Detect duplicate parcel summary rows per parcel.",
        issues=issues,
    )


def _check_suspicious_assessment_dates(
    session: Session,
    parcel_filter: Optional[set[str]],
) -> QualityCheckResult:
    query = select(AssessmentRecord)
    if parcel_filter:
        query = query.where(AssessmentRecord.parcel_id.in_(parcel_filter))

    records = session.execute(query).scalars().all()
    expected_carry_forward_ids = _expected_carry_forward_record_ids(records)
    today = date.today()
    issues: list[QualityIssue] = []
    for record in records:
        if record.valuation_date is None:
            continue
        if record.valuation_date > today:
            issues.append(
                QualityIssue(
                    code="future_assessment_date",
                    message="Assessment valuation date is in the future.",
                    parcel_id=record.parcel_id,
                    fetch_id=record.fetch_id,
                    year=record.year,
                    details={"valuation_date": record.valuation_date.isoformat()},
                )
            )
            continue
        if record.year is None:
            continue
        if record.valuation_date.year > record.year:
            issues.append(
                QualityIssue(
                    code="assessment_date_after_year",
                    message="Assessment valuation date falls after the parcel-year.",
                    parcel_id=record.parcel_id,
                    fetch_id=record.fetch_id,
                    year=record.year,
                    details={"valuation_date": record.valuation_date.isoformat()},
                )
            )
        elif record.valuation_date.year < record.year - ASSESSMENT_STALE_YEAR_THRESHOLD:
            if record.id in expected_carry_forward_ids:
                continue
            issues.append(
                QualityIssue(
                    code="stale_assessment_date",
                    message=(
                        "Assessment valuation date is more than four years "
                        "older than the parcel-year."
                    ),
                    parcel_id=record.parcel_id,
                    fetch_id=record.fetch_id,
                    year=record.year,
                    details={"valuation_date": record.valuation_date.isoformat()},
                )
            )

    return QualityCheckResult(
        code="suspicious_assessment_dates",
        description="Detect future or stale assessment valuation dates.",
        issues=issues,
    )


def _expected_carry_forward_record_ids(records: Iterable[AssessmentRecord]) -> set[int]:
    grouped: dict[tuple[object, ...], list[AssessmentRecord]] = defaultdict(list)
    for record in records:
        if record.id is None or record.year is None or record.valuation_date is None:
            continue
        grouped[
            (
                record.parcel_id,
                record.valuation_date,
                record.valuation_classification,
                record.assessment_acres,
                record.land_value,
                record.improved_value,
                record.total_value,
            )
        ].append(record)

    carried_forward_ids: set[int] = set()
    for group in grouped.values():
        records_by_year: dict[int, list[AssessmentRecord]] = defaultdict(list)
        for record in group:
            if record.year is None:
                continue
            records_by_year[record.year].append(record)
        years = sorted(records_by_year)
        if not years:
            continue
        current_run = [years[0]]
        for year in years[1:]:
            if year == current_run[-1] + 1:
                current_run.append(year)
                continue
            if len(current_run) >= ASSESSMENT_EXPECTED_CARRY_FORWARD_RUN_LENGTH:
                for run_year in current_run:
                    carried_forward_ids.update(
                        record.id
                        for record in records_by_year[run_year]
                        if record.id is not None
                    )
            current_run = [year]
        if len(current_run) >= ASSESSMENT_EXPECTED_CARRY_FORWARD_RUN_LENGTH:
            for run_year in current_run:
                carried_forward_ids.update(
                    record.id
                    for record in records_by_year[run_year]
                    if record.id is not None
                )

    return carried_forward_ids


def _check_impossible_numeric_values(
    session: Session,
    parcel_filter: Optional[set[str]],
) -> QualityCheckResult:
    issues: list[QualityIssue] = []

    assessment_query = select(AssessmentRecord)
    if parcel_filter:
        assessment_query = assessment_query.where(
            AssessmentRecord.parcel_id.in_(parcel_filter)
        )
    for record in session.execute(assessment_query).scalars():
        for field_name, value in (
            ("assessment_acres", record.assessment_acres),
            ("land_value", record.land_value),
            ("improved_value", record.improved_value),
            ("total_value", record.total_value),
        ):
            if value is not None and value < 0:
                issues.append(
                    QualityIssue(
                        code="negative_assessment_value",
                        message="Assessment contains a negative numeric value.",
                        parcel_id=record.parcel_id,
                        fetch_id=record.fetch_id,
                        year=record.year,
                        details={"field": field_name, "value": str(value)},
                    )
                )
        if (
            record.total_value is not None
            and record.land_value is not None
            and record.improved_value is not None
            and record.total_value < (record.land_value + record.improved_value)
        ):
            issues.append(
                QualityIssue(
                    code="assessment_total_less_than_components",
                    message="Assessment total is less than land + improved value.",
                    parcel_id=record.parcel_id,
                    fetch_id=record.fetch_id,
                    year=record.year,
                    details={
                        "land_value": str(record.land_value),
                        "improved_value": str(record.improved_value),
                        "total_value": str(record.total_value),
                    },
                )
            )

    tax_query = select(TaxRecord)
    if parcel_filter:
        tax_query = tax_query.where(TaxRecord.parcel_id.in_(parcel_filter))
    for tax_record in session.execute(tax_query).scalars():
        if _has_negative_amount(tax_record.data):
            issues.append(
                QualityIssue(
                    code="negative_tax_amount",
                    message="Tax record contains a negative amount.",
                    parcel_id=tax_record.parcel_id,
                    fetch_id=tax_record.fetch_id,
                    year=tax_record.year,
                )
            )

    payment_query = select(PaymentRecord)
    if parcel_filter:
        payment_query = payment_query.where(PaymentRecord.parcel_id.in_(parcel_filter))
    for payment_record in session.execute(payment_query).scalars():
        if _has_negative_amount(payment_record.data):
            issues.append(
                QualityIssue(
                    code="negative_payment_amount",
                    message="Payment record contains a negative amount.",
                    parcel_id=payment_record.parcel_id,
                    fetch_id=payment_record.fetch_id,
                    year=payment_record.year,
                )
            )

    return QualityCheckResult(
        code="impossible_numeric_values",
        description="Detect negative or internally inconsistent numeric values.",
        issues=issues,
    )


def _check_fetch_parse_consistency(
    session: Session,
    parcel_filter: Optional[set[str]],
) -> QualityCheckResult:
    query = select(Fetch)
    if parcel_filter:
        query = query.where(Fetch.parcel_id.in_(parcel_filter))

    assessment_counts = _counts_by_fetch_id(session, AssessmentRecord, parcel_filter)
    tax_counts = _counts_by_fetch_id(session, TaxRecord, parcel_filter)
    payment_counts = _counts_by_fetch_id(session, PaymentRecord, parcel_filter)
    summary_fetch_ids = _summary_fetch_ids(session, parcel_filter)

    issues: list[QualityIssue] = []
    for fetch in session.execute(query).scalars():
        parsed_record_count = (
            assessment_counts.get(fetch.id, 0)
            + tax_counts.get(fetch.id, 0)
            + payment_counts.get(fetch.id, 0)
            + (1 if fetch.id in summary_fetch_ids else 0)
        )

        if fetch.status_code == 200 and not fetch.raw_path:
            issues.append(
                QualityIssue(
                    code="missing_raw_path",
                    message="Successful fetch is missing raw HTML path.",
                    parcel_id=fetch.parcel_id,
                    fetch_id=fetch.id,
                )
            )
        if (
            fetch.status_code == 200
            and fetch.parsed_at is None
            and fetch.parse_error is None
        ):
            issues.append(
                QualityIssue(
                    code="unparsed_successful_fetch",
                    message=UNPARSED_SUCCESSFUL_FETCH_MESSAGE,
                    parcel_id=fetch.parcel_id,
                    fetch_id=fetch.id,
                )
            )
        if fetch.status_code != 200 and fetch.parsed_at is not None:
            issues.append(
                QualityIssue(
                    code="parsed_non_200_fetch",
                    message="Non-200 fetch was marked as parsed.",
                    parcel_id=fetch.parcel_id,
                    fetch_id=fetch.id,
                    details={"status_code": fetch.status_code},
                )
            )
        if (
            fetch.parsed_at is not None
            and fetch.parse_error is None
            and parsed_record_count == 0
        ):
            issues.append(
                QualityIssue(
                    code="parsed_without_records",
                    message=PARSED_WITHOUT_RECORDS_MESSAGE,
                    parcel_id=fetch.parcel_id,
                    fetch_id=fetch.id,
                )
            )

    return QualityCheckResult(
        code="fetch_parse_consistency",
        description=(
            "Detect missing raw files, unparsed successful fetches, and "
            "parse/result mismatches."
        ),
        issues=issues,
    )


def _check_parcel_characteristics_consistency(
    session: Session,
    parcel_filter: Optional[set[str]],
) -> QualityCheckResult:
    query = select(ParcelCharacteristic)
    if parcel_filter:
        query = query.where(ParcelCharacteristic.parcel_id.in_(parcel_filter))

    issues: list[QualityIssue] = []
    for row in session.execute(query).scalars():
        classification = (row.current_valuation_classification or "").upper()
        has_x_classification = classification.startswith("X")
        has_empty_section_signal = (
            row.has_empty_valuation_breakout is True
            and row.current_tax_info_available is False
        )
        if has_x_classification and row.is_exempt_style_page is not True:
            issues.append(
                QualityIssue(
                    code="missing_exempt_style_flag",
                    message=(
                        "Parcel characteristics missed an exempt-style flag for "
                        "an X-classified parcel."
                    ),
                    parcel_id=row.parcel_id,
                    fetch_id=row.source_fetch_id,
                    details={
                        "current_valuation_classification": (
                            row.current_valuation_classification
                        )
                    },
                )
            )
        if (
            row.is_exempt_style_page is True
            and not has_x_classification
            and not has_empty_section_signal
        ):
            issues.append(
                QualityIssue(
                    code="unsupported_exempt_style_flag",
                    message="Exempt-style flag is set without supporting signals.",
                    parcel_id=row.parcel_id,
                    fetch_id=row.source_fetch_id,
                    details={
                        "current_valuation_classification": (
                            row.current_valuation_classification
                        ),
                        "has_empty_valuation_breakout": (
                            row.has_empty_valuation_breakout
                        ),
                        "current_tax_info_available": row.current_tax_info_available,
                    },
                )
            )

    return QualityCheckResult(
        code="parcel_characteristics_consistency",
        description=(
            "Detect inconsistent exempt-style classification signals in "
            "parcel_characteristics."
        ),
        issues=issues,
    )


def _check_lineage_consistency(
    session: Session,
    parcel_filter: Optional[set[str]],
) -> QualityCheckResult:
    query = select(ParcelLineageLink)
    if parcel_filter:
        query = query.where(ParcelLineageLink.parcel_id.in_(parcel_filter))

    issues: list[QualityIssue] = []
    relationship_types_by_pair: dict[tuple[str, str], set[str]] = defaultdict(set)
    fetch_ids_by_pair: dict[tuple[str, str], set[int]] = defaultdict(set)
    for row in session.execute(query).scalars():
        pair = (row.parcel_id, row.related_parcel_id)
        relationship_types_by_pair[pair].add(row.relationship_type)
        if row.source_fetch_id is not None:
            fetch_ids_by_pair[pair].add(row.source_fetch_id)

        if row.relationship_type not in VALID_LINEAGE_RELATIONSHIP_TYPES:
            issues.append(
                QualityIssue(
                    code="invalid_lineage_relationship_type",
                    message="Lineage link has an unexpected relationship type.",
                    parcel_id=row.parcel_id,
                    fetch_id=row.source_fetch_id,
                    details={"relationship_type": row.relationship_type},
                )
            )
        if row.parcel_id == row.related_parcel_id:
            issues.append(
                QualityIssue(
                    code="self_referential_lineage_link",
                    message="Lineage link points a parcel at itself.",
                    parcel_id=row.parcel_id,
                    fetch_id=row.source_fetch_id,
                    details={"relationship_type": row.relationship_type},
                )
            )

    for (
        parcel_id,
        related_parcel_id,
    ), relationship_types in relationship_types_by_pair.items():
        if len(relationship_types) <= 1:
            continue
        issues.append(
            QualityIssue(
                code="conflicting_lineage_relationship",
                message=(
                    "Lineage links contain conflicting relationship directions "
                    "for the same parcel pair."
                ),
                parcel_id=parcel_id,
                details={
                    "related_parcel_id": related_parcel_id,
                    "relationship_types": sorted(relationship_types),
                    "source_fetch_ids": sorted(
                        fetch_ids_by_pair[(parcel_id, related_parcel_id)]
                    ),
                },
            )
        )

    return QualityCheckResult(
        code="lineage_consistency",
        description=(
            "Detect invalid, self-referential, or conflicting "
            "parcel_lineage_links rows."
        ),
        issues=issues,
    )


def _check_payment_history_semantics(
    session: Session,
    parcel_filter: Optional[set[str]],
) -> QualityCheckResult:
    payment_signals_by_fetch = _payment_history_signals_by_fetch_id(
        session, parcel_filter
    )

    issues: list[QualityIssue] = []

    characteristic_query = select(ParcelCharacteristic)
    if parcel_filter:
        characteristic_query = characteristic_query.where(
            ParcelCharacteristic.parcel_id.in_(parcel_filter)
        )
    for characteristic in session.execute(characteristic_query).scalars():
        if characteristic.source_fetch_id is None:
            continue
        signals = payment_signals_by_fetch.get(characteristic.source_fetch_id)
        if signals is None:
            continue
        expected_value: Optional[bool] = None
        if signals.has_non_placeholder_row:
            expected_value = True
        elif signals.has_placeholder_row:
            expected_value = False
        if (
            expected_value is not None
            and characteristic.current_payment_history_available is not expected_value
        ):
            issues.append(
                QualityIssue(
                    code="payment_history_flag_mismatch",
                    message=(
                        "Parcel characteristics payment-history flag does not "
                        "match the stored summary payment rows."
                    ),
                    parcel_id=characteristic.parcel_id,
                    fetch_id=characteristic.source_fetch_id,
                    details={
                        "expected_current_payment_history_available": expected_value,
                        "actual_current_payment_history_available": (
                            characteristic.current_payment_history_available
                        ),
                    },
                )
            )

    fact_query = select(ParcelYearFact)
    if parcel_filter:
        fact_query = fact_query.where(ParcelYearFact.parcel_id.in_(parcel_filter))
    for fact in session.execute(fact_query).scalars():
        if fact.payment_has_placeholder_row is not True:
            continue
        if (
            (fact.payment_event_count or 0) > 0
            or fact.payment_total_amount is not None
            or fact.payment_first_date is not None
            or fact.payment_last_date is not None
        ):
            issues.append(
                QualityIssue(
                    code="placeholder_payment_rollup_conflict",
                    message=(
                        "Parcel year facts mix a placeholder payment flag with "
                        "populated payment rollup values."
                    ),
                    parcel_id=fact.parcel_id,
                    fetch_id=fact.payment_fetch_id,
                    year=fact.year,
                    details={
                        "payment_event_count": fact.payment_event_count,
                        "payment_total_amount": (
                            str(fact.payment_total_amount)
                            if fact.payment_total_amount is not None
                            else None
                        ),
                    },
                )
            )

    return QualityCheckResult(
        code="payment_history_semantics",
        description=(
            "Detect mismatches between placeholder payment rows and the "
            "derived payment-history flags."
        ),
        issues=issues,
    )


def _counts_by_fetch_id(
    session: Session,
    model,
    parcel_filter: Optional[set[str]],
) -> dict[int, int]:
    query = select(model.fetch_id, func.count(model.id)).group_by(model.fetch_id)
    if parcel_filter:
        query = query.where(model.parcel_id.in_(parcel_filter))
    return {
        int(fetch_id): int(row_count)
        for fetch_id, row_count in session.execute(query).all()
        if fetch_id is not None
    }


def _summary_fetch_ids(
    session: Session,
    parcel_filter: Optional[set[str]],
) -> set[int]:
    query = select(ParcelSummary.fetch_id)
    if parcel_filter:
        query = query.where(ParcelSummary.parcel_id.in_(parcel_filter))
    return set(session.execute(query).scalars())


def _payment_history_signals_by_fetch_id(
    session: Session,
    parcel_filter: Optional[set[str]],
) -> dict[int, _PaymentHistorySignals]:
    query = select(PaymentRecord)
    if parcel_filter:
        query = query.where(PaymentRecord.parcel_id.in_(parcel_filter))

    signals_by_fetch: dict[int, _PaymentHistorySignals] = {}
    for row in session.execute(query).scalars():
        if row.fetch_id is None:
            continue
        if str(row.data.get("source") or "") == "tax_detail_payments":
            continue
        signals = signals_by_fetch.setdefault(row.fetch_id, _PaymentHistorySignals())
        if _payment_row_is_placeholder(row.data):
            signals.has_placeholder_row = True
        else:
            signals.has_non_placeholder_row = True
    return signals_by_fetch


def _payment_row_is_placeholder(data: dict) -> bool:
    return (
        str(data.get("Date of Payment") or data.get("Date Paid") or "").strip()
        == PAYMENT_HISTORY_PLACEHOLDER
    )


def _has_negative_amount(data: dict) -> bool:
    for key, value in data.items():
        if "amount" not in str(key).lower():
            continue
        amount = _parse_amount(str(value))
        if amount is not None and amount < 0:
            return True
    return False


def _parse_amount(value: str) -> Optional[Decimal]:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", value)
    if cleaned in ("", "-", ".", "-."):
        return None
    try:
        return Decimal(cleaned)
    except Exception:
        return None
