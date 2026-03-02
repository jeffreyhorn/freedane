from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import AssessmentRecord, PaymentRecord, TaxRecord


@dataclass
class Anomaly:
    parcel_id: str
    code: str
    message: str


def detect_anomalies(session: Session, parcel_ids: Optional[Iterable[str]] = None) -> list[Anomaly]:
    anomalies: list[Anomaly] = []

    parcel_filter = None
    if parcel_ids:
        parcel_filter = set(parcel_ids)

    assessments = _group_records(session, AssessmentRecord, parcel_filter)
    taxes = _group_records(session, TaxRecord, parcel_filter)
    payments = _group_records(session, PaymentRecord, parcel_filter)

    all_parcels = set(assessments) | set(taxes) | set(payments)
    for parcel_id in sorted(all_parcels):
        if not assessments.get(parcel_id):
            anomalies.append(
                Anomaly(parcel_id, "missing_assessment", "No assessment records parsed.")
            )
        if not taxes.get(parcel_id):
            anomalies.append(
                Anomaly(parcel_id, "missing_tax", "No tax records parsed.")
            )
        if not payments.get(parcel_id):
            anomalies.append(
                Anomaly(parcel_id, "missing_payments", "No payment records parsed.")
            )

        for record in taxes.get(parcel_id, []):
            if _has_negative_amount(record.data):
                anomalies.append(
                    Anomaly(parcel_id, "negative_tax", "Negative tax amount detected.")
                )
                break
        for record in payments.get(parcel_id, []):
            if _has_negative_amount(record.data):
                anomalies.append(
                    Anomaly(parcel_id, "negative_payment", "Negative payment detected.")
                )
                break

    return anomalies


def _group_records(
    session: Session,
    model,
    parcel_filter: Optional[set[str]],
) -> dict[str, list]:
    query = select(model)
    if parcel_filter:
        query = query.where(model.parcel_id.in_(parcel_filter))
    results = session.execute(query).scalars().all()
    grouped: dict[str, list] = {}
    for record in results:
        grouped.setdefault(record.parcel_id, []).append(record)
    return grouped


def _has_negative_amount(data: dict) -> bool:
    for key, value in data.items():
        if "amount" not in key.lower():
            continue
        amount = _parse_amount(str(value))
        if amount is not None and amount < 0:
            return True
    return False


def _parse_amount(value: str) -> Optional[float]:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", value)
    if cleaned in ("", "-", ".", "-."):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None
