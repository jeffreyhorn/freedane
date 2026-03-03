from __future__ import annotations

from typing import Mapping, Optional

LINEAGE_PARENT_RELATIONSHIP = "parent"
LINEAGE_CHILD_RELATIONSHIP = "child"
VALID_LINEAGE_RELATIONSHIP_TYPES = frozenset(
    (LINEAGE_PARENT_RELATIONSHIP, LINEAGE_CHILD_RELATIONSHIP)
)
LINEAGE_MODAL_BY_RELATIONSHIP_TYPE = (
    (LINEAGE_PARENT_RELATIONSHIP, "modalParcelHistoryParents"),
    (LINEAGE_CHILD_RELATIONSHIP, "modalParcelHistoryChildren"),
)
PAYMENT_HISTORY_PLACEHOLDER = "No payments found."


def clean_optional_text(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value or value.upper() in {"N/A", "NA"}:
        return None
    return value


def is_placeholder_payment_row(row: Mapping[str, object]) -> bool:
    date_value = clean_optional_text(
        row.get("Date of Payment") or row.get("Date Paid") or row.get("col_2")
    )
    return date_value == PAYMENT_HISTORY_PLACEHOLDER
