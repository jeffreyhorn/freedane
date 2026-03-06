from __future__ import annotations

from datetime import date, datetime
from typing import Optional

DATE_FORMATS = (
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%y",
    "%m-%d-%y",
)

DATETIME_FORMATS = (
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m-%d-%Y %H:%M",
    "%m-%d-%Y %H:%M:%S",
    "%m/%d/%Y %I:%M %p",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
)


def parse_flexible_date(value: str) -> Optional[date]:
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    iso_candidate = value.strip()
    if iso_candidate.endswith("Z"):
        iso_candidate = f"{iso_candidate[:-1]}+00:00"
    try:
        return datetime.fromisoformat(iso_candidate).date()
    except ValueError:
        return None
