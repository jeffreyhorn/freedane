from __future__ import annotations

import csv
import hashlib
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, load_only

from .models import SalesTransaction

RETR_SOURCE_SYSTEM = "wisconsin_dor_retr"
_EXTRA_COLUMNS_KEY = "_extra_columns"
_SPACE_RE = re.compile(r"\s+")
_PARCEL_SEPARATOR_RE = re.compile(r"[\s./_-]+")
_ADDRESS_PUNCTUATION_RE = re.compile(r"[,.;:/#-]+")
_TRUE_TOKENS = {"y", "yes", "true", "1"}
_FALSE_TOKENS = {"n", "no", "false", "0"}

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "revenue_object_id": (
        "Revenue Object ID",
        "Revenue Object",
        "Receipt Number",
        "Receipt #",
    ),
    "document_number": ("Document Number", "Document #", "Document No"),
    "county_name": ("County", "County Name"),
    "municipality_name": ("Municipality", "Municipality Name"),
    "transfer_date": ("Transfer Date", "Conveyance Date", "Sale Date"),
    "recording_date": ("Recording Date", "Recorded Date"),
    "consideration_amount": ("Consideration", "Sale Price", "Value"),
    "property_type": ("Property Type",),
    "conveyance_type": ("Conveyance Type", "Conveyance"),
    "deed_type": ("Deed Type", "Deed"),
    "grantor_name": ("Grantor", "Seller"),
    "grantee_name": ("Grantee", "Buyer"),
    "official_parcel_number_raw": (
        "Parcel Number",
        "Parcel #",
        "Official Parcel Number",
        "PIN",
    ),
    "property_address_raw": (
        "Property Address",
        "Property Address 1",
        "Address",
        "Site Address",
    ),
    "legal_description_raw": ("Legal Description", "Legal"),
    "school_district_name": ("School District", "School District Name"),
    "arms_length_indicator_raw": (
        "Arms Length",
        "Arms Length Indicator",
        "Arms-Length Indicator",
    ),
    "usable_sale_indicator_raw": (
        "Usable Sale",
        "Usable Sale Indicator",
        "Usable Indicator",
    ),
}


class RetrImportFileError(ValueError):
    pass


@dataclass(frozen=True)
class RetrImportSummary:
    total_rows: int
    loaded_rows: int
    rejected_rows: int
    inserted_rows: int
    updated_rows: int


def ingest_retr_csv(session: Session, csv_path: Path) -> RetrImportSummary:
    file_sha256 = _hash_file(csv_path)
    existing_rows = {
        row.source_row_number: row
        for row in session.execute(
            select(SalesTransaction)
            .options(
                load_only(
                    SalesTransaction.id,
                    SalesTransaction.source_row_number,
                )
            )
            .where(SalesTransaction.source_file_sha256 == file_sha256)
        ).scalars()
    }

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, restkey=_EXTRA_COLUMNS_KEY)
            if reader.fieldnames is None:
                raise RetrImportFileError("CSV file is missing a header row.")

            fieldnames = list(reader.fieldnames)
            _validate_headers(fieldnames)

            header_binding = _resolve_header_binding(fieldnames)
            if not any(header_binding.values()):
                raise RetrImportFileError(
                    "CSV file does not contain recognizable RETR headers."
                )

            total_rows = 0
            loaded_rows = 0
            rejected_rows = 0
            inserted_rows = 0
            updated_rows = 0

            for source_row_number, row in enumerate(reader, start=1):
                total_rows += 1
                values = _build_sales_transaction_values(
                    row=row,
                    source_file_name=csv_path.name,
                    source_file_sha256=file_sha256,
                    source_row_number=source_row_number,
                    source_headers=fieldnames,
                    header_binding=header_binding,
                )

                existing = existing_rows.get(source_row_number)
                if existing is None:
                    session.add(SalesTransaction(**values))
                    inserted_rows += 1
                else:
                    _apply_sales_transaction_update(existing, values)
                    updated_rows += 1

                if values["import_status"] == "loaded":
                    loaded_rows += 1
                else:
                    rejected_rows += 1
    except OSError as exc:
        raise RetrImportFileError(f"Could not read CSV file: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise RetrImportFileError(
            "CSV file is not valid UTF-8 and could not be decoded."
        ) from exc
    except csv.Error as exc:
        raise RetrImportFileError(f"CSV file is malformed: {exc}") from exc

    return RetrImportSummary(
        total_rows=total_rows,
        loaded_rows=loaded_rows,
        rejected_rows=rejected_rows,
        inserted_rows=inserted_rows,
        updated_rows=updated_rows,
    )


def _hash_file(csv_path: Path) -> str:
    try:
        hasher = hashlib.sha256()
        with csv_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8192), b""):
                hasher.update(chunk)
    except OSError as exc:
        raise RetrImportFileError(f"Could not read CSV file: {exc}") from exc
    return hasher.hexdigest()


def _validate_headers(fieldnames: list[str]) -> None:
    if any(not (header or "").strip() for header in fieldnames):
        raise RetrImportFileError("CSV file contains blank header names.")

    header_counts = Counter(fieldnames)
    duplicate_headers = sorted(
        header for header, count in header_counts.items() if count > 1
    )
    if duplicate_headers:
        duplicate_text = ", ".join(duplicate_headers)
        raise RetrImportFileError(
            f"CSV file contains duplicate header names: {duplicate_text}"
        )


def _resolve_header_binding(fieldnames: list[str]) -> dict[str, Optional[str]]:
    fieldname_set = set(fieldnames)
    binding: dict[str, Optional[str]] = {}

    for canonical_name, aliases in _HEADER_ALIASES.items():
        chosen_alias = None
        for alias in aliases:
            if alias in fieldname_set:
                chosen_alias = alias
                break
        binding[canonical_name] = chosen_alias

    return binding


def _build_sales_transaction_values(
    *,
    row: dict[str, Optional[str] | list[str]],
    source_file_name: str,
    source_file_sha256: str,
    source_row_number: int,
    source_headers: list[str],
    header_binding: dict[str, Optional[str]],
) -> dict[str, object]:
    raw_row = _build_raw_row(row)
    values: dict[str, object] = {
        "source_system": RETR_SOURCE_SYSTEM,
        "source_file_name": source_file_name,
        "source_file_sha256": source_file_sha256,
        "source_row_number": source_row_number,
        "source_headers": source_headers,
        "raw_row": raw_row,
        "revenue_object_id": _collapsed_text(
            _bound_value(row, header_binding, "revenue_object_id")
        ),
        "document_number": _collapsed_text(
            _bound_value(row, header_binding, "document_number")
        ),
        "county_name": _collapsed_text(
            _bound_value(row, header_binding, "county_name")
        ),
        "municipality_name": _collapsed_text(
            _bound_value(row, header_binding, "municipality_name")
        ),
        "property_type": _collapsed_text(
            _bound_value(row, header_binding, "property_type")
        ),
        "conveyance_type": _collapsed_text(
            _bound_value(row, header_binding, "conveyance_type")
        ),
        "deed_type": _collapsed_text(_bound_value(row, header_binding, "deed_type")),
        "grantor_name": _collapsed_text(
            _bound_value(row, header_binding, "grantor_name")
        ),
        "grantee_name": _collapsed_text(
            _bound_value(row, header_binding, "grantee_name")
        ),
    }

    official_parcel_number_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "official_parcel_number_raw")
    )
    property_address_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "property_address_raw")
    )
    legal_description_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "legal_description_raw")
    )
    arms_length_indicator_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "arms_length_indicator_raw")
    )
    usable_sale_indicator_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "usable_sale_indicator_raw")
    )

    values.update(
        {
            "official_parcel_number_raw": official_parcel_number_raw,
            "official_parcel_number_norm": _normalize_parcel_number(
                official_parcel_number_raw
            ),
            "property_address_raw": property_address_raw,
            "property_address_norm": _normalize_address(property_address_raw),
            "legal_description_raw": legal_description_raw,
            "school_district_name": _collapsed_text(
                _bound_value(row, header_binding, "school_district_name")
            ),
            "arms_length_indicator_raw": arms_length_indicator_raw,
            "arms_length_indicator_norm": _normalize_boolean(arms_length_indicator_raw),
            "usable_sale_indicator_raw": usable_sale_indicator_raw,
            "usable_sale_indicator_norm": _normalize_boolean(usable_sale_indicator_raw),
        }
    )

    transfer_date_raw = _bound_value(row, header_binding, "transfer_date")
    recording_date_raw = _bound_value(row, header_binding, "recording_date")
    consideration_raw = _bound_value(row, header_binding, "consideration_amount")

    transfer_date, transfer_date_error = _parse_required_date(
        transfer_date_raw, field_name="transfer_date"
    )
    recording_date, recording_date_error = _parse_optional_date(
        recording_date_raw,
        field_name="recording_date",
    )
    consideration_amount, consideration_error = _parse_required_amount(
        consideration_raw, field_name="consideration_amount"
    )

    values.update(
        {
            "transfer_date": transfer_date,
            "recording_date": recording_date,
            "consideration_amount": consideration_amount,
        }
    )

    import_error = _classify_row_error(
        raw_row=raw_row,
        transfer_date_error=transfer_date_error,
        recording_date_error=recording_date_error,
        consideration_error=consideration_error,
        has_identifier=any(
            (
                official_parcel_number_raw,
                property_address_raw,
                legal_description_raw,
            )
        ),
    )
    values["import_status"] = "rejected" if import_error else "loaded"
    values["import_error"] = import_error
    return values


def _build_raw_row(row: dict[str, Optional[str] | list[str]]) -> dict[str, object]:
    raw_row: dict[str, object] = {}
    for key, value in row.items():
        if key == _EXTRA_COLUMNS_KEY:
            raw_row[key] = list(value) if isinstance(value, list) else []
            continue
        raw_row[key] = value
    return raw_row


def _bound_value(
    row: dict[str, Optional[str] | list[str]],
    header_binding: dict[str, Optional[str]],
    canonical_name: str,
) -> Optional[str]:
    header_name = header_binding.get(canonical_name)
    if header_name is None:
        return None
    value = row.get(header_name)
    return value if isinstance(value, str) else None


def _classify_row_error(
    *,
    raw_row: dict[str, object],
    transfer_date_error: Optional[str],
    recording_date_error: Optional[str],
    consideration_error: Optional[str],
    has_identifier: bool,
) -> Optional[str]:
    extra_columns = raw_row.get(_EXTRA_COLUMNS_KEY)
    if isinstance(extra_columns, list) and extra_columns:
        return "Row has extra columns beyond the header definition."
    if _row_is_blank(raw_row):
        return "Row is blank."
    if transfer_date_error:
        return transfer_date_error
    if recording_date_error:
        return recording_date_error
    if consideration_error:
        return consideration_error
    if not has_identifier:
        return (
            "At least one identifying input is required: official parcel number, "
            "property address, or legal description."
        )
    return None


def _row_is_blank(raw_row: dict[str, object]) -> bool:
    for key, value in raw_row.items():
        if key == _EXTRA_COLUMNS_KEY:
            if isinstance(value, list) and any(_trimmed_text(item) for item in value):
                return False
            continue
        if isinstance(value, str):
            if _trimmed_text(value):
                return False
            continue
        if value not in (None, ""):
            return False
    return True


def _apply_sales_transaction_update(
    transaction: SalesTransaction,
    values: dict[str, object],
) -> None:
    for key, value in values.items():
        if key in {"source_file_name", "source_headers"}:
            continue
        setattr(transaction, key, value)


def _collapsed_text(value: Optional[str]) -> Optional[str]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None
    return _SPACE_RE.sub(" ", trimmed)


def _preserved_raw_text(value: Optional[str]) -> Optional[str]:
    if _trimmed_text(value) is None:
        return None
    return value


def _trimmed_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed


def _normalize_parcel_number(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None
    return _PARCEL_SEPARATOR_RE.sub("", collapsed.upper())


def _normalize_address(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None
    normalized = _ADDRESS_PUNCTUATION_RE.sub(" ", collapsed.upper())
    return _SPACE_RE.sub(" ", normalized).strip()


def _normalize_boolean(value: Optional[str]) -> Optional[bool]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None
    normalized = trimmed.lower()
    if normalized in _TRUE_TOKENS:
        return True
    if normalized in _FALSE_TOKENS:
        return False
    return None


def _parse_required_date(
    value: Optional[str],
    *,
    field_name: str,
) -> tuple[Optional[date], Optional[str]]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None, f"{field_name} is required."
    parsed = _parse_date(trimmed)
    if parsed is None:
        return None, f"{field_name} could not be parsed."
    return parsed, None


def _parse_optional_date(
    value: Optional[str],
    *,
    field_name: str,
) -> tuple[Optional[date], Optional[str]]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None, None
    parsed = _parse_date(trimmed)
    if parsed is None:
        return None, f"{field_name} could not be parsed."
    return parsed, None


def _parse_date(value: str) -> Optional[date]:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_required_amount(
    value: Optional[str],
    *,
    field_name: str,
) -> tuple[Optional[Decimal], Optional[str]]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None, f"{field_name} is required."
    parsed = _parse_amount(trimmed)
    if parsed is None:
        return None, f"{field_name} could not be parsed."
    return parsed, None


def _parse_amount(value: str) -> Optional[Decimal]:
    normalized = value.replace("$", "").replace(",", "")
    try:
        return Decimal(normalized).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None
