from __future__ import annotations

import csv
import hashlib
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, load_only

from .models import Parcel, ParcelCharacteristic, ParcelSummary, PermitEvent

PERMIT_SOURCE_SYSTEM = "manual_permit_csv"
_EXTRA_COLUMNS_KEY = "_extra_columns"
_SPACE_RE = re.compile(r"\s+")
_HEADER_SEPARATOR_RE = re.compile(r"[ _-]+")
_HEADER_SPACE_TAB_RE = re.compile(r"[ \t]+")
_PARCEL_SEPARATOR_RE = re.compile(r"[\s./_-]+")
_ADDRESS_PUNCTUATION_RE = re.compile(r"""[,.:;#@\-\/\\()'"]""")
EXACT_PARCEL_NUMBER_MATCH_METHOD = "exact_parcel_number"
PARCEL_NUMBER_CROSSWALK_MATCH_METHOD = "parcel_number_crosswalk"
NORMALIZED_ADDRESS_MATCH_METHOD = "normalized_address"
EXACT_MATCH_CONFIDENCE = Decimal("1.0000")
PARCEL_NUMBER_CROSSWALK_CONFIDENCE = Decimal("0.9500")
ADDRESS_MATCH_CONFIDENCE = Decimal("0.9000")
_STATUS_MAP = {
    "applied": "applied",
    "application received": "applied",
    "issued": "issued",
    "approved": "issued",
    "final": "finaled",
    "finaled": "finaled",
    "closed": "finaled",
    "completed": "finaled",
    "expired": "expired",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "void": "cancelled",
}

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "permit_number": ("Permit Number", "Permit #", "Permit ID"),
    "issuing_jurisdiction": (
        "Issuing Jurisdiction",
        "Jurisdiction",
        "Municipality",
    ),
    "permit_type": ("Permit Type",),
    "permit_subtype": ("Permit Subtype", "Subtype"),
    "work_class": ("Work Class", "Project Type"),
    "permit_status_raw": ("Permit Status", "Status"),
    "description": ("Description", "Work Description"),
    "owner_name": ("Owner Name", "Owner"),
    "contractor_name": ("Contractor Name", "Contractor"),
    "parcel_number_raw": ("Parcel Number", "Parcel #", "PIN", "Tax Key"),
    "site_address_raw": ("Address", "Site Address", "Property Address"),
    "applied_date": ("Applied Date", "Application Date", "Filed Date"),
    "issued_date": ("Issued Date", "Issue Date"),
    "finaled_date": ("Finaled Date", "Final Date", "Closed Date"),
    "status_date": ("Status Date", "Last Status Date"),
    "permit_year": ("Permit Year", "Year"),
    "declared_valuation": ("Declared Valuation", "Project Valuation"),
    "estimated_cost": ("Estimated Cost", "Project Cost"),
}


class PermitImportFileError(ValueError):
    pass


@dataclass(frozen=True)
class PermitImportSummary:
    total_rows: int
    loaded_rows: int
    rejected_rows: int
    inserted_rows: int
    updated_rows: int


def ingest_permits_csv(session: Session, csv_path: Path) -> PermitImportSummary:
    file_sha256 = _hash_file(csv_path)
    existing_rows = {
        row.source_row_number: row
        for row in session.execute(
            select(PermitEvent)
            .options(load_only(PermitEvent.id, PermitEvent.source_row_number))
            .where(PermitEvent.source_file_sha256 == file_sha256)
        ).scalars()
    }

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            try:
                source_headers = next(reader)
            except StopIteration as exc:
                raise PermitImportFileError(
                    "CSV file is missing a header row."
                ) from exc
            if not source_headers:
                raise PermitImportFileError("CSV file is missing a header row.")

            _validate_headers(source_headers)
            normalized_header_to_raw = _build_normalized_header_map(source_headers)
            header_binding = _resolve_header_binding(normalized_header_to_raw)
            _validate_permit_shape_headers(header_binding)
            parcel_number_index, address_index = _build_permit_parcel_link_indexes(
                session
            )

            total_rows = 0
            loaded_rows = 0
            rejected_rows = 0
            inserted_rows = 0
            updated_rows = 0

            for source_row_number, row_values in enumerate(reader, start=1):
                total_rows += 1
                row = _build_row_mapping(source_headers, row_values)
                values = _build_permit_event_values(
                    row=row,
                    source_file_name=csv_path.name,
                    source_file_sha256=file_sha256,
                    source_row_number=source_row_number,
                    source_headers=source_headers,
                    header_binding=header_binding,
                    parcel_number_index=parcel_number_index,
                    address_index=address_index,
                )

                existing = existing_rows.get(source_row_number)
                if existing is None:
                    event = PermitEvent(**values)
                    session.add(event)
                    inserted_rows += 1
                else:
                    _apply_permit_event_update(existing, values)
                    updated_rows += 1

                if values["import_status"] == "loaded":
                    loaded_rows += 1
                else:
                    rejected_rows += 1
    except OSError as exc:
        raise PermitImportFileError(f"Could not read CSV file: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise PermitImportFileError(
            "CSV file is not valid UTF-8 and could not be decoded."
        ) from exc
    except csv.Error as exc:
        raise PermitImportFileError(f"CSV file is malformed: {exc}") from exc

    return PermitImportSummary(
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
        raise PermitImportFileError(f"Could not read CSV file: {exc}") from exc
    return hasher.hexdigest()


def _build_row_mapping(
    source_headers: list[str],
    row_values: list[str],
) -> dict[str, Optional[str] | list[str]]:
    row: dict[str, Optional[str] | list[str]] = {}
    for index, header in enumerate(source_headers):
        row[header] = row_values[index] if index < len(row_values) else None
    row[_EXTRA_COLUMNS_KEY] = row_values[len(source_headers) :]
    return row


def _validate_headers(source_headers: list[str]) -> None:
    normalized_headers = [_normalize_header_name(header) for header in source_headers]
    if any(not header for header in normalized_headers):
        raise PermitImportFileError("CSV file contains blank header names.")

    normalized_counts = Counter(normalized_headers)
    duplicate_headers = sorted(
        header for header, count in normalized_counts.items() if count > 1
    )
    if duplicate_headers:
        duplicate_text = ", ".join(duplicate_headers)
        raise PermitImportFileError(
            "CSV file contains duplicate headers after normalization: "
            f"{duplicate_text}"
        )


def _build_normalized_header_map(source_headers: list[str]) -> dict[str, str]:
    normalized_header_to_raw: dict[str, str] = {}
    for header in source_headers:
        normalized_header_to_raw[_normalize_header_name(header)] = header
    return normalized_header_to_raw


def _normalize_header_name(value: str) -> str:
    normalized = value.strip()
    if normalized.endswith(":") or normalized.endswith("#"):
        normalized = normalized[:-1]
    normalized = normalized.lower()
    normalized = _HEADER_SEPARATOR_RE.sub(" ", normalized)
    return _HEADER_SPACE_TAB_RE.sub(" ", normalized).strip()


def _resolve_header_binding(
    normalized_header_to_raw: dict[str, str],
) -> dict[str, Optional[str]]:
    binding: dict[str, Optional[str]] = {}

    for canonical_name, aliases in _HEADER_ALIASES.items():
        chosen_header: Optional[str] = None
        for alias in aliases:
            raw_header = normalized_header_to_raw.get(_normalize_header_name(alias))
            if raw_header is not None:
                chosen_header = raw_header
                break
        binding[canonical_name] = chosen_header
    return binding


def _validate_permit_shape_headers(header_binding: dict[str, Optional[str]]) -> None:
    has_locator_header = bool(
        header_binding["parcel_number_raw"] or header_binding["site_address_raw"]
    )
    has_temporal_anchor_header = any(
        header_binding[name]
        for name in (
            "applied_date",
            "issued_date",
            "finaled_date",
            "status_date",
            "permit_year",
        )
    )
    if has_locator_header and has_temporal_anchor_header:
        return
    raise PermitImportFileError(
        "CSV file does not meet minimum permit-shape requirements: "
        "must include at least one parcel locator header "
        "(Parcel Number or Site/Property Address) and at least one temporal "
        "anchor header (Applied/Issued/Finaled/Status Date or Permit Year)."
    )


def _build_permit_event_values(
    *,
    row: dict[str, Optional[str] | list[str]],
    source_file_name: str,
    source_file_sha256: str,
    source_row_number: int,
    source_headers: list[str],
    header_binding: dict[str, Optional[str]],
    parcel_number_index: dict[str, set[str]],
    address_index: dict[str, set[str]],
) -> dict[str, object]:
    warnings: list[str] = []
    raw_row = _build_raw_row(row)
    values: dict[str, object] = {
        "source_system": PERMIT_SOURCE_SYSTEM,
        "source_file_name": source_file_name,
        "source_file_sha256": source_file_sha256,
        "source_row_number": source_row_number,
        "source_headers": source_headers,
        "raw_row": raw_row,
    }

    parcel_number_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "parcel_number_raw")
    )
    site_address_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "site_address_raw")
    )
    permit_status_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "permit_status_raw")
    )
    permit_status_norm = _normalize_permit_status(permit_status_raw)

    values.update(
        {
            "parcel_number_raw": parcel_number_raw,
            "parcel_number_norm": _normalize_parcel_number(parcel_number_raw),
            "site_address_raw": site_address_raw,
            "site_address_norm": _normalize_site_address(site_address_raw),
            "permit_number": _collapsed_text(
                _bound_value(row, header_binding, "permit_number")
            ),
            "issuing_jurisdiction": _collapsed_text(
                _bound_value(row, header_binding, "issuing_jurisdiction")
            ),
            "permit_type": _collapsed_text(
                _bound_value(row, header_binding, "permit_type")
            ),
            "permit_subtype": _collapsed_text(
                _bound_value(row, header_binding, "permit_subtype")
            ),
            "work_class": _collapsed_text(
                _bound_value(row, header_binding, "work_class")
            ),
            "permit_status_raw": permit_status_raw,
            "permit_status_norm": permit_status_norm,
            "description": _collapsed_text(
                _bound_value(row, header_binding, "description")
            ),
            "owner_name": _collapsed_text(
                _bound_value(row, header_binding, "owner_name")
            ),
            "contractor_name": _collapsed_text(
                _bound_value(row, header_binding, "contractor_name")
            ),
        }
    )

    applied_date, applied_date_error = _parse_optional_date(
        _bound_value(row, header_binding, "applied_date"), field_name="applied_date"
    )
    issued_date, issued_date_error = _parse_optional_date(
        _bound_value(row, header_binding, "issued_date"), field_name="issued_date"
    )
    finaled_date, finaled_date_error = _parse_optional_date(
        _bound_value(row, header_binding, "finaled_date"), field_name="finaled_date"
    )
    status_date, status_date_error = _parse_optional_date(
        _bound_value(row, header_binding, "status_date"), field_name="status_date"
    )
    explicit_permit_year, permit_year_error = _parse_optional_year(
        _bound_value(row, header_binding, "permit_year")
    )
    declared_valuation, declared_valuation_error = _parse_optional_amount(
        _bound_value(row, header_binding, "declared_valuation"),
        field_name="declared_valuation",
    )
    estimated_cost, estimated_cost_error = _parse_optional_amount(
        _bound_value(row, header_binding, "estimated_cost"),
        field_name="estimated_cost",
    )

    for warning in (
        applied_date_error,
        issued_date_error,
        finaled_date_error,
        status_date_error,
        permit_year_error,
        declared_valuation_error,
        estimated_cost_error,
    ):
        if warning is not None:
            warnings.append(warning)

    derived_permit_year = _derive_permit_year(
        issued_date=issued_date,
        finaled_date=finaled_date,
        status_date=status_date,
        applied_date=applied_date,
    )
    permit_year = (
        explicit_permit_year
        if explicit_permit_year is not None
        else derived_permit_year
    )
    if (
        explicit_permit_year is not None
        and derived_permit_year is not None
        and explicit_permit_year != derived_permit_year
    ):
        warnings.append("permit_year_anchor_mismatch")

    values.update(
        {
            "applied_date": applied_date,
            "issued_date": issued_date,
            "finaled_date": finaled_date,
            "status_date": status_date,
            "permit_year": permit_year,
            "declared_valuation": declared_valuation,
            "estimated_cost": estimated_cost,
        }
    )
    parcel_id, parcel_link_method, parcel_link_confidence = _resolve_permit_parcel_link(
        parcel_number_raw=parcel_number_raw,
        parcel_number_norm=values["parcel_number_norm"],
        site_address_norm=values["site_address_norm"],
        parcel_number_index=parcel_number_index,
        address_index=address_index,
    )
    values.update(
        {
            "parcel_id": parcel_id,
            "parcel_link_method": parcel_link_method,
            "parcel_link_confidence": parcel_link_confidence,
        }
    )

    import_error = _classify_row_error(
        raw_row=raw_row,
        has_locator=bool(parcel_number_raw or site_address_raw),
        has_temporal_anchor=bool(
            applied_date or issued_date or finaled_date or status_date or permit_year
        ),
        had_temporal_parse_error=any(
            error is not None
            for error in (
                applied_date_error,
                issued_date_error,
                finaled_date_error,
                status_date_error,
                permit_year_error,
            )
        ),
    )
    values["import_status"] = "rejected" if import_error else "loaded"
    values["import_error"] = import_error
    values["import_warnings"] = sorted(set(warnings)) or None
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


def _apply_permit_event_update(
    transaction: PermitEvent, values: dict[str, object]
) -> None:
    for key, value in values.items():
        if key in {"loaded_at"}:
            continue
        setattr(transaction, key, value)


def _classify_row_error(
    *,
    raw_row: dict[str, object],
    has_locator: bool,
    has_temporal_anchor: bool,
    had_temporal_parse_error: bool,
) -> Optional[str]:
    if _row_is_blank(raw_row):
        return "Row is blank."
    if not has_locator:
        return "At least one parcel locator is required: parcel number or site address."
    if not has_temporal_anchor:
        if had_temporal_parse_error:
            return "No valid temporal anchor remains after parsing."
        return (
            "At least one temporal anchor is required: applied_date, issued_date, "
            "finaled_date, status_date, or permit_year."
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


def _trimmed_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed


def _collapsed_text(value: Optional[str]) -> Optional[str]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None
    return _SPACE_RE.sub(" ", trimmed)


def _preserved_raw_text(value: Optional[str]) -> Optional[str]:
    if _trimmed_text(value) is None:
        return None
    return value


def _normalize_parcel_number(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None
    return _PARCEL_SEPARATOR_RE.sub("", collapsed.upper())


def _parcel_number_crosswalk_candidates(value: Optional[str]) -> tuple[str, ...]:
    collapsed = _collapsed_text(value)
    if collapsed is None or "/" not in collapsed:
        return ()

    prefix, suffix = collapsed.split("/", 1)
    suffix_digits = "".join(char for char in suffix if char.isdigit())
    if len(suffix_digits) != 12:
        return ()

    swapped = prefix + suffix_digits[:2] + suffix_digits[3] + suffix_digits[2]
    swapped += suffix_digits[4:]

    normalized = _normalize_parcel_number(f"{prefix}/{swapped[len(prefix):]}")
    if normalized is None:
        return ()
    return (normalized,)


def _normalize_site_address(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    normalized = value.upper()
    normalized = _ADDRESS_PUNCTUATION_RE.sub(" ", normalized)
    normalized = normalized.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    normalized = _SPACE_RE.sub(" ", normalized).strip()
    return normalized or None


def _build_permit_parcel_link_indexes(
    session: Session,
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    parcel_number_index: dict[str, set[str]] = defaultdict(set)
    address_index: dict[str, set[str]] = defaultdict(set)

    for parcel_id in session.execute(select(Parcel.id)).scalars():
        normalized_parcel_id = _normalize_parcel_number(parcel_id)
        if normalized_parcel_id is not None:
            parcel_number_index[normalized_parcel_id].add(parcel_id)

    for row in session.execute(
        select(
            ParcelCharacteristic.parcel_id,
            ParcelCharacteristic.formatted_parcel_number,
        ).where(ParcelCharacteristic.formatted_parcel_number.is_not(None))
    ):
        normalized = _normalize_parcel_number(row.formatted_parcel_number)
        if normalized is not None:
            parcel_number_index[normalized].add(row.parcel_id)

    for row in session.execute(
        select(ParcelSummary.parcel_id, ParcelSummary.primary_address).where(
            ParcelSummary.primary_address.is_not(None)
        )
    ):
        normalized = _normalize_site_address(row.primary_address)
        if normalized is not None:
            address_index[normalized].add(row.parcel_id)

    return dict(parcel_number_index), dict(address_index)


def _resolve_permit_parcel_link(
    *,
    parcel_number_raw: Optional[str],
    parcel_number_norm: object,
    site_address_norm: object,
    parcel_number_index: dict[str, set[str]],
    address_index: dict[str, set[str]],
) -> tuple[Optional[str], Optional[str], Optional[Decimal]]:
    normalized_parcel = (
        parcel_number_norm if isinstance(parcel_number_norm, str) else None
    )
    normalized_address = (
        site_address_norm if isinstance(site_address_norm, str) else None
    )

    if normalized_parcel:
        direct_matches = parcel_number_index.get(normalized_parcel, set())
        if len(direct_matches) == 1:
            return (
                next(iter(direct_matches)),
                EXACT_PARCEL_NUMBER_MATCH_METHOD,
                EXACT_MATCH_CONFIDENCE,
            )

        crosswalk_matches: set[str] = set()
        for candidate in _parcel_number_crosswalk_candidates(parcel_number_raw):
            crosswalk_matches.update(parcel_number_index.get(candidate, set()))
        if len(crosswalk_matches) == 1:
            return (
                next(iter(crosswalk_matches)),
                PARCEL_NUMBER_CROSSWALK_MATCH_METHOD,
                PARCEL_NUMBER_CROSSWALK_CONFIDENCE,
            )

    if normalized_address:
        address_matches = address_index.get(normalized_address, set())
        if len(address_matches) == 1:
            return (
                next(iter(address_matches)),
                NORMALIZED_ADDRESS_MATCH_METHOD,
                ADDRESS_MATCH_CONFIDENCE,
            )

    return None, None, None


def _normalize_permit_status(value: Optional[str]) -> Optional[str]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None
    return _STATUS_MAP.get(trimmed.lower(), "unknown")


def _derive_permit_year(
    *,
    issued_date: Optional[date],
    finaled_date: Optional[date],
    status_date: Optional[date],
    applied_date: Optional[date],
) -> Optional[int]:
    for anchor in (issued_date, finaled_date, status_date, applied_date):
        if anchor is not None:
            return anchor.year
    return None


def _parse_optional_date(
    value: Optional[str], *, field_name: str
) -> tuple[Optional[date], Optional[str]]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None, None
    parsed = _parse_date(trimmed)
    if parsed is None:
        return None, f"{field_name}_unparseable"
    return parsed, None


def _parse_date(value: str) -> Optional[date]:
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_optional_year(value: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None, None

    try:
        parsed = int(trimmed)
    except ValueError:
        return None, "permit_year_unparseable"

    if parsed < 1000 or parsed > 9999:
        return None, "permit_year_unparseable"
    return parsed, None


def _parse_optional_amount(
    value: Optional[str],
    *,
    field_name: str,
) -> tuple[Optional[Decimal], Optional[str]]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None, None

    normalized = trimmed.replace("$", "").replace(",", "")
    try:
        amount = Decimal(normalized).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None, f"{field_name}_unparseable"
    return amount, None
