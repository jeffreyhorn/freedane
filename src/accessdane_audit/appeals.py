from __future__ import annotations

import csv
import hashlib
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, load_only

from .date_parsing import parse_flexible_date
from .models import AppealEvent

APPEAL_SOURCE_SYSTEM = "manual_appeal_csv"
_EXTRA_COLUMNS_KEY = "_extra_columns"
_SPACE_RE = re.compile(r"\s+")
_HEADER_SEPARATOR_RE = re.compile(r"[ _-]+")
_HEADER_SPACE_TAB_RE = re.compile(r"[ \t]+")
_PARCEL_SEPARATOR_RE = re.compile(r"[\s./_-]+")
_ADDRESS_PUNCTUATION_RE = re.compile(r"""[,.:;#@\-\/\\()'"]""")
_YEAR_TOKEN_RE = re.compile(r"(?<!\d)(\d{4})(?!\d)")
_NULL_TOKENS = {
    "n/a",
    "na",
    "n\\a",
    "none",
    "null",
    "not available",
    "not applicable",
    "tbd",
}
_REDUCTION_OUTCOME_VALUES = {"reduction_granted", "partial_reduction"}

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "appeal_number": ("Appeal Number", "Appeal #", "Case Number"),
    "docket_number": ("Docket Number", "Docket", "BOR Docket"),
    "appeal_level_raw": (
        "Appeal Level",
        "Appeal Stage",
        "Board Type",
    ),
    "appeal_reason_raw": (
        "Appeal Reason",
        "Reason",
        "Basis",
    ),
    "outcome_raw": (
        "Outcome",
        "Decision",
        "Board Decision",
    ),
    "filing_date": ("Filing Date", "Filed Date", "Appeal Date"),
    "hearing_date": ("Hearing Date", "Board Hearing Date"),
    "decision_date": ("Decision Date", "Board Decision Date"),
    "tax_year": ("Tax Year", "Assessment Year", "Year"),
    "assessed_value_before": (
        "Assessed Value Before",
        "Current Assessed Value",
        "Assessed Value",
    ),
    "requested_assessed_value": (
        "Requested Assessed Value",
        "Requested Value",
        "Claimed Value",
    ),
    "decided_assessed_value": (
        "Decided Assessed Value",
        "Final Value",
        "Board Value",
    ),
    "parcel_number_raw": ("Parcel Number", "Parcel #", "PIN", "Tax Key"),
    "site_address_raw": ("Address", "Site Address", "Property Address"),
    "owner_name_raw": ("Owner Name", "Owner"),
    "representative_name": ("Representative Name", "Agent"),
    "notes": ("Notes", "Comments"),
}


class AppealImportFileError(ValueError):
    pass


@dataclass(frozen=True)
class AppealImportSummary:
    total_rows: int
    loaded_rows: int
    rejected_rows: int
    inserted_rows: int
    updated_rows: int
    rejection_reason_counts: dict[str, int]
    warning_counts: dict[str, int]


def ingest_appeals_csv(session: Session, csv_path: Path) -> AppealImportSummary:
    file_sha256 = _hash_file(csv_path)
    existing_rows = {
        row.source_row_number: row
        for row in session.execute(
            select(AppealEvent)
            .options(load_only(AppealEvent.id, AppealEvent.source_row_number))
            .where(AppealEvent.source_file_sha256 == file_sha256)
        ).scalars()
    }

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            try:
                source_headers = next(reader)
            except StopIteration as exc:
                raise AppealImportFileError(
                    "CSV file is missing a header row."
                ) from exc
            if not source_headers:
                raise AppealImportFileError("CSV file is missing a header row.")

            _validate_headers(source_headers)
            normalized_header_to_raw = _build_normalized_header_map(source_headers)
            header_binding = _resolve_header_binding(normalized_header_to_raw)
            _validate_appeal_shape_headers(header_binding)

            total_rows = 0
            loaded_rows = 0
            rejected_rows = 0
            inserted_rows = 0
            updated_rows = 0
            rejection_reason_counts: Counter[str] = Counter()
            warning_counts: Counter[str] = Counter()

            for source_row_number, row_values in enumerate(reader, start=1):
                total_rows += 1
                row = _build_row_mapping(source_headers, row_values)
                values = _build_appeal_event_values(
                    row=row,
                    source_file_name=csv_path.name,
                    source_file_sha256=file_sha256,
                    source_row_number=source_row_number,
                    source_column_count=len(row_values),
                    source_headers=source_headers,
                    header_binding=header_binding,
                )

                existing = existing_rows.get(source_row_number)
                if existing is None:
                    event = AppealEvent(**values)
                    session.add(event)
                    inserted_rows += 1
                else:
                    _apply_appeal_event_update(existing, values)
                    updated_rows += 1

                if values["import_status"] == "loaded":
                    loaded_rows += 1
                else:
                    rejected_rows += 1
                    import_error = values.get("import_error")
                    if isinstance(import_error, str):
                        rejection_reason_counts[import_error] += 1

                import_warnings = values.get("import_warnings")
                if isinstance(import_warnings, list):
                    warning_counts.update(
                        warning
                        for warning in import_warnings
                        if isinstance(warning, str) and warning
                    )
    except OSError as exc:
        raise AppealImportFileError(f"Could not read CSV file: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise AppealImportFileError(
            "CSV file is not valid UTF-8 and could not be decoded."
        ) from exc
    except csv.Error as exc:
        raise AppealImportFileError(f"CSV file is malformed: {exc}") from exc

    return AppealImportSummary(
        total_rows=total_rows,
        loaded_rows=loaded_rows,
        rejected_rows=rejected_rows,
        inserted_rows=inserted_rows,
        updated_rows=updated_rows,
        rejection_reason_counts=dict(sorted(rejection_reason_counts.items())),
        warning_counts=dict(sorted(warning_counts.items())),
    )


def _hash_file(csv_path: Path) -> str:
    try:
        hasher = hashlib.sha256()
        with csv_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8192), b""):
                hasher.update(chunk)
    except OSError as exc:
        raise AppealImportFileError(f"Could not read CSV file: {exc}") from exc
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
        raise AppealImportFileError("CSV file contains blank header names.")

    normalized_counts = Counter(normalized_headers)
    duplicate_headers = sorted(
        header for header, count in normalized_counts.items() if count > 1
    )
    if duplicate_headers:
        duplicate_text = ", ".join(duplicate_headers)
        raise AppealImportFileError(
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


def _validate_appeal_shape_headers(header_binding: dict[str, Optional[str]]) -> None:
    has_locator_header = bool(
        header_binding["parcel_number_raw"] or header_binding["site_address_raw"]
    )
    has_temporal_anchor_header = any(
        header_binding[name]
        for name in (
            "filing_date",
            "hearing_date",
            "decision_date",
            "tax_year",
        )
    )
    has_appeal_signal_header = any(
        header_binding[name]
        for name in (
            "appeal_number",
            "docket_number",
            "outcome_raw",
            "requested_assessed_value",
            "decided_assessed_value",
        )
    )
    if has_locator_header and has_temporal_anchor_header and has_appeal_signal_header:
        return

    raise AppealImportFileError(
        "CSV file does not meet minimum appeal-shape requirements: "
        "must include at least one parcel locator header "
        "(Parcel Number or Site/Property Address), at least one temporal "
        "anchor header (Filing/Hearing/Decision Date or Tax Year), "
        "and at least one appeal signal header "
        "(Appeal Number, Docket Number, Outcome, Requested Value, or Decided Value)."
    )


def _build_appeal_event_values(
    *,
    row: dict[str, Optional[str] | list[str]],
    source_file_name: str,
    source_file_sha256: str,
    source_row_number: int,
    source_column_count: int,
    source_headers: list[str],
    header_binding: dict[str, Optional[str]],
) -> dict[str, object]:
    warnings: list[str] = []
    raw_row = _build_raw_row(row)
    values: dict[str, object] = {
        "source_system": APPEAL_SOURCE_SYSTEM,
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
    owner_name_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "owner_name_raw")
    )
    appeal_level_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "appeal_level_raw")
    )
    appeal_reason_raw = _preserved_raw_text(
        _bound_value(row, header_binding, "appeal_reason_raw")
    )
    outcome_raw = _preserved_raw_text(_bound_value(row, header_binding, "outcome_raw"))

    values.update(
        {
            "parcel_number_raw": parcel_number_raw,
            "parcel_number_norm": _normalize_parcel_number(parcel_number_raw),
            "site_address_raw": site_address_raw,
            "site_address_norm": _normalize_site_address(site_address_raw),
            "owner_name_raw": owner_name_raw,
            "owner_name_norm": _normalize_owner_name(owner_name_raw),
            "parcel_id": None,
            "parcel_link_method": None,
            "parcel_link_confidence": None,
            "appeal_number": _collapsed_text(
                _bound_value(row, header_binding, "appeal_number")
            ),
            "docket_number": _collapsed_text(
                _bound_value(row, header_binding, "docket_number")
            ),
            "appeal_level_raw": appeal_level_raw,
            "appeal_level_norm": _normalize_appeal_level(appeal_level_raw),
            "appeal_reason_raw": appeal_reason_raw,
            "appeal_reason_norm": _normalize_appeal_reason(appeal_reason_raw),
            "outcome_raw": outcome_raw,
            "outcome_norm": _normalize_outcome(outcome_raw),
            "representative_name": _collapsed_text(
                _bound_value(row, header_binding, "representative_name")
            ),
            "notes": _collapsed_text(_bound_value(row, header_binding, "notes")),
        }
    )

    filing_date, filing_date_error = _parse_optional_date(
        _bound_value(row, header_binding, "filing_date"), field_name="filing_date"
    )
    hearing_date, hearing_date_error = _parse_optional_date(
        _bound_value(row, header_binding, "hearing_date"), field_name="hearing_date"
    )
    decision_date, decision_date_error = _parse_optional_date(
        _bound_value(row, header_binding, "decision_date"), field_name="decision_date"
    )
    explicit_tax_year, tax_year_error = _parse_optional_year(
        _bound_value(row, header_binding, "tax_year")
    )
    assessed_value_before, assessed_value_before_error = _parse_optional_amount(
        _bound_value(row, header_binding, "assessed_value_before"),
        field_name="assessed_value_before",
    )
    requested_assessed_value, requested_assessed_value_error = _parse_optional_amount(
        _bound_value(row, header_binding, "requested_assessed_value"),
        field_name="requested_assessed_value",
    )
    decided_assessed_value, decided_assessed_value_error = _parse_optional_amount(
        _bound_value(row, header_binding, "decided_assessed_value"),
        field_name="decided_assessed_value",
    )

    for warning in (
        filing_date_error,
        hearing_date_error,
        decision_date_error,
        tax_year_error,
        assessed_value_before_error,
        requested_assessed_value_error,
        decided_assessed_value_error,
    ):
        if warning is not None:
            warnings.append(warning)

    warnings.extend(
        _row_shape_warnings(
            source_column_count=source_column_count,
            source_header_count=len(source_headers),
        )
    )

    derived_tax_year = _derive_tax_year(
        decision_date=decision_date,
        hearing_date=hearing_date,
        filing_date=filing_date,
    )
    tax_year = explicit_tax_year if explicit_tax_year is not None else derived_tax_year
    if (
        explicit_tax_year is not None
        and derived_tax_year is not None
        and explicit_tax_year != derived_tax_year
    ):
        warnings.append("tax_year_anchor_mismatch")

    value_change_amount: Optional[Decimal] = None
    if assessed_value_before is not None and decided_assessed_value is not None:
        value_change_amount = (decided_assessed_value - assessed_value_before).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )

    outcome_norm = values.get("outcome_norm")
    if (
        value_change_amount is not None
        and value_change_amount > Decimal("0.00")
        and isinstance(outcome_norm, str)
        and outcome_norm in _REDUCTION_OUTCOME_VALUES
    ):
        warnings.append("outcome_value_direction_mismatch")

    values.update(
        {
            "filing_date": filing_date,
            "hearing_date": hearing_date,
            "decision_date": decision_date,
            "tax_year": tax_year,
            "assessed_value_before": assessed_value_before,
            "requested_assessed_value": requested_assessed_value,
            "decided_assessed_value": decided_assessed_value,
            "value_change_amount": value_change_amount,
        }
    )

    import_error = _classify_row_error(
        raw_row=raw_row,
        has_locator=bool(parcel_number_raw or site_address_raw),
        has_temporal_anchor=bool(
            filing_date or hearing_date or decision_date or tax_year
        ),
        had_temporal_parse_error=any(
            error is not None
            for error in (
                filing_date_error,
                hearing_date_error,
                decision_date_error,
                tax_year_error,
            )
        ),
        has_appeal_signal=bool(
            values["appeal_number"]
            or values["docket_number"]
            or outcome_raw
            or requested_assessed_value is not None
            or decided_assessed_value is not None
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


def _apply_appeal_event_update(event: AppealEvent, values: dict[str, object]) -> None:
    for key, value in values.items():
        if key in {"loaded_at"}:
            continue
        setattr(event, key, value)


def _classify_row_error(
    *,
    raw_row: dict[str, object],
    has_locator: bool,
    has_temporal_anchor: bool,
    had_temporal_parse_error: bool,
    has_appeal_signal: bool,
) -> Optional[str]:
    if _row_is_blank(raw_row):
        return "Row is blank."
    if not has_locator:
        return "At least one parcel locator is required: parcel number or site address."
    if not has_temporal_anchor:
        if had_temporal_parse_error:
            return "No valid temporal anchor remains after parsing."
        return (
            "At least one temporal anchor is required: filing_date, hearing_date, "
            "decision_date, or tax_year."
        )
    if not has_appeal_signal:
        return (
            "At least one appeal signal is required: appeal_number, docket_number, "
            "outcome_raw, requested_assessed_value, or decided_assessed_value."
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
    if trimmed.lower() in _NULL_TOKENS:
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


def _normalize_site_address(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    normalized = value.upper()
    normalized = _ADDRESS_PUNCTUATION_RE.sub(" ", normalized)
    normalized = normalized.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    normalized = _SPACE_RE.sub(" ", normalized).strip()
    return normalized or None


def _normalize_owner_name(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None
    return collapsed.upper()


def _normalize_outcome(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None

    normalized = collapsed.lower()
    if normalized in {"granted", "approved"} or "reduction granted" in normalized:
        return "reduction_granted"
    if (
        normalized in {"denied", "upheld"}
        or "denied" in normalized
        or "denial" in normalized
        or "no change" in normalized
    ):
        return "denied"
    if (
        normalized == "partial"
        or "partial reduction" in normalized
        or "partially granted" in normalized
        or "modified" in normalized
    ):
        return "partial_reduction"
    if normalized in {"withdraw", "withdrawn"}:
        return "withdrawn"
    if "dismissed" in normalized:
        return "dismissed"
    if normalized in {"pending", "continued", "open"}:
        return "pending"
    return "unknown"


def _normalize_appeal_level(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None

    normalized = collapsed.lower()
    if "open book" in normalized:
        return "open_book"
    if normalized == "bor" or "board of review" in normalized:
        return "board_of_review"
    if "assessor review" in normalized or "informal review" in normalized:
        return "assessor_review"
    if (
        "circuit court" in normalized
        or "tax appeals commission" in normalized
        or "court" in normalized
    ):
        return "court"
    return "unknown"


def _normalize_appeal_reason(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None

    normalized = collapsed.lower()
    if "value" in normalized or "valuat" in normalized:
        return "valuation"
    if "class" in normalized:
        return "classification"
    if "exempt" in normalized:
        return "exemption"
    if "clerical" in normalized or "error" in normalized:
        return "clerical_error"
    return "unknown"


def _derive_tax_year(
    *,
    decision_date: Optional[date],
    hearing_date: Optional[date],
    filing_date: Optional[date],
) -> Optional[int]:
    for anchor in (decision_date, hearing_date, filing_date):
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
    return parse_flexible_date(value)


def _parse_optional_year(value: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    trimmed = _trimmed_text(value)
    if trimmed is None:
        return None, None

    try:
        parsed = int(trimmed)
    except ValueError:
        candidate_values = {
            parsed
            for token in _YEAR_TOKEN_RE.findall(trimmed)
            if 1000 <= (parsed := int(token)) <= 9999
        }
        if len(candidate_values) != 1:
            return None, "tax_year_unparseable"
        parsed = candidate_values.pop()

    if parsed < 1000 or parsed > 9999:
        return None, "tax_year_unparseable"
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
    if normalized.startswith("(") and normalized.endswith(")"):
        inner = normalized[1:-1].strip()
        inner = inner.replace(" ", "")
        normalized = f"-{inner}"
    normalized = normalized.strip().replace(" ", "")

    try:
        amount = Decimal(normalized).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None, f"{field_name}_unparseable"
    return amount, None


def _row_shape_warnings(
    *,
    source_column_count: int,
    source_header_count: int,
) -> list[str]:
    warnings: list[str] = []
    if source_column_count < source_header_count:
        warnings.append("row_shorter_than_header")
    if source_column_count > source_header_count:
        warnings.append("row_has_extra_columns")
    return warnings
