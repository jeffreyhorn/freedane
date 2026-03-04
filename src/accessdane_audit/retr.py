from __future__ import annotations

import csv
import hashlib
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Iterator, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session, load_only

from .models import (
    ParcelCharacteristic,
    ParcelSummary,
    SalesExclusion,
    SalesParcelMatch,
    SalesTransaction,
)

RETR_SOURCE_SYSTEM = "wisconsin_dor_retr"
_EXTRA_COLUMNS_KEY = "_extra_columns"
_SPACE_RE = re.compile(r"\s+")
_PARCEL_SEPARATOR_RE = re.compile(r"[\s./_-]+")
_ADDRESS_PUNCTUATION_RE = re.compile(r"[,.;:/#-]+")
_LEGAL_TEXT_PUNCTUATION_RE = re.compile(r"[^A-Z0-9]+")
_TRUE_TOKENS = {"y", "yes", "true", "1"}
_FALSE_TOKENS = {"n", "no", "false", "0"}
_STREET_SUFFIX_TOKENS = {
    "ALY",
    "AVE",
    "AVENUE",
    "BLVD",
    "CIR",
    "CIRCLE",
    "COURT",
    "CT",
    "DR",
    "DRIVE",
    "HWY",
    "LANE",
    "LN",
    "PKWY",
    "PL",
    "PLACE",
    "RD",
    "ROAD",
    "ST",
    "STREET",
    "TER",
    "TERRACE",
    "TRL",
    "WAY",
}
_ADDRESS_TOKEN_NORMALIZATION = {
    "AVENUE": "AVE",
    "AVNEUE": "AVE",
    "BOULEVARD": "BLVD",
    "CIRCLE": "CIR",
    "COURT": "CT",
    "DRIVE": "DR",
    "EAST": "E",
    "LANE": "LN",
    "NORTH": "N",
    "PLACE": "PL",
    "PARKWAY": "PKWY",
    "ROAD": "RD",
    "SOUTH": "S",
    "STREET": "ST",
    "TERRACE": "TER",
    "TRAIL": "TRL",
    "WEST": "W",
}
_FAMILY_TRANSFER_PHRASES = (
    "family transfer",
    "intra family",
    "intrafamily",
)
_GOVERNMENT_TRANSFER_PHRASES = (
    "government transfer",
    "county of ",
    "city of ",
    "village of ",
    "town of ",
    "state of ",
    "department of ",
    "school district",
    "united states",
)
_CORRECTIVE_DEED_PATTERN = re.compile(
    r"(?<![a-z-])(?:corrective deed|correction deed)\b"
)
_LEGAL_DESCRIPTION_LOT_BLOCK_PATTERN = re.compile(
    r"^\s*LOT\s+"
    r"(?:[A-Z][A-Z0-9 -]*?\((?P<lot_paren>\d+[A-Z]?)\)|(?P<lot>\d+[A-Z]?))"
    r"(?:\s*,\s*BLOCK\s+"
    r"(?:[A-Z][A-Z0-9 -]*?\((?P<block_paren>\d+[A-Z]?)\)|(?P<block>\d+[A-Z]?)))?"
    r"\s*,\s*(?P<subdivision>[^,.;]+)",
    re.IGNORECASE,
)
_PARCEL_DESCRIPTION_BLOCK_LOT_PATTERN = re.compile(
    r"^(?P<subdivision>.+?)\s+BLOCK\s+(?P<block>\d+[A-Z]?)\s+LOT\s+(?P<lot>\d+[A-Z]?)$"
)
_PARCEL_DESCRIPTION_LOT_PATTERN = re.compile(
    r"^(?P<subdivision>.+?)\s+LOT\s+(?P<lot>\d+[A-Z]?)$"
)
_PARCEL_DESCRIPTION_LEADING_LOT_PATTERN = re.compile(
    r"^LOT\s+(?P<lot>\d+[A-Z]?)(?:\s+BLOCK\s+(?P<block>\d+[A-Z]?))?\s+(?P<subdivision>.+)$"
)
MATCHER_VERSION = "sprint3_day12_v1"
EXACT_PARCEL_NUMBER_MATCH_METHOD = "exact_parcel_number"
PARCEL_NUMBER_CROSSWALK_MATCH_METHOD = "parcel_number_crosswalk"
NORMALIZED_ADDRESS_MATCH_METHOD = "normalized_address"
NORMALIZED_LEGAL_DESCRIPTION_MATCH_METHOD = "normalized_legal_description"
AUTO_ACCEPTED_MATCH_REVIEW_STATUS = "auto_accepted"
NEEDS_REVIEW_MATCH_REVIEW_STATUS = "needs_review"
EXACT_MATCH_CONFIDENCE = Decimal("1.0000")
PARCEL_NUMBER_CROSSWALK_CONFIDENCE = Decimal("0.9500")
ADDRESS_MATCH_CONFIDENCE = Decimal("0.9000")
LEGAL_DESCRIPTION_MATCH_CONFIDENCE = Decimal("0.8000")
_MATCH_BATCH_SIZE = 500

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
        "Physical Address",
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


@dataclass(frozen=True)
class SalesExclusionSpec:
    exclusion_code: str
    exclusion_reason: str
    excluded_by_rule: str


@dataclass(frozen=True)
class SalesMatchSummary:
    selected_transactions: int
    matched_transactions: int
    rows_written: int
    rows_deleted: int


@dataclass(frozen=True)
class SalesMatchSpec:
    parcel_id: str
    match_method: str
    confidence_score: Decimal
    match_rank: int
    is_primary: bool
    match_review_status: str
    matched_value: str
    matcher_version: str = MATCHER_VERSION


@dataclass(frozen=True)
class SalesMatchSyncCounts:
    inserted_rows: int
    updated_rows: int
    deleted_rows: int


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
    existing_exclusions = _load_existing_sales_exclusions(
        session,
        existing_rows.values(),
    )

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
                    transaction = SalesTransaction(**values)
                    session.add(transaction)
                    inserted_rows += 1
                    transaction_exclusions: dict[str, SalesExclusion] = {}
                else:
                    transaction = existing
                    _apply_sales_transaction_update(transaction, values)
                    updated_rows += 1
                    transaction_exclusions = existing_exclusions.get(
                        transaction.id,
                        {},
                    )

                _sync_sales_exclusions(
                    session=session,
                    transaction=transaction,
                    values=values,
                    existing_exclusions=transaction_exclusions,
                )

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


def match_sales_transactions(
    session: Session,
    *,
    sales_transaction_ids: Optional[Iterable[int]] = None,
) -> SalesMatchSummary:
    parcel_number_index = _build_parcel_number_match_index(session)
    address_index = _build_address_match_index(session)
    legal_description_index = _build_legal_description_match_index(session)

    selected_transactions = 0
    matched_transactions = 0
    rows_written = 0
    rows_deleted = 0

    for transactions in _iter_sales_transaction_batches(
        session,
        sales_transaction_ids=sales_transaction_ids,
    ):
        selected_transactions += len(transactions)
        existing_matches = _load_existing_sales_parcel_matches(session, transactions)

        for transaction in transactions:
            desired_matches = _derive_sales_parcel_matches(
                transaction,
                parcel_number_index=parcel_number_index,
                address_index=address_index,
                legal_description_index=legal_description_index,
            )
            if desired_matches:
                matched_transactions += 1

            transaction_existing_matches = existing_matches.get(transaction.id, {})
            sync_counts = _sync_sales_parcel_matches(
                session,
                transaction=transaction,
                desired_matches=desired_matches,
                existing_matches=transaction_existing_matches,
            )
            rows_written += sync_counts.inserted_rows + sync_counts.updated_rows
            rows_deleted += sync_counts.deleted_rows

    return SalesMatchSummary(
        selected_transactions=selected_transactions,
        matched_transactions=matched_transactions,
        rows_written=rows_written,
        rows_deleted=rows_deleted,
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


def _load_existing_sales_exclusions(
    session: Session,
    transactions: Iterable[SalesTransaction],
) -> dict[int, dict[str, SalesExclusion]]:
    transaction_ids = [transaction.id for transaction in transactions]
    if not transaction_ids:
        return {}

    exclusions_by_transaction: dict[int, dict[str, SalesExclusion]] = defaultdict(dict)
    for exclusion in session.execute(
        select(SalesExclusion).where(
            SalesExclusion.sales_transaction_id.in_(transaction_ids)
        )
    ).scalars():
        exclusions_by_transaction[exclusion.sales_transaction_id][
            exclusion.exclusion_code
        ] = exclusion

    return dict(exclusions_by_transaction)


def _sync_sales_exclusions(
    *,
    session: Session,
    transaction: SalesTransaction,
    values: dict[str, object],
    existing_exclusions: dict[str, SalesExclusion],
) -> None:
    current_exclusions = {
        spec.exclusion_code: spec for spec in _derive_sales_exclusions(values)
    }

    if transaction.id is None and current_exclusions:
        session.flush([transaction])

    for exclusion_code, exclusion_spec in current_exclusions.items():
        existing = existing_exclusions.get(exclusion_code)
        if existing is None:
            existing = SalesExclusion(
                sales_transaction_id=transaction.id,
                exclusion_code=exclusion_spec.exclusion_code,
                exclusion_reason=exclusion_spec.exclusion_reason,
                excluded_by_rule=exclusion_spec.excluded_by_rule,
            )
            session.add(existing)
            existing_exclusions[exclusion_code] = existing
            continue

        existing.exclusion_reason = exclusion_spec.exclusion_reason
        existing.excluded_by_rule = exclusion_spec.excluded_by_rule
        existing.is_active = True

    for exclusion_code, existing in existing_exclusions.items():
        if exclusion_code in current_exclusions:
            continue
        existing.is_active = False


def _build_parcel_number_match_index(session: Session) -> dict[str, tuple[str, ...]]:
    parcel_ids_by_norm: dict[str, set[str]] = defaultdict(set)
    for parcel_id, formatted_parcel_number in session.execute(
        select(
            ParcelCharacteristic.parcel_id,
            ParcelCharacteristic.formatted_parcel_number,
        )
    ).all():
        normalized = _normalize_parcel_number(formatted_parcel_number)
        if normalized is None:
            continue
        parcel_ids_by_norm[normalized].add(parcel_id)

    return {
        normalized: tuple(sorted(parcel_ids))
        for normalized, parcel_ids in parcel_ids_by_norm.items()
    }


def _build_address_match_index(session: Session) -> dict[str, tuple[str, ...]]:
    parcel_ids_by_norm: dict[str, set[str]] = defaultdict(set)
    for parcel_id, primary_address in session.execute(
        select(ParcelSummary.parcel_id, ParcelSummary.primary_address)
    ).all():
        normalized = _normalize_address(primary_address)
        if normalized is None:
            continue
        parcel_ids_by_norm[normalized].add(parcel_id)

    return {
        normalized: tuple(sorted(parcel_ids))
        for normalized, parcel_ids in parcel_ids_by_norm.items()
    }


def _build_legal_description_match_index(
    session: Session,
) -> dict[str, tuple[str, ...]]:
    parcel_ids_by_norm: dict[str, set[str]] = defaultdict(set)
    for parcel_id, parcel_description in session.execute(
        select(ParcelSummary.parcel_id, ParcelSummary.parcel_description)
    ).all():
        normalized = _normalize_parcel_legal_description(parcel_description)
        if normalized is None:
            continue
        parcel_ids_by_norm[normalized].add(parcel_id)

    return {
        normalized: tuple(sorted(parcel_ids))
        for normalized, parcel_ids in parcel_ids_by_norm.items()
    }


def _load_existing_sales_parcel_matches(
    session: Session,
    transactions: Iterable[SalesTransaction],
) -> dict[int, dict[tuple[str, str], SalesParcelMatch]]:
    transaction_ids = [transaction.id for transaction in transactions]
    if not transaction_ids:
        return {}

    matches_by_transaction: dict[int, dict[tuple[str, str], SalesParcelMatch]] = (
        defaultdict(dict)
    )
    for transaction_id_batch in _chunked(transaction_ids, _MATCH_BATCH_SIZE):
        for row in session.execute(
            select(SalesParcelMatch).where(
                SalesParcelMatch.sales_transaction_id.in_(transaction_id_batch)
            )
        ).scalars():
            key = (row.parcel_id, row.match_method)
            matches_by_transaction[row.sales_transaction_id][key] = row

    return dict(matches_by_transaction)


def _iter_sales_transaction_batches(
    session: Session,
    *,
    sales_transaction_ids: Optional[Iterable[int]],
) -> Iterator[list[SalesTransaction]]:
    base_query = select(SalesTransaction).where(
        SalesTransaction.import_status == "loaded"
    )
    if sales_transaction_ids is not None:
        deduped_ids = sorted(set(sales_transaction_ids))
        for transaction_id_batch in _chunked(deduped_ids, _MATCH_BATCH_SIZE):
            transactions = list(
                session.execute(
                    base_query.where(
                        SalesTransaction.id.in_(transaction_id_batch)
                    ).order_by(SalesTransaction.id)
                ).scalars()
            )
            if transactions:
                yield transactions
        return

    last_seen_id = 0
    while True:
        transactions = list(
            session.execute(
                base_query.where(SalesTransaction.id > last_seen_id)
                .order_by(SalesTransaction.id)
                .limit(_MATCH_BATCH_SIZE)
            ).scalars()
        )
        if not transactions:
            return
        yield transactions
        last_seen_id = transactions[-1].id


def _derive_sales_parcel_matches(
    transaction: SalesTransaction,
    *,
    parcel_number_index: dict[str, tuple[str, ...]],
    address_index: dict[str, tuple[str, ...]],
    legal_description_index: dict[str, tuple[str, ...]],
) -> list[SalesMatchSpec]:
    exact_match_value = transaction.official_parcel_number_norm
    exact_candidates = None
    if exact_match_value is not None:
        exact_candidates = parcel_number_index.get(exact_match_value)
    if exact_candidates:
        return _build_sales_match_specs(
            exact_candidates,
            match_method=EXACT_PARCEL_NUMBER_MATCH_METHOD,
            matched_value=exact_match_value,
            confidence_score=EXACT_MATCH_CONFIDENCE,
        )

    for crosswalk_value in _parcel_number_crosswalk_candidates(
        transaction.official_parcel_number_raw
    ):
        crosswalk_candidates = parcel_number_index.get(crosswalk_value)
        if not crosswalk_candidates:
            continue
        return _build_sales_match_specs(
            crosswalk_candidates,
            match_method=PARCEL_NUMBER_CROSSWALK_MATCH_METHOD,
            matched_value=crosswalk_value,
            confidence_score=PARCEL_NUMBER_CROSSWALK_CONFIDENCE,
        )

    address_match_value = transaction.property_address_norm
    address_candidates = None
    if address_match_value is not None:
        address_candidates = address_index.get(address_match_value)
    if address_candidates:
        return _build_sales_match_specs(
            address_candidates,
            match_method=NORMALIZED_ADDRESS_MATCH_METHOD,
            matched_value=address_match_value,
            confidence_score=ADDRESS_MATCH_CONFIDENCE,
        )

    legal_match_value = _normalize_sales_legal_description(
        transaction.legal_description_raw
    )
    legal_candidates = None
    if legal_match_value is not None:
        legal_candidates = legal_description_index.get(legal_match_value)
    if legal_candidates:
        return _build_sales_match_specs(
            legal_candidates,
            match_method=NORMALIZED_LEGAL_DESCRIPTION_MATCH_METHOD,
            matched_value=legal_match_value,
            confidence_score=LEGAL_DESCRIPTION_MATCH_CONFIDENCE,
        )

    return []


def _build_sales_match_specs(
    parcel_ids: tuple[str, ...],
    *,
    match_method: str,
    matched_value: Optional[str],
    confidence_score: Decimal,
) -> list[SalesMatchSpec]:
    if matched_value is None:
        return []

    is_auto_accepted = len(parcel_ids) == 1
    return [
        SalesMatchSpec(
            parcel_id=parcel_id,
            match_method=match_method,
            confidence_score=confidence_score,
            match_rank=index,
            is_primary=is_auto_accepted and index == 1,
            match_review_status=(
                AUTO_ACCEPTED_MATCH_REVIEW_STATUS
                if is_auto_accepted
                else NEEDS_REVIEW_MATCH_REVIEW_STATUS
            ),
            matched_value=matched_value,
        )
        for index, parcel_id in enumerate(parcel_ids, start=1)
    ]


def _sync_sales_parcel_matches(
    session: Session,
    *,
    transaction: SalesTransaction,
    desired_matches: list[SalesMatchSpec],
    existing_matches: dict[tuple[str, str], SalesParcelMatch],
) -> SalesMatchSyncCounts:
    desired_by_key = {
        (match.parcel_id, match.match_method): match for match in desired_matches
    }

    inserted_rows = 0
    updated_rows = 0
    for key, desired in desired_by_key.items():
        existing = existing_matches.get(key)
        if existing is None:
            session.add(
                SalesParcelMatch(
                    sales_transaction_id=transaction.id,
                    parcel_id=desired.parcel_id,
                    match_method=desired.match_method,
                    confidence_score=desired.confidence_score,
                    match_rank=desired.match_rank,
                    is_primary=desired.is_primary,
                    match_review_status=desired.match_review_status,
                    matched_value=desired.matched_value,
                    matcher_version=desired.matcher_version,
                )
            )
            inserted_rows += 1
            continue

        if not _sales_match_needs_update(existing, desired):
            continue

        existing.confidence_score = desired.confidence_score
        existing.match_rank = desired.match_rank
        existing.is_primary = desired.is_primary
        existing.match_review_status = desired.match_review_status
        existing.matched_value = desired.matched_value
        existing.matcher_version = desired.matcher_version
        existing.matched_at = func.now()
        updated_rows += 1

    deleted_rows = 0
    for key, existing in existing_matches.items():
        if key in desired_by_key:
            continue
        session.delete(existing)
        deleted_rows += 1

    return SalesMatchSyncCounts(
        inserted_rows=inserted_rows,
        updated_rows=updated_rows,
        deleted_rows=deleted_rows,
    )


def _sales_match_needs_update(
    existing: SalesParcelMatch,
    desired: SalesMatchSpec,
) -> bool:
    return any(
        (
            existing.confidence_score != desired.confidence_score,
            existing.match_rank != desired.match_rank,
            existing.is_primary != desired.is_primary,
            existing.match_review_status != desired.match_review_status,
            existing.matched_value != desired.matched_value,
            existing.matcher_version != desired.matcher_version,
        )
    )


def _derive_sales_exclusions(values: dict[str, object]) -> list[SalesExclusionSpec]:
    if values.get("import_status") != "loaded":
        return []

    specs: list[SalesExclusionSpec] = []
    if values.get("arms_length_indicator_norm") is False:
        specs.append(
            SalesExclusionSpec(
                exclusion_code="non_arms_length",
                exclusion_reason="Excluded because the RETR arms-length indicator "
                "is false.",
                excluded_by_rule="v1_non_arms_length_indicator",
            )
        )

    if values.get("usable_sale_indicator_norm") is False:
        specs.append(
            SalesExclusionSpec(
                exclusion_code="non_usable_sale",
                exclusion_reason="Excluded because the RETR usable-sale indicator "
                "is false.",
                excluded_by_rule="v1_usable_sale_indicator",
            )
        )

    transfer_text = _exclusion_texts(
        values,
        "conveyance_type",
        "deed_type",
    )
    if _contains_any_exclusion_phrase(transfer_text, _FAMILY_TRANSFER_PHRASES):
        specs.append(
            SalesExclusionSpec(
                exclusion_code="family_transfer",
                exclusion_reason="Excluded because conveyance or deed text indicates "
                "a family transfer.",
                excluded_by_rule="v1_family_transfer_keywords",
            )
        )

    government_text = transfer_text + _exclusion_texts(
        values,
        "grantor_name",
        "grantee_name",
    )
    if _contains_any_exclusion_phrase(
        government_text,
        _GOVERNMENT_TRANSFER_PHRASES,
    ):
        specs.append(
            SalesExclusionSpec(
                exclusion_code="government_transfer",
                exclusion_reason="Excluded because source text indicates a "
                "government transfer.",
                excluded_by_rule="v1_government_transfer_keywords",
            )
        )

    if _contains_corrective_deed_phrase(transfer_text):
        specs.append(
            SalesExclusionSpec(
                exclusion_code="corrective_deed",
                exclusion_reason="Excluded because conveyance or deed text indicates "
                "a corrective deed.",
                excluded_by_rule="v1_corrective_deed_keywords",
            )
        )

    return specs


def _exclusion_texts(
    values: dict[str, object],
    *field_names: str,
) -> tuple[str, ...]:
    text_values: list[str] = []
    for field_name in field_names:
        value = values.get(field_name)
        if not isinstance(value, str):
            continue
        collapsed = _collapsed_text(value)
        if collapsed is None:
            continue
        text_values.append(collapsed.lower())
    return tuple(text_values)


def _contains_any_exclusion_phrase(
    text_values: tuple[str, ...],
    phrases: tuple[str, ...],
) -> bool:
    return any(phrase in value for value in text_values for phrase in phrases)


def _contains_corrective_deed_phrase(text_values: tuple[str, ...]) -> bool:
    return any(_CORRECTIVE_DEED_PATTERN.search(value) for value in text_values)


def _chunked(values: list[int], size: int) -> Iterator[list[int]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


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
    normalized = _SPACE_RE.sub(" ", normalized).strip()
    normalized = _strip_trailing_city_state_zip(normalized)
    return _normalize_address_tokens(normalized)


def _normalize_address_tokens(normalized: str) -> str:
    return " ".join(
        _ADDRESS_TOKEN_NORMALIZATION.get(token, token) for token in normalized.split()
    )


def _parcel_number_crosswalk_candidates(value: Optional[str]) -> tuple[str, ...]:
    collapsed = _collapsed_text(value)
    if collapsed is None or "/" not in collapsed:
        return ()

    prefix, suffix = collapsed.split("/", 1)
    suffix_digits = "".join(char for char in suffix if char.isdigit())
    if len(suffix_digits) != 12:
        return ()

    # Some RETR exports transpose the first two 2-digit segments after the slash,
    # e.g. 0601... instead of the parcel-side 0610... representation.
    swapped = prefix + suffix_digits[:2] + suffix_digits[3] + suffix_digits[2]
    swapped += suffix_digits[4:]

    normalized = _normalize_parcel_number(f"{prefix}/{swapped[len(prefix):]}")
    if normalized is None:
        return ()
    return (normalized,)


def _normalize_sales_legal_description(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None

    match = _LEGAL_DESCRIPTION_LOT_BLOCK_PATTERN.match(collapsed.upper())
    if match is None:
        return None

    return _build_legal_description_match_key(
        subdivision=match.group("subdivision"),
        lot_value=match.group("lot_paren") or match.group("lot"),
        block_value=match.group("block_paren") or match.group("block"),
    )


def _normalize_parcel_legal_description(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None

    normalized = _LEGAL_TEXT_PUNCTUATION_RE.sub(" ", collapsed.upper())
    normalized = _SPACE_RE.sub(" ", normalized).strip()
    if not normalized:
        return None

    for pattern in (
        _PARCEL_DESCRIPTION_BLOCK_LOT_PATTERN,
        _PARCEL_DESCRIPTION_LOT_PATTERN,
        _PARCEL_DESCRIPTION_LEADING_LOT_PATTERN,
    ):
        match = pattern.match(normalized)
        if match is None:
            continue
        return _build_legal_description_match_key(
            subdivision=match.group("subdivision"),
            lot_value=match.group("lot"),
            block_value=match.groupdict().get("block"),
        )

    return None


def _build_legal_description_match_key(
    *,
    subdivision: Optional[str],
    lot_value: Optional[str],
    block_value: Optional[str],
) -> Optional[str]:
    normalized_subdivision = _normalize_legal_subdivision(subdivision)
    normalized_lot = _normalize_legal_component(lot_value)
    normalized_block = _normalize_legal_component(block_value)
    if normalized_subdivision is None or normalized_lot is None:
        return None

    components = [f"SUBDIVISION:{normalized_subdivision}", f"LOT:{normalized_lot}"]
    if normalized_block is not None:
        components.append(f"BLOCK:{normalized_block}")
    return "|".join(components)


def _normalize_legal_subdivision(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None

    normalized = _LEGAL_TEXT_PUNCTUATION_RE.sub(" ", collapsed.upper())
    normalized = _SPACE_RE.sub(" ", normalized).strip()
    if " ADDITION TO " in normalized:
        normalized = normalized.split(" ADDITION TO ", 1)[1].strip()
    if normalized.startswith("THE "):
        normalized = normalized[4:].strip()
    if not normalized:
        return None
    return normalized


def _normalize_legal_component(value: Optional[str]) -> Optional[str]:
    collapsed = _collapsed_text(value)
    if collapsed is None:
        return None

    normalized = _LEGAL_TEXT_PUNCTUATION_RE.sub("", collapsed.upper())
    if not normalized:
        return None
    return normalized


def _strip_trailing_city_state_zip(normalized: str) -> str:
    tokens = normalized.split()
    if len(tokens) < 4 or tokens[-2] != "WI" or not _is_zip_token(tokens[-1]):
        return normalized

    street_and_city_tokens = tokens[:-2]
    last_suffix_index = None
    for index, token in enumerate(street_and_city_tokens):
        if token in _STREET_SUFFIX_TOKENS:
            last_suffix_index = index

    if last_suffix_index is None:
        return normalized

    end_index = last_suffix_index
    if last_suffix_index + 1 < len(street_and_city_tokens):
        next_token = street_and_city_tokens[last_suffix_index + 1]
        if len(next_token) <= 2:
            end_index = last_suffix_index + 1

    return " ".join(street_and_city_tokens[: end_index + 1])


def _is_zip_token(token: str) -> bool:
    if len(token) == 5 and token.isdigit():
        return True
    if len(token) == 10 and token[5] == "-":
        return token[:5].isdigit() and token[6:].isdigit()
    return False


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
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d"):
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
