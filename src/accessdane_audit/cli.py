from __future__ import annotations

import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence, TypedDict

import typer
from bs4 import BeautifulSoup
from sqlalchemy import delete, select

from .anomaly import detect_anomalies
from .appeals import AppealImportFileError, ingest_appeals_csv
from .build_features import build_features
from .config import load_settings
from .db import get_session_factory, session_scope
from .db import init_db as init_db_schema
from .extraction_signals import (
    LINEAGE_MODAL_BY_RELATIONSHIP_TYPE,
    is_placeholder_payment_row,
)
from .html_utils import html_attr_text
from .models import (
    AssessmentRecord,
    Fetch,
    Parcel,
    ParcelCharacteristic,
    ParcelLineageLink,
    ParcelSummary,
    PaymentRecord,
    TaxRecord,
)
from .parcel_year_facts import rebuild_parcel_year_facts
from .parse import ParsedPage, parse_page
from .permits import PermitImportFileError, ingest_permits_csv
from .profiling import build_data_profile
from .quality import quality_report_to_dict, run_data_quality_checks
from .retr import (
    RetrImportFileError,
    build_sales_match_audit_report,
    ingest_retr_csv,
    match_sales_transactions,
)
from .sales_ratio_study import build_sales_ratio_study
from .scrape import fetch_page
from .search import search_trs
from .spatial import detect_spatial_support, spatial_support_status_to_dict
from .trs import DEFAULT_SPLIT_PARTS, enumerate_trs, parse_trs_code

app = typer.Typer(add_completion=False)


@dataclass(frozen=True)
class ParseWorkItem:
    fetch_id: int
    parcel_id: str
    raw_path: Optional[str]


@dataclass(frozen=True)
class ParcelCharacteristicRebuildSummary:
    selected_parcels: int
    eligible_fetches_scanned: int
    rows_deleted: int
    rows_written: int
    skipped_fetches: int
    parcel_failures: int


@dataclass(frozen=True)
class ParcelLineageLinkRebuildSummary:
    selected_parcels: int
    eligible_fetches_scanned: int
    rows_deleted: int
    rows_written: int
    skipped_fetches: int
    parcel_failures: int


@dataclass(frozen=True)
class _ParcelCharacteristicRebuildResult:
    eligible_fetches_scanned: int
    rows_deleted: int
    rows_written: int
    skipped_fetches: int
    parcel_failed: bool = False
    error_message: Optional[str] = None


@dataclass(frozen=True)
class _ParcelLineageLinkRebuildResult:
    eligible_fetches_scanned: int
    rows_deleted: int
    rows_written: int
    skipped_fetches: int
    parcel_failed: bool = False
    error_message: Optional[str] = None


@dataclass(frozen=True)
class ParseSummary:
    selected_fetches: int
    succeeded_fetches: int
    failed_fetches: int
    skipped_missing_raw_path: int


class AssessmentFields(TypedDict):
    valuation_classification: Optional[str]
    assessment_acres: Optional[Decimal]
    land_value: Optional[Decimal]
    improved_value: Optional[Decimal]
    total_value: Optional[Decimal]
    average_assessment_ratio: Optional[Decimal]
    estimated_fair_market_value: Optional[Decimal]
    valuation_date: Optional[date]


class ParcelCharacteristicFields(TypedDict):
    formatted_parcel_number: Optional[str]
    state_municipality_code: Optional[str]
    township: Optional[str]
    range: Optional[str]
    section: Optional[str]
    quarter_quarter: Optional[str]
    has_dcimap_link: Optional[bool]
    has_google_map_link: Optional[bool]
    has_bing_map_link: Optional[bool]
    current_assessment_year: Optional[int]
    current_valuation_classification: Optional[str]
    current_assessment_acres: Optional[Decimal]
    current_assessment_ratio: Optional[Decimal]
    current_estimated_fair_market_value: Optional[Decimal]
    current_tax_info_available: Optional[bool]
    current_payment_history_available: Optional[bool]
    tax_jurisdiction_count: Optional[int]
    is_exempt_style_page: Optional[bool]
    has_empty_valuation_breakout: Optional[bool]
    has_empty_tax_section: Optional[bool]


@dataclass(frozen=True)
class ParcelCharacteristicCandidate:
    fields: ParcelCharacteristicFields
    rank_key: tuple[int, datetime, int]


@app.command("init-db")
def init_db() -> None:
    settings = load_settings()
    init_db_schema(settings.database_url)
    typer.echo("Database initialized.")


@app.command("ingest-retr")
def ingest_retr_cmd(
    file: Path = typer.Option(
        ...,
        "--file",
        exists=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a Wisconsin DOR RETR CSV export",
    ),
) -> None:
    settings = load_settings()

    try:
        with session_scope(settings.database_url) as session:
            summary = ingest_retr_csv(session, file)
    except RetrImportFileError as exc:
        raise typer.BadParameter(str(exc), param_hint="--file") from exc

    typer.echo(
        "RETR import summary: "
        f"total={summary.total_rows} "
        f"loaded={summary.loaded_rows} "
        f"rejected={summary.rejected_rows} "
        f"inserted={summary.inserted_rows} "
        f"updated={summary.updated_rows}"
    )


@app.command("ingest-permits")
def ingest_permits_cmd(
    file: Path = typer.Option(
        ...,
        "--file",
        exists=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a permit events CSV file",
    ),
) -> None:
    settings = load_settings()

    try:
        with session_scope(settings.database_url) as session:
            summary = ingest_permits_csv(session, file)
    except PermitImportFileError as exc:
        raise typer.BadParameter(str(exc), param_hint="--file") from exc

    typer.echo(
        "Permit import summary: "
        f"total={summary.total_rows} "
        f"loaded={summary.loaded_rows} "
        f"rejected={summary.rejected_rows} "
        f"inserted={summary.inserted_rows} "
        f"updated={summary.updated_rows}"
    )
    if summary.rejection_reason_counts:
        typer.echo("Permit rejection counts:")
        for reason, count in summary.rejection_reason_counts.items():
            typer.echo(f"  {reason}={count}")
    if summary.warning_counts:
        typer.echo("Permit warning counts:")
        for warning, count in summary.warning_counts.items():
            typer.echo(f"  {warning}={count}")


@app.command("ingest-appeals")
def ingest_appeals_cmd(
    file: Path = typer.Option(
        ...,
        "--file",
        exists=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to an appeal events CSV file",
    ),
) -> None:
    settings = load_settings()

    try:
        with session_scope(settings.database_url) as session:
            summary = ingest_appeals_csv(session, file)
    except AppealImportFileError as exc:
        raise typer.BadParameter(str(exc), param_hint="--file") from exc

    typer.echo(
        "Appeal import summary: "
        f"total={summary.total_rows} "
        f"loaded={summary.loaded_rows} "
        f"rejected={summary.rejected_rows} "
        f"inserted={summary.inserted_rows} "
        f"updated={summary.updated_rows}"
    )
    if summary.rejection_reason_counts:
        typer.echo("Appeal rejection counts:")
        for reason, count in summary.rejection_reason_counts.items():
            typer.echo(f"  {reason}={count}")
    if summary.warning_counts:
        typer.echo("Appeal warning counts:")
        for warning, count in summary.warning_counts.items():
            typer.echo(f"  {warning}={count}")


@app.command("match-sales")
def match_sales_cmd() -> None:
    settings = load_settings()

    with session_scope(settings.database_url) as session:
        summary = match_sales_transactions(session)

    typer.echo(
        "Sales match summary: "
        f"selected={summary.selected_transactions} "
        f"matched={summary.matched_transactions} "
        f"rows_written={summary.rows_written} "
        f"rows_deleted={summary.rows_deleted} "
        f"needs_review={summary.needs_review_transactions} "
        f"unresolved={summary.unresolved_transactions} "
        f"ambiguous={summary.ambiguous_transactions} "
        f"low_confidence={summary.low_confidence_transactions}"
    )


@app.command("report-sales-matches")
def report_sales_matches_cmd(
    ids_file: Optional[Path] = typer.Option(
        None,
        "--ids",
        help="File with sales transaction IDs (one integer ID per line)",
    ),
    out: Optional[Path] = typer.Option(None, "--out", help="Output JSON path"),
    sample_size: int = typer.Option(
        20,
        "--sample-size",
        min=1,
        help="Maximum sample IDs per review-queue category",
    ),
) -> None:
    settings = load_settings()
    transaction_ids = (
        _collect_transaction_ids(ids_file, param_hint="--ids") if ids_file else None
    )
    with session_scope(settings.database_url) as session:
        payload = build_sales_match_audit_report(
            session,
            sales_transaction_ids=transaction_ids,
            sample_size=sample_size,
        )

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        typer.echo(json.dumps(payload, indent=2))


@app.command("spatial-support")
def spatial_support_cmd(
    out: Optional[Path] = typer.Option(None, "--out", help="Output JSON path"),
) -> None:
    settings = load_settings()
    with session_scope(settings.database_url) as session:
        payload = spatial_support_status_to_dict(detect_spatial_support(session))

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        typer.echo(json.dumps(payload, indent=2))


@app.command("sales-ratio-study")
def sales_ratio_study_cmd(
    ids: list[str] = typer.Option(
        [],
        "--id",
        help="Parcel ID to include (repeatable)",
    ),
    ids_file: Optional[Path] = typer.Option(
        None,
        "--ids",
        exists=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="File with parcel IDs (one parcel ID per line)",
    ),
    year: list[int] = typer.Option(
        [],
        "--year",
        help="Sale year filter (repeatable)",
        min=1,
    ),
    municipality: Optional[str] = typer.Option(
        None,
        "--municipality",
        help="Filter groups to this municipality name",
    ),
    valuation_class: Optional[str] = typer.Option(
        None,
        "--class",
        help="Filter groups to this valuation classification",
    ),
    version_tag: str = typer.Option(
        "sales_ratio_v1",
        "--version-tag",
        help="Version tag recorded in scoring_runs",
    ),
    out: Optional[Path] = typer.Option(None, "--out", help="Output JSON path"),
) -> None:
    settings = load_settings()
    if ids or ids_file:
        parcel_ids = _collect_ids(ids, ids_file)
        if not parcel_ids:
            raise typer.BadParameter(
                "At least one parcel ID must be provided via --id or --ids."
            )
    else:
        parcel_ids = None
    years = sorted(set(year)) if year else None

    with session_scope(settings.database_url) as session:
        payload = build_sales_ratio_study(
            session,
            version_tag=version_tag,
            parcel_ids=parcel_ids,
            years=years,
            municipality=municipality,
            valuation_classification=valuation_class,
        )

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        typer.echo(json.dumps(payload, indent=2))

    run = payload.get("run", {})
    if isinstance(run, dict) and run.get("status") == "failed":
        raise typer.Exit(code=1)


@app.command("build-features")
def build_features_cmd(
    ids: list[str] = typer.Option(
        [],
        "--id",
        help="Parcel ID to include (repeatable)",
    ),
    ids_file: Optional[Path] = typer.Option(
        None,
        "--ids",
        exists=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="File with parcel IDs (one parcel ID per line)",
    ),
    year: list[int] = typer.Option(
        [],
        "--year",
        help="Feature year filter (repeatable)",
        min=1,
    ),
    feature_version: str = typer.Option(
        "feature_v1",
        "--feature-version",
        help="Version tag recorded in scoring_runs",
    ),
    out: Optional[Path] = typer.Option(None, "--out", help="Output JSON path"),
) -> None:
    settings = load_settings()
    if ids or ids_file:
        parcel_ids = _collect_ids(ids, ids_file)
        if not parcel_ids:
            raise typer.BadParameter(
                "At least one parcel ID must be provided via --id or --ids."
            )
    else:
        parcel_ids = None
    years = sorted(set(year)) if year else None

    with session_scope(settings.database_url) as session:
        payload = build_features(
            session,
            feature_version=feature_version,
            parcel_ids=parcel_ids,
            years=years,
        )

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        typer.echo(json.dumps(payload, indent=2))

    run = payload.get("run", {})
    if isinstance(run, dict) and run.get("status") == "failed":
        raise typer.Exit(code=1)


@app.command("enumerate-trs")
def enumerate_trs_cmd(
    trs: str = typer.Option(..., "--trs", help="TRS code, e.g. 06/10"),
    sections: str = typer.Option(
        ..., "--sections", help="Section list, e.g. 1-36 or 1,2,3"
    ),
    split: str = typer.Option("", "--split", help="Comma list of sections to split"),
    split_quarters: bool = typer.Option(
        False, "--split-quarters", help="Split all sections into quarters"
    ),
    split_quarter_quarters: bool = typer.Option(
        False,
        "--split-quarter-quarters",
        help="Split quarter rows into quarter-quarters",
    ),
    split_parts: str = typer.Option(
        ",".join(DEFAULT_SPLIT_PARTS), "--split-parts", help="Subsection labels"
    ),
    quarter_quarter_parts: str = typer.Option(
        "NE,NW,SE,SW", "--quarter-quarter-parts", help="Quarter-quarter labels"
    ),
    cap: int = typer.Option(0, "--cap", help="Maximum rows to emit (0 = no cap)"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output CSV path"),
) -> None:
    rows = _enumerate_trs_rows(
        trs=trs,
        sections=sections,
        split=split,
        split_quarters=split_quarters,
        split_parts=split_parts,
        split_quarter_quarters=split_quarter_quarters,
        quarter_quarter_parts=quarter_quarter_parts,
        cap=cap,
    )

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        _write_csv(
            out,
            rows,
            [
                "trs_code",
                "township",
                "range",
                "section",
                "subsection",
                "quarter",
                "quarter_quarter",
            ],
        )
    else:
        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=[
                "trs_code",
                "township",
                "range",
                "section",
                "subsection",
                "quarter",
                "quarter_quarter",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


@app.command("search-trs")
def search_trs_cmd(
    trs: Optional[str] = typer.Option(None, "--trs", help="TRS code, e.g. 06/10"),
    township: Optional[int] = typer.Option(None, "--township", help="Township number"),
    range_: Optional[int] = typer.Option(None, "--range", help="Range number"),
    sections: str = typer.Option(
        "", "--sections", help="Section list, e.g. 1-36 or 1,2,3"
    ),
    quarters: str = typer.Option(
        "", "--quarters", help="Quarter list, e.g. NE,NW,SE,SW"
    ),
    quarter_quarters: str = typer.Option(
        "NE,NW,SE,SW", "--quarter-quarters", help="Quarter-quarter list"
    ),
    auto_split: bool = typer.Option(
        False,
        "--auto-split",
        help="Split into quarter-quarters when results are truncated",
    ),
    quarter_quarter_parts: str = typer.Option(
        "NE,NW,SE,SW",
        "--quarter-quarter-parts",
        help="Quarter-quarter parts for auto-split",
    ),
    cap: int = typer.Option(0, "--cap", help="Maximum parcel IDs to emit (0 = no cap)"),
    municipality_id: int = typer.Option(
        50, "--municipality-id", help="AccessDane municipality id"
    ),
    out: Optional[Path] = typer.Option(None, "--out", help="Output parcel ID file"),
) -> None:
    if trs:
        township, range_ = parse_trs_code(trs)
    if township is None or range_ is None:
        raise typer.BadParameter("Provide --trs or both --township and --range.")

    section_list: Sequence[Optional[int]]
    if sections:
        section_list = _parse_sections(sections)
    else:
        section_list = [None]

    quarter_list: Sequence[Optional[str]]
    if quarters:
        quarter_list = _parse_list(quarters)
    else:
        quarter_list = [None]

    if quarter_list == [None]:
        quarter_quarter_list: Sequence[Optional[str]] = [None]
    else:
        if quarter_quarters:
            quarter_quarter_list = _parse_list(quarter_quarters)
        else:
            quarter_quarter_list = [None]
    auto_parts = _parse_list(quarter_quarter_parts) if quarter_quarter_parts else []

    settings = load_settings()
    collected: list[str] = []
    seen: set[str] = set()
    for section in section_list:
        for quarter in quarter_list:
            for quarter_quarter in quarter_quarter_list:
                result = search_trs(
                    settings=settings,
                    municipality_id=municipality_id,
                    township=township,
                    range_=range_,
                    section=section,
                    quarter=quarter,
                    quarter_quarter=quarter_quarter,
                )
                if result.truncated and auto_split and quarter_quarter is None:
                    for auto_part in auto_parts:
                        sub_result = search_trs(
                            settings=settings,
                            municipality_id=municipality_id,
                            township=township,
                            range_=range_,
                            section=section,
                            quarter=quarter,
                            quarter_quarter=auto_part,
                        )
                        if sub_result.truncated:
                            typer.echo(
                                f"Warning: results truncated for {sub_result.url}",
                                err=True,
                            )
                        for parcel_id in sub_result.parcel_ids:
                            if parcel_id not in seen:
                                seen.add(parcel_id)
                                collected.append(parcel_id)
                                if cap and len(collected) >= cap:
                                    break
                        if cap and len(collected) >= cap:
                            break
                    continue
                if result.truncated:
                    typer.echo(
                        f"Warning: results truncated for {result.url}",
                        err=True,
                    )
                for parcel_id in result.parcel_ids:
                    if parcel_id not in seen:
                        seen.add(parcel_id)
                        collected.append(parcel_id)
                        if cap and len(collected) >= cap:
                            break
                if cap and len(collected) >= cap:
                    break
            if cap and len(collected) >= cap:
                break

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            "\n".join(collected) + ("\n" if collected else ""), encoding="utf-8"
        )
    else:
        typer.echo("\n".join(collected))


@app.command("search-trs-csv")
def search_trs_csv_cmd(
    trs_csv: Path = typer.Option(..., "--trs-csv", help="CSV of TRS blocks"),
    municipality_id: int = typer.Option(
        50, "--municipality-id", help="AccessDane municipality id"
    ),
    auto_split: bool = typer.Option(
        False,
        "--auto-split",
        help="Split into quarter-quarters when results are truncated",
    ),
    quarter_quarter_parts: str = typer.Option(
        "NE,NW,SE,SW",
        "--quarter-quarter-parts",
        help="Quarter-quarter parts for auto-split",
    ),
    quarter_quarters: str = typer.Option(
        "NE,NW,SE,SW",
        "--quarter-quarters",
        help="Quarter-quarter defaults when a quarter is set",
    ),
    cap: int = typer.Option(0, "--cap", help="Maximum parcel IDs to emit (0 = no cap)"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output parcel ID file"),
) -> None:
    rows = _read_trs_csv(trs_csv)
    collected = _search_trs_rows(
        rows=rows,
        municipality_id=municipality_id,
        auto_split=auto_split,
        quarter_quarter_parts=quarter_quarter_parts,
        quarter_quarters=quarter_quarters,
        cap=cap,
    )

    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        _write_lines(out, collected)
    else:
        typer.echo("\n".join(collected))


@app.command("run-all")
def run_all_cmd(
    trs: Optional[str] = typer.Option(None, "--trs", help="TRS code, e.g. 06/10"),
    sections: Optional[str] = typer.Option(
        None, "--sections", help="Section list, e.g. 1-36 or 1,2,3"
    ),
    split: str = typer.Option("", "--split", help="Comma list of sections to split"),
    split_quarters: bool = typer.Option(
        False, "--split-quarters", help="Split all sections into quarters"
    ),
    split_quarter_quarters: bool = typer.Option(
        False,
        "--split-quarter-quarters",
        help="Split quarter rows into quarter-quarters",
    ),
    split_parts: str = typer.Option(
        ",".join(DEFAULT_SPLIT_PARTS), "--split-parts", help="Subsection labels"
    ),
    quarter_quarter_parts: str = typer.Option(
        "NE,NW,SE,SW", "--quarter-quarter-parts", help="Quarter-quarter labels"
    ),
    municipality_id: int = typer.Option(
        50, "--municipality-id", help="AccessDane municipality id"
    ),
    auto_split: bool = typer.Option(
        False,
        "--auto-split",
        help="Split into quarter-quarters when results are truncated",
    ),
    quarter_quarters: str = typer.Option(
        "NE,NW,SE,SW",
        "--quarter-quarters",
        help="Quarter-quarter defaults when a quarter is set",
    ),
    cap_trs: int = typer.Option(0, "--cap-trs", help="Cap TRS rows (0 = no cap)"),
    cap_ids: int = typer.Option(0, "--cap-ids", help="Cap parcel IDs (0 = no cap)"),
    fetch_limit: int = typer.Option(
        0, "--fetch-limit", help="Cap fetches (0 = no cap)"
    ),
    parse_only: bool = typer.Option(
        False,
        "--parse-only",
        help="Skip TRS search and fetch; only parse existing fetches",
    ),
    parse_limit: int = typer.Option(
        0, "--parse-limit", help="Cap parse-stage fetches (0 = no cap)"
    ),
    parse_resume_after_fetch_id: Optional[int] = typer.Option(
        None,
        "--parse-resume-after-fetch-id",
        help="Skip parse-stage fetches through this fetch id, then continue",
    ),
    skip_tax_detail_payments: bool = typer.Option(
        False,
        "--skip-tax-detail-payments",
        help="Skip per-year payment tables inside TaxDetails modals",
    ),
    debug_parse: bool = typer.Option(
        False, "--debug-parse", help="Log parsed record counts during parsing"
    ),
    debug_limit: int = typer.Option(
        20, "--debug-limit", help="Max debug lines to print"
    ),
    debug_only_empty: bool = typer.Option(
        False, "--debug-only-empty", help="Only log when tax or payments are empty"
    ),
    reparse: bool = typer.Option(
        False,
        "--reparse",
        help="Delete existing parsed rows for each fetch before inserting",
    ),
    build_parcel_year_facts: bool = typer.Option(
        False,
        "--build-parcel-year-facts",
        help="Rebuild parcel_year_facts after parsing",
    ),
    skip_anomalies: bool = typer.Option(
        False,
        "--skip-anomalies",
        help="Skip anomaly generation after parsing",
    ),
    init_db: bool = typer.Option(False, "--init-db", help="Initialize DB schema"),
    trs_csv: Path = typer.Option(
        Path("data/trs_blocks.csv"), "--trs-csv", help="TRS CSV output"
    ),
    ids_out: Path = typer.Option(
        Path("data/parcel_ids.txt"), "--ids-out", help="Parcel ID output"
    ),
    parse_ids: Optional[Path] = typer.Option(
        None, "--parse-ids", help="Limit parse-only to IDs in a file"
    ),
    anomalies_out: Path = typer.Option(
        Path("data/anomalies.json"), "--anomalies-out", help="Anomalies output"
    ),
) -> None:
    settings = load_settings()
    run_scope_parcel_ids: Optional[list[str]] = None
    if init_db:
        init_db_schema(settings.database_url)
        typer.echo("DB initialized.")

    if not parse_only:
        if not trs or not sections:
            raise typer.BadParameter(
                "Provide --trs and --sections unless using --parse-only."
            )
        rows = _enumerate_trs_rows(
            trs=trs,
            sections=sections,
            split=split,
            split_quarters=split_quarters,
            split_parts=split_parts,
            split_quarter_quarters=split_quarter_quarters,
            quarter_quarter_parts=quarter_quarter_parts,
            cap=cap_trs,
        )
        trs_csv.parent.mkdir(parents=True, exist_ok=True)
        _write_csv(
            trs_csv,
            rows,
            [
                "trs_code",
                "township",
                "range",
                "section",
                "subsection",
                "quarter",
                "quarter_quarter",
            ],
        )
        typer.echo(f"TRS rows: {len(rows)} -> {trs_csv}")

        parcel_ids = _search_trs_rows(
            rows=rows,
            municipality_id=municipality_id,
            auto_split=auto_split,
            quarter_quarter_parts=quarter_quarter_parts,
            quarter_quarters=quarter_quarters,
            cap=cap_ids,
        )
        ids_out.parent.mkdir(parents=True, exist_ok=True)
        _write_lines(ids_out, parcel_ids)
        typer.echo(f"Parcel IDs: {len(parcel_ids)} -> {ids_out}")

        fetch_ids = parcel_ids
        if fetch_limit and fetch_limit > 0:
            fetch_ids = fetch_ids[:fetch_limit]
        run_scope_parcel_ids = fetch_ids
        typer.echo(f"Fetching {len(fetch_ids)} parcels...")

        with session_scope(settings.database_url) as session:
            for idx, parcel_id in enumerate(fetch_ids, start=1):
                if idx % 50 == 1 or idx == len(fetch_ids):
                    typer.echo(f"Fetch {idx}/{len(fetch_ids)}: {parcel_id}")
                _ensure_parcel(session, parcel_id)
                result = fetch_page(parcel_id, settings)
                session.add(
                    Fetch(
                        parcel_id=parcel_id,
                        url=result.url,
                        status_code=result.status_code,
                        raw_path=str(result.raw_path) if result.raw_path else None,
                        raw_sha256=result.raw_sha256,
                        raw_size=result.raw_size,
                    )
                )

    with session_scope(settings.database_url) as session:
        parsed_scope_ids: Optional[list[str]] = None
        if parse_only and parse_ids:
            parsed_scope_ids = _collect_ids([], parse_ids)
        work_items = _collect_parse_work_items(
            session,
            parcel_id=None,
            unparsed=not reparse,
            reparse=reparse,
            parcel_ids=parsed_scope_ids,
            resume_after_fetch_id=parse_resume_after_fetch_id,
            limit=parse_limit if parse_limit > 0 else None,
        )

    if parse_only and (
        parsed_scope_ids is not None
        or parse_resume_after_fetch_id is not None
        or parse_limit > 0
    ):
        run_scope_parcel_ids = list(
            dict.fromkeys(item.parcel_id for item in work_items)
        )

    parse_summary = _run_parse_work_items(
        settings.database_url,
        work_items,
        reparse=reparse,
        skip_tax_detail_payments=skip_tax_detail_payments,
        debug_parse=debug_parse,
        debug_limit=debug_limit,
        debug_only_empty=debug_only_empty,
    )

    if build_parcel_year_facts:
        if run_scope_parcel_ids == [] and parse_summary.selected_fetches == 0:
            typer.echo("parcel_year_facts rows built: 0")
        else:
            with session_scope(settings.database_url) as session:
                fact_rows = rebuild_parcel_year_facts(
                    session,
                    parcel_ids=run_scope_parcel_ids,
                )
            typer.echo(f"parcel_year_facts rows built: {fact_rows}")

    if not skip_anomalies:
        anomalies_out.parent.mkdir(parents=True, exist_ok=True)
        if run_scope_parcel_ids == [] and parse_summary.selected_fetches == 0:
            anomalies_out.write_text("[]", encoding="utf-8")
            typer.echo(f"Anomalies: 0 -> {anomalies_out}")
        else:
            with session_scope(settings.database_url) as session:
                anomalies = detect_anomalies(session, parcel_ids=run_scope_parcel_ids)
                anomalies_out.write_text(
                    json.dumps([asdict(item) for item in anomalies], indent=2),
                    encoding="utf-8",
                )
                typer.echo(f"Anomalies: {len(anomalies)} -> {anomalies_out}")


@app.command("fetch")
def fetch_cmd(
    ids: list[str] = typer.Option([], "--id", "-i", help="Parcel ID (repeatable)"),
    ids_file: Optional[Path] = typer.Option(None, "--ids", help="File with parcel IDs"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit number of IDs"),
) -> None:
    settings = load_settings()
    parcel_ids = _collect_ids(ids, ids_file)
    if limit:
        parcel_ids = parcel_ids[:limit]
    if not parcel_ids:
        raise typer.BadParameter("Provide at least one parcel ID via --id or --ids.")

    with session_scope(settings.database_url) as session:
        for parcel_id in parcel_ids:
            _ensure_parcel(session, parcel_id)
            result = fetch_page(parcel_id, settings)
            fetch = Fetch(
                parcel_id=parcel_id,
                url=result.url,
                status_code=result.status_code,
                raw_path=str(result.raw_path) if result.raw_path else None,
                raw_sha256=result.raw_sha256,
                raw_size=result.raw_size,
            )
            session.add(fetch)


@app.command("parse")
def parse_cmd(
    parcel_id: Optional[str] = typer.Option(None, "--parcel-id", help="Parcel ID"),
    unparsed: bool = typer.Option(
        False, "--unparsed", help="Only parse unparsed fetches"
    ),
    reparse: bool = typer.Option(
        False,
        "--reparse",
        help="Delete existing parsed rows for each fetch before inserting",
    ),
    debug_parse: bool = typer.Option(
        False, "--debug-parse", help="Log parsed record counts during parsing"
    ),
    debug_limit: int = typer.Option(
        20, "--debug-limit", help="Max debug lines to print"
    ),
    debug_only_empty: bool = typer.Option(
        False, "--debug-only-empty", help="Only log when tax or payments are empty"
    ),
    skip_tax_detail_payments: bool = typer.Option(
        False,
        "--skip-tax-detail-payments",
        help="Skip per-year payment tables inside TaxDetails modals",
    ),
    resume_after_fetch_id: Optional[int] = typer.Option(
        None,
        "--resume-after-fetch-id",
        help="Skip fetches through this fetch id, then continue",
    ),
    limit: int = typer.Option(0, "--limit", help="Max fetches to parse (0 = no cap)"),
) -> None:
    settings = load_settings()
    with session_scope(settings.database_url) as session:
        work_items = _collect_parse_work_items(
            session,
            parcel_id=parcel_id,
            unparsed=unparsed,
            reparse=reparse,
            resume_after_fetch_id=resume_after_fetch_id,
            limit=limit if limit > 0 else None,
        )

    parse_summary = _run_parse_work_items(
        settings.database_url,
        work_items,
        reparse=reparse,
        skip_tax_detail_payments=skip_tax_detail_payments,
        debug_parse=debug_parse,
        debug_limit=debug_limit,
        debug_only_empty=debug_only_empty,
    )
    if parse_summary.selected_fetches == 0:
        typer.echo("No matching fetches selected for parsing.")


def _collect_parse_work_items(
    session,
    *,
    parcel_id: Optional[str],
    unparsed: bool,
    reparse: bool,
    parcel_ids: Optional[list[str]] = None,
    resume_after_fetch_id: Optional[int] = None,
    limit: Optional[int] = None,
) -> list[ParseWorkItem]:
    query = (
        select(Fetch.id, Fetch.parcel_id, Fetch.raw_path)
        .where(Fetch.status_code == 200)
        .order_by(Fetch.id)
    )
    if parcel_id:
        query = query.where(Fetch.parcel_id == parcel_id)
    if parcel_ids is not None:
        query = query.where(Fetch.parcel_id.in_(parcel_ids))
    if unparsed and not reparse:
        query = query.where(Fetch.parsed_at.is_(None))
    if resume_after_fetch_id is not None:
        query = query.where(Fetch.id > resume_after_fetch_id)
    if limit is not None:
        query = query.limit(limit)
    rows = session.execute(query).all()
    return [
        ParseWorkItem(
            fetch_id=row.id,
            parcel_id=row.parcel_id,
            raw_path=row.raw_path,
        )
        for row in rows
    ]


def _persist_parsed_fetch(
    database_url: str,
    work_item: ParseWorkItem,
    parsed: ParsedPage,
    raw_html: str,
    *,
    reparse: bool,
) -> None:
    with session_scope(database_url) as session:
        fetch = session.get(Fetch, work_item.fetch_id)
        if fetch is None:
            return
        if reparse:
            _delete_parsed(session, fetch.id, fetch.parcel_id)
        _store_parsed(session, fetch, parsed, raw_html=raw_html)
        fetch.parsed_at = datetime.now(timezone.utc)
        fetch.parse_error = None


def _record_parse_error(database_url: str, fetch_id: int, error: str) -> None:
    with session_scope(database_url) as session:
        fetch = session.get(Fetch, fetch_id)
        if fetch is None:
            return
        fetch.parse_error = error


def _run_parse_work_items(
    database_url: str,
    work_items: Sequence[ParseWorkItem],
    *,
    reparse: bool,
    skip_tax_detail_payments: bool,
    debug_parse: bool,
    debug_limit: int,
    debug_only_empty: bool,
) -> ParseSummary:
    typer.echo(f"Parsing {len(work_items)} fetched pages...")
    total_tax = 0
    total_payments = 0
    total_assessments = 0
    succeeded_fetches = 0
    failed_fetches = 0
    skipped_missing_raw_path = 0
    debug_printed = 0

    for idx, work_item in enumerate(work_items, start=1):
        if idx % 50 == 1 or idx == len(work_items):
            typer.echo(f"Parse {idx}/{len(work_items)}: {work_item.parcel_id}")
        if not work_item.raw_path:
            skipped_missing_raw_path += 1
            continue
        try:
            html = Path(work_item.raw_path).read_text(encoding="utf-8")
            parsed = parse_page(
                html, include_tax_detail_payments=not skip_tax_detail_payments
            )
            if debug_parse:
                is_empty = (len(parsed.tax) == 0) or (len(parsed.payments) == 0)
                if (not debug_only_empty or is_empty) and debug_printed < debug_limit:
                    typer.echo(
                        f"Debug parse {work_item.parcel_id}: "
                        f"assessment={len(parsed.assessment)} "
                        f"tax={len(parsed.tax)} payments={len(parsed.payments)}"
                    )
                    debug_printed += 1
            _persist_parsed_fetch(
                database_url,
                work_item,
                parsed,
                html,
                reparse=reparse,
            )
            total_tax += len(parsed.tax)
            total_payments += len(parsed.payments)
            total_assessments += len(parsed.assessment)
            succeeded_fetches += 1
        except Exception as exc:
            failed_fetches += 1
            _record_parse_error(database_url, work_item.fetch_id, str(exc))
            typer.echo(
                f"Failed to parse fetch_id={work_item.fetch_id} "
                f"parcel_id={work_item.parcel_id} "
                f"({type(exc).__name__}: {exc})",
                err=True,
            )

    if debug_parse:
        typer.echo(
            "Debug totals: "
            f"assessment={total_assessments} tax={total_tax} payments={total_payments}"
        )

    summary = ParseSummary(
        selected_fetches=len(work_items),
        succeeded_fetches=succeeded_fetches,
        failed_fetches=failed_fetches,
        skipped_missing_raw_path=skipped_missing_raw_path,
    )
    typer.echo(
        "Parse summary: "
        f"selected={summary.selected_fetches} "
        f"succeeded={summary.succeeded_fetches} "
        f"failed={summary.failed_fetches} "
        f"skipped_missing_raw_path={summary.skipped_missing_raw_path}"
    )
    return summary


@app.command("anomalies")
def anomalies_cmd(
    ids_file: Optional[Path] = typer.Option(None, "--ids", help="File with parcel IDs"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output JSON path"),
) -> None:
    settings = load_settings()
    parcel_ids = _collect_ids([], ids_file) if ids_file else None
    with session_scope(settings.database_url) as session:
        anomalies = detect_anomalies(session, parcel_ids=parcel_ids)
        payload = [asdict(item) for item in anomalies]
        if out:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        else:
            typer.echo(json.dumps(payload, indent=2))


@app.command("build-parcel-year-facts")
def build_parcel_year_facts_cmd(
    ids: list[str] = typer.Option([], "--id", "-i", help="Parcel ID (repeatable)"),
    ids_file: Optional[Path] = typer.Option(None, "--ids", help="File with parcel IDs"),
) -> None:
    settings = load_settings()
    parcel_ids: Optional[list[str]] = _collect_ids(ids, ids_file)
    if (ids or ids_file is not None) and not parcel_ids:
        raise typer.BadParameter(
            "At least one parcel ID must be provided via --id or --ids."
        )
    if not parcel_ids:
        parcel_ids = None
    with session_scope(settings.database_url) as session:
        row_count = rebuild_parcel_year_facts(session, parcel_ids=parcel_ids)
    typer.echo(f"parcel_year_facts rows built: {row_count}")


@app.command("rebuild-parcel-characteristics")
def rebuild_parcel_characteristics_cmd(
    ids: list[str] = typer.Option([], "--id", "-i", help="Parcel ID (repeatable)"),
    ids_file: Optional[Path] = typer.Option(None, "--ids", help="File with parcel IDs"),
) -> None:
    if ids and ids_file:
        raise typer.BadParameter("Use either --id or --ids, not both.")
    settings = load_settings()
    parcel_ids = None
    if ids or ids_file is not None:
        parcel_ids = _collect_ids(ids, ids_file)
    summary = rebuild_parcel_characteristics(
        settings.database_url,
        parcel_ids=parcel_ids,
        raw_dir=settings.raw_dir,
    )
    typer.echo(f"Selected parcels: {summary.selected_parcels}")
    typer.echo(f"Eligible fetches scanned: {summary.eligible_fetches_scanned}")
    typer.echo(f"Rows deleted: {summary.rows_deleted}")
    typer.echo(f"Rows written: {summary.rows_written}")
    typer.echo(f"Skipped fetches: {summary.skipped_fetches}")
    typer.echo(f"Parcel failures: {summary.parcel_failures}")
    if summary.parcel_failures:
        raise typer.Exit(code=1)


@app.command("rebuild-parcel-lineage-links")
def rebuild_parcel_lineage_links_cmd(
    ids: list[str] = typer.Option([], "--id", "-i", help="Parcel ID (repeatable)"),
    ids_file: Optional[Path] = typer.Option(None, "--ids", help="File with parcel IDs"),
) -> None:
    if ids and ids_file:
        raise typer.BadParameter("Use either --id or --ids, not both.")
    settings = load_settings()
    parcel_ids = None
    if ids or ids_file is not None:
        parcel_ids = _collect_ids(ids, ids_file)
    summary = rebuild_parcel_lineage_links(
        settings.database_url,
        parcel_ids=parcel_ids,
        raw_dir=settings.raw_dir,
    )
    typer.echo(f"Selected parcels: {summary.selected_parcels}")
    typer.echo(f"Eligible fetches scanned: {summary.eligible_fetches_scanned}")
    typer.echo(f"Rows deleted: {summary.rows_deleted}")
    typer.echo(f"Rows written: {summary.rows_written}")
    typer.echo(f"Skipped fetches: {summary.skipped_fetches}")
    typer.echo(f"Parcel failures: {summary.parcel_failures}")
    if summary.parcel_failures:
        raise typer.Exit(code=1)


def rebuild_parcel_characteristics(
    database_url: str,
    *,
    parcel_ids: Optional[list[str]] = None,
    raw_dir: Optional[Path] = None,
) -> ParcelCharacteristicRebuildSummary:
    target_parcel_ids = _select_parcel_ids_for_parcel_characteristics_rebuild(
        database_url,
        parcel_ids=parcel_ids,
    )
    resolved_raw_dir = (
        raw_dir.expanduser().resolve()
        if raw_dir is not None
        else load_settings().raw_dir.expanduser().resolve()
    )
    eligible_fetches_scanned = 0
    rows_deleted = 0
    rows_written = 0
    skipped_fetches = 0
    parcel_failures = 0
    SessionFactory = get_session_factory(database_url)

    for parcel_id in target_parcel_ids:
        session = SessionFactory()
        try:
            result = _rebuild_parcel_characteristics_for_parcel(
                session,
                parcel_id,
                raw_dir=resolved_raw_dir,
            )
            eligible_fetches_scanned += result.eligible_fetches_scanned
            skipped_fetches += result.skipped_fetches
            if result.parcel_failed:
                session.rollback()
                parcel_failures += 1
                typer.echo(
                    result.error_message
                    or (
                        "Failed to rebuild parcel characteristics for "
                        f"parcel_id={parcel_id}"
                    ),
                    err=True,
                )
                continue
            session.commit()
        except Exception as exc:
            session.rollback()
            parcel_failures += 1
            typer.echo(
                f"Failed to rebuild parcel characteristics for parcel_id={parcel_id} "
                f"({type(exc).__name__}: {exc})",
                err=True,
            )
            continue
        finally:
            session.close()

        rows_deleted += result.rows_deleted
        rows_written += result.rows_written

    return ParcelCharacteristicRebuildSummary(
        selected_parcels=len(target_parcel_ids),
        eligible_fetches_scanned=eligible_fetches_scanned,
        rows_deleted=rows_deleted,
        rows_written=rows_written,
        skipped_fetches=skipped_fetches,
        parcel_failures=parcel_failures,
    )


def rebuild_parcel_lineage_links(
    database_url: str,
    *,
    parcel_ids: Optional[list[str]] = None,
    raw_dir: Optional[Path] = None,
) -> ParcelLineageLinkRebuildSummary:
    target_parcel_ids = _select_parcel_ids_for_parcel_lineage_links_rebuild(
        database_url,
        parcel_ids=parcel_ids,
    )
    resolved_raw_dir = (
        raw_dir.expanduser().resolve()
        if raw_dir is not None
        else load_settings().raw_dir.expanduser().resolve()
    )
    eligible_fetches_scanned = 0
    rows_deleted = 0
    rows_written = 0
    skipped_fetches = 0
    parcel_failures = 0
    SessionFactory = get_session_factory(database_url)

    for parcel_id in target_parcel_ids:
        session = SessionFactory()
        try:
            result = _rebuild_parcel_lineage_links_for_parcel(
                session,
                parcel_id,
                raw_dir=resolved_raw_dir,
            )
            eligible_fetches_scanned += result.eligible_fetches_scanned
            skipped_fetches += result.skipped_fetches
            if result.parcel_failed:
                session.rollback()
                parcel_failures += 1
                typer.echo(
                    result.error_message
                    or (
                        "Failed to rebuild parcel lineage links for "
                        f"parcel_id={parcel_id}"
                    ),
                    err=True,
                )
                continue
            session.commit()
        except Exception as exc:
            session.rollback()
            parcel_failures += 1
            typer.echo(
                f"Failed to rebuild parcel lineage links for parcel_id={parcel_id} "
                f"({type(exc).__name__}: {exc})",
                err=True,
            )
            continue
        finally:
            session.close()

        rows_deleted += result.rows_deleted
        rows_written += result.rows_written

    return ParcelLineageLinkRebuildSummary(
        selected_parcels=len(target_parcel_ids),
        eligible_fetches_scanned=eligible_fetches_scanned,
        rows_deleted=rows_deleted,
        rows_written=rows_written,
        skipped_fetches=skipped_fetches,
        parcel_failures=parcel_failures,
    )


def _select_parcel_ids_for_parcel_characteristics_rebuild(
    database_url: str,
    *,
    parcel_ids: Optional[list[str]],
) -> list[str]:
    if parcel_ids is not None:
        return list(dict.fromkeys(parcel_ids))

    with session_scope(database_url) as session:
        existing_ids = (
            session.execute(select(ParcelCharacteristic.parcel_id)).scalars().all()
        )
        eligible_fetch_ids = (
            session.execute(
                select(Fetch.parcel_id)
                .where(
                    Fetch.status_code == 200,
                    Fetch.parsed_at.is_not(None),
                    Fetch.parse_error.is_(None),
                    Fetch.raw_path.is_not(None),
                )
                .distinct()
            )
            .scalars()
            .all()
        )

    return sorted(set(existing_ids).union(eligible_fetch_ids))


def _select_parcel_ids_for_parcel_lineage_links_rebuild(
    database_url: str,
    *,
    parcel_ids: Optional[list[str]],
) -> list[str]:
    if parcel_ids is not None:
        return list(dict.fromkeys(parcel_ids))

    with session_scope(database_url) as session:
        existing_ids = (
            session.execute(select(ParcelLineageLink.parcel_id)).scalars().all()
        )
        eligible_fetch_ids = (
            session.execute(
                select(Fetch.parcel_id)
                .where(
                    Fetch.status_code == 200,
                    Fetch.parsed_at.is_not(None),
                    Fetch.parse_error.is_(None),
                    Fetch.raw_path.is_not(None),
                )
                .distinct()
            )
            .scalars()
            .all()
        )

    return sorted(set(existing_ids).union(eligible_fetch_ids))


def _rebuild_parcel_characteristics_for_parcel(
    session,
    parcel_id: str,
    *,
    raw_dir: Path,
) -> _ParcelCharacteristicRebuildResult:
    fetches = (
        session.execute(
            select(Fetch)
            .where(
                Fetch.parcel_id == parcel_id,
                Fetch.status_code == 200,
                Fetch.parsed_at.is_not(None),
                Fetch.parse_error.is_(None),
                Fetch.raw_path.is_not(None),
            )
            .order_by(Fetch.fetched_at, Fetch.id)
        )
        .scalars()
        .all()
    )

    if not fetches:
        deleted_result = session.execute(
            delete(ParcelCharacteristic).where(
                ParcelCharacteristic.parcel_id == parcel_id
            )
        )
        rows_deleted = deleted_result.rowcount or 0
        return _ParcelCharacteristicRebuildResult(
            eligible_fetches_scanned=0,
            rows_deleted=rows_deleted,
            rows_written=0,
            skipped_fetches=0,
        )

    best_candidate: Optional[ParcelCharacteristicCandidate] = None
    best_fetch: Optional[Fetch] = None
    skipped_fetches = 0

    for fetch in fetches:
        if not fetch.raw_path:
            continue
        raw_path = _resolve_raw_path(fetch.raw_path, raw_dir=raw_dir)
        if raw_path is None:
            skipped_fetches += 1
            continue
        try:
            html = raw_path.read_text(encoding="utf-8")
            parsed = parse_page(html)
            candidate = _build_parcel_characteristic_candidate(
                fetch, parsed, raw_html=html
            )
        except Exception as exc:
            skipped_fetches += 1
            typer.echo(
                f"Skipping fetch_id={fetch.id} during parcel_characteristics rebuild "
                f"for parcel_id={parcel_id} ({type(exc).__name__}: {exc})",
                err=True,
            )
            continue
        if candidate is None:
            continue
        if best_candidate is None or candidate.rank_key > best_candidate.rank_key:
            best_candidate = candidate
            best_fetch = fetch

    if best_candidate is None or best_fetch is None:
        return _ParcelCharacteristicRebuildResult(
            eligible_fetches_scanned=len(fetches),
            rows_deleted=0,
            rows_written=0,
            skipped_fetches=skipped_fetches,
            parcel_failed=True,
            error_message=(
                "Failed to rebuild parcel characteristics for "
                f"parcel_id={parcel_id} (no usable candidate from eligible fetches)"
            ),
        )

    deleted_result = session.execute(
        delete(ParcelCharacteristic).where(ParcelCharacteristic.parcel_id == parcel_id)
    )
    rows_deleted = deleted_result.rowcount or 0

    rows_written = 0
    target = ParcelCharacteristic(parcel_id=parcel_id)
    for field_name, value in best_candidate.fields.items():
        setattr(target, field_name, value)
    target.source_fetch_id = best_fetch.id
    target.built_at = datetime.now(timezone.utc)
    session.add(target)
    rows_written = 1

    return _ParcelCharacteristicRebuildResult(
        eligible_fetches_scanned=len(fetches),
        rows_deleted=rows_deleted,
        rows_written=rows_written,
        skipped_fetches=skipped_fetches,
    )


def _rebuild_parcel_lineage_links_for_parcel(
    session,
    parcel_id: str,
    *,
    raw_dir: Path,
) -> _ParcelLineageLinkRebuildResult:
    fetches = (
        session.execute(
            select(Fetch)
            .where(
                Fetch.parcel_id == parcel_id,
                Fetch.status_code == 200,
                Fetch.parsed_at.is_not(None),
                Fetch.parse_error.is_(None),
                Fetch.raw_path.is_not(None),
            )
            .order_by(Fetch.fetched_at, Fetch.id)
        )
        .scalars()
        .all()
    )

    if not fetches:
        deleted_result = session.execute(
            delete(ParcelLineageLink).where(ParcelLineageLink.parcel_id == parcel_id)
        )
        rows_deleted = deleted_result.rowcount or 0
        return _ParcelLineageLinkRebuildResult(
            eligible_fetches_scanned=0,
            rows_deleted=rows_deleted,
            rows_written=0,
            skipped_fetches=0,
        )

    processed_fetches = 0
    skipped_fetches = 0
    best_links: dict[tuple[str, str], dict[str, Optional[str]]] = {}
    best_fetches: dict[tuple[str, str], Fetch] = {}

    for fetch in fetches:
        if not fetch.raw_path:
            continue
        raw_path = _resolve_raw_path(fetch.raw_path, raw_dir=raw_dir)
        if raw_path is None:
            skipped_fetches += 1
            continue
        try:
            html = raw_path.read_text(encoding="utf-8")
            links = _extract_parcel_lineage_links(html)
        except Exception as exc:
            skipped_fetches += 1
            typer.echo(
                f"Skipping fetch_id={fetch.id} during parcel_lineage_links rebuild "
                f"for parcel_id={parcel_id} ({type(exc).__name__}: {exc})",
                err=True,
            )
            continue

        processed_fetches += 1
        candidate_rank_key = _lineage_fetch_rank_key(fetch)
        for link in links:
            related_parcel_id = link.get("related_parcel_id")
            relationship_type = link.get("relationship_type")
            if related_parcel_id is None or relationship_type is None:
                continue
            identity = (related_parcel_id, relationship_type)
            existing_fetch = best_fetches.get(identity)
            if (
                existing_fetch is not None
                and _lineage_fetch_rank_key(existing_fetch) > candidate_rank_key
            ):
                continue
            best_links[identity] = link
            best_fetches[identity] = fetch

    if processed_fetches == 0:
        return _ParcelLineageLinkRebuildResult(
            eligible_fetches_scanned=len(fetches),
            rows_deleted=0,
            rows_written=0,
            skipped_fetches=skipped_fetches,
            parcel_failed=True,
            error_message=(
                "Failed to rebuild parcel lineage links for "
                f"parcel_id={parcel_id} (no usable fetch HTML)"
            ),
        )

    deleted_result = session.execute(
        delete(ParcelLineageLink).where(ParcelLineageLink.parcel_id == parcel_id)
    )
    rows_deleted = deleted_result.rowcount or 0

    rows_written = 0
    for related_parcel_id, relationship_type in sorted(best_links):
        link = best_links[(related_parcel_id, relationship_type)]
        source_fetch = best_fetches[(related_parcel_id, relationship_type)]
        session.add(
            ParcelLineageLink(
                parcel_id=parcel_id,
                related_parcel_id=related_parcel_id,
                relationship_type=relationship_type,
                source_fetch_id=source_fetch.id,
                related_parcel_status=link.get("related_parcel_status"),
                relationship_note=link.get("relationship_note"),
                built_at=datetime.now(timezone.utc),
            )
        )
        rows_written += 1

    return _ParcelLineageLinkRebuildResult(
        eligible_fetches_scanned=len(fetches),
        rows_deleted=rows_deleted,
        rows_written=rows_written,
        skipped_fetches=skipped_fetches,
    )


@app.command("check-data-quality")
def check_data_quality_cmd(
    ids_file: Optional[Path] = typer.Option(None, "--ids", help="File with parcel IDs"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output JSON path"),
    fail_on_issues: bool = typer.Option(
        False,
        "--fail-on-issues",
        help="Exit with code 1 when any data-quality issue is found",
    ),
) -> None:
    settings = load_settings()
    parcel_ids = _collect_ids([], ids_file) if ids_file else None
    with session_scope(settings.database_url) as session:
        report = run_data_quality_checks(session, parcel_ids=parcel_ids)
    payload = quality_report_to_dict(report)
    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        typer.echo(json.dumps(payload, indent=2))
    if fail_on_issues and not report.passed:
        raise typer.Exit(code=1)


@app.command("profile-data")
def profile_data_cmd(
    ids_file: Optional[Path] = typer.Option(None, "--ids", help="File with parcel IDs"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output JSON path"),
) -> None:
    settings = load_settings()
    parcel_ids = _collect_ids([], ids_file) if ids_file else None
    with session_scope(settings.database_url) as session:
        payload = build_data_profile(session, parcel_ids=parcel_ids)
    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        typer.echo(json.dumps(payload, indent=2))


def _collect_ids(ids: Iterable[str], ids_file: Optional[Path]) -> list[str]:
    collected: list[str] = []
    if ids:
        collected.extend([parcel_id.strip() for parcel_id in ids if parcel_id.strip()])
    if ids_file:
        lines = ids_file.read_text(encoding="utf-8").splitlines()
        collected.extend([line.strip() for line in lines if line.strip()])
    return collected


def _collect_transaction_ids(
    ids_file: Path,
    *,
    param_hint: str,
) -> list[int]:
    transaction_ids: list[int] = []
    lines = ids_file.read_text(encoding="utf-8").splitlines()
    for line_number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            transaction_ids.append(int(stripped))
        except ValueError as exc:
            raise typer.BadParameter(
                f"Invalid transaction ID at line {line_number}: {stripped!r}",
                param_hint=param_hint,
            ) from exc
    return transaction_ids


def _ensure_parcel(session, parcel_id: str) -> None:
    with session.no_autoflush:
        existing = session.get(Parcel, parcel_id)
        if existing:
            return
        session.add(Parcel(id=parcel_id))
    session.flush()


def _parse_sections(value: str) -> list[int]:
    if not value:
        return []
    sections: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_str, end_str = part.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            if start > end:
                start, end = end, start
            sections.extend(list(range(start, end + 1)))
        else:
            sections.append(int(part))
    return sections


def _parse_list(value: str) -> list[str]:
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items


def _enumerate_trs_rows(
    trs: str,
    sections: str,
    split: str,
    split_quarters: bool,
    split_parts: str,
    split_quarter_quarters: bool,
    quarter_quarter_parts: str,
    cap: int,
) -> list[dict[str, str]]:
    section_list = _parse_sections(sections)
    split_sections = (
        section_list if split_quarters else (_parse_sections(split) if split else [])
    )
    parts = [part.strip() for part in split_parts.split(",") if part.strip()]
    split_map = {section: parts for section in split_sections}

    blocks = enumerate_trs(trs, section_list, split_sections=split_map)
    rows = [block.to_row() for block in blocks]
    if split_quarter_quarters:
        qq_parts = _parse_list(quarter_quarter_parts) if quarter_quarter_parts else []
        expanded: list[dict[str, str]] = []
        for row in rows:
            quarter = row.get("quarter") or row.get("subsection") or ""
            if quarter and qq_parts:
                for part in qq_parts:
                    new_row = dict(row)
                    new_row["quarter_quarter"] = part
                    expanded.append(new_row)
            else:
                expanded.append(row)
        rows = expanded
    if cap and cap > 0:
        rows = rows[:cap]
    return rows


def _search_trs_rows(
    rows: Sequence[Mapping[str, object]],
    municipality_id: int,
    auto_split: bool,
    quarter_quarter_parts: str,
    quarter_quarters: str,
    cap: int,
) -> list[str]:
    settings = load_settings()
    auto_parts = _parse_list(quarter_quarter_parts) if quarter_quarter_parts else []
    default_quarter_quarters = _parse_list(quarter_quarters) if quarter_quarters else []
    collected: list[str] = []
    seen: set[str] = set()

    for row in rows:
        row_municipality = _clean(row.get("municipality_id"))
        muni_id = int(row_municipality) if row_municipality else municipality_id
        township = _parse_optional_int(_clean(row.get("township")))
        range_ = _parse_optional_int(_clean(row.get("range")))
        if township is None or range_ is None:
            trs_code = _clean(row.get("trs_code"))
            if trs_code:
                township, range_ = parse_trs_code(trs_code)
            else:
                raise typer.BadParameter(
                    "Rows must include township/range or trs_code."
                )
        section = _parse_optional_int(_clean(row.get("section")))
        quarter = _clean(row.get("quarter"))
        quarter_quarter = _clean(row.get("quarter_quarter"))

        if quarter and quarter_quarter is None and default_quarter_quarters:
            quarter_quarter_list: Sequence[Optional[str]] = default_quarter_quarters
        else:
            quarter_quarter_list = [quarter_quarter]

        for qq in quarter_quarter_list:
            result = search_trs(
                settings=settings,
                municipality_id=muni_id,
                township=township,
                range_=range_,
                section=section,
                quarter=quarter,
                quarter_quarter=qq,
            )
            if result.truncated and auto_split and qq is None:
                for auto_part in auto_parts:
                    sub_result = search_trs(
                        settings=settings,
                        municipality_id=muni_id,
                        township=township,
                        range_=range_,
                        section=section,
                        quarter=quarter,
                        quarter_quarter=auto_part,
                    )
                    if sub_result.truncated:
                        typer.echo(
                            f"Warning: results truncated for {sub_result.url}",
                            err=True,
                        )
                    for parcel_id in sub_result.parcel_ids:
                        if parcel_id not in seen:
                            seen.add(parcel_id)
                            collected.append(parcel_id)
                            if cap and len(collected) >= cap:
                                break
                    if cap and len(collected) >= cap:
                        break
                continue
            if result.truncated:
                typer.echo(
                    f"Warning: results truncated for {result.url}",
                    err=True,
                )
            for parcel_id in result.parcel_ids:
                if parcel_id not in seen:
                    seen.add(parcel_id)
                    collected.append(parcel_id)
                    if cap and len(collected) >= cap:
                        break
            if cap and len(collected) >= cap:
                break
        if cap and len(collected) >= cap:
            break
    return collected


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_lines(path: Path, items: list[str]) -> None:
    path.write_text("\n".join(items) + ("\n" if items else ""), encoding="utf-8")


def _read_trs_csv(path: Path) -> list[dict[str, object]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, object]] = []
        for row in reader:
            township = _parse_optional_int(row.get("township") or row.get("Township"))
            range_ = _parse_optional_int(row.get("range") or row.get("Range"))
            if township is None or range_ is None:
                trs_code = _clean(
                    row.get("trs_code") or row.get("TRS") or row.get("trs")
                )
                if trs_code:
                    township, range_ = parse_trs_code(trs_code)
                if township is None or range_ is None:
                    raise typer.BadParameter(
                        "CSV must include township/range or trs_code columns."
                    )
            trs_code = _clean(row.get("trs_code") or row.get("TRS") or row.get("trs"))
            section = _parse_optional_int(row.get("section") or row.get("Section"))
            quarter = _clean(
                row.get("subsection")
                or row.get("quarter")
                or row.get("Quarter")
                or row.get("Subsection")
            )
            quarter_quarter = _clean(
                row.get("quarter_quarter")
                or row.get("QuarterQuarter")
                or row.get("quarterquarter")
            )
            rows.append(
                {
                    "township": township,
                    "range": range_,
                    "section": section,
                    "quarter": quarter,
                    "quarter_quarter": quarter_quarter,
                    "municipality_id": _clean(
                        row.get("municipality_id") or row.get("MunicipalityId")
                    ),
                }
            )
        return rows


def _parse_optional_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    return int(value)


def _clean(value: object) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    return value or None


def _store_parsed(
    session,
    fetch: Fetch,
    parsed: ParsedPage,
    *,
    raw_html: Optional[str] = None,
) -> None:
    summary = parsed.parcel_summary or {}
    if summary:
        existing = (
            session.execute(
                select(ParcelSummary).where(ParcelSummary.parcel_id == fetch.parcel_id)
            )
            .scalars()
            .all()
        )
        target = existing[0] if existing else ParcelSummary(parcel_id=fetch.parcel_id)
        target.fetch_id = fetch.id
        target.municipality_name = _get_summary_value(summary, "Municipality Name")
        target.parcel_description = _get_summary_value(summary, "Parcel Description")
        target.owner_name = _get_summary_value(summary, "Owner Name", "Owner Names")
        target.primary_address = _get_summary_value(summary, "Primary Address")
        target.billing_address = _get_summary_value(summary, "Billing Address")
        session.add(target)
        for extra in existing[1:]:
            session.delete(extra)

    for record in parsed.assessment:
        fields = _extract_assessment_fields(record)
        session.add(
            AssessmentRecord(
                parcel_id=fetch.parcel_id,
                fetch_id=fetch.id,
                year=_extract_year(record),
                valuation_classification=fields.get("valuation_classification"),
                assessment_acres=fields.get("assessment_acres"),
                land_value=fields.get("land_value"),
                improved_value=fields.get("improved_value"),
                total_value=fields.get("total_value"),
                average_assessment_ratio=fields.get("average_assessment_ratio"),
                estimated_fair_market_value=fields.get("estimated_fair_market_value"),
                valuation_date=fields.get("valuation_date"),
                data=record,
            )
        )
    for record in parsed.tax:
        session.add(
            TaxRecord(
                parcel_id=fetch.parcel_id,
                fetch_id=fetch.id,
                year=_extract_year(record),
                data=record,
            )
        )
    for record in parsed.payments:
        session.add(
            PaymentRecord(
                parcel_id=fetch.parcel_id,
                fetch_id=fetch.id,
                year=_extract_year(record),
                data=record,
            )
        )
    _upsert_parcel_lineage_links(session, fetch, raw_html=raw_html)
    _upsert_parcel_characteristic(session, fetch, parsed, raw_html=raw_html)


def _upsert_parcel_lineage_links(
    session,
    fetch: Fetch,
    *,
    raw_html: Optional[str] = None,
) -> None:
    html = raw_html
    if html is None:
        if not fetch.raw_path:
            return
        html = Path(fetch.raw_path).read_text(encoding="utf-8")

    links = _extract_parcel_lineage_links(html)
    if not links:
        return

    candidate_rank_key = _lineage_fetch_rank_key(fetch)
    for link in links:
        identity = (
            fetch.parcel_id,
            link["related_parcel_id"],
            link["relationship_type"],
        )
        existing = session.get(ParcelLineageLink, identity)
        if existing is not None and existing.source_fetch_id != fetch.id:
            existing_fetch = (
                session.get(Fetch, existing.source_fetch_id)
                if existing.source_fetch_id is not None
                else None
            )
            if (
                _lineage_fetch_rank_key(existing_fetch, existing.source_fetch_id)
                > candidate_rank_key
            ):
                continue

        target = existing or ParcelLineageLink(
            parcel_id=fetch.parcel_id,
            related_parcel_id=link["related_parcel_id"],
            relationship_type=link["relationship_type"],
        )
        target.source_fetch_id = fetch.id
        target.related_parcel_status = link["related_parcel_status"]
        target.relationship_note = link["relationship_note"]
        target.built_at = datetime.now(timezone.utc)
        session.add(target)


def _extract_parcel_lineage_links(html: str) -> list[dict[str, Optional[str]]]:
    soup = BeautifulSoup(html, "lxml")
    deduped: dict[tuple[str, str], dict[str, Optional[str]]] = {}
    for relationship_type, modal_id in LINEAGE_MODAL_BY_RELATIONSHIP_TYPE:
        modal = soup.find(id=modal_id)
        if not modal:
            continue
        for history in modal.select(".parcelhistory"):
            link = _parse_parcel_history_entry(history, relationship_type)
            if link is None:
                continue
            related_parcel_id = link["related_parcel_id"]
            relationship_name = link["relationship_type"]
            if related_parcel_id is None or relationship_name is None:
                continue
            key = (related_parcel_id, relationship_name)
            deduped[key] = link
    return list(deduped.values())


def _parse_parcel_history_entry(
    history,
    relationship_type: str,
) -> Optional[dict[str, Optional[str]]]:
    sections = history.find_all("div", recursive=False)
    if not sections:
        return None

    header = sections[0]
    anchor = header.find("a", href=True)
    if anchor is None:
        return None

    related_parcel_id = _extract_related_parcel_id(anchor)
    if related_parcel_id is None:
        return None

    badge = header.select_one(".badge")
    related_parcel_status = _clean_characteristic_text(
        badge.get_text(" ", strip=True) if badge else None
    )
    relationship_note = None
    if len(sections) > 1:
        relationship_note = _clean_characteristic_text(
            sections[1].get_text(" ", strip=True)
        )

    return {
        "related_parcel_id": related_parcel_id,
        "relationship_type": relationship_type,
        "related_parcel_status": related_parcel_status,
        "relationship_note": relationship_note,
    }


def _extract_related_parcel_id(anchor) -> Optional[str]:
    href = html_attr_text(anchor.get("href"))
    match = re.search(r"/(\d{10,14})\b", href)
    if match:
        return match.group(1)
    text = _clean_characteristic_text(anchor.get_text(" ", strip=True))
    if text and text.isdigit():
        return text
    return None


def _lineage_fetch_rank_key(
    fetch: Optional[Fetch],
    source_fetch_id: Optional[int] = None,
) -> tuple[datetime, int]:
    fetched_at = datetime.min.replace(tzinfo=timezone.utc)
    if fetch is not None and fetch.fetched_at is not None:
        fetched_at = fetch.fetched_at
    if source_fetch_id is None and fetch is not None:
        source_fetch_id = fetch.id
    return fetched_at, source_fetch_id or 0


def _upsert_parcel_characteristic(
    session,
    fetch: Fetch,
    parsed: ParsedPage,
    *,
    raw_html: Optional[str] = None,
) -> None:
    candidate = _build_parcel_characteristic_candidate(
        fetch,
        parsed,
        raw_html=raw_html,
    )
    if candidate is None:
        return

    existing = session.get(ParcelCharacteristic, fetch.parcel_id)
    if existing is not None and existing.source_fetch_id != fetch.id:
        existing_fetch = (
            session.get(Fetch, existing.source_fetch_id)
            if existing.source_fetch_id is not None
            else None
        )
        if (
            _parcel_characteristic_rank_key(existing, existing_fetch)
            >= candidate.rank_key
        ):
            return

    target = existing or ParcelCharacteristic(parcel_id=fetch.parcel_id)
    for field_name, value in candidate.fields.items():
        setattr(target, field_name, value)
    target.source_fetch_id = fetch.id
    target.built_at = datetime.now(timezone.utc)
    session.add(target)


def _build_parcel_characteristic_candidate(
    fetch: Fetch,
    parsed: ParsedPage,
    *,
    raw_html: Optional[str] = None,
) -> Optional[ParcelCharacteristicCandidate]:
    html = raw_html
    if html is None:
        if not fetch.raw_path:
            return None
        html = Path(fetch.raw_path).read_text(encoding="utf-8")

    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text(" ", strip=True)
    detail_grid = _extract_parcel_detail_grid(soup)
    current_assessment = _select_current_characteristic_assessment(parsed.assessment)

    current_assessment_year: Optional[int] = None
    current_valuation_classification: Optional[str] = None
    current_assessment_acres: Optional[Decimal] = None
    current_assessment_ratio: Optional[Decimal] = None
    current_estimated_fair_market_value: Optional[Decimal] = None
    if current_assessment is not None:
        current_assessment_year = _extract_year(current_assessment)
        assessment_fields = _extract_assessment_fields(current_assessment)
        current_valuation_classification = _clean_characteristic_text(
            assessment_fields["valuation_classification"]
        )
        current_assessment_acres = assessment_fields["assessment_acres"]
        current_assessment_ratio = assessment_fields["average_assessment_ratio"]
        current_estimated_fair_market_value = assessment_fields[
            "estimated_fair_market_value"
        ]

    formatted_parcel_number = _clean_characteristic_text(
        parsed.other.get("Parcel Number")
    )
    derived_parcel_components = _parse_formatted_parcel_number_components(
        formatted_parcel_number
    )
    state_municipality_code = (
        _clean_characteristic_text(parsed.other.get("State Municipality Code"))
        or derived_parcel_components["state_municipality_code"]
    )
    township_range_value = _first_nonempty_characteristic_text(
        detail_grid.get("Township & Range"),
        detail_grid.get("Township and Range"),
        parsed.other.get("Township & Range"),
        parsed.other.get("Township and Range"),
        parsed.other.get("Township/Range"),
    )
    township, range_ = _parse_township_range(township_range_value)
    if township is None:
        township = derived_parcel_components["township"]
    if range_ is None:
        range_ = derived_parcel_components["range"]
    section = _first_nonempty_characteristic_text(
        detail_grid.get("Section"),
        parsed.other.get("Section"),
        derived_parcel_components["section"],
    )
    quarter_quarter = _first_nonempty_characteristic_text(
        detail_grid.get("Quarter/Quarter & Quarter"),
        detail_grid.get("Quarter/Quarter"),
        parsed.other.get("Quarter/Quarter & Quarter"),
        parsed.other.get("Quarter/Quarter"),
        _extract_quarter_quarter_from_parcel_description(
            parsed.parcel_summary.get("Parcel Description")
        ),
    )

    has_dcimap_link = _has_named_link(soup, "DCiMap")
    has_google_map_link = _has_named_link(soup, "Google Map")
    has_bing_map_link = _has_named_link(soup, "Bing Map")

    has_empty_tax_section = _detect_empty_tax_section(page_text)
    current_tax_info_available = _detect_tax_info_available(parsed, page_text)
    current_payment_history_available = _detect_payment_history_available(
        parsed, page_text
    )
    tax_jurisdiction_count = _count_tax_jurisdictions(soup)

    has_empty_valuation_breakout = _detect_empty_valuation_breakout(soup)
    owner_name = _get_summary_value(
        parsed.parcel_summary or {},
        "Owner Name",
        "Owner Names",
    )
    is_exempt_style_page = _detect_exempt_style_page(
        current_valuation_classification=current_valuation_classification,
        owner_name=owner_name,
        has_empty_valuation_breakout=has_empty_valuation_breakout,
        current_tax_info_available=current_tax_info_available,
    )

    fields: ParcelCharacteristicFields = {
        "formatted_parcel_number": formatted_parcel_number,
        "state_municipality_code": state_municipality_code,
        "township": township,
        "range": range_,
        "section": section,
        "quarter_quarter": quarter_quarter,
        "has_dcimap_link": has_dcimap_link,
        "has_google_map_link": has_google_map_link,
        "has_bing_map_link": has_bing_map_link,
        "current_assessment_year": current_assessment_year,
        "current_valuation_classification": current_valuation_classification,
        "current_assessment_acres": current_assessment_acres,
        "current_assessment_ratio": current_assessment_ratio,
        "current_estimated_fair_market_value": current_estimated_fair_market_value,
        "current_tax_info_available": current_tax_info_available,
        "current_payment_history_available": current_payment_history_available,
        "tax_jurisdiction_count": tax_jurisdiction_count,
        "is_exempt_style_page": is_exempt_style_page,
        "has_empty_valuation_breakout": has_empty_valuation_breakout,
        "has_empty_tax_section": has_empty_tax_section,
    }

    if not any(value is not None for value in fields.values()):
        return None

    rank_key = (
        _parcel_characteristic_completeness(fields),
        fetch.fetched_at or datetime.min.replace(tzinfo=timezone.utc),
        fetch.id,
    )
    return ParcelCharacteristicCandidate(fields=fields, rank_key=rank_key)


def _parcel_characteristic_rank_key(
    row: ParcelCharacteristic,
    source_fetch: Optional[Fetch],
) -> tuple[int, datetime, int]:
    return (
        _parcel_characteristic_completeness(
            {
                "formatted_parcel_number": row.formatted_parcel_number,
                "state_municipality_code": row.state_municipality_code,
                "township": row.township,
                "range": row.range,
                "section": row.section,
                "quarter_quarter": row.quarter_quarter,
                "has_dcimap_link": row.has_dcimap_link,
                "has_google_map_link": row.has_google_map_link,
                "has_bing_map_link": row.has_bing_map_link,
                "current_assessment_year": row.current_assessment_year,
                "current_valuation_classification": (
                    row.current_valuation_classification
                ),
                "current_assessment_acres": row.current_assessment_acres,
                "current_assessment_ratio": row.current_assessment_ratio,
                "current_estimated_fair_market_value": (
                    row.current_estimated_fair_market_value
                ),
                "current_tax_info_available": row.current_tax_info_available,
                "current_payment_history_available": (
                    row.current_payment_history_available
                ),
                "tax_jurisdiction_count": row.tax_jurisdiction_count,
                "is_exempt_style_page": row.is_exempt_style_page,
                "has_empty_valuation_breakout": row.has_empty_valuation_breakout,
                "has_empty_tax_section": row.has_empty_tax_section,
            }
        ),
        (
            source_fetch.fetched_at
            if source_fetch and source_fetch.fetched_at
            else datetime.min.replace(tzinfo=timezone.utc)
        ),
        row.source_fetch_id or 0,
    )


def _parcel_characteristic_completeness(fields: Mapping[str, object]) -> int:
    score = 0
    for key in (
        "formatted_parcel_number",
        "state_municipality_code",
        "township",
        "range",
        "section",
        "quarter_quarter",
        "current_assessment_year",
        "current_valuation_classification",
        "current_assessment_acres",
        "current_assessment_ratio",
        "current_estimated_fair_market_value",
        "tax_jurisdiction_count",
    ):
        if fields.get(key) is not None:
            score += 1
    if any(
        fields.get(key) is True
        for key in ("has_dcimap_link", "has_google_map_link", "has_bing_map_link")
    ):
        score += 1
    if fields.get("current_tax_info_available") is not None:
        score += 1
    if fields.get("current_payment_history_available") is not None:
        score += 1
    if fields.get("has_empty_valuation_breakout") is not None:
        score += 1
    if fields.get("has_empty_tax_section") is not None:
        score += 1
    return score


def _select_current_characteristic_assessment(
    records: list[dict[str, object]],
) -> Optional[dict[str, object]]:
    candidates: list[tuple[int, int, int, dict[str, object]]] = []
    for record in records:
        if str(record.get("source") or "") == "valuation_breakout":
            continue
        year = _extract_year(record) or 0
        if year <= 0:
            continue
        source_priority = 2 if str(record.get("source") or "") == "detail" else 1
        fields = _extract_assessment_fields(record)
        completeness = sum(value is not None for value in fields.values())
        candidates.append((year, source_priority, completeness, record))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1], item[2]))
    return candidates[-1][3]


def _extract_parcel_detail_grid(soup: BeautifulSoup) -> dict[str, str]:
    grid: dict[str, str] = {}
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for idx, row in enumerate(rows):
            cells = row.find_all(["th", "td"])
            labels = [
                _clean_characteristic_text(cell.get_text(" ", strip=True))
                for cell in cells
            ]
            if labels in (
                [
                    "Township & Range",
                    "Section",
                    "Quarter/Quarter & Quarter",
                ],
                [
                    "Township and Range",
                    "Section",
                    "Quarter/Quarter",
                ],
            ) and idx + 1 < len(rows):
                value_cells = rows[idx + 1].find_all(["th", "td"])
                values = [
                    _clean_characteristic_text(cell.get_text(" ", strip=True))
                    for cell in value_cells
                ]
                if len(values) == 3:
                    grid[labels[0] or "Township & Range"] = values[0] or ""
                    grid["Section"] = values[1] or ""
                    grid[labels[2] or "Quarter/Quarter & Quarter"] = values[2] or ""
                    return grid
    return grid


def _parse_township_range(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not value:
        return None, None
    normalized = re.sub(r"[^0-9NSEW]", "", value.strip().upper())
    match = re.search(r"T(\d+)[NS]R(\d+)[EW]", normalized)
    if not match:
        return None, None
    return match.group(1).zfill(2), match.group(2).zfill(2)


def _has_named_link(soup: BeautifulSoup, label: str) -> Optional[bool]:
    href_tokens = {
        "DCiMap": ("dcimapapps.danecounty.gov", "dcmapviewer"),
        "Google Map": ("maps.google.com", "google.com/maps"),
        "Bing Map": ("bing.com/maps",),
    }
    for anchor in soup.find_all("a"):
        text = " ".join(anchor.get_text(" ", strip=True).split())
        if text == label:
            return True
        href = html_attr_text(anchor.get("href")).lower()
        if any(token in href for token in href_tokens.get(label, ())):
            return True
    return False


def _parse_formatted_parcel_number_components(
    formatted_parcel_number: Optional[str],
) -> dict[str, Optional[str]]:
    if not formatted_parcel_number:
        return {
            "state_municipality_code": None,
            "township": None,
            "range": None,
            "section": None,
        }
    match = re.search(
        r"^(?P<state>\d{3})/(?P<township>\d{2})(?P<range>\d{2})-(?P<section>\d{2})\d-",
        formatted_parcel_number.strip(),
    )
    if not match:
        return {
            "state_municipality_code": None,
            "township": None,
            "range": None,
            "section": None,
        }
    return {
        "state_municipality_code": match.group("state"),
        "township": match.group("township"),
        "range": match.group("range"),
        "section": match.group("section"),
    }


def _first_nonempty_characteristic_text(*values: Optional[str]) -> Optional[str]:
    for value in values:
        cleaned = _clean_characteristic_text(value)
        if cleaned is not None:
            return cleaned
    return None


def _extract_quarter_quarter_from_parcel_description(
    parcel_description: Optional[str],
) -> Optional[str]:
    if not parcel_description:
        return None
    match = re.search(
        r"\b(?P<quarter_quarter>NE|NW|SE|SW)\s*1/4\s*(?P<quarter>NE|NW|SE|SW)\s*1/4\b",
        parcel_description.upper(),
    )
    if not match:
        return None
    return f"{match.group('quarter_quarter')} of the {match.group('quarter')}"


def _detect_tax_info_available(parsed, page_text: str) -> Optional[bool]:
    if "No tax information available." in page_text:
        return False
    if parsed.tax:
        return True
    if "Tax Information" in page_text:
        return True
    return None


def _detect_payment_history_available(parsed, page_text: str) -> Optional[bool]:
    if "No historic payments found." in page_text:
        return False
    summary_rows = [
        row
        for row in parsed.payments
        if str(row.get("source") or "") != "tax_detail_payments"
    ]
    if summary_rows:
        if any(not _payment_row_is_placeholder(row) for row in summary_rows):
            return True
        if any(_payment_row_is_placeholder(row) for row in summary_rows):
            return False
        return True
    if "Tax Payment History" in page_text:
        return None
    return None


def _payment_row_is_placeholder(row: dict[str, object]) -> bool:
    return is_placeholder_payment_row(row)


def _count_tax_jurisdictions(soup: BeautifulSoup) -> Optional[int]:
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [
            _clean_characteristic_text(cell.get_text(" ", strip=True))
            for cell in rows[0].find_all(["th", "td"])
        ]
        if headers == ["Type", "State Code", "Description"]:
            count = 0
            for row in rows[1:]:
                cells = row.find_all(["th", "td"])
                if not cells:
                    continue
                values = [
                    _clean_characteristic_text(cell.get_text(" ", strip=True))
                    for cell in cells
                ]
                if any(values):
                    count += 1
            return count
    return None


def _detect_empty_valuation_breakout(soup: BeautifulSoup) -> Optional[bool]:
    table = soup.select_one("#ValuationBreakout table.valuationTable")
    if not table:
        return None
    rows = table.find_all("tr")
    if len(rows) <= 1:
        return True
    for row in rows[1:]:
        cells = row.find_all(["th", "td"])
        values = [
            _clean_characteristic_text(cell.get_text(" ", strip=True)) for cell in cells
        ]
        if any(values):
            return False
    return True


def _detect_empty_tax_section(page_text: str) -> Optional[bool]:
    if "No tax information available." in page_text:
        return True
    if "Tax Information" in page_text:
        return False
    return None


def _detect_exempt_style_page(
    *,
    current_valuation_classification: Optional[str],
    owner_name: Optional[str],
    has_empty_valuation_breakout: Optional[bool],
    current_tax_info_available: Optional[bool],
) -> Optional[bool]:
    classification = (current_valuation_classification or "").upper()
    if classification.startswith("X"):
        return True
    owner_upper = (owner_name or "").upper()
    if (
        has_empty_valuation_breakout is True
        and current_tax_info_available is False
        and any(
            token in owner_upper
            for token in (
                "VILLAGE OF",
                "CITY OF",
                "TOWN OF",
                "COUNTY OF",
                "STATE OF",
                "SCHOOL DISTRICT",
                "SCHOOL DIST",
                "WI DOT",
            )
        )
    ):
        return True
    if current_valuation_classification is not None:
        return False
    if owner_name is not None and has_empty_valuation_breakout is not None:
        return False
    return None


def _delete_parsed(session, fetch_id: int, parcel_id: str) -> None:
    session.execute(
        delete(AssessmentRecord).where(AssessmentRecord.fetch_id == fetch_id)
    )
    session.execute(delete(TaxRecord).where(TaxRecord.fetch_id == fetch_id))
    session.execute(delete(PaymentRecord).where(PaymentRecord.fetch_id == fetch_id))
    session.execute(delete(ParcelSummary).where(ParcelSummary.parcel_id == parcel_id))


def _extract_year(record: Mapping[str, object]) -> Optional[int]:
    year_value = record.get("year")
    year = _parse_year_value(year_value)
    if year:
        return year
    for key, value in record.items():
        key_lower = str(key).lower()
        if "year" in key_lower:
            year = _parse_year_value(value)
            if year:
                return year
    for value in record.values():
        year = _parse_year_value(value)
        if year:
            return year
    return None


def _get_summary_value(summary: Mapping[str, object], *keys: str) -> Optional[str]:
    normalized = {}
    for key, value in summary.items():
        if isinstance(key, str):
            normalized[_normalize_key(key)] = value
    for key in keys:
        value = normalized.get(_normalize_key(key))
        cleaned = _clean_text(value)
        if cleaned is not None:
            return cleaned
    return None


def _parse_year_value(value: object) -> Optional[int]:
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        return None
    value = value.strip()
    match = re.search(r"\b(\d{4})\b", value)
    if match:
        return int(match.group(1))
    return None


def _extract_assessment_fields(record: Mapping[str, object]) -> AssessmentFields:
    normalized = {}
    for key, value in record.items():
        if isinstance(key, str):
            normalized[_normalize_key(key)] = value

    def get_value(*keys: str) -> object:
        for key in keys:
            value = normalized.get(_normalize_key(key))
            if value is not None:
                return value
        return None

    valuation_classification = get_value("Valuation Classification")
    assessment_acres = _parse_decimal(get_value("Assessment Acres"), 3)
    land_value = _parse_money(get_value("Land Value", "Oand Value"))
    improved_value = _parse_money(get_value("Improved Value"))
    total_value = _parse_money(get_value("Total Value"))
    average_assessment_ratio = _parse_decimal(get_value("Average Assessment Ratio"), 4)
    estimated_fair_market_value = _parse_money(
        get_value("Estimated Fair Market Value", "Estimate Fair Market Value")
    )
    valuation_date = _parse_date(get_value("Valuation Date"))

    return {
        "valuation_classification": _clean_text(valuation_classification),
        "assessment_acres": assessment_acres,
        "land_value": land_value,
        "improved_value": improved_value,
        "total_value": total_value,
        "average_assessment_ratio": average_assessment_ratio,
        "estimated_fair_market_value": estimated_fair_market_value,
        "valuation_date": valuation_date,
    }


def _normalize_key(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _clean_text(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _clean_characteristic_text(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value or value.upper() in {"N/A", "NA"}:
        return None
    return value


def _parse_money(value: object) -> Optional[Decimal]:
    return _parse_decimal(value, 2)


def _resolve_raw_path(raw_path_value: str, *, raw_dir: Path) -> Optional[Path]:
    raw_path = Path(raw_path_value)
    candidates = (
        [raw_path] if raw_path.is_absolute() else [raw_path, raw_dir / raw_path]
    )
    for candidate_path in candidates:
        try:
            candidate = candidate_path.resolve()
            candidate.relative_to(raw_dir)
        except (OSError, ValueError):
            continue
        if candidate.exists():
            return candidate
    return None


def _parse_decimal(value: object, scale: int) -> Optional[Decimal]:
    if isinstance(value, (int, float, Decimal)):
        try:
            dec = Decimal(str(value))
        except InvalidOperation:
            return None
    elif isinstance(value, str):
        text = value.strip()
        if not text or text.upper() in {"N/A", "NA"}:
            return None
        cleaned = text.replace(",", "").replace("$", "")
        if cleaned in {"", "-", "--"}:
            return None
        try:
            dec = Decimal(cleaned)
        except InvalidOperation:
            return None
    else:
        return None

    quant = Decimal("1").scaleb(-scale)
    try:
        return dec.quantize(quant, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None


def _parse_date(value: object) -> Optional[date]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text.upper() in {"N/A", "NA"}:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None
