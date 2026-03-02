from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from accessdane_audit.parse import parse_page


@dataclass(frozen=True)
class FixtureStat:
    parcel_id: str
    assessment_count: int
    tax_count: int
    payment_count: int
    detail_payment_count: int
    summary_payment_count: int
    has_valuation_breakout: bool
    has_owner_names: bool
    has_no_address: bool
    has_no_payments_marker: bool
    summary_keys: tuple[str, ...]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scan raw AccessDane HTML fixtures and summarize parser-relevant shapes."
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=ROOT / "data" / "raw",
        help="Directory containing raw parcel HTML files.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum rows to print in each candidate section.",
    )
    parser.add_argument(
        "--sample",
        action="append",
        default=[],
        help="Parcel ID to inspect in detail (repeatable).",
    )
    parser.add_argument(
        "--sample-only",
        action="store_true",
        help="Only inspect the parcel IDs passed via --sample; skip the full corpus scan.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Maximum number of HTML files to scan from the raw corpus (0 = no cap).",
    )
    args = parser.parse_args()

    if args.sample_only:
        if not args.sample:
            print("--sample-only requires at least one --sample parcel ID.")
            return 1
        stats = scan_samples(args.raw_dir, args.sample)
    else:
        stats = scan_fixtures(args.raw_dir, max_files=args.max_files)
    if not stats:
        print(f"No HTML fixtures found under {args.raw_dir}")
        return 1

    print_summary(stats)
    print_candidates(stats, limit=max(args.limit, 1))

    for parcel_id in args.sample:
        print_sample(stats, parcel_id)

    return 0


def scan_fixtures(raw_dir: Path, *, max_files: int = 0) -> list[FixtureStat]:
    stats: list[FixtureStat] = []
    for index, path in enumerate(sorted(raw_dir.glob("*.html")), start=1):
        if max_files and index > max_files:
            break
        stats.append(_scan_path(path))
    return stats


def scan_samples(raw_dir: Path, parcel_ids: list[str]) -> list[FixtureStat]:
    stats: list[FixtureStat] = []
    for parcel_id in parcel_ids:
        path = raw_dir / f"{parcel_id}.html"
        if path.exists():
            stats.append(_scan_path(path))
    return stats


def _scan_path(path: Path) -> FixtureStat:
    html = path.read_text(encoding="utf-8")
    parsed = parse_page(html)

    detail_payment_count = sum(
        1 for row in parsed.payments if row.get("source") == "tax_detail_payments"
    )
    summary_payment_count = len(parsed.payments) - detail_payment_count
    assessment_count = len(parsed.assessment)
    tax_count = len(parsed.tax)
    payment_count = len(parsed.payments)

    return FixtureStat(
        parcel_id=path.stem,
        assessment_count=assessment_count,
        tax_count=tax_count,
        payment_count=payment_count,
        detail_payment_count=detail_payment_count,
        summary_payment_count=summary_payment_count,
        has_valuation_breakout=any(
            row.get("source") == "valuation_breakout" for row in parsed.assessment
        ),
        has_owner_names="Owner Names" in parsed.parcel_summary,
        has_no_address=(
            parsed.parcel_summary.get("Primary Address") == "No parcel address available."
        ),
        has_no_payments_marker=any(
            row.get("Date of Payment") == "No payments found." for row in parsed.payments
        ),
        summary_keys=tuple(sorted(parsed.parcel_summary)),
    )


def print_summary(stats: list[FixtureStat]) -> None:
    print(f"Total fixtures: {len(stats)}")
    print(f"With valuation breakout: {sum(item.has_valuation_breakout for item in stats)}")
    print(f"With Owner Names label: {sum(item.has_owner_names for item in stats)}")
    print(f"With no parcel address: {sum(item.has_no_address for item in stats)}")
    print(f"With 'No payments found.': {sum(item.has_no_payments_marker for item in stats)}")
    print()


def print_candidates(stats: list[FixtureStat], *, limit: int) -> None:
    sections = [
        (
            "Lowest assessment counts",
            sorted(stats, key=lambda item: (item.assessment_count, item.parcel_id))[:limit],
        ),
        (
            "Highest assessment counts",
            sorted(stats, key=lambda item: (-item.assessment_count, item.parcel_id))[:limit],
        ),
        (
            "Highest payment counts",
            sorted(stats, key=lambda item: (-item.payment_count, item.parcel_id))[:limit],
        ),
        (
            "Highest tax-detail payment counts",
            sorted(stats, key=lambda item: (-item.detail_payment_count, item.parcel_id))[:limit],
        ),
        (
            "Owner Names variants",
            [item for item in stats if item.has_owner_names][:limit],
        ),
        (
            "No-address variants",
            [item for item in stats if item.has_no_address][:limit],
        ),
        (
            "Valuation breakout variants",
            [item for item in stats if item.has_valuation_breakout][:limit],
        ),
    ]

    for title, rows in sections:
        print(title)
        if not rows:
            print("  (none)")
            print()
            continue
        for row in rows:
            print(format_row(row))
        print()


def print_sample(stats: list[FixtureStat], parcel_id: str) -> None:
    match = next((item for item in stats if item.parcel_id == parcel_id), None)
    print(f"Sample {parcel_id}")
    if match is None:
        print("  not found")
        print()
        return
    print(format_row(match))
    print(f"  summary_keys={', '.join(match.summary_keys)}")
    print()


def format_row(row: FixtureStat) -> str:
    flags: list[str] = []
    if row.has_valuation_breakout:
        flags.append("valuation_breakout")
    if row.has_owner_names:
        flags.append("owner_names")
    if row.has_no_address:
        flags.append("no_address")
    if row.has_no_payments_marker:
        flags.append("no_payments")
    flag_text = ",".join(flags) if flags else "-"
    return (
        f"  {row.parcel_id} "
        f"a={row.assessment_count} "
        f"t={row.tax_count} "
        f"p={row.payment_count} "
        f"detail_p={row.detail_payment_count} "
        f"summary_p={row.summary_payment_count} "
        f"flags={flag_text}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
