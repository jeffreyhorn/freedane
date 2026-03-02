from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, cast

from bs4 import BeautifulSoup, Tag


@dataclass
class ParsedPage:
    assessment: list[dict[str, object]]
    tax: list[dict[str, object]]
    payments: list[dict[str, object]]
    other: dict[str, Any]
    parcel_summary: dict[str, str]


def parse_page(html: str, *, include_tax_detail_payments: bool = True) -> ParsedPage:
    soup = BeautifulSoup(html, "lxml")
    if _is_accessdane_parcel(soup):
        return _parse_accessdane(
            soup, include_tax_detail_payments=include_tax_detail_payments
        )

    assessment = cast(
        list[dict[str, object]], _parse_section_tables(soup, "Assessment")
    )
    tax = cast(list[dict[str, object]], _parse_section_tables(soup, "Tax"))
    payments = cast(list[dict[str, object]], _parse_section_tables(soup, "Payment"))
    other = _extract_key_value_pairs(soup)
    return ParsedPage(
        assessment=assessment,
        tax=tax,
        payments=payments,
        other=other,
        parcel_summary={},
    )


def _is_accessdane_parcel(soup: BeautifulSoup) -> bool:
    heading = soup.select_one("#parcel_detail_heading")
    if heading:
        return True
    title = _norm(soup.title.get_text(strip=True)) if soup.title else ""
    return "Details" in title and "Parcel" in title


def _parse_accessdane(
    soup: BeautifulSoup, *, include_tax_detail_payments: bool = True
) -> ParsedPage:
    assessment: list[dict[str, object]] = []
    assessment.extend(cast(list[dict[str, object]], _parse_assessment_summary(soup)))
    assessment.extend(cast(list[dict[str, object]], _parse_assessment_detail(soup)))
    valuation = _parse_valuation_breakout(soup)
    if valuation:
        assessment.append({"source": "valuation_breakout", "rows": valuation})

    tax: list[dict[str, object]] = []
    tax.extend(_parse_tax_summary_tables(soup))
    tax_details = _parse_tax_details_modals(soup)
    if tax_details:
        tax.extend(_group_tax_details(tax_details))

    payments = _parse_tax_payments(soup)
    if include_tax_detail_payments:
        payments.extend(_parse_tax_detail_payments(soup))

    other = _extract_key_value_pairs(soup)
    parcel_summary = _parse_parcel_summary(soup)
    parcel_number = _extract_parcel_number(soup)
    if parcel_number:
        other["Parcel Number"] = parcel_number
    parcel_id = _extract_parcel_id(soup)
    if parcel_id:
        other["Parcel Id"] = parcel_id
    return ParsedPage(
        assessment=assessment,
        tax=tax,
        payments=payments,
        other=other,
        parcel_summary=parcel_summary,
    )


def _parse_assessment_summary(soup: BeautifulSoup) -> list[dict[str, str]]:
    table = soup.select_one("#assessmentSummary table")
    if not table:
        return []
    year = None
    header_cells = table.find_all("th")
    if len(header_cells) >= 2:
        year = _parse_year(header_cells[1].get_text(" ", strip=True))

    record: dict[str, str] = {"source": "summary"}
    if year:
        record["year"] = str(year)
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) != 2 or row.find_all("th"):
            continue
        key = _norm(cells[0].get_text(" ", strip=True))
        value = _norm(cells[1].get_text(" ", strip=True))
        if key:
            record[key] = value
    return [record] if len(record) > 1 else []


def _parse_assessment_detail(soup: BeautifulSoup) -> list[dict[str, str]]:
    table = soup.select_one("#assessmentDetail table")
    if not table:
        return []
    rows = table.find_all("tr")
    if not rows:
        return []
    header_cells = rows[0].find_all(["th", "td"])
    year_labels = [_norm(cell.get_text(" ", strip=True)) for cell in header_cells[1:]]
    records: list[dict[str, str]] = []
    for year_label in year_labels:
        year = _parse_year(year_label)
        record: dict[str, str] = {"source": "detail"}
        if year:
            record["year"] = str(year)
        records.append(record)

    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        field = _norm(cells[0].get_text(" ", strip=True))
        if not field:
            continue
        for idx, cell in enumerate(cells[1:]):
            if idx >= len(records):
                break
            value = _norm(cell.get_text(" ", strip=True))
            if value:
                records[idx][field] = value
    return [record for record in records if len(record) > 2]


def _parse_valuation_breakout(soup: BeautifulSoup) -> list[dict[str, str]]:
    table = soup.select_one("#ValuationBreakout table.valuationTable")
    if not table:
        return []
    rows = table.find_all("tr")
    if not rows:
        return []
    headers = [
        _norm(cell.get_text(" ", strip=True)) for cell in rows[0].find_all(["th", "td"])
    ]
    records: list[dict[str, str]] = []
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        values = [_norm(cell.get_text(" ", strip=True)) for cell in cells]
        record: dict[str, str]
        if headers and len(values) == len(headers):
            record = {
                headers[i] or f"col_{i+1}": values[i] for i in range(len(headers))
            }
        else:
            record = {f"col_{i+1}": values[i] for i in range(len(values))}
        if any(record.values()):
            records.append(record)
    return records


def _parse_tax_summary_tables(soup: BeautifulSoup) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for table in soup.select(".taxDetailTable table[data-tableyear]"):
        year_label = _attr_text(table.get("data-tableyear"))
        record: dict[str, object] = {"source": "summary"}
        year = _parse_year(year_label)
        if year:
            record["year"] = str(year)

        header_labels: list[str] = []
        expect_values = False
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            if row.find_all("th") and len(cells) == 3:
                header_labels = [
                    _norm(cell.get_text(" ", strip=True)) for cell in cells
                ]
                expect_values = True
                continue
            if expect_values and len(cells) == 3 and not row.find_all("th"):
                values = [_norm(cell.get_text(" ", strip=True)) for cell in cells]
                for idx, label in enumerate(header_labels):
                    if label:
                        record[label] = values[idx]
                expect_values = False
                continue

            if len(cells) >= 2:
                key = _norm(cells[0].get_text(" ", strip=True)).rstrip(":").strip()
                value = _norm(cells[-1].get_text(" ", strip=True))
                if key and value:
                    record[key] = value
        if len(record) > 2:
            records.append(record)
    return records


def _parse_tax_details_modals(soup: BeautifulSoup) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for modal in soup.select("div[id^='TaxDetails']"):
        table = modal.select_one("table.taxInfoTable")
        if not table:
            continue
        year = _parse_year(_attr_text(modal.get("id", "")))
        section = ""
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            if len(cells) == 1 and cells[0].get("colspan"):
                section = _norm(cells[0].get_text(" ", strip=True))
                continue
            values = [_norm(cell.get_text(" ", strip=True)) for cell in cells]
            row_classes = _class_names(row.get("class"))
            record: dict[str, str] = {
                "section": section,
                "row_type": (
                    "header"
                    if ("rowTitle" in row_classes or row.find_all("th"))
                    else "data"
                ),
            }
            if year:
                record["year"] = str(year)
            for idx, value in enumerate(values):
                record[f"col_{idx+1}"] = value
            if any(value for value in values):
                records.append(record)
    return records


def _group_tax_details(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        year = row.get("year") or "unknown"
        grouped.setdefault(year, []).append(row)
    records: list[dict[str, object]] = []
    for year, items in grouped.items():
        record: dict[str, object] = {"source": "detail"}
        if year != "unknown":
            record["year"] = year
        record["rows"] = items
        record.update(_build_tax_detail_structures(items))
        records.append(record)
    return records


def _build_tax_detail_structures(
    rows: list[dict[str, str]],
) -> dict[str, object]:
    tax_value_rows: list[dict[str, str]] = []
    tax_rate_rows: list[dict[str, str]] = []
    tax_jurisdiction_rows: list[dict[str, str]] = []
    tax_credit_rows: list[dict[str, str]] = []
    special_charge_rows: list[dict[str, str]] = []
    other_tax_item_rows: list[dict[str, str]] = []
    installment_rows: list[dict[str, str]] = []
    tax_amount_summary: dict[str, str] = {}

    active_headers: list[str] = []
    active_section = ""
    for row in rows:
        row_values = _tax_detail_row_values(row)
        if not row_values:
            continue

        section = row.get("section") or active_section
        active_section = section
        if section.startswith("Tax Payments for"):
            active_headers = []
            continue

        if row.get("row_type") == "header":
            active_headers = row_values
            continue

        if "Tax Values" in section:
            if active_headers and len(row_values) == len(active_headers):
                mapped = {
                    active_headers[idx] or f"col_{idx+1}": row_values[idx]
                    for idx in range(len(active_headers))
                }
                if active_headers[0] == "Category":
                    tax_value_rows.append(mapped)
                    continue
            if len(row_values) >= 2:
                label = _normalize_tax_detail_label(row_values[0])
                amount = row_values[-1]
                labeled_row = {"label": label, "amount": amount}
                if label == "School Levy Tax Credit":
                    tax_credit_rows.append(labeled_row)
                else:
                    tax_rate_rows.append(labeled_row)
            active_headers = []
            continue

        if section.startswith("Tax Amounts for"):
            if len(row_values) >= 2:
                label = _normalize_tax_detail_label(row_values[0])
                amount = row_values[-1]
                if label in {
                    "Total Taxes",
                    "Total Taxes Less Credits",
                    "Total Amount Due",
                }:
                    tax_amount_summary[_tax_detail_summary_key(label)] = amount
                    continue
                if label in {"LOTTERY CREDIT", "FIRST DOLLAR CREDIT"}:
                    tax_credit_rows.append({"label": label, "amount": amount})
                    continue
            if active_headers and len(row_values) == len(active_headers):
                mapped = {
                    active_headers[idx] or f"col_{idx+1}": row_values[idx]
                    for idx in range(len(active_headers))
                }
                header_label = (active_headers[0] or "").upper()
                if header_label == "JURISDICTION":
                    tax_jurisdiction_rows.append(mapped)
                    continue
                if header_label == "SPECIALS":
                    special_charge_rows.append(mapped)
                    continue
                if header_label == "OTHER TAX ITEMS":
                    other_tax_item_rows.append(mapped)
                    continue
            continue

        if section == "Installment Amounts" and len(row_values) >= 2:
            label = _normalize_tax_detail_label(row_values[0])
            amount = row_values[-1]
            installment_rows.append({"label": label, "amount": amount})
            if label == "Total Due":
                tax_amount_summary["installment_total_due"] = amount
            active_headers = []

    has_special_charges = any(
        _tax_detail_amount_is_nonzero(row.get("Amount"))
        for row in special_charge_rows
        if not _normalize_tax_detail_label(row.get("Specials") or "").startswith("No ")
    )
    has_other_tax_items = any(
        _tax_detail_amount_is_nonzero(row.get("Amount"))
        for row in other_tax_item_rows
        if not _normalize_tax_detail_label(row.get("Other Tax Items") or "").startswith(
            "No "
        )
    )

    return {
        "tax_value_rows": tax_value_rows,
        "tax_rate_rows": tax_rate_rows,
        "tax_jurisdiction_rows": tax_jurisdiction_rows,
        "tax_credit_rows": tax_credit_rows,
        "special_charge_rows": special_charge_rows,
        "other_tax_item_rows": other_tax_item_rows,
        "installment_rows": installment_rows,
        "tax_amount_summary": tax_amount_summary,
        "has_tax_credits": bool(tax_credit_rows),
        "has_special_charges": has_special_charges,
        "has_other_tax_items": has_other_tax_items,
    }


def _tax_detail_row_values(row: dict[str, str]) -> list[str]:
    values: list[tuple[int, str]] = []
    for key, value in row.items():
        if not key.startswith("col_"):
            continue
        try:
            index = int(key.split("_", 1)[1])
        except ValueError:
            continue
        values.append((index, value))
    values.sort(key=lambda item: item[0])
    return [value for _, value in values]


def _normalize_tax_detail_label(value: str) -> str:
    return _norm(value).rstrip(":").strip()


def _tax_detail_summary_key(label: str) -> str:
    return {
        "Total Taxes": "total_taxes",
        "Total Taxes Less Credits": "total_taxes_less_credits",
        "Total Amount Due": "total_amount_due",
    }[label]


def _tax_detail_amount_is_nonzero(value: str | None) -> bool:
    if not value:
        return False
    return value not in {"$0.00", "0.00", "0", "$0"}


def _parse_tax_payments(soup: BeautifulSoup) -> list[dict[str, object]]:
    table = soup.select_one("#TaxPayments table.taxTable")
    if not table:
        return []
    rows = table.find_all("tr")
    if not rows:
        return []
    headers = [
        _norm(cell.get_text(" ", strip=True)) for cell in rows[0].find_all(["th", "td"])
    ]
    records: list[dict[str, object]] = []
    last_tax_year: str | None = None
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        values = [_norm(cell.get_text(" ", strip=True)) for cell in cells]
        record: dict[str, object]
        if headers and len(values) == len(headers):
            record = {
                headers[i] or f"col_{i+1}": values[i] for i in range(len(headers))
            }
        else:
            record = {f"col_{i+1}": values[i] for i in range(len(values))}
        tax_year_value = record.get("Tax Year") or record.get("col_1")
        tax_year = tax_year_value if isinstance(tax_year_value, str) else None
        if tax_year:
            last_tax_year = tax_year
        elif last_tax_year:
            record["Tax Year"] = last_tax_year
        if any(record.values()):
            records.append(record)
    return records


def _parse_tax_detail_payments(soup: BeautifulSoup) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for modal in soup.select("div[id^='TaxDetails']"):
        table = modal.select_one("table.taxInfoTable")
        if not table:
            continue
        year = _parse_year(_attr_text(modal.get("id", "")))
        rows = table.find_all("tr")
        headers: list[str] = []
        in_payments = False
        for row in rows:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            if len(cells) == 1:
                label = _norm(cells[0].get_text(" ", strip=True))
                if label.lower().startswith("tax payments for"):
                    in_payments = True
                    headers = []
                continue
            if not in_payments:
                continue
            row_classes = _class_names(row.get("class"))
            if ("rowTitle" in row_classes or row.find_all("th")) and not headers:
                headers = [_norm(cell.get_text(" ", strip=True)) for cell in cells]
                continue
            values = [_norm(cell.get_text(" ", strip=True)) for cell in cells]
            record: dict[str, object] = {"source": "tax_detail_payments"}
            if year:
                record["year"] = str(year)
            if headers:
                for idx, header in enumerate(headers):
                    if idx >= len(values):
                        break
                    key = header or f"col_{idx+1}"
                    record[key] = values[idx]
                if len(values) < len(headers):
                    for idx in range(len(values), len(headers)):
                        key = headers[idx] or f"col_{idx+1}"
                        record.setdefault(key, "")
            else:
                for idx, value in enumerate(values):
                    record[f"col_{idx+1}"] = value
            if any(value for value in values):
                records.append(record)
    return records


def _parse_parcel_summary(soup: BeautifulSoup) -> dict[str, str]:
    table = soup.select_one("#parcelSummary table")
    if not table:
        return {}
    summary: dict[str, str] = {}
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) != 2:
            continue
        key = _norm(cells[0].get_text(" ", strip=True))
        if not key:
            continue
        value = _extract_cell_text(cells[1])
        if value:
            summary[key] = value
    return summary


def _extract_cell_text(cell: Tag) -> str:
    ul = cell.find("ul")
    if ul:
        items = [_norm(li.get_text(" ", strip=True)) for li in ul.find_all("li")]
        items = [item for item in items if item]
        return "\n".join(items).strip()
    text = _norm(cell.get_text(" ", strip=True))
    return text


def _extract_parcel_number(soup: BeautifulSoup) -> str | None:
    heading = soup.select_one("#parcel_detail_heading")
    if not heading:
        return None
    text = _norm(heading.get_text(" ", strip=True))
    match = re.search(r"Parcel Number\s*-\s*([0-9/\\-]+)", text)
    if match:
        return match.group(1)
    return None


def _extract_parcel_id(soup: BeautifulSoup) -> str | None:
    for anchor in soup.find_all("a"):
        href = _attr_text(anchor.get("href"))
        match = re.search(r"/(\d{10,14})", href)
        if match:
            return match.group(1)
    return None


def _attr_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = [item for item in value if isinstance(item, str)]
        return " ".join(parts)
    return ""


def _class_names(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    return []


def _parse_year(value: str) -> int | None:
    value = value.strip()
    match = re.search(r"(19|20)\d{2}", value)
    if match:
        return int(match.group(0))
    return None


def _parse_section_tables(soup: BeautifulSoup, title: str) -> list[dict[str, str]]:
    tables = _extract_section_tables(soup, title)
    records: list[dict[str, str]] = []
    for table in tables:
        records.extend(_table_to_records(table))
    return records


def _extract_section_tables(soup: BeautifulSoup, title: str) -> list[Tag]:
    heading = _find_heading(soup, title)
    if not heading:
        return []
    tables: list[Tag] = []
    node = heading.find_next_sibling()
    while node:
        if isinstance(node, Tag) and node.name and node.name.lower().startswith("h"):
            break
        if isinstance(node, Tag) and node.name == "table":
            tables.append(node)
        node = node.find_next_sibling()
    return tables


def _find_heading(soup: BeautifulSoup, title: str) -> Tag | None:
    title_lower = title.lower()
    for tag_name in ("h1", "h2", "h3", "h4", "h5"):
        for heading in soup.find_all(tag_name):
            text = _norm(heading.get_text(" ", strip=True)).lower()
            if title_lower in text:
                return heading
    return None


def _table_to_records(table: Tag) -> list[dict[str, str]]:
    rows = table.find_all("tr")
    if not rows:
        return []

    header_cells = rows[0].find_all(["th", "td"])
    headers = [_norm(cell.get_text(" ", strip=True)) for cell in header_cells]
    has_header = bool(rows[0].find_all("th"))

    records: list[dict[str, str]] = []
    data_rows = rows[1:] if has_header else rows
    for row in data_rows:
        cells = row.find_all(["td", "th"])
        values = [_norm(cell.get_text(" ", strip=True)) for cell in cells]
        if headers and len(headers) == len(values):
            record = {}
            for i, (header, value) in enumerate(zip(headers, values)):
                key = header or f"col_{i+1}"
                record[key] = value
        else:
            record = {}
            for i, value in enumerate(values):
                record[f"col_{i+1}"] = value
        if any(record.values()):
            records.append(record)
    return records


def _extract_key_value_pairs(soup: BeautifulSoup) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["th", "td"])
            if len(cells) != 2:
                continue
            key = _norm(cells[0].get_text(" ", strip=True))
            value = _norm(cells[1].get_text(" ", strip=True))
            if key and value and key not in pairs:
                pairs[key] = value
    for dl in soup.find_all("dl"):
        terms = dl.find_all("dt")
        defs = dl.find_all("dd")
        for term, definition in zip(terms, defs):
            key = _norm(term.get_text(" ", strip=True))
            value = _norm(definition.get_text(" ", strip=True))
            if key and value and key not in pairs:
                pairs[key] = value
    return pairs


def _norm(text: str) -> str:
    return " ".join(text.split()).strip()
