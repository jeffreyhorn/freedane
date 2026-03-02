from __future__ import annotations

from datetime import date
from decimal import Decimal

from accessdane_audit.cli import (
    _extract_assessment_fields,
    _extract_year,
    _get_summary_value,
    _parse_date,
    _parse_decimal,
    _parse_money,
)


def test_parse_decimal_and_money_normalize_currency_and_rounding() -> None:
    assert _parse_decimal("$1,234.567", 2) == Decimal("1234.57")
    assert _parse_decimal("0.1454", 3) == Decimal("0.145")
    assert _parse_money("$245,304") == Decimal("245304.00")


def test_parse_decimal_returns_none_for_empty_or_malformed_values() -> None:
    assert _parse_decimal("", 2) is None
    assert _parse_decimal("N/A", 2) is None
    assert _parse_decimal("--", 2) is None
    assert _parse_decimal("not-a-number", 2) is None
    assert _parse_decimal(None, 2) is None


def test_parse_date_accepts_supported_formats_and_rejects_bad_values() -> None:
    assert _parse_date("03/20/2023") == date(2023, 3, 20)
    assert _parse_date("03/20/23") == date(2023, 3, 20)

    assert _parse_date("") is None
    assert _parse_date("N/A") is None
    assert _parse_date("2023-03-20") is None
    assert _parse_date("02/30/2023") is None


def test_extract_year_uses_explicit_then_fallback_year_fields() -> None:
    assert _extract_year({"year": "2025 payable 2026", "Tax Year": "2024"}) == 2025
    assert _extract_year({"Tax Year": "2023 payable 2024"}) == 2023
    assert _extract_year({"Bill Label": "roll 2022", "Amount": "$1.00"}) == 2022
    assert _extract_year({"Bill Label": "current", "Amount": "$1.00"}) is None


def test_extract_assessment_fields_accepts_current_alias_keys() -> None:
    normalized = _extract_assessment_fields(
        {
            "  Valuation   Classification  ": " G1 ",
            "Assessment Acres": "0.1451",
            "Oand Value": "$123,500.00",
            "Improved Value": "$111,500.00",
            "Total Value": "$235,000.00",
            "Average Assessment Ratio": "0.95804",
            "Estimate Fair Market Value": "$245,304",
            "Valuation Date": "03/20/23",
        }
    )

    assert normalized == {
        "valuation_classification": "G1",
        "assessment_acres": Decimal("0.145"),
        "land_value": Decimal("123500.00"),
        "improved_value": Decimal("111500.00"),
        "total_value": Decimal("235000.00"),
        "average_assessment_ratio": Decimal("0.9580"),
        "estimated_fair_market_value": Decimal("245304.00"),
        "valuation_date": date(2023, 3, 20),
    }


def test_get_summary_value_accepts_label_variants_and_normalizes_whitespace() -> None:
    summary = {
        " Owner Names ": "  LUCKY STRIKES MCFARLAND LLC\nTTR INVESTMENTS LLC  ",
        "Primary Address": "4711 FARWELL ST",
    }

    assert _get_summary_value(summary, "Owner Name", "Owner Names") == (
        "LUCKY STRIKES MCFARLAND LLC\nTTR INVESTMENTS LLC"
    )
    assert _get_summary_value(summary, "Primary Address") == "4711 FARWELL ST"
    assert _get_summary_value(summary, "Billing Address") is None
