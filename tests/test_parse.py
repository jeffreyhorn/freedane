from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from accessdane_audit.cli import (
    _build_parcel_characteristic_candidate,
    _extract_assessment_fields,
    _store_parsed,
)
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import (
    AssessmentRecord,
    Fetch,
    Parcel,
    ParcelCharacteristic,
    ParcelLineageLink,
    ParcelSummary,
    PaymentRecord,
    TaxRecord,
)
from accessdane_audit.parse import parse_page


FULL_SAMPLE_PARCEL_ID = "061003330128"
SPARSE_SAMPLE_PARCEL_ID = "061001391511"
OWNER_NAMES_SAMPLE_PARCEL_ID = "061003253271"
VALUATION_BREAKOUT_SAMPLE_PARCEL_ID = "061002213581"
COMPOSITE_EDGE_SAMPLE_PARCEL_ID = "061002122981"
NO_ADDRESS_HISTORY_SAMPLE_PARCEL_ID = "061001397011"
HIGH_PAYMENT_OWNER_NAMES_SAMPLE_PARCEL_ID = "061002123751"
MIN_ASSESSMENT_SAMPLE_PARCEL_ID = "061001285911"
COMMERCIAL_NO_PAYMENT_HISTORY_SAMPLE_PARCEL_ID = "061003260511"
TAX_EMPTY_SAMPLE_PARCEL_ID = "061002275801"
LINEAGE_SAMPLE_PARCEL_ID = "061002320401"
EXEMPT_STYLE_SAMPLE_PARCEL_ID = "061002191411"
PARENT_ONLY_LINEAGE_SAMPLE_PARCEL_ID = "061002318311"


def test_parse_page_extracts_expected_sections_from_full_raw_fixture(load_raw_html) -> None:
    parsed = parse_page(load_raw_html(FULL_SAMPLE_PARCEL_ID))

    assert len(parsed.assessment) == 27
    assert len(parsed.tax) == 50
    assert len(parsed.payments) == 277
    assert parsed.parcel_summary == {
        "Municipality Name": "VILLAGE OF MCFARLAND",
        "Parcel Description": "ASSESSORS PLAT OUTLOT 74",
        "Primary Address": "4607 YAHARA DR",
        "Billing Address": "4319 TWIN VALLEY RD UNIT 4\nMIDDLETON WI 53562",
    }

    assert parsed.assessment[0] == {
        "source": "summary",
        "year": "2023",
        "Valuation Classification": "G1",
        "Assessment Acres": "0.145",
        "Land Value": "$123,500.00",
        "Improved Value": "$111,500.00",
        "Total Value": "$235,000.00",
    }

    assert parsed.tax[0] == {
        "source": "summary",
        "year": "2023",
        "Total Assessed Value": "$235,000.00",
        "Assessed Land Value": "$123,500.00",
        "Assessed Improvement Value": "$111,500.00",
        "Taxes": "$3,820.53",
        "Specials(+)": "$149.40",
        "First Dollar Credit(-)": "$71.40",
        "Lottery Credit(-)": "$265.77",
        "Amount": "$3,632.76",
    }


def test_parse_page_can_exclude_tax_detail_payments(load_raw_html) -> None:
    html = load_raw_html(FULL_SAMPLE_PARCEL_ID)

    with_tax_detail_payments = parse_page(html)
    without_tax_detail_payments = parse_page(html, include_tax_detail_payments=False)

    assert len(with_tax_detail_payments.tax) == len(without_tax_detail_payments.tax) == 50
    assert len(with_tax_detail_payments.payments) == 277
    assert len(without_tax_detail_payments.payments) == 52


def test_parse_page_extracts_structured_tax_detail_fields_from_modal_fixture(
    load_raw_html,
) -> None:
    parsed = parse_page(load_raw_html(FULL_SAMPLE_PARCEL_ID))

    detail_record = next(
        record
        for record in parsed.tax
        if record.get("source") == "detail" and record.get("year") == "2023"
    )

    assert detail_record["tax_value_rows"][0] == {
        "Category": "Land",
        "Assessed Value": "$123,500.00",
        "Average Assessment Ratio": "0.9580",
        "Estimated Fair Market Value": "$128,915",
    }
    assert detail_record["tax_rate_rows"] == [
        {"label": "Net Assessed Value Rate (mill rate)", "amount": "0.016257594"}
    ]
    assert detail_record["tax_credit_rows"] == [
        {"label": "School Levy Tax Credit", "amount": "$394.47"},
        {"label": "LOTTERY CREDIT", "amount": "$265.77"},
        {"label": "FIRST DOLLAR CREDIT", "amount": "$71.40"},
    ]
    assert detail_record["tax_jurisdiction_rows"][0] == {
        "Jurisdiction": "DANE COUNTY",
        "Amount": "$675.91",
    }
    assert detail_record["special_charge_rows"] == [
        {"Specials": "SOLID WASTE FEE", "Amount": "$149.40"}
    ]
    assert detail_record["other_tax_item_rows"] == [
        {"Other Tax Items": "No Other Tax Items.", "Amount": "$0.00"}
    ]
    assert detail_record["installment_rows"] == [
        {"label": "First Installment (Due 1/31/2024)", "amount": "$1,758.20"},
        {"label": "Second Installment (Due 7/31/2024)", "amount": "$1,874.56"},
        {"label": "Total Due", "amount": "$3,632.76"},
    ]
    assert detail_record["tax_amount_summary"] == {
        "total_taxes": "$3,820.53",
        "total_taxes_less_credits": "$3,483.36",
        "total_amount_due": "$3,632.76",
        "installment_total_due": "$3,632.76",
    }
    assert detail_record["has_tax_credits"] is True
    assert detail_record["has_special_charges"] is True
    assert detail_record["has_other_tax_items"] is False


def test_parse_page_handles_sparse_real_world_page(load_raw_html) -> None:
    parsed = parse_page(load_raw_html(SPARSE_SAMPLE_PARCEL_ID))

    assert len(parsed.assessment) == 7
    assert len(parsed.tax) == 10
    assert len(parsed.payments) == 10
    assert parsed.parcel_summary == {
        "Municipality Name": "VILLAGE OF MCFARLAND",
        "Parcel Description": "SEC 1-6-10 RR R/W THRU S1/2 SW1/4",
        "Owner Name": "WI DOT",
        "Primary Address": "No parcel address available.",
        "Billing Address": "2101 WRIGHT ST\nMADISON WI 53704",
    }
    assert parsed.payments[0] == {
        "Tax Year": "2025",
        "Date of Payment": "No payments found.",
        "Amount": "",
    }


def test_parse_page_handles_exempt_style_empty_sections_fixture(load_raw_html) -> None:
    parsed = parse_page(load_raw_html(EXEMPT_STYLE_SAMPLE_PARCEL_ID))

    assert len(parsed.assessment) == 0
    assert len(parsed.tax) == 0
    assert len(parsed.payments) == 0
    assert parsed.parcel_summary == {
        "Municipality Name": "VILLAGE OF MCFARLAND",
        "Parcel Description": "LOT 1 CSM 15364 CS110/64&68-3/2/2020 DES...",
        "Owner Name": "MCFARLAND SCHOOL DISTRICT",
        "Primary Address": "6008 OSBORN DR",
        "Billing Address": "5101 FARWELL ST\nMCFARLAND WI 53558",
    }


def test_parse_page_preserves_owner_names_variant_label(load_raw_html) -> None:
    parsed = parse_page(load_raw_html(OWNER_NAMES_SAMPLE_PARCEL_ID))

    assert parsed.parcel_summary["Owner Names"] == (
        "LUCKY STRIKES MCFARLAND LLC\nTTR INVESTMENTS LLC"
    )


def test_parse_page_extracts_valuation_breakout_and_tax_detail_payment_totals(
    load_raw_html,
) -> None:
    parsed = parse_page(load_raw_html(VALUATION_BREAKOUT_SAMPLE_PARCEL_ID))

    assert len(parsed.assessment) == 9
    assert len(parsed.tax) == 14
    assert len(parsed.payments) == 68

    valuation_breakout = parsed.assessment[-1]
    assert valuation_breakout["source"] == "valuation_breakout"
    assert len(valuation_breakout["rows"]) == 21
    assert valuation_breakout["rows"][0] == {
        "Year": "2025",
        "Valuation Date": "04/02/2025",
        "Acres": "",
        "Valuation Description": "",
        "Land Value": "",
        "Improved Value": "",
        "MFL Value": "",
        "Total Value": "",
    }
    assert valuation_breakout["rows"][1] == {
        "Year": "",
        "Valuation Date": "",
        "Acres": "0.162",
        "Valuation Description": "G1 - RESIDENTIAL",
        "Land Value": "$59,600",
        "Improved Value": "$147,500",
        "MFL Value": "",
        "Total Value": "$207,100",
    }

    assert parsed.payments[-1] == {
        "source": "tax_detail_payments",
        "year": "2019",
        "Receipt Number": "",
        "Date Paid": "Total of Payments:",
        "Principal": "$2,468.67",
        "Interest": "$0.00",
        "Penalty": "$0.00",
        "Amount": "$2,468.67",
    }


def test_parse_page_handles_composite_owner_names_breakout_and_no_payment_markers(
    load_raw_html,
) -> None:
    parsed = parse_page(load_raw_html(COMPOSITE_EDGE_SAMPLE_PARCEL_ID))

    assert len(parsed.assessment) == 10
    assert len(parsed.tax) == 16
    assert len(parsed.payments) == 79
    assert parsed.parcel_summary["Owner Names"] == "KEVIN SINGER\nERIN SINGER"

    valuation_breakout = parsed.assessment[-1]
    assert valuation_breakout["source"] == "valuation_breakout"
    assert len(valuation_breakout["rows"]) == 24
    assert valuation_breakout["rows"][0] == {
        "Year": "2025",
        "Valuation Date": "04/02/2025",
        "Acres": "",
        "Valuation Description": "",
        "Land Value": "",
        "Improved Value": "",
        "MFL Value": "",
        "Total Value": "",
    }
    assert valuation_breakout["rows"][1] == {
        "Year": "",
        "Valuation Date": "",
        "Acres": "0.201",
        "Valuation Description": "G1 - RESIDENTIAL",
        "Land Value": "$124,900",
        "Improved Value": "$510,100",
        "MFL Value": "",
        "Total Value": "$635,000",
    }

    assert parsed.payments[0] == {
        "Tax Year": "2025",
        "Date of Payment": "No payments found.",
        "Amount": "",
    }
    assert parsed.payments[-1] == {
        "source": "tax_detail_payments",
        "year": "2018",
        "Receipt Number": "",
        "Date Paid": "Total of Payments:",
        "Principal": "$12.36",
        "Interest": "$0.00",
        "Penalty": "$0.00",
        "Amount": "$12.36",
    }


def test_parse_page_handles_no_address_fixture_with_real_payments(
    load_raw_html,
) -> None:
    parsed = parse_page(load_raw_html(NO_ADDRESS_HISTORY_SAMPLE_PARCEL_ID))

    assert len(parsed.assessment) == 8
    assert len(parsed.tax) == 12
    assert len(parsed.payments) == 36
    assert parsed.parcel_summary == {
        "Municipality Name": "VILLAGE OF MCFARLAND",
        "Parcel Description": "SEC 1-6-10 PRT SE1/4 SW1/4 SW OF RR",
        "Owner Name": "ELVEHJEM ACRES LLC",
        "Primary Address": "No parcel address available.",
        "Billing Address": "4720 FARWELL ST\nMCFARLAND WI 53558",
    }

    valuation_breakout = parsed.assessment[-1]
    assert valuation_breakout["source"] == "valuation_breakout"
    assert len(valuation_breakout["rows"]) == 18
    assert valuation_breakout["rows"][1] == {
        "Year": "",
        "Valuation Date": "",
        "Acres": "5.000",
        "Valuation Description": "G5 - UNDEVELOPED",
        "Land Value": "$1,600",
        "Improved Value": "$0",
        "MFL Value": "",
        "Total Value": "$1,600",
    }

    assert parsed.payments[0] == {
        "Tax Year": "2025",
        "Date of Payment": "12/31/2025",
        "Amount": "$26.30",
    }
    assert parsed.payments[-1] == {
        "source": "tax_detail_payments",
        "year": "2020",
        "Receipt Number": "",
        "Date Paid": "Total of Payments:",
        "Principal": "$19.44",
        "Interest": "$0.00",
        "Penalty": "$0.00",
        "Amount": "$19.44",
    }


def test_parse_page_handles_high_payment_owner_names_fixture(
    load_raw_html,
) -> None:
    parsed = parse_page(load_raw_html(HIGH_PAYMENT_OWNER_NAMES_SAMPLE_PARCEL_ID))

    assert len(parsed.assessment) == 10
    assert len(parsed.tax) == 16
    assert len(parsed.payments) == 88
    assert parsed.parcel_summary == {
        "Municipality Name": "VILLAGE OF MCFARLAND",
        "Parcel Description": "PRAIRIE PLACE LOT 35",
        "Owner Names": "BRIAN L HALE\nDARLA M HALE",
        "Primary Address": "6030 SHOOTING STAR CT",
        "Billing Address": "6030 SHOOTING STAR CT\nMCFARLAND WI 53558",
    }

    valuation_breakout = parsed.assessment[-1]
    assert valuation_breakout["source"] == "valuation_breakout"
    assert len(valuation_breakout["rows"]) == 24
    assert valuation_breakout["rows"][1] == {
        "Year": "",
        "Valuation Date": "",
        "Acres": "0.188",
        "Valuation Description": "G1 - RESIDENTIAL",
        "Land Value": "$116,800",
        "Improved Value": "$599,800",
        "MFL Value": "",
        "Total Value": "$716,600",
    }

    detail_payment_count = sum(
        1 for row in parsed.payments if row.get("source") == "tax_detail_payments"
    )
    assert detail_payment_count == 72
    assert parsed.payments[0] == {
        "Tax Year": "2025",
        "Date of Payment": "12/31/2025",
        "Amount": "$10,703.25",
    }
    assert parsed.payments[-1] == {
        "source": "tax_detail_payments",
        "year": "2018",
        "Receipt Number": "",
        "Date Paid": "Total of Payments:",
        "Principal": "$12.36",
        "Interest": "$0.00",
        "Penalty": "$0.00",
        "Amount": "$12.36",
    }


def test_parse_page_handles_minimum_assessment_fixture_with_mixed_class_breakout(
    load_raw_html,
) -> None:
    parsed = parse_page(load_raw_html(MIN_ASSESSMENT_SAMPLE_PARCEL_ID))

    assert len(parsed.assessment) == 7
    assert len(parsed.tax) == 10
    assert len(parsed.payments) == 46
    assert parsed.parcel_summary == {
        "Municipality Name": "VILLAGE OF MCFARLAND",
        "Parcel Description": "LOT 1 CSM 4274 CS18/141&143-1/18/84 DESC...",
        "Owner Name": "SKAALEN RETIREMENT SERVICES INC",
        "Primary Address": "3424 COUNTY HIGHWAY MN",
        "Billing Address": "400 N MORRIS ST\nSTOUGHTON WI 53589",
    }

    assert parsed.assessment[0] == {
        "source": "summary",
        "year": "2025",
        "Valuation Classification": "G1 G4",
        "Assessment Acres": "13.798",
        "Land Value": "$143,200.00",
        "Improved Value": "$466,900.00",
        "Total Value": "$610,100.00",
    }

    valuation_breakout = parsed.assessment[-1]
    assert valuation_breakout["source"] == "valuation_breakout"
    assert len(valuation_breakout["rows"]) == 20
    assert valuation_breakout["rows"][1] == {
        "Year": "",
        "Valuation Date": "",
        "Acres": "2.000",
        "Valuation Description": "G1 - RESIDENTIAL",
        "Land Value": "$137,900",
        "Improved Value": "$466,900",
        "MFL Value": "",
        "Total Value": "$604,800",
    }
    assert valuation_breakout["rows"][2] == {
        "Year": "",
        "Valuation Date": "",
        "Acres": "11.798",
        "Valuation Description": "G4 - AGRICULTURAL",
        "Land Value": "$5,300",
        "Improved Value": "$0",
        "MFL Value": "",
        "Total Value": "$5,300",
    }

    detail_payment_count = sum(
        1 for row in parsed.payments if row.get("source") == "tax_detail_payments"
    )
    assert detail_payment_count == 37
    assert parsed.payments[0] == {
        "Tax Year": "2025",
        "Date of Payment": "No payments found.",
        "Amount": "",
    }
    assert parsed.payments[-1] == {
        "source": "tax_detail_payments",
        "year": "2021",
        "Receipt Number": "",
        "Date Paid": "Total of Payments:",
        "Principal": "$7,605.30",
        "Interest": "$152.10",
        "Penalty": "$76.05",
        "Amount": "$7,833.45",
    }


def test_parse_page_handles_commercial_fixture_with_long_no_payment_history(
    load_raw_html,
) -> None:
    parsed = parse_page(load_raw_html(COMMERCIAL_NO_PAYMENT_HISTORY_SAMPLE_PARCEL_ID))

    assert len(parsed.assessment) == 10
    assert len(parsed.tax) == 16
    assert len(parsed.payments) == 93
    assert parsed.parcel_summary == {
        "Municipality Name": "VILLAGE OF MCFARLAND",
        "Parcel Description": "JOHNSON PLACE CONDOMINIUM COMMERCIAL UNI...",
        "Owner Name": "MIDWEST INVESTMENT GROUP LLC",
        "Primary Address": "4713 DALE CURTIN DR",
        "Billing Address": "200 E MAIN ST\nWATERTOWN WI 53094",
    }

    assert parsed.assessment[0] == {
        "source": "summary",
        "year": "2015",
        "Valuation Classification": "G2",
        "Assessment Acres": "0.000",
        "Land Value": "$25,000.00",
        "Improved Value": "$55,000.00",
        "Total Value": "$80,000.00",
    }

    valuation_breakout = parsed.assessment[-1]
    assert valuation_breakout["source"] == "valuation_breakout"
    assert len(valuation_breakout["rows"]) == 24
    assert valuation_breakout["rows"][1] == {
        "Year": "",
        "Valuation Date": "",
        "Acres": "0.000",
        "Valuation Description": "G2 - COMMERCIAL",
        "Land Value": "$25,000",
        "Improved Value": "$55,000",
        "MFL Value": "",
        "Total Value": "$80,000",
    }

    detail_payment_count = sum(
        1 for row in parsed.payments if row.get("source") == "tax_detail_payments"
    )
    summary_no_payments_count = sum(
        1 for row in parsed.payments if row.get("Date of Payment") == "No payments found."
    )
    assert detail_payment_count == 68
    assert summary_no_payments_count == 10
    assert parsed.payments[:3] == [
        {"Tax Year": "2025", "Date of Payment": "No payments found.", "Amount": ""},
        {"Tax Year": "2024", "Date of Payment": "No payments found.", "Amount": ""},
        {"Tax Year": "2023", "Date of Payment": "No payments found.", "Amount": ""},
    ]
    assert parsed.payments[-1] == {
        "source": "tax_detail_payments",
        "year": "2008",
        "Receipt Number": "",
        "Date Paid": "Total of Payments:",
        "Principal": "$2,158.37",
        "Interest": "$0.00",
        "Penalty": "$0.00",
        "Amount": "$2,158.37",
    }


def test_extract_assessment_fields_normalizes_typed_values_from_parsed_record(
    load_raw_html,
) -> None:
    parsed = parse_page(load_raw_html(FULL_SAMPLE_PARCEL_ID))

    normalized = _extract_assessment_fields(parsed.assessment[1])

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


def test_store_parsed_persists_owner_name_from_owner_names_variant(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-parsed.sqlite'}"
    parcel_id = OWNER_NAMES_SAMPLE_PARCEL_ID
    parsed = parse_page(load_raw_html(parcel_id))

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed)

    with session_scope(database_url) as session:
        stored = session.execute(
            select(ParcelSummary).where(ParcelSummary.parcel_id == parcel_id)
        ).scalar_one()

    assert stored.owner_name == "LUCKY STRIKES MCFARLAND LLC\nTTR INVESTMENTS LLC"
    assert stored.primary_address == "4711 FARWELL ST"


def test_store_parsed_persists_typed_assessment_fields(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-assessment.sqlite'}"
    parcel_id = FULL_SAMPLE_PARCEL_ID
    parsed = parse_page(load_raw_html(parcel_id))

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed)
        fetch_id = fetch.id

    with session_scope(database_url) as session:
        stored = (
            session.execute(
                select(AssessmentRecord)
                .where(AssessmentRecord.fetch_id == fetch_id)
                .order_by(AssessmentRecord.id)
            )
            .scalars()
            .all()
        )

    assert len(stored) == 27

    summary_record = stored[0]
    assert summary_record.year == 2023
    assert summary_record.valuation_classification == "G1"
    assert summary_record.assessment_acres == Decimal("0.145")
    assert summary_record.land_value == Decimal("123500.00")
    assert summary_record.improved_value == Decimal("111500.00")
    assert summary_record.total_value == Decimal("235000.00")
    assert summary_record.average_assessment_ratio is None
    assert summary_record.estimated_fair_market_value is None
    assert summary_record.valuation_date is None

    detail_record = stored[1]
    assert detail_record.year == 2023
    assert detail_record.valuation_classification == "G1"
    assert detail_record.assessment_acres == Decimal("0.145")
    assert detail_record.land_value == Decimal("123500.00")
    assert detail_record.improved_value == Decimal("111500.00")
    assert detail_record.total_value == Decimal("235000.00")
    assert detail_record.average_assessment_ratio == Decimal("0.9580")
    assert detail_record.estimated_fair_market_value == Decimal("245304.00")
    assert detail_record.valuation_date == date(2023, 3, 20)


def test_store_parsed_persists_initial_parcel_characteristics_fields(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-characteristics.sqlite'}"
    parcel_id = FULL_SAMPLE_PARCEL_ID
    raw_html = load_raw_html(parcel_id)
    parsed = parse_page(raw_html)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed, raw_html=raw_html)
        fetch_id = fetch.id

    with session_scope(database_url) as session:
        stored = session.get(ParcelCharacteristic, parcel_id)

    assert stored is not None
    assert stored.source_fetch_id == fetch_id
    assert stored.formatted_parcel_number == "154/0610-033-3012-8"
    assert stored.state_municipality_code == "154"
    assert stored.township == "06"
    assert stored.range == "10"
    assert stored.section == "03"
    assert stored.quarter_quarter == "NW of the SW"
    assert stored.current_assessment_year == 2023
    assert stored.current_valuation_classification == "G1"
    assert stored.current_assessment_acres == Decimal("0.145")
    assert stored.current_assessment_ratio == Decimal("0.9580")
    assert stored.current_estimated_fair_market_value == Decimal("245304.00")
    assert stored.current_tax_info_available is True
    assert stored.current_payment_history_available is True
    assert stored.tax_jurisdiction_count == 4
    assert stored.has_empty_valuation_breakout is False
    assert stored.has_empty_tax_section is False


def test_store_parsed_marks_explicit_empty_tax_pages_in_parcel_characteristics(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-characteristics-empty-tax.sqlite'}"
    parcel_id = TAX_EMPTY_SAMPLE_PARCEL_ID
    raw_html = load_raw_html(parcel_id)
    parsed = parse_page(raw_html)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed, raw_html=raw_html)

    with session_scope(database_url) as session:
        stored = session.get(ParcelCharacteristic, parcel_id)

    assert stored is not None
    assert stored.formatted_parcel_number == "154/0610-022-7580-1"
    assert stored.current_assessment_year == 2007
    assert stored.current_tax_info_available is False
    assert stored.current_payment_history_available is False
    assert stored.tax_jurisdiction_count == 4
    assert stored.has_empty_tax_section is True
    assert stored.has_empty_valuation_breakout is False


def test_store_parsed_persists_vacant_land_characteristics_fields(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-characteristics-vacant.sqlite'}"
    parcel_id = NO_ADDRESS_HISTORY_SAMPLE_PARCEL_ID
    raw_html = load_raw_html(parcel_id)
    parsed = parse_page(raw_html)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed, raw_html=raw_html)

    with session_scope(database_url) as session:
        stored = session.get(ParcelCharacteristic, parcel_id)

    assert stored is not None
    assert stored.formatted_parcel_number == "154/0610-013-9701-1"
    assert stored.current_assessment_year == 2025
    assert stored.current_valuation_classification == "G5"
    assert stored.current_assessment_acres == Decimal("5.000")
    assert stored.has_dcimap_link is True
    assert stored.has_google_map_link is False
    assert stored.has_bing_map_link is False
    assert stored.tax_jurisdiction_count == 3
    assert stored.is_exempt_style_page is False
    assert stored.has_empty_tax_section is False


def test_store_parsed_persists_condo_characteristics_fields(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-characteristics-condo.sqlite'}"
    parcel_id = COMMERCIAL_NO_PAYMENT_HISTORY_SAMPLE_PARCEL_ID
    raw_html = load_raw_html(parcel_id)
    parsed = parse_page(raw_html)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed, raw_html=raw_html)

    with session_scope(database_url) as session:
        stored = session.get(ParcelCharacteristic, parcel_id)

    assert stored is not None
    assert stored.formatted_parcel_number == "154/0610-032-6051-1"
    assert stored.current_assessment_year == 2015
    assert stored.current_valuation_classification == "G2"
    assert stored.current_assessment_acres == Decimal("0.000")
    assert stored.current_assessment_ratio == Decimal("0.9989")
    assert stored.current_estimated_fair_market_value == Decimal("80089.00")
    assert stored.has_dcimap_link is False
    assert stored.has_google_map_link is False
    assert stored.has_bing_map_link is False
    assert stored.tax_jurisdiction_count == 4


def test_store_parsed_marks_school_district_empty_pages_as_exempt_style(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-characteristics-exempt-style.sqlite'}"
    parcel_id = EXEMPT_STYLE_SAMPLE_PARCEL_ID
    raw_html = load_raw_html(parcel_id)
    parsed = parse_page(raw_html)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed, raw_html=raw_html)

    with session_scope(database_url) as session:
        stored = session.get(ParcelCharacteristic, parcel_id)

    assert stored is not None
    assert stored.formatted_parcel_number == "154/0610-021-9141-1"
    assert stored.current_assessment_year is None
    assert stored.current_tax_info_available is False
    assert stored.current_payment_history_available is False
    assert stored.has_empty_valuation_breakout is True
    assert stored.has_empty_tax_section is True
    assert stored.is_exempt_style_page is True


def test_store_parsed_keeps_more_complete_characteristics_snapshot(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-characteristics-precedence.sqlite'}"
    parcel_id = "test-parcel"
    richer_html = load_raw_html(FULL_SAMPLE_PARCEL_ID)
    sparser_html = load_raw_html("061002140011")

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        older_fetch = Fetch(
            parcel_id=parcel_id,
            url="https://example.test/older",
            status_code=200,
            fetched_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        newer_fetch = Fetch(
            parcel_id=parcel_id,
            url="https://example.test/newer",
            status_code=200,
            fetched_at=datetime(2025, 2, 1, tzinfo=timezone.utc),
        )
        session.add_all([older_fetch, newer_fetch])
        session.flush()

        _store_parsed(session, older_fetch, parse_page(richer_html), raw_html=richer_html)
        _store_parsed(session, newer_fetch, parse_page(sparser_html), raw_html=sparser_html)

        older_fetch_id = older_fetch.id

    with session_scope(database_url) as session:
        stored = session.get(ParcelCharacteristic, parcel_id)

    assert stored is not None
    assert stored.source_fetch_id == older_fetch_id
    assert stored.formatted_parcel_number == "154/0610-033-3012-8"
    assert stored.current_assessment_year == 2023


def test_store_parsed_persists_mixed_class_residential_characteristics(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-characteristics-mixed-class.sqlite'}"
    parcel_id = MIN_ASSESSMENT_SAMPLE_PARCEL_ID
    raw_html = load_raw_html(parcel_id)
    parsed = parse_page(raw_html)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed, raw_html=raw_html)

    with session_scope(database_url) as session:
        stored = session.get(ParcelCharacteristic, parcel_id)

    assert stored is not None
    assert stored.formatted_parcel_number == "154/0610-012-8591-1"
    assert stored.current_assessment_year == 2025
    assert stored.current_valuation_classification == "G1 G4"
    assert stored.current_tax_info_available is True
    assert stored.current_payment_history_available is True
    assert stored.has_dcimap_link is True
    assert stored.has_google_map_link is True
    assert stored.has_bing_map_link is True
    assert stored.section == "01"
    assert stored.quarter_quarter == "NW of the NW"


def test_build_parcel_characteristic_candidate_uses_sparse_detail_fallbacks() -> None:
    raw_html = """
    <html>
      <body>
        <div id="parcel_detail_heading">Parcel Number - 154/0610-021-4001-1</div>
        <div id="parcelSummary">
          <table>
            <tr>
              <td>Parcel Description</td>
              <td>SEC 2-6-10 PRT SW1/4NE1/4 CSM 15364</td>
            </tr>
          </table>
        </div>
        <div id="parcelDetail">
          <table>
            <tr>
              <td>State Municipality Code</td>
              <td>154</td>
            </tr>
            <tr>
              <td>Township/Range</td>
              <td>T 6 N / R 10 E</td>
            </tr>
            <tr>
              <td>Section</td>
              <td>02</td>
            </tr>
          </table>
        </div>
        <a href="http://dcimapapps.danecounty.gov/dcmapviewer/#test">Map Viewer</a>
        <a href="http://maps.google.com/maps?q=5810+MILWAUKEE+ST">View Street Map</a>
        <a href="http://www.bing.com/maps/?v=2&where1=test">Bird's Eye</a>
      </body>
    </html>
    """
    parsed = parse_page(raw_html)
    fetch = Fetch(
        id=1,
        parcel_id="061002140011",
        url="https://example.test/061002140011",
        status_code=200,
        fetched_at=datetime(2025, 3, 2, tzinfo=timezone.utc),
    )

    candidate = _build_parcel_characteristic_candidate(fetch, parsed, raw_html=raw_html)

    assert candidate is not None
    fields = candidate["fields"]
    assert fields["formatted_parcel_number"] == "154/0610-021-4001-1"
    assert fields["state_municipality_code"] == "154"
    assert fields["township"] == "06"
    assert fields["range"] == "10"
    assert fields["section"] == "02"
    assert fields["quarter_quarter"] == "SW of the NE"
    assert fields["has_dcimap_link"] is True
    assert fields["has_google_map_link"] is True
    assert fields["has_bing_map_link"] is True


def test_store_parsed_persists_parcel_lineage_links_from_history_modals(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-lineage.sqlite'}"
    parcel_id = LINEAGE_SAMPLE_PARCEL_ID
    raw_html = load_raw_html(parcel_id)
    parsed = parse_page(raw_html)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed, raw_html=raw_html)
        fetch_id = fetch.id

    with session_scope(database_url) as session:
        links = (
            session.execute(
                select(ParcelLineageLink)
                .where(ParcelLineageLink.parcel_id == parcel_id)
                .order_by(
                    ParcelLineageLink.relationship_type,
                    ParcelLineageLink.related_parcel_id,
                )
            )
            .scalars()
            .all()
        )

    assert len(links) == 5
    assert [(link.related_parcel_id, link.relationship_type) for link in links] == [
        ("061002320501", "child"),
        ("061002320701", "child"),
        ("061002320901", "child"),
        ("061002320159", "parent"),
        ("061002320300", "parent"),
    ]

    first_child = links[0]
    assert first_child.source_fetch_id == fetch_id
    assert first_child.related_parcel_status == "Current"
    assert "LOT 1 CSM 13157" in (first_child.relationship_note or "")

    first_parent = links[-2]
    assert first_parent.related_parcel_status == "Retired - 04/21/2011"
    assert "LOT 1 CSM 8360" in (first_parent.relationship_note or "")


def test_store_parsed_persists_parent_only_lineage_links_without_children(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-lineage-parent-only.sqlite'}"
    parcel_id = PARENT_ONLY_LINEAGE_SAMPLE_PARCEL_ID
    raw_html = load_raw_html(parcel_id)
    parsed = parse_page(raw_html)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed, raw_html=raw_html)

    with session_scope(database_url) as session:
        links = (
            session.execute(
                select(ParcelLineageLink)
                .where(ParcelLineageLink.parcel_id == parcel_id)
                .order_by(ParcelLineageLink.related_parcel_id)
            )
            .scalars()
            .all()
        )

    assert [(link.related_parcel_id, link.relationship_type) for link in links] == [
        ("061002262511", "parent"),
        ("061002429501", "parent"),
    ]
    assert all(link.related_parcel_status == "Retired - 11/01/2004" for link in links)


def test_store_parsed_uses_latest_fetch_metadata_for_duplicate_lineage_links(
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-lineage-precedence.sqlite'}"
    parcel_id = "lineage-test-parcel"
    older_html = """
    <html><body>
      <div id="parcel_detail_heading">Parcel Number - 154/0610-023-2040-1</div>
      <div id="modalParcelHistoryParents" class="modal hide fade">
        <div class="modal-body">
          <div class="parcelhistory">
            <div>
              <a href="/061002320159">154/0610-023-2015-9</a>
              <span class="badge badge-important">Retired - 04/21/2011</span>
            </div>
            <div>Older lineage note</div>
          </div>
        </div>
      </div>
    </body></html>
    """
    newer_html = """
    <html><body>
      <div id="parcel_detail_heading">Parcel Number - 154/0610-023-2040-1</div>
      <div id="modalParcelHistoryParents" class="modal hide fade">
        <div class="modal-body">
          <div class="parcelhistory">
            <div>
              <a href="/061002320159">154/0610-023-2015-9</a>
              <span class="badge badge-success">Current</span>
            </div>
            <div>Newer lineage note</div>
          </div>
        </div>
      </div>
    </body></html>
    """

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        older_fetch = Fetch(
            parcel_id=parcel_id,
            url="https://example.test/older",
            status_code=200,
            fetched_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        newer_fetch = Fetch(
            parcel_id=parcel_id,
            url="https://example.test/newer",
            status_code=200,
            fetched_at=datetime(2025, 2, 1, tzinfo=timezone.utc),
        )
        session.add_all([older_fetch, newer_fetch])
        session.flush()

        _store_parsed(session, older_fetch, parse_page(older_html), raw_html=older_html)
        _store_parsed(session, newer_fetch, parse_page(newer_html), raw_html=newer_html)
        newer_fetch_id = newer_fetch.id

    with session_scope(database_url) as session:
        stored = session.get(
            ParcelLineageLink,
            (parcel_id, "061002320159", "parent"),
        )

    assert stored is not None
    assert stored.source_fetch_id == newer_fetch_id
    assert stored.related_parcel_status == "Current"
    assert stored.relationship_note == "Newer lineage note"


def test_parse_page_extracts_structured_tax_detail_fields_for_split_lot_fixture(
    load_raw_html,
) -> None:
    parsed = parse_page(load_raw_html(PARENT_ONLY_LINEAGE_SAMPLE_PARCEL_ID))

    detail_record = next(
        record
        for record in parsed.tax
        if record.get("source") == "detail" and record.get("year") == "2025"
    )

    assert detail_record["has_tax_credits"] is True
    assert detail_record["has_special_charges"] is True
    assert detail_record["has_other_tax_items"] is False
    assert detail_record["special_charge_rows"] == [
        {"Specials": "SOLID WASTE FEE", "Amount": "$162.36"}
    ]
    assert detail_record["installment_rows"][-1] == {
        "label": "Total Due",
        "amount": "$16,084.17",
    }
    assert detail_record["tax_amount_summary"] == {
        "total_taxes": "$16,241.86",
        "total_taxes_less_credits": "$15,921.81",
        "total_amount_due": "$16,084.17",
        "installment_total_due": "$16,084.17",
    }


def test_store_parsed_persists_tax_and_summary_payment_rows_without_tax_detail_payments(
    load_raw_html,
    tmp_path: Path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'store-tax-payments.sqlite'}"
    parcel_id = FULL_SAMPLE_PARCEL_ID
    parsed = parse_page(load_raw_html(parcel_id), include_tax_detail_payments=False)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        fetch = Fetch(parcel_id=parcel_id, url=f"https://example.test/{parcel_id}", status_code=200)
        session.add(fetch)
        session.flush()
        _store_parsed(session, fetch, parsed)
        fetch_id = fetch.id

    with session_scope(database_url) as session:
        stored_taxes = (
            session.execute(
                select(TaxRecord)
                .where(TaxRecord.fetch_id == fetch_id)
                .order_by(TaxRecord.id)
            )
            .scalars()
            .all()
        )
        stored_payments = (
            session.execute(
                select(PaymentRecord)
                .where(PaymentRecord.fetch_id == fetch_id)
                .order_by(PaymentRecord.id)
            )
            .scalars()
            .all()
        )

    assert len(stored_taxes) == 50
    assert len(stored_payments) == 52

    first_tax = stored_taxes[0]
    assert first_tax.year == 2023
    assert first_tax.data["Amount"] == "$3,632.76"

    first_payment = stored_payments[0]
    assert first_payment.year == 2025
    assert first_payment.data == {
        "Tax Year": "2025",
        "Date of Payment": "No payments found.",
        "Amount": "",
    }

    last_payment = stored_payments[-1]
    assert last_payment.year == 1999
    assert last_payment.data["Amount"] == "$2,267.94"
