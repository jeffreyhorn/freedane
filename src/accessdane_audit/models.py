from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


class Parcel(Base):
    __tablename__ = "parcels"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    trs_code: Mapped[Optional[str]] = mapped_column(String, index=True, nullable=True)
    section: Mapped[Optional[int]] = mapped_column(Integer, index=True, nullable=True)
    subsection: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Fetch(Base):
    __tablename__ = "fetches"
    __table_args__ = (
        Index("ix_fetches_status_code_parsed_at", "status_code", "parsed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parcel_id: Mapped[str] = mapped_column(String, ForeignKey("parcels.id"), index=True)
    url: Mapped[str] = mapped_column(String)
    status_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    raw_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    raw_sha256: Mapped[Optional[str]] = mapped_column(String, index=True, nullable=True)
    raw_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    parsed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    parse_error: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class AssessmentRecord(Base):
    __tablename__ = "assessments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parcel_id: Mapped[str] = mapped_column(String, ForeignKey("parcels.id"), index=True)
    fetch_id: Mapped[int] = mapped_column(Integer, ForeignKey("fetches.id"), index=True)
    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    valuation_classification: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    assessment_acres: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 3), nullable=True
    )
    land_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
    improved_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    total_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    average_assessment_ratio: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(8, 4), nullable=True
    )
    estimated_fair_market_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    valuation_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    data: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class TaxRecord(Base):
    __tablename__ = "taxes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parcel_id: Mapped[str] = mapped_column(String, ForeignKey("parcels.id"), index=True)
    fetch_id: Mapped[int] = mapped_column(Integer, ForeignKey("fetches.id"), index=True)
    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    data: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class PaymentRecord(Base):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parcel_id: Mapped[str] = mapped_column(String, ForeignKey("parcels.id"), index=True)
    fetch_id: Mapped[int] = mapped_column(Integer, ForeignKey("fetches.id"), index=True)
    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    data: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ParcelSummary(Base):
    __tablename__ = "parcel_summaries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parcel_id: Mapped[str] = mapped_column(
        String, ForeignKey("parcels.id"), unique=True, index=True
    )
    fetch_id: Mapped[int] = mapped_column(Integer, ForeignKey("fetches.id"), index=True)
    municipality_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    parcel_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    owner_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    primary_address: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    billing_address: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ParcelYearFact(Base):
    __tablename__ = "parcel_year_facts"
    __table_args__ = (
        Index("ix_parcel_year_facts_year", "year"),
        Index("ix_parcel_year_facts_current_owner_name", "current_owner_name"),
    )

    parcel_id: Mapped[str] = mapped_column(
        String, ForeignKey("parcels.id"), primary_key=True
    )
    year: Mapped[int] = mapped_column(Integer, primary_key=True)

    parcel_summary_fetch_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("fetches.id"), nullable=True
    )
    assessment_fetch_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("fetches.id"), nullable=True
    )
    tax_fetch_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("fetches.id"), nullable=True
    )
    payment_fetch_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("fetches.id"), nullable=True
    )

    municipality_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    current_parcel_description: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    current_owner_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    current_primary_address: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    current_billing_address: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )

    assessment_valuation_classification: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    assessment_acres: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 3), nullable=True
    )
    assessment_land_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    assessment_improved_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    assessment_total_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    assessment_average_assessment_ratio: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(8, 4), nullable=True
    )
    assessment_estimated_fair_market_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    assessment_valuation_date: Mapped[Optional[date]] = mapped_column(
        Date, nullable=True
    )

    tax_total_assessed_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    tax_assessed_land_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    tax_assessed_improvement_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    tax_taxes: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
    tax_specials: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    tax_first_dollar_credit: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    tax_lottery_credit: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    tax_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)

    payment_event_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    payment_total_amount: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    payment_first_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    payment_last_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    payment_has_placeholder_row: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )

    built_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ParcelCharacteristic(Base):
    __tablename__ = "parcel_characteristics"
    __table_args__ = (
        Index("ix_parcel_characteristics_source_fetch_id", "source_fetch_id"),
        Index(
            "ix_parcel_characteristics_current_valuation_classification",
            "current_valuation_classification",
        ),
        Index(
            "ix_parcel_characteristics_state_municipality_code",
            "state_municipality_code",
        ),
    )

    parcel_id: Mapped[str] = mapped_column(
        String, ForeignKey("parcels.id"), primary_key=True
    )
    source_fetch_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("fetches.id"), nullable=True
    )

    formatted_parcel_number: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    state_municipality_code: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    township: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    range: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    section: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    quarter_quarter: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    has_dcimap_link: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    has_google_map_link: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    has_bing_map_link: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    current_assessment_year: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    current_valuation_classification: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    current_assessment_acres: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 3), nullable=True
    )
    current_assessment_ratio: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(8, 4), nullable=True
    )
    current_estimated_fair_market_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(14, 2), nullable=True
    )

    current_tax_info_available: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    current_payment_history_available: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    tax_jurisdiction_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )

    is_exempt_style_page: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    has_empty_valuation_breakout: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    has_empty_tax_section: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )

    built_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class ParcelLineageLink(Base):
    __tablename__ = "parcel_lineage_links"
    __table_args__ = (
        Index("ix_parcel_lineage_links_related_parcel_id", "related_parcel_id"),
        Index("ix_parcel_lineage_links_relationship_type", "relationship_type"),
    )

    parcel_id: Mapped[str] = mapped_column(
        String, ForeignKey("parcels.id"), primary_key=True
    )
    related_parcel_id: Mapped[str] = mapped_column(String, primary_key=True)
    relationship_type: Mapped[str] = mapped_column(String, primary_key=True)

    source_fetch_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("fetches.id"), nullable=True
    )
    related_parcel_status: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    relationship_note: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    built_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
