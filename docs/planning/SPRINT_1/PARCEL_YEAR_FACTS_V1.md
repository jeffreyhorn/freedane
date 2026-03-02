# `parcel_year_facts` v1 Specification

## Purpose

Define the first normalized parcel-year data product that analysts can query without manually stitching together `assessments`, `taxes`, `payments`, and `parcel_summaries`.

This is a Sprint 1 scope definition for implementation on Day 7. It sets:

- the grain
- the initial v1 field set
- the physical form
- the merge and precedence rules when multiple source rows exist

## Grain

`parcel_year_facts` v1 is:

- one row per `parcel_id` per `year`

`year` is the tax / assessment year derived from parsed source rows.

Row creation rule:

- create a row when at least one of these source domains has a usable year for the parcel:
  - `assessments`
  - `taxes`
  - `payments`

Do not create a `parcel_year_facts` row from `parcel_summaries` alone because that table is not year-grained.

## Physical Form

v1 should be a physical table, not a materialized view.

Reasoning:

- It must work the same way in SQLite and Postgres.
- It should be easy to rebuild deterministically during tests and local validation.
- It should support future indexes and profiling queries without re-running complex joins every time.
- The current source data needs nontrivial precedence rules that are clearer in an explicit rebuild step than in a live view.

Recommended build mode:

- fully rebuildable derived table
- implementation can use truncate/delete + insert as the default refresh path

## Row Identity And Provenance

The table should use:

- primary key: (`parcel_id`, `year`)

v1 should also retain per-domain provenance so future debugging does not require reverse-engineering the merge:

- `parcel_id`
- `year`
- `parcel_summary_fetch_id`
- `assessment_fetch_id`
- `tax_fetch_id`
- `payment_fetch_id`
- `built_at`

Important:

- The chosen fetch for assessment, tax, and payment fields may differ for the same parcel-year.
- That is acceptable in v1 and should be explicit rather than forced into one synthetic "winner" fetch.

## V1 Field Set

### Parcel Identity / Current Parcel Snapshot

These fields come from the latest available `parcel_summaries` row for the parcel and are repeated on every parcel-year row for convenience.

Because `parcel_summaries` is not historical, use `current_` prefixes where the value could be mistaken for year-specific history.

- `municipality_name`
- `current_parcel_description`
- `current_owner_name`
- `current_primary_address`
- `current_billing_address`

### Assessment Snapshot

These fields come from the selected assessment record for the parcel-year.

- `assessment_valuation_classification`
- `assessment_acres`
- `assessment_land_value`
- `assessment_improved_value`
- `assessment_total_value`
- `assessment_average_assessment_ratio`
- `assessment_estimated_fair_market_value`
- `assessment_valuation_date`

### Tax Snapshot

These fields come from the selected tax summary record for the parcel-year.

- `tax_total_assessed_value`
- `tax_assessed_land_value`
- `tax_assessed_improvement_value`
- `tax_taxes`
- `tax_specials`
- `tax_first_dollar_credit`
- `tax_lottery_credit`
- `tax_amount`

### Payment Rollup

These fields come from an aggregate over selected payment summary rows for the parcel-year.

- `payment_event_count`
- `payment_total_amount`
- `payment_first_date`
- `payment_last_date`
- `payment_has_placeholder_row`

Semantics:

- `payment_event_count` counts usable summary payment rows for the selected fetch/year.
- `payment_total_amount` is the sum of usable summary payment `Amount` values for the selected fetch/year.
- `payment_first_date` / `payment_last_date` are the min/max parsed summary payment dates.
- `payment_has_placeholder_row` is `true` when the selected fetch/year only exposes a summary placeholder such as `No payments found.`

## Proposed SQLAlchemy Model

Day 7 should implement `parcel_year_facts` as a normal SQLAlchemy table model plus an Alembic migration.

Proposed shape:

```python
class ParcelYearFact(Base):
    __tablename__ = "parcel_year_facts"

    parcel_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("parcels.id"),
        primary_key=True,
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
    current_parcel_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    current_owner_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    current_primary_address: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    current_billing_address: Mapped[Optional[str]] = mapped_column(String, nullable=True)

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
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
```

## Exact Column Types

The v1 implementation should use these SQLAlchemy column types so the table matches the precision already used in the base parsed tables.

- `parcel_id`: `String`, non-null, composite primary key, foreign key to `parcels.id`
- `year`: `Integer`, non-null, composite primary key
- `parcel_summary_fetch_id`: `Integer`, nullable, foreign key to `fetches.id`
- `assessment_fetch_id`: `Integer`, nullable, foreign key to `fetches.id`
- `tax_fetch_id`: `Integer`, nullable, foreign key to `fetches.id`
- `payment_fetch_id`: `Integer`, nullable, foreign key to `fetches.id`
- `municipality_name`: `String`, nullable
- `current_parcel_description`: `String`, nullable
- `current_owner_name`: `String`, nullable
- `current_primary_address`: `String`, nullable
- `current_billing_address`: `String`, nullable
- `assessment_valuation_classification`: `String`, nullable
- `assessment_acres`: `Numeric(10, 3)`, nullable
- `assessment_land_value`: `Numeric(14, 2)`, nullable
- `assessment_improved_value`: `Numeric(14, 2)`, nullable
- `assessment_total_value`: `Numeric(14, 2)`, nullable
- `assessment_average_assessment_ratio`: `Numeric(8, 4)`, nullable
- `assessment_estimated_fair_market_value`: `Numeric(14, 2)`, nullable
- `assessment_valuation_date`: `Date`, nullable
- `tax_total_assessed_value`: `Numeric(14, 2)`, nullable
- `tax_assessed_land_value`: `Numeric(14, 2)`, nullable
- `tax_assessed_improvement_value`: `Numeric(14, 2)`, nullable
- `tax_taxes`: `Numeric(14, 2)`, nullable
- `tax_specials`: `Numeric(14, 2)`, nullable
- `tax_first_dollar_credit`: `Numeric(14, 2)`, nullable
- `tax_lottery_credit`: `Numeric(14, 2)`, nullable
- `tax_amount`: `Numeric(14, 2)`, nullable
- `payment_event_count`: `Integer`, nullable
- `payment_total_amount`: `Numeric(14, 2)`, nullable
- `payment_first_date`: `Date`, nullable
- `payment_last_date`: `Date`, nullable
- `payment_has_placeholder_row`: `Boolean`, nullable
- `built_at`: `DateTime(timezone=True)`, non-null, `server_default=func.now()`

Nullability rules:

- Source-domain columns are nullable because a parcel-year row can exist from only one or two contributing domains.
- `payment_event_count = 0` is valid only when a payment domain is present but only placeholder rows were selected.
- `payment_event_count = NULL` means no payment source was selected for that parcel-year.
- `payment_has_placeholder_row = NULL` means no payment source was selected.

Recommended initial indexes for v1:

- primary key on (`parcel_id`, `year`)
- non-unique index on `year`
- non-unique index on `current_owner_name`

## Intentional v1 Exclusions

These are deliberately out of scope for v1, even though the raw parser captures related data:

- Flattening `assessment` `source=valuation_breakout` rows into separate per-class value columns.
- Flattening `tax` `source=detail` modal rows into a line-item tax schema.
- Using `payments` `source=tax_detail_payments` rows in the canonical payment totals.
- Any attempt to infer historical owners from current parcel summary fields.

Rationale:

- Those shapes are useful, but they need separate modeling decisions and would make v1 brittle if forced into a flat parcel-year table immediately.

## Source Selection Rules

## 1. Parcel-Year Universe

For each parcel:

- collect the union of years present in:
  - `assessments.year`
  - `taxes.year`
  - `payments.year`

Create one output row for each distinct year in that union.

## 2. Parcel Summary Selection

`parcel_summaries` is parcel-level, not year-level.

Selection rule:

- choose the latest available parcel summary for the parcel
- latest means greatest `fetches.fetched_at`
- tie-break with greatest `fetch_id`

Populate the `current_*` summary fields from that one chosen row.

## 3. Assessment Selection

For a given parcel-year:

- only consider rows from `assessments` where `year` matches
- ignore rows whose JSON payload has `source = valuation_breakout`

Preferred record order:

1. `source = detail`
2. `source = summary`
3. any other assessment row shape

Within the same source priority, choose the record with the highest completeness score.

Assessment completeness score:

- count non-null values across:
  - `valuation_classification`
  - `assessment_acres`
  - `land_value`
  - `improved_value`
  - `total_value`
  - `average_assessment_ratio`
  - `estimated_fair_market_value`
  - `valuation_date`

Tie-breaks after completeness:

1. latest `fetches.fetched_at`
2. greatest `fetch_id`
3. greatest `assessments.id`

Why:

- A later fetch should usually win, but not if it is clearly less complete than an earlier parsed row.

## 4. Tax Selection

For a given parcel-year:

- only consider rows from `taxes` where `year` matches
- only use rows whose JSON payload has `source = summary`
- ignore `source = detail` rows in v1

Within summary rows, choose the record with the highest completeness score.

Tax completeness score:

- count non-empty values across:
  - `Total Assessed Value`
  - `Assessed Land Value`
  - `Assessed Improvement Value`
  - `Taxes`
  - `Specials(+)`
  - `First Dollar Credit(-)`
  - `Lottery Credit(-)`
  - `Amount`

Tie-breaks:

1. latest `fetches.fetched_at`
2. greatest `fetch_id`
3. greatest `taxes.id`

## 5. Payment Selection And Rollup

For a given parcel-year:

- only consider rows from `payments` where `year` matches
- only use summary payment rows:
  - rows that do **not** have `source = tax_detail_payments`

Group candidate rows by `fetch_id` first, then choose exactly one fetch for the parcel-year.

Preferred fetch order:

1. highest count of usable summary payment events
2. latest `fetches.fetched_at`
3. greatest `fetch_id`

Definition of a usable summary payment event:

- `Date of Payment` is a real date, not `No payments found.`
- `Amount` parses as a numeric amount

Placeholder rows:

- If a fetch-year has only placeholder rows such as `No payments found.`, it still counts as a valid candidate fetch.
- For such a fetch:
  - `payment_event_count = 0`
  - `payment_total_amount = NULL`
  - `payment_first_date = NULL`
  - `payment_last_date = NULL`
  - `payment_has_placeholder_row = true`

For a selected fetch-year with usable events:

- `payment_event_count` = count of usable rows
- `payment_total_amount` = sum of parsed `Amount`
- `payment_first_date` = minimum parsed `Date of Payment`
- `payment_last_date` = maximum parsed `Date of Payment`
- `payment_has_placeholder_row` = `true` only if placeholder rows also appeared alongside usable rows

Important v1 rule:

- Do not combine summary payment rows and `tax_detail_payments` rows in the same numeric rollup.
- That would risk double counting because both can describe the same underlying payment activity at different granularities.

## 6. Cross-Domain Merge Rule

After choosing parcel summary, assessment, tax, and payment sources independently:

- merge them into one parcel-year row
- allow missing domains

Examples:

- If a parcel-year has tax data but no assessment data, still emit the row with null assessment columns.
- If a parcel-year has assessment data but no payment data, still emit the row with null payment columns.

This preserves the row universe from the union of source years instead of dropping rows due to incomplete parsing.

## Null And Conflict Rules

- Prefer `NULL` over invented defaults when a source value is absent.
- Do not backfill a field from a different year.
- Do not combine two conflicting source rows from the same domain in the same parcel-year.
- Choose one winner per domain using the precedence rules above, then map that row directly.

This keeps v1 deterministic and auditable.

## Mapping Notes For Day 7 Implementation

### Assessment Field Mapping

From the chosen `assessments` row:

- `valuation_classification` -> `assessment_valuation_classification`
- `assessment_acres` -> `assessment_acres`
- `land_value` -> `assessment_land_value`
- `improved_value` -> `assessment_improved_value`
- `total_value` -> `assessment_total_value`
- `average_assessment_ratio` -> `assessment_average_assessment_ratio`
- `estimated_fair_market_value` -> `assessment_estimated_fair_market_value`
- `valuation_date` -> `assessment_valuation_date`

### Tax Field Mapping

From the chosen `taxes` summary row JSON:

- `Total Assessed Value` -> `tax_total_assessed_value`
- `Assessed Land Value` -> `tax_assessed_land_value`
- `Assessed Improvement Value` -> `tax_assessed_improvement_value`
- `Taxes` -> `tax_taxes`
- `Specials(+)` -> `tax_specials`
- `First Dollar Credit(-)` -> `tax_first_dollar_credit`
- `Lottery Credit(-)` -> `tax_lottery_credit`
- `Amount` -> `tax_amount`

### Parcel Summary Mapping

From the chosen `parcel_summaries` row:

- `municipality_name` -> `municipality_name`
- `parcel_description` -> `current_parcel_description`
- `owner_name` -> `current_owner_name`
- `primary_address` -> `current_primary_address`
- `billing_address` -> `current_billing_address`

## Known v1 Limitations

- Current owner/address data is repeated across all years and is not historical ownership.
- Class-split valuation details remain available only in raw `assessment` JSON, not flattened into the mart.
- Payment totals are based on summary payment rows only; modal tax-detail payments are intentionally excluded from v1 rollups.
- A later fetch with a slightly newer timestamp may lose to an older fetch if the older row is materially more complete.

These are acceptable for v1 because the goal is a stable, auditable parcel-year mart, not the final analytics schema.
