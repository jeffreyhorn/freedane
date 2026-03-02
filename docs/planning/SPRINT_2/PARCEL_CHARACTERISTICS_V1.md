# `parcel_characteristics` v1 Specification

## Purpose

Define the first normalized parcel-level characteristic layer for Sprint 2.

This table is intended to capture stable current parcel attributes and local context from AccessDane before external sales matching begins.

This Day 5 design sets:

- the grain
- the physical form
- the initial field set
- lineage handling
- merge and precedence rules when multiple fetches expose the same parcel details

## Design Summary

v1 should use two related normalized tables:

- `parcel_characteristics`
- `parcel_lineage_links`

Reasoning:

- `parcel_characteristics` should stay one row per parcel so it is easy to query and profile.
- Parent/child parcel history is inherently one-to-many and should not be flattened into wide columns.
- Keeping lineage in its own table avoids an awkward repeated-column design and makes rebuild logic clearer.

## Grain

### `parcel_characteristics`

`parcel_characteristics` v1 is:

- one row per `parcel_id`

This row represents the best available current characteristic snapshot for the parcel from the AccessDane corpus.

### `parcel_lineage_links`

`parcel_lineage_links` v1 is:

- one row per (`parcel_id`, `related_parcel_id`, `relationship_type`)

Where:

- `relationship_type = "parent"` means the related parcel is a parent of the current parcel
- `relationship_type = "child"` means the related parcel is a child of the current parcel

## Physical Form

Both tables should be physical tables, not views.

Reasoning:

- The project already uses deterministic rebuildable tables successfully in Sprint 1.
- The characteristic and lineage layers will need explicit precedence rules.
- Physical tables support profiling and completeness checks without repeating expensive parsing or joins.

Recommended build mode:

- fully rebuildable derived tables
- default refresh strategy: delete + insert within the selected parcel scope

## Row Identity And Provenance

### `parcel_characteristics`

Primary key:

- `parcel_id`

Each row should retain a single source fetch reference:

- `parcel_id`
- `source_fetch_id`
- `built_at`

Important design decision:

- v1 uses a bundle-level winner fetch for characteristics
- all characteristic fields on one row come from the same selected fetch

This avoids mixed-field provenance and keeps the current snapshot easy to debug.

### `parcel_lineage_links`

Primary key:

- (`parcel_id`, `related_parcel_id`, `relationship_type`)

Each row should retain:

- `parcel_id`
- `related_parcel_id`
- `relationship_type`
- `source_fetch_id`
- `related_parcel_status`
- `relationship_note`
- `built_at`

## `parcel_characteristics` v1 Field Set

v1 should focus on fields that are likely to be stable, broadly available, and useful before external matching starts.

### Parcel Identity And Mapping

These fields are parcel-level characteristics that are not already fully normalized elsewhere.

- `formatted_parcel_number`
- `state_municipality_code`
- `township`
- `range`
- `section`
- `quarter_quarter`
- `has_dcimap_link`
- `has_google_map_link`
- `has_bing_map_link`

Notes:

- `formatted_parcel_number` is the human-readable parcel/PIN format shown on the page, not the internal `parcel_id`.
- `quarter_quarter` should store the displayed legal-location fragment as shown on the parcel detail grid.

### Current Assessment Snapshot

These are current characteristic-style fields derived from the current assessment summary block on the parcel page.

They intentionally duplicate a subset of current-year assessment information because they are useful as parcel-level current attributes and are often the quickest way to describe a parcel’s current state.

- `current_assessment_year`
- `current_valuation_classification`
- `current_assessment_acres`
- `current_assessment_ratio`
- `current_estimated_fair_market_value`

### Current Tax Availability / Local Context

These fields capture useful local context without flattening full tax history into this table.

- `current_tax_info_available`
- `current_payment_history_available`
- `tax_jurisdiction_count`

Semantics:

- `current_tax_info_available = false` when the page explicitly renders `No tax information available.`
- `current_payment_history_available = false` when the page explicitly renders `No historic payments found.` or only placeholder `No payments found.` rows
- `tax_jurisdiction_count` is the count of rows in the visible taxing-jurisdiction table when present

### Current Page Variant Flags

These flags are useful because Sprint 2 has already shown that some AccessDane page variants are structurally different.

- `is_exempt_style_page`
- `has_empty_valuation_breakout`
- `has_empty_tax_section`

Semantics:

- `is_exempt_style_page = true` when the page shape is consistent with exempt-style parcels (for example, exempt classification labels or empty-value current assessment shapes)
- `has_empty_valuation_breakout = true` when the valuation-breakout table renders headers but no data rows
- `has_empty_tax_section = true` when the page explicitly renders the no-tax-information empty state

## `parcel_lineage_links` v1 Field Set

v1 lineage should capture the minimum useful relationship details exposed in the parent/child parcel history modals.

- `parcel_id`
- `related_parcel_id`
- `relationship_type`
- `related_parcel_status`
- `relationship_note`
- `source_fetch_id`
- `built_at`

Notes:

- `related_parcel_status` should store values such as `Current` or `Retired` when they are shown.
- `relationship_note` should store the short description shown with the lineage record (for example, lot/CSM references), trimmed but not aggressively normalized.

## Proposed SQLAlchemy Models

Day 6 should implement both tables as standard SQLAlchemy models plus an Alembic migration.

Proposed shape:

```python
class ParcelCharacteristic(Base):
    __tablename__ = "parcel_characteristics"

    parcel_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("parcels.id"),
        primary_key=True,
    )
    source_fetch_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("fetches.id"),
        nullable=True,
    )

    formatted_parcel_number: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    state_municipality_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    township: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    range: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    section: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    quarter_quarter: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    has_dcimap_link: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    has_google_map_link: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    has_bing_map_link: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    current_assessment_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
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
    tax_jurisdiction_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    is_exempt_style_page: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    has_empty_valuation_breakout: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    has_empty_tax_section: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    built_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class ParcelLineageLink(Base):
    __tablename__ = "parcel_lineage_links"

    parcel_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("parcels.id"),
        primary_key=True,
    )
    related_parcel_id: Mapped[str] = mapped_column(String, primary_key=True)
    relationship_type: Mapped[str] = mapped_column(String, primary_key=True)

    source_fetch_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("fetches.id"),
        nullable=True,
    )
    related_parcel_status: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    relationship_note: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    built_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
```

## Exact Column Types

### `parcel_characteristics`

- `parcel_id`: `String`, non-null, primary key, foreign key to `parcels.id`
- `source_fetch_id`: `Integer`, nullable, foreign key to `fetches.id`
- `formatted_parcel_number`: `String`, nullable
- `state_municipality_code`: `String`, nullable
- `township`: `String`, nullable
- `range`: `String`, nullable
- `section`: `String`, nullable
- `quarter_quarter`: `String`, nullable
- `has_dcimap_link`: `Boolean`, nullable
- `has_google_map_link`: `Boolean`, nullable
- `has_bing_map_link`: `Boolean`, nullable
- `current_assessment_year`: `Integer`, nullable
- `current_valuation_classification`: `String`, nullable
- `current_assessment_acres`: `Numeric(10, 3)`, nullable
- `current_assessment_ratio`: `Numeric(8, 4)`, nullable
- `current_estimated_fair_market_value`: `Numeric(14, 2)`, nullable
- `current_tax_info_available`: `Boolean`, nullable
- `current_payment_history_available`: `Boolean`, nullable
- `tax_jurisdiction_count`: `Integer`, nullable
- `is_exempt_style_page`: `Boolean`, nullable
- `has_empty_valuation_breakout`: `Boolean`, nullable
- `has_empty_tax_section`: `Boolean`, nullable
- `built_at`: `DateTime(timezone=True)`, non-null, `server_default=func.now()`

### `parcel_lineage_links`

- `parcel_id`: `String`, non-null, primary key component, foreign key to `parcels.id`
- `related_parcel_id`: `String`, non-null, primary key component
- `relationship_type`: `String`, non-null, primary key component
- `source_fetch_id`: `Integer`, nullable, foreign key to `fetches.id`
- `related_parcel_status`: `String`, nullable
- `relationship_note`: `String`, nullable
- `built_at`: `DateTime(timezone=True)`, non-null, `server_default=func.now()`

Recommended initial indexes:

- `parcel_characteristics.source_fetch_id`
- `parcel_characteristics.current_valuation_classification`
- `parcel_characteristics.state_municipality_code`
- `parcel_lineage_links.related_parcel_id`
- `parcel_lineage_links.relationship_type`

## Nullability Rules

Important nullability expectations:

- Most fields are nullable because AccessDane page variants are inconsistent.
- `current_tax_info_available = false` is different from `NULL`:
  - `false` means the page explicitly said tax info is unavailable
  - `NULL` means the extractor could not determine availability
- `current_payment_history_available = false` is different from `NULL`:
  - `false` means the page explicitly showed only placeholder/no-history content
  - `NULL` means the extractor could not determine availability
- `tax_jurisdiction_count = 0` is valid when the section is present but empty
- `tax_jurisdiction_count = NULL` means the section was not parsed or not detectable

## Source Selection Rules

## 1. Parcel Universe

Create a `parcel_characteristics` row for every parcel that has at least one successful fetched page with usable parcel-summary or parcel-detail content.

Create `parcel_lineage_links` rows only when lineage relationships are explicitly present in the page content.

## 2. Winner Fetch For `parcel_characteristics`

For each parcel:

- consider only successful fetches (`status_code = 200`)
- consider only fetches whose raw HTML can be parsed into at least parcel-summary content

Choose one winner fetch for the full `parcel_characteristics` bundle.

Preferred winner order:

1. highest characteristic completeness score
2. latest `fetches.fetched_at`
3. greatest `fetch_id`

Bundle-level selection is intentional:

- do not mix characteristic fields across multiple fetches in v1
- keep one clear `source_fetch_id` per parcel row

## 3. Characteristic Completeness Score

The winner fetch should be the one with the most usable characteristic content.

Recommended completeness score:

- count non-null / non-empty values across:
  - `formatted_parcel_number`
  - `state_municipality_code`
  - `township`
  - `range`
  - `section`
  - `quarter_quarter`
  - `current_assessment_year`
  - `current_valuation_classification`
  - `current_assessment_acres`
  - `current_assessment_ratio`
  - `current_estimated_fair_market_value`
  - `tax_jurisdiction_count`

Plus:

- add 1 when any map link flag is `true`
- add 1 when the extractor can explicitly determine the tax section state
- add 1 when the extractor can explicitly determine the payment-history state

Why:

- A more complete but slightly older fetch is more useful than a newer fetch that exposes less parcel detail.

## 4. Merge Rules For `parcel_characteristics`

v1 uses winner-fetch bundle replacement, not field-level merge.

Rule:

- after selecting the winner fetch, populate all `parcel_characteristics` fields from that single fetch

Do not backfill missing fields from older fetches in v1.

Reasoning:

- field-level merges make provenance ambiguous
- the current Sprint 2 need is a stable, explainable current snapshot
- bundle-level merge keeps Day 6 and Day 7 implementation simpler and safer

## 5. Merge Rules For `parcel_lineage_links`

`parcel_lineage_links` should be rebuilt as a deduplicated union across successful fetches in scope.

For each parsed lineage relationship:

- normalize the relationship into (`parcel_id`, `related_parcel_id`, `relationship_type`)
- if duplicate links appear across multiple fetches, keep one row
- choose the latest fetch as the winner for `source_fetch_id`, `related_parcel_status`, and `relationship_note`
- tie-break with greatest `fetch_id`

This allows lineage to accumulate across fetches without duplicating the same relationship.

## 6. Relationship Type Normalization

Allowed v1 relationship values:

- `parent`
- `child`

Do not introduce more granular relationship types in v1.

If AccessDane later exposes more specific relationship semantics, add them in a future revision rather than overloading the v1 type values.

## Initial Residential Target

Sprint 2 should treat this as the first stable characteristic set to target for residential parcels:

- `formatted_parcel_number`
- `state_municipality_code`
- `township`
- `range`
- `section`
- `quarter_quarter`
- `has_dcimap_link`
- `has_google_map_link`
- `has_bing_map_link`
- `current_assessment_year`
- `current_valuation_classification`
- `current_assessment_acres`
- `current_assessment_ratio`
- `current_estimated_fair_market_value`

Why this set first:

- it is likely to be more consistently extractable than niche tax-detail or report-only fields
- it is immediately useful for segmentation, matching, and parcel-type profiling
- it avoids blocking Sprint 2 progress on the more speculative report-endpoint work

## Intentional v1 Exclusions

These are out of scope for `parcel_characteristics` v1:

- flattening full tax history into parcel-level columns
- storing payment totals in `parcel_characteristics` (Sprint 1 already covers payment rollups in `parcel_year_facts`)
- storing full recorded-document history in this table
- using report-endpoint-only fields that are not yet proven stable
- inferring parcel lineage from anything other than explicit parent/child page content

Rationale:

- v1 should favor fields that are stable and explainable from the current HTML corpus
- speculative or highly repeated datasets deserve separate normalized tables later

## Day 6 Implementation Mapping

Day 6 should implement:

- SQLAlchemy models for `parcel_characteristics` and `parcel_lineage_links`
- Alembic migration for both tables
- baseline tests that confirm the schema builds from an empty database

Day 7 should then implement:

- the first extractor/storage path for the initial residential characteristic set
- the first lineage extraction pass if the parser work is ready

## Known Risks / Open Questions

- Some parcel classes expose explicit empty-state pages instead of real tax or valuation data.
- Some fields may be easier to source from report endpoints than the default HTML pages.
- `quarter_quarter` and other legal-location fields may require careful normalization because AccessDane mixes display labels.
- The current static HTML may expose link availability more reliably than some deeper parcel-detail attributes.

These are implementation details to validate in later Sprint 2 days, not blockers for the schema design.
