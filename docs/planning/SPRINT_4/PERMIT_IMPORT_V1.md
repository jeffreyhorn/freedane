# Sprint 4 Permit Import v1

Prepared on: 2026-03-05

## Purpose

Define the first stable import shape for permit-event datasets used to explain assessment changes that are not driven by market sales.

This document sets the v1 design for:

- `permit_events`

It also defines:

- expected permit CSV fields and header-alias handling
- normalization rules for key permit attributes
- row-level rejection and audit behavior
- idempotent repeat-load semantics

The goal is to make Day 3/Day 4 implementation deterministic instead of deciding permit ingest behavior ad hoc during coding.

## Scope

This is a v1 import design for permit records coming from county/municipal exports or manually curated records-request spreadsheets.

The design assumes:

- input is a flat CSV file
- one CSV row represents one permit event record
- ingest is local-file driven (`accessdane ingest-permits --file ...`)

This document does not define:

- appeal ingest shape (Sprint 4 Day 6+)
- full analyst case-management workflow
- advanced geospatial matching requirements
- OCR/PDF extraction

## Design Goals

- Keep ingest resilient to sparse and inconsistent source data.
- Preserve enough raw source data for audit/debug of import and normalization decisions.
- Support parcel attachment when possible without dropping unlinked rows.
- Ensure same-file re-runs are idempotent.
- Keep schema and rules extensible for future records-request templates.

## Input Model

Each permit CSV is treated as one load unit.

Every imported row must carry file-level metadata:

- `source_system`
- `source_file_name`
- `source_file_sha256`
- `source_row_number`
- `source_headers`
- `raw_row`
- `loaded_at`

Definitions:

- `source_row_number` is the 1-based ordinal of parsed data records after the header.
- `source_file_sha256` is computed from raw file bytes exactly as read from disk.
- `raw_row` stores source values exactly as parsed from CSV (pre-normalization).

## `permit_events`

### Grain

One row per imported permit record (import/audit grain, not deduplicated real-world project grain).

### Primary Key

- surrogate integer `id`

### Required Columns

#### Source/Audit Fields

- `id`
- `source_system`
  - v1 constant: `manual_permit_csv`
- `source_file_name`
- `source_file_sha256`
- `source_row_number`
- `source_headers` (JSON array)
- `raw_row` (JSON object)
- `import_status`
  - enum-like string:
    - `loaded`
    - `rejected`
- `import_error` (nullable text)
- `loaded_at`
- `updated_at`

#### Parcel-Linking Fields

- `parcel_number_raw` (nullable string)
- `parcel_number_norm` (nullable string)
- `site_address_raw` (nullable string)
- `site_address_norm` (nullable string)
- `parcel_id` (nullable FK to `parcels.id`)
- `parcel_link_method` (nullable string)
  - suggested values:
    - `exact_parcel_number`
    - `parcel_number_crosswalk`
    - `normalized_address`
- `parcel_link_confidence` (nullable numeric(5,4))

#### Core Permit Fields

- `permit_number` (nullable string)
- `issuing_jurisdiction` (nullable string)
- `permit_type` (nullable string)
- `permit_subtype` (nullable string)
- `work_class` (nullable string)
  - examples: `new_construction`, `addition`, `alteration`, `repair`, `demolition`, `other`
- `permit_status_raw` (nullable string)
- `permit_status_norm` (nullable string)
  - examples: `applied`, `issued`, `finaled`, `expired`, `cancelled`, `unknown`
- `description` (nullable text)
- `owner_name` (nullable string)
- `contractor_name` (nullable string)
- `applied_date` (nullable date)
- `issued_date` (nullable date)
- `finaled_date` (nullable date)
- `status_date` (nullable date)
- `declared_valuation` (nullable numeric(14,2))
- `estimated_cost` (nullable numeric(14,2))
- `permit_year` (nullable integer)

### Required vs Nullable Rules

Most business fields remain nullable in v1.

A row should be `loaded` when:

- CSV parsing succeeds
- row is not fully blank after trimming
- at least one parcel locator is present:
  - `parcel_number_raw` or `site_address_raw`
- at least one temporal anchor is present:
  - one of `applied_date`, `issued_date`, `finaled_date`, `status_date`, or `permit_year`

A row should be `rejected` when:

- row is fully blank
- no parcel locator is available
- no temporal anchor is available
- numeric/date parsing fails for a value that is present in a required anchor field

Important v1 rule:

- rejected rows are still inserted into `permit_events` with full audit metadata and `raw_row`.

## Expected CSV Fields

The importer should map source headers into canonical internal fields with an explicit alias map.

### Canonical v1 Fields

- `permit_number`
- `issuing_jurisdiction`
- `permit_type`
- `permit_subtype`
- `work_class`
- `permit_status_raw`
- `description`
- `owner_name`
- `contractor_name`
- `parcel_number_raw`
- `site_address_raw`
- `applied_date`
- `issued_date`
- `finaled_date`
- `status_date`
- `permit_year`
- `declared_valuation`
- `estimated_cost`

### Header Alias Strategy

The alias map should cover common variations, e.g.:

- permit number:
  - `Permit Number`
  - `Permit #`
  - `Permit ID`
- parcel number:
  - `Parcel Number`
  - `Parcel #`
  - `PIN`
  - `Tax Key`
- site address:
  - `Address`
  - `Site Address`
  - `Property Address`
- issued date:
  - `Issued Date`
  - `Issue Date`
- finaled date:
  - `Finaled Date`
  - `Final Date`
  - `Closed Date`
- valuation/cost:
  - `Declared Valuation`
  - `Project Valuation`
  - `Estimated Cost`
  - `Project Cost`

Deterministic rule:

- if multiple physical headers map to the same canonical field, choose the highest-precedence alias from the documented list and ignore lower-precedence aliases.

File-level failure conditions:

- unreadable CSV
- duplicate identical header names in the same file
- no recognizable permit-shape headers

## Normalization Rules

### Strings

- trim leading/trailing whitespace
- collapse repeated internal whitespace for normalized comparison fields
- preserve original token/casing in `raw_row`

### Parcel Number

- `parcel_number_raw`: preserve trimmed source string
- `parcel_number_norm`: uppercase + remove spaces and common separators (`-`, `/`, `.`, `_`)
- null out normalized value when raw is blank

### Address

- `site_address_raw`: preserve trimmed source value
- `site_address_norm`: uppercase + collapse spaces + normalize punctuation to spaces
- do not drop unit/suite identifiers

### Dates

Accepted formats:

- `MM/DD/YYYY`
- `YYYY-MM-DD`
- `MM-DD-YYYY`

If parse fails:

- preserve token in `raw_row`
- set normalized date field null
- reject row only if it leaves the row without any valid temporal anchor

`permit_year` derivation:

- use explicit parsed year field if present and valid
- otherwise derive from first available anchor date in order:
  - `issued_date`
  - `finaled_date`
  - `status_date`
  - `applied_date`

### Currency/Amounts

- parse to `numeric(14,2)` after stripping currency symbols, commas, and whitespace
- keep literal zero as `0.00`
- blank values become null
- unparseable valuation/cost tokens do not reject row by themselves in v1; keep null and preserve raw token

### Status Mapping

`permit_status_norm` should map common source tokens case-insensitively:

- `applied`, `application received` -> `applied`
- `issued`, `approved` -> `issued`
- `final`, `finaled`, `closed`, `completed` -> `finaled`
- `expired` -> `expired`
- `cancelled`, `canceled`, `void` -> `cancelled`
- anything else:
  - keep `permit_status_raw`
  - set `permit_status_norm = "unknown"`

Unknown status does not reject the row.

## Parcel Attachment Behavior

v1 attachment behavior should be best-effort:

- attempt parcel link during ingest when `parcel_number_norm` or `site_address_norm` is available
- if exactly one parcel is found, set:
  - `parcel_id`
  - `parcel_link_method`
  - `parcel_link_confidence`
- if zero or multiple parcels are found:
  - keep row `loaded`
  - leave `parcel_id` null
  - keep locator fields for later reconciliation

This ensures permit context is ingestible even when parcel linkage is imperfect.

## Rejection Handling

### File-Level Rejection

Fail command before inserts when:

- file cannot be opened/read
- CSV decoding/parsing fails globally
- header recognition fails

### Row-Level Rejection

Insert row with `import_status = "rejected"` when:

- row is blank
- no parcel locator exists
- no valid temporal anchor exists after parsing

For rejected rows:

- keep audit metadata
- keep `raw_row`
- set `import_error` with actionable reason text

## Repeat-Load Behavior

Same-file re-run must be idempotent.

Enforce unique key:

- `unique(source_file_sha256, source_row_number)`

Importer behavior for duplicates:

- upsert existing row
- recompute normalized fields from current logic
- refresh `import_status` and `import_error`
- preserve first-seen `loaded_at`
- refresh `updated_at`

Different files containing same permit are allowed to create separate rows in v1.

## Day 3 Implementation Notes

Day 3 should implement:

- `permit_events` model with fields above
- Alembic migration with constraints/indexes
- migration tests

Recommended indexes:

- unique `permit_events(source_file_sha256, source_row_number)`
- `permit_events.import_status`
- `permit_events.parcel_number_norm`
- `permit_events.site_address_norm`
- `permit_events.parcel_id`
- `permit_events.permit_year`
- `permit_events.permit_status_norm`

## Open Questions Deferred Past v1

- whether to separate load metadata into a dedicated `permit_loads` table
- whether to persist multiple parcel-link candidates per permit (vs single best link)
- whether to split permit-status taxonomy by jurisdiction
- whether to attach source document URLs/binaries for permit evidence

## Acceptance For This Design

This design is complete enough for Day 3/Day 4 when:

- schema can be implemented without adding core columns mid-implementation
- importer can tolerate messy manual data without pipeline breakage
- same-file reloads remain idempotent
- permit rows can be attached to parcels when feasible and preserved when not
