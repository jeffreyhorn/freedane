# Sprint 3 RETR Import v1

Prepared on: 2026-03-03

## Purpose

Define the first stable import shape for Wisconsin DOR RETR CSV exports.

This document sets the v1 design for:

- `sales_transactions`
- `sales_parcel_matches`
- `sales_exclusions`

It also defines:

- the expected RETR CSV columns the importer should recognize
- normalization rules for those columns
- row-level rejection behavior
- repeat-load semantics

The goal is to make Day 9 and Day 10 implementation work deterministic instead of deciding schema and ingest rules ad hoc during coding.

## Scope

This is a v1 import design for CSV exports produced from Wisconsin DOR RETR search/export workflows.

The design assumes:

- the input is a flat CSV file
- one CSV row represents one RETR transaction row from the export
- the import path is local-file driven (`accessdane ingest-retr --file ...`)

This document does not define:

- the full matching algorithm
- exclusion rule logic beyond schema needs
- external automation against the DOR site
- any attempt to reconcile duplicate transactions across different export files beyond explicit repeat-load rules

## Design Goals

- Preserve enough raw source data to debug import problems later.
- Normalize the core sale fields needed for matching and exclusion logic.
- Allow row-level rejection without losing auditability.
- Make reloading the same file idempotent.
- Avoid assuming a single RETR export layout will never drift; support header aliases where practical.
- Keep the schema narrow enough for Day 9 implementation, but not so narrow that Day 10 immediately needs a migration.

## Input Model

The importer should treat each RETR CSV as a single load unit.

The file-level metadata that must be captured on every imported row:

- `source_file_name`
- `source_file_sha256`
- `source_row_number`
- `loaded_at`

Definitions:

- `source_row_number` is the 1-based CSV data-row ordinal after the header row, so the first data row is `1`.
- The header row is not counted.
- `source_row_number` refers to the Nth parsed CSV record after the header, not the physical text-line number in the file.
- A fully blank physical line that produces no CSV record does not consume a row number.
- A parsed CSV record whose cells are all blank still consumes a row number, even if that row is later marked `rejected`.
- `source_file_sha256` is the SHA-256 of the raw file bytes exactly as read from disk.
- Do not decode first, normalize newlines, or otherwise transform the bytes before hashing.

Reasoning:

- There is no separate load-tracking table in v1.
- File metadata must therefore live on `sales_transactions` so every imported row remains auditable even if the original file is moved or deleted.

## `sales_transactions`

### Grain

One row per imported RETR CSV row.

This is an import/audit grain, not a deduplicated market-sale grain.

If the same real-world transfer appears in two different export files, v1 may store two rows.
That is acceptable because:

- it preserves the source audit trail
- exact duplicate collapse across different files can be handled later if needed
- idempotency for exact same-file reloads is still guaranteed separately

### Primary Key

- surrogate integer `id`

### Required Columns

These fields should exist physically in v1.

#### Source / Audit Fields

- `id`
- `source_system`
  - constant value: `wisconsin_dor_retr`
- `source_file_name`
- `source_file_sha256`
- `source_row_number`
- `source_headers`
  - JSON array of the original header names seen in the file
- `raw_row`
  - JSON object containing the original row values exactly as read from CSV
- `import_status`
  - enum-like string:
    - `loaded`
    - `rejected`
- `import_error`
  - nullable text describing why a row was rejected
- `loaded_at`
- `updated_at`

#### Stable Source Identity Fields

These are imported even when they remain nullable because they are the closest thing to a source-side transaction identity.

- `revenue_object_id`
  - nullable string for any export field that behaves like a RETR row or receipt identifier
- `document_number`
  - nullable string
- `county_name`
  - nullable string
- `municipality_name`
  - nullable string

#### Core Transaction Fields

These are the primary normalized fields needed for downstream analytics.

- `transfer_date`
  - nullable `date`
- `recording_date`
  - nullable `date`
- `consideration_amount`
  - nullable `numeric(14, 2)`
- `property_type`
  - nullable string
- `conveyance_type`
  - nullable string
- `deed_type`
  - nullable string
- `grantor_name`
  - nullable string
- `grantee_name`
  - nullable string

#### Matching Inputs

These should be preserved even if they are messy because they drive Day 12 matching.

- `official_parcel_number_raw`
  - nullable string
- `official_parcel_number_norm`
  - nullable string
- `property_address_raw`
  - nullable string
- `property_address_norm`
  - nullable string
- `legal_description_raw`
  - nullable text
- `school_district_name`
  - nullable string

#### Initial Classification / Review Fields

These support exclusion logic and human follow-up without requiring another table change immediately.

- `arms_length_indicator_raw`
  - nullable string
- `arms_length_indicator_norm`
  - nullable boolean
- `usable_sale_indicator_raw`
  - nullable string
- `usable_sale_indicator_norm`
  - nullable boolean
- `review_status`
  - enum-like string:
    - `unreviewed`
    - `needs_review`
    - `reviewed`
- `review_notes`
  - nullable text

### Required vs Nullable Rules

The table schema should allow most business fields to remain nullable.
The importer should require only the audit fields and enough raw row content to preserve traceability.

The `loaded` / `rejected` decision should follow one rule set, not two phases with different acceptance criteria.

Rows should be accepted as `loaded` when:

- CSV parsing succeeds
- file-level required metadata is available
- the row contains at least one non-empty field after trimming
- all v1-critical fields required for downstream use are present
- all present v1-critical fields parse successfully

V1-critical values for `loaded` rows are:

- `transfer_date`
- `consideration_amount`
- at least one identifying input:
  - `official_parcel_number_raw`, or
  - `property_address_raw`, or
  - `legal_description_raw`

Rows should be marked `rejected` when:

- the row is structurally unreadable
- all relevant cells are empty
- any v1-critical field is missing when required for `loaded`
- a required normalization step for a v1-critical field fails in a way that makes the row unusable

Important v1 rule:

- rejected rows still get a `sales_transactions` row
- they keep `raw_row`, source metadata, and `import_error`
- their normalized business fields may remain null

That preserves auditability and prevents silent data loss.

## `sales_parcel_matches`

### Grain

One row per candidate transaction-to-parcel match.

This is intentionally not limited to the final accepted match.
The matcher should be able to persist multiple candidate links for one transaction.

### Primary Key

- surrogate integer `id`

### Required Columns

- `id`
- `sales_transaction_id`
  - FK to `sales_transactions.id`
- `parcel_id`
  - FK to `parcels.id`
- `match_method`
  - enum-like string:
    - `exact_parcel_number`
    - `normalized_address`
    - `legal_description`
    - `manual`
- `confidence_score`
  - numeric(5, 4), nullable
- `match_rank`
  - integer, nullable
- `is_primary`
  - boolean, defaults false
- `match_review_status`
  - enum-like string:
    - `auto_accepted`
    - `needs_review`
    - `confirmed`
    - `rejected`
- `matched_value`
  - nullable string describing the normalized token/value that produced the match
- `matcher_version`
  - nullable string
- `matched_at`
  - timestamp

### Uniqueness Rule

v1 should enforce uniqueness on:

- (`sales_transaction_id`, `parcel_id`, `match_method`)

Additional invariant:

- at most one row per `sales_transaction_id` may have `is_primary = true`

Reasoning:

- the same matching pass should not create duplicate candidate rows
- multiple different methods can still point at the same parcel
- downstream code should be able to rely on a single primary match when one exists

## `sales_exclusions`

### Grain

One row per exclusion reason applied to one transaction.

This should remain a separate table instead of a single boolean on `sales_transactions`.
One transfer can be excluded for multiple independent reasons.

### Primary Key

- surrogate integer `id`

### Required Columns

- `id`
- `sales_transaction_id`
  - FK to `sales_transactions.id`
- `exclusion_code`
  - stable machine-readable code such as:
    - `non_arms_length`
    - `family_transfer`
    - `government_transfer`
    - `corrective_deed`
    - `missing_consideration`
- `exclusion_reason`
  - human-readable explanation
- `is_active`
  - boolean, defaults true
- `excluded_by_rule`
  - nullable string naming the rule or workflow that applied it
- `excluded_at`
  - timestamp
- `notes`
  - nullable text

### Uniqueness Rule

v1 should enforce uniqueness on:

- (`sales_transaction_id`, `exclusion_code`)

## Expected RETR CSV Columns

The importer should normalize source headers into a canonical internal field set.

Because export headers may drift slightly, v1 should support reasonable header aliases instead of hard-coding one exact spelling per field.

### Canonical v1 Fields

The importer should try to populate these internal fields from the CSV:

- `revenue_object_id`
- `document_number`
- `county_name`
- `municipality_name`
- `transfer_date`
- `recording_date`
- `consideration_amount`
- `property_type`
- `conveyance_type`
- `deed_type`
- `grantor_name`
- `grantee_name`
- `official_parcel_number_raw`
- `property_address_raw`
- `legal_description_raw`
- `school_district_name`
- `arms_length_indicator_raw`
- `usable_sale_indicator_raw`

### Header Alias Strategy

The importer should recognize common variations for the same concept, for example:

- parcel number:
  - `Parcel Number`
  - `Parcel #`
  - `Official Parcel Number`
  - `PIN`
- transfer date:
  - `Transfer Date`
  - `Conveyance Date`
  - `Sale Date`
- recording date:
  - `Recording Date`
  - `Recorded Date`
- consideration:
  - `Consideration`
  - `Sale Price`
  - `Value`
- grantor:
  - `Grantor`
  - `Seller`
- grantee:
  - `Grantee`
  - `Buyer`

V1 does not need to recognize every possible historical RETR export heading.
But the alias map should be explicit and easy to extend.

### File-Level Failure Rule

The importer should reject the entire file before inserting rows if:

- no recognizable headers for the core RETR export shape are found
- duplicate header names make row parsing ambiguous

This is a file-level setup error, not a row-level data-quality issue.

## Normalization Rules

### Strings

- Trim leading/trailing whitespace.
- Collapse repeated internal whitespace to a single space for normalized fields.
- Preserve original spacing/casing in `raw_row`.
- Use uppercase normalization only for comparison-oriented fields where useful:
  - `official_parcel_number_norm`
  - `property_address_norm`

### Parcel Number

- Preserve the exported parcel number exactly in `official_parcel_number_raw`.
- Build `official_parcel_number_norm` by:
  - trimming
  - uppercasing
  - removing spaces
  - removing obvious formatting separators (`-`, `/`, `.`, `_`)
- Leave the normalized value null if the raw value is blank after trimming.

The importer should not try to map RETR parcel numbers to AccessDane `parcel_id` during ingest.
That belongs in `sales_parcel_matches`.

### Address

- Preserve the exported address exactly in `property_address_raw`.
- Build `property_address_norm` by:
  - trimming
  - uppercasing
  - collapsing repeated spaces
  - normalizing common punctuation to spaces
  - preserving apartment/unit text rather than dropping it

Do not over-normalize in v1.
The goal is stable matching input, not full postal standardization.

### Dates

- Parse into `date` values.
- Accept common export formats:
  - `MM/DD/YYYY`
  - `YYYY-MM-DD`
- If a date field is present but unparseable:
  - keep the raw value only in `raw_row`
  - mark the row `rejected` if that date is required for downstream use in v1
- If `transfer_date` is blank or missing:
  - keep the raw row
  - mark the row `rejected`
  - set `import_error` to a transfer-date-required message

V1 critical date:

- `transfer_date`

`recording_date` may remain null without forcing rejection.

### Currency / Amounts

- Parse money-like fields into `numeric(14, 2)`.
- Strip currency symbols, commas, and surrounding whitespace.
- Preserve a literal zero as `0.00`.
- Treat blank values as null.

`consideration_amount` should cause a row-level rejection when:

- the source value is blank or missing, so the required v1 `consideration_amount` cannot be populated
- the source value is present but cannot be parsed as a decimal amount

Important distinction:

- a literal exported zero should load as `0.00`
- a blank consideration value should not be treated as zero

### Booleans

For normalized boolean flags (`arms_length_indicator_norm`, `usable_sale_indicator_norm`):

- trim surrounding whitespace before comparison
- compare case-insensitively after normalization
- accept `Y`, `Yes`, `True`, `1` as true
- accept `N`, `No`, `False`, `0` as false
- preserve the original token in the corresponding `_raw` field
- leave the normalized field null when the source token is blank or unrecognized

Unrecognized values should not reject the row in v1.
They should remain reviewable.

## Rejection Handling

### File-Level Rejection

The importer should fail the command before inserting anything when:

- the file cannot be opened
- CSV decoding fails at the file level
- required header recognition fails

### Row-Level Rejection

The importer should insert a `sales_transactions` row with `import_status = "rejected"` when:

- the row is blank
- a v1-critical field has invalid syntax
- normalization detects a structural issue that makes the row unusable

If a row fails one of those requirements:

- keep the raw data
- set `import_status = "rejected"`
- populate `import_error`

This preserves row counts and auditability while still protecting downstream logic from malformed data.

## Repeat-Load Behavior

### Same File Re-Run

Reloading the exact same file should be idempotent.

V1 should enforce a uniqueness constraint on:

- (`source_file_sha256`, `source_row_number`)

Importer behavior for that case:

- upsert the existing row instead of creating a duplicate
- replace normalized values, `raw_row`, and `import_error`
- refresh `updated_at`
- keep the same `id`

Reasoning:

- this makes repeated local imports safe
- it supports importer fixes without requiring manual cleanup

### Different File, Same Real-World Transaction

Do not attempt cross-file deduplication in v1.

If two different files contain what looks like the same real-world sale:

- both rows may exist in `sales_transactions`
- later logic may match both rows, exclude one, or flag them for review

That is safer than guessing transaction equivalence too early.

## Day 9 Implementation Notes

Day 9 should implement:

- SQLAlchemy models for the three tables above
- migration-managed indexes and uniqueness constraints
- enum-like fields as strings with documented allowed values (not DB-native enums)

Recommended initial indexes:

- `sales_transactions.source_file_sha256`
- unique `sales_transactions(source_file_sha256, source_row_number)`
- `sales_transactions.transfer_date`
- `sales_transactions.official_parcel_number_norm`
- `sales_transactions.property_address_norm`
- `sales_transactions.import_status`
- unique `sales_parcel_matches(sales_transaction_id, parcel_id, match_method)`
- partial unique `sales_parcel_matches(sales_transaction_id)` where `is_primary = true`
- `sales_parcel_matches.parcel_id`
- `sales_parcel_matches.match_review_status`
- unique `sales_exclusions(sales_transaction_id, exclusion_code)`

## Open Questions Deferred Past v1

- Whether to add a dedicated `sales_loads` table for file-level metadata.
- Whether a durable source-side unique transaction key exists in all useful RETR exports.
- Whether duplicate transactions across overlapping exports should be merged automatically.
- Whether deed/reference fields should be split into a separate source-details JSON column.
- Whether transaction-level `review_status` and match-level `match_review_status` should eventually move into separate analyst-workflow tables.

## Acceptance For This Design

This design is complete enough for Day 9 / Day 10 when:

- the schema can be implemented without inventing new core columns mid-implementation
- the importer can distinguish file-level failures from row-level rejections
- the same file can be reloaded safely
- later matching work has preserved parcel number, address, and legal-description inputs
