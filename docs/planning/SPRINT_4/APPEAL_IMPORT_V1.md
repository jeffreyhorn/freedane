# Sprint 4 Appeal Import v1

Prepared on: 2026-03-06

## Purpose

Define the first stable import shape for appeal and Board of Review datasets used to explain assessed-value changes that are not driven by market-sale signals.

This document sets the v1 design for:

- `appeal_events`

It also defines:

- expected appeals CSV fields and header-alias handling
- normalization rules for hearing/decision dates, outcomes, and value fields
- parcel-linking strategy and fallback keys
- row-level rejection/audit behavior
- idempotent repeat-load semantics

The goal is to make Day 7 and Day 8 implementation deterministic instead of deciding appeal-ingest behavior ad hoc during coding.

## Scope

This is a v1 import design for appeal records from county or municipal exports, Board of Review records requests, or manually curated spreadsheets.

The design assumes:

- input is a flat CSV file
- one CSV row represents one appeal event record
- ingest is local-file driven (`accessdane ingest-appeals --file ...`)

This document does not define:

- permit ingest shape (already defined in `PERMIT_IMPORT_V1.md`)
- parcel-year rollup logic (Sprint 4 Day 9+)
- case-management UI/workflow
- OCR/PDF extraction logic

## Design Goals

- Keep ingest resilient to sparse, inconsistent, and manually entered source data.
- Preserve raw source values for auditability of import and normalization choices.
- Attach appeals to parcels when possible without dropping unlinked rows.
- Ensure same-file re-runs are idempotent.
- Keep schema and rules extensible for future records-request templates.

## Input Model

Each appeals CSV is treated as one load unit.

Every imported row must carry file-level metadata:

- `source_system`
- `source_file_name`
- `source_file_sha256`
- `source_row_number`
- `source_headers`
- `raw_row`
- `loaded_at`

Definitions:

- `source_row_number` is the 1-based physical CSV data-row index after the header line, counting every row in the file (including blank/empty lines).
- `source_file_sha256` is computed from raw file bytes exactly as read from disk.
- `source_headers` stores raw CSV header cells exactly as read, in original column order.
- normalized header keys are ingest-time only; v1 does not persist them.
- `raw_row` stores source values exactly as parsed from CSV (pre-normalization).

## `appeal_events`

### Grain

One row per imported appeal event record (import/audit grain, not deduplicated real-world case grain).

### Primary Key

- surrogate integer `id`

### Schema Columns

#### Source/Audit Fields

- `id`
- `source_system`
  - v1 constant: `manual_appeal_csv`
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
- `import_warnings` (nullable JSON array of non-fatal warning codes)
- `loaded_at`
- `updated_at`

#### Parcel-Linking Fields

- `parcel_number_raw` (nullable string)
- `parcel_number_norm` (nullable string)
- `site_address_raw` (nullable string)
- `site_address_norm` (nullable string)
- `owner_name_raw` (nullable string)
- `owner_name_norm` (nullable string)
- `parcel_id` (nullable FK to `parcels.id`)
- `parcel_link_method` (nullable string)
  - suggested values:
    - `exact_parcel_number`
    - `parcel_number_crosswalk`
    - `normalized_address`
    - `normalized_address_owner`
- `parcel_link_confidence` (nullable numeric(5,4))

#### Core Appeal Fields

- `appeal_number` (nullable string)
- `docket_number` (nullable string)
- `appeal_level_raw` (nullable string)
- `appeal_level_norm` (nullable string)
  - examples: `open_book`, `board_of_review`, `assessor_review`, `court`, `unknown`
- `appeal_reason_raw` (nullable text)
- `appeal_reason_norm` (nullable string)
  - examples: `valuation`, `classification`, `exemption`, `clerical_error`, `unknown`
- `filing_date` (nullable date)
- `hearing_date` (nullable date)
- `decision_date` (nullable date)
- `tax_year` (nullable integer)
- `outcome_raw` (nullable string)
- `outcome_norm` (nullable string)
  - examples: `reduction_granted`, `partial_reduction`, `denied`, `withdrawn`, `dismissed`, `pending`, `unknown`
- `assessed_value_before` (nullable numeric(14,2))
- `requested_assessed_value` (nullable numeric(14,2))
- `decided_assessed_value` (nullable numeric(14,2))
- `value_change_amount` (nullable numeric(14,2))
- `representative_name` (nullable string)
- `notes` (nullable text)

### Required vs Nullable Rules

Most business fields remain nullable in v1.

A row should be `loaded` when:

- CSV parsing succeeds
- row is not fully blank after trimming
- at least one parcel locator is present:
  - `parcel_number_raw` or `site_address_raw`
- at least one temporal anchor is present:
  - one of `filing_date`, `hearing_date`, `decision_date`, or `tax_year`
- at least one appeal signal is present:
  - one of `appeal_number`, `docket_number`, `outcome_raw`, `requested_assessed_value`, or `decided_assessed_value`

A row should be `rejected` when:

- row is fully blank
- no parcel locator is available
- no temporal anchor is available
- no appeal signal is available
- after parsing, no valid temporal anchor remains

Important v1 rule:

- rejected rows are still inserted into `appeal_events` with full audit metadata and `raw_row`.

## Expected CSV Fields

The importer should map source headers into canonical internal fields with an explicit alias map.

### Canonical v1 Fields

- `appeal_number`
- `docket_number`
- `appeal_level_raw`
- `appeal_reason_raw`
- `outcome_raw`
- `filing_date`
- `hearing_date`
- `decision_date`
- `tax_year`
- `assessed_value_before`
- `requested_assessed_value`
- `decided_assessed_value`
- `parcel_number_raw`
- `site_address_raw`
- `owner_name_raw`
- `representative_name`
- `notes`

### Header Normalization

Before applying alias and precedence rules, normalize every raw CSV header with this exact algorithm, in order:

1. Trim leading and trailing whitespace.
2. If, after trimming, the header ends with a single `:` or `#`, remove that one trailing character.
3. Convert the full header to lowercase.
4. Replace every run of one or more separator characters (space ` `, underscore `_`, or hyphen `-`) with a single space.
5. Collapse any remaining internal whitespace runs (space/tab) to a single space.

All alias lookups and header comparisons must use this normalized key and compare by exact string equality.

### Header Alias Strategy

Alias groups should cover common variations and records-request formats.

Examples:

- appeal number:
  - `Appeal Number`
  - `Appeal #`
  - `Case Number`
- docket number:
  - `Docket`
  - `Docket Number`
  - `BOR Docket`
- outcome:
  - `Outcome`
  - `Decision`
  - `Board Decision`
- tax year:
  - `Tax Year`
  - `Assessment Year`
  - `Year`
- requested value:
  - `Requested Value`
  - `Claimed Value`
  - `Requested Assessed Value`
- decided value:
  - `Final Value`
  - `Decided Value`
  - `Board Value`
- locator fields:
  - `Parcel Number`, `Parcel #`, `PIN`, `Tax Key`
  - `Address`, `Site Address`, `Property Address`

Deterministic rule:

- within each alias group, aliases are ordered highest to lowest precedence
- if multiple physical headers map to the same canonical field, use the first matching alias and ignore lower-precedence aliases

File-level failure conditions:

- unreadable CSV
- duplicate headers after normalization
- header set does not include any parcel locator header
- header set does not include any temporal anchor header

## Normalization Rules

### Strings

- trim leading and trailing whitespace
- collapse repeated internal whitespace for normalized comparison fields
- preserve source tokens/casing in `raw_row`

### Parcel Number

- `parcel_number_raw`: preserve source text when nonblank
- `parcel_number_norm`: uppercase plus separator removal (`-`, `/`, `.`, `_`, whitespace)
- null normalized value when raw is blank

### Address

- `site_address_raw`: preserve source value when nonblank
- `site_address_norm` algorithm:
  1. uppercase
  2. replace punctuation characters (`, . : ; # @ - / \\ ( ) ' "`) with spaces
  3. normalize all whitespace to single spaces
  4. trim
- do not remove unit/suite identifiers in v1

### Owner Name

- `owner_name_raw`: preserve source text when nonblank
- `owner_name_norm`: uppercase plus collapsed whitespace
- no person/entity parsing in v1

### Dates

Accepted formats:

- `MM/DD/YYYY`
- `YYYY-MM-DD`
- `MM-DD-YYYY`

If parse fails:

- keep original token in `raw_row`
- set normalized date field null
- add warning code (`filing_date_unparseable`, `hearing_date_unparseable`, `decision_date_unparseable`)

`tax_year` derivation:

- if explicit `tax_year` parses as a 4-digit year, keep it as authoritative
- otherwise derive from first available date in order:
  - `decision_date`
  - `hearing_date`
  - `filing_date`
- if explicit year disagrees with derived year:
  - keep explicit `tax_year`
  - add warning `tax_year_anchor_mismatch`
  - do not reject row solely for mismatch

### Outcome Mapping

`outcome_norm` should map common tokens case-insensitively:

- `granted`, `approved`, `reduction granted` -> `reduction_granted`
- `partial`, `partially granted`, `modified` -> `partial_reduction`
- `denied`, `upheld`, `no change` -> `denied`
- `withdrawn`, `withdraw` -> `withdrawn`
- `dismissed` -> `dismissed`
- `pending`, `continued`, `open` -> `pending`
- blank -> null
- any other nonblank token -> `unknown`

Unknown outcome tokens should not reject the row.

### Appeal Level Mapping

`appeal_level_norm` should map common tokens case-insensitively:

- `open book` -> `open_book`
- `board of review`, `bor` -> `board_of_review`
- `assessor review`, `informal review` -> `assessor_review`
- `court`, `circuit court`, `tax appeals commission` -> `court`
- blank -> null
- any other nonblank token -> `unknown`

### Appeal Reason Mapping

`appeal_reason_norm` should map common tokens case-insensitively:

Deterministic precedence rule:

- evaluate these mapping rules top-to-bottom and stop on first match
- this resolves overlapping phrases consistently (for example `valuation error` maps to `valuation`)

- contains `value` or `valuation` -> `valuation`
- contains `class` or `classification` -> `classification`
- contains `exempt` or `exemption` -> `exemption`
- contains `clerical` or `error` -> `clerical_error`
- blank -> null
- any other nonblank token -> `unknown`

### Amount Fields

Applicable columns:

- `assessed_value_before`
- `requested_assessed_value`
- `decided_assessed_value`

Rules:

- strip `$`, commas, and surrounding whitespace before parse
- support parenthesized negatives (for example `(12345.67)`)
- blank values become null
- unparseable tokens become null and emit warning code (`<field>_unparseable`)

Derived value:

- `value_change_amount = decided_assessed_value - assessed_value_before` when both are present
- otherwise null
- if computed change is positive for a row mapped to a reduction outcome, emit warning `outcome_value_direction_mismatch`

## Parcel Attachment Strategy

v1 attachment should be best-effort and non-destructive.

Candidate-key priority:

1. normalized parcel number exact match
2. parcel-number crosswalk fallback
3. normalized site address unique match
4. normalized site address plus owner-name tie-break

Set `parcel_id`, `parcel_link_method`, and `parcel_link_confidence` only when exactly one candidate remains.

Suggested confidence values:

- `exact_parcel_number`: `1.0000`
- `parcel_number_crosswalk`: `0.9500`
- `normalized_address_owner`: `0.9200`
- `normalized_address`: `0.9000`

If zero or multiple parcels remain:

- keep row `loaded` (if row meets shape requirements)
- leave `parcel_id` null
- retain locator fields for reconciliation

## Rejection Handling

### File-Level Rejection

Fail command before inserts when:

- file cannot be opened or decoded as UTF-8
- CSV parsing fails globally
- header normalization/recognition requirements are not met

### Row-Level Rejection

Insert row with `import_status = "rejected"` when:

- row is blank
- row has no parcel locator
- row has no valid temporal anchor
- row has no appeal signal

For rejected rows:

- preserve audit metadata
- preserve `raw_row`
- populate `import_error` with actionable reason text

## Repeat-Load Behavior

Idempotency boundary is file-hash plus row number:

- unique key: `(source_file_sha256, source_row_number)`
- rerunning the exact same file updates existing rows in place
- reruns must refresh warning/error fields and parcel-link fields using current normalization logic

Cross-file duplicates:

- same real-world appeal may appear in multiple files
- v1 allows this and preserves source-level provenance
- cross-file deduplication is deferred to later analysis/reconciliation stages

## Required Indexes

v1 should index at least:

- `appeal_events(source_file_sha256)`
- unique `appeal_events(source_file_sha256, source_row_number)`
- `appeal_events.import_status`
- `appeal_events.parcel_number_norm`
- `appeal_events.site_address_norm`
- `appeal_events.parcel_id`
- `appeal_events.tax_year`
- `appeal_events.outcome_norm`
- `appeal_events.hearing_date`

## Operator Summary Output

`ingest-appeals` should print summary counters mirroring permit ingest conventions:

- `total`, `loaded`, `rejected`, `inserted`, `updated`
- rejection reason counts
- warning code counts

## Open v1 Decisions To Revisit Later

- whether to support many-to-many parcel links per appeal event
- whether to split owner/appellant metadata into a separate party table
- whether to add explicit document URL or attachment fields
- whether to persist hearing-panel or member-level vote details

## Acceptance Checklist

The Day 7/Day 8 implementation should satisfy:

- appeal CSV rows can be loaded with robust handling of sparse/manual data
- loaded rows can attach to parcels when locator data is sufficient
- rejected rows are still auditable in `appeal_events`
- same-file re-runs are idempotent
- operator-facing summary diagnostics are explicit enough for recovery workflows
