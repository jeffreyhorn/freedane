# Parcel-Year Permit/Appeal Integration (Sprint 4)

Prepared on: 2026-03-06

## Purpose

Define the v1 integration contract for rolling `permit_events` and `appeal_events`
into `parcel_year_facts`.

This is the design target for Sprint 4 Day 10 implementation.

## Scope

In scope:

- annual permit rollups in `parcel_year_facts`
- annual appeal rollups in `parcel_year_facts`
- recent-permit indicators for analyst context
- null/unknown semantics for robust downstream analysis
- scoped/full rebuild semantics and idempotency expectations

Out of scope:

- changing permit/appeal import schemas
- redesigning permit/appeal parcel-linking at ingest time
- UI/report presentation details (handled in Day 11)

## Source Inputs

Primary sources:

- `permit_events`
- `appeal_events`
- existing `parcel_year_facts` source tables (`assessments`, `taxes`, `payments`, `parcel_summaries`)

Event inclusion rule:

- include only rows where `import_status = 'loaded'`

## Parcel And Year Resolution

### Parcel resolution

Each event must resolve to an `effective_parcel_id` before it can contribute to
`parcel_year_facts`.

Resolution order:

1. use `event.parcel_id` when present
2. else, for events with `parcel_number_norm`, attempt normalized parcel lookup in two stages:
   1. match to normalized `parcels.id`
   2. if no match, match to normalized `parcel_characteristics.formatted_parcel_number`
3. else (or if both normalized lookups fail) unresolved; do not include in parcel-year rollups

Notes:

- This fallback is required because appeals v1 currently preserves parcel locator fields
  but does not yet populate `appeal_events.parcel_id` during import.
- For this integration step, we intentionally stop after those two normalized parcel-number
  lookups; we do not attempt slash-based crosswalk candidate generation or address-based
  fallback matching before marking an event unresolved.
- The alignment with permit matching behavior is limited to using the same normalized
  parcel-number indexes on `parcels` and `parcel_characteristics`.
- Unresolved events remain auditable in event tables and are intentionally not dropped.

### Year resolution

- permit year key: `permit_events.permit_year`
- appeal year key: `appeal_events.tax_year`

Event rows with null year after import normalization are excluded from annual rollups.

### Parcel-year universe

Current year universe is built from assessments/taxes/payments only.

For v1 context integration, year universe must be expanded to include years present in:

- resolved permit events
- resolved appeal events

This ensures permit/appeal context appears even when no tax/assessment row exists for
that parcel-year.

## Proposed Parcel-Year Fields

Add the following columns to `parcel_year_facts`.

### Permit rollups

- `permit_event_count` (`Integer`, nullable)
- `permit_declared_valuation_known_count` (`Integer`, nullable)
- `permit_declared_valuation_sum` (`Numeric(14,2)`, nullable)
- `permit_estimated_cost_known_count` (`Integer`, nullable)
- `permit_estimated_cost_sum` (`Numeric(14,2)`, nullable)
- `permit_status_applied_count` (`Integer`, nullable)
- `permit_status_issued_count` (`Integer`, nullable)
- `permit_status_finaled_count` (`Integer`, nullable)
- `permit_status_cancelled_count` (`Integer`, nullable)
- `permit_status_expired_count` (`Integer`, nullable)
- `permit_status_unknown_count` (`Integer`, nullable)
- `permit_recent_1y_count` (`Integer`, nullable)
- `permit_recent_2y_count` (`Integer`, nullable)
- `permit_has_recent_1y` (`Boolean`, nullable)
- `permit_has_recent_2y` (`Boolean`, nullable)

### Appeal rollups

- `appeal_event_count` (`Integer`, nullable)
- `appeal_reduction_granted_count` (`Integer`, nullable)
- `appeal_partial_reduction_count` (`Integer`, nullable)
- `appeal_denied_count` (`Integer`, nullable)
- `appeal_withdrawn_count` (`Integer`, nullable)
- `appeal_dismissed_count` (`Integer`, nullable)
- `appeal_pending_count` (`Integer`, nullable)
- `appeal_unknown_outcome_count` (`Integer`, nullable)
- `appeal_value_change_known_count` (`Integer`, nullable)
- `appeal_value_change_total` (`Numeric(14,2)`, nullable)
- `appeal_value_change_reduction_total` (`Numeric(14,2)`, nullable)
- `appeal_value_change_increase_total` (`Numeric(14,2)`, nullable)

## Rollup Definitions

### Permit annual metrics

For each `(parcel_id, year)`:

- `permit_event_count` = count of included permit events with `permit_year = year`
- `permit_declared_valuation_known_count` = count where `declared_valuation is not null`
- `permit_declared_valuation_sum` = sum of non-null `declared_valuation`
- `permit_estimated_cost_known_count` = count where `estimated_cost is not null`
- `permit_estimated_cost_sum` = sum of non-null `estimated_cost`
- status counts use `permit_status_norm` exact values:
  - `applied`
  - `issued`
  - `finaled`
  - `cancelled`
  - `expired`
  - `unknown`
- events where `permit_status_norm` is null are excluded from `permit_status_*_count`
  buckets but still included in `permit_event_count`

Recent indicators for each `(parcel_id, year)`:

- `permit_recent_1y_count` = permit events where `permit_year in {year, year - 1}`
- `permit_recent_2y_count` = permit events where `permit_year in {year, year - 1, year - 2}`
- `permit_has_recent_1y` = `permit_recent_1y_count > 0`
- `permit_has_recent_2y` = `permit_recent_2y_count > 0`

### Appeal annual metrics

For each `(parcel_id, year)`:

- `appeal_event_count` = count of included appeal events with `tax_year = year`
- outcome counts map from `outcome_norm` buckets:
  - `reduction_granted`
  - `partial_reduction`
  - `denied`
  - `withdrawn`
  - `dismissed`
  - `pending`
  - null or any other value -> `appeal_unknown_outcome_count`
- `appeal_value_change_known_count` = count where `value_change_amount is not null`
- `appeal_value_change_total` = sum of non-null `value_change_amount`
- `appeal_value_change_reduction_total` = sum where `value_change_amount < 0`
- `appeal_value_change_increase_total` = sum where `value_change_amount > 0`

## Null / Unknown Semantics

Counts and booleans:

- keep nullable to align with existing `parcel_year_facts` rollups
  (for example `payment_event_count` and `payment_has_placeholder_row`)
- evaluate null vs zero/false per metric event set:
  - annual metrics use events where `event_year = year`
  - recent-window metrics use their own defined windows
- set a count/boolean to null when no matching events exist for that metric event set
  and parcel-year
- use numeric zero / false only when at least one contributing event exists in that
  metric event set and the true computed value is actually zero / false
- valid combinations include:
  - `permit_event_count = null` with `permit_has_recent_1y = true` when only `year - 1`
    permits exist
  - `permit_event_count = null` with `permit_recent_2y_count > 0` when only `year - 1`
    and/or `year - 2` permits exist

Numeric sums:

- keep nullable
- set to null when no contributing non-null values exist
- set to numeric value (including `0.00`) when at least one contributing value exists

Unknown outcomes:

- include in `appeal_unknown_outcome_count` instead of dropping them

## Rebuild And Idempotency Semantics

Rebuild path remains `rebuild_parcel_year_facts(...)`.

Full rebuild behavior:

- delete all rows in `parcel_year_facts`
- regenerate deterministic rows from source tables + permit/appeal rollups

Scoped rebuild behavior (`parcel_ids` provided):

- delete only facts for selected parcels
- regenerate only selected parcels
- recent indicators are computed from events for the same selected parcel only

Idempotency expectations:

- repeated rebuilds with unchanged source data produce the same values
- same-file permit/appeal reimports remain deduplicated upstream via
  `(source_file_sha256, source_row_number)` unique constraints

## Day 10 Implementation Notes

1. Add migration for new `parcel_year_facts` columns.
2. Extend rebuild pipeline to load and group resolved permit/appeal events by parcel-year.
3. Expand year universe to include permit and appeal years.
4. Populate new rollup fields in `_build_fact_row`.
5. Add integration tests for:
   - permit/appeal year-only rows with no assessment/tax/payment
   - recent-permit indicator windows
   - appeal outcome buckets and value-change sums
   - scoped rebuild correctness

## Acceptance Criteria For This Design

- A parcel-year row can represent permit/appeal context even when core tax/assessment
  data is absent for that year.
- All loaded permit/appeal events with resolvable parcel + year contribute exactly once.
- Unknown and sparse event data stay analyzable without silent dropping.
