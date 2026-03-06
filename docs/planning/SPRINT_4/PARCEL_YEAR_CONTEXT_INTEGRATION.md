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
2. else, for events with `parcel_number_norm`, match to `parcels.id` using exact normalized equality
3. else unresolved; do not include in parcel-year rollups

Notes:

- This fallback is required because appeals v1 currently preserves parcel locator fields
  but does not yet populate `appeal_events.parcel_id` during import.
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

- `permit_event_count` (`Integer`, not null default `0`)
- `permit_declared_valuation_known_count` (`Integer`, not null default `0`)
- `permit_declared_valuation_sum` (`Numeric(14,2)`, nullable)
- `permit_estimated_cost_known_count` (`Integer`, not null default `0`)
- `permit_estimated_cost_sum` (`Numeric(14,2)`, nullable)
- `permit_status_issued_count` (`Integer`, not null default `0`)
- `permit_status_finaled_count` (`Integer`, not null default `0`)
- `permit_status_cancelled_count` (`Integer`, not null default `0`)
- `permit_status_expired_count` (`Integer`, not null default `0`)
- `permit_recent_1y_count` (`Integer`, not null default `0`)
- `permit_recent_2y_count` (`Integer`, not null default `0`)
- `permit_has_recent_1y` (`Boolean`, not null default `false`)
- `permit_has_recent_2y` (`Boolean`, not null default `false`)

### Appeal rollups

- `appeal_event_count` (`Integer`, not null default `0`)
- `appeal_reduction_granted_count` (`Integer`, not null default `0`)
- `appeal_partial_reduction_count` (`Integer`, not null default `0`)
- `appeal_denied_count` (`Integer`, not null default `0`)
- `appeal_withdrawn_count` (`Integer`, not null default `0`)
- `appeal_dismissed_count` (`Integer`, not null default `0`)
- `appeal_pending_count` (`Integer`, not null default `0`)
- `appeal_unknown_outcome_count` (`Integer`, not null default `0`)
- `appeal_value_change_known_count` (`Integer`, not null default `0`)
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
- status counts use `permit_status_norm` exact values

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

- use explicit zero/false defaults when no matching events exist for that parcel-year
- this avoids null-as-zero ambiguity in downstream aggregations

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
