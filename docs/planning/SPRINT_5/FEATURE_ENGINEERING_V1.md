# Sprint 5 Feature Engineering v1 Spec

Prepared on: 2026-03-08

## Purpose

Define the deterministic feature-generation contract for `accessdane build-features` before implementation work starts.

This spec translates Sprint 5 feature goals into build-ready definitions for:

- feature formulas
- null/default behavior
- quality guardrails
- versioning and run identity
- full vs scoped rebuild controls

## Scope

In scope for v1:

- deterministic writes to `parcel_features`
- one feature row per `(parcel_id, year, feature_version)`
- explainable `source_refs_json` and `feature_quality_flags`
- compatibility with `score-fraud` v1 contracts in `FRAUD_SIGNAL_V1.md`, with `build_features` scope semantics defined in this spec (`scope_json` normalized; raw operator inputs recorded separately)

Compatibility note:

- for `build_features`, `scoring_runs.scope_json` intentionally stores resolved/normalized scope for deterministic hashing/idempotency
- exact raw operator input scope is captured separately via run-logging payloads (for example `input_summary_json`)
- Sprint 5 docs must use this split consistently

Out of scope for v1:

- ML-derived features
- geometry-dependent features as hard requirements
- non-deterministic sampling or stochastic imputations

## Source Inputs

Primary source table:

- `parcel_year_facts` (assessment, permit, appeal, municipality/class context)

Supporting source tables:

- `sales_transactions`
- `sales_parcel_matches`
- `sales_exclusions`
- `parcel_lineage_links`

Run ledger and persistence:

- `scoring_runs` (`run_type = build_features`)
- `parcel_features`

## Build Grain

One output row per `(parcel_id, year, feature_version)`.

Candidate rows are selected from `parcel_year_facts` for the resolved scope.

Deterministic row order for processing and insertion:

- sort by `parcel_id` ascending, then `year` ascending

## Scope Resolution

`build-features` v1 scope controls:

- full build: no `--id`, no `--ids`, no `--year`
- scoped build by parcel IDs: `--id` repeatable or `--ids` file
- scoped build by year: `--year` repeatable
- combined scope: intersection of selected parcels and years

Scope normalization rules:

- trim IDs, deduplicate, sort ascending
- deduplicate years, sort ascending
- reject empty normalized ID scope when user explicitly passed `--id/--ids`

Resolved, normalized scope is persisted in `scoring_runs.scope_json` (this is the scope used for candidate row selection and deterministic hashing/idempotency).
Raw operator scope inputs (exact CLI flags/arguments) are recorded separately per the Sprint 5 run-logging contract (for example `scoring_runs.input_summary_json`) so original operator intent remains reconstructable.

## Deterministic Run Identity

For each `build_features` run:

- `scoring_runs.run_type = build_features`
- `scoring_runs.version_tag = <feature_version>`
- `scoring_runs.scope_hash = sha256(canonical_json({"scope": scope_json, "config": config_json}))`

Canonical serialization requirement (must match existing `sales_ratio_study` `_scope_hash` behavior):

- serialize with sorted keys (`sort_keys = true`)
- serialize with compact separators (`separators = (",", ":")`)
- encode serialized JSON as UTF-8 bytes before hashing
- persist hash output as a lowercase hex digest string (`sha256(...).hexdigest()`)

`config_json` must include at least:

- `feature_version`
- `sale_selection_strategy = latest_eligible_sale_same_year`
- `peer_percentile_min_group_size = 2`
- `permit_capture_rate = 0.75`
- `appeal_window_years = 3`
- `lineage_reference_year_offset = -1`

Any formula or threshold change must modify `config_json` and therefore `scope_hash`.

## Canonical Supporting Rules

The following sections define canonical, deterministic rules shared across feature calculations to keep outputs reproducible across reruns and implementations.

## Eligible Sale Selection

Eligible sales for parcel/year feature use:

- `sales_transactions.import_status = loaded`
- primary match exists in `sales_parcel_matches` for parcel
- `transfer_date` in target feature year
- `consideration_amount > 0`
- no active exclusion in `sales_exclusions`

If multiple eligible sales exist for the same parcel/year:

- choose latest `transfer_date`
- tie-break by highest `sales_transactions.id`

This selected sale is the canonical source for ratio-based feature inputs.

## Peer Group Key

Peer grouping key for ratio percentile:

- `year`
- `municipality_name` (casefolded)
- `assessment_valuation_classification` (casefolded)

Rows missing municipality or classification do not contribute to peer percentile and receive a quality flag.

## Feature Definitions

All decimals are quantized to destination column precision before write.

Rounding and overflow rule:

- quantize to destination scale using `ROUND_HALF_UP`
- if a rounded value still exceeds destination `Numeric(p,s)` precision, clamp to nearest representable bound and add a quality flag

Overflow quality flag:

- `numeric_precision_clamped`

## 1) `assessment_to_sale_ratio` (`Numeric(10,6)`)

Definition:

- `assessment_total_value / selected_sale_consideration_amount`

Inputs:

- `parcel_year_facts.assessment_total_value`
- canonical eligible sale amount for same parcel/year

Null/default semantics:

- null if assessment is null
- null if assessment is negative
- null if no eligible sale exists
- null if sale amount is non-positive

Quality flags:

- `missing_assessment_total_value`
- `invalid_assessment_total_value`
- `missing_eligible_sale`
- `invalid_sale_amount`

## 2) `peer_percentile` (`Numeric(6,4)`, persisted range `[0,1]`)

Definition:

- percentile rank of parcel `assessment_to_sale_ratio` within its peer key
- formula: `(count(lower) + 0.5 * count(equal)) / N`
- mathematical (pre-quantization) output is strictly `(0,1)` for valid `N`
- persisted output is quantized to `Numeric(6,4)`, so stored values may be `0.0000` or `1.0000` after rounding

Inputs:

- non-null `assessment_to_sale_ratio` values in same peer key
- this non-null population is `N` for percentile computation and is also the basis for `peer_percentile_min_group_size` and `source_refs_json.peer_group.group_size`

Null/default semantics:

- null if parcel ratio is null
- null if peer key is missing municipality/class
- null if peer group size `< peer_percentile_min_group_size`

Quality flags:

- `missing_peer_group_dimensions`
- `insufficient_peer_group`

## 3) `yoy_assessment_change_pct` (`Numeric(10,6)`)

Definition:

- `(current_assessment_total_value - prior_year_assessment_total_value) / prior_year_assessment_total_value`

Inputs:

- `parcel_year_facts.assessment_total_value` for `year` and `year - 1`

Null/default semantics:

- null if current or prior value missing
- null if prior value `<= 0`

Quality flags:

- `missing_prior_year_assessment`
- `nonpositive_prior_year_assessment`

## 4) `permit_adjusted_expected_change` (`Numeric(14,2)`)

Definition:

- expected assessment delta from permit investment signal
- basis selection order:
  1. `permit_declared_valuation_sum` when known count `> 0`
  2. `permit_estimated_cost_sum` when known count `> 0`
  3. `0.00` when no permit valuation/cost basis is present for the year
- expected change amount: `basis * permit_capture_rate`

Inputs:

- `permit_declared_valuation_known_count`
- `permit_declared_valuation_sum`
- `permit_estimated_cost_known_count`
- `permit_estimated_cost_sum`
- `permit_event_count`

Null/default semantics:

- `0.00` when permit basis fields are absent (`permit_event_count` and basis sums are null/zero for the year)
- null when permit activity exists but no declared/estimated valuation basis is known (for example `permit_event_count > 0` with both valuation known-count fields `<= 0`); emit `unresolved_permit_basis` and trace `permits.basis = unknown`
- null when row-level permit context is internally inconsistent (for example known count `> 0` with missing corresponding sum); emit `missing_permit_context` and trace `permits.basis = unknown`

Note on current mart semantics:

- current `parcel_year_facts` permit rollups may leave `permit_event_count` as null for zero-event years, so v1 treats "no observed permit basis" as `0.00` and MUST emit `permit_zero_signal_inferred`.

Quality flags:

- `missing_permit_context`
- `unresolved_permit_basis`
- `permit_zero_signal_inferred`

Permit quality-flag mapping:

- `missing_permit_context`: permit context is internally inconsistent and cannot produce a deterministic non-null permit basis
- `unresolved_permit_basis`: permit activity exists but both declared and estimated basis inputs are unresolved for value computation
- `permit_zero_signal_inferred`: v1 inferred a deterministic zero signal from absent/null permit basis fields per current mart semantics

## 5) `permit_adjusted_gap` (`Numeric(14,2)`)

Definition:

- `actual_assessment_change_amount - permit_adjusted_expected_change`
- `actual_assessment_change_amount = current_assessment_total_value - prior_year_assessment_total_value`

Null/default semantics:

- null if expected change is null
- null if current/prior assessment values are missing

Quality flags:

- `missing_permit_adjusted_expected_change`
- `missing_assessment_for_permit_gap`

## 6) `appeal_value_delta_3y` (`Numeric(14,2)`)

Definition:

- sum of `appeal_value_change_total` across `[year-2, year]` for same parcel

Null/default semantics:

- null if all 3 years are missing appeal context
- otherwise treat missing yearly values as `0.00` and return rolling sum

Quality flags:

- `missing_appeal_context_3y`

## 7) `appeal_success_rate_3y` (`Numeric(6,4)`, range `[0,1]`)

Definition:

- rolling success rate across `[year-2, year]`
- numerator: `sum(appeal_reduction_granted_count + appeal_partial_reduction_count)`
- denominator: `sum(appeal_event_count)`
- success rate: `numerator / denominator`

Null/default semantics:

- evaluate each year in `[year-2, year]` independently from that year's appeal context
- if all 3 years have no appeal context, return null
- otherwise treat missing yearly appeal counts as `0` in numerator/denominator rollups
- after summing, return null if denominator `<= 0`

Quality flags:

- `missing_appeal_context_3y`
- `no_appeals_in_window`

## 8) `lineage_value_reset_delta` (`Numeric(14,2)`)

Definition:

- lineage reset signal comparing current parcel value to related lineage baseline
- baseline source:
  - related parcel IDs from `parcel_lineage_links` for target parcel, deduplicated by `related_parcel_id` before any aggregation
  - reference values from related parcels at `year - 1`
- delta: `current_assessment_total_value - sum(related_prior_year_assessment_total_values)`

Null/default semantics:

- `0.00` when parcel has no lineage links
- null when lineage links exist but reference assessments are unavailable
- null when current assessment is missing

Quality flags:

- `missing_current_assessment_for_lineage`
- `missing_lineage_reference_values`

## Feature Quality Flags

`feature_quality_flags` is a JSON array of stable string codes.

Rules:

- deduplicated
- sorted lexicographically before persistence
- empty list allowed
- never null in memory; persist as a JSON array on disk, using `[]` when empty (never persist `null`)

`quality_warning_count` in run summary equals total flag count across inserted rows.

## `source_refs_json` Contract

Each feature row must include a deterministic trace payload.

Required top-level keys:

- `assessment`: `{ "parcel_id": <id>, "year": <year> }`
- `sales`: `{ "sales_transaction_id": <id|null>, "match_id": <id|null> }`
- `peer_group`: `{ "year": <year>, "municipality": <text|null>, "classification": <text|null>, "group_size": <int|null> }`
  - `municipality` and `classification` MUST be the canonicalized (casefolded) values used in the peer grouping key, not raw source strings.
  - `group_size` is the count of rows in the exact `(year, municipality, classification)` peer population with non-null `assessment_to_sale_ratio` (the same population used for percentile `N` and minimum-group-size checks); null means no peer group key was resolvable.
- `permits`: `{ "window_years": [year, year], "basis": "declared|estimated|none|unknown", "source_parcel_year_keys": [{"parcel_id": <id>, "year": <year>}...] }`
- `appeals`: `{ "window_years": [year-2, year], "source_parcel_year_keys": [{"parcel_id": <id>, "year": <year>}...] }`
- `lineage`: `{ "relationship_count": <int>, "related_parcel_ids": [<id>...], "reference_year": <year-1> }`
  - `relationship_count` is the count of unique `related_parcel_id` values after deduplication (not raw lineage-link row count).
  - `related_parcel_ids` MUST be deduplicated and sorted ascending before persistence.

Permit trace mapping:

- `window_years`: inclusive `[start_year, end_year]`; for v1 permit features this is always `[year, year]`
- `basis = declared`: direct declared valuation support for computed non-null permit feature value; `source_parcel_year_keys` are sorted, deduplicated supporting parcel-year keys.
- `basis = estimated`: inferred from permit-related inputs (not directly declared); `source_parcel_year_keys` are sorted, deduplicated materially contributing parcel-year keys.
- `basis = none`: no relevant permit activity for the window and deterministic zero-valued permit feature outcome; `source_parcel_year_keys = []`.
- `basis = unknown`: permit activity exists but no resolvable declared/estimated basis, or permit context is internally inconsistent (`unresolved_permit_basis` or `missing_permit_context`), so corresponding permit-based feature value is null; `source_parcel_year_keys` are sorted, deduplicated considered parcel-year keys (or `[]` if none resolvable).
- `source_parcel_year_keys` is never null in memory

Composite key arrays must be sorted deterministically by `parcel_id` ascending, then `year` ascending.

## Idempotency And Write Strategy

For selected scope and `feature_version`:

- delete existing `parcel_features` rows in-scope for same `feature_version`
- insert rebuilt deterministic rows

Rerun guarantees:

- same source data + same scope/config => same feature values, flags, and source refs
- out-of-scope rows are untouched during scoped rebuilds

## Run Summary Contract (`build-features`)

Minimum summary keys:

- `selected_parcels`
- `selected_parcel_years`
- `rows_deleted`
- `rows_inserted`
- `rows_skipped`
- `quality_warning_count`

`rows_skipped` reasons include:

- invalid year
- unresolved primary grain key
- hard validation failures that prevent deterministic feature materialization

## Implementation Notes For Day 7

- implement feature helpers as pure functions where possible
- avoid per-row DB lookups; preload maps keyed by `(parcel_id, year)`
- keep all ordering stable (`sorted(...)`) before hashes, writes, and JSON payload generation
- enforce quantization exactly to ORM numeric scales before insert

## Acceptance For This Spec

This spec is complete when Day 7 can implement `accessdane build-features` without redefining:

- feature formulas
- null/default behavior
- quality flag vocabulary
- run/version/hash semantics
- full vs scoped rebuild behavior
