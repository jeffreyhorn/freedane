# Sprint 6 Parcel Dossier v1 Contract

Prepared on: 2026-03-10

## Purpose

Define the first stable contract for `accessdane parcel-dossier` before implementation.

This contract locks v1 behavior for:

- command inputs and scope rules
- output payload schema
- required evidence sections
- deterministic ordering rules
- null and empty-section semantics
- scoped validation fixtures for implementation tests

The goal is to prevent ad hoc output-shape decisions during Day 3 and Day 4 coding.

## Scope

In scope for v1:

- one-parcel dossier output from existing Sprint 1-5 tables
- full evidence chain sections for:
  - assessment history
  - matched sales
  - peer context
  - permit events
  - appeal events
  - score reason-code evidence
- deterministic section order and row ordering
- explicit behavior when source data is sparse

Out of scope for v1:

- writeback/case disposition edits (`case_reviews`, planned for Day 8+)
- interactive dashboard behavior
- jurisdiction-specific narrative templates
- non-deterministic ranking or sampling

## Design Goals

- Keep dossier output analyst-usable without direct SQL.
- Keep evidence traceable back to concrete source rows.
- Preserve deterministic output for the same inputs.
- Avoid silent section omissions; empty or unavailable sections must be explicit.
- Keep score framing as triage signal, not proof.

## Baseline Assumptions

- Sprint 5 baseline tables are present: `parcel_features`, `fraud_scores`, `fraud_flags`, `scoring_runs`.
- Core source tables are present: `assessments`, `sales_transactions`, `sales_parcel_matches`, `sales_exclusions`, `permit_events`, `appeal_events`, `parcel_year_facts`, `parcels`.
- v1 command is read-only; no new run table writes are required for dossier generation.

## Command Contract

## `accessdane parcel-dossier`

### CLI shape

```bash
accessdane parcel-dossier \
  (--id <parcel_id> | --parcel-id <parcel_id>) \
  [--year <year> ...] \
  [--feature-version <feature_version>] \
  [--ruleset-version <ruleset_version>] \
  [--out <path>]
```

### Required and optional arguments

- `--id` (required if `--parcel-id` is not provided; single value in v1)
  - target parcel ID to assemble dossier for
- `--parcel-id` (required if `--id` is not provided; alias for `--id`)
  - compatibility alias for docs that used `--parcel-id`
  - mutually exclusive with `--id`; exactly one of `--id` or `--parcel-id` must be provided
- `--year` (optional, repeatable)
  - filters all year-grained sections to selected years
  - normalized to sorted unique years
- `--feature-version` (optional; default `feature_v1`)
  - selects feature/peer context rows
- `--ruleset-version` (optional; default `scoring_rules_v1`)
  - selects score and reason-code context
- `--out` (optional)
  - writes JSON payload to file instead of stdout

### Scope resolution rules

- If `--year` is omitted: `request.years = []` and no year filter is applied.
- If `--year` is provided: include only those years in section queries and persist sorted unique years in `request.years`.
- `feature_version` filter applies to `peer_context` and `reason_code_evidence`.
- `ruleset_version` filter applies only to `reason_code_evidence`.
- Parcel ID matching is exact against `parcels.id`.

### Exit behavior

- Success (`exit 0`): payload emitted with all required top-level keys.
- Parcel not found (`exit 1`): payload includes `run.status = failed` and `error.code = parcel_not_found`.
- Invalid option values (`exit 2`): handled by Typer/Click option parsing (for example invalid year); usage/error text is emitted to stderr and no JSON payload is emitted.
- Partial source availability:
  - command still succeeds when one or more sections cannot be populated
  - affected sections must be marked `status = unavailable` with diagnostics

## Output Payload Contract

Top-level payload keys (canonical order when present):

1. `run`
2. `request`
3. `parcel`
4. `section_order`
5. `sections`
6. `timeline`
7. `diagnostics`
8. `error`

Presence requirements by run status:

- Success (`run.status = succeeded`):
  - required: `run`, `request`, `parcel`, `section_order`, `sections`, `timeline`, `diagnostics`, `error`
  - `error` must be `null`
- Hard precondition failure (`run.status = failed` with precondition code):
  - required: `run`, `request`, `error`
  - optional: `diagnostics`
  - must not be present: `parcel`, `section_order`, `sections`, `timeline`
- Late/operational failure (`run.status = failed` after some assembly work):
  - required: `run`, `request`, `error`
  - optional: `diagnostics`, `parcel`, `section_order`, `sections`, `timeline`
  - if optional dossier-shape keys are present, they must conform to the success-schema field contracts

### `run`

- `run_id` (`null` in v1)
- `run_persisted` (`false` in v1)
- `run_type` (`parcel_dossier`)
- `version_tag` (`parcel_dossier_v1`)
- `status` (`succeeded` or `failed`)

### `request`

- `parcel_id`
- `years` (array; empty array means "no year filter")
- `feature_version`
- `ruleset_version`

### `parcel`

- `parcel_id`
- `trs_code`
- `municipality_name`
- `current_owner_name`
- `current_primary_address`
- `current_parcel_description`

`parcel` fields are sourced from one `parcel_year_facts` row with null fallbacks:

- If `request.years` is empty: use the most recent available row across all years (highest year in DB).
- If `request.years` is non-empty: use the most recent available row within requested years (highest year in `request.years` that exists). If no row exists for requested years, all `parcel` fields are `null`.

`trs_code` is sourced directly from `parcels.trs_code` and does not depend on `parcel_year_facts` year selection.

### `section_order`

Required fixed order in v1:

1. `assessment_history`
2. `matched_sales`
3. `peer_context`
4. `permit_events`
5. `appeal_events`
6. `reason_code_evidence`

### `sections`

Each section object must include:

- `status`:
  - `populated`
  - `empty`
  - `unavailable`
- `summary` (section-specific counters)
- `rows` (array; empty when no rows)
- `message` (nullable string; required when status is `empty` or `unavailable`)

### `timeline`

Normalized evidence timeline assembled from section rows:

- `event_count`
- `rows` with normalized event objects:
  - `event_date` (ISO date string or `null`)
  - `event_type` (enum-like string)
  - `year` (nullable int)
  - `title`
  - `details` (object)
  - `source`:
    - `table`
    - `row_id`
    - `source_refs` (object; optional)

### `diagnostics`

- `warnings` (array of warning codes)
- `unavailable_sections` (array of section keys)
- `query_filters` (normalized filter summary)

### `error`

- `null` when `run.status = succeeded`
- object when `run.status = failed`:
  - `code`
  - `message`

## Section Contracts

All decimal values must be serialized as strings, not JSON floats.

All sections below must follow the shared `status/summary/rows/message` shape.

## 1) `assessment_history`

Purpose:

- show multi-year assessed value context for the parcel

Primary source:

- `assessments` (preferred), with optional fallback fields from `parcel_year_facts`

Row fields (logical output fields with source mappings):

- `assessment_id` (from `assessments.id`; `null` when row is sourced only from `parcel_year_facts`)
- `year` (from `assessments.year`; fallback: `parcel_year_facts.year`)
- `valuation_classification` (from `assessments.valuation_classification`; fallback: `parcel_year_facts.assessment_valuation_classification`)
- `total_value` (from `assessments.total_value`; fallback: `parcel_year_facts.assessment_total_value`)
- `land_value` (from `assessments.land_value`; fallback: `parcel_year_facts.assessment_land_value`)
- `improved_value` (from `assessments.improved_value`; fallback: `parcel_year_facts.assessment_improved_value`)
- `estimated_fair_market_value` (from `assessments.estimated_fair_market_value`; fallback: `parcel_year_facts.assessment_estimated_fair_market_value`)
- `average_assessment_ratio` (from `assessments.average_assessment_ratio`; fallback: `parcel_year_facts.assessment_average_assessment_ratio`)
- `valuation_date` (from `assessments.valuation_date`; fallback: `parcel_year_facts.assessment_valuation_date`)
- `source_fetch_id` (from `assessments.fetch_id`; fallback: `parcel_year_facts.assessment_fetch_id`)

Deterministic row order:

- `year DESC`, then `assessment_id DESC`

Summary fields:

- `row_count`
- `year_min`
- `year_max`

## 2) `matched_sales`

Purpose:

- show sale evidence linked to the parcel and basic match/exclusion context

Primary sources:

- `sales_parcel_matches` (`is_primary = true` preferred in v1)
- `sales_transactions`
- `sales_exclusions` (active exclusions aggregated by transaction)

Row fields:

- `sales_transaction_id`
- `transfer_date`
- `recording_date`
- `consideration_amount`
- `arms_length_indicator` (from `sales_transactions.arms_length_indicator_norm`, normalized boolean indicator)
- `usable_sale_indicator` (from `sales_transactions.usable_sale_indicator_norm`, normalized boolean indicator)
- `document_number`
- `match_method`
- `match_review_status`
- `match_confidence_score` (from `sales_parcel_matches.confidence_score`)
- `is_primary_match` (from `sales_parcel_matches.is_primary`)
- `active_exclusion_codes` (array)

Deterministic row order:

- `transfer_date DESC NULLS LAST`, then `sales_transaction_id DESC`

Summary fields:

- `row_count`
- `arms_length_true_count`
- `usable_sale_true_count`
- `excluded_sale_count`

## 3) `peer_context`

Purpose:

- expose peer-relative context used by Sprint 5 scoring features

Primary sources:

- `parcel_features` filtered by `feature_version`
- `parcel_year_facts` for municipality/class labels

Row fields:

- `run_id` (feature run id, from `parcel_features.run_id`)
- `year`
- `feature_version`
- `municipality_name`
- `valuation_classification` (from `parcel_year_facts.assessment_valuation_classification`)
- `assessment_to_sale_ratio`
- `peer_percentile`
- `yoy_assessment_change_pct`
- `permit_adjusted_gap`
- `appeal_value_delta_3y`
- `lineage_value_reset_delta`
- `feature_quality_flags` (array)

Deterministic row order:

- `year DESC`, then `run_id DESC NULLS LAST`

Summary fields:

- `row_count`
- `missing_ratio_count`
- `missing_peer_percentile_count`
- `quality_flagged_row_count`

## 4) `permit_events`

Purpose:

- show permit activity context related to assessed value changes

Primary source:

- `permit_events` filtered to `import_status = loaded`

Row fields:

- `permit_event_id`
- `permit_year`
- `issued_date`
- `applied_date`
- `finaled_date`
- `status_date`
- `permit_number`
- `permit_type`
- `permit_subtype`
- `work_class`
- `permit_status` (from `permit_events.permit_status_norm`; normalized canonical status)
- `declared_valuation`
- `estimated_cost`
- `description`
- `parcel_link_method`
- `parcel_link_confidence`

Deterministic row order:

- `coalesce(issued_date, applied_date, status_date) DESC NULLS LAST`, then `permit_event_id DESC`

Summary fields:

- `row_count`
- `open_status_count`
- `closed_status_count`
- `declared_valuation_known_count`

## 5) `appeal_events`

Purpose:

- show appeals context and value-change outcomes

Primary source:

- `appeal_events` filtered to `import_status = loaded`

Row fields:

- `appeal_event_id`
- `tax_year`
- `filing_date`
- `hearing_date`
- `decision_date`
- `appeal_number`
- `docket_number`
- `appeal_level` (from `appeal_events.appeal_level_norm`; normalized canonical level)
- `outcome` (from `appeal_events.outcome_norm`; normalized canonical outcome)
- `assessed_value_before`
- `requested_assessed_value`
- `decided_assessed_value`
- `value_change_amount`
- `representative_name`
- `parcel_link_method`
- `parcel_link_confidence`

Deterministic row order:

- `coalesce(decision_date, hearing_date, filing_date) DESC NULLS LAST`, then `appeal_event_id DESC`

Summary fields:

- `row_count`
- `successful_reduction_count`
- `denied_or_no_change_count`
- `unknown_outcome_count`

## 6) `reason_code_evidence`

Purpose:

- show score/risk evidence and underlying triggered reason codes

Primary sources:

- `fraud_scores` filtered by `ruleset_version` and `feature_version`
- `fraud_flags`

Row fields:

- `score_id`
- `run_id` (scoring run id, from `fraud_scores.run_id`)
- `feature_run_id` (from `fraud_scores.feature_run_id`)
- `year`
- `score_value`
- `risk_band`
- `requires_review`
- `reason_code_count`
- `reason_codes` (ordered array of objects):
  - `reason_code`
  - `reason_rank`
  - `severity_weight`
  - `metric_name`
  - `metric_value`
  - `threshold_value`
  - `comparison_operator`
  - `explanation`
  - `source_refs` (object)

Deterministic row order:

- `year DESC`, then `score_value DESC`, then `score_id DESC`
- nested `reason_codes`: `reason_rank ASC`, then `reason_code ASC`

Summary fields:

- `row_count`
- `high_risk_count`
- `medium_risk_count`
- `review_required_count`

## Timeline Assembly Contract

Timeline row sources:

- assessments -> `event_type = assessment`
- matched sales -> `event_type = sale`
- permit events -> `event_type = permit`
- appeal events -> `event_type = appeal`
- reason code evidence -> `event_type = score`

Event-date derivation:

- assessment: `valuation_date`, else `YYYY-01-01` from `year`
- sale: `transfer_date`, else `recording_date`
- permit: `issued_date`, else `applied_date`, else `status_date`, else `YYYY-01-01` from `permit_year`
- appeal: `decision_date`, else `hearing_date`, else `filing_date`, else `YYYY-01-01` from `tax_year`
- score: `YYYY-12-31` from score `year` when no explicit scored date is included

Deterministic timeline order:

- `event_date DESC NULLS LAST`
- tie-break by event-type precedence:
  - `score`, `appeal`, `permit`, `sale`, `assessment`
- final tie-break: stable source key (`table ASC`, then `row_id ASC`)

## Null And Empty Semantics

### Null semantics

- Unknown scalar field values must be serialized as `null`.
- Numeric zeros must remain explicit string zeros (for example `"0.00"`), not `null`.
- Arrays are never `null`; use empty arrays when no values exist.

### Empty section behavior

When a section has no qualifying rows:

- `status = empty`
- `rows = []`
- `summary.row_count = 0`
- all non-`row_count` summary fields must still be present for that section's schema:
  - min/max-style fields (for example `year_min`, `year_max`) must be `null`
  - count-style fields (for example `*_count`) must be `0`
- `message` explains why no rows were present (for example `no_sales_for_scope`)

### Unavailable section behavior

When a section cannot be built because of missing source artifacts:

- `status = unavailable`
- `rows = []`
- `summary.row_count = 0`
- all non-`row_count` summary fields must still be present for that section's schema:
  - min/max-style fields must be `null`
  - count-style fields must be `0`
- `message` provides actionable diagnostic code (for example `missing_fraud_scores_for_requested_versions`)
- section key must also appear in `diagnostics.unavailable_sections`

## Failure Contract

When command fails, payload requirements are:

- always required:
  - `run` with `run.status = failed`
  - `request` echoing resolved request shape
  - `error` with `error.code` and `error.message`
- optional: `diagnostics`

Failure classes in v1:

- hard precondition failures:
  - `parcel_not_found`
  - `invalid_scope`
  - `unsupported_version_selector`
- late/operational failures:
  - `source_query_error`

Hard precondition failure payload shape:

- required keys: `run`, `request`, `error`
- optional key: `diagnostics`
- must not include: `parcel`, `section_order`, `sections`, `timeline`

Late/operational failure payload shape:

- required keys: `run`, `request`, `error`
- optional: `diagnostics`, `parcel`, `section_order`, `sections`, `timeline`
- if `sections`/`timeline` are present, they must follow the success-schema contracts

## Scoped Validation Fixtures (Day 4 Test Targets)

These fixtures define the minimum scenario matrix for dossier command tests.

## Fixture A: Full evidence chain, single parcel

- Input: one parcel with assessment, sales, permit, appeal, and score rows.
- Assertions:
  - all six required sections are `status = populated`
  - section order matches `section_order`
  - timeline includes all five event types

## Fixture B: Sparse parcel with assessment-only history

- Input: one parcel with assessments only.
- Assertions:
  - `assessment_history.status = populated`
  - all other evidence sections `status = empty`
  - command succeeds (`run.status = succeeded`)

## Fixture C: Version-filter mismatch

- Input: parcel has feature rows but no score rows for requested ruleset/version pair.
- Assertions:
  - `reason_code_evidence.status = unavailable`
  - diagnostics include `missing_fraud_scores_for_requested_versions`
  - non-score sections still populate normally

## Fixture D: Multi-year parcel with explicit year filter

- Input: parcel with 5+ years of data, run with `--year` subset.
- Assertions:
  - all year-grained sections include only requested years
  - year arrays are deduplicated/sorted in `request.years`
  - timeline contains no out-of-scope years

## Fixture E: Deterministic output replay

- Input: run same command twice against unchanged DB.
- Assertions:
  - byte-for-byte identical JSON payload after stable key ordering
  - row orders unchanged across all sections

## Non-Goals For Day 2

- Implementing `accessdane parcel-dossier` code path.
- Adding new schema/migrations (`case_reviews` is Day 8+).
- Defining UI behavior beyond JSON contract needs.

Day 3 and Day 4 implementation must conform to this contract unless an explicit contract revision is committed first.
