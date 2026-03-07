# Sprint 5 Fraud Signal v1 Contract

Prepared on: 2026-03-07

## Purpose

Define the first stable contract for Sprint 5 fraud-signal analytics so Day 3 through Day 12 implementation can be deterministic.

This document defines v1 contracts for:

- `accessdane sales-ratio-study`
- `accessdane build-features`
- `accessdane score-fraud`
- persistence tables: `parcel_features`, `fraud_scores`, `fraud_flags`
- run/version metadata in `scoring_runs`

It also defines:

- reason-code conventions and explainability requirements
- scoped/full rerun semantics
- idempotency behavior by scope and version

## Scope

This is a v1 contract for transparent, rules-first scoring.

In scope:

- reproducible ratio-study metrics by year, class, and area
- parcel-level feature generation from sales + parcel-year context
- score and flag persistence with versioned rule metadata
- source-row traceability for every score explanation

Out of scope for v1:

- opaque/ML model scoring
- analyst case-review writeback workflows (`case_reviews`)
- dashboard implementation (CLI outputs only)
- mandatory geometry-derived features

## Design Goals

- Keep all score outputs explainable in plain language.
- Make reruns deterministic and auditable.
- Preserve links from score output back to source rows and thresholds.
- Support full-corpus and scoped reruns without destructive cross-scope side effects.
- Keep contracts strict enough for migration/test enforcement while allowing feature growth.

## Baseline Assumptions

- Sprint 4 schema is current at `20260307_0010`.
- `parcel_year_facts` includes permit/appeal context rollups.
- `sales_transactions`, `sales_parcel_matches`, and `sales_exclusions` provide sale truth and exclusion context.
- Spatial support remains optional and feature-flagged.

## Run Metadata Contract: `scoring_runs`

`scoring_runs` is the v1 run ledger for all Sprint 5 analytics commands (despite the table name).

### Grain

One row per command execution attempt.

### Required fields

- `id` (surrogate integer PK)
- `run_type` (string)
  - allowed values in v1:
    - `sales_ratio_study`
    - `build_features`
    - `score_fraud`
- `status` (string)
  - allowed values:
    - `running`
    - `succeeded`
    - `failed`
- `version_tag` (string)
  - ratio version, feature version, or ruleset version depending on `run_type`
- `scope_hash` (string, nullable)
  - deterministic hash of scope/config payload
- `scope_json` (JSON object)
- `config_json` (JSON object)
- `input_summary_json` (JSON object, nullable)
- `output_summary_json` (JSON object, nullable)
- `error_summary` (text, nullable)
- `parent_run_id` (nullable FK to `scoring_runs.id`)
- `started_at` (timestamp with timezone)
- `completed_at` (timestamp with timezone, nullable)
- `created_at` (timestamp with timezone)

### Notes

- `parent_run_id` links dependency chains (example: `score_fraud` run depends on a specific `build_features` run).
- `scope_json` must preserve exact operator scope input (`ids`, years, or study grouping filters).
- `scope_hash` enables cheap same-scope/idempotency checks.

## Command Contracts

## `accessdane sales-ratio-study`

### Purpose

Calculate sales-ratio metrics by year, class, and area for fairness diagnostics and downstream feature context.

### Inputs

- arms-length sales only by default (exclude rows with active exclusion rules)
- matched sale-to-parcel relationships
- parcel-year assessed values aligned to sale year
- optional scope controls:
  - `--id` / `--ids`
  - `--year` (repeatable)
  - `--municipality`
  - `--class`

### Output contract

Command emits JSON payload to stdout or `--out` path:

- `run`: run metadata (`run_id`, `run_type`, `version_tag`, `status`)
- `scope`: resolved scope inputs
- `summary`: row counts and coverage counters
- `groups`: array of grouped metrics with fields:
  - `year`
  - `municipality_name`
  - `valuation_classification`
  - `area_key`
  - `sale_count`
  - `median_ratio`
  - `cod`
  - `prd`
  - `outlier_low_count`
  - `outlier_high_count`
  - `excluded_count`

### Failure behavior

- if scope resolves to zero eligible sales, command succeeds with empty `groups` and explicit zero counters
- if required source tables are missing, command fails and records `status = failed` in `scoring_runs`

## `accessdane build-features`

### Purpose

Generate parcel-level feature vectors for scoring using sales, parcel-year context, permits, appeals, and lineage signals.

### Inputs

- `parcel_year_facts`
- sale context (`sales_transactions`, `sales_parcel_matches`, `sales_exclusions`)
- lineage context (`parcel_lineage_links`)
- optional scoped controls (`--id`, `--ids`, `--year`)
- `--feature-version` (required or defaulted)

### Output contract

Command emits JSON payload to stdout or `--out` path:

- `run`: run metadata (`run_id`, `run_type`, `version_tag`, `status`)
- `scope`: resolved scope
- `summary`:
  - `selected_parcels`
  - `selected_parcel_years`
  - `rows_deleted`
  - `rows_inserted`
  - `rows_skipped`
  - `quality_warning_count`

### Persistence target

- writes feature rows to `parcel_features`
- each row records `run_id`, `feature_version`, and source-trace payload

## `accessdane score-fraud`

### Purpose

Apply transparent rules to feature rows and persist explainable score outputs.

### Inputs

- `parcel_features` scoped by selected `feature_version`
- `--ruleset-version` (required or defaulted)
- optional explicit `--feature-run-id` to pin source feature batch
- optional scoped controls (`--id`, `--ids`, `--year`)

### Output contract

Command emits JSON payload to stdout or `--out` path:

- `run`: run metadata (`run_id`, `run_type`, `version_tag`, `status`, `parent_run_id`)
- `summary`:
  - `features_considered`
  - `scores_inserted`
  - `flags_inserted`
  - `high_risk_count`
  - `medium_risk_count`
  - `low_risk_count`
- `top_flags` (sample list for operator triage)

### Score output requirements

Each persisted score must include:

- normalized numeric score from `0` to `100`
- one or more reason codes when score is non-zero
- threshold and observed values for each reason
- source-link payloads sufficient to trace back to assessments/sales/permits/appeals/lineage

## Persistence Model

## `parcel_features`

### Grain

One row per `(parcel_id, year, feature_version)` in v1.

### Required columns

- `id` (PK)
- `run_id` (FK -> `scoring_runs.id`)
- `parcel_id` (FK -> `parcels.id`)
- `year` (integer)
- `feature_version` (string)
- `assessment_to_sale_ratio` (numeric, nullable)
- `peer_percentile` (numeric, nullable)
- `yoy_assessment_change_pct` (numeric, nullable)
- `permit_adjusted_expected_change` (numeric, nullable)
- `permit_adjusted_gap` (numeric, nullable)
- `appeal_value_delta_3y` (numeric, nullable)
- `appeal_success_rate_3y` (numeric, nullable)
- `lineage_value_reset_delta` (numeric, nullable)
- `feature_quality_flags` (JSON array, nullable)
- `source_refs_json` (JSON object)
- `built_at` (timestamp with timezone)
- `updated_at` (timestamp with timezone)

### Keys and indexes

- unique key: `(parcel_id, year, feature_version)`
- index: `(feature_version, year)`
- index: `(run_id)`

## `fraud_scores`

### Grain

One row per scored `(parcel_id, year, ruleset_version)`.

### Required columns

- `id` (PK)
- `run_id` (FK -> `scoring_runs.id`)
- `feature_run_id` (FK -> `scoring_runs.id`, nullable)
- `parcel_id` (FK -> `parcels.id`)
- `year` (integer)
- `ruleset_version` (string)
- `score_value` (numeric 0-100)
- `risk_band` (string; `high`, `medium`, `low`, `informational`)
- `requires_review` (boolean)
- `reason_code_count` (integer)
- `score_summary_json` (JSON object)
- `scored_at` (timestamp with timezone)
- `updated_at` (timestamp with timezone)

### Keys and indexes

- unique key: `(parcel_id, year, ruleset_version)`
- index: `(ruleset_version, score_value)`
- index: `(requires_review, risk_band)`
- index: `(run_id)`

## `fraud_flags`

### Grain

One row per reason code attached to one `fraud_scores` row.

### Required columns

- `id` (PK)
- `run_id` (FK -> `scoring_runs.id`)
- `score_id` (FK -> `fraud_scores.id`)
- `parcel_id` (FK -> `parcels.id`)
- `year` (integer)
- `ruleset_version` (string)
- `reason_code` (string)
- `reason_rank` (integer; 1 = strongest contributor)
- `severity_weight` (numeric, nullable)
- `metric_name` (string)
- `metric_value` (numeric/text serialized)
- `threshold_value` (numeric/text serialized)
- `comparison_operator` (string; e.g. `<`, `>`, `between`)
- `explanation` (text; plain-language sentence)
- `source_refs_json` (JSON object)
- `flagged_at` (timestamp with timezone)

### Keys and indexes

- unique key: `(score_id, reason_code)`
- index: `(reason_code, ruleset_version)`
- index: `(parcel_id, year)`

## Reason-Code Conventions

Reason codes must be stable, machine-readable, and human-auditable.

### Format

`<family>__<condition>`

Examples:

- `ratio__low_assessment_to_sale_ratio`
- `peer__persistent_underassessment_vs_peers`
- `improvement__post_improvement_stagnation`
- `classification__unusual_reclassification`
- `appeal__appeal_pattern_outlier`
- `lineage__parcel_lineage_value_drop`

### Requirements

- reason codes are versioned by `ruleset_version`, not renamed in-place
- each flag row must include exact threshold/metric evidence
- each flag row must include a plain-language `explanation` suitable for analyst-facing output

## Explainability Requirements

Every score must be reproducible from persisted data only.

Minimum explainability payload:

- scoring rule version (`ruleset_version`)
- contributing feature values from `parcel_features`
- triggered reason codes and thresholds
- references to source rows (sale IDs, permit event IDs, appeal event IDs, parcel-year row keys, lineage row IDs where used)

Plain-language requirement:

- score output must support one-sentence explanations per reason code without requiring SQL interpretation.

## Scoped And Full Rerun Semantics

## Full rerun

- command scope covers all eligible parcels/years
- existing rows for the same version are replaced in-table before insert
  - `parcel_features`: replace rows for `feature_version`
  - `fraud_scores`/`fraud_flags`: replace rows for `ruleset_version`
- run summary records delete/insert counts

## Scoped rerun

- scope defined by `--id`/`--ids` and optional `--year`
- only rows within resolved scope and same version are replaced
- out-of-scope rows for the same version are preserved

## Idempotency

- rerunning same command with same version and same resolved scope must produce identical persisted rows and equivalent summaries (except timestamps/run IDs)
- unique keys enforce no duplicate logical rows inside one version/scope

## Versioning Contract

- `feature_version` and `ruleset_version` are explicit operator-visible strings
- a version string change is required when rule thresholds or feature definitions change materially
- `scoring_runs.config_json` stores parameter details for reproducibility beyond version tag alone

## Data Quality And Safety Rules

- missing source data should yield null feature values and explicit feature-quality flags, not command failure
- scoring should skip or down-weight features with failed-quality flags as defined by the active ruleset
- commands must fail fast only for structural issues (missing tables, invalid CLI params, unsupported version values)

## Day 3 Implementation Notes

Day 3 implementation should enforce this contract with migration and tests:

1. Add new SQLAlchemy models and Alembic migration for `scoring_runs`, `parcel_features`, `fraud_scores`, and `fraud_flags`.
2. Add migration tests validating required columns, unique keys, and indexes.
3. Ensure `init-db` upgrades cleanly from current head to new head.
4. Keep table/column naming exactly aligned with this document unless a Day 3 blocker requires an explicit contract amendment.

## Acceptance Criteria For This Contract

- all three Sprint 5 analytics commands have explicit IO contracts
- persistence shape is defined with grain, required columns, and uniqueness rules
- reason-code and explanation requirements are explicit and testable
- full/scoped rerun behavior and idempotency expectations are unambiguous
