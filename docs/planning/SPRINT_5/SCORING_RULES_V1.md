# Sprint 5 Scoring Rules v1

Prepared on: 2026-03-09

## Goal

Define an implementation-ready, transparent v1 fraud scoring contract for `accessdane score-fraud` using persisted `parcel_features`.

This document finalizes:

- rule thresholds and severity weighting
- deterministic reason-code taxonomy
- `fraud_scores` and `fraud_flags` generation semantics
- score versioning and rerun comparability rules

## Scope

In scope for v1:

- deterministic, rules-based scoring only
- explainable reason codes with one plain-language explanation per flag
- full and scoped reruns with stable replacement behavior

Out of scope for v1:

- ML-driven/learned models
- adaptive thresholds learned at runtime
- case-management workflow semantics (Sprint 6)

## Inputs And Selection

`score-fraud` consumes `parcel_features` rows filtered by:

- `feature_version` (required)
- optional explicit `feature_run_id`
- optional `--id` / `--ids` scope
- optional `--year` scope

Candidate feature rows are the deterministic scoring grain:

- one score candidate per `(parcel_id, year, feature_version)`

If scope resolves to zero feature rows, command succeeds with zero counters.

## Normalization And Precision

- All rule and weight arithmetic is performed using `Decimal` with 4-decimal working precision.
- Each per-flag `severity_weight` is quantized to 4 decimal places and stored as `Numeric(8,4)` on `fraud_flags`.
- `raw_score` is computed as the unbounded sum of those 4-decimal `severity_weight` contributions; no per-flag or intermediate rounding to 2 decimal places is applied.
- `raw_score` is persisted in `score_summary_json` at 4 decimal places.
- `score_value` is derived solely from `raw_score`, clamped to `[0.00, 100.00]`, then quantized to `Numeric(6,2)` immediately before persistence.
- Rounding mode for quantization to both 4 decimal places and 2 decimal places is `ROUND_HALF_UP`.

## Ruleset Identifier

Initial ruleset identifier:

- `ruleset_version = scoring_rules_v1`

Any material change to thresholds, severity weights, triggering predicates, reason-code names, or risk-band thresholds requires a new `ruleset_version`.

## Rule Evaluation Model

For each candidate feature row, evaluate the rule catalog below.

General evaluation rules:

- A rule can contribute at most one reason code row per score row.
- Rules with missing required inputs are skipped (not failed).
- Rules requiring multiple predicates may use one primary metric for `metric_name`/`metric_value`/`threshold_value`; secondary predicates must be encoded in `source_refs_json` and explanation text.
- If a rule defines multiple tiers over the same metric, tiers must be evaluated in descending `severity_weight` order and only the first satisfied tier may fire.
- Rules are evaluated independently, then summed.

Score formula:

- `raw_score = sum(triggered_rule_weight)`
- `score_value = max(0.00, min(100.00, raw_score))`

## Rule Catalog (`scoring_rules_v1`)

### R1: Assessment-To-Sale Ratio Floor

- Reason code: `ratio__assessment_to_sale_below_floor`
- Primary metric: `assessment_to_sale_ratio`
- Comparator: `<`
- Trigger tiers:
  - `ratio < 0.55` => `severity_weight = 35.0000`, `threshold_value = "0.55"`
  - `0.55 <= ratio < 0.70` => `severity_weight = 20.0000`, `threshold_value = "0.70"`
- Required input(s): `assessment_to_sale_ratio`
- Skip when required input is null.

### R2: Peer Percentile Bottom Tail

- Reason code: `peer__assessment_ratio_bottom_peer_percentile`
- Primary metric: `peer_percentile`
- Comparator: `<=`
- Trigger tiers:
  - `peer_percentile <= 0.05` => `severity_weight = 20.0000`, `threshold_value = "0.05"`
  - `0.05 < peer_percentile <= 0.10` => `severity_weight = 12.0000`, `threshold_value = "0.10"`
- Required input(s): `peer_percentile`
- Skip when required input is null.

### R3: Permit-Adjusted Gap (Unexplained Increase)

- Reason code: `permit_gap__assessment_increase_unexplained_by_permits`
- Primary metric: `permit_adjusted_gap`
- Comparator: `>=`
- Trigger tiers:
  - `permit_adjusted_gap >= 50000` => `severity_weight = 20.0000`, `threshold_value = "50000"`
  - `20000 <= permit_adjusted_gap < 50000` => `severity_weight = 12.0000`, `threshold_value = "20000"`
- Required input(s): `permit_adjusted_gap`
- Skip when required input is null.

### R4: Year-Over-Year Spike Without Strong Permit Support

- Reason code: `yoy__assessment_spike_without_support`
- Primary metric: `yoy_assessment_change_pct`
- Comparator: `>=`
- Trigger predicate:
  - `yoy_assessment_change_pct >= 0.20`
  - and `permit_adjusted_expected_change` is null or `permit_adjusted_expected_change <= 10000`
- Trigger tiers:
  - `yoy_assessment_change_pct >= 0.35` => `severity_weight = 16.0000`, `threshold_value = "0.35"`
  - `0.20 <= yoy_assessment_change_pct < 0.35` => `severity_weight = 10.0000`, `threshold_value = "0.20"`
- Required input(s): `yoy_assessment_change_pct`
- Secondary evidence fields:
  - `permit_adjusted_expected_change` (numeric; may be null)
  - `permit_adjusted_expected_change_condition` (string enum: `"is_null"` when predicate passes due to null value, `"<=_cutoff"` when predicate passes due to numeric cutoff)
  - `permit_adjusted_expected_change_cutoff` (string, fixed value `"10000"`)
  - permit basis from `source_refs_json.feature_sources.permits.basis`
- Skip when required input is null.

### R5: Appeal Pattern Suggesting Persistent Downward Pressure

- Reason code: `appeal__recurring_successful_reductions`
- Primary metric: `appeal_success_rate_3y`
- Comparator: `>=`
- Trigger predicate:
  - `appeal_success_rate_3y >= 0.60`
  - and `appeal_value_delta_3y <= -5000`
- Trigger tiers:
  - `appeal_success_rate_3y >= 0.75` and `appeal_value_delta_3y <= -15000`
    => `severity_weight = 12.0000`, `threshold_value = "0.75"`
  - otherwise when predicate passes
    => `severity_weight = 8.0000`, `threshold_value = "0.60"`
- Required input(s): `appeal_success_rate_3y`, `appeal_value_delta_3y`
- Secondary evidence fields: `appeal_value_delta_3y`, `appeal_value_delta_threshold` (string: `"-15000"` for high tier, `"-5000"` otherwise)
- Skip when either required input is null.

### R6: Lineage-Linked Value Reset Drop

- Reason code: `lineage__post_lineage_value_drop`
- Primary metric: `lineage_value_reset_delta`
- Comparator: `<=`
- Trigger tiers:
  - `lineage_value_reset_delta <= -100000` => `severity_weight = 18.0000`, `threshold_value = "-100000"`
  - `-100000 < lineage_value_reset_delta <= -50000` => `severity_weight = 10.0000`, `threshold_value = "-50000"`
- Required input(s): `lineage_value_reset_delta`
- Skip when required input is null.

## Quality Flag Interaction

`feature_quality_flags` are non-fatal for v1 scoring.

Behavior:

- score evaluation proceeds if required inputs are present
- null required inputs skip the corresponding rule
- `score_summary_json` must record:
  - all source `feature_quality_flags`, serialized into `quality_flags` (that is, `score_summary_json.quality_flags` mirrors serialized `parcel_features.feature_quality_flags`)
  - skipped rule list with skip reasons (`missing_required_input`)

No additional v1 down-weighting is applied solely due to quality flags. Any change to this rule requires a new `ruleset_version`.

## Risk Bands And Review Routing

Risk-band thresholds (`score_value` after cap):

- `high`: `score_value >= 70.00`
- `medium`: `40.00 <= score_value < 70.00`
- `low`: `score_value < 40.00`

`requires_review` semantics:

- `true` for `high` and `medium`
- `false` for `low`

## Reason-Code Taxonomy

Format:

- `<family>__<condition>`

v1 families:

- `ratio`
- `peer`
- `permit_gap`
- `yoy`
- `appeal`
- `lineage`

Stability requirements:

- reason-code strings are immutable within a `ruleset_version`
- renames/splits/merges require a new `ruleset_version`
- reason rank ordering is deterministic: `severity_weight DESC`, then `reason_code ASC`

## Plain-Language Explanation Templates

Each triggered reason row must include a deterministic one-sentence explanation.

Template patterns:

- `ratio__assessment_to_sale_below_floor`:
  - `Assessment-to-sale ratio {metric_value} is below threshold {threshold_value}.`
- `peer__assessment_ratio_bottom_peer_percentile`:
  - `Peer percentile {metric_value} is at or below threshold {threshold_value} for the parcel peer group.`
- `permit_gap__assessment_increase_unexplained_by_permits`:
  - `Permit-adjusted gap {metric_value} exceeds threshold {threshold_value}, indicating unexplained assessed-value increase.`
- `yoy__assessment_spike_without_support`:
  - `Year-over-year assessment change {metric_value} exceeds threshold {threshold_value} without strong permit support ({permit_adjusted_expected_change_condition} vs cutoff {permit_adjusted_expected_change_cutoff}).`
- `appeal__recurring_successful_reductions`:
  - `Appeal success rate {metric_value} meets threshold {threshold_value} with net negative appeal value change over 3 years.`
- `lineage__post_lineage_value_drop`:
  - `Lineage reset delta {metric_value} is at or below threshold {threshold_value}, indicating a post-lineage value drop.`

`metric_value` and `threshold_value` must be serialized as decimal strings in `fraud_flags`.

## `fraud_scores` Generation Semantics

For each scored row, persist:

- `run_id`: current scoring run id
- `feature_run_id`: resolved source feature run id (required/non-null for normal `score-fraud` runs; nullable only for explicit backfill/repair runs)
- `parcel_id`, `year`, `ruleset_version`, `feature_version`
- `score_value`, `risk_band`, `requires_review`
- `reason_code_count`: count of triggered reasons persisted in `fraud_flags`
- `score_summary_json` object with minimum keys:
  - `ruleset_version`
  - `feature_version`
  - `raw_score`
  - `score_value`
  - `risk_band`
  - `quality_flags`
  - `triggered_reason_codes`
  - `skipped_rules`

`triggered_reason_codes` and `quality_flags` must be deduplicated and sorted.

## `fraud_flags` Generation Semantics

One `fraud_flags` row per triggered reason code.

Required mapping:

- `reason_code`: triggered reason code
- `reason_rank`: 1-based rank after deterministic ordering (`1 = strongest contributor`; larger values are weaker contributors)
- `severity_weight`: rule tier weight
- `metric_name`: primary metric column name
- `metric_value`: observed value as string
- `threshold_value`: threshold value as string
- `comparison_operator`: operator string. For v1 `scoring_rules_v1`, rule evaluations must only emit `<`, `<=`, or `>=`. The broader v1 `fraud_flags` contract may support additional operators; see `FRAUD_SIGNAL_V1.md`.
- `explanation`: template-filled plain-language sentence
- `source_refs_json`: source evidence object, minimum keys:
  - `feature_row`: `{ "parcel_id": <id>, "year": <year>, "feature_version": <version> }`
  - `feature_run_id`: `<id|null>`
  - `feature_sources`: copied or filtered from `parcel_features.source_refs_json`

Uniqueness and consistency must comply with the existing schema constraints.

## Run Summary Contract (`score-fraud`)

Minimum command summary keys:

- `features_considered`
- `scores_inserted`
- `flags_inserted`
- `high_risk_count`
- `medium_risk_count`
- `low_risk_count`

`top_flags` output should be deterministic and ordered by:

1. score value descending
2. reason rank ascending
3. parcel id ascending
4. year ascending

## Rerun And Replacement Semantics

For `(ruleset_version, feature_version)` and a resolved scope defined as:

- parcel/year scope filters
- plus supplied `feature_run_id` / `--feature-run-id` filter when provided

Apply replacement as follows:

- delete existing in-scope `fraud_scores` for the resolved scope (including `feature_run_id` filter when supplied)
- delete corresponding `fraud_flags` via parent-score replacement semantics within the same resolved scope
- insert deterministic rebuilt rows for exactly the resolved scope

Scoped reruns must preserve out-of-scope rows, including rows for other `feature_run_id` values or parcels outside the resolved scope.

## Comparability Contract

Comparable scoring runs require:

- same `ruleset_version`
- same `feature_version`
- same resolved scope
- same source feature values

For comparison workflows:

- use `feature_version` deltas to compare feature logic changes
- use `ruleset_version` deltas to compare rule/threshold changes
- do not compare absolute score shifts across different versions without version labels

## Implementation Notes For Day 11

- keep rule evaluation pure and deterministic per row
- centralize threshold constants in one module-level structure
- avoid runtime randomness or timestamp-derived behavior in scoring logic
- serialize all numeric evidence values with explicit string formatting

## Acceptance Criteria For This Spec

This spec is complete when Day 11 can implement `accessdane score-fraud` without redefining:

- rule thresholds and severity weighting
- risk-band mapping
- reason-code vocabulary and explanations
- score/flag persistence semantics
- versioning and rerun comparability behavior
