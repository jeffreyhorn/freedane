# Sprint 7 Parser Drift Detection v1 Contract

Prepared on: 2026-03-17

## Purpose

Define the first stable contract for parser drift detection in Sprint 7 before implementing snapshot/diff commands.

This contract locks v1 behavior for:

- parser drift signal schema
- baseline snapshot format
- diff and severity thresholds
- alert routing payload
- validation fixtures for stable/noisy/breaking drift scenarios

## Scope

In scope for v1:

- one machine-readable baseline snapshot artifact
- one diff artifact comparing current parser profile against baseline
- deterministic severity classification (`ok|warn|error`)
- machine-readable alert payload for warning and error outcomes

Out of scope for v1:

- external paging integrations (Slack/email/PagerDuty)
- automatic parser rollback or hotfix deployment
- adaptive or ML-based threshold tuning
- cross-municipality drift normalization

## Design Goals

- Detect parser breakage early when AccessDane markup changes.
- Keep signal computation deterministic and auditable.
- Reduce false alarms from tiny sample sizes and expected noise.
- Preserve clear operator triage paths for warning and error outcomes.

## Baseline Assumptions

- Sprint 1 `profile-data` coverage payload shape is stable and available via `accessdane profile-data`.
- Sprint 6 operational workflow remains the baseline ingestion/analysis path.
- Drift diagnostics run over recent refresh outputs from Sprint 7 retention paths.

## Contract Terminology

- `snapshot`: one point-in-time parser profile artifact.
- `baseline`: approved reference snapshot used for drift comparison.
- `current`: snapshot computed for the run being evaluated.
- `signal`: one metric comparison with baseline, delta, and severity.
- `overall_severity`: max severity across all non-ignored signals.

## Parser Drift Signal Schema (v1)

Signal families required in v1:

1. `field_coverage_shift`
2. `selector_miss_shift`
3. `extraction_null_rate_shift`

Each emitted signal must include:

- `signal_id` (stable identifier)
- `family` (`field_coverage_shift|selector_miss_shift|extraction_null_rate_shift`)
- `metric_key` (stable metric key)
- `baseline_value` (nullable float)
- `current_value` (nullable float)
- `delta_absolute` (`current_value - baseline_value`, nullable)
- `delta_relative` (nullable float)
- `sample_size_baseline` (int)
- `sample_size_current` (int)
- `severity` (`ok|warn|error`)
- `reason_code` (stable reason code)
- `ignored` (bool)
- `ignore_reason` (nullable string)

`delta_relative` computation contract:

- Formula: `(current_value - baseline_value) / baseline_value`
- `delta_relative` must be `null` when:
  - `baseline_value` is `null`
  - `current_value` is `null`
  - `baseline_value == 0`
- When `delta_relative` is `null` because `baseline_value == 0`, signal must set:
  - `ignored = true`
  - `ignore_reason = zero_baseline`

## Required Metrics (v1)

### Field coverage shift metrics

Use these coverage metrics from profile-style outputs:

- `coverage.parsed_successful_fetch_rate`
- `coverage.parse_error_successful_fetch_rate`
- `coverage.parcel_summary_parcel_rate`
- `coverage.parcel_year_fact_parcel_rate`
- `coverage.parcel_year_fact_source_year_rate`
- `coverage.parcel_characteristic_parcel_rate`
- `coverage.parcel_lineage_parcel_rate`

### Selector miss shift metrics

Define selector-miss rates from missing section counts over successful fetches:

- `selector_miss.assessment_fetch_rate` = `missing_sections.assessment_fetches / counts.successful_fetches`
- `selector_miss.tax_fetch_rate` = `missing_sections.tax_fetches / counts.successful_fetches`
- `selector_miss.payment_fetch_rate` = `missing_sections.payment_fetches / counts.successful_fetches`

Also include detail tax selector coverage rates:

- `tax_detail_field_presence.tax_value_rows.rate`
- `tax_detail_field_presence.tax_rate_rows.rate`
- `tax_detail_field_presence.tax_jurisdiction_rows.rate`
- `tax_detail_field_presence.tax_amount_summary.rate`
- `tax_detail_field_presence.installment_rows.rate`

### Extraction null-rate shift metrics

v1 must support null-rate metrics keyed by canonical extraction fields:

- `extraction_null_rate.<field_key>`

Contract rules:

- field keys are implementation-defined in Day 6 but must be stable across runs
- rates are `null_count / eligible_record_count`
- each key must include denominator counts in snapshot and diff artifacts

## Baseline Snapshot Format (v1)

Snapshot top-level keys (canonical order):

1. `run`
2. `scope`
3. `metrics`
4. `diagnostics`
5. `error`

Key-order note:

- Canonical key order is for presentation/readability only; JSON consumers must validate by field presence rules, not object member order.

Presence rules:

- `run`, `scope`, `metrics`, `diagnostics`, and `error` are always present.
- On successful snapshot generation, `error` must be `null`.
- On failed snapshot generation, `error` must be an object with `code` and `message`.

`run` fields:

- `run_type` (`parser_drift_snapshot`)
- `version_tag` (`parser_drift_v1`)
- `snapshot_id`
- `generated_at` (UTC RFC3339 with trailing `Z`)

`scope` fields:

- `profile_name` (for example `daily_refresh`)
- `run_date`
- `feature_version`
- `ruleset_version`
- `artifact_root`
- `parcel_filter_count` (nullable int)

`scope.run_date` format rule:

- Must be `YYYYMMDD` for compatibility with refresh artifact paths and lookup keys.

`metrics` fields:

- `counts` (object of supporting denominators used by drift metrics)
- `field_coverage` (object keyed by required field coverage metric keys)
- `selector_miss` (object keyed by required selector miss metric keys)
- `extraction_null_rate` (object keyed by extraction field keys)

`diagnostics` fields:

- `warnings` (array of strings)
- `source_artifacts` (array of paths used to build snapshot)

## Diff Artifact Format (v1)

Diff top-level keys (canonical order):

1. `run`
2. `baseline`
3. `current`
4. `summary`
5. `signals`
6. `alerts`
7. `diagnostics`
8. `error`

Key-order note:

- Canonical key order is for presentation/readability only; JSON consumers must validate by field presence rules, not object member order.

Presence rules:

- `run`, `baseline`, `current`, `summary`, `signals`, `alerts`, `diagnostics`, and `error` are always present.
- `run.status` must be `succeeded|failed`.
- When `run.status = succeeded`, `error` must be `null`.
- When `run.status = failed`, `error` must be an object with `code` and `message`.

`summary` fields:

- `signal_count`
- `signal_ok_count`
- `signal_warn_count`
- `signal_error_count`
- `ignored_signal_count`
- `overall_severity` (`ok|warn|error`)

`baseline` and `current` must include:

- `snapshot_id`
- `generated_at`
- `artifact_path`

`error` must be `null` on successful diff generation.

## Severity Threshold Contract (v1)

Default threshold policy (absolute delta unless stated otherwise):

- Field coverage decreases:
  - `warn` when drop `>= 0.02`
  - `error` when drop `>= 0.05`
- Parse error rate increases:
  - metric key: `coverage.parse_error_successful_fetch_rate`
  - directionality: higher values are always worse
  - `warn` when increase `>= 0.01`
  - `error` when increase `>= 0.03`
- Selector miss rate increases:
  - `warn` when increase `>= 0.02`
  - `error` when increase `>= 0.05`
- Extraction null-rate increases:
  - `warn` when increase `>= 0.03`
  - `error` when increase `>= 0.08`

Sample-size guardrails:

- If `sample_size_current < 25`, severity is capped at `warn`.
- If either side has denominator `0`, signal must set:
  - `severity = ok`
  - `ignored = true`
  - `ignore_reason = insufficient_denominator`

Overall severity rules:

- `error` if any non-ignored signal is `error`
- else `warn` if any non-ignored signal is `warn`
- else `ok`

## Alert Routing Payload Contract (v1)

Emit alert payload only when `overall_severity != ok`.

Top-level keys (canonical order):

1. `run`
2. `alert`
3. `impacted_signals`
4. `operator_actions`

`alert` fields:

- `alert_type` (`parser_drift`)
- `severity` (`warn|error`)
- `reason_codes` (array of deduplicated signal reason codes)
- `routing_key`:
  - `warn`: `ops.parser_drift.warn`
  - `error`: `ops.parser_drift.error`
- `generated_at` (UTC RFC3339 with trailing `Z`)

`impacted_signals`:

- array of signal objects where `severity` equals alert severity

`operator_actions`:

- ordered array of actionable steps
- first action must reference source artifacts and baseline snapshot paths

## Validation Fixtures (Day 6+ Test Targets)

### Fixture A: Stable run (no drift)

- Current metrics effectively match baseline.
- Expected: all signals `ok`, `overall_severity = ok`, no alert payload.

### Fixture B: Noisy small sample

- One or more metrics exceed warn thresholds but denominators are below guardrail.
- Expected: capped severity behavior and explicit `insufficient_denominator` ignores where applicable.

### Fixture C: Selector break regression

- Significant rise in `selector_miss.*` rates.
- Expected: one or more `error` signals and `ops.parser_drift.error` routing key.

### Fixture D: Coverage degradation without hard failure

- Coverage rates drop into warn range only.
- Expected: `overall_severity = warn` and warn alert payload.

### Fixture E: Extraction null-rate regression

- One extraction null-rate metric crosses error threshold.
- Expected: `error` severity with field-specific reason code and operator action guidance.

## Non-Goals For Day 5 Contract

- Implementing snapshot computation logic.
- Implementing diff command wiring in CLI.
- Implementing load monitoring or annual refresh automation.
