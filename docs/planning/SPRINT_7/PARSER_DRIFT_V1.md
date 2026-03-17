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
- `ignore_reason` (nullable string code)

`reason_code` contract:

- `reason_code` format must be
  `<family>.<reason_token>.<metric_key>`, where `<metric_key>` is the exact
  signal `metric_key` value.
- Allowed `reason_token` values in v1:
  - `no_drift` (non-ignored signals with `severity = ok`)
  - `warn_threshold` (non-ignored signals with `severity = warn`)
  - `error_threshold` (non-ignored signals with `severity = error`)
  - `insufficient_denominator` (ignored signals due to zero denominator)
  - `missing_metric_value` (ignored signals due to null metric values)

`sample_size_*` semantics:

- `sample_size_baseline` and `sample_size_current` are the per-metric denominators
  used to compute `baseline_value` and `current_value` for that signal.
- For `coverage.parsed_successful_fetch_rate` and
  `coverage.parse_error_successful_fetch_rate`, sample size must map to
  `counts.successful_fetches`.
- For `coverage.parcel_summary_parcel_rate`,
  `coverage.parcel_year_fact_parcel_rate`,
  `coverage.parcel_characteristic_parcel_rate`, and
  `coverage.parcel_lineage_parcel_rate`, sample size must map to
  `counts.parcels`.
- For `coverage.parcel_year_fact_source_year_rate`, sample size must map to
  `counts.source_parcel_years`.
- For `selector_miss.*` metrics, sample size must map to
  `counts.successful_fetches`.
- For `tax_detail_field_presence.*` metrics, sample size must map to
  `counts.detail_tax_records`.
- For extraction null-rate signals, sample size must map to the corresponding
  field-level `eligible_record_count`.

`ignore_reason` allowed values in v1:

- `"insufficient_denominator"`
- `"missing_metric_value"`

Null-value severity/ignore contract:

- If either `baseline_value` or `current_value` is `null`, the signal must be
  emitted with:
  - `severity = ok`
  - `ignored = true`
  - `ignore_reason = "missing_metric_value"`
- Threshold comparisons must not be applied when either value is `null`.
- If the zero-denominator guardrail also applies (`sample_size_baseline == 0`
  or `sample_size_current == 0`), `ignore_reason` must be
  `"insufficient_denominator"`.

`delta_absolute` computation contract:

- Formula: `current_value - baseline_value`
- `delta_absolute` must be `null` when:
  - `baseline_value` is `null`
  - `current_value` is `null`

`delta_relative` computation contract:

- Formula: `(current_value - baseline_value) / baseline_value`
- `delta_relative` must be `null` when:
  - `baseline_value` is `null`
  - `current_value` is `null`
  - `baseline_value == 0`
- When `delta_relative` is `null` because `baseline_value == 0`, signal must remain
  eligible for severity evaluation using absolute-delta policy unless another
  ignore rule applies.

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

Signal-family assignment rule:

- All `tax_detail_field_presence.*.rate` signals must use
  `family = selector_miss_shift` in v1.

### Extraction null-rate shift metrics

v1 must support null-rate metrics keyed by canonical extraction fields:

- `extraction_null_rate.<field_key>`

Contract rules:

- field keys are implementation-defined in Day 6 but must be stable across runs
- rates are `null_count / eligible_record_count`
- each key must include denominator counts in snapshot and diff artifacts

Metric-key lookup mapping rules:

- `metric_key` values are path-style identifiers, not literal keys stored with
  dots in JSON objects.
- `coverage.<name>` maps to `metrics.field_coverage.<name>`.
- `selector_miss.<name>` maps to `metrics.selector_miss.<name>`.
- `tax_detail_field_presence.<name>.rate` maps to
  `metrics.tax_detail_field_presence.<name>.rate`.
- `extraction_null_rate.<field_key>` maps to
  `metrics.extraction_null_rate.<field_key>.rate`.
- JSON object members inside `metrics.field_coverage`, `metrics.selector_miss`,
  and `metrics.tax_detail_field_presence` use only `<name>` leaf keys (no
  `coverage.` / `selector_miss.` / `tax_detail_field_presence.` prefixes in the
  stored key names).

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
- `run_id` (must equal `snapshot_id` for v1 compatibility with common run payload validators)
- `snapshot_id`
- `status` (`succeeded|failed`)
- `run_persisted` (boolean)
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
- `field_coverage` (object keyed by `<name>` leaf keys that correspond to
  `coverage.<name>` metric keys)
- `selector_miss` (object keyed by `<name>` leaf keys that correspond to
  `selector_miss.<name>` metric keys)
- `tax_detail_field_presence` (object keyed by `<name>` leaf keys that
  correspond to `tax_detail_field_presence.<name>.rate` metric keys)
- `extraction_null_rate` (object keyed by extraction field keys)

Concrete key examples:

- `metrics.field_coverage.parsed_successful_fetch_rate`
- `metrics.selector_miss.assessment_fetch_rate`
- `metrics.tax_detail_field_presence.tax_value_rows.rate`

`metrics.extraction_null_rate` value shape:

- Each `metrics.extraction_null_rate.<field_key>` value must be an object with:
  - `rate` (nullable float)
  - `null_count` (int)
  - `eligible_record_count` (int)

`metrics.tax_detail_field_presence` value shape:

- Each `metrics.tax_detail_field_presence.<field_key>` value must be an object
  with:
  - `rate` (nullable float)
  - `count` (int)

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

`run` fields:

- `run_type` (`parser_drift_diff`)
- `version_tag` (`parser_drift_v1`)
- `run_id` (must equal `diff_id` for v1 compatibility with common run payload validators)
- `diff_id` (stable diff identifier)
- `baseline_snapshot_id`
- `current_snapshot_id`
- `run_persisted` (boolean)
- `generated_at` (UTC RFC3339 with trailing `Z`)
- `status` (`succeeded|failed`)

`summary` fields:

- `signal_count`
- `signal_ok_count`
- `signal_warn_count`
- `signal_error_count`
- `ignored_signal_count`
- `overall_severity` (`ok|warn|error`)

`summary` count semantics:

- `signal_count` is the total emitted signal count (including ignored signals).
- `ignored_signal_count` counts signals where `ignored = true`.
- `signal_ok_count`, `signal_warn_count`, and `signal_error_count` count only
  non-ignored signals by severity.
- Required invariant:
  `signal_count = ignored_signal_count + signal_ok_count + signal_warn_count + signal_error_count`.

`baseline` and `current` must include:

- `snapshot_id`
- `generated_at`
- `artifact_path`

`signals` field contract:

- `signals` is always an array of signal objects conforming to the Parser Drift
  Signal Schema defined in this contract.
- `signals` must include all evaluated metrics for the run, including ignored
  signals.
- Ordering must be deterministic: sort by `metric_key` ascending, then
  `family` ascending, then `signal_id` ascending.
- For `family = extraction_null_rate_shift` signals, producers must derive
  `baseline_value`, `current_value`, and `sample_size_*` from the snapshot
  `metrics.extraction_null_rate.<field_key>` objects (`rate` and
  `eligible_record_count`).

`alerts` field contract:

- `alerts` is always an array.
- When `summary.overall_severity = ok`, `alerts` must be `[]`.
- When `summary.overall_severity = warn|error`, `alerts` must contain exactly one
  embedded alert summary object with:
  - `alert_id` (stable join key for this diff-level alert summary)
  - `alert_type`
  - `severity`
  - `routing_key`
  - `reason_codes`
- Any standalone alert payload emitted under the Alert Routing Payload contract must
  correspond 1:1 to an entry in `diff.alerts` by matching `alert_id`.

`error` must be `null` on successful diff generation.

## Severity Threshold Contract (v1)

Default threshold policy (absolute delta unless stated otherwise):

- Field coverage decreases (metric keys only):
  - `coverage.parsed_successful_fetch_rate`
  - `coverage.parcel_summary_parcel_rate`
  - `coverage.parcel_year_fact_parcel_rate`
  - `coverage.parcel_year_fact_source_year_rate`
  - `coverage.parcel_characteristic_parcel_rate`
  - `coverage.parcel_lineage_parcel_rate`
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
- Tax-detail field presence rates decrease:
  - metric key pattern: `tax_detail_field_presence.*.rate`
  - directionality: lower values are always worse
  - `warn` when drop `>= 0.02`
  - `error` when drop `>= 0.05`
- Extraction null-rate increases:
  - `warn` when increase `>= 0.03`
  - `error` when increase `>= 0.08`

Sample-size guardrails:

- If `sample_size_current < 25`, severity is capped at `warn`.
- If either `sample_size_baseline` or `sample_size_current` is `0`, signal must set:
  - `severity = ok`
  - `ignored = true`
  - `ignore_reason = "insufficient_denominator"`

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

`run` field contract:

- `run` must use the same field shape and values as the associated diff artifact
  `run` object (`run_type`, `version_tag`, `run_id`, `diff_id`,
  `baseline_snapshot_id`, `current_snapshot_id`, `run_persisted`,
  `generated_at`, `status`), so alert consumers can join alerts directly back to
  the evaluated diff.

`alert` fields:

- `alert_id` (must match the corresponding `diff.alerts[0].alert_id`)
- `alert_type` (`parser_drift`)
- `severity` (`warn|error`)
- `reason_codes` (array of deduplicated signal reason codes from
  `impacted_signals` only)
- `routing_key`:
  - `warn`: `ops.parser_drift.warn`
  - `error`: `ops.parser_drift.error`
- `generated_at` (UTC RFC3339 with trailing `Z`)

`alert.reason_codes` derivation rule:

- Build from `impacted_signals[*].reason_code` only (not from all diff signals).
- Deduplicate and sort ascending for deterministic payloads.

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
