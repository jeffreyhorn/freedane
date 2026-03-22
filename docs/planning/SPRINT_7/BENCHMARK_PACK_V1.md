# Sprint 7 Benchmark Pack v1 Contract

Prepared on: 2026-03-21

## Purpose

Define the first stable benchmark-pack contract in Sprint 7 before implementing
benchmark generation commands.

This contract locks v1 behavior for:

- benchmark pack schema for recurring fairness/performance studies
- baseline-vs-current comparability keys and tolerance rules
- recurring benchmark cadence and retention expectations
- escalation criteria and operator actions for benchmark regressions

## Scope

In scope for v1:

- one machine-readable benchmark pack payload per benchmark run
- deterministic summary metrics for:
  - coverage
  - risk-band mix
  - disposition mix
  - geography/class segments
- deterministic baseline-vs-current comparison output with severity classification
- machine-readable escalation payload for warning/critical regressions

Out of scope for v1:

- automatic threshold/ruleset publication based only on benchmark results
- external paging integrations (Slack/email/PagerDuty)
- adaptive or model-driven tolerance tuning
- cross-jurisdiction benchmark normalization in one pack

## Design Goals

- Keep benchmark evidence deterministic, auditable, and reproducible.
- Make fairness/performance drift visible before promotion decisions.
- Reduce false alarms by requiring explicit minimum sample thresholds.
- Keep benchmark outputs compatible with threshold-promotion governance packet
  assembly.

## Baseline Assumptions

- Sprint 6 review workflow artifacts are available and stable:
  - `analysis_artifacts/review_queue.json`
  - `analysis_artifacts/review_feedback.json`
  - `investigation_artifacts/investigation_report.json`
- Sprint 7 refresh automation artifacts are available under:
  - `data/refresh_runs/<run_date>/<profile_name>/<run_id>/...`
- Annual refresh governance linkage is defined in:
  - `docs/planning/SPRINT_7/ANNUAL_REFRESH_V1.md`
- Threshold promotion packet requirements are defined in:
  - `docs/planning/SPRINT_7/THRESHOLD_PROMOTION_GOVERNANCE_NOTES.md`

## Contract Terminology

- `benchmark run`: one invocation of benchmark generation.
- `benchmark pack`: one produced benchmark payload artifact.
- `current`: measured period represented by the subject benchmark run.
- `baseline`: approved comparison benchmark selected by baseline policy.
- `segment`: one `(geography_key, class_key)` grouping.
- `signal`: one comparison metric with severity and reason code.

## Benchmark Pack Artifact Contract (v1)

Required canonical path:

- `data/benchmark_packs/<run_date>/<profile_name>/<benchmark_run_id>/benchmark_pack.json`

Optional companion paths (if generator emits split artifacts):

- `.../benchmark_pack_summary.json`
- `.../benchmark_pack_segments.csv`
- `.../benchmark_pack_alert.json` (emitted iff one or more `alerts[]` items exist)

Companion alert artifact contract (`benchmark_pack_alert.json`):

- emission rule:
  - emit iff one or more `alerts[]` items exist (`alerts.length > 0`)
  - do not emit when `alerts = []` (`alerts.length = 0`)
  - producers must ensure that if `comparison.overall_severity` is `warn` or `critical`, then `alerts.length >= 1`
- JSON shape:
  - top-level object with:
    - `generated_at` (RFC3339/ISO-8601 UTC with trailing `Z`)
    - `alert_count` (int)
    - `alerts` (array of shared alert summaries)
- each `alerts[]` companion summary must include:
  - `alert_id`
  - `alert_type` (`benchmark_pack`)
  - `severity` (`warn|critical`)
  - `reason_codes` (array of strings)
  - `summary` (string)
  - `generated_at` (RFC3339/ISO-8601 UTC with trailing `Z`)
  - `routing_key` (string)
  - `context` (object)
- companion mapping rules:
  - source records are from canonical benchmark pack payload `benchmark_pack.json` at `benchmark_pack.alerts[i]`
  - only source records with `benchmark_pack.alerts[i].level in {warn, critical}` are eligible for companion emission
  - destination records are in companion payload `benchmark_pack_alert.json` at `benchmark_pack_alert.alerts[j]`
  - `benchmark_pack_alert.alerts[j].alert_id = benchmark_pack.alerts[i].id`
  - `benchmark_pack_alert.alerts[j].alert_type = benchmark_pack`
  - `benchmark_pack_alert.alerts[j].severity = benchmark_pack.alerts[i].level`
  - `benchmark_pack_alert.alerts[j].reason_codes = [benchmark_pack.alerts[i].code]`
  - `benchmark_pack_alert.alerts[j].summary = benchmark_pack.alerts[i].message`
  - `benchmark_pack_alert.alerts[j].generated_at = benchmark_pack.alerts[i].created_at`
  - `benchmark_pack_alert.alerts[j].routing_key = <profile_name>:<feature_version>:<ruleset_version>:<benchmark_pack.alerts[i].code>`
  - `context` must include:
    - `scope` (from `benchmark_pack.alerts[i].scope`)
    - `segment_id` (nullable; from `benchmark_pack.alerts[i].segment_id`)
    - `signal_id` (nullable; from `benchmark_pack.alerts[i].signal_id`)
  - when segment/signal context is unavailable, values must be explicit `null` (not omitted)

Path token rule:

- `run_date` uses `YYYYMMDD`.

Top-level keys (canonical order):

1. `run`
2. `scope`
3. `summary`
4. `segments`
5. `comparison`
6. `alerts`
7. `diagnostics`
8. `error`

Canonical-order note:

- canonical top-level key order is for deterministic producer output and diff stability only
- JSON object member order is non-semantic; consumers must not rely on member ordering for parsing or validation

Presence rules:

- all top-level keys above must always be present
- when `run.status = succeeded`:
  - `summary`, `segments`, and `comparison` must reflect the completed benchmark
  - `alerts` may be empty or contain one or more alert items conforming to the `alerts` schema
  - `diagnostics` must be a non-null object conforming to the `diagnostics` schema
  - `error` must be `null`
- when `run.status = failed`:
  - `summary` must remain an object conforming to the summary schema and may use deterministic zero-value placeholders
  - `segments` may be emitted as an empty array (`[]`)
  - `comparison` must use deterministic failed-run placeholders:
    - `comparison.baseline_reference = null`
    - `comparison.comparable = false`
    - `comparison.non_comparable_reasons = ["run_failed"]`
    - `comparison.signals = []`
    - `comparison.overall_severity = ok`
  - `alerts` must be an empty array (`[]`)
  - `diagnostics` must be a non-null object conforming to the `diagnostics` schema
  - `diagnostics.failure` must be non-null and include failure metadata
  - `error` must be a non-null object conforming to the `error` schema and include at least `code` and `message`
- generators must not omit or change the type of any top-level key based on status; only the contents may vary

### `run` object

Required fields:

- `run_type` (`benchmark_pack`)
- `version_tag` (`benchmark_pack_v1`)
- `run_id` (must equal `benchmark_run_id`)
- `benchmark_run_id`
- `status` (`succeeded|failed`)
- `run_persisted` (bool)
- `started_at` (RFC3339/ISO-8601 UTC with trailing `Z`)
- `finished_at` (RFC3339/ISO-8601 UTC with trailing `Z`)

### `scope` object

Required fields:

- `profile_name` (for example `daily_refresh`, `annual_refresh`)
- `run_date` (`YYYYMMDD`)
- `feature_version`
- `ruleset_version`
- `source_run_id` (refresh run id used as benchmark source)
- `source_artifacts` (array of artifact paths used to compute benchmark)
- `period_start` (RFC3339/ISO-8601 UTC with trailing `Z`)
- `period_end` (RFC3339/ISO-8601 UTC with trailing `Z`)
- `period_length_days` (integer; inclusive UTC whole-day count for the benchmark period)
- `top_n` (int; reflects queue/report slicing policy)

`period_length_days` derivation rule (deterministic):

- convert `period_start` and `period_end` to UTC instants
- compute `D = period_end - period_start` in seconds
- compute `period_length_days = 1 + floor(D / 86400)`
- use this same derivation anywhere `period_length_days` appears, including `baseline_reference.period_length_days`

### `alerts` array

Top-level type:

- `alerts` is always an array

Each `alerts[]` item must include:

- `id` (string; stable, generator-assigned identifier)
- `level` (`warn|critical`)
- `code` (string; machine-parseable reason code)
- `message` (string; operator-readable summary)
- `scope` (`run|segment|comparison`)
- `created_at` (RFC3339/ISO-8601 UTC with trailing `Z`)

Optional fields:

- `segment_id` (string; required when `scope = segment`; must match a `segments[].segment_id`)
- `signal_id` (string; links to one `comparison.signals[].signal_id`)
- `metadata` (object; defaults to `{}`)

Additional rules:

- for `run.status = failed`, `alerts` must be `[]`
- consumers must tolerate unknown alert fields as a forward-compatible extension point

Alert routing compatibility:

- benchmark pack `alerts[]` reuses the Sprint 7 shared alert-routing model used by parser drift/load monitoring artifacts
- each benchmark alert must be converted into one shared alert summary object for routing
- canonical field mapping into the shared alert-routing payload:
  - `alerts[].id` -> `alert_id`
  - fixed `alert_type = benchmark_pack`
  - `alerts[].level` -> `severity`
  - `alerts[].code` -> `reason_codes` as single-element array (`[code]`)
  - `alerts[].message` -> `summary`
  - `alerts[].scope`, `alerts[].segment_id`, `alerts[].signal_id` -> routing context fields
  - `alerts[].created_at` -> `generated_at`
  - `routing_key` must be derived as `<profile_name>:<feature_version>:<ruleset_version>:<code>`
- consumers expecting one embedded alert summary must fan out `alerts[]` and route each item as a separate alert instance

### `diagnostics` object

Top-level type:

- `diagnostics` is always a non-null object

Required fields:

- `schema_version` (string; must be `benchmark_pack_v1`)
- `generator_name` (string)
- `generator_version` (string)
- `input_artifacts` (array of strings; canonical paths to key benchmark inputs)
- `runtime` object:
  - `duration_ms` (integer; `>= 0`)
  - `cpu_seconds` (number; `>= 0`; may be `0` if unavailable)
- `counters` object:
  - `total_segments` (integer; `>= 0`)
  - `skipped_segments` (integer; `>= 0`)
- `failure` (`object|null`):
  - must be `null` when `run.status = succeeded`
  - must be non-null when `run.status = failed` and include:
    - `stage` (string; for example `load_inputs`, `compute_segments`, `persist_outputs`, or `unknown`)
    - `class` (`validation|upstream|infrastructure|unexpected`)
    - `retryable` (boolean)
    - `details` (optional object; implementation-defined structured metadata)

Optional fields:

- `notes` (array of strings)
- `trace_id` (string)

### `error` object

Top-level type:

- `error` is `null` when `run.status = succeeded`
- `error` is non-null when `run.status = failed`

Required fields when non-null:

- `code` (string; stable machine-parseable failure code)
- `message` (string; human-readable failure summary)

Optional fields when non-null:

- `details` (`object|null`)
- `occurred_at` (RFC3339/ISO-8601 UTC with trailing `Z`)

Deterministic placeholder rules for failed runs:

- if failure occurs before domain metrics are computed:
  - `summary`, `segments`, and `comparison.signals` must use empty/zero-value payloads as allowed by their schemas
  - `alerts` must be `[]`
  - `diagnostics.failure.stage` should reflect the last known stage, or `unknown`
  - `error.code` and `error.message` must still be non-empty
- generators must not emit `error = null` when `run.status = failed`

## Summary Metric Schema (v1)

Top-level metric families in the benchmark pack:

- `summary` contains three required metric families:
  - coverage
  - risk-band mix
  - disposition mix
- `segments` is a separate top-level family for geography/class segmented metrics

Summary key notation and JSON shape:

- dotted names in this section (for example `coverage.queue_parcel_count`) are path-style identifiers, not literal JSON member names containing dots
- `summary` must be a nested JSON object with this structure:
  - `summary.coverage.<metric_name>`
  - `summary.risk_band_mix.<band>.{count,rate}`
  - `summary.disposition_mix.<disposition>.{count,rate}`
- producers must not flatten `summary` into dotted member names

`comparison.signals[].metric_key` mapping:

- for non-segment signals, `metric_key` maps to a path under `summary` by prefixing `summary.`
  - example: `metric_key = risk_band_mix.high.rate` maps to JSON path `summary.risk_band_mix.high.rate`
  - example: `metric_key = coverage.review_rate` maps to JSON path `summary.coverage.review_rate`
- segment-prefixed keys (`segment.{segment_id}...`) map to paths under the matching object in `segments[]`, not under `summary`

### 1) Coverage

Required keys:

- `coverage.scored_parcel_count`
- `coverage.queue_parcel_count`
- `coverage.reviewed_case_count`
- `coverage.requires_review_count`
- `coverage.review_rate` (`reviewed_case_count / max(queue_parcel_count, 1)`)

### 2) Risk-Band Mix

Required keys:

- `risk_band_mix.low.count`
- `risk_band_mix.low.rate`
- `risk_band_mix.medium.count`
- `risk_band_mix.medium.rate`
- `risk_band_mix.high.count`
- `risk_band_mix.high.rate`

Rate denominator rule:

- denominator is `queue_parcel_count`
- each `risk_band_mix.*.rate` must be computed as `count / max(queue_parcel_count, 1)` so zero-denominator cases never emit NaN/Infinity

### 3) Disposition Mix

Required keys:

- `disposition_mix.confirmed_issue.count`
- `disposition_mix.false_positive.count`
- `disposition_mix.inconclusive.count`
- `disposition_mix.needs_field_review.count`
- `disposition_mix.duplicate_case.count`
- `disposition_mix.unreviewed.count`
- `disposition_mix.<key>.rate` for each key above

Rate denominator rule:

- denominator is `reviewed_case_count` for reviewed dispositions
- `unreviewed.rate` denominator is `queue_parcel_count`
- each `disposition_mix.*.rate` must be computed as `count / max(denominator, 1)` using the applicable denominator above so zero-denominator cases never emit NaN/Infinity

### 4) Geography/Class Segments

`segments` is an array of segment objects.

Each segment must include:

- `segment_id` (`<geography_key>::<class_key>`)
- `geography_key`
- `class_key`
- `queue_parcel_count`
- `reviewed_case_count`
- `risk_band_mix` object with `low|medium|high` count/rate fields
- `disposition_mix` object with v1 disposition count/rate fields

Segment rate denominator rules (deterministic):

- segment `risk_band_mix.*.rate` values must be computed as `count / max(segment.queue_parcel_count, 1)`
- segment `disposition_mix.*.rate` values for reviewed dispositions must be computed as `count / max(segment.reviewed_case_count, 1)`
- segment `disposition_mix.unreviewed.rate` must be computed as `count / max(segment.queue_parcel_count, 1)`
- segment rate calculations must always use per-segment counts (never run-level totals)
- all segment rate calculations must use `max(denominator, 1)` so zero-denominator cases never emit NaN/Infinity

Segment identifier character constraints (v1):

- `geography_key` and `class_key` must use characters matching `[A-Za-z0-9_-]+`
- `segment_id` must equal `<geography_key>::<class_key>`
- `segment_id`, `geography_key`, and `class_key` must not contain `.`, whitespace, `*`, or `?`
- segment identifiers are opaque identifiers; do not embed additional dot-delimited structure

Deterministic segment ordering:

- `segments` must be sorted by:
  1. `geography_key` ascending
  2. `class_key` ascending

## Baseline-vs-Current Comparability Contract (v1)

`comparison` must include:

- `baseline_reference` (`object|null`)
- `comparable` (bool)
- `non_comparable_reasons` (array of strings)
- `signals` (array)
- `overall_severity` (`ok|warn|critical`)

### Baseline selection policy

Use this order:

1. explicit operator baseline override (if provided and comparable on all comparability keys)
2. latest approved benchmark baseline matching all comparability keys:
   - `profile_name`
   - `feature_version`
   - `ruleset_version`
   - `top_n`
   - `period_length_days`
3. latest prior successful benchmark pack matching all comparability keys above
4. if no comparable baseline exists, mark run non-comparable without synthesizing fallback baseline

### Comparability key contract

When `comparison.comparable = true`, `baseline_reference` must be a non-null object including:

- `benchmark_run_id`
- `run_date`
- `profile_name`
- `feature_version`
- `ruleset_version`
- `top_n`
- `period_length_days`

`comparison.comparable = true` only when all conditions hold:

- `profile_name` matches
- `feature_version` matches
- `ruleset_version` matches
- `top_n` matches
- `period_length_days` matches
- baseline artifact version is `benchmark_pack_v1`

If not comparable:

- `comparison.comparable = false`
- `comparison.non_comparable_reasons` must be non-empty
- `comparison.non_comparable_reasons` should include `no_comparable_baseline_found` when baseline lookup fails
- `comparison.baseline_reference = null`
- `comparison.signals` must be empty
- `comparison.overall_severity = ok`

Allowed `comparison.non_comparable_reasons` values (v1):

- `no_comparable_baseline_found`
- `profile_name_mismatch`
- `feature_version_mismatch`
- `ruleset_version_mismatch`
- `top_n_mismatch`
- `period_length_days_mismatch`
- `baseline_schema_version_mismatch`
- `operator_override_not_comparable`
- `run_failed`

### Signal schema

Each `comparison.signals[]` item must include:

- `signal_id`
- `family` (`coverage|risk_band_mix|disposition_mix|segment_mix`)
- `metric_key`
- `baseline_value` (nullable float)
- `current_value` (nullable float)
- `delta_absolute` (nullable float)
- `delta_relative` (nullable float)
- `sample_size` (int)
- `severity` (`ok|warn|critical`)
- `reason_code` (non-empty string; see v1 reason code contract)
- `ignored` (bool)
- `ignore_reason` (nullable string)

v1 `reason_code` contract:

- format: `<family>.<reason_token>.<metric_key>`
  - `family` must exactly equal `comparison.signals[].family`
  - `metric_key` must exactly equal `comparison.signals[].metric_key`
  - parsing rule: only the first two `.` separators are structural (`family` and `reason_token`);
    the remaining suffix is the literal `metric_key` (which may itself contain `.`)
  - `reason_token` must be one of:
    - `baseline_missing`
    - `current_missing`
    - `non_comparable`
    - `low_sample_size`
    - `no_change`
    - `within_tolerance`
    - `beyond_tolerance`

Deterministic `reason_token` derivation order (first match wins):

1. if `ignored = true` and `ignore_reason = insufficient_sample_size`, use `low_sample_size`
2. else if `baseline_value = null` and `current_value != null`, use `baseline_missing`
3. else if `current_value = null` and `baseline_value != null`, use `current_missing`
4. else if `baseline_value = null` and `current_value = null`, use `non_comparable`
5. else if `delta_absolute = 0`, use `no_change`
6. else if `severity = warn` or `severity = critical`, use `beyond_tolerance`
7. else if `severity = ok`, use `within_tolerance`
8. else use `non_comparable`

`sample_size` semantics (deterministic by metric namespace):

- `coverage.*` signals: `sample_size = summary.coverage.queue_parcel_count`
- `risk_band_mix.*` signals: `sample_size = summary.coverage.queue_parcel_count`
- `disposition_mix.*` signals:
  - for `disposition_mix.unreviewed.rate`: `sample_size = summary.coverage.queue_parcel_count`
  - for other `disposition_mix.*.rate`: `sample_size = summary.coverage.reviewed_case_count`
- `segment.*` signals:
  - find the segment object in `segments[]` where `segments[].segment_id == {segment_id}` from `metric_key`
  - for `segment.{segment_id}.risk_band_mix.*.rate_delta_abs`: `sample_size = matched_segment.queue_parcel_count`
  - for `segment.{segment_id}.disposition_mix.unreviewed.rate_delta_abs`: `sample_size = matched_segment.queue_parcel_count`
  - for other `segment.{segment_id}.disposition_mix.*.rate_delta_abs`: `sample_size = matched_segment.reviewed_case_count`
- if required source counts are unavailable, emit `sample_size = 0` and mark signal ignored using `ignore_reason = missing_current_metric`

Allowed `ignore_reason` values:

- `insufficient_sample_size`
- `missing_baseline_metric`
- `missing_current_metric`

Canonical `metric_key` formats:

- run-level and aggregate metrics use direct field-path style keys (for example `risk_band_mix.high.rate`)
- segment-level signals must use:
  - `segment.{segment_id}.disposition_mix.false_positive.rate_delta_abs`
  - `segment.{segment_id}.risk_band_mix.high.rate_delta_abs`
- `{segment_id}` must exactly match one `segments[].segment_id`
- for unambiguous round-tripping in dot-delimited `metric_key` values, `segment_id` (and component `geography_key`/`class_key`) must follow the segment identifier character constraints above
- `family` assignment is deterministic:
  - if `metric_key` starts with `segment.`, `family` must be `segment_mix`
  - otherwise `family` must match the metric namespace (`coverage`, `risk_band_mix`, or `disposition_mix`)

Value and delta semantics:

- all `*.rate` values in this contract are fractions on `[0, 1]` (not `[0, 100]`)
- rate deltas and thresholds use the same `[0, 1]` scale (for example `0.04` means a 4-point fraction-scale change)
- for non-segment-delta metrics:
  - `baseline_value` = baseline metric value for `metric_key`
  - `current_value` = current metric value for `metric_key`
  - `delta_absolute` = signed delta (`current_value - baseline_value`)
  - `delta_relative` = `(current_value - baseline_value) / baseline_value` when `baseline_value != 0`, else `null`
- for segment keys ending in `rate_delta_abs`:
  - `baseline_value` and `current_value` are the underlying per-segment rates
  - `delta_absolute` = absolute delta `abs(current_value - baseline_value)`
  - `delta_relative` = `null`

Deterministic `comparison.signals` ordering:

- producers must emit `comparison.signals` sorted by:
  1. `metric_key` ascending
  2. `family` ascending
  3. `signal_id` ascending

### Drift tolerance policy (default v1)

Absolute delta thresholds (fraction-scale rate points unless noted):

- `risk_band_mix.high.rate`:
  - evaluate increase-only using signed `delta_absolute`
  - `warn`: `delta_absolute >= 0.04`
  - `critical`: `delta_absolute >= 0.08`
- `risk_band_mix.medium.rate`:
  - evaluate absolute magnitude using `abs(delta_absolute)`
  - `warn`: `abs(delta_absolute) >= 0.06`
  - `critical`: `abs(delta_absolute) >= 0.10`
- `disposition_mix.false_positive.rate`:
  - evaluate increase-only using signed `delta_absolute`
  - `warn`: `delta_absolute >= 0.03`
  - `critical`: `delta_absolute >= 0.06`
- `disposition_mix.confirmed_issue.rate`:
  - evaluate drop-only using signed `delta_absolute`
  - `warn`: `delta_absolute <= -0.03`
  - `critical`: `delta_absolute <= -0.06`
- `coverage.review_rate`:
  - evaluate drop-only using signed `delta_absolute`
  - `warn`: `delta_absolute <= -0.10`
  - `critical`: `delta_absolute <= -0.20`

Segment-level policy:

- for each segment with `queue_parcel_count >= 25`, evaluate exactly two absolute delta metrics:
  - `segment.{segment_id}.disposition_mix.false_positive.rate_delta_abs`
  - `segment.{segment_id}.risk_band_mix.high.rate_delta_abs`
- delta definitions:
  - `segment.{segment_id}.disposition_mix.false_positive.rate_delta_abs = abs(current_segment.disposition_mix.false_positive.rate - baseline_segment.disposition_mix.false_positive.rate)`
  - `segment.{segment_id}.risk_band_mix.high.rate_delta_abs = abs(current_segment.risk_band_mix.high.rate - baseline_segment.risk_band_mix.high.rate)`
- `warn` at `>= 0.07`; `critical` at `>= 0.12`

Minimum-sample guardrails:

- run-level disposition metrics: do not severity-evaluate `disposition_mix.*.rate` when `summary.coverage.reviewed_case_count < 20`
- segment-level disposition metrics (including `segment.{segment_id}.disposition_mix.false_positive.rate_delta_abs`): do not severity-evaluate when that segment's `reviewed_case_count < 20`
- segment metrics in general: do not severity-evaluate when segment `queue_parcel_count < 25`
- guarded metrics must emit `ignored = true` with `ignore_reason = insufficient_sample_size`

`comparison.overall_severity` derivation:

- max severity over non-ignored signals (`critical > warn > ok`)
- if all signals are ignored, `overall_severity = ok`

## Cadence Policy (v1)

Required recurring cadence:

- `daily_refresh` benchmark pack: weekly (recommended every Monday)
- `annual_refresh` benchmark pack: required for each annual cutover candidate run
- `annual_refresh` benchmark pack in correction replay mode: required for each `annual_refresh` run with `request.replay_mode = correction_replay`

On-demand benchmark runs are allowed when:

- threshold-promotion proposal is being assembled
- operator investigates suspected fairness/performance regressions

## Retention Expectations (v1)

Minimum retention policy:

- retain all benchmark pack JSON artifacts for at least `400` days
- retain segment CSV companions (if emitted) for at least `400` days
- maintain latest pointers by profile and version:
  - `data/benchmark_packs/latest/<profile_name>/<feature_version>/<ruleset_version>/latest_benchmark_pack.json`
  - `data/benchmark_packs/latest/<profile_name>/<feature_version>/<ruleset_version>/latest_benchmark_pack_alert.json` (when present)

Baseline retention policy:

- never delete benchmark packs referenced by an approved threshold-promotion decision record
- baseline pointers must be versioned and auditable; rebasing baseline requires
  explicit operator action and decision note

## Escalation Criteria And Operator Actions (v1)

### Escalation criteria

Escalation level is based on `comparison.overall_severity`:

- `ok`: no escalation
- `warn`: analyst review required before next promotion proposal
- `critical`: promotion freeze recommended until triage completes

Mandatory `critical` escalation triggers:

- any non-ignored `critical` signal in `risk_band_mix` or `disposition_mix`
- `coverage.review_rate` critical drop
- two or more distinct segment-level critical signals in one benchmark run

### Required operator actions

When `warn`:

1. review top contributing `reason_code` values
2. validate source artifact integrity for benchmark inputs
3. attach benchmark summary to open governance notes if promotion is pending

When `critical`:

1. open an incident/tracking entry in the operations log
2. run targeted refresh/load diagnostics recheck
3. attach benchmark pack to threshold promotion packet as blocking evidence
4. require explicit reviewer sign-off before any threshold/ruleset publication

## Governance Linkage (v1)

Threshold promotion evidence packet integration:

- when `benchmark_pack_summary.json` is emitted, `threshold_promotion_decision.evidence.benchmark_summary_path` must reference that summary artifact
- when split summary is not emitted, `threshold_promotion_decision.evidence.benchmark_summary_path` must reference the canonical `benchmark_pack.json`
- when benchmark severity is `critical`, promotion decision must not be `approved`
  without explicit documented override rationale

## Non-Goals And Deferred Items

Deferred beyond v1:

- automated tolerance tuning from rolling history
- fairness metrics that require external demographic joins
- automatic benchmark-triggered model/rules rollback
- multi-profile unified benchmark dashboards

## Acceptance Criteria For Day 11 Contract Completion

- benchmark pack payload schema is explicit and versioned
- comparability keys and non-comparable behavior are explicit
- drift tolerances and minimum-sample guardrails are explicit
- cadence and retention policy are explicit
- escalation criteria and governance linkage are explicit
