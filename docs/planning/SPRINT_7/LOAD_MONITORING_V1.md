# Sprint 7 Load Monitoring and Alerting v1 Contract

Prepared on: 2026-03-17

## Purpose

Define the first stable contract for load monitoring and alerting in Sprint 7 before implementing diagnostics and threshold evaluators.

This contract locks v1 behavior for:

- load metric schema (durations, row volumes, queue sizes, failure rates, freshness windows)
- threshold policy (`ok|warn|critical`) and reason code contract
- alert payload contract and operator action mapping
- historical rollup windows used for recurring trend checks

## Scope

In scope for v1:

- one machine-readable load diagnostics payload for each refresh run
- deterministic threshold evaluation for each emitted metric
- one alert payload when overall severity is `warn` or `critical` on succeeded monitor runs
- rollup windows for short-term and medium-term trend checks

Out of scope for v1:

- external paging integrations (Slack/email/PagerDuty)
- auto-remediation or self-healing retries outside refresh-runner retry support
- anomaly models or adaptive threshold tuning
- cross-environment fleet dashboards

## Design Goals

- Keep load/freshness signals deterministic and auditable.
- Detect data staleness and load regressions early.
- Preserve low-noise alerting through explicit minimum-sample rules.
- Emit clear operator actions that map to existing Sprint 6 and Sprint 7 commands.

## Baseline Assumptions

- Sprint 7 refresh runner payloads are available under:
  - `data/refresh_runs/<run_date>/<profile_name>/<run_id>/health_summary/refresh_run_payload.json`
- Refresh payload shape follows:
  - `docs/planning/SPRINT_7/REFRESH_AUTOMATION_V1.md`
- Parser drift payloads are available for correlation but are optional for v1 load evaluation.
- `daily_refresh` is the primary monitored profile in v1; `analysis_only` is supported with stage-availability caveats.

## Contract Terminology

- `monitor run`: one invocation of the load diagnostics command.
- `subject run`: refresh run being evaluated.
- `sample`: one refresh run payload included in historical rollups.
- `metric`: one measured load/freshness quantity with threshold evaluation.
- `signal`: one metric plus severity and reason classification.

## Diagnostics Payload Contract (v1)

Top-level keys (canonical order):

1. `run`
2. `subject`
3. `summary`
4. `signals`
5. `rollups`
6. `alerts`
7. `diagnostics`
8. `error`

Key-order note:

- canonical key order is presentation-only; producers/consumers must validate by presence and field rules, not object member order

Presence rules:

- all top-level keys above are always present
- when `run.status = succeeded`, `error` must be `null`
- when `run.status = failed`, `signals` and `rollups` must be emitted as empty arrays and `error` must include `code` and `message`
- when `run.status = failed`, deterministic placeholders must be emitted:
  - `summary.overall_severity = critical`
  - all summary counts (`signal_count`, per-severity counts, `evaluated_metric_count`, `ignored_metric_count`) = `0`
  - `alerts = []`
  - `diagnostics.warnings` and `diagnostics.source_artifacts` must be present arrays; they may include pre-failure context collected before the error (or be empty when no context was collected)
  - `diagnostics.history.profile_name = subject.profile_name`
  - `diagnostics.history.sample_count_total = 0`
  - `diagnostics.history.window_bounds` must include `window_1d`, `window_7d`, `window_14d`, and `window_30d` with `start_at = null` and `end_at = null`
  - `diagnostics.threshold_overrides = {}`

### `run` fields

- `run_type` (`load_monitoring`)
- `version_tag` (`load_monitoring_v1`)
- `run_id` (unique monitor run identifier; equal to monitor execution identifier for v1 compatibility with common run-envelope consumers)
- `status` (`succeeded|failed`)
- `run_persisted` (bool; indicates whether monitor payload artifact write succeeded)
- `started_at` (RFC3339/ISO-8601 UTC with trailing `Z`, for example `2026-03-15T22:10:29Z`)
- `finished_at` (RFC3339/ISO-8601 UTC with trailing `Z`, for example `2026-03-15T22:10:31Z`)

### `subject` fields

- `run_id`
- `profile_name`
- `run_date` (`YYYYMMDD`)
- `feature_version`
- `ruleset_version`
- `refresh_payload_path` (nullable string path)
- `refresh_status` (`succeeded|failed|unknown`)
- placeholder rule: when refresh payload is unavailable/unreadable, emit `refresh_payload_path = null` and `refresh_status = unknown`
- `refresh_finished_at` (nullable RFC3339/ISO-8601 UTC with trailing `Z`)
- nullability rule: `refresh_finished_at` should mirror refresh payload `run.finished_at` when payload is available; it may be `null` only when refresh payload is unavailable or unreadable

### `summary` fields

- `overall_severity` (`ok|warn|critical`)
- `signal_count`
- `signal_ok_count`
- `signal_warn_count`
- `signal_critical_count`
- `evaluated_metric_count`
- `ignored_metric_count`

Summary count semantics:

- `signal_count` counts all emitted `signals`, including ignored signals
- `signal_ok_count`, `signal_warn_count`, and `signal_critical_count` count by `signals[].severity`, including ignored signals
- `evaluated_metric_count` counts `signals` where `ignored = false`
- `ignored_metric_count` counts `signals` where `ignored = true`
- for deterministic count behavior, when `ignored = true`, producers must emit `severity = ok`

Required arithmetic invariants:

- `signal_count = signal_ok_count + signal_warn_count + signal_critical_count`
- `signal_count = evaluated_metric_count + ignored_metric_count`

`overall_severity` derivation on succeeded runs:

- computed as max severity across non-ignored signals (`critical > warn > ok`)
- when all signals are ignored, `overall_severity = ok`

### `alerts` field contract

- `alerts` is always an array
- when `summary.overall_severity = ok`, `alerts` must be `[]`
- when `run.status = failed`, `alerts` must be `[]` regardless of `summary.overall_severity`
- when `summary.overall_severity = warn|critical`, `alerts` must contain exactly one embedded alert summary object with:
  - `alert_id`
  - `alert_type` (`load_monitoring`)
  - `severity` (`warn|critical`)
  - `severity` must equal `summary.overall_severity`
  - `generated_at` (RFC3339/ISO-8601 UTC with trailing `Z`)
  - `reason_codes` (sorted unique reason codes from non-ignored signals matching alert severity; ascending lexicographic sort on full reason code string)
  - `signal_count` (count of non-ignored signals matching alert severity)

### `diagnostics` fields

- `warnings` (array of warning strings)
- `source_artifacts` (array of artifact paths read by the monitor)
- `history` object:
  - `profile_name` (profile used for history lookup)
  - `sample_count_total` (candidate sample count before metric-specific filters)
  - `window_bounds` object keyed by rollup window (`window_1d`, `window_7d`, `window_14d`, `window_30d`) with `start_at` and `end_at` nullable timestamps (RFC3339/ISO-8601 UTC with trailing `Z` when non-null)
  - `window_bounds.*.start_at` / `end_at` may be `null` only for failed-run placeholders or when window bounds cannot be resolved from monitor context
- `threshold_overrides` (object; empty in v1 unless operator override flags are supplied)

### `rollups` field contract

- `rollups` is always an array of rollup objects
- rollup object schema and required fields are defined in `Historical Rollup Windows (v1)`
- deterministic ordering: `rollups` must be emitted in this order:
  1. `window_1d`
  2. `window_7d`
  3. `window_14d`
  4. `window_30d`

## Load Metric Contract (v1)

Each emitted signal must include:

- `signal_id` (stable identifier)
- `metric_key` (stable metric key)
- `family` (`duration|volume|queue_size|failure_rate|freshness`)
- `subject_value` (nullable float)
- `baseline_value` (nullable float)
- `delta_absolute` (nullable float)
- `delta_relative` (nullable float)
- `sample_size` (int)
- `severity` (`ok|warn|critical`)
- `reason_code`
- `ignored` (bool)
- `ignore_reason` (nullable string code)

Allowed `ignore_reason` values in v1:

- `metric_unavailable_for_profile`
- `insufficient_history`
- `missing_subject_value`
- `invalid_baseline`

Signal value computation rules:

- `delta_absolute` formula: `subject_value - baseline_value`
- `delta_absolute` is `null` when `subject_value` or `baseline_value` is `null`
- `delta_relative` formula: `(subject_value - baseline_value) / baseline_value`
- `delta_relative` is `null` when:
  - `subject_value` is `null`
  - `baseline_value` is `null`
  - `baseline_value == 0`
- when `subject_value` is `null`, signal must be emitted with `ignored = true`, `ignore_reason = missing_subject_value`, and `severity = ok`; threshold evaluation must be skipped
- when metric is unavailable for the active profile, signal must be emitted with `ignored = true`, `ignore_reason = metric_unavailable_for_profile`, and `severity = ok`; threshold evaluation must be skipped
- when `baseline_value == 0`, apply baseline-zero handling policy defined in `Threshold Policy Contract (v1)`
- `sample_size` is the denominator count used to compute the signal metric for the subject run
- for 7-day failure-rate metrics, `sample_size` is the denominator run count in that window
- for freshness metrics, `sample_size` is the count of successful samples considered when resolving the latest-success timestamp
- for duration, volume, and queue-size metrics derived from one subject refresh run, `sample_size` must be `1`
- when `subject_value` is unavailable and signal is ignored with `ignore_reason = missing_subject_value`, `sample_size` must be `0`

Baseline computation rules:

- baseline source window for relative-comparison metrics is `window_30d`
- baseline sample set includes prior runs only (subject run is excluded)
- baseline window membership must use the same per-sample `sample_event_at` resolution, window-bound derivation algorithm, and inclusion semantics defined in `Historical Rollup Windows (v1)`
- for duration, volume, and queue-size metrics, `baseline_value` is the `p50` of successful samples in the baseline window
- for failure-rate and freshness metrics, `baseline_value` is `null` in v1 because severity is evaluated with absolute threshold rules only
- if fewer than `3` successful baseline samples are available for a relative-comparison metric, signal must be ignored with `ignore_reason = insufficient_history`
- baseline `p50` for signal evaluation uses nearest-rank percentile semantics and is intentionally allowed with `>=3` samples (separate from rollup percentile publication thresholds)

Deterministic `signals` ordering:

- `signals` array must be sorted by:
  1. `metric_key` ascending
  2. `family` ascending
  3. `signal_id` ascending

### Required duration metrics

- `duration.total_seconds`
- `duration.stage.ingest_context.seconds`
- `duration.stage.build_context.seconds`
- `duration.stage.score_pipeline.seconds`
- `duration.stage.analysis_artifacts.seconds`
- `duration.stage.investigation_artifacts.seconds`
- `duration.stage.health_summary.seconds`

Source mapping:

- `duration.total_seconds` -> `summary.duration_seconds_total`
- stage durations -> `stages[].duration_seconds` by `stage_id`

### Required row-volume metrics

- `volume.review_queue.returned_count`
- `volume.review_feedback.reviewed_case_count`
- `volume.review_feedback.recommendation_count`

Expected source mapping:

- parse `analysis_artifacts/review_queue.json` summary returned count
- parse `analysis_artifacts/review_feedback.json` `summary.reviewed_case_count`
- parse `analysis_artifacts/review_feedback.json` `recommendations` arrays and derive:
  - `volume.review_feedback.recommendation_count = len(recommendations.threshold_tuning_candidates) + len(recommendations.exclusion_tuning_candidates)`

### Required queue-size metrics

- `queue_size.review_queue.high_risk_count`
- `queue_size.review_queue.unreviewed_count`
- `queue_size.case_review.in_review_count`

Expected source mapping:

- `review_queue.json` row band/status aggregates for current queue output
- case review list query snapshot emitted by the load monitor implementation

### Required failure-rate metrics

- `failure_rate.stage.failed_fraction_7d`
- `failure_rate.stage.blocked_fraction_7d`
- `failure_rate.run.failed_fraction_7d`

Computation:

- numerator = historical samples in window with matching failure condition
- denominator = historical samples in window
- metric emitted as fraction in `[0.0, 1.0]`
- subject run must be included in numerator/denominator when it falls inside the 7-day window
- numerator predicates by metric key:
  - `failure_rate.stage.failed_fraction_7d`: count samples where any stage in refresh payload `stages[]` has `status = failed`
  - `failure_rate.stage.blocked_fraction_7d`: count samples where any stage in refresh payload `stages[]` has `status = blocked`
  - `failure_rate.run.failed_fraction_7d`: count samples where refresh payload `run.status = failed`
- when denominator is `0`, emit `subject_value = null`, `sample_size = 0`, and ignore signal with `ignore_reason = insufficient_history`
- when denominator is less than `3`, emit computed fraction but ignore signal with `ignore_reason = insufficient_history` for alerting stability

### Required freshness metrics

- `freshness.hours_since_last_successful_daily_refresh`
- `freshness.hours_since_last_successful_score_pipeline`
- `freshness.hours_since_last_successful_review_feedback`

Computation:

- `now_utc - latest_successful_timestamp` in hours
- `now_utc` must be the monitor payload `run.finished_at` timestamp
- values must be rounded to 2 decimal places using round-half-up behavior (no truncation, no bankers rounding)
- freshness lookback sample set is `window_30d` membership using the same `sample_event_at` derivation and boundary semantics defined in `Historical Rollup Windows (v1)`
- eligible successful sample set for `latest_successful_timestamp` must include the subject run when it has qualifying success status for the evaluated freshness metric
- qualifying success predicate and timestamp source by metric key:
  - `freshness.hours_since_last_successful_daily_refresh`:
    - qualifying sample: refresh payload `run.status = succeeded`
    - timestamp source: refresh payload `run.finished_at`
  - `freshness.hours_since_last_successful_score_pipeline`:
    - qualifying sample: refresh payload contains `score_pipeline` stage with `status = succeeded`
    - timestamp source: matching `score_pipeline` stage `finished_at`
  - `freshness.hours_since_last_successful_review_feedback`:
    - qualifying sample: refresh payload contains `analysis_artifacts` stage with successful command result `command_id = review_feedback`
    - timestamp source: `analysis_artifacts` stage `finished_at`
- when `latest_successful_timestamp` is unavailable, emit `subject_value = null`, `sample_size = 0`, and ignore signal with `ignore_reason = insufficient_history`

## Threshold Policy Contract (v1)

Severity levels:

- `ok`
- `warn`
- `critical`

Policy precedence:

1. ignore rules
2. absolute freshness/failure guardrails
3. relative-change threshold checks
4. fallback to `ok`

Baseline-zero handling policy:

- for `duration`, `volume`, and `queue_size` families:
  - if `baseline_value == 0`, signal must be ignored with `ignore_reason = invalid_baseline`
  - `severity = ok`, `delta_relative = null`
- for `failure_rate` and `freshness` families:
  - `baseline_value` is `null`; severity is evaluated directly from absolute thresholds in this section

### Default threshold table

Durations:

- `warn` when `delta_relative >= 0.50`
- `critical` when `delta_relative >= 1.00`

Row/queue volumes:

- `warn` when `abs(delta_relative) >= 0.35`
- `critical` when `abs(delta_relative) >= 0.60`

Failure rates:

- `warn` when value `>= 0.10`
- `critical` when value `>= 0.20`

Freshness windows:

- `freshness.hours_since_last_successful_daily_refresh`:
  - `warn` >= `30`
  - `critical` >= `48`
- `freshness.hours_since_last_successful_score_pipeline`:
  - `warn` >= `36`
  - `critical` >= `60`
- `freshness.hours_since_last_successful_review_feedback`:
  - `warn` >= `48`
  - `critical` >= `72`

### Reason code contract

Format:

- `<family>.<reason_token>.<metric_key>`

Parsing rule:

- only the first two `.` separators are structural
- the remaining suffix is the literal `metric_key` (which may include additional `.` characters)

Allowed `reason_token` values:

- `ok_within_threshold`
- `warn_relative_delta`
- `critical_relative_delta`
- `warn_failure_rate`
- `critical_failure_rate`
- `warn_freshness`
- `critical_freshness`
- `ignored_unavailable_profile`
- `ignored_insufficient_history`
- `ignored_missing_subject_value`
- `ignored_invalid_baseline`

Reason-token derivation order:

1. if `ignored = true` and `ignore_reason = metric_unavailable_for_profile` -> `ignored_unavailable_profile`
2. else if `ignored = true` and `ignore_reason = insufficient_history` -> `ignored_insufficient_history`
3. else if `ignored = true` and `ignore_reason = missing_subject_value` -> `ignored_missing_subject_value`
4. else if `ignored = true` and `ignore_reason = invalid_baseline` -> `ignored_invalid_baseline`
5. else if `severity = critical` and `family = freshness` -> `critical_freshness`
6. else if `severity = warn` and `family = freshness` -> `warn_freshness`
7. else if `severity = critical` and `family = failure_rate` -> `critical_failure_rate`
8. else if `severity = warn` and `family = failure_rate` -> `warn_failure_rate`
9. else if `severity = critical` -> `critical_relative_delta`
10. else if `severity = warn` -> `warn_relative_delta`
11. else -> `ok_within_threshold`

## Alert Payload Contract (v1)

Emit alert payload only when the associated diagnostics payload has both:

- `run.status = succeeded`
- `summary.overall_severity != ok`

Top-level keys (canonical order):

1. `run`
2. `alert`
3. `impacted_signals`
4. `operator_actions`
5. `error`

Presence rules:

- `run`, `alert`, `impacted_signals`, `operator_actions`, and `error` are always present
- emitted v1 alert payloads must have `run.status = succeeded`
- `error` must be `null` for emitted v1 alerts
- `impacted_signals` must include all non-ignored signals whose `severity` matches `alert.severity`
- `impacted_signals[]` entries must use the same signal object schema defined in `Load Metric Contract (v1)`
- deterministic ordering: `impacted_signals` must be sorted by `metric_key`, then `family`, then `signal_id` (ascending)

### `run` fields

- alert payload `run` object must use the same field contract as diagnostics payload `run`:
  - `run_type`
  - `version_tag`
  - `run_id`
  - `status`
  - `run_persisted`
  - `started_at`
  - `finished_at`

### `alert` fields

- `alert_id` (stable join key, unique per subject run + severity)
- `alert_type` (`load_monitoring`)
- `severity` (`warn|critical`)
- `generated_at` (RFC3339/ISO-8601 UTC with trailing `Z`)
- `subject_run_id`
- `profile_name`
- `reason_codes` (sorted unique reason codes from `impacted_signals`, ascending lexicographic sort on full reason code string)
- `title`
- `summary`
- join invariant: for the same subject run + severity, standalone `alert.alert_id` must equal diagnostics payload `alerts[0].alert_id`

### `operator_actions` fields

- `operator_actions` is an ordered array of action objects
- each action object must include:
  - `action_id` (stable identifier)
  - `rank` (1-based integer order)
  - `severity` (`warn|critical`)
  - `severity` must equal top-level `alert.severity` for the emitted alert payload
  - `title`
  - `description`
  - `message` (compatibility alias of `description` for shared alert consumers)
  - `command` (nullable shell command string)
  - `required_artifact_paths` (array of artifact paths the action depends on)
  - `artifact_paths` (compatibility alias of `required_artifact_paths` for shared alert consumers)
  - `automatable` (bool; `false` in v1)
- alias equality invariants:
  - `message` must exactly equal `description`
  - `artifact_paths` must exactly equal `required_artifact_paths`
- deterministic ordering and rank invariants:
  - array must be sorted by `rank` ascending
  - `rank` values must be unique
  - first rank must be `1`
  - ranks must be contiguous integers (`1..N`) without gaps

Operator action mapping:

- warn alert actions (in order):
  1. review refresh payload and stage statuses for the subject run
  2. inspect latest parser drift diff output if available
  3. rerun `analysis_only` profile if data freshness is acceptable but report artifacts look stale
- critical alert actions (in order):
  1. run immediate `daily_refresh` rerun from failure boundary
  2. inspect ingest source availability and parser drift outputs
  3. validate score/report artifacts regenerate successfully
  4. if critical freshness persists for two consecutive runs, open incident note in operations log

## Historical Rollup Windows (v1)

Required windows:

- `window_1d`
- `window_7d`
- `window_14d`
- `window_30d`

Canonical `window_hours` mapping:

- `window_1d = 24`
- `window_7d = 168`
- `window_14d = 336`
- `window_30d = 720`

Each rollup object must include:

- `window_key`
- `window_hours`
- `sample_count`
- `successful_run_count`
- `failed_run_count`
- `duration_total_p50_seconds` (nullable)
- `duration_total_p95_seconds` (nullable)
- `review_queue_returned_p50` (nullable)
- `review_queue_returned_p95` (nullable)

Rollup count invariants:

- `sample_count = successful_run_count + failed_run_count`
- samples excluded for missing `sample_event_at` (per window inclusion rules) must not contribute to `sample_count` or either status count

Percentile derivation rules:

- percentile inputs use only included samples (after `sample_event_at` filtering)
- percentile method for all rollup `p50`/`p95` fields: nearest-rank on ascending sorted values (no interpolation)
- nearest-rank definition:
  - `rank = ceil((p / 100) * n)` with `p in {50, 95}` and `n = number of included values`
  - rank is 1-indexed over ascending sorted input values
- `duration_total_p50_seconds` / `duration_total_p95_seconds`:
  - source metric: refresh payload `summary.duration_seconds_total`
  - units: seconds
  - include only samples where source metric is present
- `review_queue_returned_p50` / `review_queue_returned_p95`:
  - source metric: `analysis_artifacts/review_queue.json` `summary.returned_count`
  - units: case-count
  - include only samples where source metric is present

Window inclusion rules:

- include samples by `sample_event_at` within window bounds
- `sample_event_at` resolution order is applied per historical sample:
  1. sample refresh payload `run.finished_at` when non-null
  2. sample refresh payload `run.started_at` when non-null
  3. otherwise sample has no `sample_event_at` and must be excluded from rollup window membership
- per-window `start_at`/`end_at` derivation:
  - `end_at` anchor is diagnostics payload `run.finished_at` (monitor run completion timestamp)
  - for each `window_key`, `start_at = end_at - window_hours`
  - if `run.finished_at` is unavailable (failed placeholder payload), both `start_at` and `end_at` must be `null`
- boundary semantics for membership:
  - use half-open interval `(start_at, end_at]`
  - sample is included iff `sample_event_at > start_at` and `sample_event_at <= end_at`
- use `daily_refresh` samples by default
- allow profile override in monitor request; profile must be explicit in output diagnostics

Minimum sample rules:

- relative-delta checks require at least `3` historical samples
- percentile rollups require at least `5` samples; otherwise percentile fields are `null`
- the 5-sample threshold applies to non-null percentile input values for that specific percentile field (not raw window `sample_count`)
- rollup percentile minimums apply only to rollup output fields and do not override the baseline `p50` rule used for signal evaluation

## Validation Fixtures (v1)

Fixture A: stable load

- subject durations/volumes within normal range
- expected `overall_severity = ok`, no alert emitted

Fixture B: mild runtime regression

- stage and total duration exceed warn threshold only
- expected `overall_severity = warn`, warn alert emitted

Fixture C: stale pipeline

- freshness exceeds critical threshold for score pipeline
- expected `overall_severity = critical`, critical alert emitted

Fixture D: repeated failed refreshes

- 7-day failed-fraction crosses critical threshold
- expected `overall_severity = critical`, critical alert emitted

Fixture E: insufficient history

- less than three historical samples
- expected relative-delta metrics ignored with `ignore_reason = insufficient_history` (and reason token `ignored_insufficient_history`)

## Non-Goals (v1)

- Implementing alert transport integrations.
- Changing refresh-runner stage semantics.
- Automatic threshold updates based on observed drift/load behavior.
