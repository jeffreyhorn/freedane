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
- one alert payload when overall severity is `warn` or `critical`
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

Presence rules:

- all top-level keys above are always present
- when `run.status = succeeded`, `error` must be `null`
- when `run.status = failed`, `signals` and `rollups` must be emitted as empty arrays and `error` must include `code` and `message`

### `run` fields

- `run_type` (`load_monitoring`)
- `version_tag` (`load_monitoring_v1`)
- `monitor_id` (unique monitor run identifier)
- `status` (`succeeded|failed`)
- `started_at` (UTC ISO-8601)
- `finished_at` (UTC ISO-8601)

### `subject` fields

- `run_id`
- `profile_name`
- `run_date` (`YYYYMMDD`)
- `feature_version`
- `ruleset_version`
- `refresh_payload_path`
- `refresh_status` (`succeeded|failed`)
- `refresh_finished_at` (nullable ISO-8601 for failed pre-stage runs)

### `summary` fields

- `overall_severity` (`ok|warn|critical`)
- `signal_count`
- `signal_ok_count`
- `signal_warn_count`
- `signal_critical_count`
- `evaluated_metric_count`
- `ignored_metric_count`

## Load Metric Contract (v1)

Each emitted signal must include:

- `signal_id` (stable identifier)
- `metric_key` (stable metric key)
- `metric_family` (`duration|volume|queue_size|failure_rate|freshness`)
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
- `volume.review_feedback.reviewed_rows`
- `volume.review_feedback.recommendation_count`

Expected source mapping:

- parse `analysis_artifacts/review_queue.json` summary returned count
- parse `analysis_artifacts/review_feedback.json` summary reviewed row count and recommendation count

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

### Required freshness metrics

- `freshness.hours_since_last_successful_daily_refresh`
- `freshness.hours_since_last_successful_score_pipeline`
- `freshness.hours_since_last_successful_review_feedback`

Computation:

- `now_utc - latest_successful_timestamp` in hours
- values are floats with 2 decimal precision in outputs

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

- `<metric_family>.<reason_token>.<metric_key>`

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

## Alert Payload Contract (v1)

Emit alert payload only when `summary.overall_severity != ok`.

Top-level keys (canonical order):

1. `run`
2. `alert`
3. `impacted_signals`
4. `operator_actions`
5. `error`

Presence rules:

- `error` must be `null` for emitted v1 alerts
- `impacted_signals` must include all non-ignored signals whose `severity` matches `alert.severity`

### `alert` fields

- `alert_id` (stable join key, unique per subject run + severity)
- `alert_type` (`load_monitoring`)
- `severity` (`warn|critical`)
- `generated_at` (UTC ISO-8601)
- `subject_run_id`
- `profile_name`
- `reason_codes` (sorted unique reason codes from `impacted_signals`)
- `title`
- `summary`

### Operator action mapping

For `warn` alerts:

1. review refresh payload and stage statuses for the subject run
2. inspect latest parser drift diff output if available
3. rerun `analysis_only` profile if data freshness is acceptable but report artifacts look stale

For `critical` alerts:

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

Each rollup object must include:

- `window_key`
- `window_hours`
- `sample_count`
- `successful_run_count`
- `failed_run_count`
- `duration_total_p50_seconds`
- `duration_total_p95_seconds`
- `review_queue_returned_p50`
- `review_queue_returned_p95`

Window inclusion rules:

- include samples by `refresh_finished_at` within window bounds
- use `daily_refresh` samples by default
- allow profile override in monitor request; profile must be explicit in output diagnostics

Minimum sample rules:

- relative-delta checks require at least `3` historical samples
- percentile rollups require at least `5` samples; otherwise percentile fields are `null`

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
- expected relative-delta metrics ignored with `ignored_insufficient_history`

## Non-Goals (v1)

- Implementing alert transport integrations.
- Changing refresh-runner stage semantics.
- Automatic threshold updates based on observed drift/load behavior.
