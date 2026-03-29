# Sprint 8 Observability And SLO Contract v1

Prepared on: 2026-03-29

## Purpose

Define the first stable observability and SLO contract before Sprint 8 Day 10
implementation begins.

This contract locks v1 behavior for:

- operational metrics derived from refresh, parser drift, load monitoring, annual refresh, and benchmark artifacts
- SLI/SLO definitions and error-budget burn alert thresholds
- dashboard and rollup artifact schemas
- publication cadence and operator review expectations

## Scope

In scope for v1:

- artifact-driven metrics for production operations health
- SLO evaluation over rolling windows
- burn-rate classification and alert severity mapping
- dashboard payload contracts for downstream rendering

Out of scope for v1:

- vendor-specific metrics backends or managed APM integrations
- per-tenant/custom SLO overrides
- automatic remediation triggered directly from burn alerts
- cost/usage observability for infrastructure billing

## Design Goals

- Keep observability deterministic and reproducible from existing run artifacts.
- Make SLO violations actionable with clear burn-rate severity and owner handoff.
- Support analyst-readiness decisions without requiring ad-hoc manual queries.
- Preserve environment boundaries and promotion controls from earlier Sprint 8 contracts.

## Baseline Assumptions

- refresh/scheduler contracts remain authoritative for run lifecycle semantics:
  - [Sprint 7 refresh automation contract](../SPRINT_7/REFRESH_AUTOMATION_V1.md)
  - [Sprint 8 scheduler reliability contract](SCHEDULER_RELIABILITY_V1.md)
- parser drift, load monitoring, and benchmark payload contracts remain unchanged:
  - [Sprint 7 parser drift contract](../SPRINT_7/PARSER_DRIFT_V1.md)
  - [Sprint 7 load monitoring contract](../SPRINT_7/LOAD_MONITORING_V1.md)
  - [Sprint 7 benchmark pack contract](../SPRINT_7/BENCHMARK_PACK_V1.md)
- annual refresh payload/signoff semantics remain unchanged:
  - [Sprint 7 annual refresh contract](../SPRINT_7/ANNUAL_REFRESH_V1.md)
- environment and route boundaries remain in force:
  - [environment and promotion contract](ENVIRONMENT_PROMOTION_V1.md)
  - [alert transport contract](ALERT_TRANSPORT_V1.md)

## Source Artifact Contract

Observability aggregation consumes only contract-defined artifact outputs. Source
paths below are environment-scoped under each environment's
`ACCESSDANE_ARTIFACT_BASE_DIR`.

| Domain | Required source artifact(s) | Required identifiers |
| --- | --- | --- |
| `refresh` | `refresh_run.json` and scheduler run payload for the same logical execution | `run_id`, `profile_name`, `run_date`, terminal `status`, `started_at_utc`, `finished_at_utc` |
| `parser_drift` | parser drift diff/alert payloads when emitted | `run_id`, `status`, `severity` (when alert emitted), `generated_at` |
| `load_monitoring` | `load_monitor.json`, `load_monitor_alert.json` (when emitted) | `run_id`, `status`, `severity` (when alert emitted), `generated_at` |
| `annual_refresh` | annual runner payload + annual signoff artifact | `run_id`, `status`, `stage_statuses`, `annual_signoff.run.status`, `finished_at_utc` |
| `benchmark_pack` | `benchmark_pack.json`, trend payload, companion alert payload (when emitted) | `benchmark_run_id`, `status`, `comparison.overall_severity`, `generated_at` |

Source-ingest rules:

- missing optional companion alerts are valid only when source contract says alert emission is conditional.
- missing required primary artifacts are recorded as `artifact_missing` failures in observability rollups.
- all timestamps must be normalized to UTC RFC3339 with trailing `Z`.

## Metric Taxonomy (v1)

Metrics are emitted in two classes:

- `event_metrics`: one record per observed run/event, used for traceability.
- `window_metrics`: aggregated metrics over fixed windows (`1h`, `6h`, `24h`, `28d`), used for SLO/burn evaluation.

Each metric record must include:

- `metric_id`
- `domain` (`refresh|parser_drift|load_monitoring|annual_refresh|benchmark_pack`)
- `environment`
- `profile_name` (nullable when not applicable)
- `window` (`event|1h|6h|24h|28d`)
- `observed_at_utc`
- `value`
- `numerator` and `denominator` (required for ratio/compliance metrics; nullable for gauges)

## SLI/SLO Contract v1

Measurement window:

- rolling 28-day window per `environment` and `profile_name` when profile-scoped.
- for annual refresh metrics, window is rolling 365 days due to low event frequency.

### 1) Refresh Reliability And Latency

SLI 1.1: refresh success ratio

- definition:
  - `successful_refresh_runs / total_refresh_runs`
  - include scheduled and catch-up runs; exclude operator-cancelled runs.
- SLO target:
  - `daily_refresh`: `>= 98.0%`
  - `analysis_only`: `>= 99.0%`
  - `annual_refresh`: `>= 95.0%`

SLI 1.2: refresh completion latency compliance

- definition:
  - ratio of runs meeting latency threshold:
    - `daily_refresh <= 90m`
    - `analysis_only <= 30m`
    - `annual_refresh <= 8h`
- SLO target:
  - all profiles `>= 95.0%` compliance

### 2) Parser Drift Health

SLI 2.1: parser drift error-free ratio

- definition:
  - runs with no `error`-severity parser drift alert / total parser drift evaluations.
- SLO target:
  - `>= 99.0%`

SLI 2.2: parser drift artifact coverage

- definition:
  - parser drift evaluations with required snapshot + diff artifacts present / total parser drift evaluations.
- SLO target:
  - `>= 99.5%`

### 3) Load Monitoring Health

SLI 3.1: load-monitor critical-free ratio

- definition:
  - load-monitor runs without `critical` alert / total load-monitor runs.
- SLO target:
  - `>= 98.5%`

SLI 3.2: load-monitor execution timeliness

- definition:
  - load-monitor runs completed within 10 minutes / total load-monitor runs.
- SLO target:
  - `>= 99.0%`

### 4) Annual Refresh Readiness

SLI 4.1: annual signoff success ratio

- definition:
  - annual refresh runs with `annual_signoff.run.status = approved` / total annual refresh runs that reached signoff stage.
- SLO target:
  - `>= 95.0%`

SLI 4.2: annual required-stage completion ratio

- definition:
  - annual refresh runs where all required stages reached `succeeded` / total annual refresh runs.
- SLO target:
  - `>= 97.0%`

### 5) Benchmark Pack Reliability And Freshness

SLI 5.1: benchmark generation success ratio

- definition:
  - successful benchmark-pack runs / total benchmark-pack runs.
- SLO target:
  - `>= 99.0%`

SLI 5.2: benchmark freshness compliance

- definition:
  - hourly checks where latest benchmark pack age <= 24h / total hourly checks.
- SLO target:
  - `>= 99.0%`

## Error Budget And Burn-Rate Policy

For ratio/compliance SLOs:

- `error_budget = 1 - slo_target`
- `observed_error_rate = bad_events / total_events`
- `burn_rate = observed_error_rate / error_budget`

Multi-window burn classification:

- `critical` burn alert:
  - trigger when `burn_rate_1h >= 14` and `burn_rate_6h >= 7`
- `warn` burn alert:
  - trigger when `burn_rate_6h >= 4` and `burn_rate_24h >= 2`
- `info` burn alert:
  - trigger when `burn_rate_24h >= 1` and 7-day exhaustion projection < 21 days

Low-traffic protection:

- do not emit burn alerts when denominator `< 20` events in the longer window;
  emit `insufficient_sample_size` note in dashboard and evaluation artifacts instead.

Alert transport mapping:

- for Alert Transport Contract v1 compatibility, observability burn alerts route as canonical `alert_type = load_monitoring`.
- `observability_slo` is carried as non-routing metadata (for example `alert_subtype = observability_slo`).
- severity maps directly to alert transport severity (`info|warn|critical`).

## Dashboard Artifact Contract v1

Rollup root:

- `<ACCESSDANE_ARTIFACT_BASE_DIR>/ops_observability/<run_date>/<observability_run_id>/`

Required files:

- `observability_rollup.json`
- `observability_slo_evaluation.json`
- `observability_dashboard_snapshot.json`
- `observability_metric_timeseries.csv`

### `observability_rollup.json` required structure

Top-level keys:

- `run`
- `inputs`
- `metrics`
- `slo_status`
- `burn_alerts`
- `errors`

`run` required fields:

- `observability_run_id`
- `run_date`
- `environment`
- `generated_at_utc`
- `window_start_utc`
- `window_end_utc`

`metrics` required fields:

- array of metric records conforming to Metric Taxonomy v1.

`slo_status` required fields:

- one record per SLI:
  - `sli_id`
  - `target`
  - `observed`
  - `compliant` (bool)
  - `error_budget_remaining` (0..1)

`burn_alerts` required fields:

- array of burn-alert objects:
  - `sli_id`
  - `severity`
  - `burn_rate_1h`
  - `burn_rate_6h`
  - `burn_rate_24h`
  - `triggered_at_utc`
  - `routing_key`

### `observability_slo_evaluation.json` required structure

Top-level keys:

- `evaluation`
- `sli_results`
- `non_computable`

`evaluation` required fields:

- `contract_version` (must be `observability_slo_v1`)
- `environment`
- `generated_at_utc`
- `measurement_window` (`28d` or `365d` for annual SLIs)

`sli_results` required fields:

- one record per SLI:
  - `sli_id`
  - `numerator`
  - `denominator`
  - `observed`
  - `target`
  - `error_budget_burn_rate` (nullable for non-ratio metrics)
  - `status` (`ok|burning|breached`)

### `observability_dashboard_snapshot.json` required structure

Top-level keys:

- `snapshot`
- `panels`
- `alerts`

Required panel ids:

- `refresh_reliability`
- `refresh_latency`
- `parser_drift_health`
- `load_monitor_health`
- `annual_refresh_readiness`
- `benchmark_freshness`
- `slo_burn_overview`

Each panel must include:

- `panel_id`
- `title`
- `window`
- `series[]` with labeled datapoints
- `status` (`ok|warn|critical|no_data`)

## Publication Cadence

Cadence rules:

- event-driven publish:
  - run Day 10 rollup/evaluation after each terminal scheduler execution, with
    max publication delay of 15 minutes.
- daily publish:
  - produce a full 28-day dashboard snapshot once per day by 06:00 UTC.
- weekly summary publish:
  - produce Monday 00:00 UTC weekly summary snapshot for trend review.

Retention:

- keep rollup/evaluation artifacts for 90 days minimum.
- keep weekly snapshots for 365 days minimum.

## Operator Expectations

Daily operator checklist:

1. review `observability_dashboard_snapshot.json` and confirm no `critical` panel status.
2. review `observability_slo_evaluation.json` for `status != ok`.
3. acknowledge any emitted observability burn alerts via standard alert transport workflow.
4. open incident ticket when any SLI remains `breached` for two consecutive daily snapshots.

## Backward Compatibility And Versioning

- v1 metrics and dashboard files are additive to existing run artifacts and must
  not alter source contract payload schemas.
- breaking changes to required fields, SLI identifiers, or burn thresholds
  require `observability_slo_v2`.
- Day 10 implementation must emit `contract_version = observability_slo_v1` in
  all observability artifacts.

## Non-Goals For v1

- real-time streaming dashboards (< 1-minute freshness)
- predictive anomaly detection/forecasting models
- automatic rerun/remediation driven directly by observability alerts
