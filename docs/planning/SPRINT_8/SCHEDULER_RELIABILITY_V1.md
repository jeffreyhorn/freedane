# Sprint 8 Scheduler Reliability Contract v1

Prepared on: 2026-03-26

## Purpose

Define the first stable contract for managed scheduler behavior before Sprint 8 scheduler integration implementation begins.

This contract locks v1 behavior for:

- trigger intake and scheduling semantics
- retry, backoff, and dead-letter routing rules
- overlap/concurrency policy for recurring refresh profiles
- scheduler run and incident state transitions
- required scheduler failure metadata and operator-facing artifacts
- SLA/SLO targets for schedule adherence and run completion

## Scope

In scope for v1:

- managed scheduling for `accessdane refresh-runner` profile executions
- scheduled, manual, and catch-up trigger types
- deterministic retry orchestration with bounded attempts
- dead-letter persistence for terminal scheduler failures
- incident lifecycle states tied to scheduler outcomes

Out of scope for v1:

- cloud-vendor-specific scheduler products and APIs
- cross-region active/active execution
- auto-remediation beyond bounded retry policy
- policy promotion workflow (covered in later Sprint 8 days)

## Design Goals

- Preserve deterministic refresh-run artifacts while adding resilient orchestration.
- Prevent silent run loss for missed windows, overlap, or transient failures.
- Keep failure handling auditable through explicit state transitions.
- Ensure operators can triage from one scheduler failure payload.

## Baseline Assumptions

- `refresh-runner` contract remains authoritative for stage semantics and payload shape:
  - [Sprint 7 refresh automation contract](../SPRINT_7/REFRESH_AUTOMATION_V1.md)
- Existing lock behavior remains the overlap safety primitive:
  - per-profile lock under `<ACCESSDANE_ARTIFACT_BASE_DIR>/locks/<profile>.lock` (with `data/...` as the default artifact base dir)
- Environment boundary rules from Sprint 8 remain in force:
  - [environment and promotion contract](ENVIRONMENT_PROMOTION_V1.md)

## Managed Scheduler Model

### Logical Components

- `trigger_planner`:
  - materializes due schedule windows into trigger records
- `run_dispatcher`:
  - chooses next eligible trigger for execution
- `run_executor`:
  - invokes `refresh-runner` with resolved run context
- `failure_router`:
  - classifies failures into retry vs dead-letter outcomes

### Execution Unit

A scheduler execution unit is one `scheduler_run_id` that may contain one or more attempts for the same logical trigger.

`refresh-runner` `run_id` remains the canonical run identifier for stage payloads and run-root artifacts.

## Trigger Model v1

Supported trigger types:

- `scheduled`:
  - produced from recurring schedule definitions
- `manual_retry`:
  - operator-initiated rerun from explicit boundary/context
- `catch_up`:
  - system-generated rerun for a missed/deferred schedule window

Required trigger fields:

| Field | Description |
| --- | --- |
| `trigger_id` | Unique immutable trigger identifier. |
| `trigger_type` | `scheduled|manual_retry|catch_up`. |
| `profile_name` | Refresh profile (`daily_refresh`, `analysis_only`, etc.). |
| `scheduled_for_utc` | Intended schedule window timestamp in UTC. |
| `created_at_utc` | Trigger creation timestamp. |
| `requested_by` | `system` for scheduled/catch-up; actor id for manual retry. |
| `run_context` | Requested feature/ruleset/version/file context for execution. |

## Retry, Backoff, And Dead-Letter Contract

### Attempt Limits

Default max attempts per scheduler execution unit:

- `scheduled`: `3`
- `catch_up`: `3`
- `manual_retry`: `2`

Attempt 1 is the initial dispatch; retries consume remaining attempt budget.

### Backoff Policy

Backoff delay before each retry attempt:

- `delay_seconds = min(300 * 2**(attempt_index - 2), 3600) + jitter_seconds`
- `jitter_seconds` uniform in `[0, 60]`
- attempt indices are 1-based; backoff applies starting at attempt 2

### Retry Classification Rules

Scheduler-level classification uses refresh payload status plus execution context:

Retry-eligible failure classes:

- executor/process interruption before stable payload write
- `refresh-runner` failure with `error.code = overlapping_run`
- `refresh-runner` failure with `error.code = stage_failure`
- `refresh-runner` failure with `error.code = annual_refresh_failed`

Non-retryable failure classes:

- invalid run context/configuration (`invalid_run_context`)
- unsupported profile (`unsupported_profile`)
- invalid retry boundary (`invalid_retry_boundary`)
- annual preflight validation failures (`annual_preflight_failed`)

If `max_attempts` is exhausted for a retry-eligible class, the execution unit transitions to dead-letter.

### Dead-Letter Routing

Dead-letter artifact root (environment-local):

- `<ACCESSDANE_ARTIFACT_BASE_DIR>/dead_letter/<profile_name>/<run_date>/<scheduler_run_id>.json`

Dead-letter payload must include:

- final `scheduler_run.state = dead_lettered`
- terminal failure class and reason
- all attempt summaries (attempt index, start/end, exit/error)
- pointers to any produced refresh payload artifacts
- recommended operator action summary

## Overlap Policy v1

Concurrency key:

- `<ACCESSDANE_ENVIRONMENT>:<profile_name>`

Hard rule:

- at most one active `running` scheduler execution for a concurrency key.

When overlap is detected on dispatch:

1. first overlap result for the same schedule window:
   - transition trigger/execution to `retry_pending`
   - apply retry backoff policy
2. overlap persisting through all attempt budget:
   - transition to `dead_lettered`
   - open incident with overlap-specific reason

No second concurrent run is allowed to bypass profile lock behavior.

## Scheduler Run State Contract

### States

Allowed scheduler execution states:

- `queued`
- `dispatched`
- `running`
- `retry_pending`
- `succeeded`
- `failed_pending_dead_letter`
- `dead_lettered`
- `cancelled`

### Required Transitions

- `queued -> dispatched`
- `dispatched -> running|retry_pending|failed_pending_dead_letter`
- `running -> succeeded|retry_pending|failed_pending_dead_letter`
- `retry_pending -> dispatched|dead_lettered`
- `failed_pending_dead_letter -> dead_lettered`
- terminal states: `succeeded`, `dead_lettered`, `cancelled`

Transition invariants:

- in this contract, an attempt is a scheduler dispatch attempt (a transition into `dispatched`), regardless of whether execution later reaches `running`.
- `attempt_count` increments on each transition to `dispatched`, so dispatch-time outcomes (for example overlap or dispatch errors) consume attempt budget.
- `started_at_utc` and `finished_at_utc` must be set for each execution attempt (each time the run enters `running`).
- `failed_pending_dead_letter` is non-terminal and must transition to `dead_lettered` before execution is considered terminal.
- terminal transitions must set required failure metadata (`failure_class`, `failure_code`, and `failure_message` when applicable).

## Incident State Contract

### Incident States

Allowed incident states:

- `open`
- `acknowledged`
- `mitigating`
- `resolved`
- `closed`

### Incident Opening Conditions

Open incident when any of these occur:

- scheduler execution enters `dead_lettered`
- same profile has two consecutive executions ending in a non-success terminal state
- overlap retries for a scheduled window exhaust attempt budget

### Incident Transition Rules

- initial state is `open`
- `open -> acknowledged|resolved`
- `acknowledged -> mitigating|resolved`
- `mitigating -> resolved`
- `resolved -> closed`

Required incident metadata:

- `incident_id`
- `opened_at_utc`
- `severity` (`critical|warn|info`)
- `scheduler_run_id`
- `profile_name`
- `failure_class`
- `acknowledged_by` (nullable until acknowledged)
- `mitigation_summary` (nullable until mitigation)
- `resolved_at_utc` (nullable until resolved)

## Required Scheduler Run Metadata

Scheduler run payload path:

- `<ACCESSDANE_REFRESH_LOG_DIR>/<scheduler_run_id>.json` (`ACCESSDANE_REFRESH_LOG_DIR` defaults to `<ACCESSDANE_ARTIFACT_BASE_DIR>/scheduler_logs`)

Required top-level keys:

1. `scheduler_run`
2. `trigger`
3. `attempts`
4. `result`
5. `incident`

Required `scheduler_run` fields:

| Field | Description |
| --- | --- |
| `scheduler_run_id` | Immutable execution-unit identifier. |
| `state` | Current scheduler execution state. |
| `profile_name` | Active refresh profile. |
| `run_date` | Logical run date (`YYYYMMDD`, UTC). See derivation rule below. |
| `refresh_run_id` | Bound refresh-runner `run_id` (nullable until dispatch). |
| `attempt_count` | Total attempts executed so far. |
| `max_attempts` | Attempt budget for this execution unit. |
| `created_at_utc` | Creation timestamp. |
| `updated_at_utc` | Last state transition timestamp. |

`run_date` derivation rule (must be applied consistently across scheduler payloads and artifact paths):

- for `scheduled` and `catch_up` triggers: `run_date = format_utc_yyyymmdd(trigger.scheduled_for_utc)`
- for `manual_retry` triggers: `run_date = format_utc_yyyymmdd(scheduler_run.created_at_utc)` using the first persisted creation timestamp
- derived `run_date` is immutable for a given `scheduler_run` and must be used verbatim in dead-letter/artifact path segments that include run-date components

Required `result` fields:

| Field | Description |
| --- | --- |
| `status` | Terminal scheduler outcome (`succeeded|dead_lettered|cancelled`). |
| `failure_class` | Nullable; required for non-success outcomes (`retryable|non_retryable|exhausted_retries`). |
| `failure_code` | Nullable; required for non-success outcomes. |
| `failure_message` | Nullable; required for non-success outcomes. |
| `failed_stage_id` | Nullable; refresh failed stage id when available. |
| `attempt_count` | Attempt count at terminal outcome. |
| `max_attempts` | Attempt budget applied to this execution unit. |
| `last_attempt_finished_at_utc` | Nullable; timestamp of last finished attempt. |
| `dead_letter_path` | Nullable unless `status = dead_lettered`. |

Required `attempts[]` fields:

| Field | Description |
| --- | --- |
| `attempt_index` | 1-based attempt number. |
| `state` | Attempt terminal state (`succeeded|failed|overlap_blocked|dispatch_error`). |
| `started_at_utc` | Attempt start timestamp. |
| `finished_at_utc` | Attempt finish timestamp. |
| `duration_seconds` | Attempt wall-clock duration. |
| `refresh_payload_path` | Path to refresh payload if written; else `null`. |
| `failure_code` | Failure/error code if non-success. |
| `failure_message` | Human-readable failure summary if non-success. |
| `failed_stage_id` | Refresh failed stage id when available; else `null`. |

## SLA/SLO Contract v1

Measurement window:

- rolling 28-day window per environment/profile pair.

### SLI 1: Schedule Adherence

Definition:

- `schedule_start_delay_seconds = first_running_started_at_utc - scheduled_for_utc`

SLO target:

- 99% of scheduled executions start within 10 minutes (`<= 600s`).
- 100% start within 30 minutes (`<= 1800s`) unless marked dead-lettered due to sustained overlap.

### SLI 2: Run Completion Reliability

Definition:

- ratio of scheduled executions ending in `succeeded` within attempt budget.

SLO target:

- `daily_refresh`: >= 97.5%
- `analysis_only`: >= 99.0%

### SLI 3: Completion Latency

Definition:

- wall-clock from first attempt start to terminal state.

SLO target:

- `daily_refresh` p95 <= 90 minutes
- `analysis_only` p95 <= 30 minutes

### SLI 4: Dead-Letter Rate

Definition:

- dead-lettered scheduled executions / total scheduled executions.

SLO target:

- <= 1.0%

## Operator Expectations

On dead-letter or critical scheduler incident:

1. inspect scheduler run payload and final attempt failure metadata
2. inspect referenced refresh payload and stage command results
3. apply one of:
   - manual retry from failed stage boundary
   - corrective config/input fix followed by new scheduled run
   - incident escalation if data/platform dependency is unresolved

## Backward Compatibility And Versioning

- `refresh-runner` payload contract remains unchanged by this document.
- scheduler metadata envelopes are additive and must not remove existing run-root artifacts.
- breaking changes to scheduler state enums, required fields, or retry policy require a v2 contract.

## Non-Goals For v1

- dynamic priority preemption between profiles
- arbitrary long-lived trigger queues and historical replay orchestration
- cross-environment failover scheduler handoff
