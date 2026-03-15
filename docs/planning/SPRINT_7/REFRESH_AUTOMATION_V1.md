# Sprint 7 Scheduled Refresh Automation v1 Contract

Prepared on: 2026-03-15

## Purpose

Define the first stable contract for recurring refresh automation delivered in Sprint 7, where scheduled runs refresh the Sprint 6 investigation workflow and artifacts, before implementing scheduler and runner code.

This contract locks v1 behavior for:

- scheduled job matrix and stage ordering
- version/tag and artifact path conventions
- failure semantics, retry policy, and idempotency expectations
- job-level observability payload schema

## Scope

In scope for v1:

- one refresh workflow contract that can run on a schedule
- deterministic stage ordering for recurring operations
- run-scoped artifact naming and retention-friendly paths
- structured status/diagnostic payloads for each stage

Out of scope for v1:

- distributed task queue infrastructure
- real-time event streaming
- external paging integrations (email/Slack/PagerDuty)
- automated threshold/ruleset promotion writes

## Design Goals

- Keep recurring refresh behavior deterministic and auditable.
- Ensure partial failures are explicit and recoverable.
- Preserve rerun safety through idempotent stage semantics.
- Make stage-level status and timing machine-readable.

## Baseline Assumptions

- Sprint 6 commands exist and are operational:
  - `init-db`
  - `review-queue`
  - `parcel-dossier`
  - `case-review ...`
  - `review-feedback`
  - `investigation-report`
- Upstream Sprint 5 scoring pipeline commands exist and are operational:
  - `sales-ratio-study`
  - `build-features`
  - `score-fraud`
- Source ingest commands remain available for refresh scope changes:
  - `ingest-retr`, `match-sales`
  - `ingest-permits`, `ingest-appeals`, `build-parcel-year-facts`

## Scheduled Job Matrix (v1)

The recurring refresh workflow uses ordered stages:

1. `ingest_context`
2. `build_context`
3. `score_pipeline`
4. `analysis_artifacts`
5. `investigation_artifacts`
6. `health_summary`

## Stage Definitions

### Stage: `ingest_context`

Required commands (enabled by input availability):

- `.venv/bin/accessdane ingest-retr --file <retr_export.csv>`
- `.venv/bin/accessdane match-sales`
- `.venv/bin/accessdane ingest-permits --file <permits.csv>`
- `.venv/bin/accessdane ingest-appeals --file <appeals.csv>`

Behavior:

- Stage may skip permit/appeal/retr substeps when source files are not provided.
- Each substep records `status = succeeded|failed|skipped`.

### Stage: `build_context`

Required command:

- `.venv/bin/accessdane build-parcel-year-facts`

Behavior:

- Must run after any ingest changes.
- Must fail hard when context build fails.

### Stage: `score_pipeline`

Required commands:

- `.venv/bin/accessdane sales-ratio-study --version-tag <sales_ratio_version_tag>`
- `.venv/bin/accessdane build-features --feature-version <feature_version>`
- `.venv/bin/accessdane score-fraud --feature-version <feature_version> --ruleset-version <ruleset_version>`

Behavior:

- Stage must run in fixed order.
- `score-fraud` runs only after `build-features` succeeds.

### Stage: `analysis_artifacts`

Required commands:

- `.venv/bin/accessdane review-queue --top <n> --feature-version <feature_version> --ruleset-version <ruleset_version> --out <path> --csv-out <path>`
- `.venv/bin/accessdane review-feedback --feature-version <feature_version> --ruleset-version <ruleset_version> --out <path> --sql-out <path>`

Behavior:

- Stage uses run-context `feature_version` and `ruleset_version` values and passes them explicitly to stage commands.
- Stage succeeds with zero reviewed cases; that is not treated as a failure.

### Stage: `investigation_artifacts`

Required command:

- `.venv/bin/accessdane investigation-report --top <n> --feature-version <feature_version> --ruleset-version <ruleset_version> --html-out <path> --out <path>`

Behavior:

- Stage fails if report command fails.
- HTML and JSON artifacts are both required outputs.

### Stage: `health_summary`

Required actions:

- Build one run summary payload from prior stage outputs.
- Emit stage duration totals, status rollup, and high-level counts.

Behavior:

- Runs even when prior stage fails to ensure terminal status is persisted.

## Scheduling Profile Contract

v1 scheduling profiles:

- `daily_refresh`:
  - full stage chain
  - intended for nightly recurring operation
- `analysis_only`:
  - `analysis_artifacts`, `investigation_artifacts`, `health_summary`
  - used when data/scoring are already current
- `annual_refresh`:
  - full stage chain with annual source files and explicit checkpoint metadata

Profile must be recorded in the run payload as `profile_name`.

## Version/Tag And Artifact Path Conventions

Required run-scoped context:

- `run_date` in `YYYYMMDD`
- `feature_version`
- `ruleset_version`
- `profile_name`

Required artifact root:

- `data/refresh_runs/<run_date>/<profile_name>/`

Required stage artifact path pattern:

- `data/refresh_runs/<run_date>/<profile_name>/<stage_id>/<artifact_name>`

Required latest pointers:

- `data/refresh_runs/latest/<profile_name>/...` as copies or symlinks

Version/tag conventions:

- `sales_ratio_study.version_tag`: `<profile_name>_<run_date>_<sales_ratio_base>`
- In stage command examples, `<sales_ratio_version_tag>` means the full `sales_ratio_study.version_tag` value above.
- `build_features.feature_version`: operator-selected version (for example `feature_v1`) unless explicitly promoting
- `score_fraud.ruleset_version`: operator-selected ruleset (for example `scoring_rules_v1`)

## Failure Semantics And Retry Rules

Stage status values:

- `succeeded`
- `failed`
- `skipped`
- `blocked`

Rules:

- If a stage fails, downstream dependent stages are marked `blocked`.
- `health_summary` is never blocked.
- Soft-skips are allowed only for ingest substeps with missing optional source files.

Retry policy:

- v1 runner supports manual rerun with same run context.
- Runner must support per-stage rerun from first failed stage boundary.
- Retried stage must overwrite only its own stage artifacts in the same run scope.

## Idempotency Expectations

Idempotency applies to same:

- source inputs
- `feature_version`
- `ruleset_version`
- profile and stage options

Expected idempotent outcomes:

- stage statuses stable for unchanged inputs
- stage artifact schema stable
- score/report outputs deterministic for unchanged source data

Non-idempotent allowances:

- timestamps (`started_at`, `finished_at`, duration fields)
- run identifiers in metadata

## Observability Payload Contract (v1)

Top-level keys (canonical order):

1. `run`
2. `request`
3. `summary`
4. `stages`
5. `artifacts`
6. `diagnostics`
7. `error`

Presence rules:

- `run`, `request`, `summary`, `stages`, `artifacts`, `diagnostics`, and `error` are always present.
- When `run.status = succeeded`, `error` must be `null`.
- When `run.status = failed`, `error` must be an object with `code`, `message`, and `failed_stage_id`.
- On failed runs, non-error sections still appear and reflect partial execution (including `failed`, `blocked`, and `skipped` stage statuses where applicable).

`run` fields:

- `run_type` (`refresh_automation`)
- `version_tag` (`refresh_automation_v1`)
- `profile_name`
- `status` (`succeeded|failed`)
- `started_at`, `finished_at`

`request` fields:

- `profile_name`
- `run_date`
- `feature_version`
- `ruleset_version`
- `top_n`
- `source_files` (object with retr/permits/appeals paths or null)

`summary` fields:

- `stage_count`
- `stage_succeeded_count`
- `stage_failed_count`
- `stage_blocked_count`
- `stage_skipped_count`
- `duration_seconds_total`

`stages[]` fields:

- `stage_id`
- `status`
- `started_at`
- `finished_at`
- `duration_seconds`
- `attempt`
- `command_results[]` with:
  - `command_id`
  - `status`
  - `exit_code`
  - `artifact_paths`
  - `error_code` (nullable)

`error` fields when failed:

- `code`
- `message`
- `failed_stage_id`

`artifacts` fields:

- `root_path` (run-scoped artifact root)
- `latest_pointer_path` (latest alias root for `profile_name`)
- `stage_artifacts` (object keyed by `stage_id`, each value is an array of artifact paths)

`diagnostics` fields:

- `warnings` (array of warning strings; empty when none)
- `skip_reasons` (array of objects with `stage_id`, `command_id`, `reason`)
- `retry` (object with `attempt_count`, `retried_from_stage_id` nullable)

## Validation Fixtures (Day 3+ Test Targets)

### Fixture A: Happy-path daily refresh

- All stages succeed.
- Validate deterministic stage ordering and required artifact paths.

### Fixture B: Optional ingest skip

- Missing permit file input.
- Validate permit ingest substep reports `skipped` and downstream stages still run.

### Fixture C: Hard failure in `score_pipeline`

- Simulate scoring failure.
- Validate downstream stages are `blocked` and top-level run status is `failed`.

### Fixture D: Stage retry from failure boundary

- First run fails in one stage; second run retries from failure stage.
- Validate overwritten artifacts only within retried stage scope.

### Fixture E: Idempotent replay

- Repeat run with identical inputs.
- Validate stage status and artifact schema stability.

## Non-Goals For Day 2 Contract

- Implementing scheduler runtime integration.
- Implementing drift/load diagnostics engines.
- Implementing annual refresh orchestration.
