# Sprint 6 Case Review Workflow v1 Contract

Prepared on: 2026-03-12

## Purpose

Define the first stable contract for persisted analyst review outcomes before Day 9 implementation.

This contract locks v1 behavior for:

- `case_reviews` data model fields and constraints
- workflow statuses, dispositions, and valid state transitions
- idempotent create/update semantics
- review ownership and audit metadata
- linkage back to scored parcel context (`fraud_scores` + version selectors)
- validation rules and failure codes

## Scope

In scope for v1:

- one persisted record per scored parcel context under review
- CLI create/update/list flows for analyst operations
- deterministic list filtering and ordering for queue/report overlays
- explicit audit timestamps for review lifecycle events

Out of scope for v1:

- multi-step approval workflows
- threaded comments or attachment blob storage
- UI-level assignment balancing
- automatic threshold tuning (planned for later Sprint 6 days)

## Design Goals

- Make review outcomes durable and queryable without custom SQL.
- Keep each review traceable to one `fraud_scores` row and scoring context.
- Support reliable retries and idempotent CLI behavior.
- Prevent ambiguous lifecycle transitions.
- Keep payloads deterministic for downstream queue/dossier overlays.

## Baseline Assumptions

- Sprint 5/6 scored data exists: `scoring_runs`, `fraud_scores`, `fraud_flags`, `parcel_year_facts`.
- `review-queue` output rows include `score_id`, `parcel_id`, `year`, `feature_version`, `ruleset_version`.
- Dossier/report surfaces consume JSON payloads with explicit `run.status` and `error` fields.

## Data Model Contract

## Table: `case_reviews`

Required columns (v1):

- `id` (PK, integer, autoincrement)
- `parcel_id` (FK -> `parcels.id`, not null)
- `year` (int, not null)
- `score_id` (FK -> `fraud_scores.id`, not null)
- `score_run_id` (FK -> `scoring_runs.id`, not null)
- `feature_version` (string, not null)
- `ruleset_version` (string, not null)
- `status` (string enum, not null)
- `disposition` (string enum, nullable)
- `reviewer` (string, nullable)
- `assigned_reviewer` (string, nullable)
- `note` (text, nullable)
- `evidence_links_json` (JSON array, not null; default `[]`)
- `created_at` (timestamptz, not null, default now)
- `updated_at` (timestamptz, not null, default now, auto-update)
- `reviewed_at` (timestamptz, nullable)
- `closed_at` (timestamptz, nullable)

Required uniqueness and integrity:

- unique context key: (`score_id`, `feature_version`, `ruleset_version`)
- `score_id` row must match (`parcel_id`, `year`, `feature_version`, `ruleset_version`)
- `score_run_id` must match `fraud_scores.run_id` for referenced `score_id`

Required indexes (v1):

- (`status`, `updated_at DESC`)
- (`disposition`, `reviewed_at DESC`)
- (`assigned_reviewer`, `status`, `updated_at DESC`)
- (`parcel_id`, `year`)

## Lifecycle Contract

## `status` enum (v1)

- `pending`
- `in_review`
- `resolved`
- `closed`

## `disposition` enum (v1)

- `confirmed_issue`
- `false_positive`
- `inconclusive`
- `needs_field_review`
- `duplicate_case`

Rules:

- `disposition` must be null for `pending` and `in_review`.
- `disposition` is required for `resolved` and `closed`.
- `reviewed_at` is required when status enters `resolved`.
- `closed_at` is required when status enters `closed`.

## Valid Transitions

Allowed:

- `pending -> in_review`
- `pending -> resolved`
- `in_review -> pending`
- `in_review -> resolved`
- `resolved -> in_review` (reopen)
- `resolved -> closed`
- `closed -> in_review` (explicit reopen)

Disallowed:

- any transition not listed above

Transition behavior:

- entering `resolved` sets `reviewed_at` if not already set
- entering `closed` sets `closed_at` and keeps `reviewed_at`
- reopening (`closed -> in_review`) clears `closed_at` and preserves historical `reviewed_at`

## Evidence Links Contract

`evidence_links_json` is an array of objects with shape:

- `kind` (required enum): `dossier`, `queue_row`, `reason_code`, `external_doc`, `field_visit`
- `ref` (required string)
- `label` (optional string)

Requirements:

- create requires at least 1 evidence link
- update may omit evidence links (existing links remain unchanged)
- update may replace evidence links only when explicitly requested
- `resolved`/`closed` records must have at least 1 evidence link

## Ownership Metadata Contract

- `assigned_reviewer`: current queue owner (optional)
- `reviewer`: person who performed the latest substantive review update (optional for `pending`, expected for `in_review`/`resolved`/`closed`)

Guidance:

- assignment changes should not require status changes
- reviewer changes during substantive updates should refresh `updated_at`

## CLI Workflow Contract

## Command family

```bash
accessdane case-review create ...
accessdane case-review update ...
accessdane case-review list ...
```

## `case-review create`

Required inputs:

- `--score-id <id>`
- `--status <pending|in_review|resolved|closed>` (default `pending`)
- `--feature-version <value>` (default `feature_v1`)
- `--ruleset-version <value>` (default `scoring_rules_v1`)

Optional inputs:

- `--disposition <...>`
- `--reviewer <name>`
- `--assigned-reviewer <name>`
- `--note <text>`
- `--evidence-link <kind=...,ref=...,label=...>` (repeatable)

Behavior:

- create is idempotent on (`score_id`, `feature_version`, `ruleset_version`)
- if same context exists and supplied fields are identical, return existing row with `created=false`
- if same context exists and supplied fields conflict, fail with `duplicate_case_review`

## `case-review update`

Required inputs:

- `--id <case_review_id>`

Optional patch inputs:

- `--status ...`
- `--disposition ...`
- `--reviewer ...`
- `--assigned-reviewer ...`
- `--note ...`
- `--set-evidence-link ...` (replace full list)

Behavior:

- update is idempotent for same input patch
- no-op patch returns success with `updated=false`
- invalid transition/disposition combinations fail before write

## `case-review list`

Optional filters:

- `--status ...` (repeatable)
- `--disposition ...` (repeatable)
- `--reviewer ...`
- `--assigned-reviewer ...`
- `--parcel-id ...`
- `--year ...` (repeatable)
- `--feature-version ...`
- `--ruleset-version ...`
- `--limit <n>` (default 100, max 1000)
- `--offset <n>` (default 0)

Deterministic order:

1. `updated_at DESC`
2. `id DESC`

## Payload Contract

Top-level key order (success):

1. `run`
2. `request`
3. `summary` (list only)
4. `review` (create/update only)
5. `reviews` (list only)
6. `error`

`run` fields:

- `run_id: null`
- `run_persisted: false`
- `run_type: case_review`
- `version_tag: case_review_v1`
- `status: succeeded|failed`

`error` fields when failed:

- `code`
- `message`

## Validation And Failure Codes

Expected JSON failure codes (`exit 1`):

- `case_review_not_found`
- `duplicate_case_review`
- `score_not_found`
- `score_context_mismatch`
- `invalid_transition`
- `invalid_disposition_for_status`
- `evidence_link_required`
- `invalid_evidence_link`
- `source_query_error`

Typer/Click parse/option failures remain `exit 2` with no JSON payload.

## Day 9 Implementation Notes

Day 9 should implement this contract directly with migration + CLI + tests.

Minimum Day 9 test coverage implied by this contract:

- create idempotency (new/create replay/conflict)
- transition matrix validation
- disposition/status validation rules
- evidence-link requirements
- deterministic list order and filters
- score linkage integrity checks

## Deferred Decisions (Post-v1)

- additional dispositions for legal routing
- structured multi-note history table instead of single `note`
- SLA timers and escalation metadata
- reviewer group assignment
