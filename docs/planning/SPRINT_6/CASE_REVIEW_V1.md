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
- `created_at` (timezone-aware timestamp, UTC; e.g., `DateTime(timezone=True)`; not null, default now)
- `updated_at` (timezone-aware timestamp, UTC; e.g., `DateTime(timezone=True)`; not null, default now, auto-update)
- `reviewed_at` (timezone-aware timestamp, UTC; e.g., `DateTime(timezone=True)`; nullable)
- `closed_at` (timezone-aware timestamp, UTC; e.g., `DateTime(timezone=True)`; nullable)

Required uniqueness and integrity:

- unique context key: (`score_id`, `feature_version`, `ruleset_version`)
- `score_id` row must match (`parcel_id`, `year`, `feature_version`, `ruleset_version`)
- `score_run_id` must match `fraud_scores.run_id` for referenced `score_id`

Required indexes (v1):

- (`status`, `updated_at`)
- (`disposition`, `reviewed_at`)
- (`assigned_reviewer`, `status`, `updated_at`)
- (`parcel_id`, `year`)

Index declarations remain column-based for portability; query-level DESC ordering is defined in the list contract.

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
- entering `pending` or `in_review` requires `disposition = null`
- reopening (`resolved -> in_review` or `closed -> in_review`) explicitly clears `disposition`
- reopening (`closed -> in_review`) clears `closed_at` and preserves historical `reviewed_at`

## Evidence Links Contract

`evidence_links_json` is an array of objects with shape:

- `kind` (required enum): `dossier`, `queue_row`, `reason_code`, `external_doc`, `field_visit`
- `ref` (required string)
- `label` (optional string)

Requirements:

- create with initial status `resolved` or `closed` requires at least 1 evidence link
- create with initial status `pending` or `in_review` may omit evidence links
- update may omit evidence links (existing links remain unchanged)
- update may replace evidence links only when explicitly requested
- any record ending in `resolved` or `closed` must have at least 1 evidence link (incoming and/or existing)

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

- `--disposition <...>` (required when `--status resolved` or `--status closed`)
- `--reviewer <name>`
- `--assigned-reviewer <name>`
- `--note <text>`
- `--evidence-link <kind=...,ref=...,label=...>` (repeatable; required when `--status resolved` or `--status closed`)

Behavior:

- create is idempotent on (`score_id`, `feature_version`, `ruleset_version`)
- idempotency comparison uses a canonicalized create payload that includes only client-controlled JSON payload fields:
  - `status`, `disposition`, `reviewer`, `assigned_reviewer`, `note`, `evidence_links`
- request fields persist to matching DB columns; specifically, `evidence_links` is serialized into `evidence_links_json`
- string fields are whitespace-trimmed before comparison
- `evidence_links` is normalized by `(kind, ref, label)` sorting and exact-item de-duplication, then compared via persisted `evidence_links_json`
- server-managed fields (`id`, `created_at`, `updated_at`, `reviewed_at`, `closed_at`) are excluded
- score-derived linkage fields (`parcel_id`, `year`, `score_run_id`) are validated from `score_id` context, not caller-supplied
- if same context exists and canonicalized fields are identical, return existing row with `created=false`
- if same context exists and canonicalized fields differ, fail with `duplicate_case_review`
- validation: if requested initial status is `resolved` or `closed` and `--disposition` is missing, fail with `invalid_disposition_for_status`
- validation: if requested initial status is `resolved` or `closed` and no evidence links are present, fail with `evidence_link_required`

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
- updates that result in `resolved`/`closed` must have at least one evidence link after applying the patch
- if an update would leave a `resolved`/`closed` record with no evidence links (including explicit clearing), fail with `evidence_link_required` before write

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
6. `diagnostics`
7. `error`

Presence rules by `run.status`:

- when `run.status = succeeded`:
  - required: `run`, `request`, `diagnostics`, `error`
  - `error` must be present and `null`
  - create/update: `review` required, `summary`/`reviews` omitted
  - list: `summary` and `reviews` required, `review` omitted
- when `run.status = failed`:
  - required: `run`, `request`, `error`
  - `diagnostics` optional
  - `review`, `reviews`, `summary` must be omitted

`request` schema (echoed, command-specific):

- create:
  - `score_id`, `status`, `disposition`, `reviewer`, `assigned_reviewer`, `note`
  - `feature_version`, `ruleset_version`
  - `evidence_links` (canonicalized array after normalization)
- update:
  - `id`
  - `patch` object containing only supplied fields (`status`, `disposition`, `reviewer`, `assigned_reviewer`, `note`, `evidence_links`)
- list:
  - `statuses`, `dispositions`, `years` (always arrays; sorted unique; `[]` means unscoped/no filter)
  - `reviewer`, `assigned_reviewer`, `parcel_id`, `feature_version`, `ruleset_version`
  - `limit`, `offset`

`request` normalization rules:

- trim surrounding whitespace for string fields (`reviewer`, `assigned_reviewer`, `note`, evidence `kind/ref/label`) before evidence-link normalization/deduplication
- deduplicate repeatable filters and sort lexicographically (for enums/strings) or ascending (for years)
- for list commands, always emit `statuses`, `dispositions`, and `years` in `request`; omitted/empty input normalizes to `[]` (unscoped), while non-empty arrays apply restrictive filtering
- normalize evidence links as canonical tuples `(kind, ref, label)` with `kind`/`ref` required and non-empty after trimming, and `label` optional where omitted or trimmed-empty values normalize to `null`; perform exact duplicate removal and deterministic ordering on these normalized tuples

`run` fields:

- `run_id: null`
- `run_persisted: false`
- `run_type: case_review`
- `version_tag: case_review_v1`
- `status: succeeded|failed`

`diagnostics` (success or failure when available):

- `warnings` (array; may be empty)
- `normalization` (object; optional canonicalization details such as normalized evidence-link count)

`error` fields when failed:

- `code`
- `message`

## Validation And Failure Codes

Expected JSON failure codes (`exit 1`):

- `case_review_not_found`: requested case-review id does not exist for an operation that requires it (for example, update/read by id).
- `duplicate_case_review`: create encountered existing context (`score_id`, `feature_version`, `ruleset_version`) with non-identical canonicalized client-controlled fields.
- `score_not_found`: referenced `score_id` does not exist.
- `score_context_mismatch`: supplied context linkage is inconsistent with the referenced score record (parcel/year/version/run context mismatch).
- `invalid_transition`: requested status change is not allowed by the transition matrix.
- `invalid_disposition_for_status`: disposition is missing/invalid for the resulting status per lifecycle rules.
- `evidence_link_required`: resulting state requires at least one evidence link, but none remain after validation/patch application.
- `invalid_evidence_link`: one or more evidence-link entries fail required shape validation (`kind`/`ref` required after trimming).
- `source_query_error`: source read failed for reasons other than not-found/validation conditions.

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
