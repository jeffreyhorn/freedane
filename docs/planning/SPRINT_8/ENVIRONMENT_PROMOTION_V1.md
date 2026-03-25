# Sprint 8 Environment And Promotion Contract v1

Prepared on: 2026-03-25

## Purpose

Define the first stable contract for production environment boundaries and promotion workflow before Sprint 8 implementation work starts.

This contract locks v1 behavior for:

- environment profile structure and required config keys
- secrets vs non-secrets boundaries
- artifact-root segregation by environment
- run-id naming and traceability rules
- promotion workflow, approvals, rollback, and emergency freeze controls

## Scope

In scope for v1:

- three execution environments: `dev`, `stage`, `prod`
- one-way promotion chain: `dev -> stage -> prod`
- promotion metadata and approval requirements
- explicit rollback and freeze semantics

Out of scope for v1:

- cloud-specific deployment tooling (Terraform/Helm/etc.)
- external secret manager implementation details
- cross-repository artifact replication tooling
- business policy decisions outside technical guardrails

## Design Goals

- Prevent accidental cross-environment data/artifact contamination.
- Make every promotion decision auditable and reproducible.
- Require explicit approvals before production activation.
- Provide deterministic rollback paths with preserved evidence.

## Environment Model

Environment responsibilities:

- `dev`:
  - feature development, local validation, and draft threshold iteration
  - may use synthetic/test fixtures
- `stage`:
  - pre-production verification with production-like controls
  - promotion dress rehearsal and smoke validation
- `prod`:
  - analyst-facing operational environment
  - restricted change surface and strongest approval controls

Required boundary rule:

- each environment must have distinct database and artifact roots.
- no environment may read/write another environment's mutable artifact roots.

## Environment Profile Contract

Each environment profile must expose this key set.

Profile representation contract:

- v1 profiles are key/value configuration maps that use env-var style uppercase keys.
- canonical environment identity key is `ACCESSDANE_ENVIRONMENT`.
- legacy `environment_name` may be accepted as a read-only compatibility alias, but writers must emit `ACCESSDANE_ENVIRONMENT`.

| Key | Required | Secret | Description |
| --- | --- | --- | --- |
| `ACCESSDANE_ENVIRONMENT` | yes | no | One of `dev`, `stage`, `prod`. |
| `DATABASE_URL` | yes | yes | Environment-specific database connection URL. |
| `ACCESSDANE_BASE_URL` | yes | no | Source endpoint root. |
| `ACCESSDANE_RAW_DIR` | yes | no | Environment-local root for raw scraper/html capture artifacts. |
| `ACCESSDANE_USER_AGENT` | yes | no | Traceable user-agent string. |
| `ACCESSDANE_TIMEOUT` | yes | no | HTTP timeout setting for fetch commands. |
| `ACCESSDANE_RETRIES` | yes | no | Retry count for fetch operations. |
| `ACCESSDANE_BACKOFF` | yes | no | Retry backoff seconds. |
| `ACCESSDANE_REFRESH_PROFILE` | yes | no | Default refresh runner profile. |
| `ACCESSDANE_FEATURE_VERSION` | yes | no | Default active feature version selector. |
| `ACCESSDANE_RULESET_VERSION` | yes | no | Default active ruleset selector. |
| `ACCESSDANE_SALES_RATIO_BASE` | yes | no | Default sales-ratio base token. |
| `ACCESSDANE_REFRESH_TOP` | yes | no | Default queue/report `top` size. |
| `ACCESSDANE_ARTIFACT_BASE_DIR` | yes | no | Refresh artifact root for that environment. |
| `ACCESSDANE_REFRESH_LOG_DIR` | yes | no | Scheduler log root (must be under refresh artifact root). |
| `ACCESSDANE_BENCHMARK_BASE_DIR` | yes | no | Benchmark artifact root for that environment. |
| `ALERT_ROUTE_GROUP` | yes | no | Alert routing group key for the environment. |
| `PROMOTION_APPROVER_GROUP` | yes | no | Default approver group identifier. |
| `PROMOTION_FREEZE_FILE` | yes | no | Path to freeze-control file for the environment. |

Profile validation rules:

- path-valued keys must resolve inside approved environment roots.
- `ACCESSDANE_RAW_DIR`, `ACCESSDANE_ARTIFACT_BASE_DIR`, `ACCESSDANE_REFRESH_LOG_DIR`, and `ACCESSDANE_BENCHMARK_BASE_DIR` must be environment-local and must not point at another environment's root.
- `ACCESSDANE_ENVIRONMENT` must match profile file/selector identity.
- prod profile must not allow fallback defaults that point to `dev`/`stage` paths.

## Secrets Boundary Rules

Classify and handle secrets as follows:

- secret keys:
  - `DATABASE_URL`
  - future alert transport credentials (`SMTP_*`, `SLACK_*`, `PAGERDUTY_*`)
- non-secret keys:
  - version selectors, profile names, artifact roots, queue sizing, routing group names

Required controls:

- secrets must come from runtime secret injection (env manager/secret store), never committed files.
- generated payloads and logs must not print secret values.
- promotion manifests must include redacted config snapshots only.

## Artifact Root Contract

Recommended environment-isolated roots:

- `dev`:
  - raw: `data/environments/dev/raw`
  - refresh: `data/environments/dev/refresh_runs`
  - benchmark: `data/environments/dev/benchmark_packs`
- `stage`:
  - raw: `data/environments/stage/raw`
  - refresh: `data/environments/stage/refresh_runs`
  - benchmark: `data/environments/stage/benchmark_packs`
- `prod`:
  - raw: `data/environments/prod/raw`
  - refresh: `data/environments/prod/refresh_runs`
  - benchmark: `data/environments/prod/benchmark_packs`

Hard requirements:

- no shared mutable run roots across environments.
- latest pointers (`latest/...`) are environment-local.
- lock files (`locks/<profile>.lock`) are environment-local.

Compatibility note with Sprint 7 docs:

- Sprint 7 operations docs reference shared roots (`data/refresh_runs`, `data/benchmark_packs`) for a single-environment setup.
- Sprint 8 supersedes that layout for multi-environment operation: canonical mutable roots are environment-isolated as defined above.
- if legacy shared roots are used temporarily, they are valid only for one environment at a time and must not be shared across `dev`, `stage`, and `prod`.

## Run-Id Naming Contract

Refresh run id format:

- `<run_date>_<profile_name>_<feature_version>_<ruleset_version>_<HHMMSS>`

Benchmark run id format:

- `<run_date>_<profile_name>_<feature_version>_<ruleset_version>_<HHMMSS>`

Run-id rules:

- use UTC timestamps.
- `<run_date>` must be UTC calendar date in `YYYYMMDD`.
- `<HHMMSS>` must be UTC wall-clock time in `HHMMSS`.
- allow only `[A-Za-z0-9._-]`.
- must be safe as a single path component:
  - `run_id` must not be `.` or `..`
  - `run_id` must not contain traversal substrings such as `..`, `./`, `../`, `/.`, or `\\`
- include enough context to map each run to active feature/ruleset selectors.

Promotion traceability requirement:

- promotion records must reference source `run_id` and target `run_id` pairs.

## Promotion Unit And Metadata Contract

Promotion unit:

- selected config + selector state:
  - `feature_version`
  - `ruleset_version`
  - threshold/routing overrides (when present)
- required evidence references:
  - refresh run payload
  - load-monitor output
  - parser-drift output
  - benchmark pack output

Required promotion manifest fields:

- `promotion_id` (unique, immutable)
- `source_environment`
- `target_environment`
- `requested_by`
- `requested_at_utc`
- `source_run_id`
- `target_run_id` (nullable before activation; required after activation starts)
- `feature_version`
- `ruleset_version`
- `evidence_artifacts[]`
- `approval_state`
- `approvals[]`
- `activation_state`
- `activation_started_at_utc` (nullable until `activation_state` becomes `in_progress`)
- `activated_by` (nullable until `activation_state` reaches terminal state)
- `activated_at_utc` (nullable until `activation_state` reaches terminal state)
- `rollback_reference` (previous active selectors in target)

`approvals[]` record contract:

- each approval record must include:
  - `approved_by`
  - `approved_at_utc`
  - `approver_role`
- allowed `approver_role` enum values:
  - `engineering_owner`
  - `analyst_owner`
  - `release_operator`
- approval records are append-only; edits require manifest invalidation and re-approval.

## Manifest State Enums And Transitions

`approval_state` allowed values:

- `pending`
- `approved`
- `expired`
- `invalidated`
- `rejected`

`activation_state` allowed values:

- `not_started`
- `in_progress`
- `succeeded`
- `failed`
- `rolled_back`
- `aborted`

Required transition rules:

- initial manifest state: `approval_state = pending`, `activation_state = not_started`
- activation may start only from `approval_state = approved`
- `activation_state` may transition:
  - `not_started -> in_progress`
  - `in_progress -> succeeded|failed|aborted`
  - `succeeded -> rolled_back`
- when transitioning to `in_progress`, `activation_started_at_utc` must be set and immutable for that activation attempt.
- when transitioning from `in_progress` to a terminal state (`succeeded|failed|aborted`), `activated_by` and `activated_at_utc` must be set.
- manifest edits after any recorded approval force:
  - `approval_state = invalidated`
  - `activation_state` remains unchanged until explicit re-approval and activation action
- `approval_state` expiry semantics:
  - `approved -> expired` when activation has not started and required approvals are no longer fully unexpired at evaluation time.
  - `expired -> approved` only after fresh approvals satisfy role/count requirements.

## Promotion Workflow v1

1. Prepare candidate in source environment.
   - collect required evidence artifacts from source run roots.
   - generate promotion manifest with `approval_state = pending`.
2. Validate candidate for target environment.
   - schema/version compatibility checks.
   - required evidence presence and freshness checks.
3. Approve candidate.
   - approvers recorded in manifest.
   - approval count and role rules enforced by source/target pair.
4. Activate in target environment.
   - write active selector set atomically.
   - persist activation record and pointer to rollback reference.
5. Post-activation verification.
   - run target smoke sequence (`refresh`, `load-monitor`, `benchmark-pack` minimal checks).
   - mark manifest `activation_state = succeeded|failed`.

## Approval Controls

Approval policy matrix:

| Promotion | Required approvals | Additional rules |
| --- | --- | --- |
| `dev -> stage` | 1 | approver role must be one of `engineering_owner`, `analyst_owner`, `release_operator`; approver cannot equal requester |
| `stage -> prod` | 2 | one `analyst_owner` + one `engineering_owner`; neither can equal requester |

Required approval behavior:

- no self-approval.
- each approval record must include `approved_at_utc`.
- each approval expires 24h after its own `approved_at_utc`.
- activation is allowed only when, at `activation_started_at_utc`, all required approvals are present and unexpired.
- manifest edits after any approval invalidate all recorded approvals; fresh approvals must be collected against the edited manifest.

## Rollback Semantics

Rollback trigger conditions:

- post-activation smoke failure
- critical operational alert within first validation window
- explicit operator rollback request with incident reference

Rollback behavior:

- restore previous target active selectors from `rollback_reference`.
- do not delete failed promotion artifacts.
- create rollback record with:
  - `rolled_back_by`
  - `rolled_back_at_utc`
  - `rollback_reason`
  - linked incident/ticket id

## Emergency Freeze Semantics

Freeze levels:

- `advisory`:
  - promotion allowed with explicit override note.
- `hard`:
  - all promotions to frozen environment blocked.
  - only break-glass activation allowed.

Freeze file contract (`PROMOTION_FREEZE_FILE`):

- `state`: `none|advisory|hard`
- `reason`
- `owner`
- `set_at_utc`
- `expires_at_utc` (optional)

Break-glass rules for `hard` freeze:

- requires two explicit override approvers.
- requires incident id.
- must emit `break_glass_used = true` in promotion manifest.

## Audit Artifacts

Recommended environment-local promotion registry root:

- `data/environments/<environment_name>/promotion_registry/`

Required saved artifacts per promotion:

- `manifest.json`
- `approval_log.json`
- `activation_log.json`
- optional `rollback_log.json`

Retention requirement:

- keep promotion registry records for at least 400 days.

## Day 3 Implementation Gates

Day 3 implementation is considered complete when:

- environment profile loader validates the full key contract above.
- promotion command path enforces source/target matrix and approval counts.
- freeze file checks are enforced before activation.
- rollback reference is captured on every successful activation.
