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

| Key | Required | Secret | Description |
| --- | --- | --- | --- |
| `environment_name` | yes | no | One of `dev`, `stage`, `prod`. |
| `DATABASE_URL` | yes | yes | Environment-specific database connection URL. |
| `ACCESSDANE_BASE_URL` | yes | no | Source endpoint root. |
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
- `environment_name` must match profile file/selector identity.
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
  - refresh: `data/environments/dev/refresh_runs`
  - benchmark: `data/environments/dev/benchmark_packs`
- `stage`:
  - refresh: `data/environments/stage/refresh_runs`
  - benchmark: `data/environments/stage/benchmark_packs`
- `prod`:
  - refresh: `data/environments/prod/refresh_runs`
  - benchmark: `data/environments/prod/benchmark_packs`

Hard requirements:

- no shared mutable run roots across environments.
- latest pointers (`latest/...`) are environment-local.
- lock files (`locks/<profile>.lock`) are environment-local.

## Run-Id Naming Contract

Refresh run id format:

- `<run_date>_<profile_name>_<feature_version>_<ruleset_version>_<HHMMSS>`

Benchmark run id format:

- `<run_date>_<profile_name>_<feature_version>_<ruleset_version>_<HHMMSS>`

Run-id rules:

- use UTC timestamps.
- allow only `[A-Za-z0-9._-]`.
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
- `feature_version`
- `ruleset_version`
- `evidence_artifacts[]`
- `approval_state`
- `approved_by[]`
- `approved_at_utc[]`
- `activation_state`
- `activated_by`
- `activated_at_utc`
- `rollback_reference` (previous active selectors in target)

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
| `dev -> stage` | 1 | approver cannot equal requester |
| `stage -> prod` | 2 | one data/analyst owner + one engineering owner; neither can equal requester |

Required approval behavior:

- no self-approval.
- approvals expire after 24h if activation has not occurred.
- manifest edits after approval invalidate prior approvals.

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
