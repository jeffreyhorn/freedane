# Sprint 7 Annual Refresh Workflow and Governance v1 Contract

Prepared on: 2026-03-20

## Purpose

Define the first stable annual-refresh contract before implementing annual-refresh
runner automation.

This contract locks v1 behavior for:

- annual refresh sequence covering new assessment roll ingest, RETR import,
  context rebuild, scoring refresh, and investigation/report artifact refresh
- required operator checkpoints and sign-off records for completion
- partial-year correction replay/backfill policy
- linkage between annual refresh execution and threshold/ruleset promotion
  governance

## Scope

In scope for v1:

- one deterministic annual refresh sequence that operators can execute and audit
- explicit checkpoint gates with blocking/pass criteria
- one sign-off record schema for annual run completion
- explicit correction-run policy for replay/backfill after annual cutover
- explicit handoff boundary into threshold promotion governance review

Out of scope for v1:

- full auto-approval of threshold or ruleset promotion
- external ticketing integrations (Jira/ServiceNow/etc.)
- automatic rollback of production threshold selections
- multi-jurisdiction annual refresh orchestration in one run

## Design Goals

- Keep annual refresh execution deterministic, repeatable, and auditable.
- Prevent silent cutover to unvalidated annual data.
- Preserve analyst trust by separating data refresh completion from threshold
  promotion approval.
- Make correction-run handling explicit and versioned.

## Baseline Assumptions

- Sprint 7 refresh automation v1 contract is already defined in:
  - `docs/planning/SPRINT_7/REFRESH_AUTOMATION_V1.md`
- Runner stages already used for recurring refresh remain canonical:
  - `ingest_context`
  - `build_context`
  - `score_pipeline`
  - `analysis_artifacts`
  - `investigation_artifacts`
  - `health_summary`
- Required source inputs for annual refresh are available:
  - annual assessment roll extracts (or equivalent upstream parcel assessment feed)
  - RETR export files covering target annual window
- Permit and appeal files remain optional enrichments; missing optional files are
  allowed with explicit skip records.

## Contract Terminology

- `annual refresh run`: one run using `profile_name = annual_refresh`.
- `annual target year`: assessment year being cut over.
- `cutover candidate`: the annual run selected for final operator sign-off.
- `correction run`: replay/backfill run performed after annual cutover because of
  corrected source data.
- `promotion packet`: evidence package used for threshold/ruleset promotion review.

## Annual Refresh Sequence (v1)

Annual refresh uses this ordered sequence:

1. `preflight_validation` (operator checkpoint gate)
2. `ingest_context`
3. `build_context`
4. `score_pipeline`
5. `analysis_artifacts`
6. `investigation_artifacts`
7. `health_summary`
8. `annual_signoff` (operator checkpoint gate)

Implementation note:

- For Day 10 runner implementation, `preflight_validation` and `annual_signoff`
  are orchestration checkpoints, while canonical stage execution still uses the
  six refresh stages defined in `REFRESH_AUTOMATION_V1.md`.

### Stage intent details

#### 1) `preflight_validation`

Required checks before any ingest/build:

- confirm annual target year and run context values are set
- confirm assessment roll source manifest is present
- confirm RETR source manifest is present
- confirm destination artifact root is writable
- confirm no overlapping `annual_refresh` run lock exists

Failure behavior:

- if any preflight check fails, run status is `failed` and no downstream stage
  may execute
- failure must include explicit `error.code` and failing checkpoint identifier

#### 2) `ingest_context`

Required minimum commands:

- `.venv/bin/accessdane ingest-retr --file <retr_export.csv>`
- `.venv/bin/accessdane match-sales`

Optional ingest commands (may run if files provided):

- `.venv/bin/accessdane ingest-permits --file <permits.csv>`
- `.venv/bin/accessdane ingest-appeals --file <appeals.csv>`

Annual requirement:

- RETR ingest is mandatory for annual refresh v1; missing RETR input is a hard
  failure for `annual_refresh` profile.

#### 3) `build_context`

Required command:

- `.venv/bin/accessdane build-parcel-year-facts`

Annual requirement:

- context rebuild must run after annual ingest updates, even if source row
  counts are unchanged from prior run.

#### 4) `score_pipeline`

Required commands:

- `.venv/bin/accessdane sales-ratio-study --version-tag <annual_sales_ratio_tag>`
- `.venv/bin/accessdane build-features --feature-version <annual_feature_version>`
- `.venv/bin/accessdane score-fraud --feature-version <annual_feature_version> --ruleset-version <ruleset_version>`

Annual requirement:

- `annual_feature_version` must differ from prior annual cutover feature version.
- `ruleset_version` may remain unchanged in v1; threshold promotion is a
  separate governance decision.

#### 5) `analysis_artifacts`

Required commands:

- `.venv/bin/accessdane review-queue --top <top> --feature-version <annual_feature_version> --ruleset-version <ruleset_version> --out <path> --csv-out <path>`
- `.venv/bin/accessdane review-feedback --feature-version <annual_feature_version> --ruleset-version <ruleset_version> --out <path> --sql-out <path>`

#### 6) `investigation_artifacts`

Required command:

- `.venv/bin/accessdane investigation-report --top <top> --feature-version <annual_feature_version> --ruleset-version <ruleset_version> --html-out <path> --out <path>`

#### 7) `health_summary`

Required output:

- one run summary payload with stage outcomes, duration, and row/count diagnostics

#### 8) `annual_signoff`

Required operator activity:

- complete all annual checkpoint sign-offs
- record sign-off packet location and approver identity
- designate run as `cutover_candidate = true` only after all checkpoints pass

## Annual Operator Checkpoints (v1)

All checkpoints are required for annual completion.

### Checkpoint matrix

1. `CP-01_SOURCE_MANIFEST`
  - verifies assessment roll + RETR source manifests
  - blocking: yes
2. `CP-02_INGEST_RECONCILIATION`
  - verifies ingest row-count and reject-count reconciliation
  - blocking: yes
3. `CP-03_CONTEXT_REBUILD_VALIDATION`
  - verifies parcel-year fact rebuild completed and basic sanity checks pass
  - blocking: yes
4. `CP-04_SCORING_DISTRIBUTION_REVIEW`
  - verifies score output exists and distribution drift is reviewed
  - blocking: yes
5. `CP-05_INVESTIGATION_ARTIFACT_REVIEW`
  - verifies queue/feedback/report artifacts are produced and readable
  - blocking: yes
6. `CP-06_CUTOVER_AUTHORIZATION`
  - two-person approval that run can be designated as annual cutover candidate
  - blocking: yes

## Sign-Off Record Contract (v1)

Each annual refresh run must produce a sign-off record JSON artifact.

Required path:

- `data/refresh_runs/<run_date>/annual_refresh/<run_id>/governance/annual_signoff.json`

Required top-level keys:

1. `run`
2. `annual_context`
3. `checkpoints`
4. `approvals`
5. `artifacts`
6. `error`

### `run` object

- `run_id`
- `profile_name` (must be `annual_refresh`)
- `status` (`pending_signoff|approved|rejected`)
- `created_at`
- `updated_at`

### `annual_context` object

- `annual_target_year`
- `feature_version`
- `ruleset_version`
- `sales_ratio_version_tag`
- `source_manifest_paths` (array)

### `checkpoints` array

Each checkpoint object must include:

- `checkpoint_id` (for example `CP-03_CONTEXT_REBUILD_VALIDATION`)
- `status` (`pending|passed|failed|waived`)
- `reviewer`
- `reviewed_at` (nullable while `pending`)
- `evidence_paths` (array)
- `notes` (nullable)

Rules:

- `waived` is disallowed for `CP-06_CUTOVER_AUTHORIZATION`.
- annual run cannot be `approved` if any checkpoint is `pending` or `failed`.

### `approvals` array

Required for approval:

- at least two distinct approvers for `approved` status
- each approval object includes:
  - `approver`
  - `role`
  - `decision` (`approve|reject`)
  - `decided_at`
  - `comment` (nullable)

### `artifacts` object

- `refresh_run_payload_path`
- `review_queue_path`
- `review_feedback_path`
- `investigation_report_json_path`
- `investigation_report_html_path`
- `load_monitoring_payload_path` (nullable in v1)

### `error` object

- `null` when status is `approved`
- required when status is `rejected`, with:
  - `code`
  - `message`

## Backfill and Replay Policy (v1)

v1 supports correction replay after annual cutover using explicit run context.

### Allowed correction types

1. `correction_replay_retr`
  - for RETR export corrections
2. `correction_replay_permits`
  - for permit corrections
3. `correction_replay_appeals`
  - for appeal corrections
4. `correction_replay_assessment_roll`
  - for assessment roll corrections

### Replay requirements

- replay runs must use `profile_name = annual_refresh`
- replay run must reference prior cutover run via:
  - `request.parent_run_id`
  - `request.correction_reason_code`
- replay runs must produce a new `run_id`; they do not overwrite prior sign-off
  artifacts
- replay run output must emit a correction summary artifact:
  - `governance/correction_summary.json`

### Correction summary contract

Required keys:

1. `parent_run_id`
2. `correction_reason_code`
3. `corrected_sources`
4. `affected_metrics`
5. `recommended_actions`

### Cutover rules after correction

- prior cutover remains historical record and must not be deleted
- new correction run may become the current cutover candidate only after full
  checkpoint re-approval (`CP-01` through `CP-06`)
- threshold/ruleset promotion decisions must reference latest approved cutover
  or approved correction run evidence

## Governance Linkage: Threshold Promotion

Annual refresh completion does not auto-promote thresholds or rulesets.

Required linkage:

- approved annual sign-off record must be attached to threshold promotion packet
- promotion packet must include review-feedback evidence generated from the
  annual feature/ruleset context
- promotion review must reference:
  - annual sign-off artifact path
  - review-feedback artifact path
  - benchmark/fairness comparison summary (available in later Sprint 7 days)

Detailed governance policy is documented in:

- `docs/planning/SPRINT_7/THRESHOLD_PROMOTION_GOVERNANCE_NOTES.md`

## Required Artifacts (v1)

Per annual run root:

- `health_summary/refresh_run_payload.json`
- `analysis_artifacts/review_queue.json`
- `analysis_artifacts/review_feedback.json`
- `investigation_artifacts/investigation_report.json`
- `investigation_artifacts/investigation_report.html`
- `governance/annual_signoff.json`
- `governance/correction_summary.json` (only for correction replay runs)

## Failure Semantics (v1)

- any failed blocking checkpoint prevents `approved` sign-off state
- annual run may complete stage execution but remain operationally incomplete
  until sign-off is approved
- if sign-off is rejected, status must be `rejected` and include `error` object

## Acceptance Criteria for Day 9 Contract Completion

- annual sequence is explicitly defined through sign-off completion
- operator checkpoints and sign-off schema are explicit
- replay/backfill correction policy is explicit and versioned
- threshold promotion linkage to annual review-feedback evidence is explicit

