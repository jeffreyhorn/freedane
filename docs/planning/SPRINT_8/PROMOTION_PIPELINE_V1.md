# Sprint 8 Promotion Pipeline Contract v1

Prepared on: 2026-03-30

## Purpose

Define the first stable pipeline-enforced contract for threshold/ruleset promotion
before Sprint 8 Day 12 implementation begins.

This contract locks v1 behavior for:

- promotion request bundle structure and required metadata
- pipeline gate stages and blocking behavior
- evidence completeness/provenance requirements for promotion approval
- approval, freeze, and environment-boundary policy checks
- gate-result audit artifacts and failure-code taxonomy

## Scope

In scope for v1:

- pipeline checks for `dev -> stage` and `stage -> prod` promotion requests
- validation of promotion manifest shape and safety constraints
- evidence artifact existence, freshness, and provenance checks
- approval policy and freeze-policy enforcement before activation
- deterministic gate output payload for CI and operator workflows

Out of scope for v1:

- cloud-vendor-specific CI runner mechanics
- automatic post-approval activation (activation remains explicit)
- policy exceptions outside explicit break-glass/freeze controls
- retrospective business decisioning beyond required technical evidence

## Design Goals

- Convert promotion governance from human-only notes into enforceable checks.
- Block unsafe promotions before any state mutation in target environment.
- Keep promotion decisions reproducible from immutable artifacts and metadata.
- Preserve existing environment-separation and freeze semantics.

## Baseline Assumptions

- Environment and promotion boundaries from Sprint 8 Day 2 remain authoritative:
  - [environment and promotion contract](ENVIRONMENT_PROMOTION_V1.md)
- Existing activation logic and checks remain authoritative for activation-time behavior:
  - `accessdane promotion-activate`
- [`src/accessdane_audit/promotion.py`](../../../src/accessdane_audit/promotion.py)
- Prior governance notes remain the human policy baseline this contract formalizes:
  - [Sprint 7 threshold promotion governance notes](../SPRINT_7/THRESHOLD_PROMOTION_GOVERNANCE_NOTES.md)
- Operational evidence producers remain authoritative:
  - refresh runner, parser drift, load monitoring, benchmark pack, observability rollups

## Promotion Pipeline Model (v1)

Pipeline stages are evaluated in order; any failed stage blocks promotion.

1. `request_normalization`
   - normalize request paths and identifiers
   - reject unsafe path components and malformed timestamps
2. `manifest_contract_validation`
   - enforce required promotion manifest keys and field invariants
3. `evidence_integrity_validation`
   - verify required evidence references exist and hashes match
4. `approval_policy_validation`
   - enforce role/count/distinct-identity constraints and approval recency
5. `freeze_policy_validation`
   - enforce `none|advisory|hard` freeze semantics and break-glass constraints
6. `activation_readiness_validation`
   - ensure request is activation-ready without mutating active selectors

Required stage behavior:

- all stages listed in this v1 contract (`request_normalization` through
  `activation_readiness_validation`) are required stages.
- for these v1 stages, valid outcomes are `passed` or `failed`; `skipped`
  must not be emitted for any stage defined in this document.
- `failed` in any required stage sets overall gate status to `failed`.
- support for optional stages, and corresponding `status = skipped`
  behavior, is reserved for future pipeline versions that define such
  stages explicitly.
- when optional stages are introduced in a future version, any `skipped`
  outcome must include explicit `skip_reason`; in gate result artifacts this
  must be recorded as `details.skip_reason` whenever `status = skipped`.
- pipeline exits non-zero when overall status is `failed`.

## Promotion Request Bundle Contract

Promotion pipeline v1 consumes one request bundle per promotion attempt.

Recommended path:

- `<ACCESSDANE_ARTIFACT_BASE_DIR>/promotion_requests/<promotion_id>/`

Required files:

- `manifest.json`
- `evidence_index.json`

Optional files:

- `decision_memo.md`
- `override_justification.md` (required only for break-glass or advisory-freeze override)

### `manifest.json` minimum contract

Manifest keys must include all fields already enforced by activation logic:

- `promotion_id`
- `source_environment`
- `target_environment`
- `requested_by`
- `requested_at_utc`
- `source_run_id`
- `target_run_id`
- `feature_version`
- `ruleset_version`
- `evidence_artifacts[]`
- `approval_state`
- `approvals[]`
- `activation_state`
- `activation_started_at_utc`
- `activated_by`
- `activated_at_utc`
- `rollback_reference`
- `freeze_override_note`
- `break_glass_used`
- `break_glass_incident_id`

Pre-activation manifest invariants (enforced by gate and activation-time checks):

- `approval_state` must be `approved`.
- `activation_state` must be `not_started`.
- `activation_started_at_utc` must be `null`.
- `activated_by` must be `null`.
- `activated_at_utc` must be `null`.

Additional pipeline-required keys (v1):

- `contract_version` (must be `promotion_pipeline_v1`)
- `source_commit_sha` (40-char lowercase git SHA)
- `source_pr_number` (integer)
- `change_summary` (non-empty string)
- `flags` (object)
- `flags.annual_refresh_impact` (boolean; required in v1 manifests)

### `evidence_index.json` contract

Top-level keys:

- `contract_version` (must be `promotion_pipeline_v1`)
- `promotion_id`
- `generated_at_utc`
- `artifacts[]`

Each `artifacts[]` item must include:

- `artifact_type`
- `path`
- `sha256`
- `run_id`
- `generated_at_utc`

`artifact_type` enum values (v1):

- `refresh_payload`
- `review_feedback`
- `parser_drift_diff`
- `load_monitor`
- `benchmark_pack`
- `observability_rollup`
- `annual_signoff` (required for annual-refresh-driven promotion requests)

## Evidence Requirements

For all promotions, gate requires at least one valid artifact of each type:

- `refresh_payload`
- `review_feedback`
- `parser_drift_diff`
- `load_monitor`
- `benchmark_pack`
- `observability_rollup`

Conditional requirement (machine-evaluable):

- when `manifest.flags.annual_refresh_impact = true`, `annual_signoff` is
  required and must indicate approved signoff state.
- promotions whose rationale references annual-refresh
  corrections/backfill impact must set `manifest.flags.annual_refresh_impact`
  to `true`.

Evidence validation rules:

- every `manifest.evidence_artifacts[]` entry must appear in `evidence_index.artifacts[]`.
- every indexed path must exist and resolve under the active environment artifact roots.
- SHA-256 in evidence index must match on-disk bytes.
- evidence timestamps must be UTC with trailing `Z`.

Evidence freshness policy (v1):

- `stage -> prod`: all required artifacts must be <= 7 days old at gate evaluation.
- `dev -> stage`: all required artifacts must be <= 14 days old.

## Approval Policy Contract

Approval records are sourced from `manifest.approvals[]`.

Reference timestamp semantics:

- for gate evaluation, `request.activation_started_at_utc` is the gate
  evaluation start time (UTC RFC3339 with trailing `Z`) and is the reference
  timestamp for approval/freeze freshness checks in this contract.
- activation-time checks remain authoritative in `promotion-activate`; when
  activation begins, it may re-check policy freshness against actual
  activation start time and reject promotions that became stale/expired after
  gate evaluation.

General rules:

- no self-approval by `requested_by`.
- approval timestamps (`approved_at_utc`) must be <=
  `request.activation_started_at_utc`.
- approvals where `approved_at_utc + 24h <= request.activation_started_at_utc`
  are stale and do not count.

Path-specific requirements:

- `dev -> stage`
  - at least one valid approval
  - approver role must be one of allowed roles
- `stage -> prod`
  - at least two valid approvals from distinct identities
  - must include at least one `engineering_owner` and one `analyst_owner`

Allowed `approver_role` enum values:

- `engineering_owner`
- `analyst_owner`
- `release_operator`

## Freeze Policy Contract

Freeze state is loaded from environment `PROMOTION_FREEZE_FILE`.

Allowed freeze states:

- `none`
- `advisory`
- `hard`

Policy:

- `none`
  - no additional override requirements
  - `break_glass_used` must be `false`.
- `advisory`
  - `freeze_override_note` is required
  - `break_glass_used` must be `false`.
- `hard`
  - promotion blocked unless `break_glass_used = true`
  - `break_glass_incident_id` required
  - freeze file must include `override_approvers[]` with at least two distinct identities
  - freeze file must include `override_expires_at_utc`
  - override must be unexpired relative to `request.activation_started_at_utc`
    (gate reference time); activation may still reject at activation time if
    override freshness changes before activation starts.

## Environment And Path Safety Contract

- `source_environment -> target_environment` must be one of:
  - `dev -> stage`
  - `stage -> prod`
- `target_environment` must match active `ACCESSDANE_ENVIRONMENT` for gate execution.
- all artifact and manifest references must use safe single-segment ids for path tokens.
- absolute paths and path traversal segments are not allowed in identifiers.

## Pipeline Result Artifact Contract

On every pipeline invocation, emit one gate result payload.

Recommended path:

- `<ACCESSDANE_ARTIFACT_BASE_DIR>/promotion_gate_results/<promotion_id>/<gate_run_id>.json`

Required top-level keys:

1. `gate`
2. `request`
3. `stages`
4. `summary`
5. `errors`

`gate` required fields:

- `contract_version` (must be `promotion_pipeline_v1`)
- `gate_run_id`
- `evaluated_at_utc`
- `environment`
- `status` (`passed|failed`)

`request` required fields:

- `promotion_id`
- `source_environment`
- `target_environment`
- `manifest_path`
- `activation_started_at_utc` (gate evaluation start timestamp used as the
  reference time for approval/freeze expiry checks; must be UTC RFC3339 with
  trailing `Z`)

`stages[]` required fields:

- `stage_id`
- `status` (`passed|failed` for v1 required stages; `skipped` reserved for
  future contract versions that define optional stages)
- `checked_at_utc`
- `details`
- `details.skip_reason` (required when `status = skipped`; otherwise omitted;
  applies to future optional stages)

`summary` required fields:

- `total_stages`
- `failed_stages`
- `blocking_error_count`

`errors[]` required fields:

- `code`
- `message`
- `stage_id`
- `path` (nullable)

## Failure Code Taxonomy (v1)

Minimum stable error codes:

- `manifest_missing_field`
- `manifest_invalid_value`
- `promotion_path_not_allowed`
- `target_environment_mismatch`
- `evidence_missing`
- `evidence_hash_mismatch`
- `evidence_stale`
- `approval_insufficient`
- `approval_role_missing`
- `approval_self_approval`
- `freeze_override_required`
- `break_glass_invalid`
- `path_safety_violation`

## CI Integration Contract

Required PR/merge gate behavior:

- promotion-related PRs must run promotion gate in CI using request bundle artifacts.
- CI check must fail when gate status is `failed`.
- CI must attach or publish gate result JSON for reviewer inspection.

Required named checks (v1):

- `promotion-manifest-validate`
- `promotion-evidence-verify`
- `promotion-policy-gate`

## Audit And Retention

Retention policy:

- keep promotion request bundles and gate results for >= 400 days.
- gate results are append-only artifacts; updates require new `gate_run_id`.

Audit minimums:

- every activation in target environment must map to a passed gate result for the same `promotion_id`.
- activation records must reference gate-run metadata (`gate_run_id`, `evaluated_at_utc`).

## Backward Compatibility And Versioning

- v1 is additive relative to existing `promotion-activate` behavior.
- breaking schema or policy changes require `promotion_pipeline_v2`.
- Day 12 implementation must keep activation-time policy checks and add pre-activation pipeline checks, not replace activation safeguards.

## Non-Goals For v1

- auto-approvals based on metric-only heuristics
- cross-repository policy federation
- replacement of human decision accountability with automation-only promotion
