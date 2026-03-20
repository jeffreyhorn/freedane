# Sprint 7 Threshold Promotion Governance Notes (v1)

Prepared on: 2026-03-20

## Purpose

Define the v1 governance workflow for promoting threshold and ruleset changes
after annual refresh execution.

These notes intentionally keep promotion as a human-approved workflow, not an
automatic pipeline mutation.

## Trigger Conditions

Promotion review may be initiated when at least one condition is true:

- annual refresh sign-off is approved and analysts request threshold updates
- correction replay materially changes risk-band or disposition distributions
- recurring review-feedback evidence indicates sustained precision/recall drift

## Promotion Workflow

1. Proposal
  - proposer submits promotion request packet
2. Evidence assembly
  - assemble required artifacts listed below
3. Governance review
  - designated reviewers evaluate proposal against acceptance policy
4. Decision
  - approve, reject, or request revisions
5. Versioned publication
  - approved promotion is recorded with new threshold/ruleset version tag
6. Post-promotion monitoring
  - monitor early-run diagnostics for regressions

## Required Evidence Packet (v1)

Every promotion proposal must include:

- approved annual sign-off record:
  - `governance/annual_signoff.json`
- latest applicable review-feedback artifact:
  - `analysis_artifacts/review_feedback.json`
- associated refresh run payload:
  - `health_summary/refresh_run_payload.json`
- summary memo with:
  - current threshold/ruleset versions
  - proposed threshold/ruleset versions
  - expected risk-band impact
  - expected analyst workload impact
- regression-risk checklist:
  - false-positive risk
  - false-negative risk
  - fairness and geographic segment impact notes

## Minimum Decision Rules

Approval is allowed only when:

- annual sign-off status is `approved`
- evidence packet is complete and references one concrete run context
- decision includes at least two reviewers with distinct identities
- decision rationale is recorded in writing

Rejection is required when:

- annual sign-off is missing or `rejected`
- review-feedback evidence is absent or incompatible with proposed context
- proposal lacks rollback or fallback plan

## Decision Record Contract

Each governance decision should emit one decision record artifact.

Recommended path:

- `data/refresh_runs/<run_date>/annual_refresh/<run_id>/governance/threshold_promotion_decision.json`

Recommended keys:

1. `proposal`
2. `evidence`
3. `decision`
4. `approvers`
5. `publication`
6. `monitoring_plan`
7. `error`

### `proposal`

- `proposal_id`
- `parent_run_id`
- `current_feature_version`
- `current_ruleset_version`
- `proposed_feature_version` (nullable)
- `proposed_ruleset_version` (nullable)
- `proposed_threshold_set_version`
- `submitted_by`
- `submitted_at`

### `evidence`

- `annual_signoff_path`
- `review_feedback_path`
- `refresh_payload_path`
- `benchmark_summary_path` (nullable until benchmark packs are implemented)

### `decision`

- `status` (`approved|rejected|needs_revision`)
- `decided_at`
- `rationale`
- `effective_date` (nullable unless approved)

### `approvers`

Array of reviewer decisions:

- `reviewer`
- `role`
- `decision` (`approve|reject`)
- `comment` (nullable)

### `publication`

- `published_threshold_set_version` (nullable)
- `published_ruleset_version` (nullable)
- `change_log_entry` (nullable)

### `monitoring_plan`

- `owner`
- `first_check_at`
- `checks` (array; examples: `load_monitor`, `review_feedback_delta`)

## Rollback Expectations

Approved promotions must define a rollback path before publication:

- rollback trigger conditions
- rollback executor role
- rollback target version
- maximum allowed rollback lead time

## Governance Boundaries For v1

- No automatic threshold promotion writes from refresh runner.
- No single-reviewer approvals for production promotion.
- No promotion approval without annual sign-off and review-feedback evidence.

## Planned Follow-On (Post-Day-9)

- Day 10: annual refresh runner/checklist automation should emit metadata that
  simplifies evidence packet assembly.
- Days 11-12: benchmark pack outputs should be added as required promotion
  evidence once contract and generator are in place.

