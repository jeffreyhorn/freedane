# Sprint 5 Plan

Prepared on: 2026-03-07

## Sprint Goal

Sprint 5 should produce the first explainable fraud-signal scoring system on top of the Sprint 4 context-ingestion baseline.
The primary product is a repeatable feature-and-scoring workflow that can be rerun, audited, and explained parcel-by-parcel.

This plan is based on:

- [docs/planning/devplan.md](../devplan.md), especially Sprint 5 scope for sales-ratio study, feature engineering, scoring, and calibration
- [docs/planning/SPRINT_5/HANDOFF.md](HANDOFF.md), especially carryover priorities around feature storage/versioning, transparent rules, and scoped validation-first execution

Total planned effort: `80` hours.

## Planning Constraints

- All 14 days in the two-week sprint have scheduled work.
- No single day exceeds `10` hours.
- Each day is executed on its own short-lived branch created from current `main`.
- Each day ends with the same delivery loop:
  - commit the day's changes on that day's branch
  - open a Pull Request from that day's branch into `main`
  - review the PR
  - make any follow-up fixes required by that review
  - hand the PR off for merge, which marks the day complete
  - start the next day from a fresh branch created from the newly updated `main`

## Standard Daily Closeout

Reserve the last `1` hour of each day for the PR loop unless the day says otherwise.

Recommended branch naming pattern:

- `sprint5/day-01-baseline`
- `sprint5/day-02-signal-contract`
- `sprint5/day-03-feature-schema`
- continue the same pattern through Day 14

Standard daily branch workflow:

1. Update local `main` to the latest merged state.
2. Create a fresh branch for that day's scoped work.
3. Do only that day's planned changes on the branch.
4. Open the PR from that branch back into `main`.
5. After review fixes are merged, delete the branch and begin the next day from `main`.

Daily closeout sequence:

1. Run the day's targeted validation.
2. Commit only the intended changes for that day on the day-specific branch.
3. Open a PR into `main` with a focused description of scope, tests run, and known limits.
4. Review the PR and address review comments on the same branch that same day.
5. Hand the PR off for merge after the fixes are in.
6. After merge, delete the day branch and treat that merge as the day's completion point.

## Day-By-Day Plan

### Day 1 (`5h`): Re-Establish Sprint 5 Baseline And Lock Scope

Focus:

- Start Sprint 5 from a known-good Sprint 4 baseline and capture startup evidence before feature/scoring work begins.

Planned work:

- Run the handoff baseline checks: `make typecheck && make lint && make format && make test`.
- Run `.venv/bin/accessdane init-db` and `.venv/bin/accessdane spatial-support --out data/sprint5_baseline_spatial_support.json`.
- Confirm current schema/migration head and identify required new scoring tables.
- Convert Sprint 5 carryover priorities into an execution checklist with acceptance gates.

Deliverables:

- `docs/planning/SPRINT_5/DAY_1_BASELINE.md`
- baseline spatial-support artifact under `data/`

PR closeout:

- Commit baseline evidence and checklist artifacts.
- Open a baseline PR and merge after review.

### Day 2 (`5h`): Design Fraud-Signal Contract And Persistence Model

Focus:

- Define the v1 scoring data contract before implementing storage and commands.

Planned work:

- Define v1 output contracts for `sales-ratio-study`, `build-features`, and `score-fraud`.
- Define table-level contracts for `parcel_features`, `fraud_scores`, `fraud_flags`, and run/version metadata.
- Define reason-code conventions and explainability requirements for scored outputs.
- Define scoped/full rerun and idempotency behavior.

Deliverables:

- `docs/planning/SPRINT_5/FRAUD_SIGNAL_V1.md`

PR closeout:

- Commit Sprint 5 signal contract doc.
- Open PR, address comments, merge.

### Day 3 (`4h`): Implement Feature/Score Schema And Migration Scaffolding

Focus:

- Add migration-managed storage for Sprint 5 features and scoring outputs.

Planned work:

- Add SQLAlchemy models for `parcel_features`, `fraud_scores`, and `fraud_flags` (+ run metadata table if required by Day 2 contract).
- Add Alembic migration(s) with keys/indexes for versioned reruns and parcel/year lookups.
- Update migration tests to enforce new schema constraints.
- Keep schema aligned with Day 2 contracts.

Deliverables:

- new scoring/feature model(s)
- Alembic revision(s)
- migration test updates

PR closeout:

- Commit schema and migration tests.
- Open PR, review, merge.

### Day 4 (`5h`): Implement `accessdane sales-ratio-study` Core

Focus:

- Deliver first working sales-ratio study command and storage/report output path.

Planned work:

- Implement `accessdane sales-ratio-study` CLI command shell and aggregation entry point.
- Compute baseline sales-ratio metrics by year, class, and area.
- Persist or emit reproducible study outputs per Day 2 contract.
- Add first-pass tests for metric correctness on representative fixtures.

Deliverables:

- `sales-ratio-study` command v1
- ratio-study implementation
- core tests

PR closeout:

- Commit command, implementation, and tests.
- Open PR, resolve review, merge.

### Day 5 (`5h`): Harden Sales-Ratio Study And Operator Diagnostics

Focus:

- Make ratio-study outputs resilient and operator-usable.

Planned work:

- Add edge-case handling for sparse years, small peer groups, and missing-class/area values.
- Add diagnostics in command output for excluded/insufficient groups.
- Extend tests for null handling, deterministic reruns, and scoped execution.
- Document ratio-study operator usage and interpretation notes.

Deliverables:

- hardened ratio-study behavior
- expanded test coverage
- operations documentation for ratio study

PR closeout:

- Commit hardening, tests, and docs.
- Open PR, address review, merge.

### Day 6 (`7h`): Design Feature Engineering Spec

Focus:

- Define deterministic parcel-feature generation logic before implementation.

Planned work:

- Specify feature definitions for assessment-to-sale ratio, peer percentile, year-over-year value change, permit-adjusted change, appeal outcome deltas, and lineage effects.
- Define feature null/default semantics and quality guards.
- Define feature versioning strategy and run identifiers.
- Define build scope controls (full vs selected parcel/year segments).

Deliverables:

- `docs/planning/SPRINT_5/FEATURE_ENGINEERING_V1.md`

PR closeout:

- Commit feature-engineering design doc.
- Open PR, review, merge.

### Day 7 (`7h`): Implement `accessdane build-features` Base Pipeline

Focus:

- Deliver first pass of parcel feature generation and persistence.

Planned work:

- Implement `accessdane build-features` command and pipeline skeleton.
- Build core features from sales + parcel-year baseline context.
- Write feature rows to `parcel_features` with run/version metadata.
- Add integration tests for deterministic feature writes and scoped rebuild behavior.

Deliverables:

- `build-features` command v1
- feature pipeline base implementation
- integration tests

PR closeout:

- Commit command, pipeline, and tests.
- Open PR, address review, merge.

### Day 8 (`7h`): Add Permit/Appeal/Lineage Feature Layers

Focus:

- Complete Sprint 5 feature families using Sprint 4 and Sprint 2 context inputs.

Planned work:

- Add permit-adjusted and appeal-delta feature calculations.
- Add lineage-sensitive feature effects where lineage links are present.
- Ensure unresolved/missing context does not break feature builds.
- Expand tests for mixed-context parcel scenarios.

Deliverables:

- expanded feature set in `build-features`
- context-aware feature tests

PR closeout:

- Commit context feature expansions and tests.
- Open PR, review, merge.

### Day 9 (`7h`): Feature Quality Checks And Operator Reporting

Focus:

- Make feature outputs auditable and safe to consume by scoring.

Planned work:

- Add feature-level validation checks (required fields, bounds, impossible combinations).
- Add feature run summaries and output reporting for operators.
- Add targeted tests for feature validation behavior and run summaries.
- Update operations docs for feature rebuild workflow.

Deliverables:

- feature validation/reporting layer
- updated Sprint 5 operations guidance

PR closeout:

- Commit validation/reporting implementation and docs.
- Open PR, resolve review, merge.

### Day 10 (`7h`): Design Scoring Rules And Reason-Code Taxonomy

Focus:

- Finalize transparent scoring logic contract before implementation.

Planned work:

- Define v1 fraud scoring rule set and weighting/threshold semantics.
- Define explainability requirements: reason-code structure and human-readable narratives.
- Define `fraud_scores` and `fraud_flags` generation semantics from feature inputs.
- Define score versioning and rerun comparability requirements.

Deliverables:

- `docs/planning/SPRINT_5/SCORING_RULES_V1.md`

PR closeout:

- Commit scoring-rules design doc.
- Open PR, review, merge.

### Day 11 (`7h`): Implement `accessdane score-fraud` And Persisted Outputs

Focus:

- Deliver first working explainable scoring command.

Planned work:

- Implement `accessdane score-fraud` command.
- Compute rule-based scores from `parcel_features` and persist `fraud_scores` / `fraud_flags`.
- Emit reason codes and plain-language explanation payloads.
- Add tests for rule correctness, reason-code determinism, and rerun idempotency.

Deliverables:

- `score-fraud` command v1
- scoring implementation
- scoring/reason-code tests

PR closeout:

- Commit scoring command, storage path, and tests.
- Open PR, address review, merge.

### Day 12 (`6h`): Harden Scoring Workflow And Ranked Output Surfaces

Focus:

- Make scoring outputs stable and directly usable for analyst triage.

Planned work:

- Add ranking output surfaces for top-flagged parcels and category breakdowns.
- Add safeguards for missing/partial feature rows.
- Expand tests for ranking determinism, tie-breaking, and explanation consistency.
- Update Sprint 5 operations docs for end-to-end feature+score runs.

Deliverables:

- hardened scoring/ranking outputs
- expanded scoring workflow docs/tests

PR closeout:

- Commit scoring hardening and docs/tests.
- Open PR, resolve review, merge.

### Day 13 (`4h`): Add Calibration Support Artifacts

Focus:

- Provide practical threshold tuning workflows for analysts.

Planned work:

- Add threshold-tuning SQL scripts and/or notebook templates for score calibration.
- Add a lightweight calibration notes doc describing expected inputs and outputs.
- Validate artifacts against current score outputs for basic compatibility.

Deliverables:

- calibration SQL/notebook artifact(s)
- calibration usage notes

PR closeout:

- Commit calibration support artifacts.
- Open PR, review, merge.

### Day 14 (`4h`): Sprint 5 Acceptance Pack And Sprint 6 Handoff

Focus:

- Close Sprint 5 with acceptance evidence and a clear handoff for investigation workflow buildout.

Planned work:

- Run targeted Sprint 5 validation set across ratio-study, feature, and score commands.
- Document acceptance results against Sprint 5 criteria.
- Finalize Sprint 5 operations docs for repeatable end-to-end execution.
- Draft Sprint 6 handoff with open risks and follow-up priorities.

Deliverables:

- `docs/planning/SPRINT_5/ACCEPTANCE_REVIEW.md`
- updated Sprint 5 operations documentation
- `docs/planning/SPRINT_6/HANDOFF.md` draft

PR closeout:

- Commit acceptance artifacts and handoff docs.
- Open final Sprint 5 PR, resolve review comments, merge.

## Planned Effort Summary

- Days 1-5: `24h` on baseline setup, signal contract/schema, and sales-ratio study logic
- Days 6-9: `28h` on feature engineering design, implementation, and validation
- Days 10-12: `20h` on scoring engine and reason-code outputs
- Days 13-14: `8h` on calibration support and Sprint 5 closeout

This preserves the `80h` Sprint 5 estimate from the development plan while structuring delivery as 14 PR-driven daily increments from `main`.
