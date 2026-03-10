# Sprint 6 Plan

Prepared on: 2026-03-10

## Sprint Goal

Sprint 6 should turn the Sprint 5 fraud-signal outputs into an investigation workflow that an analyst can use without direct SQL.
The primary product is an analyst-facing dossier + review queue workflow with persisted case-review outcomes and clear risk-signal framing.

This plan is based on:

- [docs/planning/devplan.md](../devplan.md), especially Sprint 6 scope for dossier output, reporting surfaces, case-review tracking, and feedback loop hardening
- [docs/planning/SPRINT_6/HANDOFF.md](HANDOFF.md), especially carryover priorities around parcel dossier, ranked reporting, and review-outcome persistence

Total planned effort: `80` hours.

## Planning Constraints

- All 14 days in the two-week sprint have scheduled work.
- No single day exceeds `10` hours.
- Each day is executed on its own short-lived branch created from current `main`.
- Each day starts by creating a fresh branch off updated `main` for that day's scope.
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

- `sprint6/day-01-baseline`
- `sprint6/day-02-dossier-contract`
- `sprint6/day-03-dossier-schema`
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

### Day 1 (`5h`): Re-Establish Sprint 6 Baseline And Investigation Scope

Focus:

- Start Sprint 6 from a known-good Sprint 5 baseline and lock dossier/reporting scope boundaries.

Planned work:

- Run handoff baseline checks: `make typecheck && make lint && make format && make test`.
- Run Sprint 5 pipeline smoke sequence (`init-db`, `sales-ratio-study`, `build-features`, `score-fraud`) and confirm expected run chaining.
- Convert Sprint 6 carryover priorities into an execution checklist with acceptance gates.
- Confirm current schema head and identify required new persistence objects for case review workflow.

Deliverables:

- `docs/planning/SPRINT_6/DAY_1_BASELINE.md`
- Sprint 6 execution checklist artifact

PR closeout:

- Commit baseline evidence and checklist updates.
- Open baseline PR and merge after review.

### Day 2 (`5h`): Define Parcel Dossier Contract And Evidence Sections

Focus:

- Lock the parcel-dossier output contract before implementing command logic.

Planned work:

- Define `accessdane parcel-dossier --id ...` output schema and section ordering.
- Specify evidence sections for assessment history, sales, peer context, permit/appeal events, and reason-code evidence.
- Define deterministic sorting, null semantics, and explicit empty-section behavior.
- Define scoped validation fixtures for dossier command behavior.

Deliverables:

- `docs/planning/SPRINT_6/PARCEL_DOSSIER_V1.md`

PR closeout:

- Commit dossier contract doc.
- Open PR, address review, merge.

### Day 3 (`6h`): Implement Dossier Data Access Layer And Supporting Schema Updates

Focus:

- Add any required schema/index support and shared data-access primitives for dossier generation.

Planned work:

- Add migration-managed schema/index updates required for dossier query efficiency and evidence joins.
- Add shared query helpers for timeline assembly across source tables.
- Add migration and data-access tests for new constraints/indexes.
- Keep all changes aligned with Day 2 dossier contract.

Deliverables:

- migration revision(s) for dossier support
- dossier query/helper primitives
- migration test updates

PR closeout:

- Commit schema/query-layer changes and tests.
- Open PR, resolve review, merge.

### Day 4 (`8h`): Implement `accessdane parcel-dossier` Command Core

Focus:

- Deliver first working parcel dossier command with full evidence chain assembly.

Planned work:

- Implement `accessdane parcel-dossier` CLI command and orchestration path.
- Assemble timeline output from assessments, sales matches, peer snapshots, permit/appeal events, and fraud-score reason codes.
- Add deterministic ordering and human-readable narrative labels.
- Add core command tests for output shape, deterministic ordering, and missing-section handling.

Deliverables:

- `parcel-dossier` command v1
- dossier assembly implementation
- core dossier command tests

PR closeout:

- Commit command/implementation/tests.
- Open PR, review, merge.

### Day 5 (`6h`): Design Review Queue And Batch Reporting Contract

Focus:

- Define ranked-review queue and batch report output contracts before UI/report implementation.

Planned work:

- Define top-N queue contract (required fields, sorting, filters, and pagination limits).
- Define export/report payload shape for analyst handoff (JSON and optional CSV).
- Define comparability semantics across reruns/version tags.
- Define queue diagnostics for skipped/filtered rows.

Deliverables:

- `docs/planning/SPRINT_6/REVIEW_QUEUE_V1.md`

PR closeout:

- Commit review-queue/report contract doc.
- Open PR, address review, merge.

### Day 6 (`7h`): Implement Ranked Review Queue And Batch Exports

Focus:

- Deliver first working top-N review queue generation from scored outputs.

Planned work:

- Implement queue generator command/module from `fraud_scores` + `fraud_flags`.
- Add scope filters (ids/years/version selectors) and deterministic ordering.
- Add JSON export path and optional CSV output for analyst workflows.
- Add tests for ranking determinism, filters, and export schema stability.

Deliverables:

- ranked review queue generator
- batch export outputs
- queue/export tests

PR closeout:

- Commit queue implementation and tests.
- Open PR, resolve review, merge.

### Day 7 (`7h`): Implement Static Investigation Report Surface (v1)

Focus:

- Deliver a lightweight analyst-facing report surface without full app infrastructure overhead.

Planned work:

- Implement static HTML (or minimal dashboard runner) output from queue+dossier payloads.
- Add key analyst interactions: top queue summary, parcel dossier drill-in links/anchors, and reason-code summaries.
- Add deterministic report build command and artifact output path.
- Add smoke tests for report generation and required sections.

Deliverables:

- static report runner v1
- report artifact template/assets
- report smoke tests

PR closeout:

- Commit report runner/assets/tests.
- Open PR, address review, merge.

### Day 8 (`5h`): Define Case Review Data Model And Workflow Contract

Focus:

- Specify how analysts record outcomes before implementing persistence.

Planned work:

- Define `case_reviews` contract (fields, statuses, dispositions, required evidence links, and audit timestamps).
- Define idempotent update semantics and review ownership metadata.
- Define linkage from review records back to score/version context.
- Define validation rules and error semantics for invalid transitions.

Deliverables:

- `docs/planning/SPRINT_6/CASE_REVIEW_V1.md`

PR closeout:

- Commit case-review contract doc.
- Open PR, review, merge.

### Day 9 (`6h`): Implement `case_reviews` Persistence And CLI Workflow

Focus:

- Deliver persisted analyst review tracking with CLI-level create/update/list flows.

Planned work:

- Add migration-managed `case_reviews` table and supporting indexes.
- Implement create/update/list CLI paths with validation and deterministic filters.
- Add linkages to parcel/year and score-version identifiers.
- Add tests for lifecycle transitions, validation errors, and query behavior.

Deliverables:

- `case_reviews` schema + CLI workflow
- case-review lifecycle tests

PR closeout:

- Commit schema/CLI/tests.
- Open PR, resolve review, merge.

### Day 10 (`5h`): Add Review-Outcome Feeds Into Queue And Dossier Views

Focus:

- Integrate analyst review outcomes into investigation surfaces.

Planned work:

- Surface review status/disposition in queue exports and dossier outputs.
- Add filters for reviewed/unreviewed queues and disposition-specific views.
- Ensure historical review outcomes remain visible across reruns with version context.
- Add tests for merged queue+dossier review overlays.

Deliverables:

- review-outcome enriched queue/dossier outputs
- overlay/filter tests

PR closeout:

- Commit review integration and tests.
- Open PR, address review, merge.

### Day 11 (`6h`): Implement Feedback Loop Artifacts For Threshold/Exclusion Tuning

Focus:

- Convert case-review outcomes into actionable threshold/exclusion feedback artifacts.

Planned work:

- Add report/SQL outputs summarizing false-positive rates by reason code and risk band.
- Add per-rule outcome slices to support threshold tuning proposals.
- Add artifact command or scripted query workflow for repeatable extraction.
- Add tests for feedback artifact shape and deterministic aggregation behavior.

Deliverables:

- review-feedback artifact generator
- threshold/exclusion feedback summaries
- feedback aggregation tests

PR closeout:

- Commit feedback artifacts and tests.
- Open PR, review, merge.

### Day 12 (`5h`): Write Methodology Memo And Risk-Signal Framing Guardrails

Focus:

- Document score interpretation and misuse boundaries for analyst and stakeholder audiences.

Planned work:

- Draft methodology memo describing what the score means and what it does not mean.
- Document triage-vs-proof language and required analyst verification steps.
- Add review-outcome interpretation guidance (false positive, insufficient evidence, escalated).
- Cross-link memo from dossier/report/operations docs.

Deliverables:

- `docs/planning/SPRINT_6/METHODOLOGY_MEMO.md`

PR closeout:

- Commit methodology memo and doc links.
- Open PR, address review, merge.

### Day 13 (`5h`): Sprint 6 Hardening And Operations Runbook

Focus:

- Harden the integrated investigation workflow and document repeatable operator execution.

Planned work:

- Run targeted end-to-end dossier/queue/case-review validation set.
- Fix edge-case defects discovered during integration dry run.
- Finalize Sprint 6 operations runbook for repeatable refresh/report/review execution.
- Add troubleshooting notes for common data-shape or workflow failures.

Deliverables:

- `docs/planning/SPRINT_6/OPERATIONS.md`
- hardening fixes and targeted tests

PR closeout:

- Commit runbook + hardening updates.
- Open PR, resolve review, merge.

### Day 14 (`4h`): Sprint 6 Acceptance Pack And Sprint 7 Handoff

Focus:

- Close Sprint 6 with acceptance evidence and a clear handoff for automation/monitoring work.

Planned work:

- Run targeted Sprint 6 acceptance validation set across dossier, queue/reporting, and case-review flows.
- Document acceptance results against Sprint 6 criteria.
- Finalize Sprint 6 operations docs for repeatable analyst usage.
- Draft Sprint 7 handoff with open risks and automation priorities.

Deliverables:

- `docs/planning/SPRINT_6/ACCEPTANCE_REVIEW.md`
- updated Sprint 6 operations documentation
- `docs/planning/SPRINT_7/HANDOFF.md` draft

PR closeout:

- Commit acceptance artifacts and handoff docs.
- Open final Sprint 6 PR, resolve review comments, merge.

## Planned Effort Summary

- Days 1-4: `24h` on dossier contract, schema support, and core parcel-dossier implementation
- Days 5-7: `20h` on ranked queue generation and lightweight report surface
- Days 8-10: `16h` on case-review workflow design, persistence, and integration into outputs
- Days 11-14: `20h` on feedback-loop artifacts, methodology/docs, hardening, and sprint closeout

This preserves the `80h` Sprint 6 estimate from the development plan while structuring delivery as 14 PR-driven daily increments from `main`.
