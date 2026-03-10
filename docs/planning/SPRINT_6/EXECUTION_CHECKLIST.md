# Sprint 6 Execution Checklist

Created: 2026-03-10

Source references:

- [Sprint 6 plan](PLAN.md)
- [Sprint 6 handoff](HANDOFF.md)
- [Development plan Sprint 6 section](../devplan.md)

## Delivery Blocks With Acceptance Gates

### Parcel Dossier Contract And Core (Days 2-4)

- [ ] Day 2: finalize dossier command contract (`PARCEL_DOSSIER_V1.md`)
  - Acceptance gate: output schema, section ordering, null semantics, and deterministic sort rules are explicit.
- [ ] Day 3: add dossier-support schema/index/query-layer scaffolding
  - Acceptance gate: migration tests pass and helper queries cover required evidence joins.
- [ ] Day 4: implement `accessdane parcel-dossier` core
  - Acceptance gate: one-parcel output includes assessment, sales, peer, permit/appeal, and reason-code evidence without direct SQL.

### Ranked Queue And Reporting Surface (Days 5-7)

- [ ] Day 5: finalize ranked queue/report contract (`REVIEW_QUEUE_V1.md`)
  - Acceptance gate: top-N fields, sorting, filtering, and export payload expectations are documented.
- [ ] Day 6: implement ranked queue and export paths
  - Acceptance gate: deterministic ranking tests and export-schema tests pass.
- [ ] Day 7: implement minimal analyst-facing report surface
  - Acceptance gate: static report build succeeds with queue summary + dossier drill-in paths.

### Case Review Workflow (Days 8-10)

- [ ] Day 8: finalize case review workflow contract (`CASE_REVIEW_V1.md`)
  - Acceptance gate: statuses, dispositions, transition rules, and linkage to scoring context are explicit.
- [ ] Day 9: implement `case_reviews` persistence and CLI flows
  - Acceptance gate: create/update/list flows are test-covered with invalid-transition handling.
- [ ] Day 10: integrate review outcomes into queue and dossier outputs
  - Acceptance gate: reviewed/unreviewed filtering and disposition overlays are visible and deterministic.

### Feedback, Docs, And Hardening (Days 11-14)

- [ ] Day 11: implement review-outcome feedback artifacts for threshold/exclusion tuning
  - Acceptance gate: repeatable aggregation artifacts are generated from persisted review outcomes.
- [ ] Day 12: write methodology memo (`METHODOLOGY_MEMO.md`)
  - Acceptance gate: documentation clearly distinguishes triage signal from proof and defines analyst verification expectations.
- [ ] Day 13: harden workflow and finalize runbook (`OPERATIONS.md`)
  - Acceptance gate: targeted integration checks pass and troubleshooting guidance is documented.
- [ ] Day 14: complete acceptance pack + Sprint 7 handoff
  - Acceptance gate: Sprint 6 acceptance criteria are explicitly assessed and Sprint 7 risks/priorities are documented.

## Sprint 6 Acceptance Criteria Tracking

- [ ] Analysts can open one parcel and inspect full evidence chain without direct SQL.
- [ ] Review outcomes are persisted and usable for future threshold/exclusion refinement.
- [ ] Output surface and documentation consistently frame score as risk signal, not proof.
