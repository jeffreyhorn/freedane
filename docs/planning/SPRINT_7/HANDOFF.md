# Sprint 7 Handoff (Draft)

Prepared on: 2026-03-14

## Starting Baseline

Sprint 7 starts from a completed Sprint 6 investigation workflow baseline:

- parcel dossier workflow is implemented (`accessdane parcel-dossier`)
- ranked review queue and export workflow are implemented (`accessdane review-queue`)
- static investigation report runner is implemented (`accessdane investigation-report`)
- case-review persistence and lifecycle workflow are implemented (`accessdane case-review ...`)
- review-feedback artifact generator is implemented (`accessdane review-feedback`)
- methodology and framing guardrails are documented in Sprint 6 docs
- schema/migration head includes case review support (`20260312_0013`)

Latest Sprint 6 closeout evidence:

- targeted Sprint 6 workflow test set: `35 passed`
- full quality/regression suite: `229 passed`
- targeted integration smoke validates queue -> review -> feedback -> dossier -> report interoperability

Reference artifacts:

- [Sprint 6 acceptance review](../SPRINT_6/ACCEPTANCE_REVIEW.md)
- [Sprint 6 operations runbook](../SPRINT_6/OPERATIONS.md)
- [Sprint 6 methodology memo](../SPRINT_6/METHODOLOGY_MEMO.md)
- [Sprint 6 plan](../SPRINT_6/PLAN.md)

## Highest-Priority Carryover

Start Sprint 7 with these items in order:

1. Add scheduled refresh jobs for recurring ingest/build/score/report workflow execution.
2. Add parser drift detection and monitoring signals when AccessDane markup changes.
3. Add load monitoring and alerting for command/runtime/data-volume failures.
4. Add annual refresh workflow for new assessment rolls and RETR exports.
5. Add recurring benchmark packs for fairness/performance sanity checks.
6. Define threshold-promotion governance workflow (proposal -> review -> approved ruleset version).

## Open Risks And Constraints

- Sprint 6 workflow is reproducible but still manually orchestrated; recurring operation depends on operator discipline.
- Parser drift can still break ingestion without early-warning automation.
- Monitoring and alerting are not yet first-class operational outputs.
- Review feedback artifacts exist, but change-promotion controls are not yet automated.

## Recommended First Validation Pass

Before Sprint 7 automation work begins:

```bash
make typecheck && make lint && make format && make test
.venv/bin/accessdane init-db
.venv/bin/accessdane review-queue --top 10 --out data/sprint7_baseline_queue.json
.venv/bin/accessdane review-feedback --out data/sprint7_baseline_feedback.json
.venv/bin/accessdane investigation-report --top 10 --html-out data/sprint7_baseline_report.html
```

Then verify one end-to-end analyst cycle:

```bash
.venv/bin/accessdane parcel-dossier --id <parcel_id> --out data/sprint7_baseline_dossier.json
.venv/bin/accessdane case-review create --score-id <score_id> --status in_review --reviewer "<analyst>"
.venv/bin/accessdane case-review update --id <case_review_id> --status resolved --disposition false_positive
.venv/bin/accessdane review-feedback --out data/sprint7_post_review_feedback.json
```

## What Should Not Be Redone

- Re-implementing Sprint 6 dossier/queue/review/report contracts without a concrete defect.
- Reworking Sprint 5 scoring logic unless review-feedback evidence indicates a specific rules defect.
- Treating score output as adjudicated proof in any automation/reporting surface.

## Sprint 7 Success Definition

Sprint 7 should make Sprint 6 operationally sustainable:

- recurring refresh/report cycles run on schedule with clear run status and alerting
- parser/load drift is detected early and surfaced to operators
- annual refresh and benchmark workflows are documented and reproducible
- threshold/ruleset promotion workflow is explicit, auditable, and versioned
