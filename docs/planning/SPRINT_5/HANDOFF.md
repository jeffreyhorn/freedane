# Sprint 5 Handoff (Draft)

Prepared on: 2026-03-06

## Starting Baseline

Sprint 5 starts from a completed Sprint 4 context-ingestion baseline:

- permit importer is implemented (`accessdane ingest-permits --file ...`)
- appeal importer is implemented (`accessdane ingest-appeals --file ...`)
- permit/appeal manual templates are available in `docs/templates/`
- `parcel_year_facts` includes permit and appeal rollup/context fields
- optional spatial guardrails are in place (`accessdane spatial-support`)
- schema/migration head is `20260307_0010`

Latest Sprint 4 closeout evidence:

- targeted Sprint 4 test set: `67 passed`
- clean-db migration bootstrap to head succeeded
- permit/appeal template ingest commands executed without importer failure
- parcel-year rebuild and spatial-mode probe executed successfully

Reference artifacts:

- [Sprint 4 acceptance review](../SPRINT_4/ACCEPTANCE_REVIEW.md)
- [Sprint 4 operations runbook](../SPRINT_4/OPERATIONS.md)
- [Sprint 4 plan](../SPRINT_4/PLAN.md)

## Highest-Priority Carryover

Start Sprint 5 with these items in order:

1. Define and migrate feature storage for scoring inputs (`parcel_features` or equivalent) plus run metadata/versioning.
2. Implement sales-ratio study command with reproducible year/class/area rollups.
3. Implement feature build pipeline that joins sales, permit, appeal, lineage, and parcel-year context into explainable parcel-level features.
4. Implement transparent rules-based scoring with reason codes (`score-fraud` v1) before any opaque model.
5. Add calibration outputs (SQL/notebook/report) and threshold tuning workflow for analyst review.

## Open Risks And Constraints

- Parcel linkage quality for manual permit/appeal files still depends on source locator quality; unresolved links reduce downstream feature completeness.
- Context ingestion is now stable, but there is not yet a dedicated analyst case-review persistence workflow.
- Spatial support is intentionally guarded and optional; geometry-derived features should remain feature-flagged until a PostGIS-backed runtime is confirmed.
- Full-corpus refresh workflows can still be long-running; Sprint 5 should keep scoped validation paths first-class.

## Recommended First Validation Pass

Before Sprint 5 feature work begins:

```bash
make typecheck && make lint && make format && make test
.venv/bin/accessdane init-db
.venv/bin/accessdane spatial-support --out data/sprint5_baseline_spatial_support.json
```

Then confirm Sprint 4 ingestion + context paths on representative files:

```bash
.venv/bin/accessdane ingest-permits --file <permits.csv>
.venv/bin/accessdane ingest-appeals --file <appeals.csv>
.venv/bin/accessdane build-parcel-year-facts
```

## What Should Not Be Redone

- Redesigning permit/appeal import contracts unless a concrete defect is found.
- Reworking parcel-year permit/appeal rollup semantics before Sprint 5 scoring requirements prove a gap.
- Forcing geometry/PostGIS as a hard prerequisite for core scoring workflows.

## Sprint 5 Success Definition

Sprint 5 should produce an explainable scoring baseline that analysts can inspect and reproduce:

- every score has deterministic reason codes tied to persisted features
- feature generation can be rerun with stable versioned logic
- output supports ranked parcel review without requiring ad hoc SQL exploration
