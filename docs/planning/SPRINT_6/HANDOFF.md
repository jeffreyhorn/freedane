# Sprint 6 Handoff (Draft)

Prepared on: 2026-03-10

## Starting Baseline

Sprint 6 starts from a completed Sprint 5 fraud-signal baseline:

- `accessdane sales-ratio-study` implemented and persisted via `scoring_runs`
- `accessdane build-features` implemented with persisted `parcel_features`
- `accessdane score-fraud` implemented with persisted `fraud_scores` and `fraud_flags`
- ranked score outputs and reason-code breakdowns available for analyst triage
- calibration support artifacts available:
  - `docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql`
  - `docs/planning/SPRINT_5/CALIBRATION_NOTES.md`
- migration head remains `20260307_0011`

Latest Sprint 5 closeout evidence:

- targeted Sprint 5 test set: `32 passed`
- closeout run chain succeeded:
  - run `4`: `sales_ratio_study`
  - run `5`: `build_features`
  - run `6`: `score_fraud`
- persisted scoring output in validation scope:
  - `fraud_scores`: `58684`
  - `fraud_flags`: `16344`

Reference artifacts:

- [Sprint 5 acceptance review](../SPRINT_5/ACCEPTANCE_REVIEW.md)
- [Sprint 5 operations runbook](../SPRINT_5/OPERATIONS.md)
- [Sprint 5 plan](../SPRINT_5/PLAN.md)

## Highest-Priority Carryover

Start Sprint 6 with these items in order:

1. Implement `accessdane parcel-dossier --id ...` to produce one parcel evidence chain (assessment/sales/permit/appeal/reason codes).
2. Add ranked review queue output for top-N parcels with export-friendly payloads.
3. Add minimal analyst-facing report surface (static HTML runner or lightweight dashboard).
4. Add `case_reviews` persistence for analyst notes/dispositions/false-positive tracking.
5. Add a threshold/exclusion feedback loop informed by reviewed cases.
6. Write methodology memo clarifying score meaning, limits, and non-proof framing.

## Open Risks And Constraints

- Score explainability is strong, but there is still no first-class case-management workflow for human review outcomes.
- Local data currently contains sparse segmentation context (for example TRS concentration), which can limit neighborhood-level dashboard realism.
- Calibration workflow is query-driven and reproducible, but not yet coupled to governed promotion steps for threshold changes.
- Risk of overconfidence remains if downstream UI language presents score as proof rather than triage signal.

## Recommended First Validation Pass

Before Sprint 6 feature work begins:

```bash
make typecheck && make lint && make format && make test
.venv/bin/accessdane init-db
.venv/bin/accessdane sales-ratio-study --version-tag sales_ratio_v1
.venv/bin/accessdane build-features --feature-version feature_v1
.venv/bin/accessdane score-fraud --feature-version feature_v1 --ruleset-version scoring_rules_v1
psql "${DATABASE_URL/postgresql+psycopg/postgresql}" -v ON_ERROR_STOP=1 -f docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql
```

Note: this validation sequence assumes `DATABASE_URL` points to PostgreSQL (with the `postgresql+psycopg` scheme). The repository default is SQLite; use the Sprint 5 operations runbook for guidance on PostgreSQL overrides and calibration SQL execution.

Then run a dossier-first smoke test on a small known parcel set once `parcel-dossier` is introduced.

## What Should Not Be Redone

- Reworking Sprint 5 rule/feature contracts unless a concrete defect is found.
- Re-implementing Sprint 5 ingestion/context foundations before dossier/report requirements prove a gap.
- Reframing calibration artifacts as model training; Sprint 6 should remain transparent and rules-driven.

## Sprint 6 Success Definition

Sprint 6 should make Sprint 5 scoring operationally usable for ongoing investigations:

- an analyst can open one parcel and inspect the full evidence chain without direct SQL
- review outcomes can be captured and fed back into future threshold/exclusion tuning
- the interface and docs clearly distinguish triage signal from proof
