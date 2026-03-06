# Sprint 4 Acceptance Review

Reviewed on: 2026-03-06

## Overall Status

Sprint 4 is acceptable to close.

Sprint 4 delivered the planned permits/appeals context layer and integration outputs:

- migration-managed `permit_events` and `appeal_events` ingestion paths
- manual-curation CSV templates for permits and appeals
- parcel-year integration fields and rebuild logic for permit/appeal context
- optional spatial support guardrails (`point_only` vs `geometry_postgis`)
- operator-facing documentation for permit/appeal import and parcel-year refresh workflows

## Validation Evidence

Targeted Sprint 4 closeout checks run:

- `.venv/bin/python -m pytest tests/test_permits.py tests/test_appeals.py tests/test_parcel_year_facts.py tests/test_migrations.py tests/test_spatial.py tests/test_cli.py -n auto -m "not slow"`
- `DATABASE_URL=sqlite:////tmp/freedane_day14_validation.sqlite .venv/bin/accessdane init-db`
- `DATABASE_URL=sqlite:////tmp/freedane_day14_validation.sqlite .venv/bin/accessdane ingest-permits --file docs/templates/permit_events_template.csv`
- `DATABASE_URL=sqlite:////tmp/freedane_day14_validation.sqlite .venv/bin/accessdane ingest-appeals --file docs/templates/appeal_events_template.csv`
- `DATABASE_URL=sqlite:////tmp/freedane_day14_validation.sqlite .venv/bin/accessdane build-parcel-year-facts`
- `DATABASE_URL=sqlite:////tmp/freedane_day14_validation.sqlite .venv/bin/accessdane spatial-support`

Observed results:

- targeted Sprint 4 test set: `67 passed in 24.16s`
- migration path on empty SQLite DB upgraded cleanly through `20260307_0010`
- template-ingest command compatibility confirmed:
  - permit import summary: `total=0 loaded=0 rejected=0 inserted=0 updated=0`
  - appeal import summary: `total=0 loaded=0 rejected=0 inserted=0 updated=0`
- parcel-year rebuild completed cleanly on the validation DB: `parcel_year_facts rows built: 0`
- spatial guardrail behavior on SQLite runtime: `mode = point_only`

## Acceptance Criteria Status

### 1. A Permit Or Appeal Record Can Be Attached And Surfaced In Parcel Timelines

Status: Pass

Evidence:

- Permit linkage coverage is exercised in permit importer tests, including exact parcel-number, crosswalk, and normalized-address paths.
- Parcel-year integration tests verify permit/appeal rollups and year-only rows (including rows with no assessment/tax/payment source rows).
- Rebuild behavior is tested for both full and scoped parcel-year rebuilds.

### 2. Missing Or Messy Manual Data Does Not Break The Pipeline

Status: Pass

Evidence:

- Permit and appeal test coverage includes sparse rows, null-token normalization, row-shape warnings, unparseable temporal values, and idempotent same-file upserts.
- Rejected rows are preserved with row-level diagnostics instead of being dropped.
- Header-only template files are ingest-compatible and execute without importer failure.

### 3. The Data Model Supports Future Records Requests Without Redesign

Status: Pass

Evidence:

- Sprint 4 added explicit permit/appeal event tables with file-level provenance, normalized matching fields, and idempotent dedupe keys.
- Parcel-year context columns are now migration-managed and rebuildable from source tables.
- Optional spatial columns and runtime capability gating allow future geometry expansion without forcing PostGIS adoption immediately.

### 4. Non-Geometry Environments Remain Fully Functional

Status: Pass

Evidence:

- Runtime spatial mode detection explicitly reports `point_only` mode for non-PostgreSQL backends.
- Spatial capability tests cover both geometry-absent and geometry-present modes.
- Normal import and rebuild commands continue to operate without PostGIS.

### 5. Sprint 4 Operator Documentation Is Present

Status: Pass

Evidence:

- Permit and appeal operational runbooks are available.
- A consolidated Sprint 4 operations runbook now documents permit/appeal ingest plus parcel-year integration and spatial checks.
- Template usage and day-level design docs are in place for reproducible operator workflows.

## Remaining Limits / Sprint 5 Inputs

- Appeals still rely on source locator quality; unresolved parcel linkage remains a practical source of context loss in parcel-year rollups.
- Spatial support is currently guardrail-only; geometry analytics are intentionally deferred.
- Sprint 4 added context enrichment, but it did not yet add the scoring/rules engine needed for explainable fraud-signal ranking.

## Recommendation

Close Sprint 4 and begin Sprint 5 using the handoff draft in `docs/planning/SPRINT_5/HANDOFF.md`.

Sprint 4 scope is implemented, validated against targeted acceptance checks, and documented well enough for next-sprint feature work to proceed without rediscovery.
