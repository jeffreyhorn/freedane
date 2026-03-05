# Sprint 4 Plan

Prepared on: 2026-03-05

## Sprint Goal

Sprint 4 should add non-sale context needed to distinguish suspicious assessment behavior from legitimate valuation change.
The primary product is a repeatable permits and appeals ingest workflow, integrated into parcel-year facts and operator-facing timelines, with enough resilience to handle messy records-request or manually curated inputs.

This plan is based on:

- [docs/planning/devplan.md](../devplan.md), especially Sprint 4 scope for permits, appeals, templates, and parcel-year integration
- [docs/planning/SPRINT_4/HANDOFF.md](HANDOFF.md), especially baseline validation and carryover priorities around review workflow durability and analysis-ready outputs

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

- `sprint4/day-01-baseline`
- `sprint4/day-02-permits-schema`
- `sprint4/day-03-permits-migration`
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

### Day 1 (`6h`): Re-Establish Sprint 4 Baseline And Lock Scope

Focus:

- Start Sprint 4 from a known-good Sprint 3 baseline and capture acceptance metrics before new ingestion paths are added.

Planned work:

- Run the handoff validation pass: `make typecheck && make lint && make format && make test`.
- Run `.venv/bin/accessdane report-sales-matches --out data/sprint4_baseline_match_audit.json`.
- Confirm current schema and migration head state before adding new event tables.
- Convert Sprint 4 scope and handoff carryover priorities into an execution checklist.

Deliverables:

- `docs/planning/SPRINT_4/DAY_1_BASELINE.md`
- baseline match-audit artifact under `data/`

PR closeout:

- Commit baseline and checklist artifacts.
- Open a doc-and-baseline PR and merge after review.

### Day 2 (`6h`): Design Permit Import Contract And Normalization Rules

Focus:

- Define v1 permit ingestion inputs and normalization behavior with tolerant handling of messy records.

Planned work:

- Define expected permit CSV columns and optional fields.
- Define normalization for parcel number, dates, valuation/cost fields, and permit type/status.
- Define import status semantics, row-level validation outcomes, and idempotency behavior.
- Specify required indexes and dedupe keys for permit events.

Deliverables:

- `docs/planning/SPRINT_4/PERMIT_IMPORT_V1.md`

PR closeout:

- Commit permit import design doc.
- Open PR, address comments, merge.

### Day 3 (`6h`): Implement Permit Event Schema And Migration

Focus:

- Add database structures for permit events and load tracking.

Planned work:

- Add SQLAlchemy model(s) for `permit_events`.
- Add Alembic migration and required indexes.
- Update migration tests to enforce schema and constraints.
- Keep schema aligned with Day 2 contract.

Deliverables:

- new permit model(s)
- Alembic revision
- migration test updates

PR closeout:

- Commit schema and test changes.
- Open PR, resolve review, merge.

### Day 4 (`5h`): Implement `accessdane ingest-permits --file ...`

Focus:

- Deliver first working permit import path from file to normalized events.

Planned work:

- Build CLI command and importer entry point.
- Parse and normalize permit rows with forgiving field handling.
- Record import outcomes and row-level status.
- Add fixtures and tests for valid rows and malformed-row handling.

Deliverables:

- `ingest-permits` CLI command
- permit importer implementation
- importer tests

PR closeout:

- Commit CLI, importer, and tests.
- Open PR, address review comments, merge.

### Day 5 (`5h`): Harden Permit Ingestion For Edge Cases And Ops

Focus:

- Make permit ingest robust for manual and inconsistent data sources.

Planned work:

- Add edge-case normalization paths for sparse, partial, and inconsistent rows.
- Improve importer error reporting and counters.
- Document operational usage and failure recovery.
- Extend tests for idempotent reloads and malformed-value tolerance.

Deliverables:

- hardened permit importer behavior
- updated ops notes/tests

PR closeout:

- Commit hardening changes and docs.
- Open PR, resolve review, merge.

### Day 6 (`6h`): Design Appeals Import Contract And Mapping

Focus:

- Define v1 appeals ingestion shape and normalization rules.

Planned work:

- Define expected appeals CSV template and canonical fields.
- Define normalization rules for hearing dates, outcomes, requested value, and decided value.
- Define parcel-linking strategy and fallback keys.
- Define import status semantics and dedupe behavior for appeals.

Deliverables:

- `docs/planning/SPRINT_4/APPEAL_IMPORT_V1.md`

PR closeout:

- Commit appeals import design doc.
- Open PR, address feedback, merge.

### Day 7 (`6h`): Implement Appeal Event Schema, Migration, And CLI Path

Focus:

- Add database and command foundations for appeals ingest.

Planned work:

- Add SQLAlchemy model(s) for `appeal_events`.
- Add Alembic migration and indexes.
- Implement `accessdane ingest-appeals --file ...` CLI command shell and importer path.
- Add migration and basic importer tests.

Deliverables:

- appeal model(s)
- Alembic revision
- `ingest-appeals` command + base importer path

PR closeout:

- Commit schema, CLI, and tests.
- Open PR, resolve review, merge.

### Day 8 (`6h`): Harden Appeals Import And Validation Coverage

Focus:

- Ensure appeals ingest handles realistic county/manual data issues.

Planned work:

- Add tolerant parsing and normalization for inconsistent appeals rows.
- Add import diagnostics and rejection reporting.
- Expand tests for malformed/partial rows and duplicate handling.
- Update docs for appeals ingest operations.

Deliverables:

- hardened appeals importer
- expanded test coverage
- updated operations documentation

PR closeout:

- Commit importer hardening, tests, and docs.
- Open PR, address review, merge.

### Day 9 (`6h`): Design Parcel-Year Integration For Permits And Appeals

Focus:

- Define how permit and appeal signals roll into `parcel_year_facts`.

Planned work:

- Define annual rollups for permit counts, estimated improvement totals, and recent-permit indicators.
- Define annual appeals rollups (counts, outcome mix, value change impact where present).
- Define null/unknown handling and backfill behavior.
- Specify rebuild and idempotency semantics for parcel-year enrichment.

Deliverables:

- `docs/planning/SPRINT_4/PARCEL_YEAR_CONTEXT_INTEGRATION.md`

PR closeout:

- Commit integration design doc.
- Open PR, review, merge.

### Day 10 (`6h`): Implement Parcel-Year Permit/Appeal Rollups

Focus:

- Ship integration logic from event tables into parcel-year mart outputs.

Planned work:

- Implement permit rollup joins into `parcel_year_facts`.
- Implement appeals rollup joins into `parcel_year_facts`.
- Add/adjust rebuild path for enrichment refresh.
- Add tests validating rollups and scoped rebuild behavior.

Deliverables:

- parcel-year enrichment implementation
- integration tests

PR closeout:

- Commit rollup implementation and tests.
- Open PR, address review comments, merge.

### Day 11 (`6h`): Surface Context In Timelines And Analyst Outputs

Focus:

- Make permit/appeal context visible where analysts review parcel activity.

Planned work:

- Extend reporting outputs to include permit and appeal context signals.
- Add timeline-oriented summaries for parcel-level investigation.
- Add first-pass neighborhood/context aggregates needed by handoff priorities.
- Add tests for reporting shape and coverage.

Deliverables:

- updated reporting output(s) with context features
- tests for timeline/context surfacing

PR closeout:

- Commit reporting updates and tests.
- Open PR, resolve review, merge.

### Day 12 (`6h`): Build Reusable CSV Templates For Manual Curation

Focus:

- Deliver practical templates that support records-request and manual-entry workflows.

Planned work:

- Create permit and appeal CSV templates with canonical headers and notes.
- Add validation guidance and accepted-value examples.
- Document manual curation workflow and ingest expectations.
- Add lightweight tests/fixtures proving template compatibility.

Deliverables:

- `docs/templates/permit_events_template.csv`
- `docs/templates/appeal_events_template.csv`
- template usage notes

PR closeout:

- Commit templates and docs/tests.
- Open PR, review, merge.

### Day 13 (`5h`): Add Optional Geometry/Point Support Guardrails

Focus:

- Add future-friendly spatial support without hard-requiring PostGIS.

Planned work:

- Add optional schema fields for parcel geometry or point coordinates.
- Gate PostGIS-specific behavior behind availability checks.
- Keep non-geometry environments fully functional.
- Add tests/docs covering both geometry-present and geometry-absent modes.

Deliverables:

- optional spatial support implementation
- compatibility tests/docs

PR closeout:

- Commit spatial guardrail changes.
- Open PR, address review, merge.

### Day 14 (`5h`): Sprint 4 Acceptance Pack And Sprint 5 Handoff

Focus:

- Close Sprint 4 with validated operators docs, acceptance evidence, and next-step handoff.

Planned work:

- Run targeted Sprint 4 validation set and verify importer resilience goals.
- Document acceptance results against Sprint 4 criteria.
- Write/update operations docs for permit/appeal ingestion and parcel-year integration.
- Draft Sprint 5 handoff with open risks and follow-up priorities.

Deliverables:

- `docs/planning/SPRINT_4/ACCEPTANCE_REVIEW.md`
- updated Sprint 4 operations documentation
- `docs/planning/SPRINT_5/HANDOFF.md` draft

PR closeout:

- Commit acceptance artifacts and final docs.
- Open final Sprint 4 PR, resolve review comments, merge.

## Planned Effort Summary

- Days 1-5: `28h` on permit import path
- Days 6-8: `18h` on appeals import path
- Days 9-11: `18h` on parcel-year integration and context surfacing
- Days 12-14: `16h` on templates, docs, optional geometry handling, and edge-case hardening

This preserves the `80h` Sprint 4 estimate from the development plan while structuring delivery as 14 PR-driven daily increments from `main`.
