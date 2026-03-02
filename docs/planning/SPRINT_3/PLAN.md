# Sprint 3 Plan

Prepared on: 2026-03-02

## Sprint Goal

Sprint 3 should deliver the first usable market-sales layer on top of the current AccessDane extraction stack.
The primary product is a repeatable Wisconsin DOR RETR import and sale-to-parcel matching workflow, while also closing the highest-value Sprint 2 operational gaps that directly affect maintainability and trust in the new data.

This plan is based on:

- [docs/planning/devplan.md](../devplan.md), especially the Sprint 3 goal to ingest and match market sales
- [docs/planning/SPRINT_3/HANDOFF.md](HANDOFF.md), especially the carryover priorities around rebuild control, parse ergonomics, profiling, and quality

Total planned effort: `80` hours.

## Planning Constraints

- All 14 days in the two-week sprint have scheduled work.
- No single day exceeds `10` hours.
- Each day is executed on its own short-lived branch created from current `main`.
- Each day ends with the same delivery loop:
  - commit the day’s changes on that day’s branch
  - open a Pull Request from that day’s branch into `main`
  - review the PR
  - make any follow-up fixes required by that review
  - hand the PR off for merge, which marks the day complete
  - start the next day from a fresh branch created from the newly updated `main`

## Standard Daily Closeout

Reserve the last `1` hour of each day for the PR loop unless the day says otherwise.

Recommended branch naming pattern:

- `sprint3/day-01-baseline`
- `sprint3/day-02-rebuild-design`
- `sprint3/day-03-characteristics-rebuild`
- continue the same pattern through Day 14

Standard daily branch workflow:

1. Update local `main` to the latest merged state.
2. Create a fresh branch for that day’s scoped work.
3. Do only that day’s planned changes on the branch.
4. Open the PR from that branch back into `main`.
5. After review fixes are merged, delete the branch and begin the next day from `main`.

Daily closeout sequence:

1. Run the day’s targeted validation.
2. Commit only the intended changes for that day on the day-specific branch.
3. Open a PR into `main` with a focused description of scope, tests run, and known limits.
4. Review the PR and address review comments on the same branch that same day.
5. Hand the PR off for merge after the fixes are in.
6. After merge, delete the day branch and treat that merge as the day’s completion point.

## Day-By-Day Plan

### Day 1 (`6h`): Re-establish Sprint 3 Baseline And Scope The First Increment

Focus:

- Re-run the Sprint 3 handoff baseline commands.
- Confirm the current schema, tests, and scoped acceptance set still behave as expected.
- Convert the handoff priorities into an execution checklist for the first four days.

Planned work:

- Run `.venv/bin/python -m pytest` and `.venv/bin/accessdane init-db`.
- Run the Sprint 2 scoped acceptance set from the Sprint 3 handoff.
- Capture the current baseline metrics for `parcel_characteristics`, `parcel_lineage_links`, and richer tax-detail fields.
- Record any drift from the Sprint 3 handoff assumptions.

Deliverables:

- `docs/planning/SPRINT_3/DAY_1_BASELINE.md`
- refreshed local baseline artifacts under `data/` as needed

PR closeout:

- Commit the baseline note and any doc-only clarifications.
- Open a PR for the baseline refresh and merge after review.

### Day 2 (`6h`): Design Rebuild Paths For Sprint 2 Derived Outputs

Focus:

- Define the concrete rebuild behavior for `parcel_characteristics` and `parcel_lineage_links`.

Planned work:

- Specify table ownership and replacement rules for both outputs.
- Decide whether rebuilds should be parcel-scoped, full-table, or both.
- Define CLI command shape, input options, and operational semantics.
- Define idempotency expectations and interaction with existing parse-time upserts.

Deliverables:

- `docs/planning/SPRINT_3/REBUILD_DESIGN.md`

PR closeout:

- Commit the rebuild design spec.
- Open a PR, review it, address feedback, and hand it off for merge.

### Day 3 (`6h`): Implement `parcel_characteristics` Rebuild Path

Focus:

- Make `parcel_characteristics` rebuildable without relying on parse-time side effects.

Planned work:

- Add a dedicated rebuild function for `parcel_characteristics`.
- Add a CLI entry point or subcommand option for full and scoped rebuilds.
- Ensure rebuild logic reuses existing extraction logic instead of forking it.
- Add tests for full rebuild and parcel-scoped rebuild behavior.

Deliverables:

- rebuild implementation
- tests for `parcel_characteristics` rebuilds

PR closeout:

- Commit the code and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 4 (`6h`): Implement `parcel_lineage_links` Rebuild Path

Focus:

- Make lineage links rebuildable independently from reparse runs.

Planned work:

- Add a dedicated rebuild function for `parcel_lineage_links`.
- Support full rebuild and parcel-scoped rebuilds.
- Preserve normalized dedupe behavior and latest-fetch metadata rules.
- Add tests for rebuild correctness and scoped replacement behavior.

Deliverables:

- rebuild implementation
- tests for `parcel_lineage_links` rebuilds

PR closeout:

- Commit the code and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 5 (`6h`): Improve Long-Running Parse Ergonomics

Focus:

- Reduce the operational friction called out in the Sprint 3 handoff for large reparses.

Planned work:

- Review the current `parse --reparse` behavior against the handoff goals.
- Add one or two high-value ergonomics improvements such as progress visibility, bounded batch commits, resumable behavior, or clearer failure reporting.
- Update operations docs to reflect the improved behavior.
- Add regression coverage for the new operational path.

Deliverables:

- parser-ergonomics improvement
- updated docs/tests

PR closeout:

- Commit the code, docs, and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 6 (`6h`): Extend Profiling For Sprint 2 Outputs

Focus:

- Make the new Sprint 2 outputs measurable before building market-sales joins on top of them.

Planned work:

- Extend `profile-data` to report `parcel_characteristics` coverage.
- Add lineage coverage and richer tax-detail field presence metrics.
- Add filtered-scope assertions and output-shape tests.
- Update README or operations docs with the new profile fields.

Deliverables:

- expanded profiling output
- tests for the new profiling metrics

PR closeout:

- Commit the code, docs, and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 7 (`5h`): Add Quality Checks For The New Extraction Layers

Focus:

- Tighten trust in the new Sprint 2 outputs before introducing sales matching.

Planned work:

- Add quality checks around exempt-style page classification.
- Add lineage consistency checks.
- Add a check for placeholder payment-history semantics where useful.
- Add focused tests and brief documentation.

Deliverables:

- expanded `check-data-quality`
- tests and notes for the new checks

PR closeout:

- Commit the code, docs, and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 8 (`6h`): Define RETR Import Schema And Import Rules

Focus:

- Start the core Sprint 3 build from `devplan.md`: ingest market sales.

Planned work:

- Define stable schemas for `sales_transactions`, `sales_parcel_matches`, and `sales_exclusions`.
- Define the expected Wisconsin DOR RETR CSV columns and normalization rules.
- Define load metadata, rejection handling, and repeat-load behavior.
- Define which fields are required for v1 and which can remain nullable.

Deliverables:

- `docs/planning/SPRINT_3/RETR_IMPORT_V1.md`

PR closeout:

- Commit the schema/import design doc.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 9 (`6h`): Implement Sales Schema And Migration

Focus:

- Add the database foundation for RETR ingestion.

Planned work:

- Add SQLAlchemy models for `sales_transactions`, `sales_parcel_matches`, and `sales_exclusions`.
- Add the Alembic migration for the new tables and initial indexes.
- Update migration tests to enforce the new schema.
- Keep the schema aligned with the Day 8 design.

Deliverables:

- new models
- Alembic revision
- migration tests

PR closeout:

- Commit the schema changes and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 10 (`6h`): Implement `accessdane ingest-retr --file ...`

Focus:

- Deliver the first working RETR import path.

Planned work:

- Build the CLI command to load a RETR CSV file.
- Normalize core transaction fields on ingest.
- Record load outcomes and row-level import status.
- Add fixtures and tests for successful import plus malformed-row handling.

Deliverables:

- `ingest-retr` CLI
- importer implementation
- importer tests

PR closeout:

- Commit the CLI, importer, and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 11 (`6h`): Add Exclusion Logic For Non-Comparable Sales

Focus:

- Make imported sales analytically usable instead of treating every transfer as comparable.

Planned work:

- Normalize initial exclusion rules for non-arms-length and clearly non-comparable transfers.
- Persist explicit exclusion reasons in `sales_exclusions`.
- Keep excluded transactions visible for auditability rather than dropping them.
- Add tests for common exclusion cases and documentation for the initial rule set.

Deliverables:

- v1 exclusion logic
- `sales_exclusions` population path
- tests and docs

PR closeout:

- Commit the exclusion logic and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 12 (`5h`): Implement Exact And Normalized Address Matching

Focus:

- Build the first two matching tiers from the Sprint 3 dev plan.

Planned work:

- Implement exact official parcel number matching where the RETR row supports it.
- Implement normalized address matching as the second pass.
- Write confidence tiers for those match methods.
- Add tests for exact matches, address matches, and controlled non-matches.

Deliverables:

- first-pass matcher
- confidence scoring for direct matches
- tests

PR closeout:

- Commit the matching logic and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 13 (`5h`): Implement Legal-Description Fallback And Review Queue

Focus:

- Complete the v1 multi-step matcher with explicit handling for uncertainty.

Planned work:

- Add legal-description fallback heuristics.
- Route unresolved or low-confidence cases into a review queue state instead of silently linking them.
- Add audit output for ambiguous cases.
- Add tests for fallback matches and manual-review routing.

Deliverables:

- legal-description fallback
- manual review queue behavior
- ambiguity audit coverage

PR closeout:

- Commit the matcher extension and tests.
- Open a PR, review it, fix comments, and hand it off for merge.

### Day 14 (`5h`): Finalize Match Audit Reporting, Docs, And Sprint Acceptance Pack

Focus:

- Close the sprint with an operator-usable first release of market-sales ingest and matching.

Planned work:

- Build a match-quality report with confidence tiers and unresolved-case counts.
- Document how to export Wisconsin DOR RETR data and load it with `ingest-retr`.
- Run the targeted Sprint 3 validation set.
- Write the Sprint 3 acceptance review and Sprint 4 handoff draft.

Deliverables:

- match audit report
- RETR import documentation
- `docs/planning/SPRINT_3/ACCEPTANCE_REVIEW.md`
- `docs/planning/SPRINT_4/HANDOFF.md` draft

PR closeout:

- Commit the closeout docs, report path, and any final fixes.
- Open the final PR, review it, fix comments, and hand it off for merge to complete Sprint 3.

## Planned Effort Summary

- Days 1-7: `41h` on Sprint 2 carryover work that directly supports Sprint 3 stability
- Days 8-14: `39h` on RETR ingestion, sales matching, exclusion logic, auditability, and Sprint 3 closeout

This preserves the `80h` sprint total from the development plan while ensuring Sprint 3 starts from a maintainable operational baseline instead of layering market-sale ingestion onto unresolved Sprint 2 workflow gaps.
