# Sprint 2 Day-By-Day Plan

## Sprint Objective

Expand the AccessDane extraction layer so the project captures more parcel characteristics and local context before external matching begins, while carrying forward the highest-value operational and data-quality follow-up work from Sprint 1.

Sprint 2 scope from the development plan:

- Extend `parse.py` to capture additional parcel-detail fields and normalize them into `parcel_characteristics`.
- Capture parcel lineage if parent/child parcel history is present.
- Normalize richer tax detail fields, including exemptions or special charge indicators when available.
- Evaluate whether the AccessDane "Summary Report" / "Custom Report" endpoints provide richer structured data, and, if useful, add an optional fetch path.
- Add parser fixtures specifically for edge-case parcels: condos, vacant land, split lots, exempt parcels, and parcels with sparse tax history.

Sprint 2 carryover from the handoff:

- Refine the remaining `suspicious_assessment_dates` findings so the check is less noisy.
- Investigate known missing-section parcels before treating parser gaps as solved.
- Improve full-refresh ergonomics and scoped iteration behavior.
- Expand the first-pass quality checks where it helps validate the new extraction work.

Constraints for this plan:

- 14 consecutive work days.
- Every day has planned work.
- No single day exceeds 10 hours.
- Total planned effort is 80 hours.

## Hour Summary

- Week 1 total: 40 hours
- Week 2 total: 40 hours
- Sprint total: 80 hours

## Day 1 (6 hours)

Focus:

Re-establish the Sprint 1 baseline and turn the handoff into a concrete Sprint 2 task list.

Work:

- Re-run the baseline validation sequence from the Sprint 2 handoff.
- Capture the current `check-data-quality` and `profile-data` outputs as Sprint 2 starting metrics.
- Review the remaining `338` `suspicious_assessment_dates` findings and identify the dominant patterns.
- Review the known missing-section counts (`7` missing assessments, `42` missing tax rows, `7` missing payment rows) and build an initial investigation list.

Outputs:

- Sprint 2 baseline metrics snapshot.
- Ranked issue list for carryover fixes and extraction priorities.

## Day 2 (6 hours)

Focus:

Reduce the highest-noise data-quality findings before broader parser changes begin.

Work:

- Refine the `suspicious_assessment_dates` logic using the dominant carry-forward patterns found on Day 1.
- Add targeted tests that lock in the new stale-date boundaries and any allowed historical patterns.
- Re-run quality checks on representative parcels and compare the issue count delta.
- Document any remaining date patterns that should stay flagged as true anomalies.

Outputs:

- Calibrated assessment-date quality rule.
- Updated tests and before/after issue-count notes.

## Day 3 (6 hours)

Focus:

Investigate current missing-section parcels so Sprint 2 parser work targets real gaps.

Work:

- Inspect representative parcels from the missing-assessment, missing-tax, and missing-payment sets.
- Classify whether the gaps are parser blind spots, true source omissions, or malformed/edge-case HTML.
- Add fixture notes for the most useful missing-section examples.
- Decide which gaps should be fixed during Sprint 2 parser expansion and which should remain explicit data limitations.

Outputs:

- Missing-section triage note.
- Prioritized list of parser gaps vs source-data limits.

## Day 4 (6 hours)

Focus:

Improve iteration ergonomics before the heavier extraction work.

Work:

- Refine `run-all` or related CLI behavior so scoped parser work does not trigger unnecessary broad downstream steps.
- Tighten the operational path for full refreshes vs scoped smoke runs.
- Add or update tests around the new CLI behavior.
- Update operational docs if command behavior changes.

Outputs:

- Faster, clearer scoped workflow for parser iteration.
- Stabilized CLI behavior for Sprint 2 development.

## Day 5 (6 hours)

Focus:

Define the schema and precedence rules for the new extraction outputs.

Work:

- Design the `parcel_characteristics` table, including grain, provenance, and nullability.
- Decide whether parcel lineage lives in the same table, a dedicated table, or both.
- Define the first characteristic set to target for residential parcels.
- Specify merge and overwrite rules when multiple fetches expose the same characteristic fields.

Outputs:

- Sprint 2 schema design note for `parcel_characteristics` and lineage handling.
- Initial field list and merge rules.

## Day 6 (5 hours)

Focus:

Add the new schema and migration scaffolding.

Work:

- Implement the SQLAlchemy models for `parcel_characteristics` and any lineage table needed.
- Add Alembic migrations for the new schema objects and indexes.
- Update migration tests to cover the new tables on empty-database setup.
- Confirm the schema remains cleanly buildable from `head`.

Outputs:

- Migration-managed Sprint 2 schema additions.
- Updated migration coverage.

## Day 7 (5 hours)

Focus:

Build the first stable characteristic extraction path.

Work:

- Extend parsing/storage to capture the initial stable property characteristics from existing AccessDane HTML.
- Link the extracted characteristics back to the originating fetch.
- Add focused tests for the first characteristic set using known residential fixtures.
- Confirm there are no regressions in current summary/assessment/tax/payment extraction.

Outputs:

- First working `parcel_characteristics` extraction path.
- Regression coverage for the initial field set.

## Day 8 (6 hours)

Focus:

Broaden characteristic coverage across common residential parcels.

Work:

- Expand parsing for additional parcel-detail labels and normalize them into typed or canonical fields.
- Handle label variants and sparse field presence without breaking current parses.
- Add fixture coverage for representative residential pages with different field shapes.
- Measure early field completeness against a small sample set.

Outputs:

- Broader residential characteristic coverage.
- Initial completeness notes for the characteristic fields.

## Day 9 (6 hours)

Focus:

Capture parcel lineage where AccessDane exposes parent/child history.

Work:

- Parse parcel lineage or history sections if present in current HTML pages.
- Normalize parent/child references into the chosen schema.
- Add fixtures and tests for parcels with split/merged or lineage-like histories.
- Document known limitations if lineage is inconsistently exposed.

Outputs:

- Working parcel-lineage extraction path or explicit no-data determination.
- Tests for lineage parsing behavior.

## Day 10 (6 hours)

Focus:

Normalize richer tax detail fields beyond the current summary-level shape.

Work:

- Extend tax parsing to capture exemptions, credits, specials, or special-charge indicators when they are available in the source HTML.
- Normalize the new fields into a predictable structure tied to the existing tax records or supporting table.
- Add tests for edge cases including exempt parcels and sparse tax histories.
- Confirm current tax totals and payment extraction do not regress.

Outputs:

- Richer tax-detail extraction.
- Regression coverage for the new tax fields.

## Day 11 (6 hours)

Focus:

Evaluate the optional AccessDane report endpoints.

Work:

- Inspect the "Summary Report" / "Custom Report" endpoints or flows to determine whether they provide materially better structured data.
- Compare the report output against the current HTML parser for field richness, stability, and fetch complexity.
- If the endpoint is clearly useful, implement a minimal optional fetch path behind an explicit CLI flag or setting.
- If it is not clearly useful, document the no-go decision and why.

Outputs:

- Optional report-fetch prototype or a documented rejection decision.
- Clear recommendation on whether Sprint 2 should depend on the report path.

## Day 12 (6 hours)

Focus:

Deepen fixture coverage across the edge-case parcel types called out in the dev plan.

Work:

- Add parser fixtures for condos, vacant land, split lots, exempt parcels, and sparse-tax parcels.
- Expand tests for `parcel_characteristics`, lineage, and richer tax fields across those cases.
- Revisit any missing-section parcels discovered earlier that now have a concrete fix.
- Make sure the new coverage still protects against regressions in the preexisting parse paths.

Outputs:

- Stronger edge-case parser regression suite.
- Updated list of unresolved parser blind spots.

## Day 13 (5 hours)

Focus:

Run completeness and profiling review for the new Sprint 2 extraction.

Work:

- Rebuild the new derived/normalized outputs against the current local dataset.
- Re-run `profile-data` and any Sprint 2-specific field completeness queries.
- Check whether the new characteristic set is available for most residential parcels.
- Review whether the remaining missing-section and quality findings have improved, worsened, or stayed flat.

Outputs:

- Sprint 2 completeness review.
- Baseline coverage metrics for the new fields.

## Day 14 (5 hours)

Focus:

Prepare the Sprint 2 acceptance package and next-step handoff.

Work:

- Re-run the full automated test suite.
- Review Sprint 2 deliverables against the development plan acceptance criteria.
- Document any deliberate deferrals, especially around report endpoints, lineage gaps, or low-coverage fields.
- Produce a concise Sprint 2 closeout note and identify what should move into Sprint 3.

Outputs:

- Sprint 2 acceptance review.
- Deferred-items list for Sprint 3.

## Expected End-Of-Sprint Deliverables

- `parcel_characteristics` implemented as a migration-managed normalized table.
- Parcel lineage extraction implemented or explicitly documented as unavailable/inconsistent.
- Richer tax-detail extraction for available exemptions or special indicators.
- Parser fixtures covering the new edge-case parcel types.
- Optional report-fetch path implemented, or a documented decision not to use it.
- Updated profiling and quality notes describing field completeness and remaining gaps.

## Sequence Rationale

Sprint 2 starts by using the Sprint 1 handoff data to reduce known noise and clarify current parser gaps before adding new extraction work. It then adds schema control before broadening parser logic, so the new normalized outputs stay migration-managed from the start. The optional report-fetch evaluation is intentionally delayed until after the HTML parser has been extended; that prevents the sprint from overcommitting to a parallel fetch path before the team knows whether the current source can already provide enough value. The final days are reserved for completeness review and acceptance packaging so Sprint 3 can start from a measured, documented baseline instead of another exploratory state.
