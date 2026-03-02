# Sprint 1 Closeout

Closed on: 2026-03-01

## Outcome

Sprint 1 is complete enough to hand off into Sprint 2.

The core Sprint 1 objective was met:

- harden the existing McFarland ingestion and parsing pipeline so downstream analysis can rely on it

## What Was Delivered

Sprint 1 delivered these concrete capabilities:

- Alembic-managed schema control with baseline and forward revisions
- migration-backed `init-db`
- parser regression coverage built on real raw HTML fixtures
- normalization tests for money, decimals, dates, label variants, and year extraction
- `parcel_year_facts` v1 as a rebuildable derived table
- `build-parcel-year-facts` and optional `run-all --build-parcel-year-facts`
- structured `check-data-quality` reporting
- structured `profile-data` reporting
- contributor bootstrap and operational runbooks

## Final Validation Snapshot

Final acceptance evidence from Day 13:

- automated tests: `36 passed in 17.50s`
- full `parcel_year_facts` rebuild: `58,684` rows
- full profile: local artifact `data/sprint1_acceptance_profile.json`
- full quality report: local artifact `data/sprint1_acceptance_quality.json`

Key current metrics:

- `2,657` parcels
- `2,657` successful fetches
- `2,657` parsed fetches
- `0` parse errors
- `58,684` source parcel-years
- `58,684` `parcel_year_facts` rows
- `parcel_year_fact_source_year_rate = 1.0`

Current non-blocking findings:

- `338` `suspicious_assessment_dates` issues remain in the quality report
- `7` successful fetches have no assessments
- `42` successful fetches have no tax rows
- `7` successful fetches have no payment rows

These are tracked as Sprint 2 follow-up work, not Sprint 1 blockers.

## Operational Reality

The implementation is stable, but one operational constraint is now explicit:

- full-corpus `run-all --parse-only --reparse` is long-running on the current local dataset

That does not block Sprint 1 acceptance.
It does mean the preferred workflow is:

- use full refreshes for longer maintenance runs
- use scoped `parse`, `build-parcel-year-facts --ids`, `check-data-quality --ids`, and `profile-data --ids` for fast iteration

## Handoff Artifacts

Sprint 1 handoff is anchored by these documents:

- [ACCEPTANCE_REVIEW.md](ACCEPTANCE_REVIEW.md)
- [OPERATIONS.md](OPERATIONS.md)
- [PARCEL_YEAR_FACTS_V1.md](PARCEL_YEAR_FACTS_V1.md)
- [CHECKLIST.md](CHECKLIST.md)
- [HANDOFF.md](../SPRINT_2/HANDOFF.md)

## Recommendation

Do not reopen Sprint 1 for more feature work.

Sprint 2 should start from the current green baseline and focus on:

- resolving the highest-value data-quality findings
- improving full-refresh ergonomics
- expanding data completeness and downstream analysis readiness
