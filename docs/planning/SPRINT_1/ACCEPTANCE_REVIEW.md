# Sprint 1 Acceptance Review

Reviewed on: 2026-03-01

## Overall Status

Sprint 1 is acceptable to close with a small operational caveat.

The core Sprint 1 deliverables are present:

- migration-managed schema control
- parser and normalization tests
- `parcel_year_facts` v1
- structured data-quality checks
- `profile-data` audit reporting
- contributor and operations documentation

The remaining caveat is operational, not structural:

- a full-corpus `run-all --parse-only --reparse` pass is long-running on the current local dataset, so scoped commands remain the recommended path for fast iterative validation

## Validation Evidence

Acceptance checks run during this review:

- `.venv/bin/python -m pytest`
- `.venv/bin/accessdane check-data-quality --out data/sprint1_acceptance_quality.json`
- `.venv/bin/accessdane build-parcel-year-facts`
- `.venv/bin/accessdane profile-data --out data/sprint1_acceptance_profile.json`

Observed results:

- automated tests: `36 passed in 17.50s`
- full `parcel_year_facts` rebuild: `58,684` rows built
- profile snapshot: local artifact `data/sprint1_acceptance_profile.json`
- quality snapshot: local artifact `data/sprint1_acceptance_quality.json`

Current profile highlights:

- `2,657` parcels
- `2,657` fetches
- `2,657` successful fetches
- `2,657` parsed fetches
- `0` parse errors
- `60,284` assessments
- `108,188` taxes
- `399,295` payments
- `58,684` `parcel_year_facts` rows
- `58,684` source parcel-years
- `parcel_year_fact_source_year_rate = 1.0`

Current quality highlights:

- duplicate parcel summaries: `0` issues
- suspicious assessment dates: `338` issues
- impossible numeric values: `0` issues
- fetch/parse consistency: `0` issues

The current quality failures are all data findings from the `suspicious_assessment_dates` check, not execution failures in the pipeline.

## Acceptance Criteria Status

### 1. Migration Control Is In Place

Status: Pass

Evidence:

- Alembic is configured and used by `accessdane init-db`.
- The schema has explicit revisions through `20260301_0003`.
- Migration workflow is documented in [README.md](../../../README.md) and [OPERATIONS.md](OPERATIONS.md).

### 2. The Current Schema Is Fully Accounted For

Status: Pass

Evidence:

- Core tables are migration-managed.
- Follow-up revisions cover the fetch parse-queue index and `parcel_year_facts`.
- Migration tests validate empty-database setup and idempotent initialization.

### 3. Parser Regression Tests Exist

Status: Pass

Evidence:

- The repo has a `tests/` suite with real HTML fixtures.
- Tests cover parcel summary, assessment, tax, payment, sparse pages, and edge-case fixture shapes.

### 4. Data Normalization Is Tested

Status: Pass

Evidence:

- Tests cover dates, currency, decimals, malformed values, `_extract_year`, label normalization, and `parcel_year_facts` transform behavior.

### 5. `parcel_year_facts` v1 Exists

Status: Pass

Evidence:

- `parcel_year_facts` is implemented as a rebuildable derived table with a migration-managed schema.
- The full rebuild completed in this review and matched the source parcel-year universe exactly.

### 6. Data-Quality Checks Exist

Status: Pass

Evidence:

- `accessdane check-data-quality` exists and emits structured JSON.
- It currently checks duplicate summaries, suspicious dates, impossible numeric values, and fetch/parse consistency.

Notes:

- The latest full-dataset run still reports `338` suspicious assessment date findings.
- This does not fail the acceptance criterion, because the criterion is about the existence and utility of the checks.

### 7. Profiling / Audit Reporting Exists

Status: Pass

Evidence:

- `accessdane profile-data` exists and reports counts, missing sections, and coverage ratios.
- The latest full-dataset profile completed successfully and shows `1.0` source-year coverage for `parcel_year_facts`.

### 8. End-To-End Rebuild Works

Status: Pass with operational caveat

Evidence:

- The current local dataset has `2,657` successful fetches and `2,657` parsed fetches with `0` parse errors.
- A full `parcel_year_facts` rebuild and full `profile-data` run completed successfully during this review.
- Scoped end-to-end parse, facts rebuild, quality, and profile flows were validated on Day 11.

Caveat:

- A full-corpus `run-all --parse-only --reparse` pass is long-running in the current interactive workflow, so it was not rerun to completion during this acceptance pass.
- The documented workaround is to use scoped `parse` and `--ids` workflows for fast validation, and reserve full refreshes for longer-running maintenance runs.

### 9. Documentation Is Good Enough For Reuse

Status: Pass

Evidence:

- [README.md](../../../README.md) now documents bootstrap, migrations, tests, rebuild, quality, and profiling.
- [OPERATIONS.md](OPERATIONS.md) provides the operational runbook.
- [PARCEL_YEAR_FACTS_V1.md](PARCEL_YEAR_FACTS_V1.md) documents the mart design and merge rules.

## Sprint 2 Deferrals

These are the main items that should carry forward:

- Refine `suspicious_assessment_dates` further so the remaining `338` findings are better separated into real anomalies vs expected historical carry-forward behavior.
- Improve long-running operational paths, especially the ergonomics of full-corpus `run-all --parse-only --reparse`.
- Consider making `run-all` smarter about scoped downstream steps so small parser iterations do not trigger broad anomaly-style work.
- Expand data-quality checks beyond the first-pass integrity rules added in Sprint 1.
- Add deeper validation or diagnostics for the `7` successful fetches missing assessments, `42` missing tax rows, and `7` missing payment rows.

## Recommendation

Sprint 1 should be treated as complete enough to hand off into Sprint 2 planning.

The implementation is stable, the test suite is green, the derived parcel-year table rebuilds successfully across the current corpus, and the repo now has enough operational guidance for another contributor to continue without reverse-engineering the workflow.
