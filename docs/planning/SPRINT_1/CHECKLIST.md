# Sprint 1 Acceptance Checklist

## Purpose

Use this checklist to determine whether Sprint 1 is complete and ready to hand off to Sprint 2.

Sprint 1 target:

- Harden the existing McFarland ingestion and parsing pipeline so downstream analysis can rely on it.

## Acceptance Criteria

### 1. Migration Control Is In Place

- A managed migration framework exists in the repository.
- The current schema is represented by an explicit baseline migration.
- A fresh database can be initialized by applying migrations, not only by ORM `create_all`.
- Schema changes needed during Sprint 1 are recorded as migration revisions.
- The migration workflow is documented for contributors.

Pass condition:

- A contributor can start with an empty database and reach the current schema using the migration tool alone.

### 2. The Current Schema Is Fully Accounted For

- All existing core tables are included in migration-managed schema creation:
  - `parcels`
  - `fetches`
  - `assessments`
  - `taxes`
  - `payments`
  - `parcel_summaries`
- Existing required indexes and uniqueness guarantees are preserved.
- The schema no longer depends on hidden runtime `ALTER TABLE` behavior for normal setup.

Pass condition:

- The migrated schema matches the intended application schema and supports the current CLI commands.

### 3. Parser Regression Tests Exist

- A `tests/` suite exists in the repository.
- Representative raw HTML fixtures are covered by automated tests.
- Tests verify at least:
  - parcel summary extraction
  - assessment parsing
  - tax parsing
  - payment parsing
  - typed assessment normalization
- Tests include at least one sparse or irregular page case.

Pass condition:

- Parser regressions in common parse paths are likely to be caught automatically.

### 4. Data Normalization Is Tested

- Date parsing is covered by tests.
- Currency and decimal normalization are covered by tests.
- Basic malformed or empty value handling is covered by tests.
- Any new `parcel_year_facts` transform logic has automated validation.

Pass condition:

- Core normalization rules can be changed safely with automated feedback.

### 5. `parcel_year_facts` v1 Exists

- A canonical parcel-year data product exists as a table, materialized view, or documented rebuildable derived table.
- Its grain is one row per parcel per year.
- It is built from the current local source tables:
  - `assessments`
  - `taxes`
  - `payments`
  - `parcel_summaries`
- Merge rules for multiple source rows per parcel-year are documented or encoded clearly.

Pass condition:

- Analysts can query one normalized parcel-year row without stitching together multiple raw tables manually.

### 6. Data-Quality Checks Exist

- There are checks for duplicate parcel summaries.
- There are checks for invalid or suspicious dates.
- There are checks for impossible numeric values or obviously bad ranges.
- There are checks for fetch/parse completeness and consistency.
- The checks produce structured output suitable for repeatable use.

Pass condition:

- The repository can surface likely data-integrity problems without manual SQL investigation.

### 7. Profiling / Audit Reporting Exists

- A profiling command exists, such as `accessdane profile-data`.
- It reports useful coverage metrics, including at minimum:
  - parcel count
  - fetch count
  - successful fetch count
  - parsed record counts
  - parse error count
  - missing-section counts
  - `parcel_year_facts` coverage
- The output includes ratios or percentages where useful.

Pass condition:

- A user can quickly assess the health of the local McFarland dataset from one command.

### 8. End-To-End Rebuild Works

- A full rebuild from the existing raw HTML corpus can be run against a clean database.
- Parsing completes without systemic regressions.
- `parcel_year_facts` can be rebuilt after parsing.
- The profiling command runs successfully on the rebuilt database.

Pass condition:

- Sprint 1 functionality works together as one operational path, not only as isolated pieces.

### 9. Documentation Is Good Enough For Reuse

- The schema migration workflow is documented.
- The local rebuild workflow is documented.
- Operational guidance exists for rebuilding `parcel_year_facts` and using `check-data-quality` / `profile-data`.
- The test workflow is documented.
- The profiling workflow is documented.
- Known Sprint 1 limitations and Sprint 2 follow-ups are listed.

Pass condition:

- Another contributor can continue the project without reverse-engineering the new workflow.

## Recommended Review Sequence

Review Sprint 1 in this order:

1. Confirm migrations can build the schema from empty.
2. Run the automated tests.
3. Rebuild the derived parcel-year data.
4. Run the profiling command.
5. Review documentation and remaining known gaps.

## Non-Goals For Sprint 1

Sprint 1 should not be considered failed if these are still unfinished:

- External sales ingestion.
- Permit or appeal imports.
- Fraud scoring.
- Comparable-sales logic.
- Dashboard work.

These belong to later sprints.
