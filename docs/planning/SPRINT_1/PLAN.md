# Sprint 1 Day-By-Day Plan

## Sprint Objective

Harden the existing McFarland ingestion and parsing pipeline so it can support trustworthy downstream analysis.

Sprint 1 scope from the development plan:

- Add schema migration tooling.
- Convert the current schema into managed migrations.
- Add parser regression fixtures and an initial automated test suite.
- Add data-quality checks and a profiling command.
- Build `parcel_year_facts` v1.

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

Establish the Sprint 1 implementation baseline and define the schema migration target.

Work:

- Review the current SQLAlchemy models, `init_db` behavior, and CLI flows that read/write the database.
- Inventory all current tables, implicit constraints, and ad hoc schema mutations.
- Define the target migration strategy (recommended: Alembic with a single baseline migration plus forward-only revisions).
- Document the current schema and identify immediate cleanup issues that would block migration work.

Outputs:

- Short migration design note.
- Initial list of schema mismatches and risky areas.

## Day 2 (6 hours)

Focus:

Install and wire up migration tooling.

Work:

- Add the migration dependency and initialize the migration directory structure.
- Configure migration environment to point at the existing SQLAlchemy metadata.
- Create the baseline migration representing the current production schema.
- Verify the migration can create the schema from an empty database.

Outputs:

- Working migration framework in the repo.
- Baseline migration file.

## Day 3 (6 hours)

Focus:

Normalize schema behavior and remove hidden schema drift.

Work:

- Replace ad hoc schema mutation logic where practical with explicit migrations.
- Decide which runtime schema guards remain temporarily and which move fully into migrations.
- Add missing indexes and constraints needed for current tables if they are currently only implied.
- Run a clean rebuild against an empty local database and validate the resulting schema.

Outputs:

- Cleaner schema lifecycle.
- Follow-up migration(s) if needed after the baseline.

## Day 4 (6 hours)

Focus:

Build the test harness for parser and normalization work.

Work:

- Create the `tests/` structure and basic pytest configuration if needed.
- Add test helpers for loading representative raw HTML fixtures.
- Select a fixture sample from the current `data/raw/` set that covers common parse paths.
- Add first-pass tests for `parse_page` basic success and typed assessment field normalization.

Outputs:

- Initial automated test harness.
- First parser regression tests.

## Day 5 (6 hours)

Focus:

Expand parser regression coverage to reduce future breakage.

Work:

- Add fixtures covering common and edge-case parcel pages.
- Add tests for assessment, tax, payment, and parcel summary extraction.
- Add tests for empty or sparse sections, malformed values, and date parsing.
- Add tests around current anomaly-detection assumptions where appropriate.

Outputs:

- A representative parser regression suite.
- Clear list of parser gaps discovered during test writing.

## Day 6 (5 hours)

Focus:

Define the `parcel_year_facts` v1 model.

Work:

- Specify the grain: one row per parcel per year.
- Choose the initial field set sourced from assessments, taxes, payments, and parcel summary data.
- Define how to resolve conflicts when multiple fetches or multiple records exist for the same parcel-year.
- Decide whether v1 is a physical table, materialized view, or rebuildable derived table.

Outputs:

- `parcel_year_facts` v1 schema specification.
- Merge and precedence rules for source data.

## Day 7 (5 hours)

Focus:

Implement `parcel_year_facts` v1.

Work:

- Add the schema/migration for `parcel_year_facts`.
- Implement the transformation logic to populate or refresh it from current base tables.
- Validate the row counts and spot-check sample parcels for correctness.
- Note any source-data holes that require future extraction improvements.

Outputs:

- Working `parcel_year_facts` v1 build path.
- First validation notes.

## Day 8 (6 hours)

Focus:

Build data-quality checks for pipeline trustworthiness.

Work:

- Define checks for duplicate parcel summaries, bad dates, impossible numeric values, and fetch/parse inconsistencies.
- Implement reusable validation queries or Python checks.
- Add structured pass/fail output suitable for CLI use.
- Make sure failures are informative enough to drive debugging.

Outputs:

- First set of data-quality checks.
- Reusable validation logic for reporting.

## Day 9 (6 hours)

Focus:

Create the profiling and audit command.

Work:

- Add `accessdane profile-data` (or equivalent) to summarize coverage and quality.
- Report counts for parcels, fetches, parsed records, parse errors, missing sections, and `parcel_year_facts` coverage.
- Include key percentages so the output is useful for trend tracking across reruns.
- Ensure the command can run against the existing McFarland dataset without manual SQL.

Outputs:

- Working profiling command.
- Useful baseline output for the current data.

## Day 10 (6 hours)

Focus:

Strengthen automated tests around the new Sprint 1 additions.

Work:

- Add tests for migrations on an empty database.
- Add tests for `parcel_year_facts` build behavior.
- Add tests for data-quality rules and profiling output shape.
- Fix any brittleness uncovered in the CLI or transformation logic.

Outputs:

- Broader Sprint 1 test coverage.
- Stabilized implementation across schema, transforms, and CLI.

## Day 11 (6 hours)

Focus:

Run an end-to-end rebuild and reconcile results.

Work:

- Execute a full local rebuild using the current raw HTML corpus.
- Re-run parsing and `parcel_year_facts` generation against a clean database.
- Run the profiling command and capture baseline metrics.
- Investigate major mismatches, surprising null rates, or obvious regressions.

Outputs:

- End-to-end validation notes.
- Issue list for any defects found during rebuild.

## Day 12 (6 hours)

Focus:

Fix defects found in end-to-end validation and tighten documentation.

Work:

- Resolve the highest-priority problems found on Day 11.
- Adjust parsing, transforms, migrations, or checks as needed.
- Document the rebuild workflow and the intended Sprint 1 operational path.
- Make sure a fresh contributor can understand how to initialize, migrate, test, and profile the data.

Outputs:

- Corrected Sprint 1 implementation.
- Updated repo documentation for the new workflow.

## Day 13 (5 hours)

Focus:

Prepare Sprint 1 acceptance review.

Work:

- Re-run the full automated test suite.
- Re-run the data profile and confirm stable output after fixes.
- Check Sprint 1 deliverables against the dev plan acceptance criteria.
- Identify any deliberate deferrals that should move into Sprint 2.

Outputs:

- Acceptance checklist with pass/fail status.
- Deferred items list for Sprint 2.

## Day 14 (5 hours)

Focus:

Finalize, package, and hand off Sprint 1 results.

Work:

- Clean up small documentation gaps and naming inconsistencies.
- Produce a concise Sprint 1 summary: what was built, what metrics improved, and what remains risky.
- Confirm that migrations, tests, `parcel_year_facts`, and the profiling command are all in a usable state.
- Prepare the repo for Sprint 2 planning by documenting known parser/data gaps discovered during the sprint.

Outputs:

- Sprint 1 closeout note.
- Clear handoff into Sprint 2.

## Expected End-Of-Sprint Deliverables

- Managed database migrations in place.
- Initial automated parser and normalization tests.
- `parcel_year_facts` v1 implemented and rebuildable.
- Data-quality validation checks.
- `accessdane profile-data` (or equivalent) implemented.
- Rebuild and profiling documentation.

## Sequence Rationale

The sprint starts with migration control because reliable schema management is the foundation for every later table and importer. It then adds tests before substantial transformation work so parser and normalization changes are safer. `parcel_year_facts` is built only after the schema and test harness exist, and profiling comes after that so the new mart can be measured immediately. The final days are reserved for end-to-end validation, defect cleanup, and acceptance review instead of new feature work.
