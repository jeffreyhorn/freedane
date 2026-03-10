# Sprint 5 Calibration Notes (v1)

Prepared on: 2026-03-09

## Goal

Provide a repeatable, SQL-first threshold-tuning workflow for `score-fraud` outputs without changing core scoring logic.

Primary artifact:

- `docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql`

## Inputs

Calibration queries expect persisted Sprint 5 scoring outputs:

- `fraud_scores` rows for a target `(ruleset_version, feature_version)`
- `fraud_flags` rows tied to those scores
- `scoring_runs` metadata for run-level inventory

Default target versions in the SQL file:

- `ruleset_version = scoring_rules_v1`
- `feature_version = feature_v1`

## Operator Workflow

1. Run feature build and scoring for the target versions.
2. Open `CALIBRATION_SQL_V1.sql` and set target versions and threshold values once at the top of the file by editing the `\set ruleset_version`, `\set feature_version`, `\set high_min`, `\set medium_min`, and `\set window_size` variables (each `target` CTE references these).
3. Execute the SQL file:

```bash
psql "${DATABASE_URL/postgresql+psycopg/postgresql}" \
  -v ON_ERROR_STOP=1 \
  -f docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql
```

4. Interpret outputs in this order:
   - query 1: confirm available scoring runs and persisted row counts
   - query 2: inspect score distribution by buckets
   - query 3: compare candidate threshold pairs by review queue size (% and count)
   - query 4: inspect reason-code mix under selected thresholds
   - query 5: produce a borderline parcel list for manual analyst review
   - query 6: inspect TRS-level concentration for neighborhood pattern analysis
5. Record recommended threshold updates and rationale in review notes before changing scoring contracts.

## Decision Guardrails

- Do not change thresholds in place under an existing `ruleset_version`.
- If thresholds or band cutoffs change, publish a new `ruleset_version`.
- Prefer threshold choices that keep review queue size operationally tractable while preserving high-severity reason-code coverage.

## Basic Compatibility Validation

Validated on 2026-03-09 with current repository schema:

1. Initialize schema at configured `DATABASE_URL`:

```bash
.venv/bin/accessdane init-db
```

2. Execute the calibration SQL file:

```bash
psql "${DATABASE_URL/postgresql+psycopg/postgresql}" \
  -v ON_ERROR_STOP=1 \
  -f docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql
```

Result: all queries executed successfully against the current scoring tables and column names on an empty local database (no scored rows), validating schema compatibility and empty-result behavior only; workbook behavior with populated scoring outputs has not yet been validated.
