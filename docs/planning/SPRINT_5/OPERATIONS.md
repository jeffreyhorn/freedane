# Sprint 5 Operations Runbook

Prepared on: 2026-03-10

## Goal

Provide one repeatable end-to-end Sprint 5 workflow from sales-ratio study through scoring and calibration.

## Prerequisites

- virtualenv dependencies installed: `pip install -e '.[dev]'`
- database URL configured (defaults to `sqlite:///data/accessdane.sqlite`; Sprint 5 validation and calibration step 6 have used PostgreSQL)
  - if running calibration: ensure `DATABASE_URL` points to a PostgreSQL URL; if you use a separate shell variable (for example `PSQL_URL`) for the `psql` CLI, ensure it also points to a PostgreSQL URL usable by `psql`
  - if staying on SQLite: skip calibration step 6
- schema initialized to migration head:

```bash
.venv/bin/accessdane init-db
```

## Standard Sprint 5 End-To-End Sequence

1. Refresh sales linkage context.

```bash
.venv/bin/accessdane ingest-retr --file <retr_export.csv>
.venv/bin/accessdane match-sales
```

2. Refresh event context (if permit/appeal source files changed).

```bash
.venv/bin/accessdane ingest-permits --file <permits.csv>
.venv/bin/accessdane ingest-appeals --file <appeals.csv>
.venv/bin/accessdane build-parcel-year-facts
```

3. Run sales ratio study.

```bash
.venv/bin/accessdane sales-ratio-study \
  --version-tag sales_ratio_v1 \
  --out data/sprint5_sales_ratio_study.json
```

4. Build features.

```bash
.venv/bin/accessdane build-features \
  --feature-version feature_v1 \
  --out data/sprint5_build_features.json
```

5. Score fraud signals.

```bash
.venv/bin/accessdane score-fraud \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/sprint5_score_fraud.json
```

6. Run calibration workbook for threshold tuning.

```bash
psql "${DATABASE_URL/postgresql+psycopg/postgresql}" \
  -v ON_ERROR_STOP=1 \
  -f docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql
```

If your shell does not support `${.../.../...}` substitution, set a plain PostgreSQL URL first (for example `PSQL_URL="postgresql://user:pass@host:port/dbname"`) and run `psql "$PSQL_URL" ...`.

## Scoped Validation Loop

Use this loop before full refreshes:

```bash
.venv/bin/accessdane sales-ratio-study --ids <parcel_ids.txt> --year <year> --version-tag sales_ratio_v1_scope
.venv/bin/accessdane build-features --ids <parcel_ids.txt> --year <year> --feature-version feature_v1_scope
.venv/bin/accessdane score-fraud --ids <parcel_ids.txt> --year <year> --feature-version feature_v1_scope --ruleset-version scoring_rules_v1
```

## Sprint 5 Targeted Validation Cadence

For Sprint 5 scoring changes, use:

```bash
.venv/bin/python -m pytest \
  tests/test_sales_ratio_study.py \
  tests/test_build_features.py \
  tests/test_score_fraud.py \
  tests/test_migrations.py \
  -n auto -m "not slow"
```

## Operational Notes

- Keep `feature_version` and `ruleset_version` explicit for reproducible reruns and comparability.
- Calibrate thresholds in SQL/notebook workflow first; publish threshold/rule changes under a new `ruleset_version`.
- Use `rankings.skipped_feature_breakdown` and feature diagnostics before analyst handoff to avoid triaging structurally invalid rows.
- Treat score output as risk triage, not proof.

## Reference Docs

- [SALES_RATIO_STUDY_OPERATIONS.md](SALES_RATIO_STUDY_OPERATIONS.md)
- [BUILD_FEATURES_OPERATIONS.md](BUILD_FEATURES_OPERATIONS.md)
- [SCORE_FRAUD_OPERATIONS.md](SCORE_FRAUD_OPERATIONS.md)
- [CALIBRATION_NOTES.md](CALIBRATION_NOTES.md)
- [CALIBRATION_SQL_V1.sql](CALIBRATION_SQL_V1.sql)
- [FRAUD_SIGNAL_V1.md](FRAUD_SIGNAL_V1.md)
- [SCORING_RULES_V1.md](SCORING_RULES_V1.md)
