# Sprint 7 Day 1 Baseline

Date: 2026-03-15

Branch: `sprint7/day-01-baseline`

## Commands Run

Day 1 baseline validation was executed with:

```bash
make typecheck && make lint && make format && make test
.venv/bin/accessdane init-db
.venv/bin/accessdane review-queue --top 10 --out data/sprint7_day1_baseline_queue.json
.venv/bin/accessdane review-feedback --out data/sprint7_day1_baseline_feedback.json --sql-out data/sprint7_day1_baseline_feedback.sql
.venv/bin/accessdane investigation-report --top 10 --html-out data/sprint7_day1_baseline_report.html --out data/sprint7_day1_baseline_report.json
```

Runtime schema state checks were also run against the configured database URL:

```bash
.venv/bin/python - <<'PY'
from sqlalchemy import inspect, text
from accessdane_audit.config import load_settings
from accessdane_audit.db import get_engine, HEAD_REVISION

settings = load_settings()
engine = get_engine(settings.database_url)
with engine.connect() as conn:
    version = conn.execute(text("select version_num from alembic_version")).scalar_one_or_none()
print("head_revision", HEAD_REVISION)
print("db_revision", version)
tables = set(inspect(engine).get_table_names())
for name in ("parcel_features", "fraud_scores", "fraud_flags", "case_reviews"):
    print(name, name in tables)
PY
```

## Validation Result

- `make typecheck`: passed
- `make lint`: passed
- `make format`: passed
- `make test`: passed (`229 passed in 49.30s`)
- `accessdane init-db`: passed
- Sprint 6 workflow smoke sequence: passed end-to-end
  - `review-queue`: `run.status = succeeded`
  - `review-feedback`: `run.status = succeeded`
  - `investigation-report`: `run.status = succeeded`

Smoke artifacts generated:

- `data/sprint7_day1_baseline_queue.json`
- `data/sprint7_day1_baseline_feedback.json`
- `data/sprint7_day1_baseline_feedback.sql`
- `data/sprint7_day1_baseline_report.json`
- `data/sprint7_day1_baseline_report.html`

## Sprint 7 Baseline Snapshot

From smoke artifacts:

- queue summary:
  - `candidate_count`: `58684`
  - `filtered_count`: `58637`
  - `returned_count`: `10`
  - `truncated`: `true`
- feedback summary:
  - `reviewed_case_count`: `0`
  - `false_positive_count`: `0`
  - interpretation: feedback generator is operational; current environment has no resolved/closed review sample yet
- investigation report summary:
  - `queue_row_count`: `10`
  - `dossier_failure_count`: `0`
  - `html_size_bytes`: `18604`

## Schema And Migration Baseline

- Runtime `database_url`: configured PostgreSQL URL (credentials redacted)
- Repo migration head: `20260312_0013`
- Runtime DB `alembic_version`: `20260312_0013`
- Required Sprint 6 investigation tables present:
  - `parcel_features`: present
  - `fraud_scores`: present
  - `fraud_flags`: present
  - `case_reviews`: present

Interpretation:

- Sprint 6 investigation surfaces are healthy and reproducible at Sprint 7 start.
- Migration state is current.
- Sprint 7 can proceed to automation and monitoring contract work.

## Day 1 Exit Decision

Day 1 is complete enough to proceed to Sprint 7 Day 2.

The baseline is green, Sprint 6 workflow smoke validation is complete, migration state is aligned to head, and Sprint 7 execution gates are defined in [EXECUTION_CHECKLIST.md](EXECUTION_CHECKLIST.md).
