# Sprint 8 Day 1 Baseline

Date: 2026-03-24

Branch: `sprint8/day-01-baseline`

## Commands Run

Day 1 baseline validation was executed with:

```bash
make typecheck && make lint && make format && make test
.venv/bin/python -m pytest tests/test_refresh_automation.py tests/test_parser_drift.py tests/test_load_monitoring.py tests/test_benchmark_pack.py -n auto -m "not slow"
scripts/run_scheduled_refresh.sh
.venv/bin/accessdane init-db
.venv/bin/accessdane parser-drift-snapshot --out data/sprint8_baseline_parser_snapshot.json
.venv/bin/accessdane load-monitor --out data/sprint8_baseline_load_monitor.json
.venv/bin/accessdane benchmark-pack --run-date 20260325 --out data/sprint8_baseline_benchmark_pack.json --trend-out data/sprint8_baseline_benchmark_trend.json
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
- `make test`: passed (`297 passed in 55.53s`)
- targeted Sprint 8 automation tests: passed (`68 passed in 21.02s`)
- `scripts/run_scheduled_refresh.sh`: passed
  - run id: `20260325_daily_refresh_feature_v1_scoring_rules_v1_010121`
  - run status: `succeeded`
  - stage summary: `6 succeeded`, `0 failed`, `0 blocked`
- `accessdane init-db`: passed
- `parser-drift-snapshot`: passed (`run.status = succeeded`)
- `load-monitor`: failed (`run.status = failed`, `summary.overall_severity = critical`)
- `benchmark-pack`: passed (`run.status = succeeded`, `comparison.overall_severity = ok`)

Smoke artifacts generated:

- `data/refresh_runs/scheduler_logs/20260325_daily_refresh_feature_v1_scoring_rules_v1_010121.json`
- `data/sprint8_baseline_parser_snapshot.json`
- `data/sprint8_baseline_load_monitor.json`
- `data/sprint8_baseline_benchmark_pack.json`
- `data/sprint8_baseline_benchmark_trend.json`

## Section 1 And Section 6 Verification (`docs/todo_immediately.md`)

Section 1 (migration head check):

- repo migration head: `20260312_0013`
- runtime DB revision: `20260312_0013`
- key workflow tables present:
  - `parcel_features`: present
  - `fraud_scores`: present
  - `fraud_flags`: present
  - `case_reviews`: present

Section 6 go/no-go scaffolding:

- `init-db` succeeded: yes
- load/ingest commands completed without blocking errors: partial
  - refresh run completed successfully, but ingest commands were skipped because source files were not provided (`retr`, `permits`, `appeals`)
- scoring pipeline completed: yes
- review queue and investigation report artifacts exist: yes
- no unresolved `critical` monitor/diagnostic alerts without owner assignment: no
  - `load-monitor` produced `critical` with `load_monitoring_failed`
  - error path shows duplicated artifact-base prefix:
    - `/Users/jeff/experiments/freedane/data/refresh_runs/data/refresh_runs/.../refresh_run_payload.json`

Current decision from scaffolding: **NO-GO** for analyst intake until load-monitor baseline is corrected and criticals are either resolved or explicitly owned.

## Day 1 Gap Map (Productionization Priorities)

- managed scheduler reliability baseline exists but is still script-first (`scripts/run_scheduled_refresh.sh`)
- observability baseline shows a blocking gap in load-monitor source artifact resolution
- startup go/no-go criteria are documented but not yet automated as a single executable gate
- data ingest dependency handling is not analyst-safe when source exports are missing (currently skip-based)

## Day 1 Exit Decision

Day 1 baseline and gap mapping are complete enough to proceed to Sprint 8 Day 2 contract work.

Next-day precondition carryover:

- treat the load-monitor critical path-resolution issue as a tracked Day 4 startup-automation blocker for analyst readiness
- keep analyst intake blocked until the Section 6 go/no-go gate is green or explicitly risk-accepted
