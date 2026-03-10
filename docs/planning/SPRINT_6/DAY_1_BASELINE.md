# Sprint 6 Day 1 Baseline

Date: 2026-03-10

Branch: `sprint6/day-01-baseline`

## Commands Run

Day 1 baseline validation was executed with:

```bash
make typecheck && make lint && make format && make test
.venv/bin/accessdane init-db
.venv/bin/accessdane sales-ratio-study --version-tag sprint6_day1_sales_ratio_v1 --out data/sprint6_day1_sales_ratio_study.json
.venv/bin/accessdane build-features --feature-version sprint6_day1_feature_v1 --out data/sprint6_day1_build_features.json
.venv/bin/accessdane score-fraud --feature-version sprint6_day1_feature_v1 --ruleset-version scoring_rules_v1 --out data/sprint6_day1_score_fraud.json
psql "${DATABASE_URL/postgresql+psycopg/postgresql}" -v ON_ERROR_STOP=1 -f docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql
```

Runtime schema state checks were also run against the configured `.env` database URL:

```bash
.venv/bin/python - <<'PY'
from sqlalchemy import inspect, text
from accessdane_audit.config import load_settings
from accessdane_audit.db import get_engine, HEAD_REVISION

settings = load_settings()
engine = get_engine(settings.database_url)
with engine.connect() as conn:
    version = conn.execute(text("select version_num from alembic_version")).scalar_one_or_none()
redacted_url = settings.database_url
if "@" in redacted_url and "://" in redacted_url:
    scheme, rest = redacted_url.split("://", 1)
    creds, host = rest.split("@", 1)
    user = creds.split(":", 1)[0]
    redacted_url = f"{scheme}://{user}@{host}"
print(redacted_url)
print(HEAD_REVISION, version)
tables = set(inspect(engine).get_table_names())
for name in ("parcel_features", "fraud_scores", "fraud_flags", "scoring_runs", "case_reviews"):
    print(name, name in tables)
PY
```

## Validation Result

- `make typecheck`: passed (`mypy` clean for `src/`)
- `make lint`: passed (`ruff` + `mypy` + `black --check` clean)
- `make format`: passed (`black` no-op; import sorting clean)
- `make test`: passed (`182 passed in 36.86s`)
- `accessdane init-db`: passed
- Sprint 5 pipeline smoke sequence: passed end-to-end
  - `sales_ratio_study` run: `10` (`version_tag=sprint6_day1_sales_ratio_v1`)
  - `build_features` run: `11` (`version_tag=sprint6_day1_feature_v1`)
  - `score_fraud` run: `12` (`version_tag=scoring_rules_v1`, `parent_run_id=11`)
- Calibration SQL: passed (`docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql` ran to completion)

Smoke artifacts generated:

- `data/sprint6_day1_sales_ratio_study.json`
- `data/sprint6_day1_build_features.json`
- `data/sprint6_day1_score_fraud.json`

## Sprint 6 Baseline Snapshot

From smoke artifacts:

- sales ratio summary:
  - `candidate_sales_count`: `349`
  - `included_sales_count`: `316`
  - `group_count`: `12`
- feature build summary:
  - `selected_parcel_years`: `58684`
  - `rows_inserted`: `58684`
  - `quality_warning_count`: `209995`
- score summary:
  - `scores_inserted`: `58684`
  - `flags_inserted`: `16344`
  - `high_risk_count`: `19`
  - `medium_risk_count`: `28`

## Schema And Migration Baseline

- Runtime `database_url`: `postgresql+psycopg://accessdane@localhost:5432/accessdane` (password omitted)
- Repo migration head: `20260307_0011`
- Runtime DB `alembic_version`: `20260307_0011`
- Sprint 5 scoring tables present:
  - `parcel_features`: present
  - `fraud_scores`: present
  - `fraud_flags`: present
  - `scoring_runs`: present
- Sprint 6 review persistence table status:
  - `case_reviews`: absent

Interpretation:

- Sprint 5 baseline is healthy and reproducible at Sprint 6 start.
- Run chaining is intact (`score_fraud` linked to the feature run).
- Sprint 6 can proceed to contract-first implementation for dossier and review workflow.

## Required New Persistence Objects (For Sprint 6 Case Review Workflow)

Based on Sprint 6 scope and current schema, the first required additions are:

- `case_reviews` table for analyst outcomes:
  - review identity (`id`, `parcel_id`, `year`)
  - workflow state (`status`, `disposition`)
  - analyst ownership (`reviewer`, optional queue assignment metadata)
  - evidence linkage (`score_id` and/or `run_id` + ruleset/feature versions)
  - notes (`note` or structured evidence summary)
  - audit timestamps (`created_at`, `updated_at`, `reviewed_at`)
- supporting indexes/constraints:
  - lookup index for queue views (`status`, `updated_at`)
  - lookup index for analyst reporting (`disposition`, `reviewed_at`)
  - uniqueness/idempotency constraint to prevent duplicate active reviews for same parcel/year/version context
  - foreign key integrity to scored rows/run metadata where applicable

## Day 1 Exit Decision

Day 1 is complete enough to proceed to Sprint 6 Day 2.

The baseline is green, smoke validation chain is complete, migration state is current, and Sprint 6 execution gates are defined in [EXECUTION_CHECKLIST.md](EXECUTION_CHECKLIST.md).
