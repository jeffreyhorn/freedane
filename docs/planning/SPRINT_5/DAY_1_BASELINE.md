# Sprint 5 Day 1 Baseline

Date: 2026-03-07

Branch: `sprint5/day-01-baseline`

## Commands Run

Day 1 baseline validation was executed with:

```bash
make typecheck && make lint && make format && make test
.venv/bin/accessdane init-db
.venv/bin/accessdane spatial-support --out data/sprint5_baseline_spatial_support.json
.venv/bin/alembic heads
```

Schema baseline checks were also run against the runtime `DATABASE_URL`:

```bash
.venv/bin/python - <<'PY'
from sqlalchemy import inspect, text
from accessdane_audit.config import load_settings
from accessdane_audit.db import get_engine, HEAD_REVISION

settings = load_settings()
engine = get_engine(settings.database_url)
with engine.connect() as conn:
    version = conn.execute(text("select version_num from alembic_version")).scalar_one_or_none()
print(HEAD_REVISION, version)
print("parcel_features" in set(inspect(engine).get_table_names()))
print("fraud_scores" in set(inspect(engine).get_table_names()))
print("fraud_flags" in set(inspect(engine).get_table_names()))
print("scoring_runs" in set(inspect(engine).get_table_names()))
PY
```

## Validation Result

- `make typecheck`: passed (`mypy` clean for `src/`)
- `make lint`: passed (`ruff` + `mypy` + `black --check` clean)
- `make format`: passed (`black` no-op; import sorting clean)
- `make test`: passed (`154 passed in 41.18s`)
- `accessdane init-db`: passed (runtime DB initialized at migration head)
- spatial baseline artifact generated: `data/sprint5_baseline_spatial_support.json`

## Spatial Baseline Snapshot

From `data/sprint5_baseline_spatial_support.json`:

- `database_dialect`: `postgresql`
- `mode`: `geometry_postgis`
- `postgis_available`: `true`
- `geometry_columns_available`: `true`

Interpretation:

- Sprint 5 can keep core scoring workflows backend-agnostic.
- Optional geometry-derived features can be designed behind explicit feature flags because PostGIS is available in the current runtime.

## Schema And Migration Baseline

- Repo migration head: `20260307_0010`
- Runtime DB `alembic_version`: `20260307_0010`
- Sprint 5 scoring tables not yet present:
  - `parcel_features`: absent
  - `fraud_scores`: absent
  - `fraud_flags`: absent
  - `scoring_runs`: absent

Interpretation:

- Sprint 4 baseline is stable and current.
- Day 2 and Day 3 should define and add migration-managed Sprint 5 feature/scoring storage before command implementations begin.

## Sprint 5 Execution Checklist With Acceptance Gates

### Contract And Storage (Days 2-3)

- [ ] Day 2: finalize fraud-signal contract and persistence model (`FRAUD_SIGNAL_V1.md`)
  - Acceptance gate: contracts define command IO shape, reason-code semantics, rerun/idempotency behavior, and required tables/indexes.
- [ ] Day 3: add schema + migration for `parcel_features`, `fraud_scores`, `fraud_flags`, and run metadata table(s)
  - Acceptance gate: migration tests pass and runtime `init-db` upgrades cleanly to new head.

### Sales-Ratio Study (Days 4-5)

- [ ] Day 4: implement `accessdane sales-ratio-study` core metrics by year/class/area
  - Acceptance gate: deterministic outputs on fixtures and command-level tests pass.
- [ ] Day 5: harden ratio-study null/sparse-group handling and diagnostics
  - Acceptance gate: edge-case tests pass and operator docs cover exclusions/insufficient groups.

### Feature Engineering (Days 6-9)

- [ ] Day 6: finalize feature engineering contract (`FEATURE_ENGINEERING_V1.md`)
  - Acceptance gate: definitions cover all Sprint 5 required feature families and null semantics.
- [ ] Day 7: implement `accessdane build-features` base pipeline
  - Acceptance gate: feature rows are persisted with run/version metadata and reruns are deterministic.
- [ ] Day 8: add permit/appeal/lineage feature layers
  - Acceptance gate: context-aware feature tests pass for mixed and sparse parcel scenarios.
- [ ] Day 9: add feature validation/reporting and operator workflow docs
  - Acceptance gate: invalid feature states are surfaced explicitly and summaries are reproducible.

### Scoring Engine (Days 10-12)

- [ ] Day 10: finalize scoring rules and reason-code taxonomy (`SCORING_RULES_V1.md`)
  - Acceptance gate: rule precedence and explanation contracts are explicit and versioned.
- [ ] Day 11: implement `accessdane score-fraud` with persisted scores/flags
  - Acceptance gate: score outputs include deterministic reason codes and pass idempotency tests.
- [ ] Day 12: harden ranking outputs and end-to-end score workflow docs
  - Acceptance gate: ranking consistency tests pass and missing-feature safeguards behave as designed.

### Calibration And Closeout (Days 13-14)

- [ ] Day 13: add calibration SQL/notebook support artifacts
  - Acceptance gate: calibration artifacts execute against current output schema without manual rewrites.
- [ ] Day 14: produce Sprint 5 acceptance review + Sprint 6 handoff draft
  - Acceptance gate: targeted Sprint 5 validation set is green and acceptance criteria are explicitly assessed.

## Day 1 Exit Decision

Day 1 is complete enough to proceed to Sprint 5 Day 2.

The Sprint 4 baseline is green, runtime schema is current at head, spatial baseline is captured, and the Sprint 5 execution checklist now includes explicit acceptance gates for each delivery block.
