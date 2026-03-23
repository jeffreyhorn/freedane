# Sprint 7 Operations Runbook

Prepared on: 2026-03-23

## Goal

Operate Sprint 7 automation and monitoring surfaces as a repeatable workflow:

- scheduled refresh orchestration (`refresh-runner`)
- parser drift snapshot/diff diagnostics
- load monitoring and alert payload generation
- annual refresh execution and signoff artifacts
- recurring benchmark pack generation

## Prerequisites

- dependencies installed in virtualenv:
  - `pip install -e '.[dev]'`
- database configured:
  - `DATABASE_URL` (default `sqlite:///data/accessdane.sqlite`)
- schema at migration head:

```bash
.venv/bin/accessdane init-db
```

- source files available for ingest-enabled runs:
  - RETR export CSV
  - permits CSV (optional)
  - appeals CSV (optional)
  - annual assessment manifest JSON (annual profile only)

## Artifact Layout

Primary artifact roots:

- refresh automation: `data/refresh_runs/`
- benchmark packs: `data/benchmark_packs/`

Key refresh paths:

- run root: `data/refresh_runs/<run_date>/<profile_name>/<run_id>/`
- scheduler log payloads: `data/refresh_runs/scheduler_logs/<run_id>.json`
- latest pointer: `data/refresh_runs/latest/<profile>/<feature>/<ruleset>/latest_run.json`
- lock files: `data/refresh_runs/locks/<profile>.lock`

Key benchmark paths:

- run root: `data/benchmark_packs/<run_date>/<profile>/<run_id>/`
- latest pointer: `data/benchmark_packs/latest/<profile>/<feature>/<ruleset>/latest_benchmark_pack.json`

## Daily Operations Sequence

1. Run scheduled refresh.

```bash
ACCESSDANE_REFRESH_PROFILE=daily_refresh \
ACCESSDANE_FEATURE_VERSION=feature_v1 \
ACCESSDANE_RULESET_VERSION=scoring_rules_v1 \
ACCESSDANE_ARTIFACT_BASE_DIR=data/refresh_runs \
ACCESSDANE_RETR_FILE=/path/to/retr.csv \
ACCESSDANE_PERMITS_FILE=/path/to/permits.csv \
ACCESSDANE_APPEALS_FILE=/path/to/appeals.csv \
scripts/run_scheduled_refresh.sh
```

2. Capture parser drift snapshot (post-refresh baseline/current profile).

```bash
.venv/bin/accessdane parser-drift-snapshot \
  --profile-name daily_refresh \
  --run-date <YYYYMMDD> \
  --out data/refresh_runs/<YYYYMMDD>/daily_refresh/<run_id>/parser_drift_snapshot.json
```

3. Compare against baseline snapshot and emit alert payload if needed.

```bash
.venv/bin/accessdane parser-drift-diff \
  --baseline <baseline_snapshot.json> \
  --current data/refresh_runs/<YYYYMMDD>/daily_refresh/<run_id>/parser_drift_snapshot.json \
  --out data/refresh_runs/<YYYYMMDD>/daily_refresh/<run_id>/parser_drift_diff.json \
  --alert-out data/refresh_runs/<YYYYMMDD>/daily_refresh/<run_id>/parser_drift_alert.json
```

4. Run load diagnostics for the subject refresh run.

```bash
.venv/bin/accessdane load-monitor \
  --artifact-base-dir data/refresh_runs \
  --profile-name daily_refresh \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --subject-run-id <run_id> \
  --out data/refresh_runs/<YYYYMMDD>/daily_refresh/<run_id>/load_monitor.json \
  --alert-out data/refresh_runs/<YYYYMMDD>/daily_refresh/<run_id>/load_monitor_alert.json
```

These `--out` and `--alert-out` files are convenience copies for operator workflows.
Canonical refresh and monitoring artifacts still remain under each run root and latest pointers in `data/refresh_runs/`.

5. Build recurring benchmark pack and trend artifact.

```bash
.venv/bin/accessdane benchmark-pack \
  --run-date <YYYYMMDD> \
  --run-id <run_id> \
  --profile-name daily_refresh \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --top-n 100 \
  --artifact-base-dir data/benchmark_packs \
  --out data/benchmark_packs/<YYYYMMDD>/daily_refresh/<run_id>/benchmark_pack.json \
  --trend-out data/benchmark_packs/<YYYYMMDD>/daily_refresh/<run_id>/benchmark_pack_trend.json \
  --alert-out data/benchmark_packs/<YYYYMMDD>/daily_refresh/<run_id>/benchmark_pack_alert.json
```

`--out`, `--trend-out`, and `--alert-out` are convenience copies for operator workflows.
Canonical benchmark artifacts are also persisted under the benchmark run root and latest pointer paths under `data/benchmark_packs/`.

## Annual Refresh Sequence

Run annual refresh profile when new roll/source files are ready:

```bash
.venv/bin/accessdane annual-refresh-runner \
  --run-date <YYYYMMDD> \
  --annual-target-year <YYYY> \
  --assessment-manifest-file /path/to/assessment_manifest.json \
  --retr-file /path/to/retr.csv \
  --permits-file /path/to/permits.csv \
  --appeals-file /path/to/appeals.csv \
  --artifact-base-dir data/refresh_runs \
  --out data/refresh_runs/<YYYYMMDD>/annual_refresh_run.json
```

Verify annual signoff artifacts exist under run root:

- `annual_signoff/annual_stage_checklist.json`
- `annual_signoff/annual_signoff.json`

## Troubleshooting

### Scheduler / Refresh Runner

- `overlapping_run`:
  - active profile lock exists at `data/refresh_runs/locks/<profile>.lock`.
  - verify no active process is running for that profile, then remove stale lock.
- `invalid_retry_boundary`:
  - `--retried-from-stage-id` must be one of selected profile stages.
- `annual_preflight_failed`:
  - check missing/invalid `--annual-target-year`, `--assessment-manifest-file`, `--retr-file`, or replay-mode context.
- `stage_failure`:
  - inspect per-stage `command_results` in run payload to identify failed command id and exit code.

### Parser Drift

- `snapshot_build_failed`:
  - verify DB connectivity and parser profile source records.
- `diff_build_failed`:
  - ensure both baseline/current snapshots are valid snapshot payloads.
- no alert payload:
  - expected when severity is below alert threshold.
  - `--alert-out` stale files are now removed automatically when no alert is emitted.

### Load Monitoring

- `load_monitoring_failed`:
  - subject refresh payload not found or malformed.
  - verify `--subject-run-id` or `--subject-refresh-payload` points to a valid run artifact.
- no alert payload or `missing_alert_id`:
  - diagnostics still written to `--out`.
  - `--alert-out` stale files are now removed automatically when no valid alert is emitted.

### Benchmark Pack

- `benchmark_pack_failed`:
  - usually version selector mismatch, invalid profile/version args, or upstream scoring/queue artifacts unavailable.
- non-comparable comparison:
  - check `comparison.non_comparable_reasons` (for example `no_comparable_baseline_found`, `baseline_run_failed`).
- failed run persistence behavior:
  - failed packs do not update `latest/*` pointers.

## Validation Loop (Targeted)

Run this targeted Sprint 7 validation set after automation/monitoring changes:

```bash
.venv/bin/python -m pytest \
  tests/test_refresh_automation.py \
  tests/test_parser_drift.py \
  tests/test_load_monitoring.py \
  tests/test_benchmark_pack.py \
  -n auto -m "not slow"
```

## References

- [REFRESH_AUTOMATION_V1.md](REFRESH_AUTOMATION_V1.md)
- [SCHEDULER_TEMPLATES.md](SCHEDULER_TEMPLATES.md)
- [PARSER_DRIFT_V1.md](PARSER_DRIFT_V1.md)
- [LOAD_MONITORING_V1.md](LOAD_MONITORING_V1.md)
- [ANNUAL_REFRESH_V1.md](ANNUAL_REFRESH_V1.md)
- [BENCHMARK_PACK_V1.md](BENCHMARK_PACK_V1.md)
