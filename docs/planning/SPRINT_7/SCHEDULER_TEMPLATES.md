# Sprint 7 Scheduler Templates

Created: 2026-03-16

This document provides cron-compatible templates for recurring `refresh-runner`
execution using the scheduler entrypoint script:

- `scripts/run_scheduled_refresh.sh`

## Required Runtime Assumptions

- Run from repository root or use absolute paths in environment variables.
- `.venv/bin/accessdane` exists and is executable.
- `DATABASE_URL` and any required AccessDane environment values are available.

## Environment Variables

Common scheduler variables:

- `ACCESSDANE_REFRESH_PROFILE` (`daily_refresh` or `analysis_only`)
- `ACCESSDANE_FEATURE_VERSION` (default `feature_v1`)
- `ACCESSDANE_RULESET_VERSION` (default `scoring_rules_v1`)
- `ACCESSDANE_SALES_RATIO_BASE` (default `sales_ratio_v1`)
- `ACCESSDANE_REFRESH_TOP` (default `100`)
- `ACCESSDANE_ARTIFACT_BASE_DIR` (default `data/refresh_runs`)
- `ACCESSDANE_REFRESH_LOG_DIR` (default `${ACCESSDANE_ARTIFACT_BASE_DIR}/scheduler_logs`)

Optional source-file inputs for ingest-enabled runs:

- `ACCESSDANE_RETR_FILE`
- `ACCESSDANE_PERMITS_FILE`
- `ACCESSDANE_APPEALS_FILE`

Optional retry controls:

- `ACCESSDANE_RETRY_STAGE`
- `ACCESSDANE_RETRY_ATTEMPT` (default `2` when retry stage is set)

## Cron Templates

Replace `/path/to/repo` below with your local repository path.

Daily refresh every night at 01:30 UTC:

```cron
30 1 * * * cd /path/to/repo && \
  LOG_DIR="${ACCESSDANE_REFRESH_LOG_DIR:-${ACCESSDANE_ARTIFACT_BASE_DIR:-data/refresh_runs}/scheduler_logs}" && \
  mkdir -p "${LOG_DIR}" && \
  ACCESSDANE_REFRESH_PROFILE=daily_refresh \
  ACCESSDANE_FEATURE_VERSION=feature_v1 \
  ACCESSDANE_RULESET_VERSION=scoring_rules_v1 \
  scripts/run_scheduled_refresh.sh >> "${LOG_DIR}/cron_daily.log" 2>&1
```

Analysis-only refresh every weekday at 06:15 UTC:

```cron
15 6 * * 1-5 cd /path/to/repo && \
  LOG_DIR="${ACCESSDANE_REFRESH_LOG_DIR:-${ACCESSDANE_ARTIFACT_BASE_DIR:-data/refresh_runs}/scheduler_logs}" && \
  mkdir -p "${LOG_DIR}" && \
  ACCESSDANE_REFRESH_PROFILE=analysis_only \
  ACCESSDANE_FEATURE_VERSION=feature_v1 \
  ACCESSDANE_RULESET_VERSION=scoring_rules_v1 \
  scripts/run_scheduled_refresh.sh >> "${LOG_DIR}/cron_analysis.log" 2>&1
```

Manual retry from `score_pipeline` stage:

```bash
ACCESSDANE_REFRESH_PROFILE=daily_refresh \
ACCESSDANE_RETRY_STAGE=score_pipeline \
ACCESSDANE_RETRY_ATTEMPT=2 \
scripts/run_scheduled_refresh.sh
```

## Retention And Locking Notes

- Each run writes to `data/refresh_runs/<run_date>/<profile_name>/<run_id>/`.
- A per-profile lock file guards overlap: `data/refresh_runs/locks/<profile_name>.lock`.
- Lock files include JSON metadata (`profile_name`, `pid`, `started_at`) for debugging.
- In-progress runs write `.in_progress` marker files and remove them on completion.
- If a scheduler run crashes and leaves a stale lock, verify no active process matches the
  lock metadata and remove `data/refresh_runs/locks/<profile_name>.lock` before rerunning.
- `latest` pointer metadata is published only after run completion:
  - `data/refresh_runs/latest/<profile_name>/<feature_version>/<ruleset_version>/latest_run.json`
