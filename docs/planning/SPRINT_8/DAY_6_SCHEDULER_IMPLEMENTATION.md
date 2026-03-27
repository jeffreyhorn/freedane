# Sprint 8 Day 6 Scheduler Implementation Notes

## Scope Delivered

Day 6 implements a managed scheduler adapter that wraps `refresh-runner` execution
with resilient retry/dead-letter behavior:

- durable scheduler payloads under `ACCESSDANE_REFRESH_LOG_DIR`
- retry orchestration for retryable failure classes
- overlap-safe dispatch handling using profile lock detection
- dead-letter routing with incident metadata when terminal failure occurs

## Implementation

### New Scheduler Integration Module

- `src/accessdane_audit/scheduler_integration.py`

Key behavior:

- builds scheduler envelope:
  - `scheduler_run`
  - `trigger`
  - `attempts`
  - `result`
  - `incident`
- maps trigger type to default attempt budget:
  - `scheduled`: 3
  - `catch_up`: 3
  - `manual_retry`: 2
- classifies retryable vs terminal failure classes and transitions:
  - `queued -> dispatched -> running`
  - `running -> retry_pending|succeeded|failed_pending_dead_letter`
  - `failed_pending_dead_letter -> dead_lettered`
- writes dead-letter payload copy to:
  - `<ACCESSDANE_ARTIFACT_BASE_DIR>/dead_letter/<profile_name>/<run_date>/<scheduler_run_id>.json`

### New CLI Command

- `accessdane scheduler-runner`

Added in `src/accessdane_audit/cli.py` with support for:

- trigger metadata (`--trigger-type`, `--scheduled-for-utc`, `--requested-by`)
- runtime profile/context options aligned with refresh-runner
- retry controls (`--max-attempts`, backoff parameters, optional sleep)
- scheduler payload output (`--out`)

## Test Coverage Added

- `tests/test_scheduler_integration.py`

Coverage includes:

- retryable failure then success on next attempt
- overlap exhaustion routing to dead-letter
- non-retryable terminal failure without retry loop
- CLI smoke test for `scheduler-runner` option mapping/output

## Validation

Executed during Day 6 work:

```bash
make typecheck
make lint
make format
make test
```

Result:

- all checks passed
