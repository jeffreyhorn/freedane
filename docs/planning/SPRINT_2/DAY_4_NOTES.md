# Sprint 2 Day 4 Notes

Date: 2026-03-01

## Focus

Improve iteration ergonomics for `run-all` so small parser loops do not trigger unnecessary broad downstream work.

## Change Made

`run-all` in [cli.py](../../../src/accessdane_audit/cli.py) now behaves better for scoped parser work in two ways.

### 1. Scoped Downstream Steps

When `run-all` has a clear parcel scope, downstream steps now keep that same scope:

- `--parse-only --parse-ids ...` now scopes `build-parcel-year-facts` to the same parcel IDs
- anomaly generation at the end of `run-all` is also scoped to the same parcel IDs

For non-parse-only runs, the downstream scope now follows the parcel IDs fetched during that run.

This prevents small parser iterations from forcing a broader fact rebuild or a broader anomaly scan than the user asked for.

### 2. Explicit Anomaly Skip

`run-all` now accepts:

- `--skip-anomalies`

This stops the command after parsing (and optional fact rebuilding) without writing the anomaly JSON file.

That gives a faster path for tight parser/debug loops where anomaly output is not needed yet.

## Test Coverage

New CLI coverage was added in [test_cli.py](../../../tests/test_cli.py):

- scoped `run-all --parse-only --parse-ids --build-parcel-year-facts` only rebuilds facts for the selected parcel set
- `--skip-anomalies` prevents the anomaly output file from being written

## Validation

Validation run:

```bash
python -m compileall src tests
.venv/bin/python -m pytest tests/test_cli.py tests/test_quality.py
.venv/bin/python -m pytest
```

Observed results:

- compile step passed
- targeted tests: `7 passed`
- full suite: `38 passed in 39.52s`

## Documentation Updates

The operational docs were updated to match the new behavior:

- [README.md](../../../README.md)
- [OPERATIONS.md](../SPRINT_1/OPERATIONS.md)

The key user-facing change is that `run-all` no longer has to behave like a full-sweep command during scoped parser iteration.
