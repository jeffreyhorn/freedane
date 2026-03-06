# Sprint 4 Operations Runbook

Prepared on: 2026-03-06

## Goal

Provide one operator workflow for Sprint 4 permit/appeal ingestion, parcel-year context refresh, and spatial-mode checks.

## Prerequisites

- virtualenv dependencies installed: `pip install -e '.[dev]'`
- database URL configured (defaults to `sqlite:///data/accessdane.sqlite`)
- schema initialized to migration head:

```bash
.venv/bin/accessdane init-db
```

## Standard Sprint 4 Ingestion Sequence

1. Ingest permit data.

```bash
.venv/bin/accessdane ingest-permits --file <permits.csv>
```

2. Ingest appeal data.

```bash
.venv/bin/accessdane ingest-appeals --file <appeals.csv>
```

3. Rebuild parcel-year context so new events appear in parcel-year outputs.

```bash
.venv/bin/accessdane build-parcel-year-facts
```

For scoped refreshes:

```bash
.venv/bin/accessdane build-parcel-year-facts --ids <parcel_ids.txt>
# or repeat --id <parcel_id>
```

4. (Optional) Check runtime spatial mode.

```bash
.venv/bin/accessdane spatial-support
.venv/bin/accessdane spatial-support --out data/spatial_support.json
```

## Validation Cadence For Sprint 4 Work

Use this targeted suite for permit/appeal/context/spatial changes:

```bash
.venv/bin/python -m pytest \
  tests/test_permits.py \
  tests/test_appeals.py \
  tests/test_parcel_year_facts.py \
  tests/test_migrations.py \
  tests/test_spatial.py \
  tests/test_cli.py \
  -n auto -m "not slow"
```

## Operational Notes

- Permit and appeal importers are idempotent for same-file reruns (`source_file_sha256`, `source_row_number`).
- Rejected rows remain stored with `import_error` for auditability.
- `build-parcel-year-facts` is deterministic and supports full or scoped rebuilds.
- Non-PostgreSQL runtimes stay in `point_only` mode and continue normal operation.

## Reference Docs

- [PERMIT_OPERATIONS.md](PERMIT_OPERATIONS.md)
- [APPEAL_OPERATIONS.md](APPEAL_OPERATIONS.md)
- [PARCEL_YEAR_CONTEXT_INTEGRATION.md](PARCEL_YEAR_CONTEXT_INTEGRATION.md)
- [SPATIAL_GUARDRAILS.md](SPATIAL_GUARDRAILS.md)
- [../../templates/README.md](../../templates/README.md)
