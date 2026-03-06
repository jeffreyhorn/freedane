# Permit Import Operations (Sprint 4)

Prepared on: 2026-03-06

## Goal

Provide a repeatable operator workflow for loading manual or records-request permit CSV files with `accessdane ingest-permits`.

## Prerequisites

- virtualenv dependencies installed (`pip install -e '.[dev]'`)
- database initialized (`.venv/bin/accessdane init-db`)
- a UTF-8 permit CSV file with at least:
  - one parcel locator header (`Parcel Number` or `Address` alias)
  - one temporal anchor header (`Applied Date`, `Issued Date`, `Finaled Date`, `Status Date`, or `Permit Year`)

## Run Import

```bash
.venv/bin/accessdane ingest-permits --file ~/Downloads/permits.csv
```

The command prints:

- base counters: `total`, `loaded`, `rejected`, `inserted`, `updated`
- rejection breakdowns by reason text (when any rows are rejected)
- warning breakdowns by warning code (when any warnings are emitted)

## Warning And Rejection Diagnostics

Common warning codes:

- `row_shorter_than_header`: row had fewer columns than the header list; missing cells were padded as null.
- `row_has_extra_columns`: row had extra trailing columns; values were preserved in `raw_row["_extra_columns"]`.
- `*_unparseable`: value could not be parsed into date/year/amount and was set to null.
- `permit_year_anchor_mismatch`: explicit `Permit Year` disagreed with date-derived year.

Common rejection reasons:

- `Row is blank.`
- `At least one parcel locator is required: parcel number or site address.`
- `At least one temporal anchor is required: applied_date, issued_date, finaled_date, status_date, or permit_year.`
- `No valid temporal anchor remains after parsing.`

Rejected rows are still inserted into `permit_events` for auditability.

## Sparse/Manual CSV Behavior

The importer treats common placeholder tokens as missing values, including:

- `N/A`, `NA`, `N\A`, `None`, `Null`, `Not Available`, `Not Applicable`, `TBD`

This prevents placeholder text from creating avoidable parse warnings or false "unknown" status values.

## Failure Recovery

1. If file-level validation fails, correct the CSV header/encoding issue and rerun.
2. If many rows are rejected, inspect `permit_events.import_error` plus command-level rejection counts.
3. If warning counts are high, inspect `permit_events.import_warnings` and `raw_row` to identify source-specific cleanup rules.
4. Re-run the same file safely: rows are idempotently upserted by `(source_file_sha256, source_row_number)`.
