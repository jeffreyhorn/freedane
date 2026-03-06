# Appeal Import Operations (Sprint 4)

Prepared on: 2026-03-06

## Goal

Provide a repeatable operator workflow for loading manual or records-request
appeal CSV files with `accessdane ingest-appeals`.

## Prerequisites

- virtualenv dependencies installed (`pip install -e '.[dev]'`)
- database initialized (`.venv/bin/accessdane init-db`)
- a UTF-8 appeal CSV file with at least:
  - one parcel locator header (`Parcel Number` or `Address` alias)
  - one temporal anchor header (`Filing Date`, `Hearing Date`, `Decision Date`, or `Tax Year`)
  - one appeal signal header (`Appeal Number`, `Docket Number`, `Outcome`, `Requested Value`, or `Final Value`)

## Run Import

```bash
.venv/bin/accessdane ingest-appeals --file ~/Downloads/appeals.csv
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
- `tax_year_anchor_mismatch`: explicit `Tax Year` disagreed with date-derived year.
- `outcome_value_direction_mismatch`: reduction-like outcome was paired with a positive value change.

Common rejection reasons:

- `Row is blank.`
- `At least one parcel locator is required: parcel number or site address.`
- `At least one temporal anchor is required: filing_date, hearing_date, decision_date, or tax_year.`
- `No valid temporal anchor remains after parsing.`
- `At least one appeal signal is required: appeal_number, docket_number, outcome_raw, requested_assessed_value, or decided_assessed_value.`

Rejected rows are still inserted into `appeal_events` for auditability.

## Sparse/Manual CSV Behavior

The importer treats common placeholder tokens as missing values, including:

- `N/A`, `NA`, `N\A`, `None`, `Null`, `Not Available`, `Not Applicable`, `TBD`

Date parsing tolerates common spreadsheet/operator forms:

- `MM/DD/YYYY`, `MM-DD-YYYY`, `YYYY-MM-DD`
- `YYYY/MM/DD`, `MM/DD/YY`, `MM-DD-YY`
- ISO datetime forms such as `YYYY-MM-DDTHH:MM:SSZ`

`Tax Year` parsing accepts a standalone 4-digit year and can recover one clear year token from text like `Tax Year 2025.0`.

## Failure Recovery

1. If file-level validation fails, correct the CSV header/encoding issue and rerun.
2. If many rows are rejected, inspect `appeal_events.import_error` and command-level rejection counts.
3. If warning counts are high, inspect `appeal_events.import_warnings` and `raw_row` for source-specific cleanup rules.
4. Re-run the same file safely: rows are idempotently upserted by `(source_file_sha256, source_row_number)`.
