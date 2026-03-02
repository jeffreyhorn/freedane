# Sprint 1 Operations Guide

## Purpose

This guide documents the practical Sprint 1 workflow for rebuilding derived data and checking the health of a local McFarland dataset.

## Fresh Contributor Setup

Bootstrap a local environment like this:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
accessdane init-db
```

If you only need runtime commands and not tests, `pip install -e .` is enough.
Use `alembic current` and `alembic upgrade head` when you want to inspect or apply migrations manually.

## Recommended Order

Run the core maintenance path in this order:

```bash
accessdane init-db
accessdane run-all --parse-only --reparse --build-parcel-year-facts
accessdane check-data-quality --out data/quality_report.json
accessdane profile-data --out data/profile_report.json
```

This sequence does four things:

1. Ensures the schema is at the current Alembic `head`.
2. Re-parses stored raw HTML into the normalized source tables.
3. Rebuilds the parcel-year derived table from those source tables.
4. Produces machine-readable quality and coverage snapshots.

On the current local corpus, this can be a long-running command path because `run-all --parse-only --reparse` processes the full set of stored successful fetches.
Use the scoped commands later in this guide when you need a fast smoke test instead of a full refresh.

## `parcel_year_facts`

`parcel_year_facts` is a rebuildable derived table, not a source-of-truth ingestion table.
It is built from:

- `assessments`
- `taxes`
- `payments`
- the latest `parcel_summaries` row per parcel

Operational rules:

- Rebuild it after any parser change that alters stored parsed rows.
- Rebuild it after manual backfills or cleanup in the source tables.
- Do not treat existing rows as self-maintaining; the table is only refreshed when `build-parcel-year-facts` runs.

Scope behavior:

- `accessdane build-parcel-year-facts` deletes and recreates all rows in the table.
- `accessdane build-parcel-year-facts --id 061003330128` deletes and recreates rows only for that parcel.
- `accessdane build-parcel-year-facts --ids data/parcel_ids.txt` does the same for the listed parcel set.

Use scoped rebuilds while iterating on a small parser change.
Use a full rebuild after broad parser logic changes or before taking a fresh profile snapshot.

## Data Quality Checks

`accessdane check-data-quality` is the first hard gate after parsing.
It currently checks for:

- duplicate `parcel_summaries` rows
- suspicious assessment dates
- impossible numeric values
- fetch/parse consistency issues

Recommended usage:

```bash
accessdane check-data-quality --out data/quality_report.json --fail-on-issues
```

Use `--fail-on-issues` when the command is part of a scripted workflow and you want the shell step to stop on any detected issue.
Use `--ids` to narrow investigation to a known parcel subset during parser debugging.

Output shape:

- top-level `passed`
- top-level `checks`
- one object per check with `code`, `description`, `passed`, `issue_count`, and `issues`

Treat this command as an integrity screen.
If it fails, inspect the affected parcels before trusting `parcel_year_facts` or any exported analysis.

## Profiling / Audit

`accessdane profile-data` is a descriptive audit snapshot, not a pass/fail gate.
It summarizes:

- row counts across source and derived tables
- missing-section counts for successful fetches
- coverage ratios for parsing and `parcel_year_facts`

Recommended usage:

```bash
accessdane profile-data --out data/profile_report.json
```

Use `--ids` when you want to compare a targeted parcel subset against a recent parser change.

Output shape:

- `scope`
- `counts`
- `missing_sections`
- `coverage`

Use the profile output to answer questions like:

- How many successful fetches are still unparsed?
- How many successful fetches are missing tax or payment rows?
- Does `parcel_year_facts` cover the same parcel-year universe as the current source tables?

## Practical Scenarios

Parser change against one or two fixtures:

```bash
accessdane parse --parcel-id 061003330128 --reparse
accessdane parse --parcel-id 061002213581 --reparse
accessdane parse --parcel-id 061001397011 --reparse
accessdane build-parcel-year-facts --ids data/parcel_ids.txt
accessdane check-data-quality --ids data/parcel_ids.txt
accessdane profile-data --ids data/parcel_ids.txt
```

For tight iteration, prefer `parse` over `run-all`.
`run-all` can now keep downstream work scoped when `--parse-ids` is used, and `--skip-anomalies` can bypass the final anomaly step entirely.
It is still broader than the lowest-level parse commands, so use `parse` first when you want the tightest loop.
Keep `data/parcel_ids.txt` aligned with the parcel IDs you just reparsed when using the scoped follow-up commands.

Full local refresh before analysis:

```bash
accessdane run-all --parse-only --reparse --build-parcel-year-facts
accessdane check-data-quality --out data/quality_report.json --fail-on-issues
accessdane profile-data --out data/profile_report.json
```

Test and validation pass before handoff:

```bash
.venv/bin/python -m pytest
accessdane check-data-quality --out data/quality_report.json --fail-on-issues
accessdane profile-data --out data/profile_report.json
```

Use this sequence when you want to confirm the repo is in a handoff-ready state for another contributor.

## Current Limits

Keep these Sprint 1 limits in mind:

- `parcel_year_facts` is rebuild-driven and does not auto-refresh.
- `parcel_year_facts` intentionally excludes valuation breakout rows and `tax_detail_payments`.
- `profile-data` reports completeness and coverage, but it does not diagnose record-level correctness.
- `check-data-quality` only covers a first pass of integrity checks; it is not a full audit of every field.
