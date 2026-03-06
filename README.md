# AccessDane McFarland Audit

Migration-managed data pipeline for AccessDane parcel records. It can enumerate parcels by TRS, fetch and archive raw parcel HTML, parse assessment/tax/payment data into Postgres, and produce quality checks, profiling output, and derived parcel-year facts for downstream analysis.

## Current scope and limitations

- This is a local operator workflow, not a hosted service or public API.
- It depends on the current AccessDane HTML structure and may need parser updates if the site changes.
- Raw HTML and generated audit artifacts are stored locally and are intentionally not committed to the repo.
- Full-corpus fetch and reparse runs can be long-running; scoped parcel workflows are the practical path for quick iteration.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
accessdane init-db
```

`accessdane init-db` now applies managed database migrations to `head`.
If it detects a pre-Alembic database that already has the legacy schema, it stamps the baseline revision first and then upgrades to `head` as a temporary transition step.
Use `accessdane init-db` as the default migration path.
Direct `alembic` commands use `alembic.ini` unless you explicitly point them at the same `DATABASE_URL`.

For a minimal runtime-only install, use `pip install -e .` instead.
Use `.[dev]` when you want the pytest-based test workflow locally.

## Environment variables

- `DATABASE_URL` (default `sqlite:///data/accessdane.sqlite`)
- `.env` includes a Postgres example connection string.
The app auto-loads `.env` via `python-dotenv`.
Copy `.env.example` to `.env` to customize local settings.
- `ACCESSDANE_BASE_URL` (default `https://accessdane.danecounty.gov`)
- `ACCESSDANE_RAW_DIR` (default `data/raw`)
- `ACCESSDANE_USER_AGENT` (default `AccessDaneAudit/0.1 (+contact)`)
- `ACCESSDANE_TIMEOUT` (seconds, default `30`)
- `ACCESSDANE_RETRIES` (default `3`)
- `ACCESSDANE_BACKOFF` (seconds, default `1.5`)

## Contributor Workflow

Fresh contributor bootstrap:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
accessdane init-db
```

Manual migration workflow:

```bash
accessdane init-db
```

If you intentionally want raw Alembic commands instead, make sure they target the same database URL as the app.

Automated test workflow:

```bash
.venv/bin/python -m pytest
```

Full-dataset validation workflow after broad parser or transform changes:

```bash
accessdane run-all --parse-only --reparse --build-parcel-year-facts
accessdane check-data-quality --out data/quality_report.json --fail-on-issues
accessdane profile-data --out data/profile_report.json
```

For a quick parcel-subset smoke test, prefer the lower-level scoped commands in the Operations section instead of this full-dataset `run-all` path.

## Typical flow

1. Enumerate TRS blocks (township/range + sections with splits).
2. Resolve TRS blocks to parcel IDs (data source or endpoint).
3. Scrape each parcel page and save raw HTML.
4. Parse assessment, tax, and payments.
5. Run anomaly detection on parsed data.

The TRS-to-parcel resolution step is intentionally left configurable. Add your data source in `src/accessdane_audit/trs.py` or feed a CSV into the CLI.

## CLI examples

```bash
accessdane enumerate-trs --trs 06/10 --sections 1-36 --split 1,2,3 --out data/trs_blocks.csv
accessdane search-trs-csv --trs-csv data/trs_blocks.csv --auto-split --out data/parcel_ids.txt
accessdane fetch --ids data/parcel_ids.txt
accessdane parse --unparsed
accessdane build-parcel-year-facts
accessdane check-data-quality
accessdane profile-data
accessdane ingest-retr --file data/sample_retr.csv
accessdane match-sales
accessdane report-sales-matches --out data/sales_match_audit.json
accessdane anomalies --out data/anomalies.json
```

Operational notes:

- `build-parcel-year-facts` rebuilds a derived table from `assessments`, `taxes`, `payments`, and the latest `parcel_summaries`.
- The rebuild is destructive within its scope: a full run replaces all `parcel_year_facts` rows, while `--id` and `--ids` only replace rows for the selected parcels.
- `parse` now also maintains `parcel_characteristics` and an incremental deduplicated `parcel_lineage_links` layer during normal parse-time storage.
- `TaxRecord.data` for `source == "detail"` now includes structured modal-derived fields such as `tax_credit_rows`, `special_charge_rows`, `installment_rows`, and `tax_amount_summary` in addition to the raw detail `rows`.
- `run-all --build-parcel-year-facts` now keeps the rebuild scoped to the current run when `--parse-ids` is used.
- `check-data-quality` and `profile-data` both support `--ids` to limit runs to a parcel subset and `--out` to write JSON for later review.
- `check-data-quality --fail-on-issues` exits with status code `1` when any check fails, which is useful for CI or repeatable local gates.
- `report-sales-matches` emits a JSON audit snapshot of sales-match confidence tiers, method counts, and unresolved/ambiguous/low-confidence review queues.
- `run-all --skip-anomalies` skips the final anomaly JSON step for faster parser iteration.
- `parse` supports `--resume-after-fetch-id` and `--limit` so long reparses can resume in smaller chunks.
- `run-all --parse-only` supports `--parse-resume-after-fetch-id` and `--parse-limit` for the same chunked parse-stage control.
- Both parse paths now print a final parse summary with selected, succeeded, failed, and skipped fetch counts.

To reparse and replace existing rows for each fetch:

```bash
accessdane parse --reparse
```

Single command:

```bash
accessdane run-all --trs 06/10 --sections 1-36 --split-quarters --split-quarter-quarters --auto-split
```

To also rebuild `parcel_year_facts` in the same run:

```bash
accessdane run-all --parse-only --reparse --build-parcel-year-facts
```

To keep a scoped parse-only iteration from writing anomalies:

```bash
accessdane run-all --parse-only --reparse --parse-ids data/parcel_ids.txt --skip-anomalies
```

To rebuild `parcel_year_facts` for only a subset of parcels:

```bash
accessdane build-parcel-year-facts --ids data/parcel_ids.txt
```

To run data-quality checks and fail the shell step when issues are found:

```bash
accessdane check-data-quality --out data/quality_report.json --fail-on-issues
```

To profile only a selected parcel subset and save the JSON snapshot:

```bash
accessdane profile-data --ids data/parcel_ids.txt --out data/profile_report.json
```

To force reparsing (replace existing rows) during run-all:

```bash
accessdane run-all --trs 06/10 --sections 1-36 --split-quarters --split-quarter-quarters --auto-split --reparse
```

Parse-only (no TRS search or fetching):

```bash
accessdane run-all --parse-only --reparse
```

Parse-only for a subset of parcel IDs:

```bash
accessdane run-all --parse-only --reparse --parse-ids data/parcel_ids.txt
```

Resume a long parse-only run from a later fetch id and stop after a bounded chunk:

```bash
accessdane run-all --parse-only --reparse --parse-resume-after-fetch-id 120000 --parse-limit 500 --skip-anomalies
```

Resume the direct parse command after a known fetch id:

```bash
accessdane parse --reparse --resume-after-fetch-id 120000 --limit 500
```

Debug parsing output (counts per parcel):

```bash
accessdane run-all --parse-only --reparse --debug-parse --debug-only-empty --debug-limit 50
```

To triage raw HTML fixtures for parser test coverage:

```bash
.venv/bin/python scripts/fixture_scan.py --max-files 50 --limit 5
.venv/bin/python scripts/fixture_scan.py --sample-only --sample 061002122981
```

Use `--max-files` for quick corpus samples, or `--sample-only` to inspect specific parcel IDs without scanning the full `data/raw/` directory.

Use `--split-quarters` on `enumerate-trs` to split all sections into quarters rather than a subset.
Use `--split-quarter-quarters` to further expand quarter rows into quarter-quarters (defaults to NE,NW,SE,SW).
Use `--cap` to limit the number of rows emitted (useful for testing).

`search-trs-csv` also supports `--cap` to limit the number of parcel IDs written.
`search-trs` supports `--cap` as well, and `fetch` already has `--limit` for similar testing control.

`search-trs` defaults to municipality id `50` (McFarland). Override with `--municipality-id` if needed.

The TRS CSV includes `quarter` and `quarter_quarter` columns; by default quarter_quarter is blank so `search-trs-csv` can expand it when quarters are present.

## Operations

Recommended rebuild loop from an existing raw HTML corpus:

```bash
accessdane init-db
accessdane run-all --parse-only --reparse --build-parcel-year-facts
accessdane check-data-quality --out data/quality_report.json
accessdane profile-data --out data/profile_report.json
```

Use `run-all --parse-only --reparse` when parser logic changes and you want stored parsed rows replaced from existing `fetches.raw_path` files.
Re-run `build-parcel-year-facts` after any reparse or manual edits to source tables so the derived table stays in sync.
The same reparse path still refreshes the parse-time source tables, while `rebuild-parcel-characteristics` and `rebuild-parcel-lineage-links` can repair those derived Sprint 2 outputs independently.
`parse --reparse` and `run-all --parse-only --reparse` now both commit work fetch-by-fetch, so one bad parcel is less likely to roll back the entire run.
Use `--resume-after-fetch-id` with `parse` or `--parse-resume-after-fetch-id` with `run-all` to resume after a known successful checkpoint.
Use `--limit` or `--parse-limit` to break a full-corpus reparse into bounded chunks.
For a quick scoped smoke run on a small parcel subset, prefer `parse`, `build-parcel-year-facts --ids`, `check-data-quality --ids`, and `profile-data --ids` directly.
If you do use `run-all` for scoped parser iteration, `--parse-ids` now keeps `build-parcel-year-facts` and anomaly generation scoped to the same parcel subset.
`run-all --parse-only` also keeps downstream rebuilds scoped to the actual parse-stage subset when `--parse-limit` or `--parse-resume-after-fetch-id` is used.
Use `--skip-anomalies` when you want `run-all` to stop after parsing (and optional fact rebuild) without writing anomaly output.
On the current local McFarland corpus, the full parse-only rebuild path can take materially longer than a quick smoke test because it processes thousands of stored fetches.

`check-data-quality` returns a JSON object with top-level `passed` and a `checks` list.
Each check includes `code`, `description`, `passed`, `issue_count`, and `issues`.
It now also checks the new extraction-layer consistency rules, including exempt-style classification flags, lineage link sanity, and placeholder payment-history mismatches.
Use it to catch concrete integrity failures before relying on downstream analysis.

`profile-data` returns a JSON object with `scope`, `counts`, `missing_sections`, `coverage`, and `tax_detail_field_presence`.
`counts` and `coverage` now include `parcel_characteristics` / lineage completeness, while `tax_detail_field_presence` shows how often key structured fields are populated on `source == "detail"` tax rows.
Use it to measure how complete the current local dataset is, not as a correctness gate.

`ingest-retr --file ...` keeps imported non-comparable transfers visible in `sales_transactions` and now writes active `sales_exclusions` rows for the initial v1 rules: non-arms-length, non-usable-sale, family-transfer, government-transfer, and corrective-deed signals.
Those exclusions are synced on same-file re-imports so the importer updates existing exclusion rows instead of duplicating them.
Use `match-sales` and `report-sales-matches` after each RETR import to refresh candidate links and produce an operator-facing review-queue snapshot.

The AccessDane `Summary Report` / `Custom Report` endpoints were evaluated during Sprint 2 and are intentionally not part of the fetch pipeline right now.
They currently resolve to a PDF print flow rather than a cleaner machine-readable data source, so the project continues to rely on the main parcel HTML pages.

More detailed operational guidance lives in [docs/planning/SPRINT_1/OPERATIONS.md](docs/planning/SPRINT_1/OPERATIONS.md).
Sprint 3 RETR-specific operations live in [docs/planning/SPRINT_3/RETR_OPERATIONS.md](docs/planning/SPRINT_3/RETR_OPERATIONS.md).
Sprint 4 permit and appeal import operations live in
[docs/planning/SPRINT_4/PERMIT_OPERATIONS.md](docs/planning/SPRINT_4/PERMIT_OPERATIONS.md)
and
[docs/planning/SPRINT_4/APPEAL_OPERATIONS.md](docs/planning/SPRINT_4/APPEAL_OPERATIONS.md).

## Notes

- Raw HTML is stored on disk under `data/raw/` and indexed in the DB.
- Parsing is tolerant and schema-light; it stores JSON blobs so you can refine the extraction rules later.
- Parcel Summary fields are persisted in `parcel_summaries` when present. The table enforces a unique `parcel_id`.
- Assessment summary fields are also stored as typed columns in `assessments` (while keeping JSON).
- Schema changes should now be made through Alembic migrations rather than ad hoc runtime DDL.
