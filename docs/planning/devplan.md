# McFarland Assessment Fraud Detection Development Plan

## Purpose

Build a defensible analysis stack that can identify parcels, neighborhoods, or assessment workflows in McFarland that deserve further review for possible assessment manipulation, systematic undervaluation, or unequal treatment.

This plan assumes:

- The current repository already handles McFarland parcel discovery, page scraping, HTML storage, and basic parsing into Postgres.
- "Fraud detection" here means risk detection and evidence assembly, not automatic proof of criminal conduct.
- Every sprint below is two 40-hour work weeks (80 hours total).

## Current Baseline

The repository already provides a usable ingestion foundation:

- TRS enumeration and AccessDane parcel search for McFarland (`municipality_id=50` by default).
- Parcel page fetch and raw HTML persistence under `data/raw/`.
- HTML parsing into `parcels`, `fetches`, `assessments`, `taxes`, `payments`, and `parcel_summaries`.
- A minimal anomaly pass that only flags missing parsed sections and negative amounts.

Observed local assets in this workspace:

- `data/trs_blocks.csv` contains 577 TRS rows.
- `data/parcel_ids.txt` contains 2,657 parcel IDs.
- `data/raw/` currently contains 2,657 raw parcel pages.
- `data/anomalies.json` currently contains only simple parsing/data-completeness anomalies.

Current gaps:

- No external market-sale data.
- No parcel-to-sale matching layer.
- No annual parcel snapshot mart.
- No comparable-sales engine.
- No fraud-specific features, scoring, or case workflow.
- No test suite.
- Schema evolution is ad hoc (`create_all` plus manual `ALTER TABLE`), which will not scale.

## Investigation Principles

The tools should focus on measurable signals that can be explained and audited:

- Sale ratio analysis: compare assessed value to arms-length sale price.
- Uniformity analysis: compare each parcel to peers by class, age, size, neighborhood, and year.
- Time-series analysis: look for unexplained assessment stagnation or abrupt favorable changes.
- Improvement lag analysis: look for permits or visible improvements without corresponding assessed-value changes.
- Process analysis: look for patterns around ownership, appeals, parcel splits, and reclassifications.

The system should never label a parcel as "fraud" without human review. It should produce:

- A risk score.
- A ranked list of reason codes.
- An evidence packet showing why the parcel was flagged.

## What To Build

The target system should add six major capabilities on top of the current scraper/parser.

### 1. Stable Data Platform

Build a durable Postgres-first analytics layer:

- Adopt schema migrations (Alembic or equivalent).
- Add reproducible load metadata for every import job.
- Create canonical current-state and historical parcel views.
- Add data-quality tests and parser regression tests.

### 2. Expanded Parcel History Extraction

Extend parsing so AccessDane becomes a richer parcel history source:

- Capture more parcel characteristics from the parcel detail sections.
- Preserve annual assessment snapshots in a normalized form.
- Record parcel lineage (parent/child splits, mergers) when exposed by the site.
- Capture richer tax-line details that may indicate exemptions or special assessments.

### 3. External Market Truth Data

Bring in actual transaction data, starting with public Wisconsin transfer-return data:

- Ingest Wisconsin DOR RETR exports for McFarland and nearby comparison areas.
- Normalize sale amount, conveyance date, arms-length indicator, transfer type, property type, and exemption fields.
- Match sales to local parcel IDs and maintain confidence scores.
- Exclude or separately classify non-market transfers.

### 4. Context Data For Causality Checks

Acquire data that explains legitimate assessment changes:

- Building permits and remodel permits.
- Board of Review or appeal outcomes, if available.
- Parcel splits/combines and legal-description changes.
- Optional parcel geometry or neighborhood boundaries for spatial comparable selection.

### 5. Fraud Signal Engine

Compute explainable indicators:

- Sales ratio outliers.
- Unequal appreciation versus peer group.
- Permitted improvements without expected value changes.
- Repeated favorable deviations after sale, transfer, or appeal events.
- Clusters of unusually low assessments by owner, street, subdivision, or workflow.

### 6. Investigator Workflow

Provide analyst-facing tools:

- Ranked queue of flagged parcels.
- Parcel dossier with timeline, comps, and source records.
- Exportable CSV/JSON/Markdown evidence bundles.
- Simple dashboard or report runner for recurring review.

## Recommended Data Sources

### Priority 1: Wisconsin DOR Property Sales / RETR

Use this as the primary source of actual sale amounts and transaction characteristics.

Why it matters:

- It provides actual transferred value and conveyance metadata.
- It supports filtering to arms-length sales.
- It is the strongest public "ground truth" for assessment fairness analysis.

What to ingest:

- Sale amount.
- Value subject to fee.
- Conveyance date.
- Recording date.
- Transfer type and conveyance document type.
- Exemption code.
- Arm's-length indicator.
- Property type / predominant use / assessor classification fields.
- County document number and document locator number.
- Official parcel number when present.
- Property address and legal description.

Acquisition strategy:

- Start with manual CSV exports from the Wisconsin DOR property information site for McFarland and nearby municipalities.
- Build an importer for those exported CSVs first.
- Only automate extraction later if the export format is stable and terms of use allow it.

Important caution:

- Wisconsin DOR explicitly treats this as public RETR data, but the site disclaimer says it is informational and not intended as the only basis for detailed site-specific analysis. Use it as a primary signal, then corroborate with local parcel, permit, and deed data.

### Priority 2: Dane County Register of Deeds Metadata

Use this to validate sale events, fix ambiguous parcel matches, and support deed-chain analysis.

What to ingest:

- Recording number / document number.
- Recording date.
- Grantor and grantee names where available.
- Deed type.
- Linked parcel references if obtainable.

Acquisition strategy:

- Prefer an export or records request over brittle scraping.
- Use it as a reconciliation layer rather than the first-line market dataset.

### Priority 3: McFarland Building Permit Data

Use this to detect likely under-assessment after improvements.

What to ingest:

- Permit number.
- Parcel address.
- Issue date and final inspection date.
- Permit type.
- Declared project value, if available.
- Notes describing the scope of work.

Acquisition strategy:

- Expect this to start as manual CSV entry, PDF extraction, or a formal records request.
- Design the importer so it can handle hand-curated spreadsheets before any automation exists.

### Priority 4: Appeal / Board of Review Data

Use this to distinguish legitimate downward adjustments from suspicious patterns.

What to ingest:

- Parcel ID or address.
- Appeal filing date.
- Claimed value and final value.
- Outcome.
- Basis for appeal, if documented.

Acquisition strategy:

- Treat this as a manual or public-records-request dataset.
- Keep the import path simple and resilient to sparse data.

### Priority 5: Spatial / Neighborhood Context

Use this to improve comparable selection.

What to ingest:

- Parcel geometry or centroid.
- Subdivision, neighborhood, tax district, or school district boundaries.

Acquisition strategy:

- This is optional for the first fraud engine release.
- If acquired, enable PostGIS and calculate distance-based comparables.

## Proposed Data Model Additions

Add the following tables or materialized views in Postgres.

### Core Operational Tables

- `source_loads`
  - One row per file or job import.
  - Tracks source system, file name, checksum, load time, row counts, and status.

- `parser_runs`
  - Tracks parser version, fetch coverage, and parse-error counts.

### Parcel History Tables

- `parcel_characteristics`
  - One row per parcel per fetch (or per effective year when known).
  - Stores lot size, land area, building style, finished area, year built, bedrooms, baths, class codes, and any extracted physical descriptors.

- `parcel_lineage`
  - Parent-child parcel relationships for splits, merges, and renumbering.

- `parcel_year_facts`
  - Canonical yearly parcel grain.
  - One row per parcel per tax/assessment year with normalized assessment, tax, exemption, and characteristic fields.

### External Market Tables

- `sales_transactions`
  - One row per transfer-return transaction.
  - Stores raw and normalized sale fields plus arm's-length and exclusion flags.

- `sales_parcel_matches`
  - Links sales to parcel IDs.
  - Stores match method, confidence score, and review status.

- `sales_exclusions`
  - Explicitly records why a transaction is excluded from fairness studies.

### Context Tables

- `permit_events`
  - One row per permit or improvement event.

- `appeal_events`
  - One row per appeal / Board of Review event.

- `parcel_geometries`
  - Optional geometry store if GIS data is added.

### Analytics Tables

- `parcel_features`
  - Feature vectors by parcel-year or parcel-sale event.

- `fraud_scores`
  - Scoring outputs by parcel, score date, model/ruleset version.

- `fraud_flags`
  - Individual explainable reason codes tied to a score run.

- `case_reviews`
  - Human review tracking for triage, notes, and dispositions.

## Required Tools And Interfaces

Build these as CLI commands first, then expose selected outputs in a lightweight dashboard.

### Data Ingestion Commands

- `accessdane ingest-retr --file ...`
  - Load Wisconsin DOR RETR CSV exports into `sales_transactions`.

- `accessdane ingest-permits --file ...`
  - Load McFarland permit spreadsheets or manually curated CSVs.

- `accessdane ingest-appeals --file ...`
  - Load appeal or Board of Review records.

- `accessdane rebuild-year-facts`
  - Build or refresh the canonical parcel-year mart.

### Matching And Reconciliation Commands

- `accessdane match-sales`
  - Match `sales_transactions` to parcel IDs using parcel number, address, legal description, and date heuristics.

- `accessdane audit-matches`
  - Output unresolved, low-confidence, or many-to-many matches for review.

### Analytics Commands

- `accessdane build-features`
  - Generate parcel-year and parcel-sale features.

- `accessdane score-fraud`
  - Run rules and/or models to populate `fraud_scores` and `fraud_flags`.

- `accessdane sales-ratio-study`
  - Generate median ratio, COD, PRD, and outlier reports by class and area.

- `accessdane parcel-dossier --parcel-id ...`
  - Produce a single parcel investigation packet.

### Dashboard / Reporting

After the CLI foundation is stable, add one lightweight interface:

- Option A: Streamlit dashboard for ranked triage, parcel search, and flag drill-down.
- Option B: Static report generator that outputs HTML/Markdown dossiers plus CSV exports.

The dashboard should consume tables already produced by the CLI, not contain business logic.

## Detection Methodology Specification

The first release should be rule-based and transparent. Machine learning can come later.

### Core Fraud-Risk Signals

- `low_assessment_to_sale_ratio`
  - Parcel sold at a value materially above assessed value after filtering to arms-length sales.

- `persistent_underassessment_vs_peers`
  - Parcel remains materially below peer median over multiple years.

- `post_improvement_stagnation`
  - Permit activity suggests value-adding improvements, but assessed value does not move as expected.

- `unusual_reclassification`
  - Classification, exemption, or valuation treatment changes in a way that materially lowers taxes relative to similar parcels.

- `appeal_pattern_outlier`
  - Repeated successful reductions concentrated in specific owners, neighborhoods, or workflow paths.

- `parcel_lineage_value_drop`
  - Split/merge activity coincides with suspicious value resets.

### Peer Grouping Rules

At minimum, peer grouping should consider:

- Municipality.
- Assessment year.
- Property class.
- Approximate lot size bucket.
- Approximate living area or improvement-size bucket.
- Year built bucket.
- Geographic proximity or subdivision when available.

### Comparable Sale Rules

The comparable-sales engine should:

- Use only arms-length transactions by default.
- Prefer same municipality, then nearby comparable areas.
- Weight by recency, size similarity, class similarity, and distance.
- Emit the selected comp set and the reason each comp was included.

### Scoring Output

Each scored parcel should produce:

- A normalized score from 0-100.
- One or more reason codes.
- Supporting metrics and thresholds.
- Links back to source rows (assessment, sale, permit, appeal).

## Delivery Sequence

The order should be:

1. Harden the current pipeline and create a trustworthy parcel-year mart.
2. Expand local parcel extraction so the internal dataset is richer before external joins.
3. Add market-sale truth data and reconcile it to parcel IDs.
4. Add permits, appeals, and optional spatial context to explain legitimate changes.
5. Build explainable risk features and scoring.
6. Build analyst workflow, reporting, and review loops.

This order prevents early time being wasted on dashboards or models before the data foundation is reliable.

## Sprint Plan

## Sprint 1 (80 hours): Harden The Existing Pipeline

Goal:

Make the current ingestion and parsing stack reliable enough to support downstream analytics.

Build:

- Add schema migration tooling and migrate the current ad hoc schema into managed migrations.
- Add parser regression fixtures from a representative sample of raw HTML files.
- Add data-quality checks for duplicate parcel summaries, invalid dates, impossible numeric ranges, and fetch/parse coverage.
- Build `parcel_year_facts` v1 from existing assessments, taxes, payments, and parcel summaries.
- Add a reproducible profiling report that summarizes current McFarland coverage and parse quality.

Deliverables:

- Migration framework committed and documented.
- Initial automated test suite for parser and data normalization.
- `accessdane profile-data` or equivalent data-audit command.
- `parcel_year_facts` v1 table or materialized view.

Acceptance criteria:

- A full rebuild from current raw HTML produces consistent row counts.
- Test coverage exists for the main parse branches.
- Analysts can query one normalized row per parcel-year for current basic fields.

Estimated effort split:

- 20h migrations and schema cleanup.
- 24h tests and fixtures.
- 20h parcel-year mart.
- 16h data profiling and documentation.

## Sprint 2 (80 hours): Expand AccessDane Extraction

Goal:

Extract more useful property characteristics and local context from AccessDane before external matching begins.

Build:

- Extend `parse.py` to capture additional parcel-detail fields and normalize them into `parcel_characteristics`.
- Capture parcel lineage if parent/child parcel history is present.
- Normalize richer tax detail fields, including exemptions or special charge indicators when available.
- Evaluate whether the AccessDane "Summary Report" / "Custom Report" endpoints provide richer structured data, and, if useful, add an optional fetch path.
- Add parser fixtures specifically for edge-case parcels: condos, vacant land, split lots, exempt parcels, and parcels with sparse tax history.

Deliverables:

- New normalized `parcel_characteristics` table.
- Optional report-fetch extension if it materially improves data quality.
- Parser test fixtures covering new extraction paths.

Acceptance criteria:

- At least one stable characteristic set is available for most residential parcels.
- Richer fields are linked back to the originating fetch.
- No regressions in the current assessment/tax/payment extraction.

Estimated effort split:

- 36h parser expansion.
- 16h schema additions.
- 20h test coverage.
- 8h profiling and field completeness review.

## Sprint 3 (80 hours): Ingest And Match Market Sales

Goal:

Bring in actual transaction amounts and build the sale-to-parcel matching layer.

Build:

- Define a stable import schema for Wisconsin DOR RETR CSV exports.
- Build `accessdane ingest-retr --file ...`.
- Normalize common exclusion logic for non-arms-length or non-comparable transfers.
- Build `sales_parcel_matches` and a multi-step matcher:
  - exact official parcel number match
  - normalized address match
  - legal-description fallback
  - manual review queue for unresolved cases
- Build a match audit report for ambiguous or low-confidence links.

Deliverables:

- `sales_transactions`, `sales_parcel_matches`, and `sales_exclusions`.
- Import documentation describing how to export and load McFarland RETR data.
- Match-quality report with confidence tiers.

Acceptance criteria:

- McFarland RETR exports can be loaded without manual SQL.
- Most arms-length residential sales match directly or with explainable heuristics.
- Ambiguous cases are isolated into a review list instead of silently linked.

Estimated effort split:

- 24h import pipeline.
- 28h matching and reconciliation rules.
- 16h exclusion logic and auditing.
- 12h test fixtures and docs.

## Sprint 4 (80 hours): Add Permits, Appeals, And Context

Goal:

Add the non-sale context needed to separate suspicious behavior from legitimate valuation changes.

Build:

- Build `accessdane ingest-permits --file ...` with a forgiving schema and field-normalization layer.
- Build `accessdane ingest-appeals --file ...`.
- Design simple CSV templates for manual or records-request data entry.
- Add `permit_events` and `appeal_events`.
- Add optional support for parcel geometry or point coordinates; enable PostGIS only if geometry is actually available.
- Build joins from permits/appeals into `parcel_year_facts`.

Deliverables:

- Importers for permit and appeal datasets.
- Reusable spreadsheet templates for manual curation.
- Updated parcel-year mart with permit/appeal rollups.

Acceptance criteria:

- A permit or appeal record can be attached to a parcel and surfaced in parcel timelines.
- Missing or messy manual data does not break the pipeline.
- The data model supports future records requests without redesign.

Estimated effort split:

- 28h permit import path.
- 18h appeals import path.
- 18h parcel-year integration.
- 16h templates, docs, and edge-case handling.

## Sprint 5 (80 hours): Build The Fraud Signal Engine

Goal:

Produce the first explainable scoring system.

Build:

- Build `accessdane sales-ratio-study` to calculate sales-ratio metrics by year, class, and area.
- Build `accessdane build-features` to generate parcel-level signals:
  - assessment-to-sale ratio
  - peer percentile
  - year-over-year value change
  - permit-adjusted expected value change
  - appeal outcome deltas
  - parcel lineage effects
- Build `accessdane score-fraud` as a transparent rules engine first.
- Store outputs in `parcel_features`, `fraud_scores`, and `fraud_flags`.
- Add threshold tuning notebooks or SQL scripts for calibration.

Deliverables:

- Repeatable feature pipeline.
- Versioned ruleset and reason codes.
- Ranked parcel output with traceable evidence.

Acceptance criteria:

- Each flagged parcel can be explained in plain language.
- Analysts can reproduce why a parcel was scored from underlying data.
- The output supports both parcel-level review and neighborhood-level pattern analysis.

Estimated effort split:

- 24h sales-ratio study logic.
- 28h feature engineering.
- 20h scoring engine and reason codes.
- 8h calibration support.

## Sprint 6 (80 hours): Investigation Workflow And Reporting

Goal:

Turn the analytics output into an investigation tool that a human can actually use.

Build:

- Build `accessdane parcel-dossier --parcel-id ...` to output a parcel timeline with:
  - assessment history
  - matched sales
  - peer comparisons
  - permit/appeal events
  - reason codes
- Build batch reporting for top-N flagged parcels.
- Build a lightweight dashboard or static HTML report interface.
- Add `case_reviews` for analyst notes, dispositions, and false-positive tracking.
- Add a feedback loop so reviewed cases can refine thresholds and exclusions.
- Write a methodology memo describing what the score means and what it does not mean.

Deliverables:

- Parcel dossier generator.
- Ranked review queue.
- Minimal dashboard or static report runner.
- Human-review tracking.

Acceptance criteria:

- An analyst can open one parcel and see the full evidence chain without direct SQL.
- Review outcomes can be stored and used to improve future runs.
- The tool clearly distinguishes "risk signal" from "proof."

Estimated effort split:

- 24h dossier/report generation.
- 20h dashboard or HTML reporting.
- 16h case review workflow.
- 20h documentation, calibration feedback, and final hardening.

## Nice-To-Have Sprint 7 (80 hours): Automation And Ongoing Monitoring

This sprint is optional but valuable if the tool will be used continuously rather than as a one-time investigation.

Build:

- Scheduled refresh jobs.
- Parser drift detection when AccessDane markup changes.
- Load monitoring and alerting.
- Annual refresh workflow for new assessment rolls and new RETR exports.
- Benchmark packs for recurring fairness studies.

Deliverables:

- Operational runbook.
- Scheduled refresh scripts.
- Drift and load diagnostics.

## First Release Definition

The minimum useful release is the end of Sprint 5.

At that point, you should have:

- A stable Postgres analytics schema.
- A normalized parcel-year mart.
- McFarland sales loaded from Wisconsin DOR RETR exports.
- Permit and appeal imports (even if initially manual).
- An explainable risk score with parcel-level reasons.

Sprint 6 then makes that usable for ongoing investigation work.

## Key Risks And Mitigations

- Risk: Sales data quality is noisy or over-inclusive.
  - Mitigation: enforce explicit exclusion codes, confidence scoring, and manual review queues.

- Risk: AccessDane markup changes break parsing.
  - Mitigation: parser fixtures, regression tests, and parser-run monitoring.

- Risk: Permit and appeal data are messy or unavailable.
  - Mitigation: support manual CSV imports and treat these datasets as optional enrichments, not hard dependencies.

- Risk: The score overstates certainty.
  - Mitigation: keep v1 rule-based, transparent, and framed as triage only.

- Risk: Parcel identity changes after splits/mergers.
  - Mitigation: explicitly model parcel lineage before strong longitudinal claims are made.

## Immediate Next Actions

Start with Sprint 1, not the sales import.

The existing codebase is already close to a usable parcel-ingestion platform, but the next bottleneck is trustworthiness of the local data model. A clean parcel-year mart, migration discipline, and parser tests will reduce rework in every later sprint.

Once Sprint 1 is complete, Sprint 2 and Sprint 3 can proceed with far less risk.

## External Source Notes (Verified March 1, 2026)

- Wisconsin DOR Property Information / RETR public access exists and exposes public real estate transfer data, including export-oriented search workflows:
  - https://propertyinfo.revenue.wi.gov/wisconsinprod/Main/Home.aspx
  - https://propertyinfo.revenue.wi.gov/WisconsinProd/
- Wisconsin DOR also publishes RETR field definitions that are useful for schema design:
  - https://propertyinfo.revenue.wi.gov/WisconsinProd/Custom/RETRtemp.pdf
- Wisconsin DOR states RETR filing moved into My Tax Account in January 2026, which matters if any future automation depends on filing-system assumptions:
  - https://www.revenue.wi.gov/Pages/SLF/COTVC-News/2026-01-12.aspx
- McFarland confirms building permits are handled through its Community & Economic Development process, which supports a permit-data acquisition track even if it starts manually:
  - https://www.mcfarland.wi.us/173/Building-Remodeling
  - https://www.mcfarland.wi.us/176/When-is-a-Building-Permit-Required
- Dane County provides official public-records request workflows, which is the likely path for deed metadata, appeal records, or other non-exported records:
  - https://link.countyofdane.com/record-requests
  - https://link.countyofdane.com/Records-Request-Form/county-clerk
