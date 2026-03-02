# Sprint 2 Day 3 Notes

Date: 2026-03-01

## Focus

Inspect the shared 7-parcel cluster that is missing assessment, tax, and payment rows.

## Cluster Investigated

The shared all-missing cluster is:

- `061002140011`
- `061002140131`
- `061002140241`
- `061002191411`
- `061002430211`
- `061002430321`
- `061003305711`

## What Was Checked

For each parcel, the Day 3 inspection reviewed:

- `fetches` metadata and raw HTML paths
- a fresh `parse_page(...)` result against the stored raw HTML
- the rendered HTML structure around the valuation and payment sections
- whether the page contained actual data rows, empty shells, or explicit “not found” messages

## Observed Shared Pattern

All seven parcels have the same high-level shape:

- `status_code = 200`
- no parse error
- parcel summary fields are present
- `assessment_count = 0`
- `tax_count = 0`
- `payment_count = 0`

The page is not blank. It contains:

- parcel summary content
- parcel history / parent-child links
- document links
- a tax payment modal
- a valuation breakout modal

The important detail is that the data sections are structurally empty in the raw HTML:

- the “Valuations by Assessment Year” table renders only headers and no data rows
- the page sets `assessmentTableRowCount = '0'`
- the “Tax Payment History” modal explicitly says `No historic payments found.`
- there is no normal tax table content to parse

## Representative HTML Findings

Representative page traits from the cluster:

- `061002140011` includes `Owner Name = MCFARLAND, VILLAGE OF`
- `061002430211` includes `Owner Names = MCFARLAND, VILLAGE OF`
- the valuation classification legend includes exempt classes, and these parcels appear to use an exempt-style page shape

The cluster is strongly associated with exempt municipal parcels and parcel-history-heavy records, not with typical residential taxable parcel pages.

## Classification

Primary classification:

- source omission / alternate source page type

This does **not** look like a standard parser bug where rows exist in the HTML but our selectors miss them.
The raw pages themselves expose empty valuation and payment sections.

Secondary note:

- there may still be a future parser enhancement opportunity if AccessDane’s report endpoints or client-side data expose additional values for this parcel class

But based on the stored static HTML alone, the missing rows are expected for this page variant.

## Implication For Sprint 2

This changes the Day 3 conclusion materially:

- the shared 7-parcel all-missing cluster should be treated as a known source-page limitation, not the first parser-fix target
- the more actionable parser-gap candidate is now the remaining tax-only missing subgroup from Day 1

That means the next investigation order should be:

1. Keep the 7-parcel cluster documented as an exempt/alternate page type.
2. Move parser-gap inspection toward the tax-only missing parcels.
3. If Sprint 2 later proves the report endpoints expose more data for this class, revisit this cluster then.

## Recommended Follow-Up

Short-term:

- leave the shared 7-parcel cluster out of parser-fix prioritization
- document it as a known source limitation in Sprint 2 working notes

Medium-term:

- consider a small profiling enhancement later in Sprint 2 that distinguishes “empty exempt-style page” from generic “missing section”

That would make future coverage reports more informative without pretending this cluster is currently recoverable from the stored HTML.
