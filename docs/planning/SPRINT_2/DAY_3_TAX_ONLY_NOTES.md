# Sprint 2 Day 3 Tax-Only Follow-Up

Date: 2026-03-01

## Focus

Inspect the tax-only missing subgroup that remained after excluding the 7 all-missing parcels.

## Scope

The tax-only subgroup is the set of successful fetches that have:

- at least one parsed assessment row
- at least one parsed payment row
- zero parsed tax rows

Current size:

- `35` parcels

## Sampled Parcels

Representative sample inspected:

- `061002275801`
- `061002275851`
- `061002275901`
- `061002275911`
- `061002275991`
- `061002320251`
- `061003102759`
- `061002480711`

## Observed Shared Pattern

The sampled pages all follow the same structure:

- assessments are present
- payment-history rows are present
- the tax section is rendered as an explicit empty state

The key source-page signal is:

- the “Tax Information” section explicitly says `No tax information available.`

The payment-history modal is also present, but it is not evidence of actual paid tax rows.
It consists of repeated placeholder rows such as:

- `Tax Year = 2025`, `Date of Payment = No payments found.`

In other words:

- the parser is correctly finding the payment-history table
- the page is explicitly exposing tax absence and payment placeholders
- there is no normal tax table payload to extract into `taxes`

## Aggregate Check

The full tax-only subgroup was checked for the same source signals.

Observed counts across all `35` tax-only parcels:

- `35` contain `No tax information available.`
- `35` contain payment placeholder messaging (`No payments found.` or `No historic payments found.`)

This is a fully consistent source-page pattern, not a scattered parser miss.

## Classification

Primary classification:

- source omission / explicit empty-tax page type

This subgroup should not be treated as a tax parser bug backlog.
The stored HTML is explicitly telling us that tax details are unavailable for this parcel class.

## Important Operational Implication

These pages still contribute parsed `payments` rows because the payment-history modal contains placeholder rows.

That means:

- `payment_count > 0` does not imply actual payment events for this parcel class
- profiling based only on row presence can overstate how much meaningful payment data is available

This is not a parser defect, but it is a useful future profiling/data-quality nuance.

## Recommended Follow-Up

Short-term:

- treat the `35` tax-only parcels as another known source limitation
- do not prioritize tax-parser changes against this subgroup

Medium-term:

- add a profiling or quality distinction between real payment events and placeholder payment rows
- consider a separate “explicit no tax information” metric so these pages are not mixed with generic parser misses

## Updated Prioritization

After this follow-up, neither of the two missing-tax clusters is the best immediate parser-fix target:

- the `7` all-missing parcels are an alternate exempt-style page type with empty valuation and payment sections
- the `35` tax-only parcels are an explicit empty-tax page type with placeholder payment rows

That shifts the next highest-value parser work away from missing-section triage and back toward planned Sprint 2 extraction work, unless a new cluster with real hidden data is found.
