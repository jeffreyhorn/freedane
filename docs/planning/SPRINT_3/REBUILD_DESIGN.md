# Sprint 3 Rebuild Design

Prepared on: 2026-03-02

Branch: `sprint3/day-02-rebuild-design`

## Purpose

Define the first dedicated rebuild paths for the Sprint 2 derived outputs:

- `parcel_characteristics`
- `parcel_lineage_links`

The main goal is to stop treating these tables as parse-time side effects only.
They should become explicitly rebuildable from the already-fetched, already-parsed local dataset in the same way `parcel_year_facts` is rebuildable today.

## Current State

Today, both outputs are maintained only during `_store_parsed(...)` in [cli.py](../../../src/accessdane_audit/cli.py):

- `_upsert_parcel_characteristic(...)`
- `_upsert_parcel_lineage_links(...)`

That means:

- they only refresh when a fetch is reparsed
- they do not have a standalone repair or refresh command
- operational recovery is awkward if the extraction logic changes but the underlying parsed corpus does not need a full reparse

The baseline behavior that the rebuild path must preserve is:

- `parcel_characteristics` keeps one row per `parcel_id`
- `parcel_lineage_links` keeps one normalized row per (`parcel_id`, `related_parcel_id`, `relationship_type`)
- both outputs currently rank competing fetches by deterministic precedence rules

## Design Goals

- Rebuild from local database state plus stored raw HTML only. No network activity.
- Support both full-dataset and parcel-scoped rebuilds.
- Reuse the existing extraction logic instead of introducing a parallel parser.
- Make the rebuild deterministic: the same input corpus should produce the same output rows.
- Avoid one giant transaction for the whole corpus.
- Make failures visible and isolated, not silent.

Important practical constraint:

- `parcel_characteristics` cannot be rebuilt from database rows alone today because `_build_parcel_characteristic_candidate(...)` depends on the in-memory `parsed` object produced during parsing.
- V1 therefore must either:
  - reparse the local raw HTML inside the rebuild command to produce that in-memory parsed structure, or
  - refactor the characteristic builder so it can operate from raw HTML plus already-persisted tables without requiring the parse-time object
- Day 3 should treat this as an explicit implementation task, not an incidental detail.

## Non-Goals For V1

- No new schema changes.
- No new load-tracking tables for rebuild runs.
- No attempt to rebuild from partially missing raw HTML beyond skipping unavailable source rows.
- No field-level merge redesign for `parcel_characteristics`; preserve the current winner-takes-row behavior.

## Shared Source Universe

Both rebuild commands should operate from the same eligible fetch universe.

V1 eligible fetch criteria:

- `status_code = 200`
- `parsed_at IS NOT NULL`
- `parse_error IS NULL`
- `raw_path IS NOT NULL`

Additional runtime rule:

- if `raw_path` is set but the file is missing on disk, skip that fetch and count it as a skipped source, not a hard crash for the entire run

Reasoning:

- the rebuild logic depends on the saved raw HTML for HTML-derived extraction
- the commands should stay aligned with the same data-quality assumptions already enforced elsewhere in the pipeline

## Proposed CLI Surface

Use two explicit commands for clarity and operational symmetry:

```bash
accessdane rebuild-parcel-characteristics
accessdane rebuild-parcel-lineage-links
```

Shared scoping options:

- `--id <parcel_id>` for a single parcel
- `--ids <path>` for a newline-delimited parcel ID file

Scoping semantics:

- if neither `--id` nor `--ids` is provided, rebuild the full eligible dataset
- `--id` and `--ids` are mutually exclusive

Recommended output summary for both commands:

- parcels selected
- eligible fetches scanned
- rows inserted or updated
- rows deleted as part of scoped replacement
- skipped fetches
- parcel-level failures

Exit behavior:

- exit `0` if all selected parcels finish without parcel-level failures
- exit non-zero if any selected parcel fails

## Transaction Model

Do not use one transaction for the full rebuild.

V1 transaction behavior:

- precompute the target parcel list first
- process one parcel at a time
- each parcel rebuild runs in its own transaction
- replace data parcel-by-parcel, not with one global upfront delete
- if one parcel fails:
  - roll back only that parcel
  - record the failure in the command summary
  - continue processing remaining parcels

Reasoning:

- this matches the safer direction already adopted for `parse --reparse`
- it keeps large rebuilds from being all-or-nothing
- it limits rollback scope while remaining simple to implement

## `parcel_characteristics` Rebuild Rules

### Grain

One row per `parcel_id`.

### Replacement Semantics

Full rebuild:

- treat every eligible parcel as selected scope
- for each selected parcel, replace only that parcel's existing row inside that parcel's transaction
- do not wipe the entire table up front

Scoped rebuild:

- delete only `parcel_characteristics` rows for the selected parcel IDs
- recompute only those selected parcel IDs
- do not touch rows outside the selected scope

### Winner Selection

For each selected parcel:

1. Gather all eligible fetches for that parcel.
2. Produce the required parse context for each fetch:
   - either by reparsing the stored raw HTML into the same in-memory parsed structure used by `_store_parsed(...)`, or
   - by using a refactored equivalent helper that no longer requires the parse-time object
3. Build a candidate from each fetch using the existing `_build_parcel_characteristic_candidate(...)` logic or a refactored equivalent.
4. Discard fetches that produce no candidate.
5. Select the single winner by the same rank tuple already used today:
   - characteristic completeness
   - `fetch.fetched_at`
   - `fetch.id`
6. Replace only that parcel's row with the winning candidate inside the parcel transaction.

Important implementation rule:

- the rebuild must compute the winner from the full candidate set for that parcel
- it must not depend on whatever row currently exists in `parcel_characteristics`
- if reparsing is used to build the candidate set, it must use only local raw HTML and must not refetch anything from AccessDane

That keeps rebuilds deterministic and independent from prior incremental history.

### No-Candidate Behavior

If a selected parcel has no valid candidate from any eligible fetch:

- leave it with no `parcel_characteristics` row after rebuild

This is the correct outcome for v1 because the table represents a best current snapshot, not a guaranteed row for every parcel.

## `parcel_lineage_links` Rebuild Rules

### Grain

One row per (`parcel_id`, `related_parcel_id`, `relationship_type`).

### Replacement Semantics

Full rebuild:

- treat every eligible parcel as selected scope
- for each selected parcel, replace only that parcel-owned lineage rows inside that parcel's transaction
- do not wipe the entire table up front

Scoped rebuild:

- delete only rows where `parcel_id` is in the selected scope
- recompute only links owned by those selected parcels
- do not delete rows merely because `related_parcel_id` points at a selected parcel

Reasoning:

- the table is keyed by the parcel whose page exposes the relationship
- scoped rebuild should be based on the page owner, not every parcel mentioned in the relationship graph

### Winner Selection

For each selected parcel:

1. Gather all eligible fetches for that parcel.
2. Extract lineage links from each fetch using the existing `_extract_parcel_lineage_links(...)` logic.
3. Normalize identities by:
   - `parcel_id`
   - `related_parcel_id`
   - `relationship_type`
4. For each normalized identity, keep the metadata from the highest-ranked fetch using the current lineage rank tuple:
   - `fetch.fetched_at`
   - `fetch.id`
5. Write one row per winning normalized identity.

This preserves the current behavior where a newer fetch can replace lineage metadata for the same normalized relationship.

### Empty-Lineage Behavior

If a selected parcel has no lineage links in any eligible fetch:

- leave it with no `parcel_lineage_links` rows after rebuild

This is expected and should not be treated as an error.

## Operational Behavior And Reporting

Both commands should print a short, scan-friendly summary at completion.

Recommended shape:

```text
Selected parcels: 123
Eligible fetches scanned: 456
Rows deleted: 78
Rows written: 120
Skipped fetches: 3
Parcel failures: 1
```

Optional per-parcel progress output is acceptable for long runs, but the command should avoid noisy per-fetch logging by default.

## Implementation Guidance For Days 3 And 4

Day 3 (`parcel_characteristics`):

- explicitly solve the parse-context dependency first:
  - either reparse local raw HTML during rebuild, or
  - refactor `_build_parcel_characteristic_candidate(...)` so rebuild can call an equivalent helper without the parse-time object
- avoid duplicating the field derivation rules already inside that helper
- add tests for:
  - full rebuild
  - scoped rebuild
  - deterministic winner selection
  - removal of stale rows when a parcel is rebuilt with no surviving candidate

Day 4 (`parcel_lineage_links`):

- factor the rebuild logic so it can call `_extract_parcel_lineage_links(...)`
- preserve the current identity normalization and rank behavior
- add tests for:
  - full rebuild
  - scoped rebuild by `parcel_id`
  - latest-fetch metadata precedence
  - deletion of stale lineage rows when scoped rebuild finds no current links

## Known Limits In V1

- Rebuild commands will still rely on local raw HTML being present.
- Missing raw files will reduce rebuild completeness until the underlying fetch is repaired.
- There is still no dedicated persisted run history for rebuild jobs.
- These commands repair the Sprint 2 derived tables, but they do not replace the need for `parse --reparse` when parser output tables themselves change.

## Day 2 Exit Decision

This is specific enough to implement on Days 3 and 4 without further design work.

The main implementation choices are now fixed:

- two explicit rebuild commands
- shared eligible fetch universe
- parcel-scoped replacement semantics
- deterministic winner computation from source fetches
- one transaction per parcel with continue-on-failure behavior
