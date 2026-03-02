# Sprint 2 Day 13 Notes

Day 13 tightened the Sprint 2 documentation and acceptance framing.

Main additions:

- added a formal Sprint 2 acceptance checklist in `CHECKLIST.md`
- tightened the README so contributor-facing docs reflect the new Sprint 2 outputs:
  - `parcel_characteristics`
  - incremental `parcel_lineage_links`
  - richer structured tax-detail fields inside `TaxRecord.data`
- made the current report-endpoint no-go explicit in the README, not only in the Day 11 planning note

The Sprint 2 checklist also makes the deliberate deferrals explicit, especially:

- no dedicated full rebuild command yet for `parcel_characteristics`
- no dedicated full rebuild command yet for `parcel_lineage_links`
- no normalized richer tax-detail table yet beyond the current JSON payload
- no PDF-based report fetch/parser path

This keeps the sprint acceptance criteria aligned with what Sprint 2 actually built instead of implying more automation than currently exists.
