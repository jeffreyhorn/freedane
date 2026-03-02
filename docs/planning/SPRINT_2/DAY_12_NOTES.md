# Sprint 2 Day 12 Notes

Day 12 deepened fixture coverage across the edge-case parcel types called out in the development plan.

Added regression coverage in `tests/test_parse.py` for:

- vacant land:
  - `061001397011`
  - verifies `parcel_characteristics` persists the current `G5` snapshot, acreage, no-address/no-Google/Bing shape, and 3-jurisdiction tax context
- condo / condo-like parcel:
  - `061003260511`
  - verifies `parcel_characteristics` persists the condo-style current `G2` snapshot with zero acreage and no map-link buttons
- exempt-style empty page:
  - `061002191411`
  - verifies the page still parses with zero assessment/tax/payment rows while preserving parcel summary data
  - verifies `parcel_characteristics` marks the page as `is_exempt_style_page = true`
- split / lineage parcel with parents only:
  - `061002318311`
  - verifies lineage persists only the two parent links and does not fabricate child links
  - also verifies the 2025 structured tax-detail payload carries special-charge and installment totals on that split-lot shape

Small parser hardening included:

- `_detect_exempt_style_page(...)` now treats school-district owners (`SCHOOL DISTRICT`, `SCHOOL DIST`) as exempt-style on the empty-valuation / empty-tax page variant

Result:

- Sprint 2 now has explicit regression coverage across the main edge-case parcel categories named in the dev plan, instead of relying only on the default residential fixtures
