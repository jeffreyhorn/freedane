# Sprint 2 Day 11 Notes

Day 11 evaluated the optional AccessDane report endpoints and reached a no-go decision for Sprint 2.

## Live endpoint behavior

Checked live on March 2, 2026 using parcel `061002320401`:

- main parcel page:
  - `https://accessdane.danecounty.gov/061002320401`
  - returns normal parcel HTML
- `Summary Report` link:
  - `https://accessdane.danecounty.gov/Parcel/PrintSetUp/061002320401?printSummary=True`
  - returns `application/pdf` directly
- `Custom Report` link:
  - `https://accessdane.danecounty.gov/Parcel/PrintSetUp/061002320401`
  - returns an HTML setup form
  - submitting the default form posts back to the same path and returns `application/pdf`

The older `accessdane.countyofdane.com` hostname now redirects to `accessdane.danecounty.gov`, so any future report-path work would also need to account for the domain change.

## What the custom setup page exposes

The `Custom Report` setup page is not a hidden structured-data endpoint. It is a print configuration form with a large set of checkbox and hidden inputs describing which sections to include in the generated PDF.

The default selected sections for the tested parcel were:

- Parcel Summary
- Parcel Details
- Municipal Contacts
- Assessment Summary
- Assessment Details
- Open Book / Board of Review dates
- Zoning
- Standard Map
- Print Friendly Map
- Tax Summary
- Tax Details
- Districts
- Recorded Documents
- Valuation Breakout
- Tax Payment History
- Tax details page for each tax year
- Parcel Parents
- Parcel Children

This confirms the report path is a print assembly flow over existing AccessDane sections, not a cleaner machine-readable API.

## Recommendation

Do not implement a Sprint 2 report-fetch path.

Reasoning:

- the useful output is PDF, not HTML or JSON
- the `Custom Report` flow adds request/selection complexity but still terminates in PDF
- the underlying data appears to be the same AccessDane sections already embedded in the parcel page, just assembled into a printable document
- adopting the report path would require new PDF storage and parsing work, which is materially more brittle than extending the current HTML parser

## Practical conclusion

For Sprint 2, continue investing in the current HTML parser rather than the report endpoints.

Revisit the report path only if one of these becomes true later:

- AccessDane exposes a non-PDF report response format
- a later sprint specifically needs printable sections that are not present in the parcel HTML
- there is a clear operational reason to archive generated PDFs in addition to the raw parcel HTML
