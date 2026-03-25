# Daily Analyst Runbook: McFarland Assessment Anomaly Triage

This runbook describes how an analyst should use AccessDane outputs to decide whether assessed values look normal, anomalous, or require additional data collection.

Important framing:

- Flags are **risk signals**, not proof of fraud.
- Decisions should be evidence-based and recorded through case review.

## 1. Daily Intake

Start each analyst day with current artifacts:

- `data/daily_review_queue.json` / `data/daily_review_queue.csv`
- `data/daily_investigation_report.html`
- latest monitoring diagnostics (`load_monitor*.json`, parser drift/benchmark alerts if present)

If monitoring shows unresolved `critical` issues, pause triage and route to operations first.

## 2. Build The Working Set

Generate (or refresh) the ranked queue:

```bash
.venv/bin/accessdane review-queue \
  --top 100 \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/daily_review_queue.json \
  --csv-out data/daily_review_queue.csv
```

Pick parcels from top of queue by risk score and reason-code pattern diversity (not score alone).

## 3. Parcel-Level Evidence Review

For each selected parcel:

```bash
.venv/bin/accessdane parcel-dossier \
  --id <parcel_id> \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/dossiers/<parcel_id>.json
```

Review these evidence categories:

- assessment history and year-over-year shifts
- matched sales and confidence/fit
- permit and appeal events
- peer context and reason codes

Classify each parcel as one of:

- likely normal
- suspicious anomaly requiring follow-up
- insufficient evidence (data gap)

## 4. Record Analyst Judgment

Create/update case records as you work:

```bash
.venv/bin/accessdane case-review create \
  --score-id <score_id> \
  --status in_review \
  --reviewer "<analyst_name>" \
  --assigned-reviewer "<analyst_name>" \
  --note "Initial triage"

.venv/bin/accessdane case-review update \
  --id <case_review_id> \
  --status resolved \
  --disposition <false_positive|inconclusive|needs_field_review|confirmed_issue> \
  --set-evidence-link "kind=dossier,ref=<dossier_ref>"
```

## 5. Determine What Additional Data Is Needed

Use recurring patterns in reviewed cases to decide acquisition priorities.

## 5.1 Data-Gap Decision Matrix

Use standard 3-column markdown table formatting (single leading/trailing `|` per row):

| Observed pattern in dossiers/reviews | Likely missing input | Action |
| --- | --- | --- |
| Large assessment change with no matching permit context | permits detail depth/coverage | Request updated permits export (scoped by year/parcel class). |
| Many valuations disputed in notes with sparse appeal context | appeals coverage | Request appeals export for affected years and parcel classes. |
| Frequent low-confidence or unresolved sales matches | transfer/deed quality | Request updated RETR export and/or clerk deed details. |
| Concentrated anomalies in one segment/neighborhood with weak comparables | segment-level comparables | Acquire supplemental market context or broaden sales comparison window. |

## 5.2 Trigger Thresholds For Data Requests

Open a data acquisition request when any of these occur in a weekly review slice:

- 10%+ of reviewed cases are `insufficient evidence` for the same reason class
- 3+ high-priority parcels in same segment depend on missing permits/appeals
- recurring false positives are explicitly linked to missing/late source records

Each request should include:

- parcel IDs and years affected
- which evidence section is missing or unreliable
- expected source owner (assessor, clerk, permit office, etc.)
- urgency (`critical`/`warn`/`info`) and decision deadline

## 6. End-Of-Day Outputs

Refresh analyst feedback artifacts:

```bash
.venv/bin/accessdane review-feedback \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/daily_review_feedback.json \
  --sql-out data/daily_review_feedback.sql

.venv/bin/accessdane investigation-report \
  --top 100 \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --html-out data/daily_investigation_report.html \
  --out data/daily_investigation_report.json
```

Track in daily log:

- cases reviewed
- dispositions by type
- parcels needing external data
- open data requests and blockers

## 7. Weekly Calibration Loop

At least weekly:

- review `review-feedback` output for recurring false-positive patterns
- compare monitor/benchmark drift signals with analyst outcomes
- propose threshold/ruleset adjustments only with documented evidence

Do not promote scoring/ruleset changes without documented analyst evidence and governance review.
