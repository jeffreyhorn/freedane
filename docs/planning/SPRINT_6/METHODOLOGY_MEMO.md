# Sprint 6 Fraud Signal Methodology Memo

Prepared on: 2026-03-14

## Purpose

Document how Sprint 5/6 fraud-signal outputs must be interpreted in analyst workflows and stakeholder communication.
This memo defines required framing guardrails so score outputs are used for triage, not proof.

## Scope

This memo applies to:

- `accessdane score-fraud` outputs (`fraud_scores`, `fraud_flags`)
- `accessdane review-queue` analyst triage outputs
- `accessdane parcel-dossier` evidence-chain outputs
- `case_reviews` dispositions and downstream reporting artifacts

## What The Score Means

The fraud score is a deterministic triage priority signal generated from explicit rule logic and source data available at run time.

In practical terms, the score is:

- a ranking input for which parcels to review first
- a summary of rule triggers and weighted reason-code evidence
- versioned output tied to explicit `feature_version` and `ruleset_version`
- comparable only when scope and version context are held constant

## What The Score Does Not Mean

The score is not:

- proof of fraud
- a legal conclusion or enforcement determination
- a probability calibrated to real-world fraud incidence
- a substitute for analyst verification and corroborating evidence
- a complete representation of parcel context when data is sparse or delayed

## Required Triage-Vs-Proof Language

Use approved language in docs, reports, and analyst notes:

- "risk signal"
- "triage priority"
- "requires analyst review"
- "rule-triggered anomaly indicators"

Do not use language that implies final adjudication from score alone:

- "fraud confirmed by model"
- "parcel is fraudulent"
- "proof of fraud"
- "automatic violation finding"

## Required Analyst Verification Steps

Before recording a resolved/closed outcome, analysts must:

1. Confirm parcel identity, year, and scoring version context.
2. Validate that source records are present and interpretable (assessment, sales, permits, appeals, peer context).
3. Review reason-code evidence and threshold logic from dossier/queue outputs.
4. Cross-check for known non-fraud explanations (data lag, recording artifacts, parcel-change context, legitimate permitting activity).
5. Record review evidence in `case_reviews.note` and `case_reviews.evidence_links_json`.
6. Choose a disposition that matches evidence quality and next action.

## Review Outcome Interpretation Guidance

Use the following operational interpretations:

- False positive:
  - Use `disposition = false_positive` when evidence indicates the signal is explainable without suspected misconduct.
  - Include why the signal is non-actionable and what source facts support that conclusion.
- Insufficient evidence:
  - Use `disposition = inconclusive` when available records do not support either confirmation or dismissal.
  - Keep case in `in_review` when active follow-up is still required; resolve only when current evidence state is stable.
- Escalated:
  - Use `disposition = confirmed_issue` when verification supports escalation to compliance/investigation workflow.
  - Use `disposition = needs_field_review` when additional field validation is required before final determination.
  - Escalation must cite corroborating evidence beyond score value alone.

## Misuse Guardrails

These actions are out of bounds for Sprint 6 workflows:

- enforcement or punitive action based only on score rank
- public-facing claims that score output is adjudicated proof
- threshold changes without documented review-outcome evidence and versioned governance

## Documentation Cross-Links

- [PARCEL_DOSSIER_V1.md](PARCEL_DOSSIER_V1.md)
- [REVIEW_QUEUE_V1.md](REVIEW_QUEUE_V1.md)
- [CASE_REVIEW_V1.md](CASE_REVIEW_V1.md)
- [../SPRINT_5/OPERATIONS.md](../SPRINT_5/OPERATIONS.md)
