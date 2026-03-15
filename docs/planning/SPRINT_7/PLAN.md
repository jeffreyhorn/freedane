# Sprint 7 Plan

Prepared on: 2026-03-15

## Sprint Goal

Sprint 7 should make the Sprint 6 investigation workflow operationally sustainable through automation and ongoing monitoring.
The primary product is a scheduled, observable refresh workflow with parser/load diagnostics and recurring benchmark outputs.

This plan is based on:

- [docs/planning/devplan.md](../devplan.md), especially the Sprint 7 automation and monitoring scope
- [docs/planning/SPRINT_7/HANDOFF.md](HANDOFF.md), especially carryover priorities around scheduling, drift detection, alerting, annual refresh, and benchmark packs

Total planned effort: `80` hours.

## Planning Constraints

- All 14 days in the two-week sprint have scheduled work.
- No single day exceeds `10` hours.
- Each day is executed on its own short-lived branch created from current `main`.
- Each day starts by creating a fresh branch off updated `main` for that day's scope.
- Each day ends with the same delivery loop:
  - commit the day's changes on that day's branch
  - open a Pull Request from that day's branch into `main`
  - review the PR
  - make any follow-up fixes required by that review
  - hand the PR off for merge, which marks the day complete
  - start the next day from a fresh branch created from the newly updated `main`

## Standard Daily Closeout

Reserve the last `1` hour of each day for the PR loop unless the day says otherwise.

Recommended branch naming pattern:

- `sprint7/day-01-baseline`
- `sprint7/day-02-refresh-contract`
- `sprint7/day-03-refresh-runner`
- continue the same pattern through Day 14

Standard daily branch workflow:

1. Update local `main` to the latest merged state.
2. Create a fresh branch for that day's scoped work.
3. Do only that day's planned changes on the branch.
4. Open the PR from that branch back into `main`.
5. After review fixes are merged, delete the branch and begin the next day from `main`.

Daily closeout sequence:

1. Run the day's targeted validation.
2. Commit only the intended changes for that day on the day-specific branch.
3. Open a PR into `main` with a focused description of scope, tests run, and known limits.
4. Review the PR and address review comments on the same branch that same day.
5. Hand the PR off for merge after the fixes are in.
6. After merge, delete the day branch and treat that merge as the day's completion point.

## Day-By-Day Plan

### Day 1 (`5h`): Re-Establish Sprint 7 Baseline And Automation Scope

Focus:

- Start Sprint 7 from known-good Sprint 6 workflows and lock automation/monitoring boundaries.

Planned work:

- Run handoff baseline checks: `make typecheck && make lint && make format && make test`.
- Run Sprint 6 workflow smoke sequence (`init-db`, `review-queue`, `review-feedback`, `investigation-report`) and confirm command interoperability.
- Convert Sprint 7 carryover priorities into an execution checklist with acceptance gates.
- Confirm current migration head and identify any schema additions needed for scheduled-run metadata.

Deliverables:

- `docs/planning/SPRINT_7/DAY_1_BASELINE.md`
- Sprint 7 execution checklist artifact

PR closeout:

- Commit baseline evidence and checklist updates.
- Open baseline PR and merge after review.

### Day 2 (`5h`): Define Scheduled Refresh Workflow Contract

Focus:

- Lock recurring refresh job contract before implementing schedulers/runners.

Planned work:

- Define scheduled job matrix for ingest/context refresh, feature build, scoring, feedback, and report generation.
- Define versioning/tagging and artifact path conventions for recurring runs.
- Define failure semantics, retry rules, and idempotency expectations for scheduled runs.
- Define job-level observability payload schema (status, duration, row/run counts, failure code).

Deliverables:

- `docs/planning/SPRINT_7/REFRESH_AUTOMATION_V1.md`

PR closeout:

- Commit refresh workflow contract doc.
- Open PR, address review, merge.

### Day 3 (`6h`): Implement Scheduled Refresh Runner Core

Focus:

- Deliver a first working orchestration runner for repeatable refresh cycles.

Planned work:

- Implement a runner command/script that executes the configured Sprint 6 command chain in deterministic order.
- Add run-state capture (start/end timestamps, command exit state, structured summary payload).
- Add safe-stop behavior on hard failures with clear diagnostics.
- Add unit/integration tests for orchestration ordering and failure handling.

Deliverables:

- scheduled refresh runner v1
- orchestration diagnostics payloads
- runner tests

PR closeout:

- Commit runner implementation and tests.
- Open PR, resolve review, merge.

### Day 4 (`7h`): Add Scheduler Integration And Artifact Retention

Focus:

- Move from manual runner execution to schedulable recurring operation.

Planned work:

- Add scheduler integration path (for example cron-compatible entrypoint and documented schedule templates).
- Add per-run artifact retention structure (dated outputs, latest pointers, failure artifacts).
- Add guardrails to prevent overlapping runs and inconsistent partial outputs.
- Add tests for retention/locking behavior and deterministic path conventions.

Deliverables:

- scheduler integration scripts/templates
- artifact retention/locking implementation
- scheduler/retention tests

PR closeout:

- Commit scheduler and retention updates.
- Open PR, review, merge.

### Day 5 (`5h`): Define Parser Drift Detection Contract

Focus:

- Lock parser drift detection signals before implementing diagnostics.

Planned work:

- Define parser drift signal schema (field coverage shifts, selector misses, extraction null-rate deltas).
- Define baseline snapshot format and diff severity thresholds.
- Define alert routing payload for drift warnings/errors.
- Define validation fixtures for expected/noisy drift scenarios.

Deliverables:

- `docs/planning/SPRINT_7/PARSER_DRIFT_V1.md`

PR closeout:

- Commit drift-detection contract doc.
- Open PR, address review, merge.

### Day 6 (`6h`): Implement Parser Drift Snapshot And Diff Diagnostics

Focus:

- Deliver first working parser drift detection workflow.

Planned work:

- Implement baseline snapshot capture for parser output profile metrics.
- Implement diff command/report comparing current parser profile to baseline snapshot.
- Add severity classification output for actionable drift triage.
- Add tests for stable/noisy/breaking drift classification scenarios.

Deliverables:

- parser drift snapshot + diff implementation
- drift severity diagnostics output
- drift diagnostics tests

PR closeout:

- Commit drift tooling and tests.
- Open PR, resolve review, merge.

### Day 7 (`6h`): Define Load Monitoring And Alerting Contract

Focus:

- Specify load-monitoring expectations before implementing alerting logic.

Planned work:

- Define load metrics contract (durations, row volumes, queue sizes, failure rates, freshness windows).
- Define threshold policy contract for warning/critical states.
- Define alert payload contract and operator action mapping.
- Define historical rollup windows for recurring trend checks.

Deliverables:

- `docs/planning/SPRINT_7/LOAD_MONITORING_V1.md`

PR closeout:

- Commit load-monitoring/alerting contract doc.
- Open PR, review, merge.

### Day 8 (`5h`): Implement Load Diagnostics And Threshold Evaluation

Focus:

- Deliver first working load diagnostics and threshold classification output.

Planned work:

- Implement command/module that computes load/freshness diagnostics from recent run artifacts.
- Implement threshold evaluator output (`ok`/`warn`/`critical`) with reason codes.
- Add machine-readable alert payload export.
- Add tests for threshold transitions and diagnostics determinism.

Deliverables:

- load diagnostics command v1
- threshold evaluator output
- diagnostics/alert tests

PR closeout:

- Commit load diagnostics implementation and tests.
- Open PR, address review, merge.

### Day 9 (`6h`): Define Annual Refresh Workflow And Governance Contract

Focus:

- Lock annual data-refresh governance before implementation.

Planned work:

- Define annual refresh sequence for new assessment roll + RETR exports + context rebuild + scoring/report refresh.
- Define required operator checkpoints and sign-off records for annual refresh completion.
- Define backfill/replay policy for partial-year corrections.
- Define threshold-promotion governance linkage to review-feedback evidence.

Deliverables:

- `docs/planning/SPRINT_7/ANNUAL_REFRESH_V1.md`
- threshold-promotion governance notes

PR closeout:

- Commit annual refresh/governance contract docs.
- Open PR, resolve review, merge.

### Day 10 (`5h`): Implement Annual Refresh Runner And Checklist Automation

Focus:

- Deliver runnable annual-refresh path based on Day 9 contract.

Planned work:

- Implement annual refresh command/script wrapper around the approved sequence.
- Add automated checklist output summarizing each stage status and blocking failures.
- Add resumable stage controls for interrupted annual refresh runs.
- Add tests for stage ordering and resumability.

Deliverables:

- annual refresh runner v1
- stage checklist artifact output
- annual refresh tests

PR closeout:

- Commit annual refresh implementation and tests.
- Open PR, address review, merge.

### Day 11 (`7h`): Define Benchmark Pack Contract For Recurring Fairness Studies

Focus:

- Specify recurring benchmark pack outputs and comparability rules before implementation.

Planned work:

- Define benchmark pack schema (coverage, risk-band mix, disposition mix, geography/class segments).
- Define baseline-vs-current comparability contract and drift tolerances.
- Define recurring cadence policy and retention expectations.
- Define escalation criteria for benchmark regressions.

Deliverables:

- `docs/planning/SPRINT_7/BENCHMARK_PACK_V1.md`

PR closeout:

- Commit benchmark pack contract doc.
- Open PR, review, merge.

### Day 12 (`5h`): Implement Benchmark Pack Generator And Trend Outputs

Focus:

- Deliver repeatable recurring benchmark pack generation.

Planned work:

- Implement benchmark pack command that emits current-period summary and baseline comparison.
- Add segmented rollups needed for recurring fairness/performance checks.
- Add trend artifact output suitable for scheduled publication.
- Add tests for benchmark output stability and comparability keys.

Deliverables:

- benchmark pack generator v1
- recurring benchmark artifacts
- benchmark tests

PR closeout:

- Commit benchmark implementation and tests.
- Open PR, resolve review, merge.

### Day 13 (`6h`): Sprint 7 Hardening And Operations Runbook

Focus:

- Harden integrated automation/monitoring workflows and document repeatable operator execution.

Planned work:

- Run targeted end-to-end automation validation across scheduler, drift diagnostics, load diagnostics, annual refresh, and benchmark generation.
- Fix edge-case defects discovered during integration dry run.
- Finalize Sprint 7 operations runbook for recurring operator usage.
- Add troubleshooting notes for common scheduler/drift/load failures.

Deliverables:

- `docs/planning/SPRINT_7/OPERATIONS.md`
- hardening fixes and targeted tests

PR closeout:

- Commit runbook + hardening updates.
- Open PR, resolve review, merge.

### Day 14 (`6h`): Sprint 7 Acceptance Pack And Sprint 8 Handoff Draft

Focus:

- Close Sprint 7 with acceptance evidence and clear next-step handoff.

Planned work:

- Run targeted Sprint 7 acceptance validation across automation and monitoring surfaces.
- Document acceptance results against Sprint 7 criteria.
- Finalize Sprint 7 operations docs and escalation guidance.
- Draft Sprint 8 handoff with productionization priorities and outstanding risks.

Deliverables:

- `docs/planning/SPRINT_7/ACCEPTANCE_REVIEW.md`
- updated Sprint 7 operations documentation
- `docs/planning/SPRINT_8/HANDOFF.md` draft

PR closeout:

- Commit acceptance artifacts and handoff docs.
- Open final Sprint 7 PR, resolve review comments, merge.

## Planned Effort Summary

- Days 1-4: `23h` on baseline + scheduled refresh orchestration foundations
- Days 5-8: `22h` on parser drift and load monitoring contracts/implementation
- Days 9-12: `23h` on annual refresh governance/runner and benchmark packs
- Days 13-14: `12h` on hardening, acceptance, and handoff closeout

This preserves the `80h` Sprint 7 estimate from the development plan while structuring delivery as 14 PR-driven daily increments from `main`.
