# Sprint 8 Plan

Prepared on: 2026-03-24

## Sprint Goal

Sprint 8 should productionize the Sprint 7 automation and monitoring stack so analysts can start reliable, repeatable investigation work with clear operational guardrails.
The primary product is an analyst-ready operational baseline with environment promotion controls, resilient scheduling, integrated alert transport, observability, data quality/replay controls, and pipeline-enforced governance.

This plan is based on:

- [docs/planning/SPRINT_8/HANDOFF.md](HANDOFF.md), especially productionization carryover priorities
- [docs/todo_immediately.md](../../todo_immediately.md), especially startup migrations/loads/analysis/reporting requirements before analyst triage
- [docs/daily_analyst_runbook.md](../../daily_analyst_runbook.md), especially daily analyst workflow expectations

Total planned effort: `79` hours.

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

- `sprint8/day-01-baseline`
- `sprint8/day-02-env-contract`
- `sprint8/day-03-env-implementation`
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

### Day 1 (`6h`): Sprint 8 Baseline And Analyst-Readiness Gap Map

Focus:

- Re-establish a known-good baseline and map gaps between current state and analyst-ready operations.

Planned work:

- Create branch `sprint8/day-01-baseline` from updated `main`.
- Run handoff baseline checks and smoke validation from Sprint 8 handoff.
- Run and verify `todo_immediately` sections 1 and 6 (migration head + go/no-go criteria scaffolding).
- Produce a Sprint 8 execution checklist mapped to productionization priorities.

Deliverables:

- `docs/planning/SPRINT_8/DAY_1_BASELINE.md`
- Sprint 8 execution checklist artifact

PR closeout:

- Commit baseline evidence and checklist updates.
- Open baseline PR and merge after review.

### Day 2 (`6h`): Define Production Environment And Promotion Contract

Focus:

- Lock dev/stage/prod boundaries and promotion rules before implementation.

Planned work:

- Create branch `sprint8/day-02-env-contract` from updated `main`.
- Define environment profile contract (config keys, secret boundaries, artifact roots, run-id naming).
- Define promotion workflow and approval controls for moving runs/config between environments.
- Define rollback and emergency freeze semantics.

Deliverables:

- `docs/planning/SPRINT_8/ENVIRONMENT_PROMOTION_V1.md`

PR closeout:

- Commit environment/promotion contract doc.
- Open PR, address review, merge.

### Day 3 (`6h`): Implement Environment Profiles And Promotion Controls

Focus:

- Deliver executable environment profile selection and promotion guardrails.

Planned work:

- Create branch `sprint8/day-03-env-implementation` from updated `main`.
- Implement environment profile loader and validation for recurring commands.
- Add promotion safety checks (required metadata, compatible version selectors, explicit approval marker).
- Add tests for profile resolution and promotion-block conditions.

Deliverables:

- environment profile implementation
- promotion guardrail checks
- environment/promotion tests

PR closeout:

- Commit implementation and tests.
- Open PR, resolve review, merge.

### Day 4 (`5h`): Automate Immediate Startup Workflow End-To-End

Focus:

- Convert `docs/todo_immediately.md` into a reliable bootstrap workflow operators can run repeatedly.

Planned work:

- Create branch `sprint8/day-04-startup-automation` from updated `main`.
- Implement/validate a scripted startup sequence for migrations, data loads, analytics, and report generation.
- Ensure outputs land in deterministic startup artifact paths.
- Add failure messaging that blocks analyst intake when go/no-go checks fail.

Deliverables:

- startup automation script/workflow
- startup run artifact template and verification notes
- updates to `docs/todo_immediately.md` as needed

PR closeout:

- Commit startup automation and docs updates.
- Open PR, review, merge.

### Day 5 (`6h`): Define Managed Scheduler And Resiliency Contract

Focus:

- Specify production scheduler behavior before integrating external orchestration.

Planned work:

- Create branch `sprint8/day-05-scheduler-contract` from updated `main`.
- Define managed scheduler contract (trigger model, retries, backoff, dead-letter handling, overlap policy).
- Define incident state transitions and required run metadata for scheduler failures.
- Define SLA/SLO targets for schedule adherence and run completion.

Deliverables:

- `docs/planning/SPRINT_8/SCHEDULER_RELIABILITY_V1.md`

PR closeout:

- Commit scheduler/reliability contract doc.
- Open PR, address review, merge.

### Day 6 (`6h`): Implement Resilient Scheduler Integration

Focus:

- Deliver resilient recurring execution behavior aligned to Day 5 contract.

Planned work:

- Create branch `sprint8/day-06-scheduler-implementation` from updated `main`.
- Implement managed scheduler integration path (or adapter) with retry/dead-letter semantics.
- Add durable run-state transitions and overlap-safe locking behavior.
- Add tests for retry boundaries, dead-letter routing, and overlap protection.

Deliverables:

- resilient scheduler integration
- dead-letter/failed-run handling
- scheduler resiliency tests

PR closeout:

- Commit scheduler implementation and tests.
- Open PR, resolve review, merge.

### Day 7 (`5h`): Define Alert Transport Integration Contract

Focus:

- Lock alert delivery contract before coding notification adapters.

Planned work:

- Create branch `sprint8/day-07-alert-contract` from updated `main`.
- Define transport-neutral alert envelope and delivery policy (email/Slack/PagerDuty compatible).
- Define routing-key to destination mapping and escalation timing.
- Define delivery retry/idempotency and operator acknowledgment expectations.

Deliverables:

- `docs/planning/SPRINT_8/ALERT_TRANSPORT_V1.md`

PR closeout:

- Commit alert transport contract doc.
- Open PR, review, merge.

### Day 8 (`6h`): Implement Alert Delivery Integrations

Focus:

- Deliver first production-ready notification delivery path for existing alerts.

Planned work:

- Create branch `sprint8/day-08-alert-implementation` from updated `main`.
- Implement alert transport adapters and routing configuration.
- Integrate delivery with refresh/drift/load/benchmark critical and warn signals.
- Add tests for routing, retry, and duplicate-suppression behavior.

Deliverables:

- alert transport implementation
- routing configuration templates
- alert delivery tests

PR closeout:

- Commit alert transport implementation and tests.
- Open PR, address review, merge.

### Day 9 (`5h`): Define Observability And SLO Contract

Focus:

- Specify dashboard and service-level metrics before implementation.

Planned work:

- Create branch `sprint8/day-09-observability-contract` from updated `main`.
- Define operational metrics contract for refresh, parser drift, load monitoring, annual refresh, and benchmark runs.
- Define SLOs/SLIs and burn-alert thresholds.
- Define dashboard artifact contract and cadence for publication.

Deliverables:

- `docs/planning/SPRINT_8/OBSERVABILITY_V1.md`

PR closeout:

- Commit observability contract doc.
- Open PR, review, merge.

### Day 10 (`6h`): Implement Dashboards, Rollups, And SLO Checks

Focus:

- Deliver operator-facing observability outputs from existing run artifacts.

Planned work:

- Create branch `sprint8/day-10-observability-implementation` from updated `main`.
- Implement rollup generation for run health and latency/failure trends.
- Implement SLO evaluation outputs and artifact snapshots for operations.
- Add tests for rollup determinism and threshold classification.

Deliverables:

- observability rollup tooling
- SLO evaluation outputs
- observability tests

PR closeout:

- Commit observability implementation and tests.
- Open PR, resolve review, merge.

### Day 11 (`5h`): Define Data-Quality Guardrails And Replay Policy Contract

Focus:

- Lock recurring ingest quality gates and replay governance before implementation.

Planned work:

- Create branch `sprint8/day-11-quality-contract` from updated `main`.
- Define quality gate checks for RETR/permits/appeals ingest and downstream rebuild readiness.
- Define replay policy for partial corrections and annual backfill scenarios.
- Define analyst communication requirements for replay-impact windows.

Deliverables:

- `docs/planning/SPRINT_8/DATA_QUALITY_REPLAY_V1.md`

PR closeout:

- Commit data quality/replay contract doc.
- Open PR, address review, merge.

### Day 12 (`5h`): Implement Data-Quality Gates And Replay Controls

Focus:

- Deliver enforceable quality/replay controls for recurring operations.

Planned work:

- Create branch `sprint8/day-12-quality-implementation` from updated `main`.
- Implement pre-run quality gates and blocking behavior for bad source/derived states.
- Implement replay controls with explicit scope and audit payloads.
- Add tests for blocked-run, safe-replay, and replay-audit behavior.

Deliverables:

- quality gate implementation
- replay control implementation
- quality/replay tests

PR closeout:

- Commit quality/replay implementation and tests.
- Open PR, review, merge.

### Day 13 (`7h`): Enforce Pipeline-Based Threshold And Ruleset Promotion

Focus:

- Move threshold/ruleset promotion from documented workflow to enforceable pipeline process.

Planned work:

- Create branch `sprint8/day-13-promotion-enforcement` from updated `main`.
- Implement policy checks that require review-feedback evidence and approval metadata before promotion.
- Add automated validation in CI/pipeline path for promotion requests.
- Add tests for valid promotion, missing-evidence rejection, and version drift rejection.

Deliverables:

- promotion-enforcement pipeline checks
- governance automation updates
- promotion policy tests

PR closeout:

- Commit promotion-enforcement implementation and tests.
- Open PR, resolve review, merge.

### Day 14 (`5h`): Analyst Readiness Acceptance And Operations Handoff

Focus:

- Close Sprint 8 with proof that analysts can begin work with reliable operational support.

Planned work:

- Create branch `sprint8/day-14-acceptance-handoff` from updated `main`.
- Execute full `todo_immediately` startup sequence and capture evidence artifacts.
- Run analyst workflow smoke cycle from `daily_analyst_runbook` (queue -> dossier -> case-review -> feedback/report).
- Document Sprint 8 acceptance results and final analyst-ready handoff.

Deliverables:

- `docs/planning/SPRINT_8/ACCEPTANCE_REVIEW.md`
- finalized `docs/todo_immediately.md`
- finalized `docs/daily_analyst_runbook.md`
- Sprint 8 handoff update for next-phase work

PR closeout:

- Commit acceptance artifacts and final runbook updates.
- Open final Sprint 8 PR, resolve review comments, merge.

## Planned Effort Summary

- Days 1-4: `23h` on baseline, environment promotion foundations, and immediate startup automation
- Days 5-8: `23h` on resilient scheduler and alert transport implementation
- Days 9-12: `21h` on observability and data quality/replay controls
- Days 13-14: `12h` on governance enforcement and analyst-readiness acceptance

This preserves the `79h` Sprint 8 target while structuring delivery as 14 PR-driven daily increments from `main`, ending with analyst-ready operations.
