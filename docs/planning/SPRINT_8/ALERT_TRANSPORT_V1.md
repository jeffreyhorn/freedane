# Sprint 8 Alert Transport Contract v1

Prepared on: 2026-03-27

## Purpose

Define the first stable alert transport contract before implementing notification adapters in Sprint 8 Day 8.

This contract locks v1 behavior for:

- transport-neutral alert envelope shape
- routing key resolution and environment-scoped destination mapping
- escalation timing for unacknowledged alerts
- delivery retry, failure classification, and idempotency
- operator acknowledgment requirements and audit fields

## Scope

In scope for v1:

- transport of existing emitted alert payloads into notification channels
- adapter compatibility targets: email, Slack, PagerDuty
- environment-aware routing via `ALERT_ROUTE_GROUP`
- delivery attempt auditing under environment artifact roots
- acknowledgment metadata and escalation timers

Out of scope for v1:

- vendor-specific SDK implementation details
- message templating/i18n beyond deterministic default formatting
- auto-remediation workflows
- cross-environment alert federation

## Design Goals

- Preserve producer contracts from Sprint 7/8 while adding transport orchestration.
- Keep alert fan-out deterministic and idempotent across retries/restarts.
- Prevent silent alert loss from transient transport failures.
- Keep operator ownership auditable for critical failures.

## Baseline Assumptions

- Existing producer contracts remain authoritative:
  - [parser drift contract](../SPRINT_7/PARSER_DRIFT_V1.md)
  - [load monitoring contract](../SPRINT_7/LOAD_MONITORING_V1.md)
  - [benchmark pack contract](../SPRINT_7/BENCHMARK_PACK_V1.md)
  - [scheduler reliability contract](SCHEDULER_RELIABILITY_V1.md)
- Environment boundary and secret controls remain in force:
  - [environment and promotion contract](ENVIRONMENT_PROMOTION_V1.md)
- `ALERT_ROUTE_GROUP` is required in every environment profile and scopes route resolution.

## Source Alert Producers (v1)

| Producer | Source artifact | Alert cardinality | Source severities |
| --- | --- | --- | --- |
| `parser_drift` | parser drift standalone alert payload (`run`, `alert`, `impacted_signals`, `operator_actions`, `error`) | 0 or 1 | `warn`, `error` |
| `load_monitoring` | load-monitor standalone alert payload (`run`, `alert`, `impacted_signals`, `operator_actions`, `error`) | 0 or 1 | `warn`, `critical` |
| `benchmark_pack` | benchmark companion alert payload (`generated_at`, `alert_count`, `alerts[]`) | 0..N | `warn`, `critical` |
| `scheduler` | scheduler run payload with non-null `incident` on terminal failure | 0 or 1 | `info`, `warn`, `critical` |

Normalization rule:

- Transport operates on a canonical per-alert instance.
- For single-alert producers, one canonical alert instance is produced.
- For benchmark companion payloads, each `alerts[]` entry becomes one canonical alert instance.

## Canonical Alert Instance Contract (v1)

Each canonical alert instance must include:

| Field | Description |
| --- | --- |
| `event_id` | Stable transport event id for this alert instance. |
| `source_system` | `parser_drift|load_monitoring|benchmark_pack|scheduler`. |
| `source_payload_type` | Producer payload type name/version. |
| `source_payload_path` | Portable artifact-root-relative path (POSIX-style separators) to source artifact used for transport. |
| `source_payload_hash` | SHA-256 hash of source payload file bytes, emitted as lowercase hexadecimal (`64` chars). |
| `source_run_id` | Producer run id (or scheduler run id). |
| `alert_id` | Producer-stable alert id. |
| `alert_type` | Canonical transport alert type (`parser_drift|load_monitoring|benchmark_pack|scheduler`) used for routing policy lookup. |
| `source_alert_type` | Producer-native alert type value from source payload (nullable when not present). |
| `severity` | Canonical severity `info|warn|critical`. |
| `generated_at_utc` | Source alert generation timestamp (UTC). |
| `summary` | Human summary used by transport adapters. |
| `reason_codes` | Sorted unique reason codes. |
| `operator_actions` | Producer operator actions (normalized list, possibly empty). |

Canonical severity normalization:

- `parser_drift.warn -> warn`
- `parser_drift.error -> critical`
- `load_monitoring.warn -> warn`
- `load_monitoring.critical -> critical`
- `benchmark_pack.warn -> warn`
- `benchmark_pack.critical -> critical`
- `scheduler.incident.severity` maps directly (`info|warn|critical`)

Source payload hash encoding rule:

- read bytes directly from `source_payload_path` with no normalization or newline conversion.
- compute SHA-256 over those bytes.
- persist/emit `source_payload_hash` as lowercase hexadecimal (`64` chars).

## Routing Contract (v1)

### Canonical Routing Key

Canonical route key format:

- `<alert_route_group>.<alert_type>.<severity>`

Where:

- `alert_route_group` is `ALERT_ROUTE_GROUP` from the active environment profile
- `alert_type` is canonical alert type (`parser_drift`, `load_monitoring`, `benchmark_pack`, `scheduler`)
- `severity` is canonical severity (`info`, `warn`, `critical`)

If producer payload already includes `routing_key`:

- preserve original value as `route.source_routing_key`
- still compute canonical route key for policy lookup
- `route.canonical_routing_key` is the single source of truth for transport routing decisions.
- routing fields are defined only under `route` in the transport envelope (not duplicated under `alert`).

### Destination Mapping

Route policy resolution must support exact-match then wildcard fallback:

1. exact key: `<group>.<alert_type>.<severity>`
2. alert-type wildcard: `<group>.<alert_type>.*`
3. group fallback: `<group>.*.<severity>`
4. default fallback: `<group>.*.*`

Each resolved route policy must declare:

- `primary_destinations[]`
- `escalation_destinations[]`
- `ack_required`
- `ack_timeout_seconds` (required key; nullable only when `ack_required = false`)
- `escalation_schedule_seconds[]`

Route-policy timing semantics:

- `ack_timeout_seconds` is the required acknowledgment timeout from first successful delivery (`delivery.first_delivery_at_utc`).
- `escalation_schedule_seconds[]` is a sorted list of relative offsets from `delivery.first_delivery_at_utc` when escalation fan-out must be attempted while acknowledgment remains pending.
- `ack_deadline_utc` is derived, not independently configured:
  - when `ack_required = true`: `ack_deadline_utc = first_delivery_at_utc + ack_timeout_seconds`
  - when `ack_required = false`: `ack_deadline_utc = null`

### Default v1 Policy

| Canonical severity | Primary destinations | `ack_required` / `ack_timeout_seconds` | `escalation_schedule_seconds[]` |
| --- | --- | --- | --- |
| `critical` | Slack ops-critical, PagerDuty primary service, oncall email | `true` / `900` | `[900, 1800]` |
| `warn` | Slack ops-warn, team email | `true` / `14400` | `[14400]` |
| `info` | Slack ops-info (or equivalent low-noise channel) | `false` / `null` | `[]` |

## Transport Envelope Contract (v1)

Top-level keys (canonical order for examples/artifacts; not a semantic requirement for JSON object parsing):

1. `envelope`
2. `alert`
3. `route`
4. `destinations`
5. `delivery`
6. `acknowledgment`
7. `error`

### `envelope` fields

- `contract_version` (must be `alert_transport_v1`)
- `event_id`
- `transport_run_id` (id for a transport execution batch)
- `emitted_at_utc`
- `run_date` (`YYYYMMDD`, derived from `emitted_at_utc` in UTC)
- `environment`
- `alert_route_group`

### `alert` fields

- canonical alert instance object from this contract

### `route` fields

- `canonical_routing_key`
- `source_routing_key` (nullable)
- `policy_id`
- `policy_version`

### `destinations` fields

`destinations` is an array. Each item must include:

- `destination_id` (stable id from route policy)
- `channel_type` (`email|slack|pagerduty`)
- `channel_target` (non-secret logical target id)
- `is_primary` (bool)
- `is_escalation` (bool)

### `delivery` fields

- aggregate delivery fields:
  - `overall_status` (`pending|delivered|failed_retryable|failed_terminal|suppressed_duplicate`)
  - `first_delivery_at_utc` (nullable; set to timestamp of first successful primary delivery attempt across all primary destinations)
  - `delivery_receipts` (array of per-channel receipt summaries)
- per-destination delivery fields under `deliveries[]` (required, one item per destination):
  - `destination_id`
  - `status` (`pending|delivered|failed_retryable|failed_terminal|suppressed_duplicate`)
  - `attempt_count`
  - `max_attempts`
  - `last_attempt_at_utc` (nullable)
  - `next_attempt_at_utc` (nullable)
- legacy singular delivery fields (`delivery.status`, `delivery.attempt_count`, `delivery.max_attempts`, `delivery.next_attempt_at_utc`) are not valid in v1 transport envelopes.

### `acknowledgment` fields

- `ack_required` (bool)
- `ack_state` (`not_required|pending|acknowledged|expired`)
- `ack_deadline_utc` (nullable)
- `acked_at_utc` (nullable)
- `acked_by` (nullable)
- `incident_id` (nullable)

### `error` fields

- nullable; when non-null includes:
  - `code`
  - `message`
  - `retryable` (bool)

## Retry And Idempotency Contract

### Delivery Retry Policy

Default max attempts per destination:

- `critical`: 6
- `warn`: 4
- `info`: 2

Backoff before attempt `n` (`n` starts at `2`):

- `delay_seconds = min(30 * 2**(n - 2), 900) + jitter_seconds`
- `jitter_seconds` uniform in `[0, 30]`

Retryable failure classes:

- transport timeout
- transient `5xx`/rate-limit responses
- temporary network/connectivity failures

Non-retryable failure classes:

- invalid destination configuration
- authentication/authorization rejected with non-recoverable status
- malformed payload rejected as client error

### Idempotency Key

Per destination idempotency key must be:

- `sha256("<event_id>|<destination_id>|<canonical_routing_key>|<severity>")`

Canonicalization and encoding rules:

- build the preimage string exactly in this order with literal `|` separators and no extra whitespace trimming:
  - `<event_id>|<destination_id>|<canonical_routing_key>|<severity>`
- encode preimage bytes as UTF-8.
- compute SHA-256 digest over those UTF-8 bytes.
- persist/emit idempotency key as lowercase hexadecimal (`64` chars).

Idempotency invariants:

- duplicate attempts with same idempotency key must not create duplicate operator-visible messages when destination supports dedupe keys
- when destination lacks native dedupe, transport must suppress duplicates via local delivery state
- duplicate-suppressed destination attempts must set `delivery.deliveries[i].status = suppressed_duplicate`
- when all destinations are duplicate-suppressed for an event, set `delivery.overall_status = suppressed_duplicate`

## Acknowledgment And Escalation Contract

Acknowledgment policy:

- `critical` alerts require acknowledgment.
- `warn` alerts default to acknowledgment required in v1.
- `info` alerts do not require acknowledgment.

Acknowledgment deadlines:

- when `ack_required = true`, `ack_deadline_utc` must be derived from route policy timeout:
  - `ack_deadline_utc = first_delivery_at_utc + ack_timeout_seconds`
- when `ack_required = false`, `ack_deadline_utc = null`
- `first_delivery_at_utc` comes from `delivery.first_delivery_at_utc` and must be set on first successful primary delivery.

Escalation behavior:

- escalation scheduling is driven by route-policy `escalation_schedule_seconds[]` from `delivery.first_delivery_at_utc`.
- for each schedule offset `t` in `escalation_schedule_seconds[]`:
  - if `ack_required = true`, `ack_state = pending`, and `now_utc >= first_delivery_at_utc + t`, enqueue escalation destinations for that step (exactly once per step).
  - record escalation attempt metadata in `delivery.delivery_receipts`.
- set/retain `incident_id` for acknowledged/escalated critical and warn alerts.
- if `ack_required = true` and `now_utc >= ack_deadline_utc` with no acknowledgment, transition `ack_state` to `expired` (independent of escalation-step count).

## Artifact And Audit Contract

Transport artifact root:

- `<ACCESSDANE_ARTIFACT_BASE_DIR>/alerts/<run_date>/<event_id>/`

`run_date` derivation rule:

- `run_date` must be UTC `YYYYMMDD` derived from `envelope.emitted_at_utc`.
- this derived `run_date` must be used consistently in artifact paths and envelope metadata.

Required artifacts:

- `alert_transport_envelope.json` (latest envelope snapshot)
- `delivery_attempts.jsonl` (append-only attempt log)
- `delivery_status.json` (terminal/latest status summary)

Retention policy:

- keep all `critical` transport artifacts for at least 90 days
- keep `warn|info` artifacts for at least 30 days

## Security And Secrets

- destination credentials (`SMTP_*`, `SLACK_*`, `PAGERDUTY_*`) must come from runtime secret injection only
- transport artifacts must never persist secret values, access tokens, or webhook URLs
- destination identifiers in artifacts must be logical ids, not raw secret-bearing endpoints

## Backward Compatibility And Versioning

v1 compatibility rules:

- producer alert payload contracts remain unchanged
- transport normalization is additive and must not mutate producer artifacts
- unknown producer fields must be ignored unless explicitly required by this contract

Breaking changes requiring v2:

- top-level transport envelope key changes
- canonical severity enum changes
- idempotency key derivation changes
- acknowledgment state machine changes

## Non-Goals Deferred To Later Sprints

- cross-org routing policies and tenant-scoped policy engines
- transport QoS partitioning by business-hours calendar
- automatic ticket creation workflows beyond incident id linkage
