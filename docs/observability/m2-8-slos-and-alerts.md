# M2.8 operational SLO and alert policy

This document defines the initial provider-neutral operating policy for Converge. The objectives are
engineering targets for future production operation. They are not measured achievements, capacity
claims, or benchmark results. Executable alert rules, collectors, Alertmanager/provider integration,
dashboards, deployment probes, and benchmarks remain outside this slice.

The policy uses only the fixed telemetry catalog and existing health endpoints. Infrastructure may
attach bounded routing labels such as `service`, `deployment`, and `environment` when scraping, but
application metric labels remain exactly those in the telemetry catalog. Never attach a board,
principal, operation, event, socket, snapshot, Redis-entry, URL, SQL, error-message, or other
instance-specific value as a metric label.

## Service-level indicators and objectives

All objectives use a rolling 30-day window. Scheduled maintenance is excluded only when an external
change record identifies its exact interval; application telemetry does not infer maintenance.

| Service objective                 | SLI                                                                                                                    | Initial objective |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | ----------------- |
| HTTP API availability             | Fraction of bounded probes for API `GET /health/ready` that return 200                                                 | 99.9%             |
| Distributed socket availability   | Fraction of bounded probes for API `GET /health/socket-ready` that return 200, corroborated by `converge_socket_ready` | 99.5%             |
| Worker core availability          | Fraction of bounded probes for worker `GET /health/ready` that return 200                                              | 99.9%             |
| Delivery publication availability | Fraction of bounded probes for worker `GET /health/delivery-ready` that return 200                                     | 99.5%             |
| Trustworthy recovery success      | Successful classified recovery requests divided by all non-authorization classified recovery requests                  | 99.9%             |

API HTTP readiness is PostgreSQL-backed. Redis consumer or watchdog failure alone must not count as
HTTP API unavailability. Worker core readiness represents PostgreSQL verification plus snapshot and,
when enabled, compaction startup. Redis failure alone must not count as worker-core unavailability.
Socket and delivery readiness deliberately carry the Redis-dependent availability signals.

Recovery success uses:

- numerator: `snapshot_tail + refreshed`;
- denominator: `snapshot_tail + refreshed + recovery_blocked + retryable_failure`; and
- exclusion: `authorization_failure`.

The exact 30-day recovery ratio is:

```promql
sum(increase(converge_recovery_requests_total{outcome=~"snapshot_tail|refreshed"}[30d]))
/
sum(increase(converge_recovery_requests_total{outcome=~"snapshot_tail|refreshed|recovery_blocked|retryable_failure"}[30d]))
```

Evaluate it only when the denominator is nonzero. The four availability SLOs require an external
bounded HTTP probe history. No current application metric represents API HTTP readiness, worker core
readiness, or worker delivery readiness, so their authoritative SLO calculations are not PromQL in
this slice. `converge_socket_ready` is valid corroborating evidence but does not replace probe history
or externally recorded maintenance windows.

## Latency objectives

| Operation                  | Existing histogram                             | Initial p99 objective |
| -------------------------- | ---------------------------------------------- | --------------------- |
| Recovery request           | `converge_recovery_duration_seconds`           | under 10 seconds      |
| Outbox publication attempt | `converge_outbox_publication_duration_seconds` | under 5 seconds       |
| Snapshot capture           | `converge_snapshot_duration_seconds`           | under 60 seconds      |
| Compaction attempt         | `converge_compaction_duration_seconds`         | under 60 seconds      |

These thresholds seed SLO evaluation and warning alerts. They are not benchmark evidence. M2.8
benchmark work may revise them only from reproducible measurements with the topology, workload,
sample count, and percentile method recorded.

## Alert catalog

Probe-based alerts are specifications for a future bounded HTTP probe provider. They intentionally
have no PromQL because the catalog emits no probe-success metric. Metric-based alerts below include
exact PromQL. Every expression is evaluated after deployment/service-level rollup; provider routing
labels must not be copied into application metric labels.

| Alert                                  | Severity | Condition                                                                      | `for` | Runbook                                                                                                          |
| -------------------------------------- | -------- | ------------------------------------------------------------------------------ | ----- | ---------------------------------------------------------------------------------------------------------------- |
| `ConvergeApiHttpUnavailable`           | critical | API `/health/ready` probe is not 200                                           | 2m    | [API HTTP/PostgreSQL outage](runbooks/m2-8-incidents.md#api-httppostgresql-outage)                               |
| `ConvergeSocketDeliveryUnavailable`    | critical | API `/health/socket-ready` is not 200 while `/health/ready` is 200             | 2m    | [Socket consumer/watchdog outage](runbooks/m2-8-incidents.md#socket-consumerwatchdog-outage)                     |
| `ConvergeWorkerCoreUnavailable`        | critical | Worker `/health/ready` probe is not 200                                        | 2m    | [API HTTP/PostgreSQL outage](runbooks/m2-8-incidents.md#api-httppostgresql-outage)                               |
| `ConvergeOutboxDeliveryUnavailable`    | critical | Worker `/health/delivery-ready` is not 200 while worker `/health/ready` is 200 | 2m    | [Redis/outbox publication outage](runbooks/m2-8-incidents.md#redisoutbox-publication-outage)                     |
| `ConvergeDeliveryQuarantineDetected`   | critical | Any quarantined delivery outcome                                               | 0m    | [Quarantined delivery/cursor loss](runbooks/m2-8-incidents.md#quarantined-delivery-or-cursor-loss)               |
| `ConvergeDeliveryFailuresElevated`     | warning  | Failed outcomes exceed 1% with at least 20 handled/failed outcomes             | 0m    | [Socket consumer/watchdog outage](runbooks/m2-8-incidents.md#socket-consumerwatchdog-outage)                     |
| `ConvergeOutboxBlockedDetected`        | critical | Any blocked publication outcome                                                | 0m    | [Blocked outbox event](runbooks/m2-8-incidents.md#blocked-outbox-event)                                          |
| `ConvergeOutboxRetriesElevated`        | warning  | Retries exceed 5% with at least 20 published/retry attempts                    | 0m    | [Redis/outbox publication outage](runbooks/m2-8-incidents.md#redisoutbox-publication-outage)                     |
| `ConvergeRecoveryBlockedDetected`      | critical | Any blocked recovery outcome                                                   | 0m    | [Recovery blocked/corrupt snapshot chain](runbooks/m2-8-incidents.md#recovery-blocked-or-corrupt-snapshot-chain) |
| `ConvergeRecoveryFailuresElevated`     | warning  | Retryable failures exceed 1% with at least 20 successful/retryable requests    | 0m    | [High recovery/publication latency](runbooks/m2-8-incidents.md#high-recovery-or-publication-latency)             |
| `ConvergeSnapshotFailuresElevated`     | warning  | Any deterministic or transient snapshot failure                                | 0m    | [Snapshot failure](runbooks/m2-8-incidents.md#snapshot-failure)                                                  |
| `ConvergeCompactionBlockedDetected`    | warning  | Any blocked compaction outcome                                                 | 0m    | [Compaction blocked/failure](runbooks/m2-8-incidents.md#compaction-blocked-or-failed)                            |
| `ConvergeBackgroundWorkStuck`          | warning  | Deferred: current aggregate active-work gauges cannot prove attempt age        | —     | [Stuck background work](runbooks/m2-8-incidents.md#stuck-background-work)                                        |
| `ConvergeRecoveryLatencyHigh`          | warning  | Recovery p99 exceeds 10s with at least 20 observations                         | 15m   | [High recovery/publication latency](runbooks/m2-8-incidents.md#high-recovery-or-publication-latency)             |
| `ConvergeOutboxPublicationLatencyHigh` | warning  | Publication p99 exceeds 5s with at least 20 observations                       | 15m   | [High recovery/publication latency](runbooks/m2-8-incidents.md#high-recovery-or-publication-latency)             |
| `ConvergeSnapshotLatencyHigh`          | warning  | Snapshot p99 exceeds 60s with at least 5 observations                          | 15m   | [Snapshot failure](runbooks/m2-8-incidents.md#snapshot-failure)                                                  |
| `ConvergeCompactionLatencyHigh`        | warning  | Compaction p99 exceeds 60s with at least 5 observations                        | 15m   | [Compaction blocked/failure](runbooks/m2-8-incidents.md#compaction-blocked-or-failed)                            |

Counter conditions already contain their required lookback, so their rule-level `for` is zero. Probe
and latency conditions use the explicit sustained durations above.

### Counter alert PromQL

`ConvergeDeliveryQuarantineDetected`:

```promql
sum(increase(converge_delivery_events_total{outcome="quarantined"}[5m])) > 0
```

`ConvergeDeliveryFailuresElevated`:

```promql
(
  sum(increase(converge_delivery_events_total{outcome="failed"}[10m]))
  /
  sum(increase(converge_delivery_events_total{outcome=~"handled|failed"}[10m]))
) > 0.01
and
sum(increase(converge_delivery_events_total{outcome=~"handled|failed"}[10m])) >= 20
```

`ConvergeOutboxBlockedDetected`:

```promql
sum(increase(converge_outbox_publications_total{outcome="blocked"}[5m])) > 0
```

`ConvergeOutboxRetriesElevated`:

```promql
(
  sum(increase(converge_outbox_publications_total{outcome="retry"}[10m]))
  /
  sum(increase(converge_outbox_publications_total{outcome=~"published|retry"}[10m]))
) > 0.05
and
sum(increase(converge_outbox_publications_total{outcome=~"published|retry"}[10m])) >= 20
```

`ConvergeRecoveryBlockedDetected`:

```promql
sum(increase(converge_recovery_requests_total{outcome="recovery_blocked"}[5m])) > 0
```

`ConvergeRecoveryFailuresElevated`:

```promql
(
  sum(increase(converge_recovery_requests_total{outcome="retryable_failure"}[10m]))
  /
  sum(increase(converge_recovery_requests_total{outcome=~"snapshot_tail|refreshed|retryable_failure"}[10m]))
) > 0.01
and
sum(increase(converge_recovery_requests_total{outcome=~"snapshot_tail|refreshed|retryable_failure"}[10m])) >= 20
```

`ConvergeSnapshotFailuresElevated`:

```promql
sum(increase(converge_snapshot_runs_total{outcome=~"deterministic_failure|transient_failure"}[15m])) > 0
```

`ConvergeCompactionBlockedDetected`:

```promql
sum(increase(converge_compaction_runs_total{outcome="blocked"}[15m])) > 0
```

### Latency alert PromQL

`ConvergeRecoveryLatencyHigh`:

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(converge_recovery_duration_seconds_bucket[15m]))
) > 10
and
sum(increase(converge_recovery_duration_seconds_count[15m])) >= 20
```

`ConvergeOutboxPublicationLatencyHigh`:

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(converge_outbox_publication_duration_seconds_bucket[15m]))
) > 5
and
sum(increase(converge_outbox_publication_duration_seconds_count[15m])) >= 20
```

`ConvergeSnapshotLatencyHigh`:

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(converge_snapshot_duration_seconds_bucket[15m]))
) > 60
and
sum(increase(converge_snapshot_duration_seconds_count[15m])) >= 5
```

`ConvergeCompactionLatencyHigh`:

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(converge_compaction_duration_seconds_bucket[15m]))
) > 60
and
sum(increase(converge_compaction_duration_seconds_count[15m])) >= 5
```

### Encoded metric-backed rules

The twelve metric-backed alerts above are encoded in
`ops/prometheus/converge-alerts.yml` as the single provider-neutral
`converge-m2-operational-alerts` group. Repository contract tests parse the YAML and enforce the
documented order, expressions, thresholds, durations, labels, runbook references, catalog membership,
minimum-volume guards, and privacy constraints. The four external-probe alerts and
`ConvergeBackgroundWorkStuck` remain deferred because the required probe history and attempt-age
evidence do not exist in the fixed metric catalog.

`promtool` was not available locally when these rules were encoded, so binary PromQL validation
remains pending for the final M2 gate. The rule file is not deployed, and Alertmanager routing,
provider integration, and dashboards remain pending.

### Deferred stuck-work alert

The manual investigation boundaries are:

- outbox publication: configured Redis publication timeout plus database finalization margin, 10s
  under the current 5s + 5s defaults;
- snapshot capture: 120s, twice the initial 60s latency objective; and
- compaction: 120s, twice the initial 60s latency objective.

However, `converge_*_active_work > 0` can remain continuously positive while different attempts
complete and replace one another. It exposes concurrency, not attempt age or identity. Exact
stuck-work PromQL is therefore deferred until bounded attempt-age evidence exists. Do not substitute
queue depth: no queue depth metric exists. Operators may inspect a sustained gauge manually using the
runbook, but it must not page as `ConvergeBackgroundWorkStuck` in this slice.

## Grouping, inhibition, and deduplication

- Group by bounded `service` and alert name. Provider-owned `deployment` and `environment` may route
  notifications but must not become application metric labels.
- A worker-core critical alert inhibits delivery-readiness alerts for the same deployment.
- An API HTTP critical alert inhibits socket-readiness alerts for the same deployment.
- A critical alert inhibits warning alerts for the same subsystem while it is firing.
- Never group or route by board, principal, operation, event, socket, snapshot, or Redis-entry ID.
- Roll duplicate replica alerts up to deployment/service level. Preserve replica evidence only in the
  provider's target inventory, not in notification grouping keys or application labels.
- Alertmanager/provider syntax and inhibition implementation remain deferred.

## Evidence, privacy, and limitations

Prometheus metrics contain only fixed low-cardinality labels and bounded machine outcomes. Structured
events are bounded diagnostic evidence, not Prometheus labels and not dynamically converted into
metrics. Operational examples and notifications must not contain identifiers, credentials, URLs,
payloads, SQL, raw errors, or stacks.

Current telemetry does not expose queue depth, oldest-work age, PostgreSQL probe counters, worker
readiness gauges, maintenance intervals, request traffic totals, or end-to-end user delivery latency.
Do not infer those signals. Publication is at least once and may duplicate after ambiguous Redis
acceptance; neither alerts nor recovery confirmation may claim exactly-once delivery.

## Tuning policy

Review objectives and thresholds after the first 30 days with complete probe and scrape coverage, and
after any material workload or topology change. A change requires reproducible evidence, observed
sample volume, false-positive/false-negative review, incident review where applicable, and an explicit
documentation change. Never tune by adding dynamic labels, weakening privacy, excluding unrecorded
outages, or converting an unmet objective into a claimed benchmark. More sensitive thresholds are
allowed when evidence supports them; less sensitive thresholds require an accepted risk decision.
