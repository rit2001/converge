# M2.8 bounded telemetry contract

This contract defines provider-neutral telemetry primitives. Domain code receives an injected
recorder; it does not depend on Prometheus, OpenTelemetry, a SaaS provider, or a process-global
registry. API and worker production paths use explicitly owned recorders without changing domain
outcomes.

## Metric catalog

Metric names, types, labels, label values, and histogram buckets are fixed at compile time. A
recorder rejects unknown names, missing or extra labels, unknown label values, type/name mismatches,
and invalid numbers. It never creates a metric dynamically.

| Metric                                         | Type                           | Labels                                                                                                           |
| ---------------------------------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `converge_delivery_events_total`               | Counter                        | `event_type=operation\|membership_revoked`, `outcome=handled\|duplicate\|quarantined\|failed`                    |
| `converge_delivery_state_transitions_total`    | Counter                        | `source=consumer\|watchdog\|socket_readiness`, `state=established\|unavailable\|recovering\|recovered\|terminal` |
| `converge_outbox_publications_total`           | Counter                        | `outcome=published\|retry\|blocked\|stale`                                                                       |
| `converge_snapshot_runs_total`                 | Counter                        | `outcome=captured\|busy\|no_progress\|deterministic_failure\|transient_failure`                                  |
| `converge_compaction_runs_total`               | Counter                        | `outcome=compacted\|no_progress\|no_boundary\|blocked\|transient_failure`                                        |
| `converge_recovery_requests_total`             | Counter                        | `outcome=snapshot_tail\|refreshed\|recovery_blocked\|retryable_failure\|authorization_failure`                   |
| `converge_outbox_publication_duration_seconds` | Histogram                      | None                                                                                                             |
| `converge_snapshot_duration_seconds`           | Histogram                      | None                                                                                                             |
| `converge_compaction_duration_seconds`         | Histogram                      | None                                                                                                             |
| `converge_recovery_duration_seconds`           | Histogram                      | None                                                                                                             |
| `converge_socket_ready`                        | Binary gauge                   | None                                                                                                             |
| `converge_delivery_consumer_ready`             | Binary gauge                   | None                                                                                                             |
| `converge_outbox_active_work`                  | Nonnegative safe-integer gauge | None                                                                                                             |
| `converge_snapshot_active_work`                | Nonnegative safe-integer gauge | None                                                                                                             |
| `converge_compaction_active_work`              | Nonnegative safe-integer gauge | None                                                                                                             |

Counters accept positive finite increments. Histograms accept nonnegative finite seconds and use
immutable cumulative buckets at `0.005`, `0.01`, `0.025`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2.5`,
`5`, `10`, `30`, `60`, `120`, and `300` seconds. Readiness gauges accept only zero or one;
active-work gauges accept nonnegative safe integers.

Metric labels never contain board, principal, operation, event, snapshot, socket, Redis-entry, or
instance identifiers. Error messages, URLs, SQL, and any caller-defined label are forbidden.

## Structured diagnostic events

The bounded event catalog is:

- `delivery.consumer.lifecycle`
- `delivery.cursor_lost`
- `delivery.board_quarantined`
- `delivery.watchdog.divergence`
- `socket.readiness.changed`
- `outbox.publication.result`
- `snapshot.capture.result`
- `compaction.result`
- `recovery.request.result`
- `worker.lifecycle`
- `api.lifecycle`

Every event has schema version 1, a catalog event name, severity, event-compatible component,
canonical injected timestamp, and a bounded machine code. Each event has its own correlation-key
allowlist drawn from `boardId`, `eventId`, `operationId`, `snapshotId`, `socketId`, `redisEntryId`, and
`correlationId`. Values are control-character sanitized and length bounded. Unknown fields and
event-inappropriate identifiers are rejected.

Events never contain board contents, commands, envelopes, snapshot or operation payloads,
credentials, tokens, database/Redis URLs, principal display names, SQL, raw exceptions, or stack
traces. Error conversion is explicit: classified failures expose a bounded machine code; unexpected
failures expose only `UNEXPECTED_ERROR` and an internal correlation ID.

## Recording, export, and failure isolation

The no-op recorder is safe for production defaults and never throws. The deterministic in-memory
recorder retains only the finite metric catalog and a configurable positive bounded number of events;
event eviction is oldest first. Snapshots contain sorted immutable copies, cumulative histogram
buckets, counter totals, gauges, and retained events. Caller mutation cannot alter recorder state.

The safe recorder wrapper contains synchronous throws and asynchronous rejections. One bounded
fallback notification may run for each failed telemetry call; fallback failures are suppressed.
Telemetry therefore cannot change delivery, recovery, snapshot, compaction, or other domain results.

## Prometheus text rendering

The dependency-free exporter renders immutable recorder snapshots in Prometheus text exposition
format 0.0.4 with content type `text/plain; version=0.0.4; charset=utf-8`. HELP descriptions, metric
names, types, label names and values, and histogram buckets come only from the fixed catalog.
Observed families and label combinations are ordered deterministically; repeated rendering of the
same snapshot is byte-identical. Label backslashes, double quotes, and newlines use Prometheus
escaping, numeric output is locale-independent, and the body ends with exactly one newline.

Counters and gauges include only series present in the snapshot. Histograms include cumulative
catalog buckets in catalog order, a `+Inf` bucket equal to the total count, sum, and count. An
unobserved histogram has no snapshot series and is omitted. Consequently, a wholly empty snapshot
renders as one newline.

Rendering validates the immutable snapshot again and fails closed on unknown metrics, unexpected
labels, type mismatches, duplicate series, invalid values, malformed events, or an invalid histogram
layout. Structured diagnostic events are validated but never converted to or included in Prometheus
metrics. The safe rendering wrapper returns one fixed sanitized error and suppresses synchronous or
asynchronous fallback failures without retaining or mutating the snapshot.

## API operational endpoints

The existing API Fastify server exposes three unauthenticated, fixed-body health routes with distinct
meanings. `GET /health/live` reports only whether the API lifecycle is running. `GET /health/ready`
performs one timeout-bounded PostgreSQL probe and reports whether PostgreSQL-backed HTTP service is
ready. `GET /health/socket-ready` performs no dependency command and reports the existing composite
Socket.IO delivery gate: local mode is ready while the application is running, while distributed mode
requires current consumer and watchdog health. Redis delivery failure can therefore make socket
readiness unavailable without changing healthy HTTP/PostgreSQL readiness.

`GET /metrics` is absent unless `API_METRICS_ENABLED` is exactly `true`. Enabling it requires a
bounded printable `API_METRICS_BEARER_TOKEN`; requests use a dedicated Bearer header and timing-safe
comparison rather than board/user authentication. Missing and invalid credentials share one fixed
401 response. Authorized requests render an immutable snapshot from the API-owned recorder, preserve
the Prometheus content type and body, and return one fixed plain-text 503 response if snapshotting or
rendering fails. Scrapes emit no telemetry and perform no board, PostgreSQL, or Redis work.

Worker HTTP exposition, deployment probe selection, dashboards, alert thresholds, external
collectors, deployment integration, and benchmark claims remain explicitly deferred.
