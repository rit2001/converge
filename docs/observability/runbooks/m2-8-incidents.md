# M2.8 operational incident runbooks

These provider-neutral runbooks use fixed health responses, fixed metric outcomes, and bounded
structured-event codes. Replace no example with a real identifier, credential, URL, payload, SQL
statement, raw exception, or stack trace. Authorization remains enabled during every response.

## API HTTP/PostgreSQL outage

- **Meaning:** API `/health/ready` is unavailable, or worker `/health/ready` is unavailable when the
  corresponding core alert is firing.
- **User impact:** PostgreSQL-backed API requests or worker snapshot/compaction work may be unavailable.
- **Immediate safe checks:** Confirm liveness separately; compare replicas; check the bounded readiness
  response and approved PostgreSQL service status without running mutating queries.
- **Evidence to inspect:** API/worker lifecycle codes, probe history, sanitized database platform
  status, and whether socket or delivery alerts are inhibited by the core outage.
- **Safe mitigation:** Restore normal database connectivity or replace an unhealthy application
  replica through the approved deployment procedure. Preserve durable data and allow normal startup
  verification to gate readiness.
- **Escalate when:** Multiple replicas fail, durable storage reports corruption, recovery is not prompt,
  or any destructive database action is proposed.
- **Recovery confirmation:** Readiness returns 200 for the current lifecycle, ordinary bounded requests
  succeed, and no new core alert fires through one alert window.
- **Prohibited:** Do not reset or recreate PostgreSQL, run ad hoc repair SQL, disable authorization,
  expose connection details, or treat Redis recovery as proof of PostgreSQL health.

## Socket consumer/watchdog outage

- **Meaning:** HTTP readiness is healthy while `/health/socket-ready` is unavailable, or delivery
  failures are elevated.
- **User impact:** Live Socket.IO sessions are fenced; clients must recover authoritative state through
  PostgreSQL before resuming.
- **Immediate safe checks:** Confirm HTTP readiness remains 200; compare consumer and watchdog bounded
  lifecycle events; inspect socket and consumer readiness gauges and transition counters.
- **Evidence to inspect:** `converge_socket_ready`, `converge_delivery_consumer_ready`, fixed consumer /
  watchdog transition outcomes, and quarantine/cursor-loss machine codes.
- **Safe mitigation:** Restore the current Redis connection or resume valid outbox publication; allow
  continuity validation, parity checks, reconnection, and PostgreSQL catch-up to recover normally.
- **Escalate when:** Cursor loss, quarantine, repeated fencing, delivery gaps, or durable/Redis head
  divergence remains after dependencies recover.
- **Recovery confirmation:** Current-generation consumer and watchdog recovery restores socket
  readiness to one, a fresh authenticated client catches up, and counters do not double-increment.
- **Prohibited:** Do not flush Redis as a first response, reset cursors, skip an entry, disable
  authorization, force sockets ready, or claim exactly-once recovery.

## Redis/outbox publication outage

- **Meaning:** Worker core is healthy while delivery readiness is unavailable, or retry outcomes are
  elevated.
- **User impact:** Durable commits remain in PostgreSQL but live distributed delivery is delayed and may
  later duplicate under the at-least-once contract.
- **Immediate safe checks:** Confirm worker core readiness, Redis service status, outbox accepting state,
  and recent published/retry/blocked/stale outcome rates.
- **Evidence to inspect:** Delivery readiness probes, `converge_outbox_publications_total`, publication
  duration, active work, and bounded `outbox.publication.result` codes.
- **Safe mitigation:** Restore Redis connectivity and let leased/retry-wait rows proceed through normal
  claim and fencing. Preserve lease expiry and board ordering.
- **Escalate when:** A blocked head appears, retry rate remains elevated, leases do not clear, or Redis
  acknowledgement evidence is ambiguous for an extended interval.
- **Recovery confirmation:** Delivery readiness returns 200, pending eligible work progresses in board
  order, exact Redis entry IDs are persisted by normal finalization, and retries normalize.
- **Prohibited:** Do not mark outbox rows published without Redis publication evidence, bypass lease
  tokens, reorder a board, flush Redis as a first response, or claim exactly-once publication.

## Quarantined delivery or cursor loss

- **Meaning:** Strict validation, sequence continuity, deduplication evidence, or Redis cursor continuity
  failed closed.
- **User impact:** Affected realtime delivery is unavailable; sockets are fenced to prevent incorrect
  state.
- **Immediate safe checks:** Keep the process fail-closed; identify the bounded quarantine/cursor-loss
  code and compare durable PostgreSQL heads with approved Redis metadata without exposing identifiers.
- **Evidence to inspect:** Quarantined delivery counter, consumer lifecycle transitions, cursor-loss and
  board-quarantined structured events, and service readiness history.
- **Safe mitigation:** Preserve evidence, restrict unauthorized Redis writers, and use the established
  PostgreSQL recovery path after continuity has been re-established.
- **Escalate when:** Evidence suggests stream recreation/trimming, schema poison, sequence gaps, or a
  compromised Redis writer.
- **Recovery confirmation:** A current generation validates continuity or safely starts from its allowed
  boundary, clients reauthenticate and catch up, and no new quarantine outcome appears.
- **Prohibited:** Do not manually advance a cursor, delete evidence, force readiness, flush Redis as a
  first response, disable authorization, or claim exactly-once recovery.

## Blocked outbox event

- **Meaning:** A deterministic/non-retryable failure or attempt exhaustion durably blocked a board head.
- **User impact:** That board's later delivery events cannot overtake the blocked head; unrelated boards
  should continue.
- **Immediate safe checks:** Confirm the bounded blocked outcome, worker core readiness, and that
  unrelated publication continues. Preserve the row and its lease/failure evidence.
- **Evidence to inspect:** Blocked publication increase, bounded result code, attempt policy, and
  PostgreSQL/Redis publication evidence through approved tooling.
- **Safe mitigation:** Correct the underlying deterministic producer/configuration defect, then use an
  reviewed recovery procedure that retains board order and fencing.
- **Escalate when:** The envelope or durable evidence is inconsistent, multiple boards block, or any
  manual status mutation is proposed.
- **Recovery confirmation:** The head is processed through the normal repository transition, later
  events proceed in order, and no false retry/published outcome was recorded.
- **Prohibited:** Do not mark the row published without Redis evidence, skip/delete the head, edit the
  payload, bypass lease tokens, or claim exactly-once recovery.

## Recovery blocked or corrupt snapshot chain

- **Meaning:** Recovery cannot produce a trustworthy verified snapshot-plus-tail chain.
- **User impact:** A client cannot safely reconstruct authoritative board state; the fixed response is
  non-retryable until durable evidence is repaired or superseded through an approved process.
- **Immediate safe checks:** Preserve all snapshots, receipts, operations, floors, and bounded failure
  codes; determine whether an earlier verified snapshot plus complete tail remains valid.
- **Evidence to inspect:** `recovery_blocked` increases, recovery result codes, snapshot verification
  state, and approved immutable receipt/floor summaries.
- **Safe mitigation:** Isolate the suspected corruption and follow reviewed durable-data recovery. Use
  normal on-demand refresh only when the failure classification permits it.
- **Escalate when:** Hash, schema, tail, floor, or receipt evidence conflicts, or no complete trustworthy
  chain exists.
- **Recovery confirmation:** The normal authenticated recovery route returns verified material whose
  reconstructed state matches authority, and success counters resume without hiding the prior event.
- **Prohibited:** Do not skip corrupt snapshots silently, delete receipts, manually advance recovery
  floors, reset PostgreSQL, disable authorization, or claim exactly-once recovery.

## Snapshot failure

- **Meaning:** Snapshot capture returned a deterministic or transient failure, or snapshot latency is
  above policy.
- **User impact:** Current writes and outbox delivery may continue, but recovery material may age and
  later recovery work may increase.
- **Immediate safe checks:** Confirm worker core readiness and Redis independence; distinguish busy,
  no-progress, deterministic, and transient outcomes; inspect active work and retry/cooldown evidence.
- **Evidence to inspect:** Snapshot outcome counter, duration histogram, active-work gauge, bounded
  snapshot result code, and verified-snapshot counts through approved tooling.
- **Safe mitigation:** Restore PostgreSQL capacity for transient failures; allow bounded backoff,
  cooldown, or changed-head retry. Correct deterministic payload/schema defects before retrying.
- **Escalate when:** Deterministic failures repeat across changed heads, active work exceeds the manual
  investigation boundary, or recovery has no trustworthy snapshot chain.
- **Recovery confirmation:** A later authoritative capture succeeds, active work returns to zero,
  heads remain unchanged by capture, and recovery verifies the new generation.
- **Prohibited:** Do not fabricate verification, mutate board heads, skip corrupt snapshots silently,
  delete receipts, manually advance floors, or require Redis for snapshot recovery.

## Compaction blocked or failed

- **Meaning:** Compaction was blocked, failed transiently, or exceeded its latency objective.
- **User impact:** Writes and recovery remain authoritative, but covered history is retained and storage
  growth may continue.
- **Immediate safe checks:** Confirm worker core readiness, compaction enablement, verified boundary,
  immutable receipts, safely published outbox evidence, and coupled-floor state.
- **Evidence to inspect:** Compaction outcomes, duration, active work, bounded result code, and approved
  floor/history summaries.
- **Safe mitigation:** Preserve history and allow bounded transient retry. Resolve missing publication or
  receipt evidence before allowing the normal authoritative repository call to compact.
- **Escalate when:** Evidence conflicts, blocked outcomes repeat for a changed boundary, deletion/floor
  atomicity is questioned, or manual floor movement is proposed.
- **Recovery confirmation:** Normal compaction either succeeds atomically or reports harmless
  no-progress/no-boundary; recovery through immutable receipts remains valid.
- **Prohibited:** Do not delete receipts, manually advance either recovery floor, delete uncovered
  history, forge publication evidence, reset PostgreSQL, or require Redis for compaction.

## Stuck background work

- **Meaning:** An active-work gauge appears continuously positive beyond the manual investigation
  boundary. Current telemetry cannot prove that the same attempt is stuck.
- **User impact:** Publication, snapshot, or compaction capacity may be occupied, but queue depth and
  per-attempt age are unknown.
- **Immediate safe checks:** Correlate gauge behavior with completed outcome-counter increases and
  duration observations. Use 10s for default outbox publication/finalization and 120s for snapshot or
  compaction as investigation boundaries, not executable alerts.
- **Evidence to inspect:** Active-work gauges, outcome deltas, duration histograms, readiness, and
  component lifecycle codes.
- **Safe mitigation:** Let configured timeouts, fencing, drain, and retry policy act. If turnover is
  continuing, do not classify the aggregate gauge as stuck.
- **Escalate when:** Outcomes stop, readiness degrades, shutdown grace expires, or approved dependency
  evidence shows an unresolved operation.
- **Recovery confirmation:** Active work returns to zero during an idle interval, outcomes resume, and
  shutdown/restart leaves no late success telemetry.
- **Prohibited:** Do not infer queue depth, abandon leases outside normal shutdown policy, kill database
  work without review, force gauges to zero, or claim a specific attempt from aggregate evidence.

## High recovery or publication latency

- **Meaning:** A guarded 15-minute p99 estimate exceeds the initial recovery or publication objective.
- **User impact:** Clients recover slowly or durable events reach Redis slowly; availability may still be
  healthy.
- **Immediate safe checks:** Verify the minimum sample guard, scrape continuity, current readiness, and
  whether a small number of boundary-bucket observations dominate the estimate.
- **Evidence to inspect:** The applicable histogram buckets/count, classified outcomes, active work,
  retry rate, and bounded dependency health.
- **Safe mitigation:** Restore dependency capacity, remove approved resource saturation, and let normal
  retries/fencing complete. Treat histogram thresholds as initial policy, not capacity proof.
- **Escalate when:** Latency persists with adequate volume, error ratios rise, readiness fails, or the
  histogram lacks resolution for a decision.
- **Recovery confirmation:** Guarded p99 remains within objective for one full alert window and outcome
  ratios/readiness remain healthy.
- **Prohibited:** Do not weaken thresholds without evidence, disable authorization, bypass durable
  finalization, expose raw diagnostics, flush Redis as a first response, or claim benchmark capacity.
