# Milestone 2 failure-recovery matrix

Status: accepted Milestone 2 failure gate

This matrix defines deterministic acceptance cases for durable distributed delivery. PostgreSQL is
the durable authority in every case. Redis delivery and Socket.IO delivery are at least once; tests
must assert final state, ordering, authorization, and bounded recovery rather than exact message
counts unless a count is explicitly part of the invariant.

## Test controls

The implementation must expose test-only barriers through injected hooks, not wall-clock sleeps:

- after domain mutation and outbox insert, before database commit;
- after database commit, before the API acknowledgement;
- after outbox lease commit, before `XADD`;
- after successful `XADD`, before the outbox published update;
- before and after conditional lease-token updates;
- before API envelope validation, local handling, and in-memory stream-cursor advancement;
- after snapshot head capture and before projection read; and
- before compaction floor update and deletion.

Use two independently constructed API instances, two worker instances with distinct owner IDs, a
real PostgreSQL database, and a disposable real Redis instance where stream semantics matter. A
controllable clock supplies lease expiry and retry deadlines. Polling helpers must have hard
deadlines and report diagnostic state. Random worker jitter is replaced by a seeded deterministic
scheduler. Process termination tests may use child processes, but ordinary race tests should prefer
barriers so the important boundary is exact.

## Matrix

| ID  | Scenario and injection                                                                                                                | Required assertion                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Layer                         |
| --- | ------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| F01 | Commit succeeds; originating API stops before live delivery/acknowledgement.                                                          | Operation, projection, receipt, sequence, and one pending outbox row exist atomically. A worker later publishes it; reconnecting clients converge through catch-up even if no live event arrives.                                                                                                                                                                                                                                                                                                                     | Integration/failure           |
| F02 | Worker leases a row and stops before `XADD`.                                                                                          | No Redis entry exists. The row remains leased until controlled expiry, is reclaimed with a new token, and is published once by the reclaimer.                                                                                                                                                                                                                                                                                                                                                                         | Failure                       |
| F03 | Worker receives an `XADD` entry ID and stops before marking the row published.                                                        | After lease expiry another worker republishes the same event ID with a different Redis entry ID, conditionally marks the row published, and API/client deduplication applies it once.                                                                                                                                                                                                                                                                                                                                 | Failure                       |
| F04 | A second worker tries before lease expiry, then after expiry.                                                                         | It cannot claim the live lease; after clock advance it reclaims it. A stale first worker cannot update the row because its lease token no longer matches.                                                                                                                                                                                                                                                                                                                                                             | Integration                   |
| F05 | Two workers claim while one board has delivery sequences `N` and `N+1`.                                                               | Only `N` is claimable. `N+1` stays unavailable until `N` is published; `SKIP LOCKED` does not bypass the predecessor.                                                                                                                                                                                                                                                                                                                                                                                                 | Integration                   |
| F06 | Two boards each have a pending head while one publisher is paused.                                                                    | Another worker claims and publishes the other board without waiting. Each board remains internally ordered.                                                                                                                                                                                                                                                                                                                                                                                                           | Integration                   |
| F07 | Redis disconnects before `XADD` is accepted.                                                                                          | Worker records a bounded retryable error, releases to `retry_wait`, applies deterministic backoff, and does not set `published_at`.                                                                                                                                                                                                                                                                                                                                                                                   | Failure                       |
| F08 | Redis disconnects after returning the `XADD` ID but before PostgreSQL acknowledgement.                                                | Outcome is treated as ambiguous and may duplicate through F03. It never permits a later same-board event to overtake the unacknowledged row.                                                                                                                                                                                                                                                                                                                                                                          | Failure                       |
| F09 | Plain `XREAD` disconnects with active sockets before and after entries are processed.                                                 | Readiness becomes false first, all local sockets close within the lifecycle bound, and the surviving process retains only its last fully processed stream cursor. A valid cursor resumes strictly after it; clients reauthenticate and catch up.                                                                                                                                                                                                                                                                      | Failure/Playwright            |
| F10 | Outbox retry appends the same stable event ID under two different Redis stream IDs.                                                   | Event ID/delivery-sequence deduplication suppresses the second operation or revocation effect while the global stream cursor advances after handling each transport entry.                                                                                                                                                                                                                                                                                                                                            | Integration                   |
| F11 | Envelope has a future sequence, wrong board, unknown version/type, unknown field, oversized payload, or mismatched payload identity.  | No local room emission occurs. A forward gap fail-closes the affected board; global schema poison makes the API not ready. The error is bounded and structured.                                                                                                                                                                                                                                                                                                                                                       | Unit/integration              |
| F12 | Revocation is committed through API A while the target has sockets on API B.                                                          | One durable revocation event is published. API B emits only the content-free revocation notice, removes/disconnects every target socket for that board, and leaves other principals/boards intact.                                                                                                                                                                                                                                                                                                                    | Multi-instance integration    |
| F13 | Revocation delivery sequence `N` is paused while operation `N+1` commits.                                                             | Worker cannot publish `N+1` first. Every API evicts the revoked principal at `N` before any local emission for `N+1`; the principal receives no later operation.                                                                                                                                                                                                                                                                                                                                                      | Failure                       |
| F14 | A surviving API process cursor is overtaken by Redis trimming or stream recreation.                                                   | API detects metadata/cursor loss, increments the overrun metric, remains fail-closed, resets to a safe current tail only with no accepted sockets, issues `XREAD`, and requires PostgreSQL recovery on reconnect.                                                                                                                                                                                                                                                                                                     | Failure                       |
| F15 | A writer waits while snapshot creation is paused after acquiring the board lock.                                                      | Snapshot head, full projection, and canonical hash represent one sequence. The writer receives the next canvas/delivery sequences only after snapshot commit.                                                                                                                                                                                                                                                                                                                                                         | Integration                   |
| F16 | Snapshot JSON, schema version, byte count, or canonical hash is corrupted.                                                            | The snapshot is never served. The system falls back only when a verified earlier snapshot plus complete tail exists; otherwise it marks recovery operator-blocked without deleting more history.                                                                                                                                                                                                                                                                                                                      | Failure                       |
| F17 | Client sequence is below `operation_recovery_floor`.                                                                                  | Tail-only request returns the strict snapshot-required recovery response. Client replaces committed state with a verified snapshot, applies the contiguous tail, preserves pending commands, and reaches the current hash.                                                                                                                                                                                                                                                                                            | Final acceptance/integration  |
| F18 | Two APIs with independent plain `XREAD` cursors and multiple clients receive duplicates, reconnect, and submit concurrent operations. | Each active API observes every new stream event, and all authorized clients eventually match the PostgreSQL snapshot, canvas head, object order, and canonical hash. No client reports `READY` with a gap.                                                                                                                                                                                                                                                                                                            | Multi-instance Playwright     |
| F19 | A worker exhausts the maximum attempt count for a board head.                                                                         | The row becomes `blocked`, later events for that board remain blocked, unrelated boards continue, readiness/metrics expose the condition, and no automatic skip occurs.                                                                                                                                                                                                                                                                                                                                               | Integration                   |
| F20 | Operator retries a blocked row while a stale lease owner finishes.                                                                    | A new token fences the stale completion. Only the current token can publish/transition; duplicates remain harmless.                                                                                                                                                                                                                                                                                                                                                                                                   | Integration                   |
| F21 | Compaction races a writer and a publisher.                                                                                            | The board lock serializes the writer. Compaction refuses any unpublished event at or below the proposed delivery floor and never removes the outbox head.                                                                                                                                                                                                                                                                                                                                                             | Final acceptance/integration  |
| F22 | Process stops after compaction deletes operations but before response.                                                                | Floor and deletions are atomic. Exact replay succeeds from the compact receipt and creates no new event. Snapshot-plus-tail remains contiguous.                                                                                                                                                                                                                                                                                                                                                                       | Final acceptance/failure      |
| F23 | Membership is revoked while Redis is entirely unavailable.                                                                            | PostgreSQL removal/outbox commit may succeed over HTTP; every API has already failed closed and accepts no Socket.IO clients. Recovery reauthenticates before any board delivery.                                                                                                                                                                                                                                                                                                                                     | Multi-instance failure        |
| F24 | API joins race with revocation consumption.                                                                                           | The board-local coordinator yields only two safe outcomes: join completes first and the later revocation evicts it, or revocation/DB deletion completes first and join is forbidden.                                                                                                                                                                                                                                                                                                                                  | Integration                   |
| F25 | API receives a same-board stream gap caused by a missing revocation followed by an operation.                                         | It emits neither the later operation nor any board data, disconnects the room, and recovers/fails closed according to the PostgreSQL delivery floor.                                                                                                                                                                                                                                                                                                                                                                  | Security integration          |
| F26 | Redis is absent after total data loss while outbox contains rows already marked published.                                            | APIs disconnect on Redis loss. A restarted process atomically creates and verifies one reserved generation sentinel, captures its validated last-generated ID with no inherited sockets, establishes plain `XREAD`, and accepts only sessions that reauthorize and use PostgreSQL snapshot/tail. Recovery of an already-observed missing stream remains cursor loss. Durable state is unchanged.                                                                                                                      | Failure                       |
| F27 | A revocation outbox head becomes operator-blocked and no later Redis event reveals a gap.                                             | The batched active-board watchdog observes a higher delivery head, disconnects every local socket for that board, and records fail-closed diagnostics; unrelated boards remain connected.                                                                                                                                                                                                                                                                                                                             | Security integration          |
| F28 | An event is appended after validated startup tail/sentinel capture but before the first blocking read reaches Redis.                  | `XREAD` using the captured exclusive lower bound returns and handles the event once in that local consumer path; the reserved sentinel is never handled as delivery data, the API does not report ready before the loop is issued, and a later client obtains authoritative state through normal join/catch-up.                                                                                                                                                                                                       | Integration                   |
| F29 | Entry `i` in a multi-entry batch fails strict validation or local handling.                                                           | Earlier successful entries may advance the cursor; entry `i` and all later returned entries do not. The next `XREAD` starts at the last success and returns `i` again; no unknown/failed entry is skipped.                                                                                                                                                                                                                                                                                                            | Failure                       |
| F30 | An API process stops with an advanced cursor and connected clients, then a new process starts.                                        | Old sockets die with the old process. The new process inherits no cursor or Redis lifecycle state, captures the current tail, establishes `XREAD` before readiness, and clients recover missed durable state from PostgreSQL.                                                                                                                                                                                                                                                                                         | Failure/Playwright            |
| F31 | Many sockets share more active boards than one watchdog batch across two API replicas.                                                | Each replica issues at most one query for 100 distinct boards per jittered five-second interval, selects boards round-robin, never queries per socket, and cannot advance a board cursor or release a revocation gap.                                                                                                                                                                                                                                                                                                 | Integration                   |
| F32 | The API delivery consumer reaches its global retained-board cap while every retained state is quarantined or otherwise non-evictable. | It allocates no additional state, emits one unavailable transition and `BOARD_STATE_CAPACITY_EXCEEDED`, preserves the last committed Redis cursor and retained quarantine evidence, and performs no delivery, cursor advance, busy retry, or recovered transition.                                                                                                                                                                                                                                                    | Unit/failure                  |
| F33 | A legitimate worker envelope or complete encoded entry exceeds its producer contract by one byte.                                     | Strict validation and UTF-8 measurement finish before Redis I/O; `XADD` is never called, the outbox row is not marked published, a bounded constant non-retryable error is recorded, and the established board-head block policy remains in force.                                                                                                                                                                                                                                                                    | Unit/failure                  |
| F34 | The stream's first/last entry contains oversized malformed sentinel or arbitrary fields during startup metadata inspection.           | Direct read-only `XINFO STREAM` materializes the fields under the trusted-broker boundary; strict post-decoding shape and complete-entry byte checks return only a bounded constant diagnostic, with no delivery or cursor advancement. This does not claim pre-decoding protection from an unauthorized writer or malicious broker.                                                                                                                                                                                  | Real-Redis failure            |
| F35 | Redis credentials or the Redis server are compromised and an attacker appends an oversized live entry.                                | Treat as an infrastructure-security incident: revoke/rotate credentials, isolate the private endpoint, stop affected consumers, inspect writer/growth monitoring, and rebuild delivery availability from PostgreSQL. Application post-decoding limits are not claimed to prevent node-redis allocation.                                                                                                                                                                                                               | Deployment/runbook            |
| F36 | Two snapshot coordinators discover the same eligible board.                                                                           | Both may report discovery, but capture uses the writer's board-key `pg_try_advisory_xact_lock`; only a lock winner rereads eligibility/heads. A peer-created snapshot becomes no longer eligible, and the unique board/head constraint prevents duplicate durable snapshots without a persistent claim table or Redis coordination.                                                                                                                                                                                   | Database/integration          |
| F37 | Snapshot capture encounters a board held by a user writer.                                                                            | The nonblocking advisory-lock attempt skips immediately, is not counted as a failure, and applies a 5,000 ms ±20% cooldown. The user commit and unrelated candidates continue; the coordinator does not busy-spin or wait on the writer.                                                                                                                                                                                                                                                                              | Database/unit                 |
| F38 | More bootstrap or ordinary candidates exist than one polling bound.                                                                   | A single-flight 30,000 ms ±20% cycle advances a deterministic round-robin board-ID cursor, inspects at most 100 IDs through bounded indexed queries, returns at most 16 eligible IDs, and runs at most two captures concurrently. Genesis boards share this traversal; no full-board or all-unsnapshotted-board materialization occurs.                                                                                                                                                                               | Database/unit                 |
| F39 | Snapshot capture has a transient PostgreSQL or infrastructure failure while another board is eligible.                                | The failed board receives full-jitter exponential retry from 5,000 ms through a 300,000 ms cap; attempts remain bounded and in memory without an M2.6 durable blocked state. The unrelated board continues within the two-slot concurrency bound, and Redis/outbox publication state is irrelevant.                                                                                                                                                                                                                   | Unit/failure                  |
| F40 | Snapshot capture deterministically fails because payload, projection/schema, or canonicalization evidence is invalid.                 | One bounded sanitized notification is emitted and the board plus canvas/delivery-head fingerprint is suppressed. The 1,000-entry deterministic-LRU cache retains no payload/content/private data; an unchanged fingerprint is not retried, while either head changing permits one new attempt. No durable data is deleted or floor advanced.                                                                                                                                                                          | Unit/failure                  |
| F41 | The snapshot coordinator stops during candidate selection or capture and receives late completions.                                   | Repeated start/stop is harmless; no new scan/capture starts, in-flight work ends only within worker shutdown grace, late results after grace schedule no work or callbacks, and timers/listeners/retry/cursor/suppression state are released.                                                                                                                                                                                                                                                                         | Unit/failure                  |
| F42 | Recovery has no verified snapshot, or its otherwise-valid tail has 101–999 operations before the background trigger.                  | After authorization, one request takes a timeout-bounded nonblocking board lock, rereads the fixed boundary, creates at most one verified exact-head snapshot, and returns an empty tail. Heads and logical board data do not change; a peer snapshot is reused and no request loops creation.                                                                                                                                                                                                                        | Database/integration          |
| F43 | On-demand recovery refresh finds a busy board lock or a PostgreSQL timeout.                                                           | The request waits no longer than its bound, creates no snapshot, and returns sanitized retryable `INTERNAL_ERROR`; it does not misclassify infrastructure contention as durable `RECOVERY_BLOCKED`.                                                                                                                                                                                                                                                                                                                   | Database/API                  |
| F44 | Recovery evidence is corrupt, unsupported, gapped, conflicting, or mismatched when refresh is considered.                             | No replacement snapshot is created to hide the evidence. Normal valid-older fallback remains available only with a complete tail of at most 100; otherwise the fixed sanitized non-retryable `RECOVERY_BLOCKED` contract remains authoritative.                                                                                                                                                                                                                                                                       | Database/API/failure          |
| F45 | Compaction discovery duplicates across workers, one candidate blocks or throws, or shutdown races in-flight work.                     | Discovery creates no claim or lease. Each worker runs single-flight 300,000 ms ±20% scans over at most 100 boards, returns at most 16 candidates, and compacts at most two concurrently. Authoritative lock-time revalidation makes duplicate/no-progress outcomes harmless. An unchanged blocked fingerprint emits once; transient failures use bounded full-jitter retry while unrelated boards continue. Idempotent shutdown drains only through grace, fences late hooks/results, and clears all in-memory state. | Final acceptance/unit/failure |

## M2.7 final acceptance evidence

The final M2.7 suite uses one uniquely named PostgreSQL database migrated through 0009 and the real
candidate, compaction, receipt, recovery-material, operation, API, and BoardTransport boundaries.
It validates F17, F21, F22, and F45 together with the focused repository and coordinator suites:
publication evidence is created through the outbox lease state machine, Redis remains unavailable,
the older safety-delayed verified generation is compacted, both floors advance atomically, exact
replay survives deletion, and snapshot-plus-tail recovery reproduces authoritative order, rotations,
heads, and canonical hash. No receipt, snapshot, or Redis entry is deleted.

## M2.4B Slice 2 evidence

The real-Redis multi-instance failure suite now covers the operation-only portions of F01, F03/F10,
F06, F18, F29, and F30 with one isolated PostgreSQL database, the real outbox worker, and two
independent API consumers. Both APIs observe the same worker `XADD` through their own plain-`XREAD`
connections and cursors, then emit through their own local Socket.IO rooms. `XINFO GROUPS` remains
empty and no Socket.IO adapter participates.

The deterministic controls pause publication, gate one board's worker callback, stop/restart one API
runtime, inject a duplicate strict `XADD`, and fail one API-local Socket.IO handler. Explicit deadlines
include bounded cursor, lifecycle, stream-ID, and board diagnostics. The evidence confirms that the
database acknowledgement precedes optional Redis publication, duplicate transport entries advance
each consumer cursor but apply one stable operation, other boards/APIs continue independently, and a
fresh offline instance recovers missed durable state from the PostgreSQL join-watermark/range path.
At M2.4B completion, membership-revocation, readiness, and watchdog portions remained intentionally
untested pending M2.5.

M2.5 Slice 1 covers the operation-ordered revocation portion of F12 and F13 in the real two-API
topology. One committed removal is independently consumed by both replicas and evicts only matching
local principal/board sockets. A deterministic worker barrier proves an unrelated board progresses
while revocation publication is unresolved, and a later same-board operation remains behind the
revocation head. Duplicate strict `XADD` evidence is harmless; a stopped API-A consumer cannot prevent
API B enforcement; rollback, no-op removal, and exact removal replay produce no additional revocation
event.

The final M2.5 acceptance topology activates both APIs with `API_DELIVERY_MODE=distributed` through
the production server-runtime composition. It covers the activated boundaries of F09, F10, F12,
F18, F27, F28, and F30: neither dynamic API port accepts a socket before consumer establishment;
PostgreSQL acknowledgement precedes the worker's optional `XADD`; two independent plain-`XREAD`
consumers and API-local rooms converge; and a deterministic 5,000 ms watchdog clock keeps sockets
ready inside the grace period before failing both replica-local readiness gates closed at the
deadline. After publication, consumers advance through the retained entry and new sockets recover
the durable gap from PostgreSQL join-watermark/range reads.

The Redis interruption case destroys only API A's real blocking-read connection and gates its next
real reconnect. API A becomes unavailable and closes its sockets while API B and the worker continue;
continuity-valid reconnect catches up the missing stream entry once and reopens only API A. This is
not a broker restart or Redis-durability claim. A committed membership removal is then independently
enforced by both activated APIs, unrelated principal/board sockets remain authorized, and a duplicate
strict revocation `XADD` advances both transport cursors without a second harmful transition. The
suite retains the at-least-once model and compares recovered client hashes with PostgreSQL authority.

## M2.6 final acceptance evidence

The six-test isolated M2.6 acceptance suite marks the snapshot portions of F15, F16, F36, F41, F42,
F43, and F44 accepted. It migrates one temporary PostgreSQL database through 0008, uses the real
snapshot/candidate/recovery repositories and coordinator, supervises automatic bootstrap through the
real worker application while an injected Redis stream remains unavailable, serves a real API on a
dynamic port, and drives authenticated Socket.IO/HTTP recovery through the real BoardTransport and
isolated pending-command persistence.

Deterministic scheduler and advisory-lock barriers prove below/at-threshold selection, immutable
genesis and exact-head capture, a complete before-or-after writer boundary, and at most one durable
snapshot during duplicate capture. Snapshot-plus-tail recovery publishes only the fully reconstructed
client state and matches stored snapshot, reconstructed, client, and PostgreSQL-authoritative hashes.
The 101-operation boundary creates one on-demand exact-head snapshot without changing either head or
logical board state, and an unchanged repeat creates none.

Controlled corruption proves verified older-chain fallback, durable invalidation, non-reconsideration,
and terminal `RECOVERY_BLOCKED` behavior without legacy fallback or pending-command loss. A
deterministically busy refresh returns retryable `INTERNAL_ERROR`; after lock release one bounded
retry recovers and drains the preserved command. A post-verification/pre-apply barrier proves stale
session results cannot mutate a replacement session. This evidence does not cover operation deletion,
recovery floors, compaction, Redis durability, deployment, or performance.

## M2.8 API observability acceptance evidence

The isolated M2.8 API suite migrates a uniquely named PostgreSQL database through 0009, uses unique
`converge:test:m28-api:*` Redis streams, the real outbox publisher, production-composed distributed
APIs on dynamic ports, authenticated Socket.IO/HTTP clients, protected production metrics, and the
existing deterministic consumer and watchdog barriers. It accepts the API-observability portions of
F09, F10, F18, F27, F28, F30, and F42-F44 without making a worker-observability claim.

Before the first blocking read is issued, `/health/live` and PostgreSQL-backed `/health/ready` return
200 while `/health/socket-ready` returns 503 and both readiness gauges are zero. Consumer
establishment changes the consumer and composite socket gauges to one. A real committed operation is
pending before worker `XADD`, is handled once by each API, and increments only the fixed `operation` /
`handled` series. A second valid Redis entry carrying the same envelope advances the transport cursor,
increments `duplicate` once, and neither reapplies nor re-emits the operation.

At 4,999 ms of deterministic publication divergence both readiness endpoints remain healthy. At the
5,000 ms watchdog boundary, HTTP readiness stays 200 while socket readiness returns 503, its gauge is
zero, connected sockets are fenced, and the watchdog/socket transition series increment once.
Publication plus parity restores socket readiness for the current generation without double-counting.
Destroying only one real blocking-read connection likewise leaves HTTP readiness at 200 while its
consumer/socket gauges fall to zero; continuity-valid reconnect handles the intervening event once and
restores those gauges.

Real HTTP recovery requests record exactly one each of `snapshot_tail`, `refreshed`,
`recovery_blocked`, `retryable_failure`, and `authorization_failure`, with five total duration
observations and unchanged response classifications. Missing and incorrect metrics credentials are
indistinguishable, authorized unchanged scrapes are byte-identical with the Prometheus content type,
and a controlled snapshot/render boundary failure returns only the fixed 503 response without changing
recorder state. Catalog validation and direct bounded-event inspection exclude board, operation,
event, socket, Redis-entry, principal, snapshot/hash, token, URL, SQL, payload, and raw-error evidence.
Shutdown records one stopping/stopped lifecycle, zeros both gauges before cleanup, fences late recovery,
and leaves no API listener, Redis reader, watchdog task, client, worker stream, or database pool open.

## M2.8 worker observability acceptance evidence

The worker acceptance reuses the real outbox failure topology with one temporary PostgreSQL database
migrated through 0009, a unique `converge:test:m28-worker:*` Redis stream, the production worker
application and operational listener, real outbox/snapshot/compaction repositories, and deterministic
component schedulers and repository barriers. Compaction is explicitly enabled. This accepts the
worker-observability evidence for F02-F08 and F36-F45 without adding deployment behavior.

While snapshot startup is held, liveness is 200 and both core and delivery readiness are 503. After
PostgreSQL, snapshot, and enabled compaction startup, core readiness becomes 200 while delivery remains
503 during controlled Redis unavailability. Current-generation Redis establishment changes only
delivery readiness to 200. All three active-work gauges begin at zero and worker lifecycle evidence is
exactly starting, ready, stopping, stopped.

A real claimed outbox event holds the active-work gauge at one through the pre-XADD barrier, appends to
the real Redis stream, and persists that exact entry ID through fenced `markPublished`. It records one
published result and one duration before returning the gauge to zero. A controlled ambiguous Redis
failure records retry only after the real repository stores `retry_wait`, records one duration, returns
active work to zero, and makes delivery readiness unavailable without changing core readiness. The
real failure cases separately verify that deterministic preparation failure records blocked only after
the durable blocked transition and a replaced lease records stale without a forbidden transition;
focused shutdown fencing confirms abandoned work records no false final outcome.

With Redis unavailable, a real genesis snapshot capture and a real safety-delayed compaction each hold
only their authoritative-call gauge at one, record one successful counter and duration, and return to
zero. Snapshot capture leaves board heads unchanged. Compaction deletes covered operation/outbox history
and advances both coupled floors from zero to one while core readiness stays healthy. Focused bounded
coordinator evidence covers busy, no-progress, no-boundary, deterministic/blocked, and transient outcome
classification plus unchanged cooldown/retry/concurrency behavior.

Metrics-disabled control returns fixed 404. Missing and incorrect credentials are indistinguishable;
authorized unchanged scrapes are byte-identical and use the Prometheus content type. Scrapes export only
fixed-catalog metric series, never structured events. Direct metric/event inspection excludes database
and Redis URLs, bearer tokens, board/event/operation/snapshot/Redis-entry identifiers, SQL, payloads,
raw errors, and stacks. A controlled snapshot/render failure returns the fixed 503 without mutating the
owned recorder. Repeated shutdown marks all readiness false before draining, stops each component once,
fences late readiness updates, closes the listener/Redis/database owners, clears deterministic timers,
and removes the retained test stream.

## M3.5B presence reconnect correction evidence

Presence Redis is a best-effort plane and is deliberately excluded from the M2 delivery readiness
matrix. A command, publisher, or subscriber loss fences its current three-client connection generation,
emits only bounded presence-unavailable evidence, and retries with one full-jitter timer. A later
fully connected and subscribed generation emits available once; stale listeners and Pub/Sub callbacks
cannot act. Focused deterministic tests cover failed initial connection, repeated loss signals,
partial-cycle cleanup, and runtime re-admission/fresh snapshot behavior. A real Redis test destroys
an owned command connection, observes unavailable then fresh availability, and resumes a bounded
snapshot. The accepted two-API topology additionally proves cross-replica room routing, late snapshots,
multi-tab session evidence, explicit leave, and a Redis-side API-A-only interruption followed by fresh
generation recovery while a PostgreSQL command still acknowledges. HTTP/socket editing readiness,
durable commands, outbox, delivery streams, and PostgreSQL are not changed by this recovery path.

## Acceptance rules

- Tests assert at-least-once publication and idempotent effects; they must not assert or describe
  exactly-once delivery.
- No test may depend on event-loop timing, arbitrary sleeps, process scheduling, or eventual Redis
  trimming. Barriers, controllable clocks, explicit stream commands, and bounded polling are required.
- Every failure test records `eventId`, board delivery sequence, canvas sequence when present, lease
  token/owner, Redis entry ID when known, and API instance/boot ID.
- Each test verifies that batches, queues, error fields, retries, and deadlines stay within configured
  bounds.
- Authorization tests inspect both event receipt and absence of post-revocation board data.
- Final convergence compares against a fresh authorized PostgreSQL recovery response, not against one
  API replica or another client.
