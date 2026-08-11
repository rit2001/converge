# ADR 006: Snapshot and compaction

Status: accepted for Milestone 2; compaction remains disabled by default

## Context

Milestone 1 reconstructs missed state from an unbounded, contiguous `board_operations` log or reloads
the current `board_objects` projection. It has no durable historical snapshot, compaction floor, or
small idempotency record independent of the operation payload. Deleting operation rows today would
break both catch-up contiguity and exact replay.

## Decision

Persist versioned, immutable full-projection snapshots in PostgreSQL and expose a
snapshot-plus-log-tail recovery contract. Compaction advances explicit canvas and delivery recovery
floors only after a snapshot has been created and verified.

### Snapshot schema

`board_snapshots` contains:

| Column                         | Meaning                                         |
| ------------------------------ | ----------------------------------------------- |
| `id uuid`                      | Stable snapshot identity and primary key.       |
| `board_id uuid`                | Owning board, with cascade deletion.            |
| `snapshot_seq bigint`          | Canvas sequence represented by the projection.  |
| `snapshot_delivery_seq bigint` | Board delivery head captured in the same lock.  |
| `schema_version integer`       | Version of the canonical snapshot payload.      |
| `projection jsonb`             | Full reducer state, not only visible objects.   |
| `canonical_hash text`          | Lowercase SHA-256 of versioned canonical bytes. |
| `object_count integer`         | Diagnostic count including tombstones.          |
| `byte_size integer`            | Canonical serialized byte count.                |
| `status text`                  | `creating`, `verified`, or `invalid`.           |
| `created_at`, `verified_at`    | Audit timestamps.                               |

`(board_id, snapshot_seq)` is unique. An index on
`(board_id, status, snapshot_seq DESC)` supports latest verified selection. The projection includes
board identity/name, `lastSeq`, deterministic stack order, every live object and tombstone, field
sequences, created/updated/deleted sequences, and its schema version. It excludes presence and other
ephemeral state.

Canonical bytes are domain-separated with `converge.snapshot.v1`. The snapshot hash is distinct from
the existing M1 visible-board diagnostic hash; a client validates the full recovery payload before it
derives visible canvas state.

`boards.operation_recovery_floor` and `boards.delivery_recovery_floor` start at `0`. A positive
operation floor means incremental recovery from a client sequence below that value is unavailable;
the client must accept a snapshot at or above the floor. A delivery cursor below the delivery floor
cannot be repaired event by event and forces fail-closed reauthentication/recovery.

### Creation and verification

`apps/worker` owns automatic snapshot creation. Snapshot scheduling and capture are independent from
outbox publication: Redis health and publication success are not prerequisites for creating a
PostgreSQL snapshot. User operation and membership-revocation transactions never create snapshots
synchronously.

A board is eligible when any one of these conditions is true:

1. It has no verified snapshot. This is immediate bootstrap eligibility, including for an empty or
   genesis board.
2. At least 1,000 canvas operations exist after the newest valid verified snapshot.
3. Estimated retained operation payload after that snapshot is at least 8 MiB.
4. Either the canvas or delivery head advanced after the snapshot and the verified snapshot is at
   least 24 hours old.

Delivery-head-only advancement qualifies only for the 24-hour trigger and does not count as a canvas
operation. A newest invalid snapshot causes immediate replacement eligibility, while candidate
comparison continues to use the newest valid verified snapshot as the recovery baseline. A board
whose canvas and delivery heads have not advanced is not resnapshotted solely because time passed.
The snapshot payload remains limited to 16 MiB.

The worker polls every 30,000 ms with ±20% jitter. A polling cycle inspects at most 100 board IDs and
returns at most 16 eligible candidates. It runs no more than two captures concurrently. Poll cycles
are single-flight and never overlap. Candidate traversal uses a deterministic round-robin board-ID
cursor, including for bootstrap candidates, so no worker loads or scans every board into application
memory at startup or during a later cycle. Bounded indexed PostgreSQL queries evaluate verified
snapshot heads and post-snapshot operation count and estimated bytes; candidate discovery must not
issue one query per board or one unbounded query over the complete board table.

M2.6 adds no persistent snapshot claim or lease table. Multiple workers may discover the same
candidate. Capture opens a short PostgreSQL transaction and uses `pg_try_advisory_xact_lock` with the
same board lock key as operations. A busy board is skipped immediately so user commits retain
priority. After acquiring the lock, capture rereads eligibility and both authoritative heads. If
another worker already created the required snapshot, the result is harmlessly no longer eligible.
The unique board/head snapshot constraint remains the final duplicate fence: duplicate discovery is
allowed, but duplicate durable snapshots are not.

While holding the lock, capture reads the board heads and complete `board_objects` projection in
deterministic stack/object order, canonicalizes the projection, computes its hash and byte count,
inserts the snapshot, rereads and rehashes the stored value, and marks it verified. Writers cannot
interleave between head capture and projection capture. Redis I/O is not involved.

The worker bounds one snapshot payload to a configurable initial 16 MiB. A board exceeding the bound
is operator-blocked for compaction and remains recoverable from its existing log; it is not truncated
or partially snapshotted.

### Coordinator retry, suppression, and lifecycle

A failed `pg_try_advisory_xact_lock` is not a snapshot failure. That worker applies a 5,000 ms ±20%
busy-board cooldown before retrying the board, while unrelated candidates continue.

Transient failures use full-jitter exponential backoff with a 5,000 ms base and 300,000 ms cap.
M2.6 does not exhaust retries into a durable blocked state. Retry work remains bounded by the cap,
polling limits, and capture concurrency; an ordinary process restart may reset transient in-memory
attempt counters. M2.8 adds operator alerting and durable operational visibility, so M2.6 makes no
such claim.

Deterministic non-retryable capture failures include a projection exceeding 16 MiB, strict
projection/schema failure, and deterministic canonicalization failure. The coordinator emits one
bounded sanitized failure notification and suppresses repeated attempts for the same board plus
canvas/delivery-head fingerprint. A changed authoritative head permits one new attempt. This
in-memory suppression cache holds at most 1,000 fingerprints and uses deterministic LRU eviction. It
never stores board objects, snapshot payloads, SQL, credentials, or principal data.

`start()` and `stop()` are idempotent. No candidate scans overlap. Stop prevents new scans and
captures. In-flight captures may finish only within the existing worker shutdown grace; after grace
expiry, late results cannot schedule work or invoke lifecycle callbacks. Shutdown releases timers,
listeners, retry state, round-robin cursor state, and suppression state.

All coordinator configuration is strictly validated as positive safe integers and, for timing
values, against timer-safe upper bounds:

| Configuration                        |    Default |
| ------------------------------------ | ---------: |
| `SNAPSHOT_POLL_INTERVAL_MS`          |    `30000` |
| `SNAPSHOT_POLL_JITTER_PERCENT`       |       `20` |
| `SNAPSHOT_CANDIDATE_SCAN_LIMIT`      |      `100` |
| `SNAPSHOT_CANDIDATE_BATCH_SIZE`      |       `16` |
| `SNAPSHOT_MAX_CONCURRENCY`           |        `2` |
| `SNAPSHOT_OPERATION_THRESHOLD`       |     `1000` |
| `SNAPSHOT_CHANGED_AGE_MS`            | `86400000` |
| `SNAPSHOT_OPERATION_BYTES_THRESHOLD` |  `8388608` |
| `SNAPSHOT_MAX_PAYLOAD_BYTES`         | `16777216` |
| `SNAPSHOT_RETRY_BASE_MS`             |     `5000` |
| `SNAPSHOT_RETRY_CAP_MS`              |   `300000` |
| `SNAPSHOT_BUSY_RETRY_MS`             |     `5000` |
| `SNAPSHOT_FAILURE_FINGERPRINT_LIMIT` |     `1000` |

The implementation slice must add these variables to `.env.example` and Turbo environment
pass-through. This documentation-only decision does not edit those configuration files.

The coordinator does not delete operations or outbox rows, advance recovery floors, compact data,
add a persistent snapshot lease table, coordinate through Redis, change HTTP/client recovery, deploy
infrastructure, or claim completed observability. Those remain later, separately gated work.

### Recovery contract

An authorized recovery request captures a fixed canvas watermark. The server selects the newest
verified snapshot whose `snapshot_seq` is at most that watermark, verifies its hash, and returns it
with contiguous operations from `snapshot_seq + 1` through the watermark in batches no larger than 100. If the client's sequence is at or above the operation recovery floor, ordinary tail-only catch-up
remains valid. If it is below the floor, the response explicitly requires snapshot replacement before
tail application.

Clients strictly validate board identity, schema, snapshot sequence, delivery metadata, canonical
hash, and each contiguous tail operation. They replace only authoritative committed state, preserve
durable pending commands, and then reapply pending optimistic overlays under the existing session and
attempt fencing. A client never splices a tail onto a different snapshot sequence.

The backward-compatible authenticated recovery endpoint is
`GET /v1/boards/:boardId/recovery`. Its strict response identifies the verified snapshot and its
canvas/delivery heads, the fixed current heads, the full snapshot projection and canonical hash, the
contiguous operation tail, and the reconstructed current canonical hash. Existing board snapshot and
operation-range endpoints retain their contracts.

The recovery response/replay tail is capped at 100 operations, while background snapshot creation
normally begins at 1,000 operations, 8 MiB, or 24 changed hours. Background creation is therefore an
efficiency mechanism, not a recovery-availability guarantee. After authorization, one recovery
request may perform at most one on-demand refresh when ordinary selection fails only because no
verified snapshot exists or an otherwise-valid tail exceeds 100 operations. Refresh uses a bounded
`pg_try_advisory_xact_lock` transaction, rereads recovery evidence under that fixed boundary, and
either uses a snapshot created by a peer or captures and verifies one exact-head snapshot with an
empty tail. It changes no canvas/delivery head, operation, projection, outbox row, membership,
receipt, or authorization state.

Lock contention and PostgreSQL timeout are retryable infrastructure failures. Snapshot corruption,
unsupported versions, gaps, conflicts, reducer/projection/hash mismatch, and deterministic inability
to create a bounded trustworthy snapshot prohibit refresh and remain non-retryable
`RECOVERY_BLOCKED`. A corrupt newest snapshot is never hidden by creating a replacement; normal ADR
fallback to an earlier verified snapshot remains valid only when its complete tail fits the
100-operation bound.

When current durable evidence cannot form any complete trustworthy snapshot-plus-tail chain, the
endpoint returns HTTP `409` with the strict `RECOVERY_BLOCKED` protocol error, the sanitized message
`Authoritative board recovery is unavailable`, and `retryable: false`. Snapshot invalidation remains
owned by the database repository; the HTTP response neither repairs nor invalidates data. PostgreSQL
connection loss, query timeout, pool shutdown, and unexpected programming failures are not durable
recovery evidence and retain the existing sanitized retryable `INTERNAL_ERROR` response.

If the newest snapshot fails validation, it is marked invalid and the server tries an earlier verified
snapshot only when the complete tail from that snapshot still exists. Otherwise the board is
operator-blocked for recovery. The system never serves a hash-mismatched snapshot or invents missing
operations. PostgreSQL backup/restore remains the remedy for corruption beyond retained recovery
material.

### Compaction

Compaction takes the same advisory lock as writers and snapshots. Under the lock it revalidates the
candidate snapshot hash and proves:

- the snapshot belongs to the board and matches its recorded heads;
- every canvas operation through the proposed floor has a definitive idempotency receipt;
- every outbox row through the proposed delivery floor is `published`;
- no pending, leased, retry-wait, or blocked event would be removed; and
- an unbroken operation tail exists above the proposed floor.

It then atomically advances both board recovery floors and deletes only:

- `board_operations.seq <= operation_recovery_floor`; and
- published `outbox_events.delivery_seq <= delivery_recovery_floor`.

Operation receipts, including normalized command JSONB for definitive equality, remain for the board
lifetime so exact replay survives compaction. At least the floor snapshot, the newest verified
snapshot, and any snapshot pinned by future version history are retained. Unpinned older snapshots
may be deleted only after the floor transaction commits. Active writers either run before or after
compaction because they share the lock; no partial projection can be captured or deleted.

Compaction is disabled by default. Its initial safety margin is one verified snapshot generation: the
newest verified snapshot cannot immediately become the deletion floor, and at least two verified
snapshots plus the floor and every pinned snapshot are retained. Changing this margin cannot bypass
hash, publication, receipt, backup, corruption, or failure-test gates.

Snapshots are recovery checkpoints, not automatically user-visible versions. A future version record
may pin one. Restoring a version materializes its state as a new forward operation and new delivery
event at the current heads; it never rewinds a floor or sequence and never mutates an old snapshot.

## Alternatives rejected

### Keep the log forever

Operationally simple, but it does not meet the bounded recovery goal and makes catch-up cost grow with
board age.

### Snapshot only visible objects

Rejected because tombstones, field sequence metadata, and stack order are required to preserve M1
conflict and projection behavior after recovery.

### Delete operations and rely on current `board_objects`

Rejected because that projection has no immutable sequence/hash contract and would destroy exact
replay evidence.

### Snapshot without the board advisory lock

Rejected because separate reads of board head and projection could describe different sequences.

### Rewind sequences during restore

Rejected because clients, receipts, outbox ordering, and audit history all rely on monotonic identity.

## Consequences

PostgreSQL remains authoritative, but authority is represented by a verified snapshot plus the
retained operation tail after compaction rather than by every historical operation payload. Recovery
floor handling becomes part of the public synchronization protocol. Compaction intentionally stops
on ambiguity and may require operator intervention; availability is never purchased by silently
discarding unverified history.

The active recovery log becomes bounded by policy, but board-lifetime idempotency receipts still grow
with accepted operation IDs. M2 makes no total durable-storage bound. Deleting receipts later requires
a product-level retry horizon and a separate decision.
