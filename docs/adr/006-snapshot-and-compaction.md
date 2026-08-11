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

The worker schedules a board when any configurable initial trigger is met: 1,000 operations since
the latest verified snapshot, 24 hours since that snapshot while the board changed, or 8 MiB of
estimated retained operation payload. These are safe starting policies to validate, not performance
or capacity claims. At most one snapshot per board is created at a time.

Snapshot creation opens a short PostgreSQL transaction, attempts the existing board advisory lock,
and skips a busy board rather than waiting indefinitely. While holding the lock it reads the board
heads and the complete `board_objects` projection in deterministic stack/object order, canonicalizes
the projection, computes its hash and byte count, inserts the snapshot, rereads and rehashes the
stored value, and marks it verified. Writers cannot interleave between head capture and projection
capture. Redis I/O is not involved.

The worker bounds one snapshot payload to a configurable initial 16 MiB. A board exceeding the bound
is operator-blocked for compaction and remains recoverable from its existing log; it is not truncated
or partially snapshotted.

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
