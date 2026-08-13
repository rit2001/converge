# ADR 004: Board delivery sequence

Status: accepted for Milestone 2

## Context

Milestone 1 has a board-local canvas sequence in `boards.last_seq`. It orders
`board_operations`, but membership revocation deliberately does not increment it. The current
`outbox_events.board_seq` is therefore nullable for revocations and cannot express the required
order between a revocation and a later canvas operation. The in-process
`BoardDeliveryCoordinator` supplies that order only inside one API process and cannot survive a
crash or coordinate replicas.

Delivery order and canvas projection order are related but different domains. Future restore
events also need to take a place in delivery order without rewinding either counter.

## Decision

Add a distinct, monotonically increasing `delivery_seq` scoped to each board. Its counter is owned
by `boards.last_delivery_seq`, initially `0`. Every new durable event that must be observed in order
increments this counter once while holding the existing board advisory lock and updates the board,
domain state, and outbox in one PostgreSQL transaction.

The initial event classes are:

- `operation.committed`, carrying one immutable canvas operation and its canvas `seq`;
- `board.membership.revoked`, carrying the revoked principal but no canvas `seq`; and
- the reserved future `version.restored`, which will be a new forward-moving mutation and must not
  rewind either sequence.

`outbox_events` stores `delivery_seq NOT NULL` and retains a nullable `canvas_seq`. The existing
`board_seq` column is renamed to `canvas_seq` so its meaning is not silently changed.
`(board_id, delivery_seq)` and `id` are unique. Operation rows also store the matching `event_id`
and `delivery_seq`, with uniqueness on `(board_id, delivery_seq)` and `event_id`. This makes the
mapping durable even after the corresponding outbox row is eventually compacted.

For a new canvas command, the transaction allocates both:

```text
canvas_seq   = boards.last_seq + 1
delivery_seq = boards.last_delivery_seq + 1
```

For membership revocation, only `delivery_seq` advances. The deletion and outbox event are in the
same allocation transaction. Removing an already-absent non-owner remains an idempotent no-op and
allocates no event.

Allocation remains under `pg_advisory_xact_lock(hashtextextended(board_id, 0))`. The transaction
also locks the `boards` row, reauthorizes the actor, performs the domain mutation, inserts exactly
one outbox row, and advances the applicable counters. The API must not hold this transaction while
performing Redis or Socket.IO I/O.

Exact replay is not a new durable event. After authorization, a matching `(board_id, op_id)` returns
the original operation, `event_id`, canvas sequence, delivery sequence, and commit time. It does not
increment either board counter and does not insert or republish an outbox row. Reuse of an operation
ID by another actor or for a different normalized command remains `IDEMPOTENCY_CONFLICT`.

Compaction must preserve this guarantee. Before operation-log rows can be removed,
`board_operation_receipts` retain `(board_id, op_id)`, actor, the complete normalized command JSONB,
its canonical hash/version, original canvas sequence, event ID, delivery sequence, and commit time for
the life of the board. Replay still uses PostgreSQL JSONB equality after strict validation; the hash is
an integrity/diagnostic accelerator, not a substitute for exact comparison. A command or actor
mismatch is an idempotency conflict, never a new event.

## Migration and backfill

The schema change is additive before it becomes constraining:

1. Add nullable delivery columns, outbox lifecycle columns, receipt storage, and board counters.
2. Stop old API writers for the cutover. Milestone 1 never had a dispatcher, so there is no valid
   distributed publication cursor to preserve.
3. For each board, deterministically order legacy operation outbox rows by canvas sequence and then
   legacy non-operation rows by `(created_at, id)`. Assign contiguous delivery sequences and copy
   operation mappings/receipts. This establishes a migration order; it does not claim to reconstruct
   the transient M1 in-process interleaving, which PostgreSQL did not record.
4. Mark all legacy outbox rows `published` with an explicit `legacy_backfill` result. They must not
   be emitted to newly started M2 API replicas. Clients recover legacy canvas state from PostgreSQL,
   and every socket reauthenticates after the cutover, so historical revocations are enforced from
   `board_members`.
5. Set `boards.last_delivery_seq` to the assigned maximum, validate uniqueness and non-null
   constraints, then enable M2 writes.

The cutover requires a brief write drain and restart of every API process. A zero-downtime mixed M1
and M2 writer deployment is explicitly unsupported because old writers cannot allocate the new
sequence.

## Invariants

- PostgreSQL is the only allocator and durable authority.
- A committed board event has exactly one stable event ID and one delivery sequence.
- Delivery sequences are unique and strictly increasing per board, but gaps may exist in retained
  storage after compaction.
- Canvas sequences remain contiguous for retained operation tails and are unaffected by access
  events.
- Workers may deliver an event more than once but may not publish a later event for that board while
  an earlier event is unpublished or operator-blocked.
- A future restore creates a new event at both current heads; it never reuses a historical delivery
  sequence or rewinds `last_seq`.

## Alternatives rejected

### Reuse canvas `last_seq`

Rejected because membership events are not canvas mutations. Advancing it would create holes in the
operation log and break the existing fixed-watermark catch-up contract.

### Order by outbox creation timestamp

Rejected because timestamps are not a uniqueness or causality mechanism. PostgreSQL `now()` is tied
to transaction start, clocks may move, and concurrent transactions can share timestamps.

### Use Redis Stream IDs as board order

Rejected because Redis is not authoritative and stream IDs are allocated after commit. A crash in
that gap would leave no durable board order, and retries receive different stream IDs.

### Maintain separate operation and control-event streams

Rejected because an API could then observe a later operation before a revocation. Reconstructing the
cross-stream barrier would reintroduce the missing total order.

## Consequences

The additional counter makes authorization and canvas events comparable without weakening M1 canvas
semantics. It serializes all delivery-bearing mutations at the existing per-board lock, so one hot
board remains a single-writer hot spot. Unrelated boards still allocate and publish concurrently.
Consumers require two cursors: delivery sequence for infrastructure/control ordering and canvas
sequence for client projection reconciliation.

Board-lifetime receipts mean M2 bounds the active recovery log but does not claim bounded total
idempotency/audit storage. A future deletion horizon would require an explicit client retry contract
and a separate ADR; silently aging receipts would regress M1 exact replay.
