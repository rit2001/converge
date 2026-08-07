# Architecture overview

Converge is a TypeScript monorepo with one web client, one API process, shared deterministic domain
packages, and PostgreSQL as durable authority. The system intentionally begins as a modular monolith.

## Durable write path

1. The client applies a command optimistically and emits it with an `opId` and `baseSeq`.
2. The API authenticates, authorizes board membership, validates size/schema, and rate-limits.
3. A PostgreSQL transaction takes a transaction-scoped advisory lock derived from `boardId`.
4. It returns the prior result for an existing `(boardId, opId)`, otherwise assigns `last_seq + 1`.
5. Conflict rules are checked against per-field/object sequence metadata in the projection.
6. The operation, updated projection, board sequence, and outbox event are committed atomically.
7. Socket.IO acknowledges the origin and broadcasts the committed operation at least once.

The outbox is present at Milestone 1, but its delivery worker is deferred. In-process broadcasting is
adequate for this single-instance milestone and is not claimed to close crash-after-commit delivery.

## Client recovery

Initial synchronization and reconnect use the same application-level protocol. After authentication
and authorization, the server joins the socket to the board room and then captures a fixed
`joinWatermark`. The client fetches the operation-log tail after its `lastAppliedSeq` through that
watermark in server-bounded batches of at most 100 operations. Live room events received while joining
or catching up are buffered, deduplicated by operation identity and sequence, and drained only in
strict sequence order. `READY` means every operation through the join watermark has been applied and
no known gap remains; a raw Socket.IO connection is not readiness.

Optimistic pending commands remain intact across synchronization and are resubmitted with their
original `opId` only after `READY`, preserving repository idempotency. Canonical serialization still
provides a current-state diagnostic hash, but the database does not yet persist a trustworthy hash for
each historical sequence, so the client cannot verify a server-issued hash specifically at the join
watermark.

This closes initial-load and temporary-disconnection gaps for the single API instance. Milestone 2
still owns crash-window outbox delivery, Redis multi-instance fan-out, snapshots, compaction, and
server-crash recovery.

## Boundaries

- `packages/protocol`: versioned transport schemas and structured errors
- `packages/canvas-engine`: pure reducer, conflict rules, canonical serialization, hashing
- `packages/database`: migrations and transactional repositories
- `packages/observability`: logging/telemetry interfaces
- `packages/testkit`: deterministic fixtures and multi-client helpers
- `apps/api`: HTTP/socket adapters, auth, authorization, validation, rate limits
- `apps/web`: canvas UI, IndexedDB pending queue, transport and reconciliation
