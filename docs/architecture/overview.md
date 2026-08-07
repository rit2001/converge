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

The client applies committed operations only in board-sequence order, ignores duplicates, buffers
future operations, and fetches a bounded missing range after detecting a gap. On reconnect it sends
`lastAppliedSeq` and pending operation IDs. Optimistic state is rebuilt by reducing committed state
then pending commands. Canonical serialization sorts objects and recursively sorts object keys before
SHA-256 hashing.

## Boundaries

- `packages/protocol`: versioned transport schemas and structured errors
- `packages/canvas-engine`: pure reducer, conflict rules, canonical serialization, hashing
- `packages/database`: migrations and transactional repositories
- `packages/observability`: logging/telemetry interfaces
- `packages/testkit`: deterministic fixtures and multi-client helpers
- `apps/api`: HTTP/socket adapters, auth, authorization, validation, rate limits
- `apps/web`: canvas UI, IndexedDB pending queue, transport and reconciliation
