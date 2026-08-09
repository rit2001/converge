# Architecture overview

Converge is a TypeScript monorepo with one web client, one API process, shared deterministic domain
packages, and PostgreSQL as durable authority. The system intentionally begins as a modular monolith.

## Durable write path

1. The client strictly validates the complete command, writes it to board-scoped IndexedDB storage,
   and only after that write succeeds applies it optimistically and makes it network-submittable.
2. On the already authenticated socket, Socket.IO requires an acknowledgement callback before any
   durable mutation is attempted, then validates size/schema and rate-limits.
3. A PostgreSQL transaction takes a transaction-scoped advisory lock derived from `boardId`, loads
   the authoritative board head, and reauthorizes current edit membership.
4. An existing `(boardId, opId)` is an exact replay only when PostgreSQL JSONB equality confirms the
   complete normalized command and stored actor are unchanged. Reuse for different intent fails with
   `IDEMPOTENCY_CONFLICT`.
5. A genuinely new command with `baseSeq` ahead of the locked board head fails with
   `RESYNC_REQUIRED`; stale bases remain subject to the existing server-authoritative conflict rules.
6. The server assigns `last_seq + 1` and checks projection conflict rules.
7. The operation, updated projection, board sequence, and outbox event are committed atomically.
8. After commit, Socket.IO attempts live publication before acknowledgement delivery. Exact retries
   may republish the stored operation, which clients deduplicate.

The outbox is present at Milestone 1, but its delivery worker is deferred. In-process broadcasting is
adequate for this single-instance milestone and is not claimed to provide exactly-once delivery or
close the process-crash-after-database-commit window.

## Client recovery

Initial synchronization and reconnect use the same application-level protocol. After authentication
and authorization, the server joins the socket to the board room and then captures a fixed
`joinWatermark`. The client fetches the operation-log tail after its `lastAppliedSeq` through that
watermark in server-bounded batches of at most 100 operations. Live room events received while joining
or catching up are buffered, deduplicated by operation identity and sequence, and drained only in
strict sequence order. `READY` means every operation through the join watermark has been applied and
no known gap remains; a raw Socket.IO connection is not readiness.

Each synchronization attempt has an identity scoped to the active board session and socket
connection. Join acknowledgements and operation-range fetches time out after 10 seconds. Retryable
failures enter `RETRY_WAIT` and retry automatically from the current committed sequence with
exponential backoff starting at 500 ms, capped at 10 seconds, and bounded production jitter. Only one
attempt and one retry timer may exist; stale acknowledgements, disconnects, and superseded sessions
cannot resume an obsolete attempt. Authorization and protocol-integrity failures are terminal. A
client sequence ahead of the authoritative head triggers a strictly validated HTTP snapshot reload,
preserving durable pending commands, before a new join.

Live events received during an active join/catch-up attempt are strictly validated and temporarily
buffered up to 1,000 unique operations or 2 MiB, whichever is reached first. Duplicates are charged
once. The next event that would cross either limit is discarded, the attempt buffer is cleared, and
bounded recovery starts from the durable operation log. Live events are intentionally discarded
during `RETRY_WAIT`; the next fixed-watermark catch-up retrieves them. Repeated overflow therefore
keeps memory bounded and remains outside `READY` rather than claiming convergence. Pending command
submission stays paused until a current attempt reaches `READY`, then its single ordered drain loop
resumes with the original command identities.

Pending persistence mutations are serialized per board. New records carry a monotonic enqueue
ordinal; legacy records remain loadable using a deterministic timestamp/operation-ID fallback.
Invalid stored rows are reported and skipped without clearing valid work. The transport drains one
command at a time only after `READY`, retaining the original normalized command, `opId`, and client
timestamp across acknowledgement timeouts and retryable responses. Retry delay starts at 500 ms,
doubles to a 10-second cap, and includes bounded production jitter; disconnect or session replacement
cancels timers, while a later `READY` resumes the same command. `RESYNC_REQUIRED` pauses submission
until the existing join/catch-up protocol reaches `READY` again.

The obsolete pending-operation-ID join field had no reconciliation behavior and has been removed, so
even large durable queues cannot invalidate board join. Successful acknowledgements and matching live
operations both prove commitment and schedule serialized IndexedDB deletion. A deletion failure does
not roll back committed state: it raises a cleanup warning and is retried, while refresh-time exact
replay remains safe if the row is still present. This is transactional idempotency with recoverable
at-least-once retry, not exactly-once network delivery. Multi-tab queue coordination remains deferred.

Canonical serialization provides a diagnostic hash of authoritative committed state. Each hash is
labelled with its board, board-session generation, and committed sequence; an asynchronous result is
published only while all three still match. Optimistic pending overlays remain visible on the canvas
but do not change or relabel the authoritative hash.

Each Workspace startup owns an opaque board-session generation, cancellation controller, and local
transport. Snapshot and pending-command continuations recheck generation ownership before every
store mutation or transport side effect. Supersession and cleanup abort fetches and disconnect only
that generation's transport, while store and transport callbacks ignore obsolete tokens. This also
keeps React Strict Mode cleanup/remount cycles from leaving an orphan socket. Browser convergence
tests require `READY`, the expected board and sequence, the expected committed objects, and a
sequence-labelled hash matching the canonical authoritative HTTP snapshot.

The database still does not persist a trustworthy hash for each historical sequence, so the client
cannot verify a server-issued hash specifically at the join watermark.

## Membership revocation

The supported runtime removal path is the owner-authenticated
`DELETE /v1/boards/:boardId/members/:userId` operation. Under the same board advisory-lock family used
for ordered canvas writes, its transaction reauthorizes the owner, protects the board owner, removes
the target membership, and atomically records a typed `board.membership.revoked` outbox event. Removal
of an already-absent non-owner is deliberately idempotent and returns `removed: false`; it still
performs local eviction so a residual socket cannot retain access. Membership changes do not advance
the canvas `last_seq`.

On the current single API instance, a keyed board-delivery coordinator encloses each canvas commit
through local publication and each membership-removal commit through targeted room eviction. Thus an
operation that owns the board coordinator first may publish before revocation, while a revocation
that owns it first commits deletion and evicts every matching principal socket from that board room
before a later operation can publish. Unrelated boards proceed independently, acknowledgement
delivery remains outside the coordinator, and the PostgreSQL advisory lock remains the durable
sequencing authority. A failed coordinated task releases its board queue, and inactive queue keys are
discarded.

Each affected socket receives a strict, content-free `board:access-revoked` event before leaving only
the revoked board room. The matching client session immediately enters terminal authorization
failure, cancels synchronization and command-retry work, clears transient live state, hides the board
projection, and preserves durable IndexedDB pending commands. Events for stale session generations or
other boards are ignored.

Direct administrative SQL changes are outside this application-owned control path and require
explicit session invalidation or API restart. This slice guarantees ordered revocation only for
sockets on one API instance. Milestone 2 will dispatch the durable revocation outbox event, publish it
through the shared control plane, and have every API instance evict its matching local sockets; no
distributed or instantaneous cross-instance revocation is claimed here.

Authoritative snapshot reload cannot recover arbitrary server data loss, and preserved pending
commands retain their original base sequences and idempotency identities; a command that remains
incompatible is handled by the existing structured submission response rather than silently rewritten.

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
