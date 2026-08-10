# ADR 005: Redis delivery topology

Status: accepted for Milestone 2

## Context

Milestone 1 calls `io.to(room).emit(...)` after commit. That call is synchronous from the handler's
perspective, provides no durable Redis acknowledgement, reaches only the current API instance, and
does not close the crash-after-commit window. Membership eviction is likewise process-local.

Milestone 2 needs a worker acknowledgement boundary, at-least-once replay, broadcast to every API
replica, duplicate tolerance, and fail-closed authorization behavior. PostgreSQL remains the durable
authority; Redis may be interrupted, trimmed, restarted, or rebuilt.

## Decision

Use one custom Redis Stream, `converge:delivery:v1`, as a bounded delivery bus. `apps/worker` claims
PostgreSQL outbox rows and executes a direct `XADD`. A syntactically valid Redis Stream entry ID
returned by `XADD` is the positive publication acknowledgement. Only after receiving it does the
worker conditionally mark the outbox row published with its lease token.

The acknowledgement means that the configured Redis server accepted the entry. It is not an
exactly-once guarantee, a subscriber acknowledgement, a Redis quorum guarantee, or a replacement for
PostgreSQL durability. Loss or ambiguity is recovered through PostgreSQL and application catch-up.

Every active API process independently consumes the stream with plain `XREAD`. Redis consumer groups
are not used. The process keeps its last fully processed Redis stream ID in memory and requests entries
strictly greater than that cursor. Each API therefore sees every retained event without per-boot group,
pending-entry, acknowledgement, cleanup, heartbeat, or reclamation state in Redis.

### Startup and the tail-capture boundary

An API connects to PostgreSQL and Redis while rejecting Socket.IO handshakes. It reads the stream's
current last-generated ID, using `0-0` for a missing or empty stream, stores that ID as its initial
cursor, and issues `XREAD COUNT 100 BLOCK 5000 STREAMS converge:delivery:v1 <cursor>` on a dedicated
connection. Only after the read loop is issued and its lifecycle/error handlers are active may the API
report Socket.IO readiness.

There is no tail-capture/read-start loss window. `XREAD` treats the supplied ID as an exclusive lower
bound and first returns already-retained entries with greater IDs before it blocks. An event appended
after tail capture but before Redis receives the first `XREAD` therefore compares greater than the
captured cursor and is returned immediately by that read.

Events at or before the startup tail are represented by PostgreSQL. The new process has no inherited
sockets, and reconnecting clients recover authoritative state through authenticated
snapshot-plus-tail synchronization.

### Cursor and batch processing

Redis batches are processed in stream-ID order. For each entry, the API strictly validates the outer
envelope, payload identity, stable event ID, and board delivery sequence before any local Socket.IO
effect. The global stream cursor advances to that entry ID only after handling finishes.

Complete local handling means the local emit or eviction path returned without error; it is not a
client receipt acknowledgement. A client that disconnects before receipt recovers from PostgreSQL.

If validation or local handling fails, the API stops at that entry, does not process later entries from
the returned batch, and does not advance beyond it. Earlier successfully handled entries may already
have advanced the cursor. The next read starts from the last success and therefore returns the failed
entry again while it remains retained. Unknown schema/type is a global delivery-integrity failure and
makes the API Socket.IO-unready.

A valid envelope with a same-board delivery gap is handled differently: the API first blocks and
fail-closes that board, then quarantines the envelope in the board's bounded buffer. That completed
fail-closed action permits the global Redis cursor to advance so unrelated boards can continue, but
the board delivery cursor does not advance and no later event for that board may be emitted. Recovery
uses retained PostgreSQL outbox data or requires reauthentication and snapshot-plus-tail state.

## Event format and retention

Each worker Redis entry has exactly six ordered fields: `schemaVersion`, `eventId`, `boardId`,
`deliverySeq`, `eventType`, and `event`. `event` contains a strict JSON envelope no larger than 128
KiB. Before `XADD`, the worker measures UTF-8 bytes for every field name/value and the complete entry.
The fixed names and bounded metadata add at most 165 bytes, so the complete entry maximum is 131,237
bytes. The API's only write is a fixed two-field initialization sentinel of 87 bytes. No HTTP or
Socket handler accepts raw stream fields.

```json
{
  "schemaVersion": 1,
  "eventId": "uuid",
  "boardId": "uuid",
  "deliverySeq": 42,
  "eventType": "operation.committed",
  "occurredAt": "2026-08-09T12:00:00.000Z",
  "payload": {}
}
```

The payload is a strict discriminated union. `operation.committed` contains the existing committed
operation, including canvas `seq`. `board.membership.revoked` contains the revoked user ID and
initiating user ID; only the content-free access-revoked message is emitted to that user. Future
`version.restored` includes the new canvas sequence and immutable source-version identity. Unknown
schema versions, unknown fields, excessive payloads, and mismatched board/event identifiers are
invalid.

The worker uses approximate `MAXLEN` trimming with a configurable initial limit of 100,000 entries,
and scheduled `MINID` trimming caps age at an initial 24 hours. Both values are provisional
operational defaults, not capacity or retention claims. PostgreSQL recovery floors define correctness
when either trim removes history. A duplicate `XADD` has a new Redis entry ID but the same stable
`eventId` and delivery sequence.

Each API keeps a bounded global in-memory queue of at most 1,000 envelopes or 16 MiB. Each blocked
board may quarantine at most 100 envelopes or 2 MiB. Crossing a global bound makes the API
Socket.IO-unready and fail-closes all sockets; crossing a board bound fail-closes that board and drops
its quarantine for PostgreSQL recovery. Memory never grows to preserve live-delivery availability.

`maximumEnvelopeBytes` is a producer-contract and post-decoding semantic limit. It is not a network
or node-redis decoder allocation limit. `XREAD COUNT 100` plus the producer maximum gives a normal
decoded batch bound of 13,127,800 bytes (or 13,179,256 bytes with the conservative RESP2 allowance).
Invalid relationships between the producer maximum, count, and queue limits are rejected, not
clamped.

### Trusted writer boundary and metadata projection

The Redis delivery service is trusted infrastructure: private, authenticated, not public/browser
reachable, and writable only by the worker for validated delivery entries and the API for the fixed
sentinel. Production must add private networking, TLS where supported, separate least-privilege ACL
credentials where supported, credential rotation, and monitoring for unexpected writers, malformed
entries, and stream growth. Local Compose does not yet claim these controls.

`XINFO STREAM` includes complete first/last field/value tuples. The API therefore calls it only inside
a Lua projection and returns to JavaScript a fixed tuple of status, validated scalar IDs, exact
counters, and server incarnation. Sentinel scripts validate exact field count/order/names, fixed
control type, and canonical 36-byte UUID-v4 generation before returning token evidence. Oversized or
invalid evidence produces a constant bounded status; ordinary inspection never returns entry fields.

The installed `redis@6.2.0` decoder has no supported maximum decoded bulk-string option and `XREAD`
materializes complete entries before application validation. Compose Redis 8.2 has no `XREAD
MAXSIZE`. Redis 8.10's `MAXSIZE` is soft and returns at least one entry even if that entry alone
exceeds it. A custom RESP client, a Redis upgrade solely for `MAXSIZE`, and a second payload store were
rejected: `MAXSIZE` does not remove the malicious-first-entry risk, and the others expand this slice's
protocol or consistency scope. Compromised writer credentials or Redis server remain an explicit
infrastructure incident, not an application parser memory-safety case.

## Ordering, gaps, and duplicates

The outbox permits at most one in-flight event per board. Redis may interleave unrelated boards, but
one board's entries are appended in delivery-sequence order. Each API keeps a local cursor for every
board with local sockets:

- `deliverySeq == cursor + 1`: apply and advance;
- `deliverySeq <= cursor` or a previously seen `eventId`: count as a duplicate and do not reapply;
- `deliverySeq > cursor + 1`: record a gap, stop board delivery, and immediately disconnect all local
  sockets for that board.

After a gap, the API reads retained outbox envelopes from PostgreSQL in delivery order. If the gap is
at or above the board delivery recovery floor, it can validate and replay the missing rows. If the
cursor predates the floor or any envelope is corrupt/unavailable, the API leaves the sockets
disconnected, resets that board state from current PostgreSQL membership and snapshot metadata, and
requires clients to reauthenticate and perform application-level snapshot-plus-tail catch-up.

Outbox ambiguity can append multiple Redis entries with different stream IDs and the same stable
event ID/delivery sequence. The API's bounded stable-ID/sequence window makes duplicate revocation a
no-op after the target sockets are already absent and makes duplicate operation delivery harmless;
the web client also deduplicates operation ID/canvas sequence. Redis stream ID is only the global
transport position.

Each API emits only to its locally owned sockets. Operation fan-out uses
`io.local.to(boardRoom).emit(...)` or a tested equivalent that bypasses non-local adapters;
revocation iterates the local namespace sockets. M2 installs no Socket.IO Redis adapter, so an event
independently consumed by each API cannot be adapter-rebroadcast and duplicated across instances.

Horizontally scaled production configures Socket.IO client and server for WebSocket-only transport.
Long-polling fallback remains disabled unless load-balancer stickiness is explicitly proven and tested;
the accepted M2 topology therefore has no sticky-session dependency.

If Redis disconnects, the API immediately becomes Socket.IO-unready, stops local durable broadcasts,
and disconnects all active sockets through a two-second bounded lifecycle. A surviving process keeps
its last fully processed global stream cursor but clears socket rooms and board-local state after
disconnection.

On recovery it compares the retained cursor with the bounded Redis-side projection of `XINFO STREAM`
first-entry ID, last-entry/last-generated, and max-deleted-entry metadata, and treats a Redis server-incarnation
change as uncertain. A missing or recreated stream, a last-generated ID behind the cursor, or
deletion/trimming beyond the cursor is delivery-integrity loss. The API never silently jumps forward
while sockets are active. It remains
fail-closed, captures a safe current tail only after no sockets are accepted, issues a new `XREAD`
after that tail, and then restores readiness. If the cursor remains valid, it captures a recovery
tail, resumes `XREAD` strictly after the retained cursor, drains and validates through that tail while
no sockets are accepted, issues the next blocking read, and then restores readiness. In both cases
clients reauthenticate and perform PostgreSQL catch-up.

A process restart does not recover an old cursor. It owns no old sockets, captures the current tail,
establishes a new plain `XREAD` loop, and only then becomes ready. No Redis pending-entry recovery or
stale lifecycle state exists.

As a backstop for an event that never reaches Redis, each API checks PostgreSQL delivery heads for at
most 100 distinct active local boards in one query every five seconds, with plus-or-minus 20% jitter
between replicas and round-robin selection when more boards are active. It is never one query per
socket. If a database head remains ahead of the local applied cursor at a check, the API fail-closes
that board room; it does not wait for a later Redis entry. The watchdog detects silent gaps but cannot
validate sequences, deliver events, or permit later events past a missing revocation.

Socket.IO connection-state recovery is not used as the durable recovery mechanism. It may later be
enabled only as a supplementary transport optimization. Converge's authenticated join,
snapshot-plus-tail protocol, stable operation IDs, and sequence validation remain authoritative.

## Alternatives considered

### Official Socket.IO Redis Streams adapter with an in-API dispatcher

The adapter supplies Socket.IO fan-out and has useful temporary-disconnection behavior. It is
rejected for the durable outbox boundary because its documented integration is an adapter behind the
ordinary `io.to(...).emit(...)` API; it does not document returning the Redis stream entry ID required
by Converge's outbox acknowledgement. This is an inference from the supported public API, not a claim
about private implementation internals. Writing directly to the adapter's internal stream would
couple Converge to an undocumented wire format. Running dispatch inside every API also mixes
connection readiness with durable worker ownership.

### Classic Redis adapter plus external `redis-emitter`

This is a supported pairing for classic Redis Pub/Sub, but Pub/Sub is not a retained log. A publish
response does not ensure disconnected API replicas observed the event, and recovery would require a
second durable cursor mechanism. `@socket.io/redis-emitter` is not assumed to work with the Redis
Streams adapter.

### Custom stream with one shared API consumer group

Rejected because a shared group load-balances each entry to only one API instance. It does not
broadcast each board event to all instances that may own sockets for that board.

### One custom-stream consumer group per API boot

Considered because separate groups technically make every API receive every entry. Rejected because
process death also destroys every socket that needed those entries, while groups leave stale group,
pending-entry, acknowledgement, heartbeat, and cleanup state. Plain per-process `XREAD` has the same
active-instance broadcast property with a smaller failure surface; restarted clients already recover
from PostgreSQL.

### PostgreSQL polling or `LISTEN`/`NOTIFY` only

Polling the outbox from every API could avoid Redis, but multiplies database reads with replica count.
`LISTEN`/`NOTIFY` alone is lossy. A durable per-instance PostgreSQL cursor would recreate a stream
broker inside the authority database. Redis already exists in local topology and isolates this
delivery pressure without becoming authoritative.

## Consequences

The design has one new deployable process (`apps/worker`) and no extra microservices. It duplicates
stream reads across API replicas by design. It provides at-least-once delivery and explicit recovery,
not exactly-once delivery. Authorized producer output and all post-decoding in-process state are
bounded; a malicious broker/writer can still force node-redis allocation before validation. A Redis failure can
reduce live availability, but it cannot erase committed board state and must not preserve stale
authorization.

## Primary references

- [Socket.IO Redis Streams adapter](https://socket.io/docs/v4/redis-streams-adapter/): public adapter
  integration, temporary-disconnection behavior, stream bounds, connection-state recovery, and sticky
  sessions.
- [Socket.IO classic Redis adapter](https://socket.io/docs/v4/redis-adapter/): Pub/Sub topology,
  outage behavior, sticky sessions, and the documented Redis emitter pairing.
- [Redis `XADD`](https://redis.io/docs/latest/commands/xadd/): append command, trimming controls, and
  returned stream entry ID.
- [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/): exclusive `XREAD`
  cursors, retained-entry reads, stream metadata, trimming, and the distinction between independent
  reads and within-group work distribution.
- [Redis `XINFO STREAM`](https://redis.io/docs/latest/commands/xinfo-stream/): summary replies include
  complete first and last entry field/value tuples.
- [Redis Lua API](https://redis.io/docs/latest/develop/interact/programmability/lua-api/): RESP integer
  replies become Lua numbers under embedded Lua 5.1.
- [Redis `MAXCOUNT`/`MAXSIZE` change](https://github.com/redis/redis/pull/15282): Redis 8.10 soft
  XREAD reply-size behavior, including returning the first oversized entry.
