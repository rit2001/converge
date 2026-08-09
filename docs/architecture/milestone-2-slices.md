# Milestone 2 implementation slices

Status: accepted Milestone 2 execution plan

Each slice is a separate review and commit. A slice starts only after the prior stop condition is
demonstrated and reviewed. Commands below are future implementation gates; none are run during the
design-only turn. Tests should be targeted while iterating and the stated root gates run before each
slice commit.

The architecture approvals listed in
[the M2 HLD](milestone-2-distributed-delivery.md#accepted-human-decisions) are accepted preconditions
for M2.1. No slice may weaken M1 behavior or claim exactly-once delivery.

## M2.1: Protocol and durable board-delivery ordering

**Goal:** Introduce the versioned delivery envelope and PostgreSQL board-delivery sequence without
starting Redis publication.

**Preconditions:** ADR 004, the legacy outbox cutover policy, command canonicalization, and migration
maintenance window are approved. A clean database and a representative M1 database fixture are
available.

**Exact invariant:** Every newly committed delivery-bearing board mutation atomically owns one stable
event ID and one unique, increasing board delivery sequence; canvas sequences remain contiguous only
for canvas operations, and exact replay allocates nothing.

**Expected files/packages:**

- `packages/protocol/src/index.ts` and protocol schema tests;
- `packages/database/src/schema.ts`, a new forward migration, repository code, and database tests;
- `apps/api/src/app.ts` only where required to pass/return the new durable result;
- `tests/integration/operations.test.ts`, `membership-revocation.test.ts`, and migration fixtures; and
- relevant architecture/status documentation only if implementation evidence changes it.

**Tests required:** New operation allocation, revocation allocation, operation/revocation interleaving,
unrelated boards, exact replay, idempotency conflict, rollback, owner protection, legacy backfill,
constraint violations, and future restore-type schema reservation. Verify legacy rows are historical
and never newly dispatched.

**Commands to run:**

```bash
pnpm --filter @converge/protocol test
pnpm --filter @converge/database test
pnpm migrate
pnpm --dir tests/integration exec vitest run operations.test.ts membership-revocation.test.ts
pnpm typecheck
pnpm lint
pnpm format:check
```

**Commit message:** `feat(database): add durable board delivery ordering`

**Explicit exclusions:** Redis clients, leases, worker process, cross-instance fan-out, snapshots,
compaction, and removal of the M1 live path.

**Stop condition:** Both clean and legacy database migrations prove constraints; transaction tests
prove cross-type order and exact replay; a review confirms no Redis/network I/O occurs inside the
allocation transaction.

## M2.2: Leased PostgreSQL outbox repository

**Goal:** Implement the bounded, token-fenced outbox state machine and claim repository without Redis
I/O.

**Preconditions:** M2.1 is merged; all new outbox rows have stable event IDs and non-null delivery
sequences.

**Exact invariant:** At most one event per board is leased at a time, no later same-board event is
claimable before every predecessor is published, expired/stale owners are fenced, and unrelated board
heads can be claimed concurrently.

**Expected files/packages:**

- `packages/database/src/schema.ts`, a forward migration, and outbox repository modules;
- `packages/protocol` only for bounded diagnostic/status schemas;
- `packages/testkit` controllable clock/barrier helpers; and
- focused database/integration tests.

**Tests required:** Every state transition, 32-row bound, free-slot-aware claiming, two-worker
contention, separate-board concurrency, lease expiry/reclaim, stale token completion, server-time
eligibility, deterministic backoff inputs, maximum-attempt blocking, operator retry, bounded errors,
indexes/constraints, and rollback.

**Commands to run:**

```bash
pnpm --filter @converge/database test
pnpm --dir tests/integration exec vitest run outbox-repository.test.ts
pnpm migrate
pnpm typecheck
pnpm lint
pnpm format:check
```

**Commit message:** `feat(database): add leased outbox state machine`

**Explicit exclusions:** Redis commands, worker lifecycle, API consumers, Socket.IO changes,
snapshots, and compaction.

**Stop condition:** Deterministic contention proves `SKIP LOCKED` cannot overtake a same-board head;
lease-token fencing and blocked-board behavior are reviewed from both SQL and tests.

## M2.3: Worker publisher and crash-window recovery

**Goal:** Add `apps/worker` and publish leased envelopes through direct Redis `XADD` with a positive
entry-ID acknowledgement.

**Preconditions:** M2.2 claim/transition APIs are stable; Redis test isolation and approved bounded
defaults are available.

**Exact invariant:** A worker marks an outbox row published only after `XADD` returns a valid entry ID
and only while its lease token is current; every ambiguous window recovers by safe retry and possible
duplicate stable event ID.

**Expected files/packages:**

- new `apps/worker` package with environment, lifecycle, publisher, and tests;
- `packages/protocol` delivery-envelope codec;
- `packages/database` transition calls only if the M2.2 interface needs correction;
- `packages/observability` worker hooks;
- `packages/testkit` Redis/failure barriers; and
- `tests/failure` publisher crash tests.

**Tests required:** F01-F08, F19, and F20 from the failure matrix; strict envelope validation; Redis
command timeout; shutdown/lease behavior; retry jitter bounds; bounded concurrency; duplicate Redis
entries with stable ID; and unrelated-board progress while one board is blocked.

**Commands to run:**

```bash
docker compose up -d postgres redis
pnpm --filter @converge/worker test
pnpm --dir tests/failure exec vitest run outbox-publisher.test.ts
pnpm typecheck
pnpm lint
pnpm format:check
pnpm build
```

**Commit message:** `feat(worker): publish durable outbox events to redis`

**Explicit exclusions:** API Redis consumption, cross-instance Socket.IO effects, snapshots,
compaction, deployment, and performance claims.

**Stop condition:** Forced termination at each database/Redis boundary leaves a recoverable row and
proves the only published transition follows an acknowledged `XADD`; no transaction spans Redis I/O.

## M2.4: Redis API consumer and multi-instance fan-out

**Goal:** Make every API replica consume the custom stream through its own process-memory plain
`XREAD` cursor and deliver ordered events to local board coordinators.

**Preconditions:** M2.3 produces strict envelopes and stable duplicates; two isolated API instances
can run against one PostgreSQL/Redis pair.

**Exact invariant:** Every active, ready API process observes every stream entry after its captured
startup tail, advances its global cursor only after complete validation/local handling, applies stable
domain effects idempotently, and never emits past a same-board gap.

**Expected files/packages:**

- `apps/api` Redis consumer, instance lifecycle, bounded queue, board cursor, and consumer tests;
- `packages/protocol` internal envelope/result schemas;
- `packages/observability` API consumer hooks;
- `packages/testkit` multi-API and Redis helpers; and
- `tests/integration` multi-instance delivery tests.

**Tests required:** Two independent `XREAD` cursors each receiving every event; tail-capture/read-start
race; deterministic stream-ID batch order; temporary disconnect with retained last-success cursor;
batch validation/handling failure without cursor advancement; cursor trimming/recreation overrun;
restart with no inherited socket/cursor state; duplicate stable event IDs under different Redis stream
IDs; bounded global/per-board queue and dedupe overflow; out-of-order/corrupt envelopes; local-only
`io.local` emission; deterministic committed-state LRU under a global retained-board cap; terminal
capacity exhaustion when only quarantined/non-evictable states remain; and multi-instance operation
convergence. Assert no consumer-group commands,
Socket.IO Redis adapter, or adapter internal formats.

**Commands to run:**

```bash
docker compose up -d postgres redis
pnpm --filter @converge/api test
pnpm --dir tests/integration exec vitest run distributed-delivery.test.ts
pnpm --dir tests/failure exec vitest run api-consumer.test.ts
pnpm typecheck
pnpm lint
pnpm format:check
pnpm build
```

**Commit message:** `feat(api): consume durable board events across replicas`

**Explicit exclusions:** Membership fail-closed cutover, snapshots, compaction, Socket.IO connection
state recovery, Redis adapter, and hosted deployment.

**Stop condition:** Two API replicas with independent in-memory cursors each receive the same worker
event; the tail race, retained-cursor reconnect, overrun, restart, failed-batch, duplicate, and gap
tests are deterministic; memory bounds fail closed; and the M1 direct broadcast remains behind a
disabled cutover flag rather than running concurrently in acceptance mode.

## M2.5: Cross-instance revocation and fail-closed readiness

**Goal:** Cut all durable room effects over to the stream, enforce revocation across replicas, and
make delivery trust part of Socket.IO readiness.

**Preconditions:** M2.4 fan-out/order is proven; client terminal revocation behavior from M1 remains
covered; the fail-closed availability trade-off is approved.

**Exact invariant:** Once a revocation sequence is consumed or a preceding delivery gap is detected,
no local socket for that board can receive a later board event until it has disconnected,
reauthenticated against PostgreSQL, and completed catch-up.

**Expected files/packages:**

- `apps/api/src/app.ts`, server lifecycle/readiness, board coordinator, revocation consumer, and tests;
- `apps/web` only for any strict reconnect/readiness protocol adjustment;
- `packages/protocol` readiness/revocation changes;
- `packages/testkit` multi-principal/multi-instance helpers;
- `tests/integration/membership-revocation.test.ts` plus distributed authorization tests; and
- `tests/playwright` multi-instance revocation/reconnect coverage.

**Tests required:** F09, F12-F14, and F23-F31; join/revocation race; every target socket on API B;
other board/principal isolation; revocation gap before a later operation; Redis loss before/after
publication; retained and overrun cursors; restart with no inherited sockets; batched round-robin
watchdog with jitter and no per-socket queries; HTTP behavior during degradation; readiness status;
current in-flight command commit; WebSocket-only multi-instance connection/reconnect with polling
rejected; and absence of direct or adapter-rebroadcast durable room publication.

**Commands to run:**

```bash
docker compose up -d postgres redis
pnpm --filter @converge/api test
pnpm --filter @converge/web test
pnpm --dir tests/integration exec vitest run membership-revocation.test.ts distributed-authorization.test.ts
pnpm --dir tests/failure exec vitest run fail-closed.test.ts
pnpm --dir tests/playwright exec playwright test distributed-revocation.spec.ts
pnpm typecheck
pnpm lint
pnpm format:check
pnpm build
```

**Commit message:** `feat(api): enforce fail-closed distributed revocation`

**Explicit exclusions:** Presence/ephemeral cross-instance fan-out, production OAuth, snapshots,
compaction, and performance claims.

**Stop condition:** The M1 direct durable broadcast path is removed/disabled; two replicas prove a
revoked socket receives no later operation through every injected race; Redis uncertainty disconnects
sockets before readiness can recover.

## M2.6: Snapshots and snapshot-plus-tail recovery

**Goal:** Persist and serve verified full-projection snapshots while keeping the operation log intact.

**Preconditions:** M2.5 delivery and readiness are stable; canonical snapshot schema/hash and initial
size/scheduling defaults are approved.

**Exact invariant:** A verified snapshot is a deterministic, hash-checked representation of exactly
one canvas and delivery head captured under the board lock, and clients reach a fixed watermark only
by applying its contiguous log tail.

**Expected files/packages:**

- `packages/protocol` snapshot/recovery schemas;
- `packages/canvas-engine` versioned full-projection canonicalization if not already sufficient;
- `packages/database` snapshot migration/repository;
- `apps/worker` bounded scheduler/creator;
- `apps/api` recovery endpoint;
- `apps/web` snapshot replacement plus existing pending/session fencing;
- relevant unit, integration, failure, and Playwright tests.

**Tests required:** Trigger policies, one-board exclusion, busy-lock skip, snapshot/writer race, complete
live/tombstone/field/stack projection, deterministic hash and size, strict schema bounds, latest valid
selection, fixed-watermark join, corrupt newest fallback, operator block without a complete chain,
pending-command preservation, stale session cancellation, and final canonical convergence.

**Commands to run:**

```bash
docker compose up -d postgres redis
pnpm --filter @converge/canvas-engine test
pnpm --filter @converge/protocol test
pnpm --filter @converge/database test
pnpm --filter @converge/worker test
pnpm --filter @converge/web test
pnpm --dir tests/integration exec vitest run snapshot-recovery.test.ts
pnpm --dir tests/failure exec vitest run snapshot-corruption.test.ts
pnpm --dir tests/playwright exec playwright test snapshot-recovery.spec.ts
pnpm typecheck
pnpm lint
pnpm format:check
pnpm build
```

**Commit message:** `feat(recovery): add verified snapshot plus tail sync`

**Explicit exclusions:** Deleting operations/outbox rows, user-visible versions, restore execution,
blob/object storage, and benchmark claims.

**Stop condition:** Snapshot recovery matches an independently reduced full log across a concurrent
writer, corrupt data is never served, and no operation/outbox row has yet been compacted.

## M2.7: Compaction and recovery-floor handling

**Goal:** Add exact idempotency receipts, atomic recovery floors, and safe deletion behind a
disabled-by-default compaction switch.

**Preconditions:** M2.6 snapshots are verified in failure tests; backup/restore and rollback
implications are approved; receipt canonicalization is stable.

**Exact invariant:** Compaction deletes only history covered by a verified snapshot and published
delivery floor, retains normalized command JSONB for definitive replay equality, and makes every
below-floor reader take an explicit snapshot path.

**Expected files/packages:**

- `packages/database` receipt/floor migration, compaction repository, and tests;
- `packages/protocol` snapshot-required/floor responses;
- `apps/worker` compaction scheduler and operator-block behavior;
- `apps/api` floor-aware tail/delivery recovery;
- `apps/web` below-floor recovery;
- `packages/observability` compaction diagnostics; and
- integration/failure/Playwright recovery tests.

**Tests required:** Receipt exact replay and conflict after deletion, unpublished/blocked outbox
refusal, atomic floor/delete rollback, writer/publisher races, floor monotonicity, retained snapshot
pinning, version-restore compatibility contract, client before floor, API delivery cursor before floor,
snapshot corruption after compaction, and no missing/duplicate final state.

**Commands to run:**

```bash
docker compose up -d postgres redis
pnpm --filter @converge/database test
pnpm --filter @converge/worker test
pnpm --filter @converge/web test
pnpm --dir tests/integration exec vitest run compaction.test.ts
pnpm --dir tests/failure exec vitest run compaction-recovery.test.ts
pnpm --dir tests/playwright exec playwright test compaction-floor.spec.ts
pnpm migrate
pnpm typecheck
pnpm lint
pnpm format:check
pnpm build
```

**Commit message:** `feat(recovery): compact operation logs behind recovery floors`

**Explicit exclusions:** Automatic production enablement, deleting idempotency receipts, destructive
down migration, user-visible version history, and restore UI.

**Stop condition:** Compaction stays disabled by default until a review confirms exact replay,
snapshot/tail recovery, corruption blocking, atomic rollback, and backup/restore procedure. Only then
may a later operational change enable scheduling.

## M2.8: Observability, failure suite, and milestone acceptance

**Goal:** Complete structured diagnostics and exercise the whole M2 topology deterministically before
making milestone claims.

**Preconditions:** M2.1-M2.7 stop conditions pass; compaction enablement decision is recorded; no
known authorization or convergence defect remains.

**Exact invariant:** Every durable failure is diagnosable by stable IDs and bounded metrics, and the
full system converges across two APIs/two workers without post-revocation leakage under every matrix
case.

**Expected files/packages:**

- `packages/observability` concrete metric/logger integration and tests;
- instrumentation in `apps/api`, `apps/worker`, and `packages/database` without semantic changes;
- `packages/testkit` final deterministic orchestration;
- `tests/failure`, `tests/integration`, and `tests/playwright` full M2 matrices;
- `tests/k6` reproducible scripts/artifact schema, without unsupported result claims;
- Compose/CI/environment/package changes required to run API, worker, PostgreSQL, and Redis locally and
  in CI; and
- final architecture/status/runbook updates.

**Tests required:** Every F01-F31 case, all required metric increments/label allowlists, bounded log
metadata/redaction, graceful shutdown, multi-instance final-state/hash convergence, clean migration,
Redis recreation, repeated full suite, and a k6 smoke that validates the harness without asserting
capacity.

**Commands to run:**

```bash
docker compose up -d
pnpm typecheck
pnpm lint
pnpm format:check
pnpm test
pnpm migrate
pnpm test:integration
pnpm --filter @converge/failure test
pnpm test:playwright
pnpm build
k6 run tests/k6/milestone-2-smoke.js
```

**Commit message:** `test(m2): complete distributed delivery acceptance gate`

**Explicit exclusions:** Production deployment, capacity/resume claim, paid infrastructure,
autoscaling, Kafka/Kubernetes, premium product features, and exactly-once language.

**Stop condition:** CI and two consecutive clean local acceptance runs pass with retained diagnostics;
the architecture/status documents match implementation; a human design/operations/security review
accepts the milestone. Deployment remains a separate authorized action.

## Cross-slice review rule

If a slice reveals that an approved invariant cannot be met, stop that slice and revise the affected
ADR/HLD before coding around it. Do not defer sequence, authorization, recovery-floor, or fencing
ambiguity to M2.8. Observability hooks and deterministic fault boundaries are added with the behavior
they describe even though final metric integration occurs in M2.8.
