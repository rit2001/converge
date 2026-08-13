# Milestone 2 release evidence

Status: locally verified and ready for pull-request review on 2026-08-13.

## Scope and guarantees

Milestone 2 adds PostgreSQL-authoritative board delivery ordering, a leased transactional outbox with
fenced worker finalization, at-least-once Redis publication and consumption, independent API-local
fan-out, distributed membership revocation, fail-closed socket readiness and watchdog enforcement,
verified snapshot-plus-tail recovery, immutable replay receipts, transactional compaction, and
coupled recovery floors.

Delivery is **at least once, not exactly once**. Stable event and operation identities make duplicate
transport evidence safe to suppress, while PostgreSQL remains authoritative for committed state and
ordering. Redis is a delivery accelerator and its durability is not certified by this milestone.

Snapshot recovery verifies the selected snapshot and contiguous operation tail before atomically
rebasing client state. Corrupt or unavailable trustworthy material fails closed. Compaction is
transactional, safety-delayed, receipt-backed, and advances operation and delivery recovery floors
together; it remains explicitly opt-in.

Bounded, low-cardinality API and worker telemetry can be rendered deterministically in Prometheus
text format. The API and worker expose opt-in protected metrics and distinct liveness, core/HTTP
readiness, socket readiness, and delivery readiness signals. Twelve metric-backed alerts have
validated PromQL fixtures, and provider-neutral SLO policy and incident runbooks are documented.

## Architecture and migration record

- M1 baseline: `8841f313bf00491585585b563c6bbb1342ec78dc`
- M2 implementation range:
  `8841f313bf00491585585b563c6bbb1342ec78dc..1283f8daeed2dd3cf810348d369a30a0883ed24c`
- M2 migrations: `0004` through `0009`
- Full forward migration chain verified: `0001` through `0009`

The principal decisions are recorded in the Milestone 2 architecture document and ADRs 004 through
006: board-local delivery sequence, Redis delivery topology, and verified snapshot/compaction
boundaries.

## Benchmark evidence

Accepted M2 evidence is limited to:

- one isolated 2-VU/30-second correctness smoke using the real distributed collaboration path; and
- one controlled local 10-VU/2-minute baseline with one API replica, one worker, and a bounded
  one-object-per-VU working set.

The local 10→50→100 execution is explicitly an **unaccepted observation**. It establishes no capacity
ceiling and does not support a 100-VU claim. High-concurrency session refinement is deferred.

These results do not claim production capacity, horizontal or multi-replica performance, an SLA,
10,000-user support, Redis durability, or exactly-once delivery.

## Local release verification

The final gate completed with no k6 execution:

- frozen lockfile install, repository formatting, root lint, root typecheck, root build, and
  Prometheus rule validation passed;
- root tests passed 492 assertions;
- protocol passed 22/22, API 253/253, worker 86/86, and web 81/81;
- the replacement-session recovery test passed five consecutive focused runs, and the complete M2.6
  acceptance file passed 6/6 twice;
- focused web recovery passed 65/65 and snapshot/recovery repository integration passed 49/49;
- compaction candidate discovery passed 11/11 twice, 32/32 alongside another candidate-producing
  suite, and 17/17 beside M2.6 recovery;
- the complete integration suite passed 180/180 twice;
- deterministic PostgreSQL/Redis failure tests passed 55, with one intentionally skipped test;
- Playwright collaboration passed 3/3;
- a clean disposable database applied migrations `0001` through `0009` in order;
- the existing development database accepted the forward migrator without reset, recreation, or new
  migration application; deterministic post-check fingerprints were stable;
- benchmark harness/artifact validation passed 14/14 and pure k6 contract validation passed 35/35;
- accepted smoke and baseline artifact hashes were unchanged, privacy scanning found no forbidden
  data, and no accepted scale-step directory exists; and
- ESLint, relevant TypeScript checks, targeted Prettier, and `git diff --check` passed for the release
  corrections.

The final bounded review found no P0/P1 defect and no reproducible material P2 correctness, security,
data-loss, ordering, or recovery defect that invalidates an M2 claim.

## Cleanup

All suite-owned databases and Redis keys were removed. Playwright servers, browsers, test processes,
listeners, pools, and timers exited. PostgreSQL and Redis were restored to their original stopped
state. The shared development database was neither reset nor recreated, and its data was not
intentionally modified.

## Deferred and explicitly unclaimed

- production deployment and provider integration;
- accepted 100-VU or 10,000-user capacity;
- multi-replica performance benchmarking;
- Redis durability certification and exactly-once delivery;
- backup/restore certification;
- dashboards and Alertmanager routing; and
- external-probe and stuck-work alert implementation.
