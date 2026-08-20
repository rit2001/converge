# Project status

Last reviewed: 2026-08-13

## Milestone 1 implemented invariants

- Strict external schemas and discriminated canvas-object invariants
- Board-local ordered transactional operations and canonical projections
- Future-base rejection, exact idempotent replay, and idempotency-conflict detection
- Server-derived authentication and owner/editor/viewer authorization
- Fixed-watermark initial synchronization and reconnect catch-up
- Persist-before-optimism pending commands with ordered retry and exact identity preservation
- Attempt-fenced synchronization retries with bounded live-operation buffering
- Board-session lifecycle isolation and sequence-specific authoritative hashes
- Owner-authorized membership removal with transactional revocation evidence and ordered,
  single-instance socket eviction
- Root environment loading with explicit-process precedence and sanitized unexpected HTTP failures

## Verification source of truth

Current test totals are intentionally not copied into this document because they become stale whenever
coverage changes. Live command output and the required GitHub Actions workflow are authoritative.

The verification categories are:

- TypeScript type checking
- ESLint
- Prettier formatting check
- Unit tests
- Clean-database migration
- PostgreSQL integration tests
- Playwright convergence and reconnect tests
- Production build

Run the exact local commands from the root `README.md`. CI repeats them with an explicit test
`DATABASE_URL` and a clean PostgreSQL service.

## Milestone 2.5 delivery activation status

The API server can now explicitly select `local` or `distributed` delivery through strict runtime
configuration. Distributed selection composes the existing Redis consumer, API-local routing,
Socket.IO readiness gate, and PostgreSQL board-head watchdog. M2.5 acceptance is complete against an
isolated real PostgreSQL/Redis topology with two independently configured APIs, the real outbox
worker, deterministic watchdog time, a single-connection Redis interruption, PostgreSQL catch-up,
and distributed membership revocation. Deployment remains deferred.

## Milestone 2.6 snapshot status

Automatic bounded snapshot creation is now activated in `apps/worker` independently from Redis
outbox publication. M2.6 acceptance is complete against an isolated PostgreSQL database migrated
through 0008, the real snapshot repositories/coordinator and worker supervision, a dynamic real API,
and a real authenticated BoardTransport. The accepted boundaries cover Redis-independent bootstrap,
threshold capture, verified snapshot-plus-tail replay, one-attempt overflow refresh, corrupt-newest
fallback, terminal blocked evidence, retryable lock contention, atomic/stale-session fencing, and
snapshot/writer and duplicate-capture races.

## Milestone 2.7 compaction status

M2.7 acceptance is complete against a disposable PostgreSQL database migrated through 0009, the
real snapshot, compaction-discovery, compaction, receipt, recovery-material, and operation
repositories, the real bounded coordinator, a dynamic real API, and a real authenticated
BoardTransport. The accepted boundaries cover safety-delayed coupled compaction, immutable-receipt
replay, floor-aware range and full recovery, blocked evidence, advisory-lock races, rollback,
duplicate coordinators, transient retry, and idempotent later progress. Compaction remains explicitly
opt-in with `COMPACTION_ENABLED=true`; snapshot/receipt deletion, Redis trimming, deployment, backup
validation, and benchmarks remain deferred.

## Milestone 2.8 observability status

The provider-neutral bounded telemetry contract now exists in `packages/observability`, including a
fixed low-cardinality metric catalog, bounded structured diagnostic events, no-op and deterministic
in-memory recorders, immutable export snapshots, explicit error classification, and telemetry
failure isolation. Distributed API delivery/recovery instrumentation is complete. Worker
outbox/snapshot/compaction instrumentation is complete. Deterministic Prometheus text rendering is
complete. API liveness, PostgreSQL-backed HTTP readiness, composite socket-readiness, and opt-in
Bearer-protected metrics endpoints are complete. The opt-in worker operational listener now exposes
separate lifecycle, PostgreSQL-backed core, and Redis/outbox delivery health plus protected metrics.
Real distributed API observability acceptance is complete against isolated PostgreSQL and Redis
failure boundaries. Real worker observability acceptance is complete against isolated PostgreSQL and
Redis failure boundaries. The provider-neutral SLO, alert-threshold, inhibition/grouping, and
incident-runbook policy is complete. The twelve metric-backed Prometheus alerts are encoded with
repository-level policy validation and pinned Prometheus 3.5.0 syntax, PromQL, and deterministic rule
fixtures enforced locally and in CI. External-probe and stuck-work alerts, Alertmanager/provider
integration, deployment probe selection, and dashboards remain pending.

The reproducible k6 collaboration workload contract is complete: vanilla k6 Engine.IO v4 / Socket.IO
v4 framing, strict acknowledgements and live-event validation, bounded deduplication/sequence state,
safe smoke/baseline/scale-step profiles, fixed low-cardinality metrics, target safeguards, and pure
helper tests are defined. One isolated 2-VU/30-second correctness smoke has passed against a
production-composed distributed API and worker with disposable PostgreSQL and test-owned Redis
state. One controlled local 10-VU/2-minute baseline also passed using a bounded one-object-per-VU
working set and real `object.update` mutations. Sanitized result artifacts record protocol,
durable-state, publication, consumer-progress, threshold, and cleanup evidence. M2.8 observability
and its reproducible benchmark framework are complete: the correctness smoke and 10-VU baseline are
accepted, while local 10→50→100 scale acceptance was not achieved. The failed executions are retained
only as an unaccepted observation; high-concurrency refinement is deferred to a future performance
milestone. No production, deployment, multi-replica-capacity, or 10,000-user claim is supported.

## Deferred by design

- Production activation of transactional-outbox dispatch (the repository, worker publisher, and
  crash-boundary failure evidence are implemented)
- Snapshot and receipt deletion, and Redis stream trimming
- Historical per-sequence authoritative hashes
- Production OAuth
- Invitations, share links, and full membership administration
- Multi-tab pending-command coordination
- Repeatable load-test claims and production deployment

The single-instance revocation path does not detect direct administrative SQL changes and does not
evict sockets connected to another API instance. Those distributed control-plane guarantees remain
Milestone 2 work.

The recorded local baseline supports only its exact correctness and timing evidence; no production
capacity, scalability, deployment, or 10,000-user claim is currently supported.

## Milestone 3 planning status

The premium product experience architecture, original product language, frontend layering/motion
decision, accessibility and performance budgets, durable Layers-panel capability audit, and eight
independently reviewable implementation slices are defined. M3.1 now establishes the light-theme
semantic token system, dark-compatible token values, reduced-motion and forced-color policies,
centralized workspace layers and portal hosts, accessible UI primitives, and tokenized existing
workspace connection chrome. It adds no dependency and changes no synchronization, protocol,
database, canvas-authority, route, or deployment behavior. Complete workspace composition, landing
and board-entry surfaces, canvas-tool redesign, collaboration presence, product dialogs, theme
switching, responsive policy, and performance acceptance remain in M3.2–M3.8. M3.4 is complete:
real two-client browser acceptance covers authoritative Canvas/Layers selection and order, explicitly
local hide/lock isolation and recovery, deterministic drag snapping with transient guides, and
authoritative rotation controls. Multi-select, undo/redo, grouping, durable reorder or rename,
presence, sharing, history, and palette work remain excluded from M3.4.

M3.5 is in progress. M3.5A is complete: the workspace derives a fail-closed, bounded synchronization
presentation from existing BoardStore and pending-queue evidence only. It distinguishes connecting,
restoring, synced, saving, reconnecting, device-local preservation, access removal, blocked recovery,
and unavailable states without changing transport, persistence, recovery, or protocol behavior.

M3.5B1 is complete: the strict, versioned ephemeral presence schemas and multi-replica Redis
Pub/Sub-plus-TTL architecture are defined with bounded, lossy semantics and no durable-plane impact.
M3.5B2 runtime presence and M3.5B3 roster/cursor UX remain pending.

M3.5B2A is complete: an uncomposed, separately owned atomic Redis presence transport now provides
bounded session records, expiry indexes, revisioned tombstones, snapshots, and validated Pub/Sub
deltas. Socket.IO admission/scheduling and the frontend presence surface remain pending in B2B/B3.

M3.5B2B1 is complete: presence is independently composed behind `API_PRESENCE_ENABLED`, binds only
after successful board join acknowledgement, and remains isolated from HTTP/socket editing readiness.
Its Redis transport reconnection correction is complete: fresh three-client generations retry under
a bounded full-jitter supervisor and notify the runtime only after a complete resubscription.
M3.5B2 API/Redis presence is accepted against two independently composed local-delivery APIs, one
Redis instance, and a disposable migrated database. Frontend presence rendering (M3.5B3) remains pending.
