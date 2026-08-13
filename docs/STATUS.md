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
repository-level policy validation; local `promtool` binary validation remains pending. External-probe
and stuck-work alerts, Alertmanager/provider integration, deployment probe selection, dashboards, and
benchmarks remain pending.

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

No benchmark or production-capacity claim is currently supported.
