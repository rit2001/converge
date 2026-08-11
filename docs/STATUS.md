# Project status

Last reviewed: 2026-08-11

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
outbox publication. Final snapshot-plus-tail recovery acceptance remains pending.

## Deferred by design

- Production activation of transactional-outbox dispatch (the repository, worker publisher, and
  crash-boundary failure evidence are implemented)
- Final snapshot recovery acceptance and operation-log compaction
- Historical per-sequence authoritative hashes
- Production OAuth
- Invitations, share links, and full membership administration
- Multi-tab pending-command coordination
- Repeatable load-test claims and production deployment

The single-instance revocation path does not detect direct administrative SQL changes and does not
evict sockets connected to another API instance. Those distributed control-plane guarantees remain
Milestone 2 work.

No benchmark or production-capacity claim is currently supported.
