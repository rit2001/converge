# Project status

Last updated: 2026-08-06

## Completed

### Milestone 0

- pnpm/Turborepo workspace with the requested app, package, test, infrastructure, and docs layout
- Pinned Node.js/pnpm/dependency versions, strict TypeScript, ESLint, Prettier, and lockfile
- Local PostgreSQL 17 and Redis 8 Docker Compose services with health checks
- Zod environment validation and a secret-free example environment
- GitHub Actions for install, typecheck, lint, unit tests, migration, integration, and Playwright
- Architecture overview, three accepted ADRs, root setup guide, and honest feature inventory

### Milestone 1

- Versioned Zod HTTP/socket schemas with bounded fields and structured errors
- Pure deterministic reducer, field-level merge behavior, canonical serializer, and SHA-256 hashing
- Migration and Drizzle schema for boards, members, projections, operations, and transactional outbox
- Board create/get and bounded operation-range HTTP endpoints
- Isolated development-only authentication adapter; it refuses to start in production
- Board-room join authorization and authorization inside every durable mutation
- `object.create`, `object.update`, `object.transform`, and `object.delete` transaction pipeline
- Transaction-scoped board advisory lock, monotonic sequencing, idempotency, projection, and outbox
- Acknowledgements, at-least-once committed broadcasts, socket rate limiting, and payload bounds
- IndexedDB pending queue, optimistic state, reconciliation, deduplication, sequence buffering, gap fetch,
  and reconnect metadata
- Rectangle/sticky canvas with selection, movement, resizing, deletion, pan, zoom, and diagnostics
- Unit, PostgreSQL integration, and two-independent-context Playwright convergence coverage

## Verification

- TypeScript: 11/11 workspace packages pass
- ESLint: 11/11 workspace packages pass
- Unit tests: 8/8 pass
- PostgreSQL integration tests: 5/5 pass
- Playwright convergence test: 1/1 passes
- Production build: all workspace build tasks pass, including optimized Next.js output

## Deferred by design

- Transactional outbox dispatcher and replay of the commit-before-broadcast crash window
- Redis Socket.IO adapter, multi-instance fan-out, presence TTLs, and distributed rate limits
- Snapshots plus operation-log-tail recovery and retention/compaction policy
- Production OAuth authentication and invitation/member-management product flows
- Ephemeral presence, cursor, selection, transform, stroke, and text previews
- Compensating undo/restore, grouping, ordering, board clear, and version restore
- Editable sticky-note text UI beyond creating/rendering the Milestone 1 note
- Failure injection for process loss, Redis interruption, and snapshot corruption
- Repeatable load benchmarks; no benchmark numbers have been invented

## Blocked

None. The local verification run used PostgreSQL host port `55432` because port `5432` was already in
use; the Compose default remains `5432` and supports `POSTGRES_PORT` overrides.

## Exact next milestone

Milestone 2 is reliable delivery and bounded recovery: implement a leased transactional-outbox
dispatcher, Redis-backed Socket.IO fan-out without making Redis authoritative, snapshot creation plus
snapshot-and-tail reconnect recovery, and deterministic failure tests for crash-after-commit and
duplicate/reordered delivery. Production authentication and advanced canvas operations remain outside
that milestone.
