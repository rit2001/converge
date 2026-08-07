# Converge

Converge is a fault-tolerant real-time visual collaboration workspace built to make its consistency
and recovery behavior inspectable. It uses a server-authoritative, board-local ordered operation log;
it does not use CRDT libraries or a prebuilt synchronization backend.

## Runtime and setup

Node.js `22.18.x` (see `.nvmrc`) and pnpm `10.15.0` are supported. Docker is required for local
PostgreSQL and Redis.

```bash
corepack enable
pnpm install
cp .env.example .env
docker compose up -d
pnpm --filter @converge/database migrate
pnpm dev
```

If port 5432 is occupied, set `POSTGRES_PORT` and update the port in `DATABASE_URL` before starting
the stack.

Open `http://localhost:3000`. The web app talks to the API at `http://localhost:4000`. Development
authentication is local scaffolding only and must not be used in production.

## Current features (Milestones 0–1)

- Rectangle and sticky-note canvas objects with select, move, resize, delete, pan, and zoom
- Optimistic local rendering with pending-operation reconciliation
- Transactional PostgreSQL operation log, projection, idempotency, sequencing, and outbox
- Authorized Socket.IO board rooms, acknowledgements, committed broadcasts, and gap recovery
- Canonical board serialization and SHA-256 convergence diagnostics
- Deterministic unit and PostgreSQL-backed integration tests

Not yet implemented: production authentication, Redis fan-out, snapshots, presence/preview events,
undo/restore, outbox delivery workers, and multi-instance deployment. See `docs/STATUS.md`.

## Validation

```bash
pnpm typecheck
pnpm lint
pnpm test
pnpm test:integration
pnpm test:playwright
```

Architecture decisions are documented under `docs/architecture` and `docs/adr`.
