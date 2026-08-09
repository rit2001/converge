# Converge

Converge is a real-time visual collaboration workspace built around a server-authoritative,
board-local ordered operation log. `apps/web` is the Next.js frontend; `apps/api` is the Fastify HTTP
and Socket.IO backend. PostgreSQL is the durable authority.

## Local setup

Use Node.js `22.18.x` (see `.nvmrc`), pnpm `10.15.0`, and Docker. From the repository root:

```bash
corepack enable
pnpm install
cp .env.example .env
docker compose up -d
pnpm migrate
pnpm dev
```

The canonical local `.env` is the repository-root file. Root development, migration, integration,
and Playwright commands load it when present; variables already supplied by the shell take
precedence. Production uses injected process environment variables and does not require `.env`.

PostgreSQL is exposed on host port `55432` by default and the example `DATABASE_URL` matches it. The
container still listens on its standard internal port `5432`. Open `http://localhost:3000`; the web
application uses the API at `http://localhost:4000`. Development authentication is local scaffolding
only and refuses production mode.

## Verification

With the Compose PostgreSQL service healthy, run:

```bash
pnpm typecheck
pnpm lint
pnpm format:check
pnpm test
pnpm migrate
pnpm test:integration
pnpm test:playwright
pnpm build
```

GitHub Actions independently runs this release-gate sequence against a clean PostgreSQL service,
including Chromium installation for Playwright. Railway/Vercel deployment remains deferred and is
not performed by CI.

## Current boundary

Milestone 1 includes strict canvas invariants, transactional operations, authorization, gap-free
catch-up, durable pending-command recovery, bounded synchronization recovery, and ordered
single-instance membership revocation. Redis fan-out, outbox dispatch, snapshots/compaction,
production OAuth, and multi-instance deployment remain deferred. No performance or concurrency
claim is made until repeatable benchmark artifacts are committed.

Architecture decisions and current limitations are documented under `docs/architecture`, `docs/adr`,
and `docs/STATUS.md`.
