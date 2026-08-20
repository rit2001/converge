import { execFile, spawn, type ChildProcess } from "node:child_process";
import { access, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { buildApp, type AppContext } from "@converge/api";
import { DevelopmentAuthAdapter } from "@converge/api/auth";
import { parseEnvironment } from "@converge/api/env";
import { BoardRepository, createPool, type DatabasePool } from "@converge/database";
import type { CanvasObject, DurableCommand } from "@converge/protocol";
import type { Browser, BrowserContext, Page } from "@playwright/test";

const runFile = promisify(execFile);
const root = fileURLToPath(new URL("../..", import.meta.url));
const webRoot = fileURLToPath(new URL("../../apps/web/", import.meta.url));
const baseDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@127.0.0.1:55432/converge";
const principal = { id: "30000000-0000-4000-8000-000000000081", displayName: "Performance User" };
const clientId = "30000000-0000-4000-8000-000000000082";

type OwnedApi = { context: AppContext; pool: DatabasePool; origin: string };
export type ProductionPerformanceTopology = {
  boardId: string;
  apiOrigin: string;
  webOrigin: string;
  open(browser: Browser): Promise<{ context: BrowserContext; page: Page }>;
  stop(): Promise<void>;
};
export type PerformanceTopologyOptions = Readonly<{ failAfter?: "database" | "api" }>;

function deterministicObject(index: number): CanvasObject {
  const base = {
    id: `40000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
    x: (index % 20) * 110 - 900,
    y: Math.floor(index / 20) * 100 - 200,
    width: 80 + ((index * 13) % 80),
    height: 64 + ((index * 7) % 64),
    rotation: ((index * 15) % 360) - 180,
    fill: ["#DDEAFE", "#FFF0A8", "#DFF4E7", "#FBE0E4"][index % 4]!,
  };
  return index % 2 === 0
    ? { ...base, kind: "rectangle", text: "" }
    : { ...base, kind: "sticky", text: `Performance note ${String(index % 20).padStart(2, "0")}` };
}

export function createHundredObjectFixture(): readonly CanvasObject[] {
  return Array.from({ length: 100 }, (_, index) => deterministicObject(index));
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string")
        return reject(new Error("PERF_PORT_UNAVAILABLE"));
      server.close((error) => (error ? reject(error) : resolve(address.port)));
    });
  });
}

function boundedOutput(child: ChildProcess): void {
  let bytes = 0;
  const consume = (chunk: Buffer): void => {
    bytes += chunk.byteLength;
    if (bytes > 32_768) child.kill("SIGTERM");
  };
  child.stdout?.on("data", consume);
  child.stderr?.on("data", consume);
}

async function stopChild(child: ChildProcess | undefined): Promise<void> {
  if (!child || child.exitCode !== null) return;
  await new Promise<void>((resolve) => {
    child.once("exit", () => resolve());
    try {
      if (child.pid) process.kill(-child.pid, "SIGTERM");
      else child.kill("SIGTERM");
    } catch {
      child.kill("SIGTERM");
    }
    setTimeout(() => {
      try {
        if (child.pid) process.kill(-child.pid, "SIGKILL");
        else child.kill("SIGKILL");
      } catch {
        /* stopped */
      }
      resolve();
    }, 2_000).unref();
  });
}

async function waitFor(url: string): Promise<void> {
  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      /* starting */
    }
    await new Promise((resolve) => setTimeout(resolve, 125));
  }
  throw new Error("PERF_STARTUP_TIMEOUT");
}

async function postgresWasRunning(): Promise<boolean> {
  const { stdout } = await runFile("docker", ["compose", "ps", "-q", "postgres"], { cwd: root });
  return stdout.trim().length > 0;
}

async function waitForPostgres(): Promise<void> {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    try {
      await runFile(
        "docker",
        ["compose", "exec", "-T", "postgres", "pg_isready", "-U", "converge", "-d", "postgres"],
        { cwd: root },
      );
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 125));
    }
  }
  throw new Error("PERF_POSTGRES_TIMEOUT");
}

async function seed(repository: BoardRepository, boardId: string): Promise<void> {
  const objects = createHundredObjectFixture();
  for (const [index, object] of objects.entries()) {
    const command: DurableCommand = {
      schemaVersion: 1,
      opId: `50000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
      boardId,
      clientId,
      baseSeq: index,
      targetId: object.id,
      clientTimestamp: "2026-08-20T00:00:00.000Z",
      type: "object.create",
      payload: object,
    };
    await repository.commitOperation(principal.id, command);
  }
}

export async function countOwnedPerformanceDatabases(): Promise<number> {
  const adminUrl = new URL(baseDatabaseUrl);
  adminUrl.pathname = "/postgres";
  const pool = createPool(adminUrl.toString());
  try {
    const result = await pool.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM pg_database WHERE datname LIKE 'converge_m38a1_%'",
    );
    return Number(result.rows[0]?.count ?? 0);
  } finally {
    await pool.end();
  }
}

export async function startProductionPerformanceTopology(
  options: PerformanceTopologyOptions = {},
): Promise<ProductionPerformanceTopology> {
  let admin: DatabasePool | undefined,
    database: DatabasePool | undefined,
    api: OwnedApi | undefined;
  let web: ChildProcess | undefined, databaseName: string | undefined;
  let postgresStarted = false,
    buildOwned = false,
    stopped: Promise<void> | undefined;
  const contexts = new Set<BrowserContext>();
  const cleanup = async (): Promise<void> => {
    await Promise.allSettled([...contexts].map((context) => context.close()));
    await stopChild(web);
    if (api) {
      await Promise.allSettled([api.context.app.close(), api.pool.end()]);
    }
    await database?.end();
    if (admin && databaseName)
      await admin
        .query(`DROP DATABASE IF EXISTS "${databaseName}" WITH (FORCE)`)
        .catch(() => undefined);
    await admin?.end();
    if (buildOwned) await rm(join(webRoot, ".next"), { recursive: true, force: true });
    if (postgresStarted)
      await runFile("docker", ["compose", "stop", "postgres"], { cwd: root }).catch(
        () => undefined,
      );
  };
  try {
    if (!(await postgresWasRunning())) {
      await runFile("docker", ["compose", "up", "-d", "postgres"], { cwd: root });
      postgresStarted = true;
    }
    await waitForPostgres();
    const adminUrl = new URL(baseDatabaseUrl);
    adminUrl.pathname = "/postgres";
    admin = createPool(adminUrl.toString());
    databaseName = `converge_m38a1_${process.pid}_${crypto.randomUUID().replaceAll("-", "").slice(0, 8)}`;
    await admin.query(`CREATE DATABASE "${databaseName}"`);
    const databaseUrl = new URL(baseDatabaseUrl);
    databaseUrl.pathname = `/${databaseName}`;
    await runFile("pnpm", ["--filter", "@converge/database", "migrate"], {
      cwd: root,
      env: { ...process.env, DATABASE_URL: databaseUrl.toString() },
    });
    database = createPool(databaseUrl.toString());
    const repository = new BoardRepository(database);
    const board = await repository.createBoard(principal.id, "M3.8A1 production smoke");
    await seed(repository, board.id);
    if (options.failAfter === "database") throw new Error("PERF_FORCED_PARTIAL_START");
    const [webPort] = await Promise.all([freePort()]);
    const webOrigin = `http://127.0.0.1:${webPort}`;
    const environment = parseEnvironment({
      NODE_ENV: "test",
      HOST: "127.0.0.1",
      API_PORT: "4000",
      WEB_ORIGIN: webOrigin,
      DATABASE_URL: databaseUrl.toString(),
      API_DELIVERY_MODE: "local",
      API_PRESENCE_ENABLED: "false",
      DEV_AUTH_USER_ID: principal.id,
      DEV_AUTH_USER_NAME: principal.displayName,
      LOG_LEVEL: "silent",
    });
    const apiPool = createPool(databaseUrl.toString());
    const context = await buildApp(environment, apiPool, new DevelopmentAuthAdapter(environment));
    await context.app.listen({ host: "127.0.0.1", port: 0 });
    const address = context.app.server.address();
    if (!address || typeof address === "string") throw new Error("PERF_API_PORT_UNAVAILABLE");
    api = { context, pool: apiPool, origin: `http://127.0.0.1:${address.port}` };
    if (options.failAfter === "api") throw new Error("PERF_FORCED_PARTIAL_START");
    try {
      await access(join(webRoot, ".next"));
      throw new Error("PERF_BUILD_DIR_NOT_OWNED");
    } catch (error) {
      if (error instanceof Error && error.message === "PERF_BUILD_DIR_NOT_OWNED") throw error;
    }
    await runFile("pnpm", ["--filter", "@converge/web", "build"], {
      cwd: root,
      env: { ...process.env, NEXT_PUBLIC_API_URL: api.origin },
    });
    buildOwned = true;
    web = spawn(
      process.execPath,
      [join(webRoot, "node_modules/next/dist/bin/next"), "start", "--port", String(webPort)],
      {
        cwd: webRoot,
        env: { ...process.env, NEXT_PUBLIC_API_URL: api.origin },
        stdio: ["ignore", "pipe", "pipe"],
        detached: true,
      },
    );
    boundedOutput(web);
    await waitFor(webOrigin);
    const stop = (): Promise<void> => (stopped ??= cleanup());
    return {
      boardId: board.id,
      apiOrigin: api.origin,
      webOrigin,
      open: async (browser) => {
        const context = await browser.newContext();
        contexts.add(context);
        const page = await context.newPage();
        await page.goto(`${webOrigin}/studio/${board.id}`);
        return { context, page };
      },
      stop,
    };
  } catch (error) {
    await cleanup();
    throw error;
  }
}
