import { execFile, spawn, type ChildProcess } from "node:child_process";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { buildApp, type AppContext } from "@converge/api";
import { DevelopmentAuthAdapter } from "@converge/api/auth";
import { parseEnvironment, type Environment } from "@converge/api/env";
import { BoardRepository, createPool, type DatabasePool } from "@converge/database";
import type { Browser, BrowserContext, Page } from "@playwright/test";
import { createClient, type RedisClientType } from "redis";

const runFile = promisify(execFile);
const root = fileURLToPath(new URL("../..", import.meta.url));
const webRoot = fileURLToPath(new URL("../../apps/web/", import.meta.url));
const databaseRoot =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@127.0.0.1:55432/converge";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const owner = { id: "10000000-0000-4000-8000-000000000071", displayName: "Browser Owner" };
const editor = { id: "10000000-0000-4000-8000-000000000072", displayName: "Browser Editor" };

type Api = { context: AppContext; pool: DatabasePool; origin: string; close(): Promise<void> };
type Web = { origin: string; process: ChildProcess; distDir: string };
export type DualReplicaBrowserTopology = {
  apiA: Api;
  apiB: Api;
  webA: Web;
  webB: Web;
  boardId: string;
  owner: typeof owner;
  editor: typeof editor;
  openA(browser: Browser): Promise<{ context: BrowserContext; page: Page }>;
  openB(browser: Browser): Promise<{ context: BrowserContext; page: Page }>;
  stop(): Promise<void>;
};
export type DualReplicaBrowserTopologyOptions = Readonly<{ failAfterApiA?: boolean }>;

async function port(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") return reject(new Error("No dynamic port"));
      server.close((error) => (error ? reject(error) : resolve(address.port)));
    });
  });
}
function boundedOutput(child: ChildProcess): void {
  let bytes = 0;
  const limit = 16_384;
  const consume = (chunk: Buffer): void => {
    bytes += chunk.length;
    if (bytes > limit) child.kill("SIGTERM");
  };
  child.stdout?.on("data", consume);
  child.stderr?.on("data", consume);
}
async function waitFor(origin: string): Promise<void> {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    try {
      if ((await fetch(origin)).ok) return;
    } catch {
      /* starting */
    }
    await new Promise((resolve) => setTimeout(resolve, 125));
  }
  throw new Error("Test web startup timed out");
}
async function ensureServices(): Promise<Array<"postgres" | "redis">> {
  const started: Array<"postgres" | "redis"> = [];
  for (const service of ["postgres", "redis"] as const) {
    const { stdout } = await runFile("docker", ["compose", "ps", "-q", service], { cwd: root });
    if (!stdout.trim()) started.push(service);
  }
  if (started.length) await runFile("docker", ["compose", "up", "-d", ...started], { cwd: root });
  return started;
}
async function waitForPostgres(): Promise<void> {
  for (let attempt = 0; attempt < 40; attempt += 1) {
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
  throw new Error("Test PostgreSQL startup timed out");
}
function environment(databaseUrl: string, webOrigin: string, principal: typeof owner): Environment {
  return parseEnvironment({
    NODE_ENV: "test",
    HOST: "127.0.0.1",
    API_PORT: "4000",
    WEB_ORIGIN: webOrigin,
    DATABASE_URL: databaseUrl,
    REDIS_URL: redisUrl,
    API_PRESENCE_ENABLED: "true",
    DEV_AUTH_USER_ID: principal.id,
    DEV_AUTH_USER_NAME: principal.displayName,
    LOG_LEVEL: "silent",
  });
}
async function apiFor(environmentValue: Environment): Promise<Api> {
  const pool = createPool(environmentValue.DATABASE_URL);
  const context = await buildApp(
    environmentValue,
    pool,
    new DevelopmentAuthAdapter(environmentValue),
  );
  await context.app.listen({ host: "127.0.0.1", port: 0 });
  const address = context.app.server.address();
  if (!address || typeof address === "string") throw new Error("No API listener");
  let closing: Promise<void> | undefined;
  return {
    context,
    pool,
    origin: `http://127.0.0.1:${address.port}`,
    close: () =>
      (closing ??= (async () => {
        try {
          await context.app.close();
        } finally {
          await pool.end();
        }
      })()),
  };
}
async function stopChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
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
        child.kill("SIGKILL");
      }
      resolve();
    }, 2_000).unref();
  });
}

/** Test-owned only: two independently compiled clients point at independent production-composed APIs. */
export async function startDualReplicaBrowserTopology(
  options: DualReplicaBrowserTopologyOptions = {},
): Promise<DualReplicaBrowserTopology> {
  let admin: DatabasePool | undefined;
  let database: DatabasePool | undefined;
  let redis: RedisClientType | undefined;
  let apiA: Api | undefined, apiB: Api | undefined;
  let webA: Web | undefined, webB: Web | undefined;
  const contexts = new Set<BrowserContext>();
  let databaseName: string | undefined;
  let boardId: string | undefined;
  let servicesStarted: Array<"postgres" | "redis"> = [];
  let nextEnv: string | undefined;
  let webTsconfig: string | undefined;
  const cleanup = async (): Promise<void> => {
    await Promise.allSettled([...contexts].map((context) => context.close()));
    await Promise.allSettled(
      [webA, webB]
        .filter((value): value is Web => Boolean(value))
        .map((web) => stopChild(web.process)),
    );
    if (nextEnv !== undefined) await writeFile(`${webRoot}/next-env.d.ts`, nextEnv);
    if (webTsconfig !== undefined) await writeFile(`${webRoot}/tsconfig.json`, webTsconfig);
    await Promise.allSettled(
      [apiA, apiB].filter((value): value is Api => Boolean(value)).map((api) => api.close()),
    );
    if (boardId && redis?.isOpen) {
      const keys = await redis.keys(`converge:presence:v1:{${boardId}}:*`);
      if (keys.length) await redis.del(keys);
    }
    if (redis?.isOpen) redis.destroy();
    await database?.end();
    try {
      if (admin && databaseName) await admin.query(`DROP DATABASE "${databaseName}"`);
    } finally {
      await admin?.end();
    }
    await Promise.allSettled(
      [webA, webB]
        .filter((value): value is Web => Boolean(value))
        .map((web) => rm(`${webRoot}/${web.distDir}`, { recursive: true, force: true })),
    );
    if (servicesStarted.length)
      await runFile("docker", ["compose", "stop", ...servicesStarted], { cwd: root }).catch(
        () => undefined,
      );
  };
  try {
    servicesStarted = await ensureServices();
    await waitForPostgres();
    [nextEnv, webTsconfig] = await Promise.all([
      readFile(`${webRoot}/next-env.d.ts`, "utf8"),
      readFile(`${webRoot}/tsconfig.json`, "utf8"),
    ]);
    databaseName = `converge_m35c_${process.pid}_${crypto.randomUUID().replaceAll("-", "").slice(0, 8)}`;
    const adminUrl = new URL(databaseRoot);
    adminUrl.pathname = "/postgres";
    admin = createPool(adminUrl.toString());
    await admin.query(`CREATE DATABASE "${databaseName}"`);
    const isolated = new URL(databaseRoot);
    isolated.pathname = `/${databaseName}`;
    await runFile("pnpm", ["--filter", "@converge/database", "migrate"], {
      cwd: root,
      env: { ...process.env, DATABASE_URL: isolated.toString() },
    });
    database = createPool(isolated.toString());
    redis = createClient({ url: redisUrl });
    redis.on("error", () => undefined);
    await redis.connect();
    const [webPortA, webPortB] = await Promise.all([port(), port()]);
    const webOriginA = `http://127.0.0.1:${webPortA}`,
      webOriginB = `http://127.0.0.1:${webPortB}`;
    apiA = await apiFor(environment(isolated.toString(), webOriginA, owner));
    if (options.failAfterApiA) throw new Error("Test topology forced startup failure");
    apiB = await apiFor(environment(isolated.toString(), webOriginB, editor));
    const repository = new BoardRepository(database);
    const board = await repository.createBoard(owner.id, "M3.5C browser topology");
    const sharedBoardId = board.id;
    boardId = sharedBoardId;
    await database.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1,$2,'editor')",
      [sharedBoardId, editor.id],
    );
    // Reuse the already-reserved web ports so the API CORS origins remain exact.
    webA = await webForAt(apiA.origin, "a", webPortA);
    webB = await webForAt(apiB.origin, "b", webPortB);
    const open = async (browser: Browser, web: Web) => {
      const context = await browser.newContext();
      contexts.add(context);
      const page = await context.newPage();
      await page.goto(`${web.origin}/studio?board=${sharedBoardId}`);
      return { context, page };
    };
    return {
      apiA,
      apiB,
      webA,
      webB,
      boardId: sharedBoardId,
      owner,
      editor,
      openA: (browser) => open(browser, webA!),
      openB: (browser) => open(browser, webB!),
      stop: cleanup,
    };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

async function webForAt(apiOrigin: string, suffix: string, webPort: number): Promise<Web> {
  const distDir = `.next-m35c-${suffix}-${crypto.randomUUID().replaceAll("-", "").slice(0, 8)}`;
  await mkdir(`${webRoot}/${distDir}`, { recursive: true });
  await writeFile(
    `${webRoot}/${distDir}/tsconfig.json`,
    JSON.stringify({
      extends: "../../../tsconfig.base.json",
      compilerOptions: {
        plugins: [{ name: "next" }],
        types: ["node", "vitest/globals"],
        incremental: true,
      },
      include: ["next-env.d.ts", "types/**/*.ts", "../app", "../src"],
      exclude: ["node_modules"],
    }),
  );
  const child = spawn(
    process.execPath,
    [join(webRoot, "node_modules/next/dist/bin/next"), "dev", "--port", String(webPort)],
    {
      cwd: webRoot,
      env: {
        ...process.env,
        NEXT_PUBLIC_API_URL: apiOrigin,
        CONVERGE_NEXT_DIST_DIR: distDir,
        CONVERGE_NEXT_TSCONFIG: `${distDir}/tsconfig.json`,
      },
      stdio: ["ignore", "pipe", "pipe"],
      detached: true,
    },
  );
  boundedOutput(child);
  const origin = `http://127.0.0.1:${webPort}`;
  try {
    await waitFor(origin);
    return { origin, process: child, distDir };
  } catch (error) {
    await stopChild(child);
    await rm(`${webRoot}/${distDir}`, { recursive: true, force: true });
    throw error;
  }
}
