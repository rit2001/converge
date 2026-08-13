import { execFile, spawn } from "node:child_process";
import crypto from "node:crypto";
import { mkdtemp, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import os from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  CleanupStack,
  assertSanitizedArtifact,
  durableEvidenceSql,
  validateEnvironmentArtifact,
  validateDurableEvidence,
  validateK6Summary,
} from "./k6-smoke-harness-lib.mjs";

const executeFile = promisify(execFile);
const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const composeServices = ["postgres", "redis"];
const databaseBaseUrl = "postgresql://converge:converge@127.0.0.1:55432";
const redisUrl = "redis://127.0.0.1:6379";
const timestampUtc = new Date().toISOString();
const runId = `${process.pid}-${crypto.randomBytes(6).toString("hex")}`;
const databaseName = `converge_m28_k6_${runId.replaceAll("-", "_")}`;
const streamKey = `converge:test:m28-k6:${runId}:delivery`;
const boardId = crypto.randomUUID();
const userId = crypto.randomUUID();
const authToken = `m28-k6-${crypto.randomBytes(24).toString("hex")}`;
const metricsToken = `m28-metrics-${crypto.randomBytes(24).toString("hex")}`;
const temporaryRoot = await mkdtemp(join(os.tmpdir(), "converge-m28-k6-"));
const summaryPath = join(temporaryRoot, "k6-summary.json");
const cleanup = new CleanupStack();
let initiallyRunning = new Set();
const debugOnce = process.env.CONVERGE_K6_ACCEPTANCE_MODE === "debug";
const debugRepeat = process.env.CONVERGE_K6_ACCEPTANCE_MODE === "repeat";
const debugConcurrent = process.env.CONVERGE_K6_ACCEPTANCE_MODE === "concurrent";
const debugSequential = process.env.CONVERGE_K6_ACCEPTANCE_MODE === "sequential";

function safeEnvironment(databaseUrl, apiPort, workerPort) {
  const common = {
    ...process.env,
    NODE_ENV: "test",
    DATABASE_URL: databaseUrl,
    REDIS_URL: redisUrl,
    REDIS_STREAM_KEY: streamKey,
    LOG_LEVEL: "silent",
  };
  return {
    api: {
      ...common,
      HOST: "127.0.0.1",
      API_PORT: String(apiPort),
      WEB_ORIGIN: "http://127.0.0.1:3000",
      API_DELIVERY_MODE: "distributed",
      API_METRICS_ENABLED: "true",
      API_METRICS_BEARER_TOKEN: metricsToken,
      DEV_AUTH_USER_ID: userId,
      DEV_AUTH_USER_NAME: "M2.8 k6 smoke user",
      DELIVERY_WATCHDOG_JITTER_RATIO: "0",
    },
    worker: {
      ...common,
      WORKER_ID: `m28-k6-${runId}`,
      WORKER_OPERATIONS_ENABLED: "true",
      WORKER_OPERATIONS_HOST: "127.0.0.1",
      WORKER_OPERATIONS_PORT: String(workerPort),
      WORKER_METRICS_ENABLED: "false",
      OUTBOX_IDLE_POLL_MS: "25",
      OUTBOX_POLL_JITTER_RATIO: "0",
      SNAPSHOT_POLL_INTERVAL_MS: "600000",
      COMPACTION_ENABLED: "false",
    },
  };
}

async function command(commandName, args, options = {}) {
  return executeFile(commandName, args, {
    cwd: repositoryRoot,
    maxBuffer: 4 * 1024 * 1024,
    ...options,
  });
}

async function allocatePort() {
  const server = createServer();
  await new Promise((resolvePromise, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolvePromise);
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  await new Promise((resolvePromise, reject) =>
    server.close((error) => (error ? reject(error) : resolvePromise())),
  );
  if (port === 0) throw new Error("Unable to allocate an isolated loopback port");
  return port;
}

async function waitUntil(label, assertion, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const value = await assertion();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 100));
  }
  throw new Error(`${label} did not become ready${lastError ? ` (${lastError.message})` : ""}`);
}

async function waitForHttp(url, expectedStatus = 200) {
  return waitUntil(url, async () => {
    const response = await fetch(url);
    return response.status === expectedStatus;
  });
}

function startApplication(executable, source, environment) {
  const child = spawn(executable, [source], {
    cwd: repositoryRoot,
    env: environment,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let diagnostics = "";
  const retain = (chunk) => {
    diagnostics = `${diagnostics}${chunk}`.slice(-8_192);
  };
  child.stdout.on("data", retain);
  child.stderr.on("data", retain);
  const exited = new Promise((resolvePromise) => child.once("exit", resolvePromise));
  return {
    child,
    diagnostics: () =>
      diagnostics.replaceAll(authToken, "[redacted]").replaceAll(metricsToken, "[redacted]"),
    exited,
  };
}

async function stopApplication(owner) {
  if (owner.child.exitCode !== null) return;
  owner.child.kill("SIGTERM");
  await Promise.race([
    owner.exited,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("Application shutdown timed out")), 15_000),
    ),
  ]);
}

async function psql(database, sql) {
  const result = await command("docker", [
    "compose",
    "exec",
    "-T",
    "postgres",
    "psql",
    "-v",
    "ON_ERROR_STOP=1",
    "-U",
    "converge",
    "-d",
    database,
    "-At",
    "-F",
    "|",
    "-c",
    sql,
  ]);
  return result.stdout.trim();
}

async function readDurableEvidence(database, evidenceBoardId, handled = 0) {
  const row = await psql(database, durableEvidenceSql(evidenceBoardId));
  const values = row.split("|").map(Number);
  if (values.length !== 11 || values.some((value) => !Number.isSafeInteger(value)))
    throw new Error("Durable evidence query returned an invalid fixed shape");
  const [
    lastSequence,
    deliveryHead,
    operations,
    distinctOperations,
    objects,
    outbox,
    distinctOutboxEvents,
    published,
    identityMismatches,
    invalidPublicationIds,
    invalidPublishedState,
  ] = values;
  return {
    lastSequence,
    deliveryHead,
    operations,
    distinctOperations,
    objects,
    outbox,
    distinctOutboxEvents,
    published,
    handled,
    identityMismatches,
    invalidPublicationIds,
    invalidPublishedState,
  };
}

async function assertPortReleased(port) {
  const server = createServer();
  await new Promise((resolvePromise, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolvePromise);
  });
  await new Promise((resolvePromise) => server.close(resolvePromise));
}

let succeeded = false;
let apiPort;
let workerPort;
let evidence;
let thresholdEvidence;
let acceptanceError;
let cleanupError;
try {
  const runningResult = await command("docker", [
    "compose",
    "ps",
    "--status",
    "running",
    "--services",
  ]);
  initiallyRunning = new Set(runningResult.stdout.trim().split("\n").filter(Boolean));
  const servicesToRestore = composeServices.filter((service) => !initiallyRunning.has(service));
  if (servicesToRestore.length > 0) {
    await command("docker", ["compose", "up", "-d", ...servicesToRestore]);
    cleanup.own("compose-services", async () => {
      await command("docker", ["compose", "stop", ...servicesToRestore]);
    });
  }
  await waitUntil("PostgreSQL", async () => {
    const result = await command("docker", [
      "compose",
      "exec",
      "-T",
      "postgres",
      "pg_isready",
      "-U",
      "converge",
      "-d",
      "converge",
    ]).catch(() => undefined);
    return result?.stdout.includes("accepting connections");
  });
  await waitUntil("Redis", async () => {
    const result = await command("docker", [
      "compose",
      "exec",
      "-T",
      "redis",
      "redis-cli",
      "PING",
    ]).catch(() => undefined);
    return result?.stdout.trim() === "PONG";
  });

  await command("docker", [
    "compose",
    "exec",
    "-T",
    "postgres",
    "createdb",
    "-U",
    "converge",
    databaseName,
  ]);
  cleanup.own("database", async () => {
    await command("docker", [
      "compose",
      "exec",
      "-T",
      "postgres",
      "dropdb",
      "--force",
      "-U",
      "converge",
      databaseName,
    ]);
    const remaining = await psql(
      "postgres",
      `SELECT count(*) FROM pg_database WHERE datname = '${databaseName}'`,
    );
    if (remaining !== "0") throw new Error("The test-owned database remained after cleanup");
  });
  cleanup.own("redis-key", async () => {
    await command("docker", ["compose", "exec", "-T", "redis", "redis-cli", "DEL", streamKey]);
    const remaining = await command("docker", [
      "compose",
      "exec",
      "-T",
      "redis",
      "redis-cli",
      "EXISTS",
      streamKey,
    ]);
    if (remaining.stdout.trim() !== "0")
      throw new Error("The test-owned Redis key remained after cleanup");
  });

  const databaseUrl = `${databaseBaseUrl}/${databaseName}`;
  await command("pnpm", ["--filter", "@converge/database", "migrate"], {
    env: { ...process.env, DATABASE_URL: databaseUrl },
  });
  const migrations = await psql(
    databaseName,
    "SELECT count(*) || '|' || max(name) FROM converge_migrations",
  );
  if (migrations !== "9|0009_board_replay_receipts_and_recovery_floors.sql")
    throw new Error("The isolated database was not migrated through 0009");
  await psql(
    databaseName,
    `INSERT INTO boards(id, name, created_by) VALUES ('${boardId}', 'M2.8 k6 smoke', '${userId}'); INSERT INTO board_members(board_id, user_id, role) VALUES ('${boardId}', '${userId}', 'owner')`,
  );

  validateDurableEvidence(await readDurableEvidence(databaseName, boardId), 0);
  const fixture = await command(
    join(repositoryRoot, "packages", "database", "node_modules", ".bin", "tsx"),
    [join(repositoryRoot, "tests", "integration", "k6-durable-fixture.ts")],
    {
      env: {
        ...process.env,
        DATABASE_URL: databaseUrl,
        CONVERGE_K6_FIXTURE_USER_ID: userId,
      },
    },
  );
  const preflightBoardId = fixture.stdout.trim();
  if (!/^[0-9a-f-]{36}$/i.test(preflightBoardId))
    throw new Error("Repository fixture returned invalid bounded evidence");
  validateDurableEvidence(await readDurableEvidence(databaseName, preflightBoardId, 1), 1);
  await psql(databaseName, `DELETE FROM boards WHERE id = '${preflightBoardId}'`);
  if (
    (await psql(databaseName, `SELECT count(*) FROM boards WHERE id = '${preflightBoardId}'`)) !==
    "0"
  )
    throw new Error("Repository fixture cleanup retained owned rows");

  [apiPort, workerPort] = await Promise.all([allocatePort(), allocatePort()]);
  const environments = safeEnvironment(databaseUrl, apiPort, workerPort);
  const worker = startApplication(
    join(repositoryRoot, "apps", "worker", "node_modules", ".bin", "tsx"),
    join(repositoryRoot, "apps", "worker", "src", "server.ts"),
    environments.worker,
  );
  cleanup.own("worker", async () => {
    await stopApplication(worker);
    await stopApplication(worker);
  });
  const api = startApplication(
    join(repositoryRoot, "apps", "api", "node_modules", ".bin", "tsx"),
    join(repositoryRoot, "apps", "api", "src", "server.ts"),
    environments.api,
  );
  cleanup.own("api", async () => {
    await stopApplication(api);
    await stopApplication(api);
  });
  await Promise.race([
    Promise.all([
      waitForHttp(`http://127.0.0.1:${workerPort}/health/ready`),
      waitForHttp(`http://127.0.0.1:${workerPort}/health/delivery-ready`),
      waitForHttp(`http://127.0.0.1:${apiPort}/health/ready`),
      waitForHttp(`http://127.0.0.1:${apiPort}/health/socket-ready`),
    ]),
    worker.exited.then(() => {
      throw new Error(`Worker exited during startup: ${worker.diagnostics()}`);
    }),
    api.exited.then(() => {
      throw new Error(`API exited during startup: ${api.diagnostics()}`);
    }),
  ]);

  const k6 = await command("node", ["scripts/run-k6-smoke.mjs"], {
    env: {
      ...process.env,
      CONVERGE_BASE_URL: `http://host.docker.internal:${apiPort}`,
      CONVERGE_SOCKET_URL: `http://host.docker.internal:${apiPort}`,
      CONVERGE_BOARD_ID: boardId,
      CONVERGE_AUTH_TOKEN: authToken,
      CONVERGE_ALLOW_REMOTE_TARGET: "true",
      CONVERGE_K6_SUMMARY_PATH: summaryPath,
      ...(debugOnce
        ? {
            CONVERGE_K6_DEBUG_ONCE: "true",
            CONVERGE_DEBUG_PHASES: "true",
            CONVERGE_COMMANDS_PER_CLIENT: "1",
          }
        : {}),
      ...(debugRepeat
        ? {
            CONVERGE_DEBUG_PHASES: "true",
            CONVERGE_DEBUG_REPEAT: "true",
            CONVERGE_COMMANDS_PER_CLIENT: "1",
          }
        : {}),
      ...(debugConcurrent
        ? {
            CONVERGE_DEBUG_PHASES: "true",
            CONVERGE_DEBUG_CONCURRENT: "true",
            CONVERGE_COMMANDS_PER_CLIENT: "1",
          }
        : {}),
      ...(debugSequential
        ? {
            CONVERGE_DEBUG_PHASES: "true",
            CONVERGE_DEBUG_SEQUENTIAL: "true",
            CONVERGE_COMMANDS_PER_CLIENT: "1",
          }
        : {}),
    },
  }).catch((error) => {
    process.stderr.write(error.stdout ?? "");
    process.stderr.write(error.stderr ?? "");
    throw new Error("The isolated k6 smoke command failed");
  });
  process.stdout.write(k6.stdout);
  process.stderr.write(k6.stderr);
  const k6Summary = JSON.parse(await readFile(summaryPath, "utf8"));
  thresholdEvidence = validateK6Summary(k6Summary);
  if (debugOnce || debugRepeat || debugConcurrent || debugSequential) {
    const debugOutput = `${k6.stdout}\n${k6.stderr}`;
    const requiredPhases = [
      "connected",
      "authenticated",
      "joined",
      "command_scheduled",
      "command_sent",
      "command_acknowledged",
      "delivery_observed",
      "closed",
    ];
    for (const phase of requiredPhases) {
      if (!debugOutput.includes(`phase=${phase}`))
        throw new Error(`The debug lifecycle did not reach phase ${phase}`);
    }
    if (debugRepeat) {
      const completed = debugOutput.match(/phase=closed/g)?.length ?? 0;
      if (completed !== 8 || thresholdEvidence.acknowledgements !== 8)
        throw new Error("The bounded repeated-iteration lifecycle did not complete 8 commands");
    }
    if (debugConcurrent) {
      const completed = debugOutput.match(/phase=closed/g)?.length ?? 0;
      if (completed !== 2 || thresholdEvidence.acknowledgements !== 2)
        throw new Error("The concurrent-join lifecycle did not complete 2 commands");
    }
    if (debugSequential) {
      const completed = debugOutput.match(/phase=closed/g)?.length ?? 0;
      if (completed !== 2 || thresholdEvidence.acknowledgements !== 2)
        throw new Error("The repeated-join lifecycle did not complete 2 commands");
    }
  }
  evidence = await waitUntil("durable publication and API handling", async () => {
    const response = await fetch(`http://127.0.0.1:${apiPort}/metrics`, {
      headers: { authorization: `Bearer ${metricsToken}` },
    });
    const metrics = await response.text();
    const handled = Number(
      /converge_delivery_events_total\{event_type="operation",outcome="handled"\} ([0-9]+)/.exec(
        metrics,
      )?.[1] ?? 0,
    );
    try {
      return validateDurableEvidence(
        await readDurableEvidence(databaseName, boardId, handled),
        thresholdEvidence.acknowledgements,
      );
    } catch {
      return undefined;
    }
  });

  assertSanitizedArtifact(k6Summary, [
    authToken,
    metricsToken,
    databaseName,
    streamKey,
    boardId,
    userId,
  ]);
  succeeded = true;
} catch (error) {
  acceptanceError = error;
} finally {
  try {
    await cleanup.close();
  } catch (error) {
    cleanupError = error;
  }
  if (apiPort) await assertPortReleased(apiPort);
  if (workerPort) await assertPortReleased(workerPort);
  const runningAfter = await command("docker", [
    "compose",
    "ps",
    "--status",
    "running",
    "--services",
  ]).then((result) => new Set(result.stdout.trim().split("\n").filter(Boolean)));
  if (
    composeServices.some((service) => initiallyRunning.has(service) !== runningAfter.has(service))
  )
    cleanupError = new Error("Compose services were not restored to their initial states");
}

if (cleanupError) throw cleanupError;
if (acceptanceError) {
  await rm(temporaryRoot, { recursive: true, force: true });
  throw acceptanceError;
}

if (!succeeded || !evidence || !thresholdEvidence)
  throw new Error("The isolated smoke did not produce complete acceptance evidence");

if (debugOnce || debugRepeat || debugConcurrent || debugSequential) {
  await rm(temporaryRoot, { recursive: true, force: true });
  process.stdout.write(
    debugRepeat
      ? "Isolated 2-VU repeated-iteration debug accepted.\n"
      : debugConcurrent
        ? "Isolated 2-VU concurrent-join debug accepted.\n"
        : debugSequential
          ? "Isolated 1-VU repeated-join debug accepted.\n"
          : "Isolated 1-VU lifecycle debug accepted.\n",
  );
  process.exit(0);
}

const commit = (await command("git", ["rev-parse", "HEAD"])).stdout.trim();
const pnpmVersion = (await command("pnpm", ["--version"])).stdout.trim();
const environmentArtifact = {
  timestampUtc,
  gitCommitSha: commit,
  k6Version: "0.57.0",
  nodeVersion: process.version.replace(/^v/, ""),
  pnpmVersion,
  os: os.platform(),
  architecture: os.arch(),
  profile: "smoke",
  virtualUsers: 2,
  duration: "30s",
  apiReplicas: 1,
  workerReplicas: 1,
  configuration: {
    apiDeliveryMode: "distributed",
    protocol: "Engine.IO v4 / Socket.IO v4",
    database: "disposable PostgreSQL migrated through 0009",
    redis: "test-owned isolated delivery stream",
    compactionEnabled: false,
  },
};
validateEnvironmentArtifact(environmentArtifact);
const artifactName = `m2-k6-smoke-${timestampUtc.replaceAll(/[-:.]/g, "").replace(/\.\d+Z$/, "Z")}`;
const artifactDirectory = join(repositoryRoot, "docs", "benchmarks", "results", artifactName);
const stagingDirectory = join(temporaryRoot, artifactName);
await mkdir(stagingDirectory, { recursive: true });
const k6Summary = JSON.parse(await readFile(summaryPath, "utf8"));
const summaryMarkdown = `# M2.8 isolated k6 smoke result

This is a correctness smoke run, not a capacity benchmark.

- Topology: one production-composed distributed API, one production-composed worker, one disposable PostgreSQL database migrated through 0009, and one test-owned Redis delivery stream.
- Profile: smoke, 2 VUs for 30 seconds, using the real Engine.IO v4 / Socket.IO v4 collaboration protocol.
- Thresholds: all configured smoke thresholds passed; ${thresholdEvidence.acknowledgements} distinct commands were acknowledged and received their matching logical deliveries; ${thresholdEvidence.liveEvents} total valid live events were observed, including peer activity.
- At-least-once evidence: ${thresholdEvidence.transportDuplicatesObserved} valid transport duplicates were observed and ${thresholdEvidence.transportDuplicatesSuppressed} were suppressed before logical application or command completion. This is informational deduplication evidence, not a failure.
- Correctness: protocol failures, sequence gaps, unexpected disconnects, and logical reapplications were zero. ${evidence.logicalReducerApplications} authoritative logical reducer applications, ${evidence.distinctOperations} distinct durable operation rows, ${evidence.distinctOutboxEvents} distinct outbox rows, both heads, publication, and API consumer handling converged.
- Cleanup: API, worker, listeners, database, Redis key, timers, and benchmark process were released; PostgreSQL and Redis were restored to their initial stopped state.
- Pre-acceptance corrections: zero-delay command scheduling, iteration identity reuse, authoritative snapshot ownership, and catch-up/live overlap reconciliation were corrected; the lifecycle now requires command, acknowledgement, and matching delivery evidence before normal closure.

No production, baseline, scale-step, capacity, or 10,000-user claim is made.
`;
assertSanitizedArtifact(summaryMarkdown, [
  authToken,
  metricsToken,
  databaseName,
  streamKey,
  boardId,
  userId,
]);
await Promise.all([
  writeFile(join(stagingDirectory, "summary.md"), summaryMarkdown, "utf8"),
  writeFile(
    join(stagingDirectory, "k6-summary.json"),
    `${JSON.stringify(k6Summary, null, 2)}\n`,
    "utf8",
  ),
  writeFile(
    join(stagingDirectory, "environment.json"),
    `${JSON.stringify(environmentArtifact, null, 2)}\n`,
    "utf8",
  ),
]);
await mkdir(dirname(artifactDirectory), { recursive: true });
await rename(stagingDirectory, artifactDirectory);
await rm(temporaryRoot, { recursive: true, force: true });
process.stdout.write(`Isolated k6 smoke accepted: ${artifactName}\n`);
