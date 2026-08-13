import { execFile } from "node:child_process";
import { createServer as createNetServer, type AddressInfo } from "node:net";
import { promisify } from "node:util";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { BoardRecoveryService, buildApp, type AppContext } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import {
  BoardDeliveryHeadWatchdog,
  type BoardDeliveryHeadWatchdogLifecycleEvent,
  type BoardDeliveryHeadWatchdogScheduler,
} from "../../apps/api/src/board-delivery-head-watchdog.js";
import {
  RedisDeliveryConsumer,
  defaultDeliveryConsumerConfiguration,
  systemDeliveryConsumerScheduler,
  type BoardQuarantineEvent,
  type DeliveryConsumerCallbacks,
  type DeliveryConsumerHooks,
  type DeliveryConsumerTransport,
  type DeliveryContext,
  type DeliveryStreamInitialization,
  type DeliveryStreamMetadata,
} from "@converge/api/delivery-consumer";
import {
  configuredDeliveryBuildOptions,
  type DistributedDeliveryStackFactories,
} from "../../apps/api/src/distributed-delivery-stack.js";
import {
  ApiDeliveryRuntime,
  type ApiDeliveryRuntimeOptions,
  type DeliveryRuntimeFactory,
  type DeliveryRuntimeLifecycleEvent,
  type DeliveryRuntimeObserver,
} from "@converge/api/delivery-runtime";
import { parseEnvironment, type Environment } from "@converge/api/env";
import { RedisDeliveryConsumerTransport } from "@converge/api/redis-delivery-transport";
import {
  createApiServer,
  type ApiServerDependencies,
  type ApiServerOwner,
} from "../../apps/api/src/server-runtime.js";
import {
  applyCommitted,
  emptyBoardState,
  hashBoardState,
  visibleObjects,
  type BoardState,
} from "@converge/canvas-engine";
import {
  BoardRepository,
  BoardRecoveryError,
  BoardRecoveryMaterialRepository,
  OutboxRepository,
  createPool,
  type DatabasePool,
} from "@converge/database";
import {
  METRIC_CATALOG,
  PROMETHEUS_CONTENT_TYPE,
  InMemoryTelemetryRecorder,
  type StructuredLogger,
} from "@converge/observability";
import {
  boardAccessRevokedEventSchema,
  boardSnapshotSchema,
  committedOperationSchema,
  deliveryEnvelopeSchema,
  encodeDeliveryStreamFields,
  operationRangeResponseSchema,
  protocolErrorSchema,
  removeBoardMemberResponseSchema,
  type BoardAccessRevokedEvent,
  type BoardSnapshot,
  type CommittedOperation,
  type DeliveryEnvelope,
  type DurableCommand,
  type JoinBoardAck,
  type MembershipRevokedDeliveryEnvelope,
  type OperationAck,
  type OperationCommittedDeliveryEnvelope,
} from "@converge/protocol";
import {
  createRectangleCommand,
  createTestSocket,
  TestAuthAdapter,
  testAuthorizationHeaders,
} from "@converge/testkit";
import {
  OutboxWorker,
  RedisDeliveryStream,
  WorkerProcessLifecycle,
  type OutboxWorkerConfiguration,
  type OutboxWorkerHooks,
  type WorkerCycleResult,
} from "@converge/worker";
import { createClient, type RedisClientType } from "redis";

const sharedDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const repositoryRoot = new URL("../..", import.meta.url);
const runFile = promisify(execFile);
const deadlineMs = 10_000;
const metricsBearerToken = "m28-api-observability-acceptance-token";

const identities = {
  owner: { id: "00000000-0000-4000-8000-000000000091", displayName: "M24B Owner" },
  editor: { id: "00000000-0000-4000-8000-000000000092", displayName: "M24B Editor" },
  viewer: { id: "00000000-0000-4000-8000-000000000093", displayName: "M25 Viewer" },
} as const;
const tokens = {
  owner: "m24b-owner-token",
  editor: "m24b-editor-token",
  viewer: "m25-viewer-token",
} as const;
const auth = new TestAuthAdapter(
  new Map<string, AuthenticatedPrincipal>([
    [tokens.owner, identities.owner],
    [tokens.editor, identities.editor],
    [tokens.viewer, identities.viewer],
  ]),
);
const logger: StructuredLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};
const workerConfiguration: OutboxWorkerConfiguration = {
  owner: "m24b-worker",
  claimBatchSize: 32,
  publishConcurrency: 8,
  leaseDurationMs: 60_000,
  idlePollMs: 250,
  pollJitterRatio: 0.2,
  publicationTimeoutMs: 5_000,
  maximumEnvelopeBytes: 128 * 1024,
};

let databaseName: string | undefined;
let isolatedDatabaseUrl: string | undefined;
let adminPool: DatabasePool | undefined;
let assertionPool: DatabasePool | undefined;
let redis: RedisClientType | undefined;
const ownedStreamKeys = new Set<string>();
const activeTopologies = new Set<TestTopology>();

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
} {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function withDeadline<T>(
  promise: Promise<T>,
  label: string,
  diagnostics: () => unknown,
): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => {
          reject(new Error(`${label} exceeded ${deadlineMs}ms: ${JSON.stringify(diagnostics())}`));
        }, deadlineMs);
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

class Journal<T> {
  readonly entries: T[] = [];
  private readonly waiters = new Set<{
    predicate: (entry: T) => boolean;
    resolve: (entry: T) => void;
  }>();

  push(entry: T): void {
    this.entries.push(entry);
    for (const waiter of this.waiters) {
      if (!waiter.predicate(entry)) continue;
      this.waiters.delete(waiter);
      waiter.resolve(entry);
    }
  }

  waitFor(predicate: (entry: T) => boolean): Promise<T> {
    const existing = this.entries.find(predicate);
    if (existing !== undefined) return Promise.resolve(existing);
    return new Promise<T>((resolve) => this.waiters.add({ predicate, resolve }));
  }
}

class AuditedRedisTransport implements DeliveryConsumerTransport {
  readonly readCursors: string[] = [];
  readonly receivedEntryIds = new Journal<string>();
  connectCalls = 0;
  cancelCalls = 0;
  closeCalls = 0;
  private reconnectGate: ReturnType<typeof deferred<void>> | undefined;
  private initialReadGate: ReturnType<typeof deferred<void>> | undefined;

  constructor(
    readonly delegate: RedisDeliveryConsumerTransport,
    pauseInitialRead = false,
  ) {
    if (pauseInitialRead) this.initialReadGate = deferred<void>();
  }

  async connect(): Promise<void> {
    this.connectCalls += 1;
    if (this.connectCalls > 1 && this.reconnectGate) {
      const gate = this.reconnectGate;
      await gate.promise;
      if (this.reconnectGate === gate) this.reconnectGate = undefined;
    }
    await this.delegate.connect();
  }

  initializeStream(input: {
    generationToken: string;
    signal: AbortSignal;
  }): Promise<DeliveryStreamInitialization> {
    return this.delegate.initializeStream(input);
  }

  verifyInitialization(input: {
    sentinelId: string;
    generationToken: string;
    signal: AbortSignal;
  }): Promise<boolean> {
    return this.delegate.verifyInitialization(input);
  }

  inspect(input: { signal: AbortSignal }): Promise<DeliveryStreamMetadata> {
    return this.delegate.inspect(input);
  }

  async readAfter(input: Parameters<DeliveryConsumerTransport["readAfter"]>[0]) {
    if (this.initialReadGate) {
      const gate = this.initialReadGate;
      await gate.promise;
      if (this.initialReadGate === gate) this.initialReadGate = undefined;
    }
    this.readCursors.push(input.cursor);
    const entries = await this.delegate.readAfter(input);
    for (const entry of entries) this.receivedEntryIds.push(entry.id);
    return entries;
  }

  cancelRead(): Promise<void> {
    this.cancelCalls += 1;
    return this.delegate.cancelRead();
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    this.releaseReconnect();
    return this.delegate.close();
  }

  pauseReconnect(): void {
    if (this.reconnectGate) throw new Error("Redis reconnect is already paused");
    this.reconnectGate = deferred<void>();
  }

  releaseReconnect(): void {
    this.reconnectGate?.resolve();
  }

  releaseInitialRead(): void {
    this.initialReadGate?.resolve();
  }

  interruptReader(): void {
    const reader = (this.delegate as unknown as { reader?: RedisClientType }).reader;
    if (!reader?.isOpen) throw new Error("Redis blocking-read connection is unavailable");
    reader.destroy();
  }

  get connectionIdentities(): { control: unknown; reader: unknown } {
    return this.delegate as unknown as { control: unknown; reader: unknown };
  }
}

interface ApiEvidence {
  label: string;
  lifecycle: Journal<DeliveryRuntimeLifecycleEvent>;
  handled: Journal<DeliveryContext>;
  cursorAdvances: Journal<string>;
  quarantines: Journal<BoardQuarantineEvent>;
  watchdogLifecycle: Journal<BoardDeliveryHeadWatchdogLifecycleEvent>;
  transport: AuditedRedisTransport | undefined;
  consumer: RedisDeliveryConsumer | undefined;
  runtime: ApiDeliveryRuntime | undefined;
  runtimeObserver: DeliveryRuntimeObserver | undefined;
  watchdog: BoardDeliveryHeadWatchdog | undefined;
  watchdogScheduler: ControlledWatchdogScheduler | undefined;
  environment: Environment | undefined;
  rejectedBeforeEstablished: boolean;
  failMetricsSnapshot: boolean;
  recoveryFailures: Map<string, "blocked" | "retryable">;
}

interface ApiInstance {
  evidence: ApiEvidence;
  context: AppContext;
  pool: DatabasePool;
  url: string;
  server?: ApiServerOwner;
  close(): Promise<void>;
}

type TestSocket = ReturnType<typeof createTestSocket>;

class ClientProbe {
  readonly operations = new Journal<CommittedOperation>();
  readonly revocations = new Journal<BoardAccessRevokedEvent>();
  readonly timeline: string[] = [];
  readonly localProbes = new Journal<string>();
  readonly rawOperationCounts = new Map<string, number>();
  readonly rawRevocationCounts = new Map<string, number>();
  readonly disconnects = new Journal<string>();
  state: BoardState;

  constructor(
    readonly socket: TestSocket,
    initialState: BoardState = emptyBoardState(),
  ) {
    this.state = initialState;
    socket.on("operation:committed", (value) => {
      const operation = committedOperationSchema.parse(value);
      this.rawOperationCounts.set(
        operation.opId,
        (this.rawOperationCounts.get(operation.opId) ?? 0) + 1,
      );
      if (operation.seq > this.state.lastSeq) this.state = applyCommitted(this.state, operation);
      this.operations.push(operation);
      this.timeline.push(`operation:${operation.opId}`);
    });
    socket.on("board:access-revoked", (value) => {
      const event = boardAccessRevokedEventSchema.parse(value);
      this.rawRevocationCounts.set(
        event.boardId,
        (this.rawRevocationCounts.get(event.boardId) ?? 0) + 1,
      );
      this.revocations.push(event);
      this.timeline.push(`revoked:${event.boardId}`);
    });
    socket.onAny((event: string, value: unknown) => {
      if (event === "m24b:local-probe" && typeof value === "string") this.localProbes.push(value);
    });
    socket.on("disconnect", (reason) => this.disconnects.push(reason));
  }

  applyCatchUp(operations: readonly CommittedOperation[]): void {
    for (const operation of operations) this.state = applyCommitted(this.state, operation);
  }
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

class ControlledWatchdogScheduler implements BoardDeliveryHeadWatchdogScheduler {
  private currentTime = 0;
  private nextTaskId = 1;
  private readonly tasks = new Map<
    number,
    { resolve: () => void; signal: AbortSignal; abort: () => void }
  >();

  now = (): number => this.currentTime;
  random = (): number => 0.5;

  wait(_delayMs: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      const taskId = this.nextTaskId++;
      const settle = (): void => {
        const task = this.tasks.get(taskId);
        if (!task) return;
        this.tasks.delete(taskId);
        signal.removeEventListener("abort", task.abort);
        resolve();
      };
      const task = { resolve: settle, signal, abort: settle };
      this.tasks.set(taskId, task);
      signal.addEventListener("abort", settle, { once: true });
    });
  }

  async elapse(milliseconds: number): Promise<void> {
    if (!Number.isSafeInteger(milliseconds) || milliseconds < 0)
      throw new Error("Watchdog clock advance must be a non-negative safe integer");
    this.currentTime += milliseconds;
    await flushMicrotasks();
  }

  get pendingTasks(): number {
    return this.tasks.size;
  }
}

interface OperationEvidence {
  eventId: string;
  boardId: string;
  deliverySeq: number;
  canvasSeq: number;
  actorId: string;
  status: string;
  redisEntryId: string | null;
  payload: OperationCommittedDeliveryEnvelope;
}

interface RevocationEvidence {
  eventId: string;
  boardId: string;
  deliverySeq: number;
  status: string;
  redisEntryId: string | null;
  payload: MembershipRevokedDeliveryEnvelope;
}

function requireAssertionPool(): DatabasePool {
  if (!assertionPool) throw new Error("The isolated PostgreSQL pool is unavailable");
  return assertionPool;
}

function requireRedis(): RedisClientType {
  if (!redis) throw new Error("The isolated Redis control client is unavailable");
  return redis;
}

function requireDatabaseUrl(): string {
  if (!isolatedDatabaseUrl) throw new Error("The isolated PostgreSQL URL is unavailable");
  return isolatedDatabaseUrl;
}

type TestNamespace = "m24b" | "m25" | "m25-final" | "m28-api";

function uniqueStreamKey(namespace: TestNamespace): string {
  const key = `converge:test:${namespace}:${crypto.randomUUID()}`;
  ownedStreamKeys.add(key);
  return key;
}

function emitLocalProbe(api: ApiInstance, boardId: string, probe: string): void {
  const io = api.context.io as unknown as {
    local: {
      to(room: string): { emit(event: "m24b:local-probe", value: string): void };
    };
  };
  io.local.to(`board:${boardId}`).emit("m24b:local-probe", probe);
}

function roomHas(api: ApiInstance, boardId: string, client: ClientProbe): boolean {
  return (
    api.context.io.sockets.adapter.rooms.get(`board:${boardId}`)?.has(client.socket.id ?? "") ??
    false
  );
}

function environment(): Environment {
  return parseEnvironment({
    NODE_ENV: "test",
    HOST: "127.0.0.1",
    API_PORT: "4000",
    WEB_ORIGIN: "http://127.0.0.1:3000",
    DATABASE_URL: requireDatabaseUrl(),
    REDIS_URL: redisUrl,
    LOG_LEVEL: "silent",
    DEV_AUTH_USER_NAME: "Unused development identity",
  });
}

function activatedEnvironment(streamKey: string, port: number): Environment {
  return parseEnvironment({
    NODE_ENV: "test",
    HOST: "127.0.0.1",
    API_PORT: String(port),
    WEB_ORIGIN: "http://127.0.0.1:3000",
    DATABASE_URL: requireDatabaseUrl(),
    API_DELIVERY_MODE: "distributed",
    REDIS_URL: redisUrl,
    REDIS_STREAM_KEY: streamKey,
    DELIVERY_WATCHDOG_INTERVAL_MS: "5000",
    DELIVERY_WATCHDOG_GRACE_MS: "5000",
    DELIVERY_WATCHDOG_QUERY_TIMEOUT_MS: "5000",
    DELIVERY_WATCHDOG_BATCH_SIZE: "100",
    DELIVERY_WATCHDOG_JITTER_RATIO: "0",
    API_METRICS_ENABLED: "true",
    API_METRICS_BEARER_TOKEN: metricsBearerToken,
    LOG_LEVEL: "silent",
    DEV_AUTH_USER_NAME: "Unused development identity",
  });
}

async function allocatePort(): Promise<number> {
  const server = createNetServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address() as AddressInfo;
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return address.port;
}

async function expectSocketConnectionRejected(url: string, token: string): Promise<void> {
  const socket = createTestSocket(url, token);
  try {
    const error = await withDeadline(
      new Promise<Error>((resolve) => {
        socket.once("connect_error", resolve);
        socket.connect();
      }),
      `socket rejection at ${url}`,
      () => ({ url, connected: socket.connected }),
    );
    expect(socket.connected).toBe(false);
    const data = (error as Error & { data?: unknown }).data;
    if (data !== undefined)
      expect(protocolErrorSchema.parse(data)).toMatchObject({ retryable: true });
  } finally {
    socket.disconnect();
  }
}

async function expectSocketReadinessRejected(api: ApiInstance, token: string): Promise<void> {
  const socket = createTestSocket(api.url, token);
  try {
    const error = await withDeadline(
      new Promise<Error>((resolve) => {
        socket.once("connect_error", resolve);
        socket.connect();
      }),
      `${api.evidence.label} readiness rejection`,
      () => apiDiagnostics(api.evidence),
    );
    expect(protocolErrorSchema.parse((error as Error & { data: unknown }).data)).toEqual({
      ok: false,
      code: "INTERNAL_ERROR",
      message: "Realtime delivery is temporarily unavailable",
      retryable: true,
    });
    expect(socket.connected).toBe(false);
  } finally {
    socket.disconnect();
  }
}

interface OperationalResponse {
  status: number;
  contentType: string;
  body: string;
}

async function operationalRequest(
  api: ApiInstance,
  path: string,
  authorization?: string,
): Promise<OperationalResponse> {
  const response = await fetch(
    `${api.url}${path}`,
    authorization === undefined ? undefined : { headers: { authorization } },
  );
  return {
    status: response.status,
    contentType: response.headers.get("content-type") ?? "",
    body: await response.text(),
  };
}

function metricValue(body: string, series: string): number {
  const line = body.split("\n").find((candidate) => candidate.startsWith(`${series} `));
  if (line === undefined) return 0;
  const value = Number(line.slice(series.length + 1));
  if (!Number.isFinite(value)) throw new Error(`Invalid metric value for ${series}`);
  return value;
}

async function scrapeMetrics(api: ApiInstance): Promise<OperationalResponse> {
  return operationalRequest(api, "/metrics", `Bearer ${metricsBearerToken}`);
}

async function waitForMetricValue(
  api: ApiInstance,
  series: string,
  expected: number,
): Promise<OperationalResponse> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const scrape = await scrapeMetrics(api);
    if (metricValue(scrape.body, series) === expected) return scrape;
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
  const finalScrape = await scrapeMetrics(api);
  throw new Error(
    `Metric ${series} did not reach ${expected}; observed ${metricValue(finalScrape.body, series)}`,
  );
}

function expectFixedMetricCatalog(body: string): void {
  const knownNames = new Set(Object.keys(METRIC_CATALOG));
  for (const line of body.split("\n")) {
    if (line === "" || line.startsWith("#")) continue;
    const metricToken = line.split(" ", 1)[0] ?? "";
    const brace = metricToken.indexOf("{");
    const renderedName = brace === -1 ? metricToken : metricToken.slice(0, brace);
    const catalogName = renderedName.replace(/_(?:bucket|sum|count)$/, "");
    expect(knownNames.has(catalogName)).toBe(true);
    if (brace === -1) continue;
    const labels = metricToken.slice(brace + 1, -1);
    for (const label of labels.matchAll(/(?:^|,)([a-z_]+)="((?:\\.|[^"\\])*)"/g)) {
      const [, key, escapedValue] = label;
      if (key === "le") {
        expect(renderedName.endsWith("_bucket")).toBe(true);
        continue;
      }
      const definition = METRIC_CATALOG[catalogName as keyof typeof METRIC_CATALOG];
      expect(Object.hasOwn(definition.labels, key!)).toBe(true);
      const values = definition.labels[key! as keyof typeof definition.labels] as
        | readonly string[]
        | undefined;
      expect(values).toContain(escapedValue);
    }
  }
}

function apiDiagnostics(evidence: ApiEvidence): unknown {
  return {
    label: evidence.label,
    lifecycle: evidence.lifecycle.entries.slice(-10),
    handled: evidence.handled.entries.slice(-10).map(({ redisEntryId, envelope }) => ({
      redisEntryId,
      eventId: envelope.eventId,
      boardId: envelope.boardId,
      deliverySeq: envelope.deliverySeq,
    })),
    cursorAdvances: evidence.cursorAdvances.entries.slice(-10),
    cursor: evidence.consumer?.lastHandledCursor,
    boardCapacity: evidence.consumer?.boardStateCapacityDiagnostics,
    readCursors: evidence.transport?.readCursors.slice(-10),
    watchdogLifecycle: evidence.watchdogLifecycle.entries.slice(-10),
    watchdog: evidence.watchdog?.diagnostics,
  };
}

async function createApiInstance(label: string, streamKey: string): Promise<ApiInstance> {
  const pool = createPool(requireDatabaseUrl());
  const evidence: ApiEvidence = {
    label,
    lifecycle: new Journal(),
    handled: new Journal(),
    cursorAdvances: new Journal(),
    quarantines: new Journal(),
    watchdogLifecycle: new Journal(),
    transport: undefined,
    consumer: undefined,
    runtime: undefined,
    runtimeObserver: undefined,
    watchdog: undefined,
    watchdogScheduler: undefined,
    environment: undefined,
    rejectedBeforeEstablished: false,
    failMetricsSnapshot: false,
    recoveryFailures: new Map(),
  };
  const createRuntime: DeliveryRuntimeFactory = (handlers, applicationObserver) => {
    evidence.runtimeObserver = applicationObserver;
    const runtime = new ApiDeliveryRuntime({
      createConsumer: (callbacks: DeliveryConsumerCallbacks) => {
        const transport = new AuditedRedisTransport(
          new RedisDeliveryConsumerTransport(redisUrl, streamKey),
        );
        const hooks: DeliveryConsumerHooks = {
          afterCursorAdvance: (entryId) => {
            evidence.cursorAdvances.push(entryId);
            return Promise.resolve();
          },
        };
        const consumer = new RedisDeliveryConsumer(
          transport,
          {
            deliver: async (context) => {
              evidence.handled.push(context);
              await callbacks.deliver(context);
            },
            quarantine: (event) => callbacks.quarantine(event),
            lifecycle: (event) => callbacks.lifecycle(event),
          },
          { ...defaultDeliveryConsumerConfiguration },
          systemDeliveryConsumerScheduler,
          hooks,
        );
        evidence.transport = transport;
        evidence.consumer = consumer;
        return consumer;
      },
      handlers,
      observer: {
        lifecycle: async (event) => {
          evidence.lifecycle.push(event);
          await applicationObserver.lifecycle(event);
        },
        quarantine: async (event) => {
          evidence.quarantines.push(event);
          await applicationObserver.quarantine(event);
        },
      },
    });
    evidence.runtime = runtime;
    return runtime;
  };
  let context: AppContext | undefined;
  try {
    context = await buildApp(environment(), pool, auth, {
      deliveryMode: {
        mode: "distributed",
        createRuntime,
      },
    });
    await withDeadline(
      context.app.listen({ host: "127.0.0.1", port: 0 }),
      `${label} API startup`,
      () => apiDiagnostics(evidence),
    );
    await withDeadline(
      evidence.lifecycle.waitFor(({ state }) => state === "established"),
      `${label} consumer establishment`,
      () => apiDiagnostics(evidence),
    );
    const address = context.app.server.address() as AddressInfo;
    let closePromise: Promise<void> | undefined;
    return {
      evidence,
      context,
      pool,
      url: `http://127.0.0.1:${address.port}`,
      close: () => {
        closePromise ??= (async () => {
          try {
            await context!.app.close();
          } finally {
            await pool.end();
          }
        })();
        return closePromise;
      },
    };
  } catch (error) {
    await context?.app.close().catch(() => undefined);
    await pool.end();
    throw error;
  }
}

async function createActivatedApiInstance(label: string, streamKey: string): Promise<ApiInstance> {
  const port = await allocatePort();
  const apiEnvironment = activatedEnvironment(streamKey, port);
  const pool = createPool(requireDatabaseUrl());
  const scheduler = new ControlledWatchdogScheduler();
  const evidence: ApiEvidence = {
    label,
    lifecycle: new Journal(),
    handled: new Journal(),
    cursorAdvances: new Journal(),
    quarantines: new Journal(),
    watchdogLifecycle: new Journal(),
    transport: undefined,
    consumer: undefined,
    runtime: undefined,
    runtimeObserver: undefined,
    watchdog: undefined,
    watchdogScheduler: scheduler,
    environment: apiEnvironment,
    rejectedBeforeEstablished: false,
    failMetricsSnapshot: false,
    recoveryFailures: new Map(),
  };
  const factories = {
    createTransport: (url: string, key: string) => {
      const transport = new AuditedRedisTransport(
        new RedisDeliveryConsumerTransport(url, key),
        true,
      );
      evidence.transport = transport;
      return transport;
    },
    createConsumer: (transport, callbacks, configuration, telemetry) => {
      const consumer = new RedisDeliveryConsumer(
        transport,
        {
          deliver: async (context) => {
            evidence.handled.push(context);
            await callbacks.deliver(context);
          },
          quarantine: async (event) => {
            evidence.quarantines.push(event);
            await callbacks.quarantine(event);
          },
          lifecycle: (event) => callbacks.lifecycle(event),
        },
        configuration,
        systemDeliveryConsumerScheduler,
        {
          afterCursorAdvance: (entryId) => {
            evidence.cursorAdvances.push(entryId);
            return Promise.resolve();
          },
        },
        telemetry,
      );
      evidence.consumer = consumer;
      return consumer;
    },
    createRuntime: (options: ApiDeliveryRuntimeOptions) => {
      evidence.runtimeObserver = options.observer;
      const runtime = new ApiDeliveryRuntime({
        ...options,
        observer: {
          lifecycle: async (event) => {
            evidence.lifecycle.push(event);
            await options.observer.lifecycle(event);
          },
          quarantine: (event) => options.observer.quarantine(event),
        },
      });
      evidence.runtime = runtime;
      return runtime;
    },
    createWatchdog: (input, configuration) => {
      const watchdog = new BoardDeliveryHeadWatchdog(
        input.repository,
        input.activeBoards,
        input.deliveryProgress,
        {
          lifecycle: async (event) => {
            evidence.watchdogLifecycle.push(event);
            await input.observer.lifecycle(event);
          },
        },
        configuration,
        scheduler,
      );
      evidence.watchdog = watchdog;
      return watchdog;
    },
  } satisfies DistributedDeliveryStackFactories;
  let context: AppContext | undefined;
  const dependencies: ApiServerDependencies = {
    createDatabasePool: () => pool,
    createAuthentication: () => auth,
    createDeliveryOptions: (parsed) => configuredDeliveryBuildOptions(parsed, factories),
    buildApplication: async (parsed, database, authentication, options) => {
      const recorder = options.telemetry;
      if (!(recorder instanceof InMemoryTelemetryRecorder))
        throw new TypeError("Activated API did not receive its owned telemetry recorder");
      context = await buildApp(parsed, database, authentication, {
        ...options,
        recoveryMaterialRepository: {
          load: (boardId) =>
            new BoardRecoveryService(new BoardRecoveryMaterialRepository(database)).load(boardId),
          loadWithOutcome: (boardId) => {
            const failure = evidence.recoveryFailures.get(boardId);
            if (failure === "blocked")
              return Promise.reject(new BoardRecoveryError("SNAPSHOT_CORRUPT"));
            if (failure === "retryable")
              return Promise.reject(new Error("controlled retryable recovery failure"));
            return new BoardRecoveryService(
              new BoardRecoveryMaterialRepository(database),
            ).loadWithOutcome(boardId);
          },
        },
        telemetrySnapshot: () => {
          if (evidence.failMetricsSnapshot) throw new Error("controlled snapshot failure");
          return recorder.snapshot();
        },
      });
      return context;
    },
  };
  let server: ApiServerOwner | undefined;
  try {
    server = await createApiServer(apiEnvironment, dependencies);
    const url = `http://127.0.0.1:${port}`;
    await expectSocketConnectionRejected(url, tokens.owner);
    evidence.rejectedBeforeEstablished = true;
    await withDeadline(server.listen(), `${label} activated API startup`, () =>
      apiDiagnostics(evidence),
    );
    if (!context) throw new Error(`${label} activated API context was not constructed`);
    let closePromise: Promise<void> | undefined;
    return {
      evidence,
      context,
      pool,
      url,
      server,
      close: () => {
        closePromise ??= server!.close();
        return closePromise;
      },
    };
  } catch (error) {
    if (server) await server.close().catch(() => undefined);
    else await pool.end().catch(() => undefined);
    throw error;
  }
}

class TestTopology {
  readonly streamKey: string;
  readonly repository = new BoardRepository(requireAssertionPool());
  readonly sockets = new Set<TestSocket>();
  readonly apis = new Set<ApiInstance>();
  readonly workerPool = createPool(requireDatabaseUrl());
  readonly workerRepository = new OutboxRepository(this.workerPool);
  readonly workerStream: RedisDeliveryStream;
  readonly worker: OutboxWorker;
  readonly workerLifecycle: WorkerProcessLifecycle;
  apiA!: ApiInstance;
  apiB!: ApiInstance;
  private closePromise: Promise<void> | undefined;

  private constructor(hooks: OutboxWorkerHooks, namespace: TestNamespace) {
    this.streamKey = uniqueStreamKey(namespace);
    this.workerStream = new RedisDeliveryStream(
      redisUrl,
      this.streamKey,
      100_000,
      24 * 60 * 60 * 1000,
      logger,
    );
    this.worker = new OutboxWorker(
      this.workerRepository,
      this.workerStream,
      workerConfiguration,
      logger,
      undefined,
      hooks,
    );
    this.workerLifecycle = new WorkerProcessLifecycle(
      this.worker,
      this.workerStream,
      this.workerPool,
      10_000,
      logger,
    );
  }

  static async create(
    hooks: OutboxWorkerHooks = {},
    namespace: TestNamespace = "m24b",
    activated = false,
  ): Promise<TestTopology> {
    const topology = new TestTopology(hooks, namespace);
    activeTopologies.add(topology);
    try {
      await topology.workerStream.connect();
      const createInstance = activated ? createActivatedApiInstance : createApiInstance;
      [topology.apiA, topology.apiB] = await Promise.all([
        createInstance("api-a", topology.streamKey),
        createInstance("api-b", topology.streamKey),
      ]);
      topology.apis.add(topology.apiA);
      topology.apis.add(topology.apiB);
      return topology;
    } catch (error) {
      await topology.close();
      throw error;
    }
  }

  static createActivated(): Promise<TestTopology> {
    return TestTopology.create({}, "m28-api", true);
  }

  diagnostics(): unknown {
    return {
      streamKey: this.streamKey,
      apiA: this.apiA ? apiDiagnostics(this.apiA.evidence) : undefined,
      apiB: this.apiB ? apiDiagnostics(this.apiB.evidence) : undefined,
    };
  }

  async createBoard(includeEditor = true, includeViewer = false): Promise<string> {
    const snapshot = await this.repository.createBoard(
      identities.owner.id,
      `m24b-${crypto.randomUUID()}`,
    );
    if (includeEditor)
      await requireAssertionPool().query(
        "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
        [snapshot.id, identities.editor.id],
      );
    if (includeViewer)
      await requireAssertionPool().query(
        "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'viewer')",
        [snapshot.id, identities.viewer.id],
      );
    return snapshot.id;
  }

  async connect(
    api: ApiInstance,
    token: string,
    boardId: string,
    lastAppliedSeq = 0,
    initialState: BoardState = emptyBoardState(),
  ): Promise<{ client: ClientProbe; joined: JoinBoardAck }> {
    const socket = createTestSocket(api.url, token);
    this.sockets.add(socket);
    await withDeadline(
      new Promise<void>((resolve, reject) => {
        socket.once("connect", () => resolve());
        socket.once("connect_error", reject);
        socket.connect();
      }),
      `${api.evidence.label} socket connection`,
      () => this.diagnostics(),
    );
    const client = new ClientProbe(socket, initialState);
    const joined = await withDeadline(
      new Promise<JoinBoardAck>((resolve) =>
        socket.emit(
          "board:join",
          {
            schemaVersion: 1,
            boardId,
            clientId: crypto.randomUUID(),
            lastAppliedSeq,
          },
          resolve,
        ),
      ),
      `${api.evidence.label} board join`,
      () => this.diagnostics(),
    );
    return { client, joined };
  }

  submit(client: ClientProbe, command: DurableCommand): Promise<OperationAck> {
    return withDeadline(
      new Promise<OperationAck>((resolve) =>
        client.socket.emit("operation:submit", command, resolve),
      ),
      `operation acknowledgement ${command.opId}`,
      () => this.diagnostics(),
    );
  }

  async removeMember(api: ApiInstance, boardId: string, userId: string) {
    const response = await api.context.app.inject({
      method: "DELETE",
      url: `/v1/boards/${boardId}/members/${userId}`,
      headers: testAuthorizationHeaders(tokens.owner),
    });
    expect(response.statusCode).toBe(200);
    return removeBoardMemberResponseSchema.parse(response.json());
  }

  runWorkerCycle(): Promise<WorkerCycleResult> {
    return withDeadline(this.worker.runCycle(), "worker publication cycle", () =>
      this.diagnostics(),
    );
  }

  waitForClientOperation(client: ClientProbe, operationId: string): Promise<CommittedOperation> {
    return withDeadline(
      client.operations.waitFor(({ opId }) => opId === operationId),
      `client delivery ${operationId}`,
      () => this.diagnostics(),
    );
  }

  waitForRevocation(client: ClientProbe, boardId: string): Promise<BoardAccessRevokedEvent> {
    return withDeadline(
      client.revocations.waitFor((event) => event.boardId === boardId),
      `client revocation ${boardId}`,
      () => this.diagnostics(),
    );
  }

  waitForHandled(api: ApiInstance, eventId: string): Promise<DeliveryContext> {
    return withDeadline(
      api.evidence.handled.waitFor(({ envelope }) => envelope.eventId === eventId),
      `${api.evidence.label} handling ${eventId}`,
      () => this.diagnostics(),
    );
  }

  waitForCursor(api: ApiInstance, redisEntryId: string): Promise<string> {
    return withDeadline(
      api.evidence.cursorAdvances.waitFor((entryId) => entryId === redisEntryId),
      `${api.evidence.label} cursor ${redisEntryId}`,
      () => this.diagnostics(),
    );
  }

  waitForReadEvidence(api: ApiInstance, redisEntryId: string): Promise<string> {
    const received = api.evidence.transport?.receivedEntryIds;
    if (!received) throw new Error(`Missing ${api.evidence.label} transport evidence`);
    return withDeadline(
      received.waitFor((entryId) => entryId === redisEntryId),
      `${api.evidence.label} XREAD evidence ${redisEntryId}`,
      () => this.diagnostics(),
    );
  }

  waitForLifecycle(
    api: ApiInstance,
    predicate: (event: DeliveryRuntimeLifecycleEvent) => boolean,
    label: string,
  ): Promise<DeliveryRuntimeLifecycleEvent> {
    return withDeadline(api.evidence.lifecycle.waitFor(predicate), label, () => this.diagnostics());
  }

  waitForWatchdog(
    api: ApiInstance,
    predicate: (event: BoardDeliveryHeadWatchdogLifecycleEvent) => boolean,
    label: string,
  ): Promise<BoardDeliveryHeadWatchdogLifecycleEvent> {
    return withDeadline(api.evidence.watchdogLifecycle.waitFor(predicate), label, () =>
      this.diagnostics(),
    );
  }

  runWatchdogChecks(): Promise<void> {
    const checks = [this.apiA, this.apiB].map((api) => {
      const watchdog = api.evidence.watchdog;
      if (!watchdog) throw new Error(`Missing ${api.evidence.label} watchdog`);
      return watchdog.runCheck();
    });
    return withDeadline(
      Promise.all(checks).then(() => undefined),
      "watchdog checks",
      () => this.diagnostics(),
    );
  }

  async elapseWatchdogClock(milliseconds: number): Promise<void> {
    for (const api of [this.apiA, this.apiB]) {
      const scheduler = api.evidence.watchdogScheduler;
      if (!scheduler) throw new Error(`Missing ${api.evidence.label} watchdog scheduler`);
      await scheduler.elapse(milliseconds);
    }
  }

  async restartApiB(): Promise<ApiInstance> {
    await this.apiB.close();
    const replacement = await createApiInstance("api-b-restart", this.streamKey);
    this.apis.add(replacement);
    this.apiB = replacement;
    return replacement;
  }

  close(): Promise<void> {
    this.closePromise ??= this.closeOnce();
    return this.closePromise;
  }

  private async closeOnce(): Promise<void> {
    for (const socket of this.sockets) socket.disconnect();
    this.sockets.clear();
    await Promise.allSettled([...this.apis].map((api) => api.close()));
    await this.workerLifecycle.shutdown("PROCESS_END");
    if (requireRedis().isOpen) await requireRedis().sendCommand(["DEL", this.streamKey]);
    ownedStreamKeys.delete(this.streamKey);
    activeTopologies.delete(this);
  }
}

async function operationEvidence(operationId: string): Promise<OperationEvidence> {
  const result = await requireAssertionPool().query<{
    event_id: string;
    board_id: string;
    delivery_seq: string;
    canvas_seq: string;
    user_id: string;
    status: string;
    redis_entry_id: string | null;
    payload: unknown;
  }>(
    `SELECT operation.event_id, operation.board_id, operation.delivery_seq,
            event.canvas_seq, operation.user_id, event.status, event.redis_entry_id, event.payload
     FROM board_operations operation
     JOIN outbox_events event ON event.id = operation.event_id
     WHERE operation.op_id = $1`,
    [operationId],
  );
  const row = result.rows[0];
  if (!row) throw new Error(`Missing durable operation evidence for ${operationId}`);
  const payload = deliveryEnvelopeSchema.parse(row.payload);
  if (payload.eventType !== "operation.committed")
    throw new Error(`Unexpected delivery event type for ${operationId}`);
  return {
    eventId: row.event_id,
    boardId: row.board_id,
    deliverySeq: Number(row.delivery_seq),
    canvasSeq: Number(row.canvas_seq),
    actorId: row.user_id,
    status: row.status,
    redisEntryId: row.redis_entry_id,
    payload,
  };
}

async function revocationEvidence(eventId: string): Promise<RevocationEvidence> {
  const result = await requireAssertionPool().query<{
    event_id: string;
    board_id: string;
    delivery_seq: string;
    status: string;
    redis_entry_id: string | null;
    payload: unknown;
  }>(
    `SELECT id AS event_id, board_id, delivery_seq, status, redis_entry_id, payload
     FROM outbox_events WHERE id = $1 AND event_type = 'board.membership.revoked'`,
    [eventId],
  );
  const row = result.rows[0];
  if (!row) throw new Error(`Missing durable revocation evidence for ${eventId}`);
  const payload = deliveryEnvelopeSchema.parse(row.payload);
  if (payload.eventType !== "board.membership.revoked")
    throw new Error(`Unexpected delivery event type for ${eventId}`);
  return {
    eventId: row.event_id,
    boardId: row.board_id,
    deliverySeq: Number(row.delivery_seq),
    status: row.status,
    redisEntryId: row.redis_entry_id,
    payload,
  };
}

async function appendDuplicate(streamKey: string, envelope: DeliveryEnvelope): Promise<string> {
  const fields = encodeDeliveryStreamFields(envelope);
  const result = await requireRedis().sendCommand([
    "XADD",
    streamKey,
    "*",
    "schemaVersion",
    fields.schemaVersion,
    "eventId",
    fields.eventId,
    "boardId",
    fields.boardId,
    "deliverySeq",
    fields.deliverySeq,
    "eventType",
    fields.eventType,
    "event",
    fields.event,
  ]);
  if (typeof result !== "string") throw new Error("Redis duplicate XADD omitted its entry ID");
  return result;
}

async function consumerGroups(streamKey: string): Promise<unknown[]> {
  const groups = await requireRedis().sendCommand(["XINFO", "GROUPS", streamKey]);
  if (!Array.isArray(groups)) throw new Error("Redis returned invalid consumer-group evidence");
  return groups;
}

async function streamContainsEvent(streamKey: string, eventId: string): Promise<boolean> {
  const entries = await requireRedis().sendCommand(["XRANGE", streamKey, "-", "+"]);
  return JSON.stringify(entries).includes(eventId);
}

async function catchUp(
  api: ApiInstance,
  boardId: string,
  token: string,
  after: number,
  watermark: number,
): Promise<CommittedOperation[]> {
  const operations: CommittedOperation[] = [];
  let cursor = after;
  while (cursor < watermark) {
    const response = await api.context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/operations?after=${cursor}&watermark=${watermark}`,
      headers: testAuthorizationHeaders(token),
    });
    expect(response.statusCode).toBe(200);
    const batch = operationRangeResponseSchema.parse(response.json());
    operations.push(...batch.operations);
    cursor = batch.nextSeq;
    if (!batch.hasMore) break;
  }
  expect(cursor).toBe(watermark);
  return operations;
}

function stateFromSnapshot(snapshot: BoardSnapshot): BoardState {
  const state = emptyBoardState();
  state.lastSeq = snapshot.lastSeq;
  for (const object of snapshot.objects) {
    state.objects[object.id] = {
      value: object,
      createdSeq: 0,
      updatedSeq: snapshot.lastSeq,
      deletedSeq: null,
      fieldSeq: {},
    };
    state.order.push(object.id);
  }
  return state;
}

async function expectConverged(client: ClientProbe, boardId: string): Promise<void> {
  const snapshot = boardSnapshotSchema.parse(
    await new BoardRepository(requireAssertionPool()).getBoard(boardId, identities.owner.id),
  );
  const authoritative = stateFromSnapshot(snapshot);
  expect(client.state.lastSeq).toBe(snapshot.lastSeq);
  expect(visibleObjects(client.state)).toEqual(snapshot.objects);
  expect(await hashBoardState(client.state)).toBe(await hashBoardState(authoritative));
}

async function withUnhandledRejectionAudit(task: () => Promise<void>): Promise<void> {
  const unhandled: unknown[] = [];
  const record = (reason: unknown): void => {
    unhandled.push(reason);
  };
  process.on("unhandledRejection", record);
  try {
    await task();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(unhandled).toEqual([]);
  } finally {
    process.off("unhandledRejection", record);
  }
}

beforeAll(async () => {
  databaseName = `converge_m24b_${process.pid}_${crypto
    .randomUUID()
    .replaceAll("-", "")
    .slice(0, 12)}`;
  const adminUrl = new URL(sharedDatabaseUrl);
  adminUrl.pathname = "/postgres";
  adminUrl.searchParams.delete("options");
  adminPool = createPool(adminUrl.toString());
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);

  const isolatedUrl = new URL(sharedDatabaseUrl);
  isolatedUrl.pathname = `/${databaseName}`;
  isolatedUrl.searchParams.delete("options");
  isolatedDatabaseUrl = isolatedUrl.toString();
  await runFile("pnpm", ["--filter", "@converge/database", "migrate"], {
    cwd: repositoryRoot,
    env: { ...process.env, DATABASE_URL: isolatedDatabaseUrl },
    maxBuffer: 1024 * 1024,
  });
  assertionPool = createPool(isolatedDatabaseUrl);
  const migrations = await assertionPool.query<{ name: string }>(
    "SELECT name FROM converge_migrations ORDER BY name",
  );
  expect(migrations.rows.map(({ name }) => name)).toEqual([
    "0001_milestone_one.sql",
    "0002_membership_revocation_outbox.sql",
    "0003_authoritative_stacking_order.sql",
    "0004_durable_board_delivery_ordering.sql",
    "0005_complete_outbox_envelope_constraints.sql",
    "0006_leased_outbox_state_machine.sql",
    "0007_verified_board_snapshots.sql",
    "0008_snapshot_invalidation_diagnostics.sql",
    "0009_board_replay_receipts_and_recovery_floors.sql",
  ]);

  redis = createClient({ url: redisUrl });
  redis.on("error", () => undefined);
  await redis.connect();
});

afterEach(async () => {
  await Promise.allSettled([...activeTopologies].map((topology) => topology.close()));
  activeTopologies.clear();
  if (redis?.isOpen && ownedStreamKeys.size > 0)
    await redis.sendCommand(["DEL", ...ownedStreamKeys]);
  ownedStreamKeys.clear();
  if (assertionPool) await assertionPool.query("DELETE FROM boards");
});

afterAll(async () => {
  await Promise.allSettled([...activeTopologies].map((topology) => topology.close()));
  if (redis?.isOpen && ownedStreamKeys.size > 0)
    await redis.sendCommand(["DEL", ...ownedStreamKeys]);
  if (redis?.isOpen) redis.destroy();
  await assertionPool?.end();
  try {
    if (adminPool && databaseName) await adminPool.query(`DROP DATABASE "${databaseName}"`);
  } finally {
    await adminPool?.end();
  }
});

describe.sequential("M2.4B real multi-instance Redis fan-out", () => {
  it("fans one worker publication into two independent API-local rooms symmetrically", async () => {
    await withUnhandledRejectionAudit(async () => {
      const topology = await TestTopology.create();
      const boardId = await topology.createBoard();
      const { client: clientA, joined: joinedA } = await topology.connect(
        topology.apiA,
        tokens.editor,
        boardId,
      );
      const { client: clientB, joined: joinedB } = await topology.connect(
        topology.apiB,
        tokens.owner,
        boardId,
      );
      expect(joinedA).toEqual({ ok: true, boardId, joinWatermark: 0 });
      expect(joinedB).toEqual({ ok: true, boardId, joinWatermark: 0 });
      expect(topology.apiA.context.io.of("/").adapter.constructor.name).toBe("Adapter");
      expect(topology.apiB.context.io.of("/").adapter.constructor.name).toBe("Adapter");
      expect(topology.apiA.context.io.of("/").adapter).not.toBe(
        topology.apiB.context.io.of("/").adapter,
      );
      const localProbe = crypto.randomUUID();
      emitLocalProbe(topology.apiA, boardId, localProbe);
      await withDeadline(
        clientA.localProbes.waitFor((value) => value === localProbe),
        "API A local-only Socket.IO probe",
        () => topology.diagnostics(),
      );
      expect(clientB.localProbes.entries).not.toContain(localProbe);

      const first = createRectangleCommand(boardId);
      const firstAck = await topology.submit(clientA, first);
      expect(firstAck).toMatchObject({ ok: true, duplicate: false, operation: { seq: 1 } });
      const pendingFirst = await operationEvidence(first.opId);
      expect(pendingFirst).toMatchObject({ status: "pending", actorId: identities.editor.id });
      expect(await streamContainsEvent(topology.streamKey, pendingFirst.eventId)).toBe(false);
      expect(clientA.operations.entries).toEqual([]);
      expect(clientB.operations.entries).toEqual([]);

      await expect(topology.runWorkerCycle()).resolves.toMatchObject({
        claimed: 1,
        outcomes: ["published"],
      });
      const publishedFirst = await operationEvidence(first.opId);
      if (!publishedFirst.redisEntryId) throw new Error("Missing first Redis publication ID");
      const [firstA, firstB, handledFirstA, handledFirstB] = await Promise.all([
        topology.waitForClientOperation(clientA, first.opId),
        topology.waitForClientOperation(clientB, first.opId),
        topology.waitForHandled(topology.apiA, publishedFirst.eventId),
        topology.waitForHandled(topology.apiB, publishedFirst.eventId),
      ]);
      expect(firstA).toEqual(firstB);
      expect(firstA).toMatchObject({ opId: first.opId, seq: 1 });
      expect(handledFirstA.redisEntryId).toBe(publishedFirst.redisEntryId);
      expect(handledFirstB.redisEntryId).toBe(publishedFirst.redisEntryId);
      expect(handledFirstA.envelope).toEqual(handledFirstB.envelope);
      expect(publishedFirst).toMatchObject({
        deliverySeq: 1,
        canvasSeq: 1,
        actorId: identities.editor.id,
        status: "published",
      });
      expect(publishedFirst.payload.occurredAt).toBe(firstA.committedAt);

      const second = createRectangleCommand(boardId);
      const secondAck = await topology.submit(clientB, second);
      expect(secondAck).toMatchObject({ ok: true, duplicate: false, operation: { seq: 2 } });
      expect(clientA.rawOperationCounts.get(second.opId)).toBeUndefined();
      expect(clientB.rawOperationCounts.get(second.opId)).toBeUndefined();
      await expect(topology.runWorkerCycle()).resolves.toMatchObject({ outcomes: ["published"] });
      const publishedSecond = await operationEvidence(second.opId);
      if (!publishedSecond.redisEntryId) throw new Error("Missing second Redis publication ID");
      const [secondA, secondB, handledSecondA, handledSecondB] = await Promise.all([
        topology.waitForClientOperation(clientA, second.opId),
        topology.waitForClientOperation(clientB, second.opId),
        topology.waitForHandled(topology.apiA, publishedSecond.eventId),
        topology.waitForHandled(topology.apiB, publishedSecond.eventId),
      ]);
      expect(secondA).toEqual(secondB);
      expect(secondA).toMatchObject({ opId: second.opId, seq: 2 });
      expect(handledSecondA.redisEntryId).toBe(publishedSecond.redisEntryId);
      expect(handledSecondB.redisEntryId).toBe(publishedSecond.redisEntryId);
      expect(publishedSecond).toMatchObject({
        deliverySeq: 2,
        canvasSeq: 2,
        actorId: identities.owner.id,
      });
      expect(clientA.rawOperationCounts).toEqual(
        new Map([
          [first.opId, 1],
          [second.opId, 1],
        ]),
      );
      expect(clientB.rawOperationCounts).toEqual(
        new Map([
          [first.opId, 1],
          [second.opId, 1],
        ]),
      );
      expect(topology.apiA.evidence.transport).not.toBe(topology.apiB.evidence.transport);
      expect(topology.apiA.evidence.consumer).not.toBe(topology.apiB.evidence.consumer);
      expect(topology.apiA.evidence.transport?.delegate).not.toBe(
        topology.apiB.evidence.transport?.delegate,
      );
      const connectionsA = topology.apiA.evidence.transport?.connectionIdentities;
      const connectionsB = topology.apiB.evidence.transport?.connectionIdentities;
      expect(connectionsA?.control).toBeDefined();
      expect(connectionsA?.reader).toBeDefined();
      expect(connectionsB?.control).toBeDefined();
      expect(connectionsB?.reader).toBeDefined();
      expect(connectionsA?.control).not.toBe(connectionsA?.reader);
      expect(connectionsB?.control).not.toBe(connectionsB?.reader);
      expect(connectionsA?.control).not.toBe(connectionsB?.control);
      expect(connectionsA?.reader).not.toBe(connectionsB?.reader);
      expect(topology.apiA.evidence.transport?.readCursors.length).toBeGreaterThan(0);
      expect(topology.apiB.evidence.transport?.readCursors.length).toBeGreaterThan(0);
      expect(topology.apiA.evidence.consumer?.lastHandledCursor).toBe(publishedSecond.redisEntryId);
      expect(topology.apiB.evidence.consumer?.lastHandledCursor).toBe(publishedSecond.redisEntryId);
      expect(await consumerGroups(topology.streamKey)).toEqual([]);
      await expectConverged(clientA, boardId);
      await expectConverged(clientB, boardId);
    });
  });

  it("returns the database acknowledgement while publication is paused", async () => {
    await withUnhandledRejectionAudit(async () => {
      const topology = await TestTopology.create();
      const boardId = await topology.createBoard();
      const { client: clientA } = await topology.connect(topology.apiA, tokens.editor, boardId);
      const { client: clientB } = await topology.connect(topology.apiB, tokens.owner, boardId);
      const command = createRectangleCommand(boardId);

      await expect(topology.submit(clientA, command)).resolves.toMatchObject({
        ok: true,
        duplicate: false,
        operation: { opId: command.opId, seq: 1 },
      });
      const pending = await operationEvidence(command.opId);
      expect(pending).toMatchObject({ status: "pending", redisEntryId: null });
      expect(await streamContainsEvent(topology.streamKey, pending.eventId)).toBe(false);
      expect(clientA.operations.entries).toEqual([]);
      expect(clientB.operations.entries).toEqual([]);

      await expect(topology.runWorkerCycle()).resolves.toMatchObject({ outcomes: ["published"] });
      const published = await operationEvidence(command.opId);
      if (!published.redisEntryId) throw new Error("Missing paused publication ID");
      await Promise.all([
        topology.waitForClientOperation(clientA, command.opId),
        topology.waitForClientOperation(clientB, command.opId),
        topology.waitForCursor(topology.apiA, published.redisEntryId),
        topology.waitForCursor(topology.apiB, published.redisEntryId),
      ]);
      const counts = await requireAssertionPool().query<{
        operation_count: string;
        outbox_count: string;
      }>(
        `SELECT
           (SELECT count(*) FROM board_operations WHERE board_id = $1) operation_count,
           (SELECT count(*) FROM outbox_events WHERE board_id = $1) outbox_count`,
        [boardId],
      );
      expect(counts.rows[0]).toEqual({ operation_count: "1", outbox_count: "1" });
    });
  });

  it("advances both cursors across duplicate Redis evidence while applying once", async () => {
    await withUnhandledRejectionAudit(async () => {
      const topology = await TestTopology.create();
      const boardId = await topology.createBoard();
      const { client: clientA } = await topology.connect(topology.apiA, tokens.editor, boardId);
      const { client: clientB } = await topology.connect(topology.apiB, tokens.owner, boardId);
      const command = createRectangleCommand(boardId);
      await topology.submit(clientA, command);
      await topology.runWorkerCycle();
      const published = await operationEvidence(command.opId);
      if (!published.redisEntryId) throw new Error("Missing original Redis publication ID");
      await Promise.all([
        topology.waitForClientOperation(clientA, command.opId),
        topology.waitForClientOperation(clientB, command.opId),
        topology.waitForCursor(topology.apiA, published.redisEntryId),
        topology.waitForCursor(topology.apiB, published.redisEntryId),
      ]);
      const before = await new BoardRepository(requireAssertionPool()).getBoard(
        boardId,
        identities.owner.id,
      );

      const duplicateId = await appendDuplicate(topology.streamKey, published.payload);
      expect(duplicateId).not.toBe(published.redisEntryId);
      await Promise.all([
        topology.waitForReadEvidence(topology.apiA, duplicateId),
        topology.waitForReadEvidence(topology.apiB, duplicateId),
        topology.waitForCursor(topology.apiA, duplicateId),
        topology.waitForCursor(topology.apiB, duplicateId),
      ]);
      expect(topology.apiA.evidence.consumer?.lastHandledCursor).toBe(duplicateId);
      expect(topology.apiB.evidence.consumer?.lastHandledCursor).toBe(duplicateId);
      expect(clientA.rawOperationCounts.get(command.opId)).toBe(1);
      expect(clientB.rawOperationCounts.get(command.opId)).toBe(1);
      expect(
        topology.apiA.evidence.handled.entries.filter(
          ({ envelope }) => envelope.eventId === published.eventId,
        ),
      ).toHaveLength(1);
      expect(
        topology.apiB.evidence.handled.entries.filter(
          ({ envelope }) => envelope.eventId === published.eventId,
        ),
      ).toHaveLength(1);
      expect(
        await new BoardRepository(requireAssertionPool()).getBoard(boardId, identities.owner.id),
      ).toEqual(before);
      expect(await consumerGroups(topology.streamKey)).toEqual([]);
    });
  });

  it("preserves same-board order while unrelated boards and the surviving API progress", async () => {
    await withUnhandledRejectionAudit(async () => {
      const paused = deferred<void>();
      const release = deferred<void>();
      const pauseTarget: { boardId: string | undefined } = { boardId: undefined };
      const topology = await TestTopology.create({
        beforeXadd: async (claim) => {
          if (claim.boardId !== pauseTarget.boardId) return;
          paused.resolve();
          await release.promise;
        },
      });
      const boardA = await topology.createBoard();
      const boardB = await topology.createBoard(false);
      const { client: clientA } = await topology.connect(topology.apiA, tokens.editor, boardA);
      const { client: clientB } = await topology.connect(topology.apiB, tokens.owner, boardA);
      const { client: boardBClient } = await topology.connect(topology.apiB, tokens.owner, boardB);
      const rapid = Array.from({ length: 3 }, () => createRectangleCommand(boardA));
      await Promise.all([
        topology.submit(clientA, rapid[0]!),
        topology.submit(clientB, rapid[1]!),
        topology.submit(clientA, rapid[2]!),
      ]);
      for (let index = 0; index < 3; index += 1)
        await expect(topology.runWorkerCycle()).resolves.toMatchObject({ outcomes: ["published"] });
      await Promise.all(
        rapid.flatMap((command) => [
          topology.waitForClientOperation(clientA, command.opId),
          topology.waitForClientOperation(clientB, command.opId),
        ]),
      );
      expect(clientA.operations.entries.map(({ seq }) => seq)).toEqual([1, 2, 3]);
      expect(clientB.operations.entries.map(({ seq }) => seq)).toEqual([1, 2, 3]);

      const laterA = createRectangleCommand(boardA);
      const independentB = createRectangleCommand(boardB);
      await Promise.all([
        topology.submit(clientB, laterA),
        topology.submit(boardBClient, independentB),
      ]);
      pauseTarget.boardId = boardA;
      const cycle = topology.runWorkerCycle();
      await withDeadline(paused.promise, "paused board-A XADD", () => topology.diagnostics());
      await topology.waitForClientOperation(boardBClient, independentB.opId);
      const boardBEvidence = await operationEvidence(independentB.opId);
      await Promise.all([
        topology.waitForHandled(topology.apiA, boardBEvidence.eventId),
        topology.waitForHandled(topology.apiB, boardBEvidence.eventId),
      ]);
      expect(clientA.operations.entries.some(({ boardId }) => boardId === boardB)).toBe(false);
      release.resolve();
      const cycleResult = await cycle;
      expect(cycleResult.claimed).toBe(2);
      expect(cycleResult.outcomes).toEqual(["published", "published"]);
      await Promise.all([
        topology.waitForClientOperation(clientA, laterA.opId),
        topology.waitForClientOperation(clientB, laterA.opId),
      ]);
      expect(clientA.operations.entries.map(({ seq }) => seq)).toEqual([1, 2, 3, 4]);
      expect(clientB.operations.entries.map(({ seq }) => seq)).toEqual([1, 2, 3, 4]);

      const stoppedCursor = topology.apiA.evidence.consumer?.lastHandledCursor;
      await topology.apiA.evidence.runtime?.stop();
      const surviving = createRectangleCommand(boardA);
      await topology.submit(clientB, surviving);
      await topology.runWorkerCycle();
      const survivingEvidence = await operationEvidence(surviving.opId);
      if (!survivingEvidence.redisEntryId) throw new Error("Missing surviving publication ID");
      await Promise.all([
        topology.waitForClientOperation(clientB, surviving.opId),
        topology.waitForHandled(topology.apiB, survivingEvidence.eventId),
      ]);
      expect(clientA.rawOperationCounts.get(surviving.opId)).toBeUndefined();
      expect(topology.apiA.evidence.consumer?.lastHandledCursor).toBe(stoppedCursor);
      expect(topology.apiB.evidence.consumer?.lastHandledCursor).toBe(
        survivingEvidence.redisEntryId,
      );
    });
  });

  it("restarts from the current tail and converges offline state through PostgreSQL range catch-up", async () => {
    await withUnhandledRejectionAudit(async () => {
      const topology = await TestTopology.create();
      const boardId = await topology.createBoard();
      const { client: clientA } = await topology.connect(topology.apiA, tokens.editor, boardId);
      const { client: clientB } = await topology.connect(topology.apiB, tokens.owner, boardId);
      const first = createRectangleCommand(boardId);
      await topology.submit(clientA, first);
      await topology.runWorkerCycle();
      await Promise.all([
        topology.waitForClientOperation(clientA, first.opId),
        topology.waitForClientOperation(clientB, first.opId),
      ]);
      const offlineState = structuredClone(clientB.state);

      await topology.apiB.close();
      const missed = [createRectangleCommand(boardId), createRectangleCommand(boardId)];
      for (const command of missed) {
        await topology.submit(clientA, command);
        await topology.runWorkerCycle();
        await topology.waitForClientOperation(clientA, command.opId);
      }
      const lastMissed = await operationEvidence(missed.at(-1)!.opId);
      if (!lastMissed.redisEntryId) throw new Error("Missing offline publication ID");

      const restarted = await topology.restartApiB();
      expect(restarted.evidence.consumer?.lastHandledCursor).toBe(lastMissed.redisEntryId);
      expect(restarted.evidence.handled.entries).toEqual([]);
      const { client: reconnected, joined } = await topology.connect(
        restarted,
        tokens.owner,
        boardId,
        offlineState.lastSeq,
        offlineState,
      );
      expect(joined).toEqual({ ok: true, boardId, joinWatermark: 3 });
      const recovered = await catchUp(restarted, boardId, tokens.owner, offlineState.lastSeq, 3);
      expect(recovered.map(({ seq }) => seq)).toEqual([2, 3]);
      reconnected.applyCatchUp(recovered);
      expect(restarted.evidence.handled.entries).toEqual([]);
      await expectConverged(reconnected, boardId);
      await expectConverged(clientA, boardId);
      expect(await consumerGroups(topology.streamKey)).toEqual([]);
    });
  });

  it("isolates one API-local handler failure without advancing its cursor", async () => {
    await withUnhandledRejectionAudit(async () => {
      const topology = await TestTopology.create();
      const boardId = await topology.createBoard();
      const { client: clientA } = await topology.connect(topology.apiA, tokens.editor, boardId);
      const { client: clientB } = await topology.connect(topology.apiB, tokens.owner, boardId);
      const failedCursor = topology.apiA.evidence.consumer?.lastHandledCursor;
      const broadcast = vi
        .spyOn(topology.apiA.context.io.of("/").adapter, "broadcast")
        .mockImplementation(() => {
          throw new Error("injected API-local Socket.IO publication failure");
        });
      try {
        const command = createRectangleCommand(boardId);
        await topology.submit(clientB, command);
        await topology.runWorkerCycle();
        const published = await operationEvidence(command.opId);
        if (!published.redisEntryId) throw new Error("Missing failure-isolation publication ID");
        await Promise.all([
          topology.waitForClientOperation(clientB, command.opId),
          topology.waitForHandled(topology.apiB, published.eventId),
          withDeadline(
            topology.apiA.evidence.lifecycle.waitFor(
              (event) =>
                event.state === "terminal" &&
                event.source === "consumer" &&
                event.code === "DELIVERY_CALLBACK_FAILED",
            ),
            "API A terminal handler failure",
            () => topology.diagnostics(),
          ),
        ]);
        expect(clientA.rawOperationCounts.get(command.opId)).toBeUndefined();
        expect(topology.apiA.evidence.consumer?.lastHandledCursor).toBe(failedCursor);
        expect(topology.apiB.evidence.consumer?.lastHandledCursor).toBe(published.redisEntryId);
        expect(published.status).toBe("published");
        await expectConverged(clientB, boardId);

        const firstRuntimeStop = topology.apiA.evidence.runtime?.stop();
        const secondRuntimeStop = topology.apiA.evidence.runtime?.stop();
        expect(secondRuntimeStop).toBe(firstRuntimeStop);
        await firstRuntimeStop;
        const firstConsumerStop = topology.apiA.evidence.consumer?.stop();
        const secondConsumerStop = topology.apiA.evidence.consumer?.stop();
        expect(secondConsumerStop).toBe(firstConsumerStop);
        await firstConsumerStop;
        expect(topology.apiA.evidence.transport?.closeCalls).toBe(1);
        const firstWorkerStop = topology.workerLifecycle.shutdown("PROCESS_END");
        const secondWorkerStop = topology.workerLifecycle.shutdown("PROCESS_END");
        expect(secondWorkerStop).toBe(firstWorkerStop);
        await firstWorkerStop;
      } finally {
        broadcast.mockRestore();
      }
    });
  });
});

describe.sequential("M2.5 distributed membership revocation across API replicas", () => {
  it("revokes matching local sockets on both APIs in board order and suppresses duplicates", async () => {
    await withUnhandledRejectionAudit(async () => {
      const revocationPaused = deferred<void>();
      const releaseRevocation = deferred<void>();
      const pauseTarget: { eventId: string | undefined } = { eventId: undefined };
      const topology = await TestTopology.create(
        {
          beforeXadd: async (claim) => {
            if (claim.eventId !== pauseTarget.eventId) return;
            revocationPaused.resolve();
            await releaseRevocation.promise;
          },
        },
        "m25",
      );
      const boardA = await topology.createBoard(true, true);
      const boardB = await topology.createBoard();
      const { client: owner } = await topology.connect(topology.apiA, tokens.owner, boardA);
      const { client: editorA } = await topology.connect(topology.apiA, tokens.editor, boardA);
      const { client: editorB } = await topology.connect(topology.apiB, tokens.editor, boardA);
      const { client: viewer } = await topology.connect(topology.apiB, tokens.viewer, boardA);
      const { client: editorOtherBoard } = await topology.connect(
        topology.apiA,
        tokens.editor,
        boardB,
      );

      const beforeRevocation = createRectangleCommand(boardA);
      await topology.submit(owner, beforeRevocation);
      await topology.runWorkerCycle();
      await Promise.all([
        topology.waitForClientOperation(editorA, beforeRevocation.opId),
        topology.waitForClientOperation(editorB, beforeRevocation.opId),
        topology.waitForClientOperation(viewer, beforeRevocation.opId),
      ]);

      const removal = await topology.removeMember(topology.apiA, boardA, identities.editor.id);
      expect(removal).toMatchObject({ removed: true, boardId: boardA });
      if (!removal.eventId) throw new Error("Expected a committed revocation event ID");
      const pendingRevocation = await revocationEvidence(removal.eventId);
      pauseTarget.eventId = removal.eventId;
      expect(pendingRevocation).toMatchObject({ status: "pending", deliverySeq: 2 });
      expect(roomHas(topology.apiA, boardA, editorA)).toBe(true);
      expect(roomHas(topology.apiB, boardA, editorB)).toBe(true);
      expect(editorA.revocations.entries).toEqual([]);
      expect(editorB.revocations.entries).toEqual([]);

      const { joined: postCommitJoin } = await topology.connect(
        topology.apiB,
        tokens.editor,
        boardA,
      );
      expect(postCommitJoin).toMatchObject({ ok: false, code: "FORBIDDEN" });

      const afterRevocation = createRectangleCommand(boardA);
      await topology.submit(owner, afterRevocation);
      const pendingOperation = await operationEvidence(afterRevocation.opId);
      expect(pendingOperation).toMatchObject({ status: "pending", deliverySeq: 3 });
      const otherBoardOperation = createRectangleCommand(boardB);
      await topology.submit(editorOtherBoard, otherBoardOperation);

      const publicationCycle = topology.runWorkerCycle();
      await withDeadline(revocationPaused.promise, "paused revocation XADD", () =>
        topology.diagnostics(),
      );
      await topology.waitForClientOperation(editorOtherBoard, otherBoardOperation.opId);
      expect(roomHas(topology.apiA, boardA, editorA)).toBe(true);
      expect(roomHas(topology.apiB, boardA, editorB)).toBe(true);
      releaseRevocation.resolve();
      const publicationResult = await publicationCycle;
      expect(publicationResult.claimed).toBe(2);
      expect(publicationResult.outcomes).toEqual(["published", "published"]);
      const publishedRevocation = await revocationEvidence(removal.eventId);
      if (!publishedRevocation.redisEntryId)
        throw new Error("Expected a published revocation Redis entry ID");
      const [revokedA, revokedB, handledA, handledB] = await Promise.all([
        topology.waitForRevocation(editorA, boardA),
        topology.waitForRevocation(editorB, boardA),
        topology.waitForHandled(topology.apiA, removal.eventId),
        topology.waitForHandled(topology.apiB, removal.eventId),
      ]);
      expect(revokedA).toEqual(revokedB);
      expect(revokedA).toEqual({
        schemaVersion: 1,
        boardId: boardA,
        code: "ACCESS_REVOKED",
        message: "Board access was revoked",
      });
      expect(handledA.redisEntryId).toBe(publishedRevocation.redisEntryId);
      expect(handledB.redisEntryId).toBe(publishedRevocation.redisEntryId);
      expect(roomHas(topology.apiA, boardA, editorA)).toBe(false);
      expect(roomHas(topology.apiB, boardA, editorB)).toBe(false);
      expect(roomHas(topology.apiB, boardA, viewer)).toBe(true);
      expect(roomHas(topology.apiA, boardB, editorOtherBoard)).toBe(true);
      expect(editorA.timeline).toEqual([`operation:${beforeRevocation.opId}`, `revoked:${boardA}`]);
      expect(editorB.timeline).toEqual(editorA.timeline);

      const duplicateId = await appendDuplicate(topology.streamKey, publishedRevocation.payload);
      await Promise.all([
        topology.waitForReadEvidence(topology.apiA, duplicateId),
        topology.waitForReadEvidence(topology.apiB, duplicateId),
        topology.waitForCursor(topology.apiA, duplicateId),
        topology.waitForCursor(topology.apiB, duplicateId),
      ]);
      expect(editorA.rawRevocationCounts.get(boardA)).toBe(1);
      expect(editorB.rawRevocationCounts.get(boardA)).toBe(1);
      expect(viewer.rawRevocationCounts.get(boardA)).toBeUndefined();

      await expect(topology.runWorkerCycle()).resolves.toMatchObject({
        claimed: 1,
        outcomes: ["published"],
      });
      await Promise.all([
        topology.waitForClientOperation(viewer, afterRevocation.opId),
        topology.waitForHandled(topology.apiA, pendingOperation.eventId),
        topology.waitForHandled(topology.apiB, pendingOperation.eventId),
      ]);
      expect(editorA.rawOperationCounts.get(afterRevocation.opId)).toBeUndefined();
      expect(editorB.rawOperationCounts.get(afterRevocation.opId)).toBeUndefined();

      const unrelatedHttp = await topology.apiA.context.app.inject({
        method: "GET",
        url: `/v1/boards/${boardB}`,
        headers: testAuthorizationHeaders(tokens.editor),
      });
      expect(unrelatedHttp.statusCode).toBe(200);

      const replay = await topology.removeMember(topology.apiB, boardA, identities.editor.id);
      expect(replay).toMatchObject({ removed: false, eventId: null });
      await expect(topology.runWorkerCycle()).resolves.toMatchObject({ claimed: 0, outcomes: [] });
      const revocationCount = await requireAssertionPool().query<{ count: string }>(
        `SELECT count(*) FROM outbox_events
         WHERE board_id = $1 AND event_type = 'board.membership.revoked'`,
        [boardA],
      );
      expect(revocationCount.rows[0]?.count).toBe("1");
      expect(await consumerGroups(topology.streamKey)).toEqual([]);
    });
  });

  it("lets API B enforce revocation while API A's delivery runtime is stopped", async () => {
    await withUnhandledRejectionAudit(async () => {
      const topology = await TestTopology.create({}, "m25");
      const boardId = await topology.createBoard();
      const { client: editorA } = await topology.connect(topology.apiA, tokens.editor, boardId);
      const { client: editorB } = await topology.connect(topology.apiB, tokens.editor, boardId);
      const stoppedCursor = topology.apiA.evidence.consumer?.lastHandledCursor;
      await topology.apiA.evidence.runtime?.stop();

      const removal = await topology.removeMember(topology.apiB, boardId, identities.editor.id);
      if (!removal.eventId) throw new Error("Expected a committed revocation event ID");
      await topology.runWorkerCycle();
      const published = await revocationEvidence(removal.eventId);
      if (!published.redisEntryId) throw new Error("Expected a revocation Redis entry ID");
      await Promise.all([
        topology.waitForRevocation(editorB, boardId),
        topology.waitForHandled(topology.apiB, removal.eventId),
      ]);

      expect(roomHas(topology.apiB, boardId, editorB)).toBe(false);
      expect(roomHas(topology.apiA, boardId, editorA)).toBe(false);
      expect(editorA.revocations.entries).toEqual([]);
      expect(topology.apiA.evidence.consumer?.lastHandledCursor).toBe(stoppedCursor);
      expect(topology.apiB.evidence.consumer?.lastHandledCursor).toBe(published.redisEntryId);
      expect(published.status).toBe("published");
    });
  });

  it("publishes nothing for rolled-back and no-op removals", async () => {
    await withUnhandledRejectionAudit(async () => {
      const topology = await TestTopology.create({}, "m25");
      const rollbackBoard = await topology.createBoard();
      const otherBoard = await topology.createBoard();
      const absentBoard = await topology.createBoard(false);
      const { client: rollbackEditor } = await topology.connect(
        topology.apiA,
        tokens.editor,
        rollbackBoard,
      );
      const { client: otherBoardEditor } = await topology.connect(
        topology.apiB,
        tokens.editor,
        otherBoard,
      );
      const rollbackRepository = new BoardRepository(requireAssertionPool(), {
        afterMembershipDelete: () => Promise.reject(new Error("forced revocation rollback")),
      });

      await expect(
        rollbackRepository.removeBoardMember(
          identities.owner.id,
          rollbackBoard,
          identities.editor.id,
        ),
      ).rejects.toThrow("forced revocation rollback");
      const noOp = await topology.removeMember(topology.apiA, absentBoard, identities.editor.id);
      expect(noOp).toMatchObject({ removed: false, eventId: null });
      await expect(topology.runWorkerCycle()).resolves.toMatchObject({ claimed: 0, outcomes: [] });

      const evidence = await requireAssertionPool().query<{
        membership_exists: boolean;
        revocation_count: string;
      }>(
        `SELECT
           EXISTS(
             SELECT 1 FROM board_members WHERE board_id = $1 AND user_id = $3
           ) membership_exists,
           (
             SELECT count(*) FROM outbox_events
             WHERE board_id = ANY($2::uuid[]) AND event_type = 'board.membership.revoked'
           ) revocation_count`,
        [rollbackBoard, [rollbackBoard, absentBoard], identities.editor.id],
      );
      expect(evidence.rows[0]).toEqual({ membership_exists: true, revocation_count: "0" });
      expect(roomHas(topology.apiA, rollbackBoard, rollbackEditor)).toBe(true);
      expect(roomHas(topology.apiB, otherBoard, otherBoardEditor)).toBe(true);
      expect(rollbackEditor.revocations.entries).toEqual([]);
      expect(otherBoardEditor.revocations.entries).toEqual([]);
      expect(await consumerGroups(topology.streamKey)).toEqual([]);
    });
  });
});

describe.sequential("M2.8 API observability acceptance under real distributed failures", () => {
  it("reports delivery, watchdog, interruption, and shutdown evidence without leaking identifiers", async () => {
    await withUnhandledRejectionAudit(async () => {
      const topology = await TestTopology.createActivated();
      const apiA = topology.apiA;
      const apiB = topology.apiB;
      expect(apiA.evidence.environment?.API_DELIVERY_MODE).toBe("distributed");
      expect(apiB.evidence.environment?.API_DELIVERY_MODE).toBe("distributed");
      expect(apiA.evidence.rejectedBeforeEstablished).toBe(true);
      expect(apiB.evidence.rejectedBeforeEstablished).toBe(true);
      expect(apiA.evidence.lifecycle.entries).toEqual([]);
      expect(apiB.evidence.lifecycle.entries).toEqual([]);

      const [liveBeforeConsumer, readyBeforeConsumer, socketBeforeConsumer] = await Promise.all([
        operationalRequest(apiA, "/health/live"),
        operationalRequest(apiA, "/health/ready"),
        operationalRequest(apiA, "/health/socket-ready"),
      ]);
      expect(liveBeforeConsumer).toMatchObject({ status: 200 });
      expect(JSON.parse(liveBeforeConsumer.body)).toEqual({ ok: true, status: "live" });
      expect(readyBeforeConsumer).toMatchObject({ status: 200 });
      expect(JSON.parse(readyBeforeConsumer.body)).toEqual({ ok: true, status: "ready" });
      expect(socketBeforeConsumer).toMatchObject({ status: 503 });

      const missingCredentials = await operationalRequest(apiA, "/metrics");
      const incorrectCredentials = await operationalRequest(
        apiA,
        "/metrics",
        "Bearer definitely-not-the-acceptance-token",
      );
      expect(missingCredentials).toEqual(incorrectCredentials);
      expect(missingCredentials.status).toBe(401);
      const initialMetrics = await scrapeMetrics(apiA);
      expect(initialMetrics.status).toBe(200);
      expect(initialMetrics.contentType).toBe(PROMETHEUS_CONTENT_TYPE);
      expect(metricValue(initialMetrics.body, "converge_delivery_consumer_ready")).toBe(0);
      expect(metricValue(initialMetrics.body, "converge_socket_ready")).toBe(0);
      expect(await scrapeMetrics(apiA)).toEqual(initialMetrics);
      expect(initialMetrics.body).not.toContain("delivery.consumer.lifecycle");
      expect(initialMetrics.body).not.toContain(metricsBearerToken);
      expectFixedMetricCatalog(initialMetrics.body);
      const recorderBeforeFailure = apiA.server?.telemetry.snapshot();
      apiA.evidence.failMetricsSnapshot = true;
      const failedMetrics = await scrapeMetrics(apiA);
      expect(failedMetrics).toMatchObject({
        status: 503,
        contentType: "text/plain; charset=utf-8",
        body: "Metrics unavailable\n",
      });
      expect(apiA.server?.telemetry.snapshot()).toEqual(recorderBeforeFailure);
      apiA.evidence.failMetricsSnapshot = false;
      expect(await scrapeMetrics(apiA)).toEqual(initialMetrics);

      apiA.evidence.transport?.releaseInitialRead();
      apiB.evidence.transport?.releaseInitialRead();
      await Promise.all([
        withDeadline(
          apiA.evidence.lifecycle.waitFor(({ state }) => state === "established"),
          "API A initial consumer establishment",
          () => topology.diagnostics(),
        ),
        withDeadline(
          apiB.evidence.lifecycle.waitFor(({ state }) => state === "established"),
          "API B initial consumer establishment",
          () => topology.diagnostics(),
        ),
      ]);
      expect(apiA.evidence.lifecycle.entries).toContainEqual({ state: "established" });
      expect(apiB.evidence.lifecycle.entries).toContainEqual({ state: "established" });
      expect((await operationalRequest(apiA, "/health/ready")).status).toBe(200);
      expect((await operationalRequest(apiA, "/health/socket-ready")).status).toBe(200);
      const establishedMetrics = await scrapeMetrics(apiA);
      expect(metricValue(establishedMetrics.body, "converge_delivery_consumer_ready")).toBe(1);
      expect(metricValue(establishedMetrics.body, "converge_socket_ready")).toBe(1);
      expect(
        metricValue(
          establishedMetrics.body,
          'converge_delivery_state_transitions_total{source="consumer",state="established"}',
        ),
      ).toBe(1);
      expect(apiA.context.io.of("/").adapter.constructor.name).toBe("Adapter");
      expect(apiB.context.io.of("/").adapter.constructor.name).toBe("Adapter");

      const boardId = await topology.createBoard(true, true);
      const otherBoardId = await topology.createBoard();
      const { client: clientA } = await topology.connect(apiA, tokens.editor, boardId);
      const { client: clientB } = await topology.connect(apiB, tokens.owner, boardId);
      const { client: otherBoardClient } = await topology.connect(
        apiA,
        tokens.editor,
        otherBoardId,
      );
      await topology.runWatchdogChecks();

      const first = createRectangleCommand(boardId);
      await expect(topology.submit(clientA, first)).resolves.toMatchObject({
        ok: true,
        duplicate: false,
        operation: { opId: first.opId, seq: 1 },
      });
      const pendingFirst = await operationEvidence(first.opId);
      expect(pendingFirst).toMatchObject({ status: "pending", redisEntryId: null });
      expect(clientA.operations.entries).toEqual([]);
      expect(clientB.operations.entries).toEqual([]);
      expect(otherBoardClient.operations.entries).toEqual([]);

      await expect(topology.runWorkerCycle()).resolves.toMatchObject({
        claimed: 1,
        outcomes: ["published"],
      });
      const publishedFirst = await operationEvidence(first.opId);
      if (!publishedFirst.redisEntryId) throw new Error("Missing activated publication ID");
      const [deliveredA, deliveredB, handledA, handledB] = await Promise.all([
        topology.waitForClientOperation(clientA, first.opId),
        topology.waitForClientOperation(clientB, first.opId),
        topology.waitForHandled(apiA, publishedFirst.eventId),
        topology.waitForHandled(apiB, publishedFirst.eventId),
      ]);
      expect(deliveredA).toEqual(deliveredB);
      expect(handledA.redisEntryId).toBe(publishedFirst.redisEntryId);
      expect(handledB.redisEntryId).toBe(publishedFirst.redisEntryId);
      expect(otherBoardClient.rawOperationCounts.get(first.opId)).toBeUndefined();
      expect(publishedFirst).toMatchObject({
        status: "published",
        canvasSeq: 1,
        deliverySeq: 1,
      });
      expect(apiA.evidence.transport).not.toBe(apiB.evidence.transport);
      expect(apiA.evidence.consumer).not.toBe(apiB.evidence.consumer);
      expect(apiA.evidence.consumer?.lastHandledCursor).toBe(publishedFirst.redisEntryId);
      expect(apiB.evidence.consumer?.lastHandledCursor).toBe(publishedFirst.redisEntryId);
      expect(await consumerGroups(topology.streamKey)).toEqual([]);
      await expectConverged(clientA, boardId);
      await expectConverged(clientB, boardId);
      const stateAfterFirst = structuredClone(clientA.state);
      const metricsAfterFirst = await waitForMetricValue(
        apiA,
        'converge_delivery_events_total{event_type="operation",outcome="handled"}',
        1,
      );
      expect(
        metricValue(
          metricsAfterFirst.body,
          'converge_delivery_events_total{event_type="operation",outcome="handled"}',
        ),
      ).toBe(1);
      for (const outcome of ["duplicate", "failed", "quarantined"])
        expect(
          metricValue(
            metricsAfterFirst.body,
            `converge_delivery_events_total{event_type="operation",outcome="${outcome}"}`,
          ),
        ).toBe(0);

      const duplicateOperationId = await appendDuplicate(
        topology.streamKey,
        publishedFirst.payload,
      );
      await Promise.all([
        topology.waitForReadEvidence(apiA, duplicateOperationId),
        topology.waitForReadEvidence(apiB, duplicateOperationId),
        topology.waitForCursor(apiA, duplicateOperationId),
        topology.waitForCursor(apiB, duplicateOperationId),
      ]);
      expect(clientA.rawOperationCounts.get(first.opId)).toBe(1);
      expect(clientB.rawOperationCounts.get(first.opId)).toBe(1);
      const metricsAfterDuplicate = await waitForMetricValue(
        apiA,
        'converge_delivery_events_total{event_type="operation",outcome="duplicate"}',
        1,
      );
      expect(
        metricValue(
          metricsAfterDuplicate.body,
          'converge_delivery_events_total{event_type="operation",outcome="handled"}',
        ),
      ).toBe(1);
      expect(
        metricValue(
          metricsAfterDuplicate.body,
          'converge_delivery_events_total{event_type="operation",outcome="duplicate"}',
        ),
      ).toBe(1);
      expect((await operationalRequest(apiA, "/health/socket-ready")).status).toBe(200);

      await new BoardRecoveryMaterialRepository(requireAssertionPool()).refresh(boardId);
      const snapshotTailRecovery = await operationalRequest(
        apiA,
        `/v1/boards/${boardId}/recovery`,
        `Bearer ${tokens.owner}`,
      );
      expect(snapshotTailRecovery.status).toBe(200);
      const refreshedBoardId = await topology.createBoard();
      const refreshedRecovery = await operationalRequest(
        apiA,
        `/v1/boards/${refreshedBoardId}/recovery`,
        `Bearer ${tokens.owner}`,
      );
      expect(refreshedRecovery.status).toBe(200);
      const blockedBoardId = await topology.createBoard();
      apiA.evidence.recoveryFailures.set(blockedBoardId, "blocked");
      const blockedRecovery = await operationalRequest(
        apiA,
        `/v1/boards/${blockedBoardId}/recovery`,
        `Bearer ${tokens.owner}`,
      );
      expect(blockedRecovery.status).toBe(409);
      expect(JSON.parse(blockedRecovery.body)).toMatchObject({ code: "RECOVERY_BLOCKED" });
      const retryableBoardId = await topology.createBoard();
      apiA.evidence.recoveryFailures.set(retryableBoardId, "retryable");
      expect(
        (
          await operationalRequest(
            apiA,
            `/v1/boards/${retryableBoardId}/recovery`,
            `Bearer ${tokens.owner}`,
          )
        ).status,
      ).toBe(500);
      expect((await operationalRequest(apiA, `/v1/boards/${boardId}/recovery`)).status).toBe(401);
      const recoveryMetrics = await scrapeMetrics(apiA);
      for (const outcome of [
        "snapshot_tail",
        "refreshed",
        "recovery_blocked",
        "retryable_failure",
        "authorization_failure",
      ])
        expect(
          metricValue(
            recoveryMetrics.body,
            `converge_recovery_requests_total{outcome="${outcome}"}`,
          ),
        ).toBe(1);
      expect(metricValue(recoveryMetrics.body, "converge_recovery_duration_seconds_count")).toBe(5);

      const paused = createRectangleCommand(boardId);
      await expect(topology.submit(clientA, paused)).resolves.toMatchObject({
        ok: true,
        operation: { opId: paused.opId, seq: 2 },
      });
      const pendingPaused = await operationEvidence(paused.opId);
      expect(pendingPaused).toMatchObject({ status: "pending", redisEntryId: null });
      expect(await streamContainsEvent(topology.streamKey, pendingPaused.eventId)).toBe(false);
      expect(clientA.rawOperationCounts.get(paused.opId)).toBeUndefined();
      expect(clientB.rawOperationCounts.get(paused.opId)).toBeUndefined();

      await topology.runWatchdogChecks();
      await topology.elapseWatchdogClock(4_999);
      await topology.runWatchdogChecks();
      expect(apiA.evidence.watchdogLifecycle.entries).toEqual([]);
      expect(apiB.evidence.watchdogLifecycle.entries).toEqual([]);
      expect(clientA.socket.connected).toBe(true);
      expect(clientB.socket.connected).toBe(true);
      expect((await operationalRequest(apiA, "/health/ready")).status).toBe(200);
      expect((await operationalRequest(apiA, "/health/socket-ready")).status).toBe(200);
      const { client: beforeDeadline, joined: beforeDeadlineJoin } = await topology.connect(
        apiB,
        tokens.viewer,
        boardId,
        1,
        structuredClone(stateAfterFirst),
      );
      expect(beforeDeadlineJoin).toMatchObject({ ok: true, joinWatermark: 2 });

      await topology.elapseWatchdogClock(1);
      await topology.runWatchdogChecks();
      await Promise.all([
        withDeadline(
          clientA.disconnects.waitFor(() => true),
          "API A watchdog disconnect",
          () => topology.diagnostics(),
        ),
        withDeadline(
          clientB.disconnects.waitFor(() => true),
          "API B watchdog disconnect",
          () => topology.diagnostics(),
        ),
        withDeadline(
          beforeDeadline.disconnects.waitFor(() => true),
          "pre-deadline watchdog disconnect",
          () => topology.diagnostics(),
        ),
      ]);
      expect(apiA.evidence.watchdogLifecycle.entries).toContainEqual({
        state: "unavailable",
        code: "DELIVERY_HEAD_DIVERGED",
        boardIds: [boardId],
      });
      expect(apiB.evidence.watchdogLifecycle.entries).toContainEqual({
        state: "unavailable",
        code: "DELIVERY_HEAD_DIVERGED",
        boardIds: [boardId],
      });
      expect((await operationalRequest(apiA, "/health/ready")).status).toBe(200);
      expect((await operationalRequest(apiA, "/health/socket-ready")).status).toBe(503);
      const divergentMetrics = await waitForMetricValue(
        apiA,
        'converge_delivery_state_transitions_total{source="watchdog",state="unavailable"}',
        1,
      );
      expect(metricValue(divergentMetrics.body, "converge_socket_ready")).toBe(0);
      expect(
        metricValue(
          divergentMetrics.body,
          'converge_delivery_state_transitions_total{source="watchdog",state="unavailable"}',
        ),
      ).toBe(1);
      expect(
        metricValue(
          divergentMetrics.body,
          'converge_delivery_state_transitions_total{source="socket_readiness",state="unavailable"}',
        ),
      ).toBe(1);
      await expectSocketReadinessRejected(apiA, tokens.owner);
      await expectSocketReadinessRejected(apiB, tokens.owner);
      const healthyHttp = await apiA.context.app.inject({
        method: "GET",
        url: `/v1/boards/${boardId}`,
        headers: testAuthorizationHeaders(tokens.owner),
      });
      expect(healthyHttp.statusCode).toBe(200);

      await expect(topology.runWorkerCycle()).resolves.toMatchObject({ outcomes: ["published"] });
      const publishedPaused = await operationEvidence(paused.opId);
      if (!publishedPaused.redisEntryId)
        throw new Error("Missing watchdog recovery publication ID");
      await Promise.all([
        topology.waitForHandled(apiA, pendingPaused.eventId),
        topology.waitForHandled(apiB, pendingPaused.eventId),
        topology.waitForCursor(apiA, publishedPaused.redisEntryId),
        topology.waitForCursor(apiB, publishedPaused.redisEntryId),
      ]);
      expect(clientA.rawOperationCounts.get(paused.opId)).toBeUndefined();
      expect(clientB.rawOperationCounts.get(paused.opId)).toBeUndefined();

      await topology.runWatchdogChecks();
      await Promise.all([
        topology.waitForWatchdog(
          apiA,
          ({ state }) => state === "recovered",
          "API A watchdog recovery",
        ),
        topology.waitForWatchdog(
          apiB,
          ({ state }) => state === "recovered",
          "API B watchdog recovery",
        ),
      ]);
      expect((await operationalRequest(apiA, "/health/socket-ready")).status).toBe(200);
      const recoveredWatchdogMetrics = await waitForMetricValue(
        apiA,
        'converge_delivery_state_transitions_total{source="watchdog",state="recovered"}',
        1,
      );
      expect(metricValue(recoveredWatchdogMetrics.body, "converge_socket_ready")).toBe(1);
      expect(
        metricValue(
          recoveredWatchdogMetrics.body,
          'converge_delivery_state_transitions_total{source="watchdog",state="recovered"}',
        ),
      ).toBe(1);
      await topology.runWatchdogChecks();
      expect(
        metricValue(
          (await scrapeMetrics(apiA)).body,
          'converge_delivery_state_transitions_total{source="watchdog",state="recovered"}',
        ),
      ).toBe(1);
      const { client: recoveredA, joined: recoveredJoinA } = await topology.connect(
        apiA,
        tokens.editor,
        boardId,
        1,
        structuredClone(stateAfterFirst),
      );
      const { client: recoveredB, joined: recoveredJoinB } = await topology.connect(
        apiB,
        tokens.owner,
        boardId,
        1,
        structuredClone(stateAfterFirst),
      );
      expect(recoveredJoinA).toMatchObject({ ok: true, joinWatermark: 2 });
      expect(recoveredJoinB).toMatchObject({ ok: true, joinWatermark: 2 });
      const recoveredOperationsA = await catchUp(apiA, boardId, tokens.editor, 1, 2);
      const recoveredOperationsB = await catchUp(apiB, boardId, tokens.owner, 1, 2);
      recoveredA.applyCatchUp(recoveredOperationsA);
      recoveredB.applyCatchUp(recoveredOperationsB);
      await expectConverged(recoveredA, boardId);
      await expectConverged(recoveredB, boardId);

      const apiBUnavailableBefore = apiB.evidence.lifecycle.entries.filter(
        ({ state }) => state === "unavailable",
      ).length;
      const transportA = apiA.evidence.transport;
      if (!transportA) throw new Error("Missing API A Redis transport");
      transportA.pauseReconnect();
      transportA.interruptReader();
      await Promise.all([
        topology.waitForLifecycle(
          apiA,
          ({ state }) => state === "unavailable",
          "API A Redis interruption",
        ),
        topology.waitForLifecycle(
          apiA,
          ({ state }) => state === "recovering",
          "API A Redis recovering",
        ),
        withDeadline(
          recoveredA.disconnects.waitFor(() => true),
          "API A Redis-interruption disconnect",
          () => topology.diagnostics(),
        ),
      ]);
      expect(recoveredB.socket.connected).toBe(true);
      expect((await operationalRequest(apiA, "/health/ready")).status).toBe(200);
      expect((await operationalRequest(apiA, "/health/socket-ready")).status).toBe(503);
      const interruptedMetrics = await waitForMetricValue(
        apiA,
        'converge_delivery_state_transitions_total{source="consumer",state="recovering"}',
        1,
      );
      expect(metricValue(interruptedMetrics.body, "converge_delivery_consumer_ready")).toBe(0);
      expect(metricValue(interruptedMetrics.body, "converge_socket_ready")).toBe(0);
      expect(
        metricValue(
          interruptedMetrics.body,
          'converge_delivery_state_transitions_total{source="consumer",state="unavailable"}',
        ),
      ).toBe(1);
      expect(
        metricValue(
          interruptedMetrics.body,
          'converge_delivery_state_transitions_total{source="consumer",state="recovering"}',
        ),
      ).toBe(1);
      await expectSocketReadinessRejected(apiA, tokens.owner);
      const httpDuringRedisRecovery = await apiA.context.app.inject({
        method: "GET",
        url: `/v1/boards/${boardId}`,
        headers: testAuthorizationHeaders(tokens.owner),
      });
      expect(httpDuringRedisRecovery.statusCode).toBe(200);

      const duringRedisInterruption = createRectangleCommand(boardId);
      await expect(topology.submit(recoveredB, duringRedisInterruption)).resolves.toMatchObject({
        ok: true,
        operation: { seq: 3 },
      });
      await topology.runWorkerCycle();
      const interruptionPublication = await operationEvidence(duringRedisInterruption.opId);
      if (!interruptionPublication.redisEntryId)
        throw new Error("Missing Redis-interruption publication ID");
      await topology.waitForClientOperation(recoveredB, duringRedisInterruption.opId);
      expect(
        apiA.evidence.handled.entries.some(
          ({ envelope }) => envelope.eventId === interruptionPublication.eventId,
        ),
      ).toBe(false);

      transportA.releaseReconnect();
      await Promise.all([
        topology.waitForLifecycle(
          apiA,
          ({ state }) => state === "recovered",
          "API A Redis recovery",
        ),
        topology.waitForHandled(apiA, interruptionPublication.eventId),
        topology.waitForCursor(apiA, interruptionPublication.redisEntryId),
      ]);
      expect(
        apiA.evidence.handled.entries.filter(
          ({ envelope }) => envelope.eventId === interruptionPublication.eventId,
        ),
      ).toHaveLength(1);
      expect(
        apiB.evidence.handled.entries.filter(
          ({ envelope }) => envelope.eventId === interruptionPublication.eventId,
        ),
      ).toHaveLength(1);
      expect(
        apiB.evidence.lifecycle.entries.filter(({ state }) => state === "unavailable"),
      ).toHaveLength(apiBUnavailableBefore);
      expect(recoveredB.rawOperationCounts.get(duringRedisInterruption.opId)).toBe(1);
      expect((await operationalRequest(apiA, "/health/socket-ready")).status).toBe(200);
      const reconnectedMetrics = await waitForMetricValue(
        apiA,
        'converge_delivery_state_transitions_total{source="consumer",state="recovered"}',
        1,
      );
      expect(metricValue(reconnectedMetrics.body, "converge_delivery_consumer_ready")).toBe(1);
      expect(metricValue(reconnectedMetrics.body, "converge_socket_ready")).toBe(1);
      expect(
        metricValue(
          reconnectedMetrics.body,
          'converge_delivery_state_transitions_total{source="consumer",state="recovered"}',
        ),
      ).toBe(1);

      const { client: postInterruptA, joined: postInterruptJoin } = await topology.connect(
        apiA,
        tokens.editor,
        boardId,
        2,
        structuredClone(recoveredA.state),
      );
      expect(postInterruptJoin).toMatchObject({ ok: true, joinWatermark: 3 });
      postInterruptA.applyCatchUp(await catchUp(apiA, boardId, tokens.editor, 2, 3));
      await expectConverged(postInterruptA, boardId);
      await expectConverged(recoveredB, boardId);

      const { client: editorB } = await topology.connect(
        apiB,
        tokens.editor,
        boardId,
        3,
        structuredClone(recoveredB.state),
      );
      const { client: viewer } = await topology.connect(
        apiB,
        tokens.viewer,
        boardId,
        3,
        structuredClone(recoveredB.state),
      );
      const { client: editorOtherBoard } = await topology.connect(
        apiA,
        tokens.editor,
        otherBoardId,
      );
      const removal = await topology.removeMember(apiB, boardId, identities.editor.id);
      if (!removal.eventId) throw new Error("Missing activated revocation event ID");
      const pendingRevocation = await revocationEvidence(removal.eventId);
      expect(pendingRevocation).toMatchObject({ status: "pending", redisEntryId: null });
      expect(postInterruptA.revocations.entries).toEqual([]);
      expect(editorB.revocations.entries).toEqual([]);

      await topology.runWorkerCycle();
      const publishedRevocation = await revocationEvidence(removal.eventId);
      if (!publishedRevocation.redisEntryId)
        throw new Error("Missing activated revocation publication ID");
      await Promise.all([
        topology.waitForRevocation(postInterruptA, boardId),
        topology.waitForRevocation(editorB, boardId),
        topology.waitForHandled(apiA, removal.eventId),
        topology.waitForHandled(apiB, removal.eventId),
      ]);
      expect(roomHas(apiA, boardId, postInterruptA)).toBe(false);
      expect(roomHas(apiB, boardId, editorB)).toBe(false);
      expect(roomHas(apiB, boardId, viewer)).toBe(true);
      expect(roomHas(apiA, otherBoardId, editorOtherBoard)).toBe(true);
      expect(viewer.revocations.entries).toEqual([]);
      expect(editorOtherBoard.revocations.entries).toEqual([]);

      const duplicateRevocationId = await appendDuplicate(
        topology.streamKey,
        publishedRevocation.payload,
      );
      await Promise.all([
        topology.waitForReadEvidence(apiA, duplicateRevocationId),
        topology.waitForReadEvidence(apiB, duplicateRevocationId),
        topology.waitForCursor(apiA, duplicateRevocationId),
        topology.waitForCursor(apiB, duplicateRevocationId),
      ]);
      expect(postInterruptA.rawRevocationCounts.get(boardId)).toBe(1);
      expect(editorB.rawRevocationCounts.get(boardId)).toBe(1);

      const authorizedAfterRevocation = createRectangleCommand(boardId);
      await topology.submit(recoveredB, authorizedAfterRevocation);
      await topology.runWorkerCycle();
      await topology.waitForClientOperation(viewer, authorizedAfterRevocation.opId);
      expect(postInterruptA.rawOperationCounts.get(authorizedAfterRevocation.opId)).toBeUndefined();
      expect(editorB.rawOperationCounts.get(authorizedAfterRevocation.opId)).toBeUndefined();
      expect(editorOtherBoard.socket.connected).toBe(true);
      await expectConverged(recoveredB, boardId);
      await expectConverged(viewer, boardId);
      expect(await consumerGroups(topology.streamKey)).toEqual([]);

      const finalMetrics = await scrapeMetrics(apiA);
      expectFixedMetricCatalog(finalMetrics.body);
      expect(finalMetrics.body).not.toContain("recovery.request.result");
      const telemetryEvidence = JSON.stringify(apiA.server?.telemetry.snapshot());
      const successfulRecoveryEvidence = [
        JSON.parse(snapshotTailRecovery.body) as Record<string, unknown>,
        JSON.parse(refreshedRecovery.body) as Record<string, unknown>,
      ];
      const forbiddenEvidence = [
        boardId,
        otherBoardId,
        refreshedBoardId,
        blockedBoardId,
        retryableBoardId,
        first.opId,
        paused.opId,
        publishedFirst.eventId,
        publishedFirst.redisEntryId,
        metricsBearerToken,
        `Bearer ${metricsBearerToken}`,
        tokens.owner,
        identities.owner.id,
        ...successfulRecoveryEvidence.flatMap((material) =>
          ["snapshotId", "snapshotHash", "reconstructedHash"]
            .map((key) => material[key])
            .filter((value): value is string => typeof value === "string"),
        ),
      ];
      for (const value of forbiddenEvidence) {
        expect(finalMetrics.body).not.toContain(value);
        expect(telemetryEvidence).not.toContain(value);
      }

      const closeA = apiA.close();
      expect(apiA.close()).toBe(closeA);
      await closeA;
      const stoppedSnapshot = apiA.server?.telemetry.snapshot();
      const stoppedLifecycleEvents = stoppedSnapshot?.events.filter(
        ({ eventName }) => eventName === "api.lifecycle",
      );
      expect(stoppedLifecycleEvents?.map(({ code }) => code)).toEqual([
        "STARTING",
        "READY",
        "STOPPING",
        "STOPPED",
      ]);
      expect(
        stoppedSnapshot?.gauges.find(({ name }) => name === "converge_delivery_consumer_ready")
          ?.value,
      ).toBe(0);
      expect(
        stoppedSnapshot?.gauges.find(({ name }) => name === "converge_socket_ready")?.value,
      ).toBe(0);
      await apiA.evidence.runtimeObserver?.lifecycle({ state: "recovered" });
      expect(
        apiA.server?.telemetry
          .snapshot()
          .events.filter(({ eventName }) => eventName === "api.lifecycle"),
      ).toEqual(stoppedLifecycleEvents);
      expect(apiA.context.app.server.listening).toBe(false);
      expect(apiA.evidence.transport?.closeCalls).toBe(1);
      expect(apiA.evidence.watchdogScheduler?.pendingTasks).toBe(0);

      const topologyClose = topology.close();
      expect(topology.close()).toBe(topologyClose);
      await topologyClose;
      await apiB.evidence.runtimeObserver?.lifecycle({ state: "recovered" });
      expect(apiB.context.app.server.listening).toBe(false);
      expect(apiB.evidence.transport?.closeCalls).toBe(1);
      expect(apiB.evidence.watchdogScheduler?.pendingTasks).toBe(0);
      expect(await requireRedis().sendCommand(["EXISTS", topology.streamKey])).toBe(0);
    });
  });
});
