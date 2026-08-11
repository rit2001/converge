import type { AddressInfo } from "node:net";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { BoardRecoveryService, buildApp, type AppContext } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import { parseEnvironment } from "@converge/api/env";
import { hashBoardState } from "@converge/canvas-engine";
import {
  BoardRecoveryMaterialRepository,
  BoardRepository,
  BoardSnapshotCandidateRepository,
  BoardSnapshotRepository,
  createPool,
  hashBoardSnapshot,
  type DatabasePool,
} from "@converge/database";
import {
  boardRecoveryMaterialSchema,
  type BoardSnapshot,
  type DeliveryStreamFields,
  type DurableCommand,
} from "@converge/protocol";
import {
  createTestSocket,
  fixtureIds,
  TestAuthAdapter,
  testAuthorizationHeaders,
} from "@converge/testkit";
import {
  createWorkerApplication,
  type OutboxPublisherComponent,
  type WorkerApplicationDatabase,
} from "../../apps/worker/src/application";
import { parseWorkerEnvironment } from "../../apps/worker/src/env";
import type { DeliveryStream } from "../../apps/worker/src/redis-stream";
import {
  SnapshotCoordinator,
  defaultSnapshotCoordinatorConfiguration,
  type SnapshotCoordinatorConfiguration,
  type SnapshotCoordinatorScheduler,
} from "../../apps/worker/src/snapshot-coordinator";
import type { BoardSessionToken } from "../../apps/web/src/board-session";
import { useBoardStore } from "../../apps/web/src/board-store";
import type { RetryScheduler } from "../../apps/web/src/pending-command-queue";
import type { PendingLoadResult, PendingOperationStore } from "../../apps/web/src/pending-db";
import { BoardTransport } from "../../apps/web/src/transport";

const sharedDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@127.0.0.1:55432/converge";
const repositoryRoot = new URL("../..", import.meta.url);
const runFile = promisify(execFile);
const recoveryToken = "m26-final-owner-token";
const owner: AuthenticatedPrincipal = { id: fixtureIds.user, displayName: "M2.6 owner" };

let databaseName: string;
let isolatedDatabaseUrl: string;
let adminPool: ReturnType<typeof createPool>;
let pool: ReturnType<typeof createPool>;
let boards: BoardRepository;
let snapshots: BoardSnapshotRepository;
let candidates: BoardSnapshotCandidateRepository;
let recovery: BoardRecoveryMaterialRepository;
let api: AppContext;
let apiUrl: string;
let generation = 80_000;
const boardIds = new Set<string>();
const transports = new Set<BoardTransport>();
const sockets = new Set<ReturnType<typeof createTestSocket>>();

function deferred<T = void>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

class ManualScheduler implements SnapshotCoordinatorScheduler {
  private nextId = 1;
  readonly timers = new Map<number, () => void>();

  setTimeout(callback: () => void): number {
    const id = this.nextId++;
    this.timers.set(id, callback);
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.timers.delete(handle as number);
  }
}

class TransportScheduler implements RetryScheduler {
  private nextId = 1;
  readonly tasks = new Map<number, { delay: number; callback: () => void }>();

  setTimeout(callback: () => void, delay: number): number {
    const id = this.nextId++;
    this.tasks.set(id, { delay, callback });
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.tasks.delete(handle as number);
  }

  random(): number {
    return 0;
  }

  runDelay(delay: number): void {
    const task = [...this.tasks].find(([, value]) => value.delay === delay);
    if (!task) throw new Error(`No transport task scheduled for ${delay}ms`);
    this.tasks.delete(task[0]);
    task[1].callback();
  }
}

class IsolatedPendingStore implements PendingOperationStore {
  readonly rows = new Map<string, DurableCommand>();

  constructor(commands: readonly DurableCommand[] = []) {
    for (const command of commands) this.rows.set(command.opId, command);
  }

  load(): Promise<PendingLoadResult> {
    return Promise.resolve({ commands: [...this.rows.values()], corruptCount: 0 });
  }

  put(command: DurableCommand): Promise<void> {
    this.rows.set(command.opId, command);
    return Promise.resolve();
  }

  delete(_boardId: string, operationId: string): Promise<void> {
    void _boardId;
    this.rows.delete(operationId);
    return Promise.resolve();
  }
}

class UnavailableRedisStream implements DeliveryStream {
  connectCalls = 0;
  closeCalls = 0;

  connect(): Promise<void> {
    this.connectCalls += 1;
    return Promise.reject(new Error("intentionally unavailable"));
  }

  isReady(): boolean {
    return false;
  }

  append(_fields: DeliveryStreamFields, _signal: AbortSignal): Promise<string> {
    void _fields;
    void _signal;
    return Promise.reject(new Error("Redis is unavailable"));
  }

  trimByAge(_signal: AbortSignal): Promise<void> {
    void _signal;
    return Promise.resolve();
  }

  resetAfterCommandTimeout(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }
}

class IdleOutboxPublisher implements OutboxPublisherComponent {
  claims = 0;
  stopCalls = 0;
  drainCalls = 0;

  run(signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      if (signal.aborted) return resolve();
      signal.addEventListener("abort", () => resolve(), { once: true });
    });
  }

  stopTakingClaims(): void {
    this.stopCalls += 1;
  }

  abandonActiveLeases(): void {}

  drain(): Promise<boolean> {
    this.drainCalls += 1;
    return Promise.resolve(true);
  }
}

const silentLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const coordinatorConfiguration = (
  overrides: Partial<SnapshotCoordinatorConfiguration> = {},
): SnapshotCoordinatorConfiguration => ({
  ...defaultSnapshotCoordinatorConfiguration,
  pollIntervalMs: 1_000,
  pollJitterPercent: 1,
  operationThreshold: 2,
  ...overrides,
});

async function createBoard(label: string): Promise<string> {
  const boardId = (await boards.createBoard(owner.id, `${label}-${crypto.randomUUID()}`)).id;
  boardIds.add(boardId);
  return boardId;
}

function objectCommand(
  boardId: string,
  baseSeq: number,
  kind: "rectangle" | "sticky" = "rectangle",
  rotation = 0,
): DurableCommand {
  const id = crypto.randomUUID();
  const common = {
    id,
    x: baseSeq * 10,
    y: baseSeq * 5,
    width: 120,
    height: 80,
    rotation,
  };
  return {
    schemaVersion: 1,
    opId: crypto.randomUUID(),
    boardId,
    clientId: fixtureIds.clientA,
    baseSeq,
    targetId: id,
    clientTimestamp: new Date().toISOString(),
    type: "object.create",
    payload:
      kind === "sticky"
        ? { ...common, kind, fill: "#fef08a", text: `sticky-${baseSeq}` }
        : { ...common, kind, fill: "#818cf8", text: "" },
  } as DurableCommand;
}

function transformCommand(
  boardId: string,
  baseSeq: number,
  targetId: string,
  rotation: number,
): DurableCommand {
  return {
    schemaVersion: 1,
    opId: crypto.randomUUID(),
    boardId,
    clientId: fixtureIds.clientA,
    baseSeq,
    targetId,
    clientTimestamp: new Date().toISOString(),
    type: "object.transform",
    payload: { rotation, x: baseSeq },
  };
}

async function heads(boardId: string): Promise<{ canvas: number; delivery: number }> {
  const result = await pool.query<{ last_seq: string; last_delivery_seq: string }>(
    "SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1",
    [boardId],
  );
  return {
    canvas: Number(result.rows[0]?.last_seq),
    delivery: Number(result.rows[0]?.last_delivery_seq),
  };
}

async function snapshotRows(boardId: string): Promise<
  Array<{
    id: string;
    snapshot_seq: string;
    snapshot_delivery_seq: string;
    status: string;
    canonical_hash: string;
  }>
> {
  return (
    await pool.query<{
      id: string;
      snapshot_seq: string;
      snapshot_delivery_seq: string;
      status: string;
      canonical_hash: string;
    }>(
      `SELECT id, snapshot_seq, snapshot_delivery_seq, status, canonical_hash
       FROM board_snapshots WHERE board_id = $1 ORDER BY snapshot_seq, id`,
      [boardId],
    )
  ).rows;
}

async function corruptHash(snapshotId: string): Promise<void> {
  await pool.query("ALTER TABLE board_snapshots DISABLE TRIGGER board_snapshots_immutable");
  try {
    await pool.query("UPDATE board_snapshots SET canonical_hash = $2 WHERE id = $1", [
      snapshotId,
      "f".repeat(64),
    ]);
  } finally {
    await pool.query("ALTER TABLE board_snapshots ENABLE TRIGGER board_snapshots_immutable");
  }
}

async function logicalBoardState(boardId: string): Promise<unknown> {
  return (
    await pool.query<{ state: unknown }>(
      `SELECT jsonb_build_object(
        'board', (SELECT to_jsonb(b) FROM boards b WHERE id = $1),
        'objects', (SELECT jsonb_agg(to_jsonb(o) ORDER BY stack_order) FROM board_objects o WHERE board_id = $1),
        'operations', (SELECT jsonb_agg(to_jsonb(op) ORDER BY seq) FROM board_operations op WHERE board_id = $1),
        'outbox', (SELECT jsonb_agg(to_jsonb(e) ORDER BY delivery_seq) FROM outbox_events e WHERE board_id = $1),
        'members', (SELECT jsonb_agg(to_jsonb(m) ORDER BY user_id) FROM board_members m WHERE board_id = $1)
      ) AS state`,
      [boardId],
    )
  ).rows[0]?.state;
}

function authenticatedFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const headers = new Headers(init?.headers);
  for (const [key, value] of Object.entries(testAuthorizationHeaders(recoveryToken)))
    headers.set(key, value);
  return fetch(input, { ...init, headers });
}

function startTransport(
  boardId: string,
  initialSeq: number,
  options: {
    pending?: DurableCommand[];
    pendingStore?: IsolatedPendingStore;
    apiBaseUrl?: string;
    fetcher?: typeof fetch;
    connect?: boolean;
  } = {},
): {
  transport: BoardTransport;
  token: BoardSessionToken;
  scheduler: TransportScheduler;
  pendingStore: IsolatedPendingStore;
} {
  const token: BoardSessionToken = { generation: ++generation, nonce: Symbol("m26-acceptance") };
  const pending = options.pending ?? [];
  const pendingStore = options.pendingStore ?? new IsolatedPendingStore(pending);
  const initial: BoardSnapshot = {
    id: boardId,
    name: "pre-recovery",
    lastSeq: initialSeq,
    objects: [],
  };
  useBoardStore.getState().beginSession(token, boardId);
  useBoardStore.getState().initializeSession(token, initial, pending);
  const scheduler = new TransportScheduler();
  const transport = new BoardTransport(boardId, crypto.randomUUID(), token, {
    apiUrl: options.apiBaseUrl ?? apiUrl,
    scheduler,
    pendingStore,
    fetcher: options.fetcher ?? authenticatedFetch,
    socketFactory: (url) => {
      const socket = createTestSocket(url, recoveryToken);
      sockets.add(socket);
      return socket as never;
    },
  });
  transports.add(transport);
  if (options.connect !== false) transport.connect();
  return { transport, token, scheduler, pendingStore };
}

async function driveRecoveredTransportReady(scheduler: TransportScheduler): Promise<void> {
  await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("retry-wait"));
  const delay = useBoardStore.getState().synchronizationDiagnostics.retryDelayMs;
  if (delay === null) throw new Error("Recovery retry was not scheduled");
  scheduler.runDelay(delay);
  await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("ready"));
  await vi.waitFor(() => expect(useBoardStore.getState().authoritativeHash.status).toBe("ready"));
}

beforeAll(async () => {
  databaseName = `converge_m26_acceptance_${process.pid}_${crypto
    .randomUUID()
    .replaceAll("-", "")
    .slice(0, 10)}`;
  const adminUrl = new URL(sharedDatabaseUrl);
  adminUrl.pathname = "/postgres";
  adminPool = createPool(adminUrl.toString());
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const databaseUrl = new URL(sharedDatabaseUrl);
  databaseUrl.pathname = `/${databaseName}`;
  isolatedDatabaseUrl = databaseUrl.toString();
  await runFile("pnpm", ["--filter", "@converge/database", "migrate"], {
    cwd: repositoryRoot,
    env: { ...process.env, DATABASE_URL: isolatedDatabaseUrl },
    maxBuffer: 1024 * 1024,
  });
  pool = createPool(isolatedDatabaseUrl);
  boards = new BoardRepository(pool);
  snapshots = new BoardSnapshotRepository(pool);
  candidates = new BoardSnapshotCandidateRepository(pool);
  recovery = new BoardRecoveryMaterialRepository(pool);
  api = await buildApp(
    parseEnvironment({
      NODE_ENV: "test",
      HOST: "127.0.0.1",
      API_PORT: "4000",
      WEB_ORIGIN: "http://127.0.0.1:3000",
      DATABASE_URL: isolatedDatabaseUrl,
      REDIS_URL: "redis://127.0.0.1:1",
      LOG_LEVEL: "silent",
      DEV_AUTH_USER_NAME: "Unused",
    }),
    pool,
    new TestAuthAdapter(new Map([[recoveryToken, owner]])),
  );
  await api.app.listen({ host: "127.0.0.1", port: 0 });
  const address = api.app.server.address() as AddressInfo;
  apiUrl = `http://127.0.0.1:${address.port}`;
  const migration = await pool.query<{ name: string }>(
    "SELECT name FROM converge_migrations ORDER BY name DESC LIMIT 1",
  );
  expect(migration.rows[0]?.name).toBe("0008_snapshot_invalidation_diagnostics.sql");
});

afterEach(async () => {
  for (const transport of transports) transport.disconnect();
  transports.clear();
  for (const socket of sockets) socket.disconnect();
  sockets.clear();
  const token = useBoardStore.getState().sessionToken;
  if (token) useBoardStore.getState().endSession(token);
  if (boardIds.size > 0)
    await pool.query("DELETE FROM boards WHERE id = ANY($1::uuid[])", [[...boardIds]]);
  boardIds.clear();
});

afterAll(async () => {
  await api?.app.close();
  await api?.app.close();
  await pool?.end();
  try {
    if (adminPool && databaseName) await adminPool.query(`DROP DATABASE "${databaseName}"`);
  } finally {
    await adminPool?.end();
  }
});

describe("M2.6 verified snapshot recovery acceptance", () => {
  it("creates an automatic verified genesis snapshot while Redis is unavailable", async () => {
    const boardId = await createBoard("automatic-bootstrap");
    const before = await heads(boardId);
    const captured = deferred<void>();
    const workerPool = createPool(isolatedDatabaseUrl);
    const redis = new UnavailableRedisStream();
    const outbox = new IdleOutboxPublisher();
    const snapshotScheduler = new ManualScheduler();
    let coordinator: SnapshotCoordinator | undefined;
    const environment = parseWorkerEnvironment({
      NODE_ENV: "test",
      DATABASE_URL: isolatedDatabaseUrl,
      REDIS_URL: "redis://127.0.0.1:1",
      LOG_LEVEL: "silent",
      SNAPSHOT_OPERATION_THRESHOLD: "2",
      SNAPSHOT_POLL_INTERVAL_MS: "1000",
      SNAPSHOT_POLL_JITTER_PERCENT: "1",
    });
    const application = await createWorkerApplication(environment, {
      createLogger: () => silentLogger as never,
      createDatabase: () => workerPool as unknown as WorkerApplicationDatabase,
      createSnapshotCoordinator: ({ database, configuration }) => {
        coordinator = new SnapshotCoordinator({
          repository: new BoardSnapshotCandidateRepository(database as DatabasePool),
          configuration,
          scheduler: snapshotScheduler,
          random: { next: () => 0.5 },
          hooks: { captured: () => captured.resolve() },
        });
        return coordinator;
      },
      createStream: () => redis,
      createOutboxPublisher: () => outbox,
      reconnectScheduler: {
        sleep: (_delay, signal) =>
          new Promise((resolve) => {
            if (signal?.aborted) return resolve();
            signal?.addEventListener("abort", () => resolve(), { once: true });
          }),
      },
    });

    await application.start();
    await captured.promise;
    const rows = await snapshotRows(boardId);
    expect(rows).toMatchObject([
      { snapshot_seq: "0", snapshot_delivery_seq: "0", status: "verified" },
    ]);
    expect(await heads(boardId)).toEqual(before);
    expect(redis.connectCalls).toBeGreaterThanOrEqual(1);
    expect(redis.isReady()).toBe(false);
    expect(outbox.claims).toBe(0);
    expect(coordinator?.diagnostics.lifecycle).toBe("running");
    const firstShutdown = application.shutdown("PROCESS_END");
    const repeatedShutdown = application.shutdown("SIGTERM");
    expect(repeatedShutdown).toBe(firstShutdown);
    await firstShutdown;
    expect(redis.closeCalls).toBe(1);
    expect(outbox.stopCalls).toBe(1);
    expect(outbox.drainCalls).toBe(1);
  });

  it("captures at threshold with exact projection evidence and fences concurrent work", async () => {
    const boardId = await createBoard("threshold");
    await snapshots.create(boardId);
    const first = objectCommand(boardId, 0, "rectangle", 27);
    await boards.commitOperation(owner.id, first);
    const policy = {
      operationThreshold: 2,
      changedAgeMs: 86_400_000,
      operationBytesThreshold: 8_388_608,
      currentTime: new Date(),
    };
    expect(
      (await candidates.discover(policy)).candidates.map(({ boardId }) => boardId),
    ).not.toContain(boardId);
    const second = objectCommand(boardId, 1, "sticky", -13);
    await boards.commitOperation(owner.id, second);
    expect(await candidates.discover(policy)).toMatchObject({
      candidates: [{ boardId, reason: "operation_count", canvasHead: 2, deliveryHead: 2 }],
    });

    const scheduler = new ManualScheduler();
    const coordinator = new SnapshotCoordinator({
      repository: candidates,
      configuration: coordinatorConfiguration(),
      scheduler,
      random: { next: () => 0.5 },
    });
    await coordinator.start();
    await coordinator.runCycle();
    const rows = await snapshotRows(boardId);
    expect(rows).toHaveLength(2);
    const latest = await snapshots.loadLatest(boardId);
    expect(latest).toMatchObject({ snapshotSeq: 2, snapshotDeliverySeq: 2 });
    expect(latest?.projection.objects.map(({ objectId }) => objectId)).toEqual([
      first.targetId,
      second.targetId,
    ]);
    expect(latest?.projection.objects.map(({ value }) => value.rotation)).toEqual([27, -13]);
    expect(latest?.canonicalHash).toBe(hashBoardSnapshot(latest!.projection));
    expect(await heads(boardId)).toEqual({ canvas: 2, delivery: 2 });
    await coordinator.runCycle();
    expect(await snapshotRows(boardId)).toHaveLength(2);
    const firstStop = coordinator.stop();
    expect(coordinator.stop()).toBe(firstStop);
    await firstStop;
    expect(scheduler.timers.size).toBe(0);

    const raceBoard = await createBoard("foreground-race");
    await boards.commitOperation(owner.id, objectCommand(raceBoard, 0));
    const lockEntered = deferred<void>();
    const releaseLock = deferred<void>();
    const racingCandidates = new BoardSnapshotCandidateRepository(pool, {
      afterAdvisoryLock: async () => {
        lockEntered.resolve();
        await releaseLock.promise;
      },
    });
    const capture = racingCandidates.capture(raceBoard, {
      ...policy,
      operationThreshold: 1,
    });
    await lockEntered.promise;
    const foreground = boards.commitOperation(owner.id, objectCommand(raceBoard, 1));
    releaseLock.resolve();
    await expect(capture).resolves.toMatchObject({ status: "captured", canvasHead: 1 });
    await foreground;
    expect(await heads(raceBoard)).toEqual({ canvas: 2, delivery: 2 });

    const duplicateBoard = await createBoard("duplicate-race");
    const firstEntered = deferred<void>();
    const firstReleased = deferred<void>();
    const firstRepository = new BoardSnapshotCandidateRepository(pool, {
      afterAdvisoryLock: async () => {
        firstEntered.resolve();
        await firstReleased.promise;
      },
    });
    const firstCapture = firstRepository.capture(duplicateBoard, policy);
    await firstEntered.promise;
    const secondCapture = await new BoardSnapshotCandidateRepository(pool).capture(
      duplicateBoard,
      policy,
    );
    expect(secondCapture).toEqual({ status: "busy" });
    firstReleased.resolve();
    await expect(firstCapture).resolves.toMatchObject({ status: "captured" });
    expect(await snapshotRows(duplicateBoard)).toHaveLength(1);
  });

  it("replays a verified snapshot plus tail atomically through the real transport", async () => {
    const boardId = await createBoard("snapshot-tail");
    const rectangle = objectCommand(boardId, 0, "rectangle", 5);
    const sticky = objectCommand(boardId, 1, "sticky", -7);
    await boards.commitOperation(owner.id, rectangle);
    await boards.commitOperation(owner.id, sticky);
    const base = await snapshots.create(boardId);
    await boards.commitOperation(owner.id, transformCommand(boardId, 2, rectangle.targetId, 41));
    await boards.commitOperation(owner.id, transformCommand(boardId, 3, sticky.targetId, 19));

    const response = await fetch(`${apiUrl}/v1/boards/${boardId}/recovery`, {
      headers: testAuthorizationHeaders(recoveryToken),
    });
    expect(response.status).toBe(200);
    const material = boardRecoveryMaterialSchema.parse(await response.json());
    expect(material.snapshotId).toBe(base.id);
    expect(material.operationTail.map(({ seq }) => seq)).toEqual([3, 4]);

    const observedSequences: number[] = [];
    const { scheduler } = startTransport(boardId, 5);
    let previous = useBoardStore.getState().committed;
    const unsubscribe = useBoardStore.subscribe((state) => {
      if (state.committed === previous) return;
      previous = state.committed;
      observedSequences.push(state.committed.lastSeq);
    });
    await driveRecoveredTransportReady(scheduler);
    unsubscribe();

    const authoritative = await recovery.load(boardId);
    const state = useBoardStore.getState();
    expect(observedSequences).toEqual([4]);
    expect(state.committed.order).toEqual([rectangle.targetId, sticky.targetId]);
    expect(state.objects.map(({ id, rotation }) => ({ id, rotation }))).toEqual([
      { id: rectangle.targetId, rotation: 41 },
      { id: sticky.targetId, rotation: 19 },
    ]);
    expect(state.authoritativeHash).toMatchObject({
      status: "ready",
      seq: 4,
      value: authoritative.reconstructedHash,
    });
    expect(material.snapshotCanonicalHash).toBe(authoritative.snapshotHash);
    expect(material.reconstructedCanonicalHash).toBe(authoritative.reconstructedHash);
    expect(await hashBoardState(state.committed)).toBe(authoritative.reconstructedHash);
  });

  it("refreshes a 101-operation tail once without logical board mutation", async () => {
    const boardId = await createBoard("tail-overflow");
    const object = objectCommand(boardId, 0);
    await boards.commitOperation(owner.id, object);
    await snapshots.create(boardId);
    for (let index = 1; index <= 101; index += 1)
      await boards.commitOperation(
        owner.id,
        transformCommand(boardId, index, object.targetId, index),
      );
    const beforeHeads = await heads(boardId);
    const beforeState = await logicalBoardState(boardId);
    const first = await fetch(`${apiUrl}/v1/boards/${boardId}/recovery`, {
      headers: testAuthorizationHeaders(recoveryToken),
    });
    const material = boardRecoveryMaterialSchema.parse(await first.json());
    expect(material).toMatchObject({
      snapshotCanvasSeq: 102,
      snapshotDeliverySeq: 102,
      capturedCanvasSeq: 102,
      capturedDeliverySeq: 102,
      operationTail: [],
    });
    expect(await heads(boardId)).toEqual(beforeHeads);
    expect(await logicalBoardState(boardId)).toEqual(beforeState);
    expect(await snapshotRows(boardId)).toHaveLength(2);
    const repeated = await fetch(`${apiUrl}/v1/boards/${boardId}/recovery`, {
      headers: testAuthorizationHeaders(recoveryToken),
    });
    expect(boardRecoveryMaterialSchema.parse(await repeated.json()).snapshotId).toBe(
      material.snapshotId,
    );
    expect(await snapshotRows(boardId)).toHaveLength(2);
  });

  it("falls back around corruption and terminally fences an unrecoverable transport", async () => {
    const fallbackBoard = await createBoard("corrupt-fallback");
    const first = objectCommand(fallbackBoard, 0);
    await boards.commitOperation(owner.id, first);
    const older = await snapshots.create(fallbackBoard);
    await boards.commitOperation(owner.id, transformCommand(fallbackBoard, 1, first.targetId, 25));
    const corrupt = await snapshots.create(fallbackBoard);
    await corruptHash(corrupt.id);
    await boards.commitOperation(owner.id, transformCommand(fallbackBoard, 2, first.targetId, 50));
    const firstResponse = await fetch(`${apiUrl}/v1/boards/${fallbackBoard}/recovery`, {
      headers: testAuthorizationHeaders(recoveryToken),
    });
    const fallback = boardRecoveryMaterialSchema.parse(await firstResponse.json());
    expect(fallback.snapshotId).toBe(older.id);
    expect(fallback.operationTail.map(({ seq }) => seq)).toEqual([2, 3]);
    const invalid = await pool.query<{ status: string }>(
      "SELECT status FROM board_snapshots WHERE id = $1",
      [corrupt.id],
    );
    expect(invalid.rows[0]?.status).toBe("invalid");
    const { scheduler } = startTransport(fallbackBoard, 4);
    await driveRecoveredTransportReady(scheduler);
    expect(useBoardStore.getState().authoritativeHash).toMatchObject({
      value: fallback.reconstructedCanonicalHash,
    });
    const repeated = await fetch(`${apiUrl}/v1/boards/${fallbackBoard}/recovery`, {
      headers: testAuthorizationHeaders(recoveryToken),
    });
    expect(boardRecoveryMaterialSchema.parse(await repeated.json()).snapshotId).toBe(older.id);

    for (const transport of transports) transport.disconnect();
    transports.clear();
    const current = useBoardStore.getState().sessionToken;
    if (current) useBoardStore.getState().endSession(current);

    const blockedBoard = await createBoard("terminal-blocked");
    const blockedSnapshot = await snapshots.create(blockedBoard);
    await corruptHash(blockedSnapshot.id);
    await boards.commitOperation(owner.id, objectCommand(blockedBoard, 0));
    await pool.query("DELETE FROM board_operations WHERE board_id = $1 AND seq = 1", [
      blockedBoard,
    ]);
    const pending = objectCommand(blockedBoard, 1, "sticky");
    const persistence = new IsolatedPendingStore([pending]);
    const requested: string[] = [];
    const blockedTransport = startTransport(blockedBoard, 0, {
      pending: [pending],
      pendingStore: persistence,
      fetcher: (input, init) => {
        requested.push(
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url,
        );
        return authenticatedFetch(input, init);
      },
    });
    await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("error"));
    expect(useBoardStore.getState()).toMatchObject({
      committed: { lastSeq: 0, order: [] },
      pending: [{ opId: pending.opId }],
      synchronizationDiagnostics: { retryCode: "RECOVERY_BLOCKED", retryScheduled: false },
    });
    expect(persistence.rows.has(pending.opId)).toBe(true);
    expect(blockedTransport.scheduler.tasks.size).toBe(0);
    expect(
      requested.filter((url) => url.endsWith(`/boards/${blockedBoard}/recovery`)),
    ).toHaveLength(1);
    expect(requested.filter((url) => url.includes("/operations?"))).toHaveLength(1);
    expect(
      requested.some(
        (url) => url.endsWith(`/v1/boards/${blockedBoard}`) && !url.endsWith("/recovery"),
      ),
    ).toBe(false);

    await snapshots.create(blockedBoard);
    const replacement = startTransport(blockedBoard, 2);
    await driveRecoveredTransportReady(replacement.scheduler);
    expect(useBoardStore.getState()).toMatchObject({
      connection: "ready",
      committed: { lastSeq: 1 },
      error: null,
    });
  });

  it("retries lock contention and fences verified material from a stale session", async () => {
    const boardId = await createBoard("retryable-lock");
    const blocker = await pool.connect();
    let holdFirstRefresh = true;
    const refreshRepository = new BoardRecoveryMaterialRepository(pool, { refreshTimeoutMs: 100 });
    const service = new BoardRecoveryService({
      load: (recoveryBoardId) => refreshRepository.load(recoveryBoardId),
      refresh: async (refreshBoardId) => {
        if (holdFirstRefresh) {
          holdFirstRefresh = false;
          await blocker.query("BEGIN");
          await blocker.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
            refreshBoardId,
          ]);
        }
        return refreshRepository.refresh(refreshBoardId);
      },
    });
    const retryApi = await buildApp(
      parseEnvironment({
        NODE_ENV: "test",
        HOST: "127.0.0.1",
        API_PORT: "4000",
        WEB_ORIGIN: "http://127.0.0.1:3000",
        DATABASE_URL: isolatedDatabaseUrl,
        REDIS_URL: "redis://127.0.0.1:1",
        LOG_LEVEL: "silent",
        DEV_AUTH_USER_NAME: "Unused",
      }),
      pool,
      new TestAuthAdapter(new Map([[recoveryToken, owner]])),
      { recoveryMaterialRepository: service },
    );
    await retryApi.app.listen({ host: "127.0.0.1", port: 0 });
    const retryAddress = retryApi.app.server.address() as AddressInfo;
    const retryUrl = `http://127.0.0.1:${retryAddress.port}`;
    let recoveryRequests = 0;
    const pending = objectCommand(boardId, 0);
    const persistence = new IsolatedPendingStore([pending]);
    const retried = startTransport(boardId, 1, {
      pending: [pending],
      pendingStore: persistence,
      apiBaseUrl: retryUrl,
      fetcher: (input, init) => {
        const url =
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        if (url.endsWith("/recovery")) recoveryRequests += 1;
        return authenticatedFetch(input, init);
      },
    });
    await vi.waitFor(() => {
      expect(useBoardStore.getState()).toMatchObject({
        connection: "retry-wait",
        pending: [{ opId: pending.opId }],
        synchronizationDiagnostics: { retryCode: "INTERNAL_ERROR", retryDelayMs: 500 },
      });
    });
    expect(persistence.rows.has(pending.opId)).toBe(true);
    expect(await snapshotRows(boardId)).toHaveLength(0);
    await blocker.query("ROLLBACK");
    blocker.release();
    retried.scheduler.runDelay(500);
    await vi.waitFor(() => {
      expect(useBoardStore.getState()).toMatchObject({
        connection: "retry-wait",
        committed: { lastSeq: 0 },
        synchronizationDiagnostics: { retryCode: "RESYNC_REQUIRED", retryDelayMs: 1_000 },
      });
    });
    expect(recoveryRequests).toBe(2);
    expect(await snapshotRows(boardId)).toHaveLength(1);
    expect(persistence.rows.has(pending.opId)).toBe(true);
    retried.scheduler.runDelay(1_000);
    await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("ready"));
    await vi.waitFor(() => expect(persistence.rows.has(pending.opId)).toBe(false));
    expect(await heads(boardId)).toEqual({ canvas: 1, delivery: 1 });
    const firstClose = retryApi.app.close();
    const repeatedClose = retryApi.app.close();
    await Promise.all([firstClose, repeatedClose]);

    retried.transport.disconnect();
    transports.delete(retried.transport);
    const retriedToken = useBoardStore.getState().sessionToken;
    if (retriedToken) useBoardStore.getState().endSession(retriedToken);

    const staleBoard = await createBoard("stale-session");
    await boards.commitOperation(owner.id, objectCommand(staleBoard, 0));
    await snapshots.create(staleBoard);
    const beforeApply = deferred<void>();
    const enteredApply = deferred<void>();
    const stale = startTransport(staleBoard, 2, { connect: false });
    await vi.waitFor(() => expect(useBoardStore.getState().authoritativeHash.status).toBe("ready"));
    const digest = crypto.subtle.digest.bind(crypto.subtle);
    let holdVerification = true;
    const digestSpy = vi
      .spyOn(crypto.subtle, "digest")
      .mockImplementation(async (...arguments_) => {
        if (holdVerification) {
          holdVerification = false;
          enteredApply.resolve();
          await beforeApply.promise;
        }
        return digest(...arguments_);
      });
    stale.transport.connect();
    await enteredApply.promise;
    stale.transport.disconnect();
    transports.delete(stale.transport);
    const replacement = startTransport(staleBoard, 0);
    await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("ready"));
    beforeApply.resolve();
    await Promise.resolve();
    await Promise.resolve();
    digestSpy.mockRestore();
    expect(useBoardStore.getState()).toMatchObject({
      sessionToken: replacement.token,
      connection: "ready",
      committed: { lastSeq: 1 },
    });
    replacement.transport.disconnect();
    replacement.transport.disconnect();
  });
});
