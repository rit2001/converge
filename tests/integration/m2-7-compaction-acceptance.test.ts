import type { AddressInfo } from "node:net";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { buildApp, type AppContext } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import { parseEnvironment } from "@converge/api/env";
import { hashBoardState } from "@converge/canvas-engine";
import {
  BoardCompactionCandidateRepository,
  BoardCompactionRepository,
  BoardRecoveryMaterialRepository,
  BoardRepository,
  BoardSnapshotRepository,
  createPool,
  OutboxRepository,
} from "@converge/database";
import {
  boardRecoveryMaterialSchema,
  protocolErrorSchema,
  type BoardSnapshot,
  type DurableCommand,
} from "@converge/protocol";
import {
  createTestSocket,
  fixtureIds,
  TestAuthAdapter,
  testAuthorizationHeaders,
} from "@converge/testkit";
import {
  CompactionCoordinator,
  type CompactionCoordinatorScheduler,
} from "../../apps/worker/src/compaction-coordinator";
import { parseWorkerEnvironment } from "../../apps/worker/src/env";
import type { BoardSessionToken } from "../../apps/web/src/board-session";
import { useBoardStore } from "../../apps/web/src/board-store";
import type { RetryScheduler } from "../../apps/web/src/pending-command-queue";
import type { PendingLoadResult, PendingOperationStore } from "../../apps/web/src/pending-db";
import { BoardTransport } from "../../apps/web/src/transport";

const sharedDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@127.0.0.1:55432/converge";
const repositoryRoot = new URL("../..", import.meta.url);
const runFile = promisify(execFile);
const token = "m27-final-owner-token";
const owner: AuthenticatedPrincipal = { id: fixtureIds.user, displayName: "M2.7 owner" };

let databaseName: string;
let isolatedDatabaseUrl: string;
let adminPool: ReturnType<typeof createPool>;
let pool: ReturnType<typeof createPool>;
let boards: BoardRepository;
let snapshots: BoardSnapshotRepository;
let recovery: BoardRecoveryMaterialRepository;
let api: AppContext;
let apiUrl: string;
let transport: BoardTransport | undefined;
let socket: ReturnType<typeof createTestSocket> | undefined;

class ManualScheduler implements CompactionCoordinatorScheduler, RetryScheduler {
  private nextId = 1;
  readonly tasks = new Map<number, { callback: () => void; delayMs: number }>();

  setTimeout(callback: () => void, delayMs: number): number {
    const id = this.nextId++;
    this.tasks.set(id, { callback, delayMs });
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.tasks.delete(handle as number);
  }

  random(): number {
    return 0;
  }

  runDelay(delayMs: number): void {
    const task = [...this.tasks].find(([, value]) => value.delayMs === delayMs);
    if (!task) throw new Error(`No task is scheduled for ${delayMs}ms`);
    this.tasks.delete(task[0]);
    task[1].callback();
  }
}

class IsolatedPendingStore implements PendingOperationStore {
  readonly rows = new Map<string, DurableCommand>();
  readonly deletionConnections: string[] = [];

  constructor(command: DurableCommand) {
    this.rows.set(command.opId, command);
  }

  load(_boardId: string): Promise<PendingLoadResult> {
    void _boardId;
    return Promise.resolve({ commands: [...this.rows.values()], corruptCount: 0 });
  }

  put(command: DurableCommand): Promise<void> {
    this.rows.set(command.opId, command);
    return Promise.resolve();
  }

  delete(_boardId: string, operationId: string): Promise<void> {
    void _boardId;
    this.deletionConnections.push(useBoardStore.getState().connection);
    this.rows.delete(operationId);
    return Promise.resolve();
  }
}

function objectCommand(
  boardId: string,
  baseSeq: number,
  kind: "rectangle" | "sticky",
  rotation: number,
): DurableCommand {
  const id = crypto.randomUUID();
  const shape = {
    id,
    x: 20 + baseSeq * 15,
    y: 30 + baseSeq * 10,
    width: 140,
    height: 90,
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
        ? { ...shape, kind, fill: "#fef08a", text: "acceptance" }
        : { ...shape, kind, fill: "#818cf8", text: "" },
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
    payload: { x: 77, rotation },
  };
}

async function publishThroughOutbox(eventId: string, publicationOrdinal: number): Promise<void> {
  const outbox = new OutboxRepository(pool);
  const claims = await outbox.claimAvailable({
    owner: "m27-final-acceptance",
    batchSize: 16,
    leaseDurationMs: 30_000,
  });
  const claim = claims.find((candidate) => candidate.eventId === eventId);
  if (!claim) throw new Error("Expected outbox event was not claimed");
  await expect(
    outbox.markPublished({
      eventId,
      leaseToken: claim.leaseToken,
      publicationId: `${publicationOrdinal}-0`,
    }),
  ).resolves.toMatchObject({ outcome: "applied", status: "published" });
}

async function durableSummary(boardId: string) {
  const result = await pool.query<{
    last_seq: string;
    last_delivery_seq: string;
    operation_recovery_floor: string;
    delivery_recovery_floor: string;
    operation_count: string;
    outbox_count: string;
    receipt_count: string;
    snapshot_count: string;
    membership_count: string;
    projection_count: string;
  }>(
    `SELECT b.last_seq, b.last_delivery_seq,
            b.operation_recovery_floor, b.delivery_recovery_floor,
            (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
            (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count,
            (SELECT count(*) FROM board_operation_receipts WHERE board_id = b.id) receipt_count,
            (SELECT count(*) FROM board_snapshots WHERE board_id = b.id) snapshot_count,
            (SELECT count(*) FROM board_members WHERE board_id = b.id) membership_count,
            (SELECT count(*) FROM board_objects WHERE board_id = b.id) projection_count
     FROM boards b WHERE b.id = $1`,
    [boardId],
  );
  const row = result.rows[0];
  if (!row) throw new Error("Board summary is unavailable");
  return row;
}

async function immutableChecksums(boardId: string) {
  const result = await pool.query<{
    projection: string;
    memberships: string;
    snapshots: string;
    receipts: string;
  }>(
    `SELECT
       (SELECT md5(COALESCE(string_agg(to_jsonb(row_value)::text, ',' ORDER BY object_id), ''))
          FROM (SELECT * FROM board_objects WHERE board_id = $1) row_value) projection,
       (SELECT md5(COALESCE(string_agg(to_jsonb(row_value)::text, ',' ORDER BY user_id), ''))
          FROM (SELECT * FROM board_members WHERE board_id = $1) row_value) memberships,
       (SELECT md5(COALESCE(string_agg(to_jsonb(row_value)::text, ',' ORDER BY id), ''))
          FROM (SELECT * FROM board_snapshots WHERE board_id = $1) row_value) snapshots,
       (SELECT md5(COALESCE(string_agg(to_jsonb(row_value)::text, ',' ORDER BY operation_id), ''))
          FROM (SELECT * FROM board_operation_receipts WHERE board_id = $1) row_value) receipts`,
    [boardId],
  );
  const row = result.rows[0];
  if (!row) throw new Error("Board checksums are unavailable");
  return row;
}

function authenticatedFetch(
  requestedUrls: string[],
): (input: RequestInfo | URL, init?: RequestInit) => Promise<Response> {
  return (input, init) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    requestedUrls.push(url);
    const headers = new Headers(init?.headers);
    for (const [key, value] of Object.entries(testAuthorizationHeaders(token)))
      headers.set(key, value);
    return fetch(input, { ...init, headers });
  };
}

beforeAll(async () => {
  databaseName = `converge_m27_final_${process.pid}_${crypto
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
    new TestAuthAdapter(new Map([[token, owner]])),
  );
  await api.app.listen({ host: "127.0.0.1", port: 0 });
  apiUrl = `http://127.0.0.1:${(api.app.server.address() as AddressInfo).port}`;
  const migration = await pool.query<{ name: string }>(
    "SELECT name FROM converge_migrations ORDER BY name DESC LIMIT 1",
  );
  expect(migration.rows[0]?.name).toBe("0009_board_replay_receipts_and_recovery_floors.sql");
});

afterAll(async () => {
  transport?.disconnect();
  transport?.disconnect();
  socket?.disconnect();
  const session = useBoardStore.getState().sessionToken;
  if (session) useBoardStore.getState().endSession(session);
  await api?.app.close();
  await api?.app.close();
  await pool?.end();
  try {
    if (adminPool && databaseName) await adminPool.query(`DROP DATABASE "${databaseName}"`);
  } finally {
    await adminPool?.end();
  }
});

describe("M2.7 bounded destructive compaction acceptance", () => {
  it("keeps activation explicitly opt-in", () => {
    const required = {
      NODE_ENV: "test",
      DATABASE_URL: isolatedDatabaseUrl,
      REDIS_URL: "redis://127.0.0.1:1",
    };
    expect(parseWorkerEnvironment(required).COMPACTION_ENABLED).toBe(false);
    expect(
      parseWorkerEnvironment({ ...required, COMPACTION_ENABLED: "true" }).COMPACTION_ENABLED,
    ).toBe(true);
    expect(() => parseWorkerEnvironment({ ...required, COMPACTION_ENABLED: "TRUE" })).toThrow(
      "Invalid worker environment configuration: COMPACTION_ENABLED",
    );
  });

  it("compacts one safety-delayed generation and recovers through receipts and HTTP", async () => {
    const boardId = (await boards.createBoard(owner.id, `m27-final-${crypto.randomUUID()}`)).id;
    const rectangle = objectCommand(boardId, 0, "rectangle", 7);
    const first = await boards.commitOperation(owner.id, rectangle);
    await publishThroughOutbox(first.event.eventId, 1);
    const boundary = await snapshots.create(boardId);

    const sticky = objectCommand(boardId, 1, "sticky", -11);
    const second = await boards.commitOperation(owner.id, sticky);
    await publishThroughOutbox(second.event.eventId, 2);
    const newest = await snapshots.create(boardId);

    const rotated = transformCommand(boardId, 2, rectangle.targetId, 43);
    const third = await boards.commitOperation(owner.id, rotated);
    await publishThroughOutbox(third.event.eventId, 3);

    const candidateRepository = new BoardCompactionCandidateRepository(pool);
    await expect(
      candidateRepository.discover({ cursor: null, scanLimit: 100, resultLimit: 16 }),
    ).resolves.toMatchObject({
      candidates: [
        {
          boardId,
          snapshotId: boundary.id,
          snapshotCanvasSeq: 1,
          snapshotDeliverySeq: 1,
          operationRecoveryFloor: 0,
          deliveryRecoveryFloor: 0,
          canvasHead: 3,
          deliveryHead: 3,
        },
      ],
    });

    const checksumsBefore = await immutableChecksums(boardId);
    const scheduler = new ManualScheduler();
    const compacted: unknown[] = [];
    const coordinator = new CompactionCoordinator({
      candidates: candidateRepository,
      compaction: new BoardCompactionRepository(pool),
      scheduler,
      random: { next: () => 0.5 },
      clock: { now: () => 10_000 },
      hooks: {
        compacted: (result) => {
          compacted.push(result);
        },
      },
    });
    await coordinator.start();
    await coordinator.runCycle();
    expect(compacted).toEqual([
      expect.objectContaining({
        boardId,
        snapshotId: boundary.id,
        deletedOperationCount: 1,
        deletedOutboxCount: 1,
        newOperationFloor: 1,
        newDeliveryFloor: 1,
      }),
    ]);
    const stop = coordinator.stop();
    expect(coordinator.stop()).toBe(stop);
    await stop;
    expect(scheduler.tasks.size).toBe(0);

    expect(await durableSummary(boardId)).toEqual({
      last_seq: "3",
      last_delivery_seq: "3",
      operation_recovery_floor: "1",
      delivery_recovery_floor: "1",
      operation_count: "2",
      outbox_count: "2",
      receipt_count: "3",
      snapshot_count: "2",
      membership_count: "1",
      projection_count: "2",
    });
    expect(await immutableChecksums(boardId)).toEqual(checksumsBefore);
    expect(
      (
        await pool.query<{ seq: string }>(
          "SELECT seq FROM board_operations WHERE board_id = $1 ORDER BY seq",
          [boardId],
        )
      ).rows,
    ).toEqual([{ seq: "2" }, { seq: "3" }]);

    const beforeReplay = await durableSummary(boardId);
    await expect(boards.commitOperation(owner.id, rectangle)).resolves.toEqual({
      duplicate: true,
      operation: first.operation,
      event: first.event,
    });
    expect(await durableSummary(boardId)).toEqual(beforeReplay);
    await expect(
      boards.commitOperation(owner.id, {
        ...rectangle,
        clientTimestamp: new Date(Date.now() + 1_000).toISOString(),
      }),
    ).rejects.toMatchObject({ code: "IDEMPOTENCY_CONFLICT" });

    await expect(boards.getOperationBatch(boardId, owner.id, 0, 3, 100)).resolves.toEqual({
      outcome: "range_below_floor",
      boardId,
    });
    const rangeResponse = await fetch(
      `${apiUrl}/v1/boards/${boardId}/operations?after=0&watermark=3`,
      { headers: testAuthorizationHeaders(token) },
    );
    expect(rangeResponse.status).toBe(409);
    expect(protocolErrorSchema.parse(await rangeResponse.json())).toEqual({
      ok: false,
      code: "RESYNC_REQUIRED",
      message: "Operation range is unavailable",
      retryable: true,
    });

    const recoveryResponse = await fetch(`${apiUrl}/v1/boards/${boardId}/recovery`, {
      headers: testAuthorizationHeaders(token),
    });
    const material = boardRecoveryMaterialSchema.parse(await recoveryResponse.json());
    expect(material).toMatchObject({
      snapshotId: newest.id,
      snapshotCanvasSeq: 2,
      snapshotDeliverySeq: 2,
      capturedCanvasSeq: 3,
      capturedDeliverySeq: 3,
      operationTail: [{ seq: 3 }],
    });
    expect(material.snapshotCanonicalHash).toBe(newest.canonicalHash);

    const pending = transformCommand(boardId, 3, sticky.targetId, 29);
    const pendingStore = new IsolatedPendingStore(pending);
    const sessionToken: BoardSessionToken = {
      generation: 270_001,
      nonce: Symbol("m27-final-acceptance"),
    };
    const initial: BoardSnapshot = { id: boardId, name: "pre-compaction", lastSeq: 0, objects: [] };
    useBoardStore.getState().beginSession(sessionToken, boardId);
    useBoardStore.getState().initializeSession(sessionToken, initial, [pending]);
    const transportScheduler = new ManualScheduler();
    const requestedUrls: string[] = [];
    transport = new BoardTransport(boardId, crypto.randomUUID(), sessionToken, {
      apiUrl,
      scheduler: transportScheduler,
      pendingStore,
      fetcher: authenticatedFetch(requestedUrls),
      socketFactory: (url) => {
        socket = createTestSocket(url, token);
        return socket as never;
      },
    });
    transport.connect();
    await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("retry-wait"));
    expect(useBoardStore.getState()).toMatchObject({
      committed: { lastSeq: 3, order: [rectangle.targetId, sticky.targetId] },
      objects: [
        { id: rectangle.targetId, rotation: 43 },
        { id: sticky.targetId, rotation: 29 },
      ],
      pending: [{ opId: pending.opId }],
      authoritativeHash: { status: "ready", seq: 3, value: material.reconstructedCanonicalHash },
    });
    expect(useBoardStore.getState().committed.objects[sticky.targetId]?.value.rotation).toBe(-11);
    expect(pendingStore.rows.has(pending.opId)).toBe(true);
    expect(await hashBoardState(useBoardStore.getState().committed)).toBe(
      material.reconstructedCanonicalHash,
    );
    expect(requestedUrls.filter((url) => url.includes("/operations?"))).toHaveLength(1);
    expect(requestedUrls.filter((url) => url.endsWith("/recovery"))).toHaveLength(1);
    expect(
      requestedUrls.some(
        (url) => url.endsWith(`/v1/boards/${boardId}`) && !url.endsWith("/recovery"),
      ),
    ).toBe(false);

    const retryDelay = useBoardStore.getState().synchronizationDiagnostics.retryDelayMs;
    if (retryDelay === null) throw new Error("Transport did not schedule its bounded reconnect");
    transportScheduler.runDelay(retryDelay);
    await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("ready"));
    await vi.waitFor(() => expect(pendingStore.rows.has(pending.opId)).toBe(false));
    expect(pendingStore.deletionConnections).toEqual(["ready"]);

    const authoritative = await recovery.load(boardId);
    await vi.waitFor(() => expect(useBoardStore.getState().committed.lastSeq).toBe(4));
    expect(useBoardStore.getState().committed.order).toEqual([rectangle.targetId, sticky.targetId]);
    expect(useBoardStore.getState().objects.map(({ id, rotation }) => ({ id, rotation }))).toEqual([
      { id: rectangle.targetId, rotation: 43 },
      { id: sticky.targetId, rotation: 29 },
    ]);
    expect(await hashBoardState(useBoardStore.getState().committed)).toBe(
      authoritative.reconstructedHash,
    );
  });
});
