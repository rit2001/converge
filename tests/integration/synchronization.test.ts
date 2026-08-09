import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { buildApp, type AppContext, type SynchronizationHooks } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import type { Environment } from "@converge/api/env";
import { BoardRepository, createPool } from "@converge/database";
import {
  boardSnapshotSchema,
  operationRangeResponseSchema,
  protocolErrorSchema,
  type CommittedOperation,
  type DurableCommand,
  type JoinBoardAck,
  type OperationAck,
} from "@converge/protocol";
import {
  createRectangleCommand,
  createTestSocket,
  TestAuthAdapter,
  testAuthorizationHeaders,
} from "@converge/testkit";
import type { BoardSessionToken } from "../../apps/web/src/board-session";
import { useBoardStore } from "../../apps/web/src/board-store";
import type { RetryScheduler } from "../../apps/web/src/pending-command-queue";
import type { PendingLoadResult, PendingOperationStore } from "../../apps/web/src/pending-db";
import { BoardTransport, SYNC_ACK_TIMEOUT_MS } from "../../apps/web/src/transport";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const pool = createPool(databaseUrl);
const repository = new BoardRepository(pool);

const identities = {
  owner: { id: "00000000-0000-4000-8000-000000000041", displayName: "Sync Owner" },
  editor: { id: "00000000-0000-4000-8000-000000000042", displayName: "Sync Editor" },
  viewer: { id: "00000000-0000-4000-8000-000000000043", displayName: "Sync Viewer" },
  outsider: { id: "00000000-0000-4000-8000-000000000044", displayName: "Sync Outsider" },
} as const;

const tokens = {
  owner: "sync-owner-token",
  editor: "sync-editor-token",
  viewer: "sync-viewer-token",
  outsider: "sync-outsider-token",
} as const;

const environment: Environment = {
  NODE_ENV: "test",
  HOST: "127.0.0.1",
  API_PORT: 4000,
  WEB_ORIGIN: "http://127.0.0.1:3000",
  DATABASE_URL: databaseUrl,
  REDIS_URL: "redis://127.0.0.1:6379",
  LOG_LEVEL: "silent",
  DEV_AUTH_USER_NAME: "Unused development identity",
};

const auth = new TestAuthAdapter(
  new Map<string, AuthenticatedPrincipal>([
    [tokens.owner, identities.owner],
    [tokens.editor, identities.editor],
    [tokens.viewer, identities.viewer],
    [tokens.outsider, identities.outsider],
  ]),
);

let activeJoinHook: SynchronizationHooks["afterRoomJoin"];
let context: AppContext;
let serverUrl: string;
type TestSocket = ReturnType<typeof createTestSocket>;
const sockets = new Set<TestSocket>();

beforeAll(async () => {
  await pool.query("SELECT 1");
  context = await buildApp(environment, pool, auth, {
    synchronizationBatchSize: 2,
    synchronizationHooks: {
      afterRoomJoin: async (hookContext) => activeJoinHook?.(hookContext),
    },
  });
  await context.app.listen({ host: "127.0.0.1", port: 0 });
  const address = context.app.server.address() as AddressInfo;
  serverUrl = `http://127.0.0.1:${address.port}`;
});

afterAll(async () => {
  activeJoinHook = undefined;
  for (const socket of sockets) socket.disconnect();
  await context.io.close();
  await context.app.close();
  await pool.end();
});

async function board(withMembers = false): Promise<string> {
  const snapshot = await repository.createBoard(
    identities.owner.id,
    `synchronization-${crypto.randomUUID()}`,
  );
  if (withMembers)
    await pool.query(
      `INSERT INTO board_members(board_id, user_id, role)
       VALUES ($1, $2, 'editor'), ($1, $3, 'viewer')`,
      [snapshot.id, identities.editor.id, identities.viewer.id],
    );
  return snapshot.id;
}

function connect(token: string): Promise<TestSocket> {
  const socket = createTestSocket(serverUrl, token);
  sockets.add(socket);
  return new Promise((resolve, reject) => {
    socket.once("connect", () => resolve(socket));
    socket.once("connect_error", reject);
    socket.connect();
  });
}

function join(socket: TestSocket, boardId: string, lastAppliedSeq: number): Promise<JoinBoardAck> {
  return new Promise((resolve) =>
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
  );
}

function submit(socket: TestSocket, command: DurableCommand): Promise<OperationAck> {
  return new Promise((resolve) => socket.emit("operation:submit", command, resolve));
}

function nextLiveOperation(socket: TestSocket): Promise<CommittedOperation> {
  return new Promise((resolve) => socket.once("operation:committed", resolve));
}

async function range(boardId: string, token: string, after: number, watermark: number) {
  return context.app.inject({
    method: "GET",
    url: `/v1/boards/${boardId}/operations?after=${after}&watermark=${watermark}`,
    headers: testAuthorizationHeaders(token),
  });
}

async function catchUp(boardId: string, token: string, after: number, watermark: number) {
  const batches = [];
  const operations: CommittedOperation[] = [];
  let cursor = after;
  while (cursor < watermark) {
    const response = await range(boardId, token, cursor, watermark);
    expect(response.statusCode).toBe(200);
    const batch = operationRangeResponseSchema.parse(response.json());
    batches.push(batch);
    operations.push(...batch.operations);
    cursor = batch.nextSeq;
    if (!batch.hasMore) break;
  }
  return { batches, operations, cursor };
}

async function persistedState(boardId: string) {
  const result = await pool.query<{
    last_seq: string;
    operation_count: string;
    objects: unknown;
    outbox_count: string;
  }>(
    `SELECT b.last_seq,
            (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
            COALESCE(
              (SELECT jsonb_agg(o.object_data ORDER BY o.object_id)
               FROM board_objects o WHERE o.board_id = b.id),
              '[]'::jsonb
            ) objects,
            (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count
     FROM boards b WHERE b.id = $1`,
    [boardId],
  );
  return result.rows[0];
}

class IntegrationScheduler implements RetryScheduler {
  private nextId = 1;
  private readonly tasks = new Map<number, { delay: number; callback: () => void }>();

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
    const entry = [...this.tasks].find(([, task]) => task.delay === delay);
    if (!entry) throw new Error(`No integration task scheduled for ${delay}ms`);
    this.tasks.delete(entry[0]);
    entry[1].callback();
  }
}

class IntegrationPendingStore implements PendingOperationStore {
  load(): Promise<PendingLoadResult> {
    return Promise.resolve({ commands: [], corruptCount: 0 });
  }

  put(): Promise<void> {
    return Promise.resolve();
  }

  delete(): Promise<void> {
    return Promise.resolve();
  }
}

describe("join watermark catch-up", () => {
  it("self-heals a delayed real Socket.IO join acknowledgement without reconnecting", async () => {
    const synchronizedBoardId = await board();
    let releaseFirstJoin!: () => void;
    let markFirstJoinEntered!: () => void;
    const firstJoinEntered = new Promise<void>((resolve) => {
      markFirstJoinEntered = resolve;
    });
    const firstJoinReleased = new Promise<void>((resolve) => {
      releaseFirstJoin = resolve;
    });
    let joinCalls = 0;
    activeJoinHook = async () => {
      joinCalls += 1;
      if (joinCalls !== 1) return;
      markFirstJoinEntered();
      await firstJoinReleased;
    };

    const token: BoardSessionToken = { generation: 70_001, nonce: Symbol("integration-sync") };
    useBoardStore.getState().beginSession(token, synchronizedBoardId);
    useBoardStore
      .getState()
      .initializeSession(
        token,
        { id: synchronizedBoardId, name: "retry integration", lastSeq: 0, objects: [] },
        [],
      );
    const scheduler = new IntegrationScheduler();
    const transport = new BoardTransport(synchronizedBoardId, crypto.randomUUID(), token, {
      apiUrl: serverUrl,
      scheduler,
      pendingStore: new IntegrationPendingStore(),
      socketFactory: () => {
        const socket = createTestSocket(serverUrl, tokens.owner);
        sockets.add(socket);
        return socket as never;
      },
    });
    transport.connect();
    await firstJoinEntered;
    scheduler.runDelay(SYNC_ACK_TIMEOUT_MS);
    await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("retry-wait"));
    scheduler.runDelay(500);
    await vi.waitFor(() => {
      expect(joinCalls).toBe(2);
      expect(useBoardStore.getState().connection).toBe("ready");
    });

    releaseFirstJoin();
    await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("ready"));
    expect(useBoardStore.getState()).toMatchObject({
      boardId: synchronizedBoardId,
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: { attempt: 2, retryScheduled: false },
    });
    transport.disconnect();
    useBoardStore.getState().endSession(token);
    activeJoinHook = undefined;
  });

  it("closes an initial snapshot gap without requiring a later operation", async () => {
    const boardId = await board();
    const snapshot = boardSnapshotSchema.parse(
      await repository.getBoard(boardId, identities.owner.id),
    );
    expect(snapshot.lastSeq).toBe(0);
    const command = createRectangleCommand(boardId);
    await repository.commitOperation(identities.owner.id, command);

    const socket = await connect(tokens.owner);
    const acknowledgement = await join(socket, boardId, snapshot.lastSeq);
    expect(acknowledgement).toEqual({ ok: true, boardId, joinWatermark: 1 });
    const caughtUp = await catchUp(boardId, tokens.owner, snapshot.lastSeq, 1);
    expect(caughtUp.operations).toMatchObject([{ seq: 1, opId: command.opId }]);
    expect(caughtUp.cursor).toBe(1);
    expect((await repository.getBoard(boardId, identities.owner.id)).objects).toEqual([
      command.payload,
    ]);
  });

  it("catches up two operations after reconnect without a trigger mutation", async () => {
    const boardId = await board();
    const reader = await connect(tokens.owner);
    const writer = await connect(tokens.owner);
    await expect(join(reader, boardId, 0)).resolves.toMatchObject({ ok: true, joinWatermark: 0 });
    await expect(join(writer, boardId, 0)).resolves.toMatchObject({ ok: true, joinWatermark: 0 });

    const firstCommand = createRectangleCommand(boardId);
    const firstLive = nextLiveOperation(reader);
    await submit(writer, firstCommand);
    expect((await firstLive).seq).toBe(1);
    reader.disconnect();

    const secondCommand = createRectangleCommand(boardId);
    const thirdCommand = createRectangleCommand(boardId);
    await submit(writer, secondCommand);
    await submit(writer, thirdCommand);

    const reconnected = await connect(tokens.owner);
    const acknowledgement = await join(reconnected, boardId, 1);
    expect(acknowledgement).toEqual({ ok: true, boardId, joinWatermark: 3 });
    const caughtUp = await catchUp(boardId, tokens.owner, 1, 3);
    expect(caughtUp.operations.map((operation) => operation.seq)).toEqual([2, 3]);
    expect(caughtUp.cursor).toBe(3);
  });

  it("handles a commit in the room-join/watermark window as a harmless duplicate", async () => {
    const boardId = await board();
    const writer = await connect(tokens.owner);
    await join(writer, boardId, 0);
    const reader = await connect(tokens.owner);
    const command = createRectangleCommand(boardId);
    const live = nextLiveOperation(reader);
    activeJoinHook = async ({ socketId }) => {
      if (socketId !== reader.id) return;
      activeJoinHook = undefined;
      const acknowledgement = await submit(writer, command);
      expect(acknowledgement).toMatchObject({ ok: true, operation: { seq: 1 } });
    };

    const acknowledgement = await join(reader, boardId, 0);
    expect(acknowledgement).toEqual({ ok: true, boardId, joinWatermark: 1 });
    const liveOperation = await live;
    const caughtUp = await catchUp(boardId, tokens.owner, 0, 1);
    expect(caughtUp.operations[0]).toMatchObject({ seq: 1, opId: command.opId });
    expect(liveOperation).toMatchObject({ seq: 1, opId: command.opId });
    const applied = new Set<string>();
    for (const operation of [...caughtUp.operations, liveOperation]) applied.add(operation.opId);
    expect(applied).toEqual(new Set([command.opId]));
  });

  it("buffers a live operation until fixed-watermark catch-up completes", async () => {
    const boardId = await board();
    for (let index = 0; index < 3; index += 1)
      await repository.commitOperation(identities.owner.id, createRectangleCommand(boardId));
    const writer = await connect(tokens.owner);
    await join(writer, boardId, 3);
    const reader = await connect(tokens.owner);
    const joined = await join(reader, boardId, 0);
    expect(joined).toEqual({ ok: true, boardId, joinWatermark: 3 });

    const live = nextLiveOperation(reader);
    await submit(writer, createRectangleCommand(boardId));
    const bufferedLive = await live;
    expect(bufferedLive.seq).toBe(4);
    const caughtUp = await catchUp(boardId, tokens.owner, 0, 3);
    expect(caughtUp.operations.map((operation) => operation.seq)).toEqual([1, 2, 3]);
    expect([...caughtUp.operations, bufferedLive].map((operation) => operation.seq)).toEqual([
      1, 2, 3, 4,
    ]);
  });

  it("uses bounded ordered batches and leaves persistence unchanged", async () => {
    const boardId = await board();
    for (let index = 0; index < 5; index += 1)
      await repository.commitOperation(identities.owner.id, createRectangleCommand(boardId));
    const before = await persistedState(boardId);
    const socket = await connect(tokens.owner);
    const joined = await join(socket, boardId, 0);
    expect(joined).toEqual({ ok: true, boardId, joinWatermark: 5 });
    const caughtUp = await catchUp(boardId, tokens.owner, 0, 5);
    expect(caughtUp.batches.map((batch) => batch.operations.length)).toEqual([2, 2, 1]);
    expect(caughtUp.operations.map((operation) => operation.seq)).toEqual([1, 2, 3, 4, 5]);
    expect(caughtUp.batches.map((batch) => batch.afterSeq)).toEqual([0, 2, 4]);
    expect(await persistedState(boardId)).toEqual(before);
  });

  it("returns protocol errors for unavailable ranges and lets the transport rebase", async () => {
    const boardId = await board();
    const staleSnapshot = boardSnapshotSchema.parse(
      await repository.getBoard(boardId, identities.owner.id),
    );
    await repository.commitOperation(identities.owner.id, createRectangleCommand(boardId));
    await pool.query("DELETE FROM board_operations WHERE board_id = $1", [boardId]);

    const unavailable = await range(boardId, tokens.owner, 0, 1);
    expect(unavailable.statusCode).toBe(409);
    expect(protocolErrorSchema.parse(unavailable.json())).toEqual({
      ok: false,
      code: "RESYNC_REQUIRED",
      message: "Operation range is unavailable",
      retryable: true,
    });

    const token: BoardSessionToken = { generation: 70_002, nonce: Symbol("range-rebase") };
    useBoardStore.getState().beginSession(token, boardId);
    useBoardStore.getState().initializeSession(token, staleSnapshot, []);
    const scheduler = new IntegrationScheduler();
    const requestedUrls: string[] = [];
    const transport = new BoardTransport(boardId, crypto.randomUUID(), token, {
      apiUrl: serverUrl,
      scheduler,
      pendingStore: new IntegrationPendingStore(),
      socketFactory: () => {
        const socket = createTestSocket(serverUrl, tokens.owner);
        sockets.add(socket);
        return socket as never;
      },
      fetcher: (input, init) => {
        requestedUrls.push(
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url,
        );
        return fetch(input, {
          ...init,
          headers: testAuthorizationHeaders(tokens.owner),
        });
      },
    });
    try {
      transport.connect();
      await vi.waitFor(() => {
        expect(useBoardStore.getState()).toMatchObject({
          connection: "retry-wait",
          committed: { lastSeq: 1 },
          synchronizationDiagnostics: { retryCode: "RESYNC_REQUIRED" },
        });
      });
      expect(requestedUrls.filter((url) => url.includes("/operations?"))).toHaveLength(1);
      expect(requestedUrls.filter((url) => !url.includes("/operations?"))).toHaveLength(1);

      scheduler.runDelay(500);
      await vi.waitFor(() => expect(useBoardStore.getState().connection).toBe("ready"));
      expect(requestedUrls.filter((url) => url.includes("/operations?"))).toHaveLength(1);
    } finally {
      transport.disconnect();
      useBoardStore.getState().endSession(token);
    }
  });

  it("rejects a client sequence ahead of the board head without writes", async () => {
    const boardId = await board();
    const before = await persistedState(boardId);
    const socket = await connect(tokens.owner);
    const acknowledgement = await join(socket, boardId, 1);
    expect(acknowledgement).toMatchObject({
      ok: false,
      code: "RESYNC_REQUIRED",
      retryable: true,
    });
    expect(await persistedState(boardId)).toEqual(before);
  });

  it("rejects unknown join/range fields and malformed sequences without writes", async () => {
    const boardId = await board();
    const before = await persistedState(boardId);
    const socket = await connect(tokens.owner);
    const invalidJoin = await new Promise<JoinBoardAck>((resolve) => {
      const request = {
        schemaVersion: 1 as const,
        boardId,
        clientId: crypto.randomUUID(),
        lastAppliedSeq: 0,
        surprise: true,
      };
      socket.emit("board:join", request, resolve);
    });
    expect(invalidJoin).toMatchObject({ ok: false, code: "INVALID_COMMAND", retryable: false });

    const unknownRange = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/operations?after=0&watermark=0&surprise=true`,
      headers: testAuthorizationHeaders(tokens.owner),
    });
    expect(unknownRange.statusCode).toBe(400);
    const malformedRange = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/operations?after=-1&watermark=0`,
      headers: testAuthorizationHeaders(tokens.owner),
    });
    expect(malformedRange.statusCode).toBe(400);
    const expectedValidationError = {
      ok: false as const,
      code: "INVALID_COMMAND" as const,
      message: "Request validation failed",
      retryable: false,
    };
    for (const response of [unknownRange, malformedRange]) {
      expect(protocolErrorSchema.parse(response.json())).toEqual(expectedValidationError);
      for (const forbidden of [
        "ZodError",
        "issues",
        "path",
        "too_small",
        "unrecognized_keys",
        "surprise",
        "-1",
        "/Users/",
        "node_modules",
      ])
        expect(response.body).not.toContain(forbidden);
    }
    expect(await persistedState(boardId)).toEqual(before);
  });
});

describe("catch-up authorization", () => {
  it("keeps HTTP authentication failures structured through catch-up and snapshot rebase", async () => {
    const boardId = await board();
    await repository.commitOperation(identities.owner.id, createRectangleCommand(boardId));
    const before = await persistedState(boardId);
    const expectedFailure = {
      ok: false as const,
      code: "AUTHENTICATION_REQUIRED" as const,
      message: "Authentication required",
      retryable: false,
    };

    const unauthenticatedSnapshot = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers: testAuthorizationHeaders("expired-token"),
    });
    const unauthenticatedRange = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/operations?after=0&watermark=1`,
      headers: testAuthorizationHeaders("expired-token"),
    });
    const missingSnapshot = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${crypto.randomUUID()}`,
      headers: testAuthorizationHeaders("expired-token"),
    });
    const missingRange = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${crypto.randomUUID()}/operations?after=0&watermark=1`,
      headers: testAuthorizationHeaders("expired-token"),
    });
    for (const response of [
      unauthenticatedSnapshot,
      unauthenticatedRange,
      missingSnapshot,
      missingRange,
    ]) {
      expect(response.statusCode).toBe(401);
      expect(protocolErrorSchema.parse(response.json())).toEqual(expectedFailure);
    }
    expect(missingSnapshot.body).toBe(unauthenticatedSnapshot.body);
    expect(missingRange.body).toBe(unauthenticatedRange.body);
    expect(await persistedState(boardId)).toEqual(before);

    const validSnapshot = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers: testAuthorizationHeaders(tokens.owner),
    });
    const validRange = await range(boardId, tokens.owner, 0, 1);
    expect(boardSnapshotSchema.parse(validSnapshot.json())).toMatchObject({ id: boardId });
    expect(operationRangeResponseSchema.parse(validRange.json())).toMatchObject({ boardId });

    const catchUpToken: BoardSessionToken = {
      generation: 70_003,
      nonce: Symbol("expired-range-auth"),
    };
    useBoardStore.getState().beginSession(catchUpToken, boardId);
    useBoardStore
      .getState()
      .initializeSession(
        catchUpToken,
        { id: boardId, name: "expired range", lastSeq: 0, objects: [] },
        [],
      );
    const catchUpTransport = new BoardTransport(boardId, crypto.randomUUID(), catchUpToken, {
      apiUrl: serverUrl,
      scheduler: new IntegrationScheduler(),
      pendingStore: new IntegrationPendingStore(),
      socketFactory: () => {
        const socket = createTestSocket(serverUrl, tokens.owner);
        sockets.add(socket);
        return socket as never;
      },
      fetcher: (input, init) =>
        fetch(input, {
          ...init,
          headers: testAuthorizationHeaders("expired-token"),
        }),
    });
    try {
      catchUpTransport.connect();
      await vi.waitFor(() =>
        expect(useBoardStore.getState()).toMatchObject({
          connection: "authorization-failed",
          synchronizationDiagnostics: {
            retryCode: "AUTHENTICATION_REQUIRED",
            retryScheduled: false,
          },
        }),
      );
      expect(await persistedState(boardId)).toEqual(before);
    } finally {
      catchUpTransport.disconnect();
      useBoardStore.getState().endSession(catchUpToken);
    }

    await pool.query("DELETE FROM board_operations WHERE board_id = $1", [boardId]);
    const afterLogRemoval = await persistedState(boardId);
    const snapshotToken: BoardSessionToken = {
      generation: 70_004,
      nonce: Symbol("expired-snapshot-auth"),
    };
    useBoardStore.getState().beginSession(snapshotToken, boardId);
    useBoardStore
      .getState()
      .initializeSession(
        snapshotToken,
        { id: boardId, name: "expired snapshot", lastSeq: 0, objects: [] },
        [],
      );
    const snapshotTransport = new BoardTransport(boardId, crypto.randomUUID(), snapshotToken, {
      apiUrl: serverUrl,
      scheduler: new IntegrationScheduler(),
      pendingStore: new IntegrationPendingStore(),
      socketFactory: () => {
        const socket = createTestSocket(serverUrl, tokens.owner);
        sockets.add(socket);
        return socket as never;
      },
      fetcher: (input, init) => {
        const url =
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
        return fetch(input, {
          ...init,
          headers: testAuthorizationHeaders(
            url.includes("/operations?") ? tokens.owner : "expired-token",
          ),
        });
      },
    });
    try {
      snapshotTransport.connect();
      await vi.waitFor(() =>
        expect(useBoardStore.getState()).toMatchObject({
          connection: "authorization-failed",
          synchronizationDiagnostics: {
            retryCode: "AUTHENTICATION_REQUIRED",
            retryScheduled: false,
          },
        }),
      );
      expect(await persistedState(boardId)).toEqual(afterLogRemoval);
    } finally {
      snapshotTransport.disconnect();
      useBoardStore.getState().endSession(snapshotToken);
    }
  });

  it("returns the shared protocol error for inaccessible board snapshots", async () => {
    const boardId = await board();
    const successful = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers: testAuthorizationHeaders(tokens.owner),
    });
    expect(successful.statusCode).toBe(200);
    expect(boardSnapshotSchema.parse(successful.json())).toMatchObject({ id: boardId, lastSeq: 0 });

    const inaccessible = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers: testAuthorizationHeaders(tokens.outsider),
    });
    const nonexistent = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${crypto.randomUUID()}`,
      headers: testAuthorizationHeaders(tokens.outsider),
    });
    expect(inaccessible.statusCode).toBe(404);
    expect(protocolErrorSchema.parse(inaccessible.json())).toEqual({
      ok: false,
      code: "BOARD_NOT_FOUND",
      message: "Board not found",
      retryable: false,
    });
    expect({ status: nonexistent.statusCode, body: nonexistent.json<unknown>() }).toEqual({
      status: inaccessible.statusCode,
      body: inaccessible.json<unknown>(),
    });
  });

  it("allows members and denies outsider or unauthenticated range access", async () => {
    const boardId = await board(true);
    await repository.commitOperation(identities.owner.id, createRectangleCommand(boardId));
    for (const token of [tokens.owner, tokens.editor, tokens.viewer]) {
      const socket = await connect(token);
      await expect(join(socket, boardId, 0)).resolves.toEqual({
        ok: true,
        boardId,
        joinWatermark: 1,
      });
      const response = await range(boardId, token, 0, 1);
      expect(response.statusCode).toBe(200);
      expect(operationRangeResponseSchema.parse(response.json()).operations).toHaveLength(1);
    }

    const outsider = await connect(tokens.outsider);
    await expect(join(outsider, boardId, 0)).resolves.toMatchObject({
      ok: false,
      code: "FORBIDDEN",
      retryable: false,
    });
    const outsiderRange = await range(boardId, tokens.outsider, 0, 1);
    expect(outsiderRange.statusCode).toBe(403);
    expect(outsiderRange.json()).toMatchObject({ code: "FORBIDDEN", retryable: false });
    const missingRange = await range(crypto.randomUUID(), tokens.outsider, 0, 1);
    expect({ status: missingRange.statusCode, body: missingRange.json<unknown>() }).toEqual({
      status: outsiderRange.statusCode,
      body: outsiderRange.json<unknown>(),
    });
    const unauthenticatedRange = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}/operations?after=0&watermark=1`,
    });
    expect(unauthenticatedRange.statusCode).toBe(401);
    expect(unauthenticatedRange.json()).toMatchObject({
      code: "AUTHENTICATION_REQUIRED",
      retryable: false,
    });
  });
});
