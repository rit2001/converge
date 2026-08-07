import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildApp, type AppContext } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import type { Environment } from "@converge/api/env";
import { createPool } from "@converge/database";
import {
  boardSnapshotSchema,
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

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:5432/converge";
const pool = createPool(databaseUrl);

const identities = {
  owner: {
    id: "00000000-0000-4000-8000-000000000031",
    displayName: "Owner",
  },
  editor: {
    id: "00000000-0000-4000-8000-000000000032",
    displayName: "Editor",
  },
  viewer: {
    id: "00000000-0000-4000-8000-000000000033",
    displayName: "Viewer",
  },
  outsider: {
    id: "00000000-0000-4000-8000-000000000034",
    displayName: "Outsider",
  },
} as const;

const tokens = {
  owner: "opaque-owner-token",
  editor: "opaque-editor-token",
  viewer: "opaque-viewer-token",
  outsider: "opaque-outsider-token",
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

let context: AppContext;
let serverUrl: string;
type TestSocket = ReturnType<typeof createTestSocket>;
const sockets = new Set<TestSocket>();

beforeAll(async () => {
  await pool.query("SELECT 1");
  context = await buildApp(environment, pool, auth);
  await context.app.listen({ host: "127.0.0.1", port: 0 });
  const address = context.app.server.address() as AddressInfo;
  serverUrl = `http://127.0.0.1:${address.port}`;
});

afterAll(async () => {
  for (const socket of sockets) socket.disconnect();
  await context.io.close();
  await context.app.close();
  await pool.end();
});

async function createPrivateBoard(): Promise<string> {
  const response = await context.app.inject({
    method: "POST",
    url: "/v1/boards",
    headers: testAuthorizationHeaders(tokens.owner),
    payload: { name: `authorization-${crypto.randomUUID()}` },
  });
  expect(response.statusCode).toBe(201);
  const board = boardSnapshotSchema.parse(response.json());
  await pool.query(
    `INSERT INTO board_members(board_id, user_id, role)
     VALUES ($1, $2, 'editor'), ($1, $3, 'viewer')`,
    [board.id, identities.editor.id, identities.viewer.id],
  );
  return board.id;
}

function joinRequest(boardId: string) {
  return {
    schemaVersion: 1 as const,
    boardId,
    clientId: "10000000-0000-4000-8000-000000000031",
    lastAppliedSeq: 0,
    pendingOpIds: [],
  };
}

function connectSocket(
  token: string,
  additionalAuth: Record<string, unknown> = {},
): Promise<TestSocket> {
  const socket = createTestSocket(serverUrl, token, additionalAuth);
  sockets.add(socket);
  return new Promise((resolve, reject) => {
    socket.once("connect", () => resolve(socket));
    socket.once("connect_error", reject);
    socket.connect();
  });
}

function rejectedConnection(
  token?: string,
  additionalAuth: Record<string, unknown> = {},
): Promise<Error & { data?: unknown }> {
  const socket = createTestSocket(serverUrl, token, additionalAuth);
  sockets.add(socket);
  return new Promise((resolve, reject) => {
    socket.once("connect", () => reject(new Error("Socket unexpectedly authenticated")));
    socket.once("connect_error", (error) => resolve(error));
    socket.connect();
  });
}

function joinBoard(socket: TestSocket, boardId: string): Promise<JoinBoardAck> {
  return new Promise((resolve) => socket.emit("board:join", joinRequest(boardId), resolve));
}

function submit(socket: TestSocket, command: unknown): Promise<OperationAck> {
  return new Promise((resolve) => socket.emit("operation:submit", command, resolve));
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
              (SELECT jsonb_agg(
                 jsonb_build_object(
                   'object_id', o.object_id,
                   'object_data', o.object_data,
                   'deleted_seq', o.deleted_seq
                 ) ORDER BY o.object_id
               ) FROM board_objects o WHERE o.board_id = b.id),
              '[]'::jsonb
            ) objects,
            (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count
     FROM boards b WHERE b.id = $1`,
    [boardId],
  );
  return result.rows[0];
}

describe("authentication boundaries", () => {
  it("rejects actor overrides and derives board ownership from the authenticated principal", async () => {
    const rejected = await context.app.inject({
      method: "POST",
      url: "/v1/boards",
      headers: testAuthorizationHeaders(tokens.owner),
      payload: {
        name: "caller-selected-owner",
        actorId: identities.outsider.id,
      },
    });
    expect(rejected.statusCode).toBe(400);
    expect(rejected.json()).toMatchObject({ code: "INVALID_COMMAND", retryable: false });

    const response = await context.app.inject({
      method: "POST",
      url: "/v1/boards",
      headers: testAuthorizationHeaders(tokens.owner),
      payload: { name: `principal-owned-${crypto.randomUUID()}` },
    });
    const board = boardSnapshotSchema.parse(response.json());
    const persisted = await pool.query<{ created_by: string }>(
      "SELECT created_by FROM boards WHERE id = $1",
      [board.id],
    );
    expect(persisted.rows[0]?.created_by).toBe(identities.owner.id);

    const socket = await connectSocket(tokens.owner);
    await expect(joinBoard(socket, board.id)).resolves.toEqual({
      ok: true,
      boardId: board.id,
      joinWatermark: 0,
    });
    const before = await persistedState(board.id);
    const command = createRectangleCommand(board.id);
    const forged = await submit(socket, { ...command, actorId: identities.outsider.id });
    expect(forged).toMatchObject({ ok: false, code: "INVALID_COMMAND", retryable: false });
    expect(await persistedState(board.id)).toEqual(before);

    const accepted = await submit(socket, command);
    expect(accepted).toMatchObject({ ok: true, operation: { seq: 1 } });
    const operation = await pool.query<{ user_id: string }>(
      "SELECT user_id FROM board_operations WHERE board_id = $1 AND seq = 1",
      [board.id],
    );
    expect(operation.rows[0]?.user_id).toBe(identities.owner.id);
  });

  it("does not let an outsider impersonate the owner through x-dev-user-id", async () => {
    const boardId = await createPrivateBoard();
    const response = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers: {
        ...testAuthorizationHeaders(tokens.outsider),
        "x-dev-user-id": identities.owner.id,
      },
    });
    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ code: "INVALID_AUTH_INPUT", retryable: false });
  });

  it("rejects Socket.IO caller identity before it can join or mutate", async () => {
    const boardId = await createPrivateBoard();
    const before = await persistedState(boardId);
    const error = await rejectedConnection(tokens.outsider, { userId: identities.owner.id });
    expect(error.data).toMatchObject({ code: "INVALID_AUTH_INPUT", retryable: false });
    expect(await persistedState(boardId)).toEqual(before);
  });
});

describe("HTTP and Socket.IO authorization matrix", () => {
  it("allows an owner to retrieve, join, and mutate a board", async () => {
    const boardId = await createPrivateBoard();
    const response = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers: testAuthorizationHeaders(tokens.owner),
    });
    expect(response.statusCode).toBe(200);
    const socket = await connectSocket(tokens.owner);
    await expect(joinBoard(socket, boardId)).resolves.toEqual({
      ok: true,
      boardId,
      joinWatermark: 0,
    });
    const acknowledgement = await submit(socket, createRectangleCommand(boardId));
    expect(acknowledgement).toMatchObject({ ok: true, operation: { seq: 1 } });
  });

  it("allows an editor to retrieve, join, and mutate a board", async () => {
    const boardId = await createPrivateBoard();
    const response = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers: testAuthorizationHeaders(tokens.editor),
    });
    expect(response.statusCode).toBe(200);
    const socket = await connectSocket(tokens.editor);
    await expect(joinBoard(socket, boardId)).resolves.toEqual({
      ok: true,
      boardId,
      joinWatermark: 0,
    });
    const acknowledgement = await submit(socket, createRectangleCommand(boardId));
    expect(acknowledgement).toMatchObject({ ok: true, operation: { seq: 1 } });
  });

  it("allows a viewer to retrieve and join but rejects every durable mutation", async () => {
    const boardId = await createPrivateBoard();
    const response = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers: testAuthorizationHeaders(tokens.viewer),
    });
    expect(response.statusCode).toBe(200);
    const socket = await connectSocket(tokens.viewer);
    await expect(joinBoard(socket, boardId)).resolves.toEqual({
      ok: true,
      boardId,
      joinWatermark: 0,
    });
    const before = await persistedState(boardId);
    const create = createRectangleCommand(boardId);
    const commands: DurableCommand[] = [
      create,
      {
        ...create,
        opId: crypto.randomUUID(),
        type: "object.update",
        payload: { fill: "#ffffff" },
      },
      {
        ...create,
        opId: crypto.randomUUID(),
        type: "object.transform",
        payload: { x: 80 },
      },
      {
        ...create,
        opId: crypto.randomUUID(),
        type: "object.delete",
        payload: {},
      },
    ];
    for (const command of commands) {
      const acknowledgement = await submit(socket, command);
      expect(acknowledgement).toMatchObject({ ok: false, code: "FORBIDDEN", retryable: false });
    }
    expect(await persistedState(boardId)).toEqual(before);
  });

  it("denies an outsider and hides existing board status", async () => {
    const boardId = await createPrivateBoard();
    const missingBoardId = crypto.randomUUID();
    const existing = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${boardId}`,
      headers: testAuthorizationHeaders(tokens.outsider),
    });
    const missing = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${missingBoardId}`,
      headers: testAuthorizationHeaders(tokens.outsider),
    });
    const existingBody = existing.json<unknown>();
    const missingBody = missing.json<unknown>();
    expect({ status: existing.statusCode, body: existingBody }).toEqual({
      status: missing.statusCode,
      body: missingBody,
    });
    expect(existing.statusCode).toBe(404);
    expect(existingBody).toEqual({ code: "BOARD_NOT_FOUND", message: "Board not found" });

    const socket = await connectSocket(tokens.outsider);
    const existingJoin = await joinBoard(socket, boardId);
    const missingJoin = await joinBoard(socket, missingBoardId);
    expect(existingJoin).toEqual(missingJoin);
    expect(existingJoin).toMatchObject({ ok: false, code: "FORBIDDEN", retryable: false });
    const before = await persistedState(boardId);
    const acknowledgement = await submit(socket, createRectangleCommand(boardId));
    expect(acknowledgement).toMatchObject({ ok: false, code: "FORBIDDEN", retryable: false });
    expect(await persistedState(boardId)).toEqual(before);
  });

  it("rejects unauthenticated HTTP and Socket.IO clients", async () => {
    const boardId = await createPrivateBoard();
    const response = await context.app.inject({ method: "GET", url: `/v1/boards/${boardId}` });
    expect(response.statusCode).toBe(401);
    expect(response.json()).toMatchObject({
      code: "AUTHENTICATION_REQUIRED",
      retryable: false,
    });
    const error = await rejectedConnection();
    expect(error.data).toMatchObject({ code: "AUTHENTICATION_REQUIRED", retryable: false });
  });

  it("rechecks authorization for a connected editor after membership removal", async () => {
    const boardId = await createPrivateBoard();
    const socket = await connectSocket(tokens.editor);
    await expect(joinBoard(socket, boardId)).resolves.toEqual({
      ok: true,
      boardId,
      joinWatermark: 0,
    });
    await pool.query("DELETE FROM board_members WHERE board_id = $1 AND user_id = $2", [
      boardId,
      identities.editor.id,
    ]);
    const before = await persistedState(boardId);
    const acknowledgement = await submit(socket, createRectangleCommand(boardId));
    expect(acknowledgement).toMatchObject({ ok: false, code: "FORBIDDEN", retryable: false });
    expect(await persistedState(boardId)).toEqual(before);
  });
});
