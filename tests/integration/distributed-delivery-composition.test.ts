import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { buildApp, type AppContext } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import type {
  DeliveryRuntimeEventHandlers,
  DeliveryRuntimeFactory,
  DeliveryRuntimeObserver,
} from "@converge/api/delivery-runtime";
import type { Environment } from "@converge/api/env";
import { BoardRepository, createPool } from "@converge/database";
import {
  boardAccessRevokedEventSchema,
  committedOperationSchema,
  deliveryEnvelopeSchema,
  type BoardAccessRevokedEvent,
  type CommittedOperation,
  type DurableCommand,
  type JoinBoardAck,
  type MembershipRevokedDeliveryEnvelope,
  type OperationAck,
} from "@converge/protocol";
import { createRectangleCommand, createTestSocket, TestAuthAdapter } from "@converge/testkit";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const pool = createPool(databaseUrl);
const repository = new BoardRepository(pool);
const identities = {
  owner: { id: "00000000-0000-4000-8000-000000000081", displayName: "Delivery Owner" },
  editor: { id: "00000000-0000-4000-8000-000000000082", displayName: "Delivery Editor" },
} as const;
const tokens = { owner: "delivery-owner-token", editor: "delivery-editor-token" } as const;
const auth = new TestAuthAdapter(
  new Map<string, AuthenticatedPrincipal>([
    [tokens.owner, identities.owner],
    [tokens.editor, identities.editor],
  ]),
);
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

type TestSocket = ReturnType<typeof createTestSocket>;
let context: AppContext;
let serverUrl: string;
let runtimeHandlers: DeliveryRuntimeEventHandlers | undefined;
const startRuntime = vi.fn(() => Promise.resolve());
const stopRuntime = vi.fn(() => Promise.resolve());
const createRuntime: DeliveryRuntimeFactory = vi.fn(
  (createdHandlers: DeliveryRuntimeEventHandlers, observer: DeliveryRuntimeObserver) => {
    runtimeHandlers = createdHandlers;
    return {
      start: async () => {
        await startRuntime();
        await observer.lifecycle({ state: "established" });
      },
      stop: stopRuntime,
    };
  },
);
const sockets = new Set<TestSocket>();

beforeAll(async () => {
  await pool.query("SELECT 1");
  context = await buildApp(environment, pool, auth, {
    deliveryMode: {
      mode: "distributed",
      createRuntime,
    },
  });
  await context.app.listen({ host: "127.0.0.1", port: 0 });
  serverUrl = `http://127.0.0.1:${(context.app.server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  for (const socket of sockets) socket.disconnect();
  await context.app.close();
  await pool.end();
});

async function createBoard(): Promise<string> {
  const board = await repository.createBoard(
    identities.owner.id,
    `distributed-composition-${crypto.randomUUID()}`,
  );
  return board.id;
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

function join(socket: TestSocket, boardId: string): Promise<JoinBoardAck> {
  return new Promise((resolve) =>
    socket.emit(
      "board:join",
      { schemaVersion: 1, boardId, clientId: crypto.randomUUID(), lastAppliedSeq: 0 },
      resolve,
    ),
  );
}

function submit(socket: TestSocket, command: DurableCommand): Promise<OperationAck> {
  return new Promise((resolve) => socket.emit("operation:submit", command, resolve));
}

function nextOperation(socket: TestSocket): Promise<CommittedOperation> {
  return new Promise((resolve) => socket.once("operation:committed", resolve));
}

function nextRevocation(socket: TestSocket): Promise<BoardAccessRevokedEvent> {
  return new Promise((resolve) =>
    socket.once("board:access-revoked", (event) =>
      resolve(boardAccessRevokedEventSchema.parse(event)),
    ),
  );
}

function roomHas(boardId: string, socket: TestSocket): boolean {
  return context.io.sockets.adapter.rooms.get(`board:${boardId}`)?.has(socket.id ?? "") ?? false;
}

async function operationOutbox(boardId: string) {
  return pool.query<{ payload: unknown }>(
    `SELECT payload FROM outbox_events
     WHERE board_id = $1 AND event_type = 'operation.committed'
     ORDER BY delivery_seq`,
    [boardId],
  );
}

function handlers(): DeliveryRuntimeEventHandlers {
  if (!runtimeHandlers) throw new Error("Expected the distributed runtime handlers");
  return runtimeHandlers;
}

describe("distributed delivery composition", () => {
  it("enforces a consumed membership revocation against matching local sockets", async () => {
    const boardId = await createBoard();
    const otherBoardId = await createBoard();
    await pool.query(
      `INSERT INTO board_members(board_id, user_id, role)
       VALUES ($1, $3, 'editor'), ($2, $3, 'editor')`,
      [boardId, otherBoardId, identities.editor.id],
    );
    const editor = await connect(tokens.editor);
    await Promise.all([
      expect(join(editor, boardId)).resolves.toMatchObject({ ok: true }),
      expect(join(editor, otherBoardId)).resolves.toMatchObject({ ok: true }),
    ]);
    const removal = await repository.removeBoardMember(
      identities.owner.id,
      boardId,
      identities.editor.id,
    );
    if (!removal.event) throw new Error("Expected a committed revocation envelope");

    const revoked = nextRevocation(editor);
    await handlers().membershipRevoked(removal.event);

    await expect(revoked).resolves.toEqual({
      schemaVersion: 1,
      boardId,
      code: "ACCESS_REVOKED",
      message: "Board access was revoked",
    });
    expect(roomHas(boardId, editor)).toBe(false);
    expect(roomHas(otherBoardId, editor)).toBe(true);
    await pool.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
      [boardId, identities.editor.id],
    );
    await expect(join(editor, boardId)).resolves.toMatchObject({
      ok: false,
      code: "FORBIDDEN",
    });
    await expect(submit(editor, createRectangleCommand(boardId))).resolves.toMatchObject({
      ok: false,
      code: "FORBIDDEN",
    });
    expect(roomHas(boardId, editor)).toBe(false);
    await expect(submit(editor, createRectangleCommand(otherBoardId))).resolves.toMatchObject({
      ok: true,
    });
    const unrelatedHttp = await context.app.inject({
      method: "GET",
      url: `/v1/boards/${otherBoardId}`,
      headers: { authorization: `Bearer ${tokens.editor}` },
    });
    expect(unrelatedHttp.statusCode).toBe(200);
  });

  it("rejects malformed revocation input without inferring a target", async () => {
    const boardId = await createBoard();
    await pool.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
      [boardId, identities.editor.id],
    );
    const editor = await connect(tokens.editor);
    await expect(join(editor, boardId)).resolves.toMatchObject({ ok: true });
    const malformed = {
      schemaVersion: 1,
      eventId: crypto.randomUUID(),
      boardId,
      deliverySeq: 1,
      eventType: "board.membership.revoked",
      occurredAt: new Date().toISOString(),
      payload: {
        revokedUserId: "not-a-principal-id",
        initiatedByUserId: identities.owner.id,
      },
    } as unknown as MembershipRevokedDeliveryEnvelope;

    await expect(handlers().membershipRevoked(malformed)).rejects.toThrow();
    expect(roomHas(boardId, editor)).toBe(true);
  });

  it("acknowledges the PostgreSQL commit, suppresses the local shortcut, and emits only consumed envelopes", async () => {
    const boardId = await createBoard();
    const otherBoardId = await createBoard();
    await pool.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
      [boardId, identities.editor.id],
    );
    const editor = await connect(tokens.editor);
    const matchingObserver = await connect(tokens.owner);
    const otherObserver = await connect(tokens.owner);
    await Promise.all([
      join(editor, boardId),
      join(matchingObserver, boardId),
      join(otherObserver, otherBoardId),
    ]);
    const matchingDeliveries: CommittedOperation[] = [];
    let otherDeliveries = 0;
    matchingObserver.on("operation:committed", (operation) =>
      matchingDeliveries.push(committedOperationSchema.parse(operation)),
    );
    otherObserver.on("operation:committed", () => {
      otherDeliveries += 1;
    });
    const command = createRectangleCommand(boardId);

    await expect(submit(editor, command)).resolves.toMatchObject({
      ok: true,
      duplicate: false,
      operation: { opId: command.opId, seq: 1 },
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(matchingDeliveries).toEqual([]);
    expect(otherDeliveries).toBe(0);
    const committedOutbox = await operationOutbox(boardId);
    expect(committedOutbox.rowCount).toBe(1);
    const envelope = deliveryEnvelopeSchema.parse(committedOutbox.rows[0]?.payload);
    if (envelope.eventType !== "operation.committed")
      throw new Error("Expected an operation delivery envelope");

    const consumed = nextOperation(matchingObserver);
    await handlers().operationCommitted(envelope);
    await expect(consumed).resolves.toMatchObject({ opId: command.opId, seq: 1 });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(matchingDeliveries).toHaveLength(1);
    expect(otherDeliveries).toBe(0);

    await expect(submit(editor, command)).resolves.toMatchObject({
      ok: true,
      duplicate: true,
      operation: { opId: command.opId, seq: 1 },
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect((await operationOutbox(boardId)).rowCount).toBe(1);
    expect(matchingDeliveries).toHaveLength(1);
    expect(otherDeliveries).toBe(0);
  });
});
