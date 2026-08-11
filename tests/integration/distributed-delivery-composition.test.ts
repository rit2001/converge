import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { buildApp, type AppContext } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import type {
  DeliveryRuntimeEventHandlers,
  DeliveryRuntimeFactory,
} from "@converge/api/delivery-runtime";
import type { Environment } from "@converge/api/env";
import { BoardRepository, createPool } from "@converge/database";
import {
  committedOperationSchema,
  deliveryEnvelopeSchema,
  type CommittedOperation,
  type DurableCommand,
  type JoinBoardAck,
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
  (createdHandlers: DeliveryRuntimeEventHandlers) => {
    runtimeHandlers = createdHandlers;
    return { start: startRuntime, stop: stopRuntime };
  },
);
const sockets = new Set<TestSocket>();

beforeAll(async () => {
  await pool.query("SELECT 1");
  context = await buildApp(environment, pool, auth, {
    deliveryMode: {
      mode: "distributed",
      createRuntime,
      membershipRevoked: vi.fn(() => Promise.resolve()),
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
