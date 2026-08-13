import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildApp, type AppContext } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import { parseEnvironment } from "@converge/api/env";
import { BoardRepository, createPool } from "@converge/database";
import {
  committedOperationSchema,
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
  owner: { id: "00000000-0000-4000-8000-000000000061", displayName: "Integrity Owner" },
  editor: { id: "00000000-0000-4000-8000-000000000062", displayName: "Integrity Editor" },
} as const;
const tokens = { owner: "integrity-owner-token", editor: "integrity-editor-token" } as const;
const auth = new TestAuthAdapter(
  new Map<string, AuthenticatedPrincipal>([
    [tokens.owner, identities.owner],
    [tokens.editor, identities.editor],
  ]),
);
const environment = parseEnvironment({
  NODE_ENV: "test",
  HOST: "127.0.0.1",
  API_PORT: "4000",
  WEB_ORIGIN: "http://127.0.0.1:3000",
  DATABASE_URL: databaseUrl,
  REDIS_URL: "redis://127.0.0.1:6379",
  LOG_LEVEL: "silent",
  DEV_AUTH_USER_NAME: "Unused development identity",
});

type TestSocket = ReturnType<typeof createTestSocket>;
let context: AppContext;
let serverUrl: string;
const sockets = new Set<TestSocket>();

beforeAll(async () => {
  await pool.query("SELECT 1");
  context = await buildApp(environment, pool, auth);
  await context.app.listen({ host: "127.0.0.1", port: 0 });
  serverUrl = `http://127.0.0.1:${(context.app.server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  for (const socket of sockets) socket.disconnect();
  await context.app.close();
  await pool.end();
});

async function board(): Promise<string> {
  const created = await repository.createBoard(
    identities.owner.id,
    `submission-integrity-${crypto.randomUUID()}`,
  );
  await pool.query("INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')", [
    created.id,
    identities.editor.id,
  ]);
  return created.id;
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

function join(socket: TestSocket, boardId: string, lastAppliedSeq = 0): Promise<JoinBoardAck> {
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

function nextCommitted(socket: TestSocket): Promise<CommittedOperation> {
  return new Promise((resolve) => socket.once("operation:committed", resolve));
}

async function durableState(boardId: string) {
  const result = await pool.query<{
    last_seq: string;
    last_delivery_seq: string;
    operation_count: string;
    projection: unknown;
    outbox_count: string;
  }>(
    `SELECT b.last_seq, b.last_delivery_seq,
            (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
            COALESCE(
              (SELECT jsonb_agg(to_jsonb(o) - 'board_id' ORDER BY o.object_id)
               FROM board_objects o WHERE o.board_id = b.id),
              '[]'::jsonb
            ) projection,
            (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count
     FROM boards b WHERE b.id = $1`,
    [boardId],
  );
  return result.rows[0];
}

async function boardLockBarrier(boardId: string): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("SELECT pg_advisory_lock(hashtextextended($1, 0))", [boardId]);
    await client.query("SELECT pg_advisory_unlock(hashtextextended($1, 0))", [boardId]);
  } finally {
    client.release();
  }
}

describe("Socket.IO mutation-submission integrity", () => {
  it("ignores a board join without an acknowledgement before room membership", async () => {
    const boardId = await board();
    const editor = await connect(tokens.editor);
    const before = await durableState(boardId);
    const serverSocket = context.io.sockets.sockets.get(editor.id ?? "");
    if (!serverSocket) throw new Error("Expected connected server socket");
    const originalJoin = serverSocket.join.bind(serverSocket);
    let roomJoinCalls = 0;
    serverSocket.join = ((rooms) => {
      roomJoinCalls += 1;
      return originalJoin(rooms);
    }) as typeof serverSocket.join;
    const unhandledRejections: unknown[] = [];
    const recordUnhandledRejection = (reason: unknown) => unhandledRejections.push(reason);
    process.on("unhandledRejection", recordUnhandledRejection);

    try {
      const emitWithoutAcknowledgement = editor.emit.bind(editor) as unknown as (
        event: "board:join",
        request: {
          schemaVersion: 1;
          boardId: string;
          clientId: string;
          lastAppliedSeq: number;
        },
      ) => void;
      emitWithoutAcknowledgement("board:join", {
        schemaVersion: 1,
        boardId,
        clientId: crypto.randomUUID(),
        lastAppliedSeq: 0,
      });

      await expect(join(editor, boardId)).resolves.toMatchObject({
        ok: true,
        boardId,
        joinWatermark: 0,
      });
      await new Promise<void>((resolve) => setImmediate(resolve));

      expect(unhandledRejections).toEqual([]);
      expect(roomJoinCalls).toBe(1);
      expect(serverSocket.rooms.has(`board:${boardId}`)).toBe(true);
      expect(await durableState(boardId)).toEqual(before);
      await expect(context.app.inject({ method: "GET", url: "/health" })).resolves.toMatchObject({
        statusCode: 200,
      });
    } finally {
      process.off("unhandledRejection", recordUnhandledRejection);
      serverSocket.join = originalJoin;
    }
  });

  it("rejects a mutation without an acknowledgement before persistence or broadcast", async () => {
    const boardId = await board();
    const editor = await connect(tokens.editor);
    const observer = await connect(tokens.owner);
    await expect(join(editor, boardId)).resolves.toMatchObject({ ok: true, joinWatermark: 0 });
    await expect(join(observer, boardId)).resolves.toMatchObject({ ok: true, joinWatermark: 0 });
    const before = await durableState(boardId);
    const received: CommittedOperation[] = [];
    observer.on("operation:committed", (operation) =>
      received.push(committedOperationSchema.parse(operation)),
    );
    const emitWithoutAcknowledgement = editor.emit.bind(editor) as unknown as (
      event: "operation:submit",
      command: DurableCommand,
    ) => void;

    emitWithoutAcknowledgement("operation:submit", createRectangleCommand(boardId));
    await join(editor, boardId);
    await boardLockBarrier(boardId);
    await join(observer, boardId);

    expect(await durableState(boardId)).toEqual(before);
    expect(received).toEqual([]);

    const command = createRectangleCommand(boardId);
    const live = nextCommitted(observer);
    const acknowledgement = await submit(editor, command);
    expect(acknowledgement).toMatchObject({
      ok: true,
      duplicate: false,
      operation: { opId: command.opId, seq: 1 },
    });
    await expect(live).resolves.toMatchObject({ opId: command.opId, seq: 1 });
  });

  it("publishes a successful commit before returning its acknowledgement", async () => {
    const boardId = await board();
    const editor = await connect(tokens.editor);
    const observer = await connect(tokens.owner);
    await join(editor, boardId);
    await join(observer, boardId);
    const command = createRectangleCommand(boardId);
    const live = nextCommitted(observer);

    const acknowledgement = await submit(editor, command);
    const published = await live;
    expect(published).toMatchObject({ opId: command.opId, seq: 1 });
    expect(acknowledgement).toMatchObject({
      ok: true,
      duplicate: false,
      operation: { opId: command.opId, seq: 1 },
    });
    expect(await durableState(boardId)).toMatchObject({
      last_seq: "1",
      last_delivery_seq: "1",
      operation_count: "1",
      outbox_count: "1",
    });
  });

  it("rejects a future base sequence through the sole public mutation transport", async () => {
    const boardId = await board();
    const editor = await connect(tokens.editor);
    await join(editor, boardId);
    const before = await durableState(boardId);
    const command = { ...createRectangleCommand(boardId), baseSeq: 9_000 };

    await expect(submit(editor, command)).resolves.toMatchObject({
      ok: false,
      code: "RESYNC_REQUIRED",
      retryable: true,
    });
    expect(await durableState(boardId)).toEqual(before);
  });

  it("replays an exact operation and rejects conflicting operation-id reuse", async () => {
    const boardId = await board();
    const editor = await connect(tokens.editor);
    const observer = await connect(tokens.owner);
    await join(editor, boardId);
    await join(observer, boardId);
    const command = createRectangleCommand(boardId);
    if (command.type !== "object.create" || command.payload.kind !== "rectangle")
      throw new Error("Expected rectangle create command");
    await submit(editor, command);
    const afterFirst = await durableState(boardId);

    const replayedLive = nextCommitted(observer);
    await expect(submit(editor, command)).resolves.toMatchObject({
      ok: true,
      duplicate: true,
      operation: { opId: command.opId, seq: 1 },
    });
    await expect(replayedLive).resolves.toMatchObject({ opId: command.opId, seq: 1 });
    expect(await durableState(boardId)).toEqual(afterFirst);

    const conflicting: DurableCommand = {
      ...command,
      payload: { ...command.payload, fill: "#000000" },
    };
    await expect(submit(editor, conflicting)).resolves.toMatchObject({
      ok: false,
      code: "IDEMPOTENCY_CONFLICT",
      retryable: false,
    });
    expect(await durableState(boardId)).toEqual(afterFirst);
  });

  it("recovers an acknowledgement-lost commit through exact replay without durable duplication", async () => {
    const boardId = await board();
    const editor = await connect(tokens.editor);
    const observer = await connect(tokens.owner);
    await join(editor, boardId);
    await join(observer, boardId);
    const command = createRectangleCommand(boardId);
    const firstLive = nextCommitted(observer);

    editor.emit("operation:submit", command, () => undefined);
    await expect(firstLive).resolves.toMatchObject({ opId: command.opId, seq: 1 });
    const afterUnknownOutcome = await durableState(boardId);
    expect(afterUnknownOutcome).toMatchObject({
      last_seq: "1",
      last_delivery_seq: "1",
      operation_count: "1",
      outbox_count: "1",
    });

    await expect(submit(editor, command)).resolves.toMatchObject({
      ok: true,
      duplicate: true,
      operation: { opId: command.opId, seq: 1 },
    });
    expect(await durableState(boardId)).toEqual(afterUnknownOutcome);
  });
});
