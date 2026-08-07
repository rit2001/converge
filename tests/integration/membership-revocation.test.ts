import type { AddressInfo } from "node:net";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { buildApp, type AppContext, type DeliveryHooks } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import { BoardDeliveryCoordinator } from "../../apps/api/src/board-delivery-coordinator";
import type { Environment } from "@converge/api/env";
import { createPool, type BoardRepositoryHooks } from "@converge/database";
import {
  boardAccessRevokedEventSchema,
  boardSnapshotSchema,
  membershipRevocationOutboxPayloadSchema,
  removeBoardMemberResponseSchema,
  type BoardAccessRevokedEvent,
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

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:5432/converge";
const pool = createPool(databaseUrl);

const identities = {
  owner: { id: "00000000-0000-4000-8000-000000000071", displayName: "Revocation Owner" },
  editor: { id: "00000000-0000-4000-8000-000000000072", displayName: "Revocation Editor" },
  viewer: { id: "00000000-0000-4000-8000-000000000073", displayName: "Revocation Viewer" },
  outsider: { id: "00000000-0000-4000-8000-000000000074", displayName: "Revocation Outsider" },
} as const;

const tokens = {
  owner: "revocation-owner-token",
  editor: "revocation-editor-token",
  viewer: "revocation-viewer-token",
  outsider: "revocation-outsider-token",
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

type MembershipHook = NonNullable<BoardRepositoryHooks["afterMembershipDelete"]>;
type OperationHook = NonNullable<DeliveryHooks["afterOperationCommit"]>;
type RevocationHook = NonNullable<DeliveryHooks["afterMembershipCommit"]>;
let activeMembershipHook: MembershipHook | undefined;
let activeOperationHook: OperationHook | undefined;
let activeRevocationHook: RevocationHook | undefined;
const coordinator = new BoardDeliveryCoordinator();
let context: AppContext;
let serverUrl: string;
type TestSocket = ReturnType<typeof createTestSocket>;
const sockets = new Set<TestSocket>();

beforeAll(async () => {
  await pool.query("SELECT 1");
  context = await buildApp(environment, pool, auth, {
    deliveryCoordinator: coordinator,
    repositoryHooks: {
      afterMembershipDelete: async (hookContext) => activeMembershipHook?.(hookContext),
    },
    deliveryHooks: {
      afterOperationCommit: async (hookContext) => activeOperationHook?.(hookContext),
      afterMembershipCommit: async (hookContext) => activeRevocationHook?.(hookContext),
    },
  });
  await context.app.listen({ host: "127.0.0.1", port: 0 });
  const address = context.app.server.address() as AddressInfo;
  serverUrl = `http://127.0.0.1:${address.port}`;
});

afterEach(() => {
  activeMembershipHook = undefined;
  activeOperationHook = undefined;
  activeRevocationHook = undefined;
});

afterAll(async () => {
  for (const socket of sockets) socket.disconnect();
  await context.io.close();
  await context.app.close();
  await pool.end();
});

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function createBoard(): Promise<string> {
  const response = await context.app.inject({
    method: "POST",
    url: "/v1/boards",
    headers: testAuthorizationHeaders(tokens.owner),
    payload: { name: `revocation-${crypto.randomUUID()}` },
  });
  const board = boardSnapshotSchema.parse(response.json());
  await pool.query(
    `INSERT INTO board_members(board_id, user_id, role)
     VALUES ($1, $2, 'editor'), ($1, $3, 'viewer')`,
    [board.id, identities.editor.id, identities.viewer.id],
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
      {
        schemaVersion: 1,
        boardId,
        clientId: crypto.randomUUID(),
        lastAppliedSeq: 0,
      },
      resolve,
    ),
  );
}

function submit(socket: TestSocket, command: DurableCommand): Promise<OperationAck> {
  return new Promise((resolve) => socket.emit("operation:submit", command, resolve));
}

function nextRevocation(socket: TestSocket): Promise<BoardAccessRevokedEvent> {
  return new Promise((resolve) =>
    socket.once("board:access-revoked", (event) =>
      resolve(boardAccessRevokedEventSchema.parse(event)),
    ),
  );
}

function nextOperation(socket: TestSocket): Promise<CommittedOperation> {
  return new Promise((resolve) => socket.once("operation:committed", resolve));
}

async function removeMember(
  boardId: string,
  userId: string,
  token?: string,
  options: { query?: string; payload?: Record<string, unknown> } = {},
) {
  return await context.app.inject({
    method: "DELETE",
    url: `/v1/boards/${boardId}/members/${userId}${options.query ?? ""}`,
    ...(token === undefined ? {} : { headers: testAuthorizationHeaders(token) }),
    ...(options.payload === undefined ? {} : { payload: options.payload }),
  });
}

function roomHas(boardId: string, socket: TestSocket): boolean {
  return context.io.sockets.adapter.rooms.get(`board:${boardId}`)?.has(socket.id ?? "") ?? false;
}

async function revocationOutbox(boardId: string) {
  return pool.query<{ board_seq: string | null; event_type: string; payload: unknown }>(
    `SELECT board_seq, event_type, payload FROM outbox_events
     WHERE board_id = $1 AND event_type = 'board.membership.revoked'
     ORDER BY created_at`,
    [boardId],
  );
}

async function membershipExists(boardId: string, userId: string): Promise<boolean> {
  const result = await pool.query(
    "SELECT 1 FROM board_members WHERE board_id = $1 AND user_id = $2",
    [boardId, userId],
  );
  return Boolean(result.rowCount);
}

describe("supported membership revocation", () => {
  it("lets the owner remove an editor atomically, emits one typed outbox event, and evicts the socket", async () => {
    const boardId = await createBoard();
    const editor = await connect(tokens.editor);
    await expect(join(editor, boardId)).resolves.toMatchObject({ ok: true });
    const revoked = nextRevocation(editor);
    const response = await removeMember(boardId, identities.editor.id, tokens.owner);
    expect(response.statusCode).toBe(200);
    const result = removeBoardMemberResponseSchema.parse(response.json());
    expect(result).toMatchObject({
      ok: true,
      boardId,
      userId: identities.editor.id,
      removed: true,
    });
    expect(result.eventId).not.toBeNull();
    await expect(revoked).resolves.toEqual({
      schemaVersion: 1,
      boardId,
      code: "ACCESS_REVOKED",
      message: "Board access was revoked",
    });
    expect(await membershipExists(boardId, identities.editor.id)).toBe(false);
    expect(roomHas(boardId, editor)).toBe(false);
    const outbox = await revocationOutbox(boardId);
    expect(outbox.rowCount).toBe(1);
    expect(outbox.rows[0]).toMatchObject({
      board_seq: null,
      event_type: "board.membership.revoked",
    });
    expect(membershipRevocationOutboxPayloadSchema.parse(outbox.rows[0]?.payload)).toMatchObject({
      eventId: result.eventId,
      boardId,
      revokedUserId: identities.editor.id,
      initiatedByUserId: identities.owner.id,
    });

    const duplicate = await removeMember(boardId, identities.editor.id, tokens.owner);
    expect(removeBoardMemberResponseSchema.parse(duplicate.json())).toMatchObject({
      removed: false,
      eventId: null,
    });
    expect((await revocationOutbox(boardId)).rowCount).toBe(1);
  });

  it("stops post-revocation delivery while authorized board clients continue", async () => {
    const boardId = await createBoard();
    const owner = await connect(tokens.owner);
    const editor = await connect(tokens.editor);
    const viewer = await connect(tokens.viewer);
    await Promise.all([join(owner, boardId), join(editor, boardId), join(viewer, boardId)]);
    await removeMember(boardId, identities.editor.id, tokens.owner);
    expect(roomHas(boardId, editor)).toBe(false);
    let removedDeliveries = 0;
    editor.on("operation:committed", () => {
      removedDeliveries += 1;
    });
    const ownerLive = nextOperation(owner);
    const viewerLive = nextOperation(viewer);
    const acknowledgement = await submit(owner, createRectangleCommand(boardId));
    expect(acknowledgement).toMatchObject({ ok: true, operation: { seq: 1 } });
    await expect(ownerLive).resolves.toMatchObject({ boardId, seq: 1 });
    await expect(viewerLive).resolves.toMatchObject({ boardId, seq: 1 });
    expect(removedDeliveries).toBe(0);
  });

  it("evicts every socket for the revoked principal", async () => {
    const boardId = await createBoard();
    const first = await connect(tokens.editor);
    const second = await connect(tokens.editor);
    await Promise.all([join(first, boardId), join(second, boardId)]);
    const events = [nextRevocation(first), nextRevocation(second)];
    await removeMember(boardId, identities.editor.id, tokens.owner);
    expect((await Promise.all(events)).map((event) => event.code)).toEqual([
      "ACCESS_REVOKED",
      "ACCESS_REVOKED",
    ]);
    expect(roomHas(boardId, first)).toBe(false);
    expect(roomHas(boardId, second)).toBe(false);
  });

  it("isolates revocation to one board and one principal", async () => {
    const boardA = await createBoard();
    const boardB = await createBoard();
    const editor = await connect(tokens.editor);
    const viewer = await connect(tokens.viewer);
    const owner = await connect(tokens.owner);
    await Promise.all([
      join(editor, boardA),
      join(editor, boardB),
      join(viewer, boardA),
      join(owner, boardB),
    ]);
    const revoked = nextRevocation(editor);
    await removeMember(boardA, identities.editor.id, tokens.owner);
    await expect(revoked).resolves.toMatchObject({ boardId: boardA });
    expect(roomHas(boardA, editor)).toBe(false);
    expect(roomHas(boardA, viewer)).toBe(true);
    expect(roomHas(boardB, editor)).toBe(true);
    expect(await membershipExists(boardB, identities.editor.id)).toBe(true);
    const editorLive = nextOperation(editor);
    await expect(submit(owner, createRectangleCommand(boardB))).resolves.toMatchObject({
      ok: true,
    });
    await expect(editorLive).resolves.toMatchObject({ boardId: boardB, seq: 1 });
  });
});

describe("membership removal authorization and rollback", () => {
  it("enforces the role matrix without unauthorized eviction or outbox writes", async () => {
    const boardId = await createBoard();
    const viewer = await connect(tokens.viewer);
    await join(viewer, boardId);
    for (const token of [tokens.editor, tokens.viewer, tokens.outsider]) {
      const response = await removeMember(boardId, identities.viewer.id, token);
      expect(response.statusCode).toBe(403);
      expect(response.json()).toMatchObject({ ok: false, code: "FORBIDDEN", retryable: false });
      expect(roomHas(boardId, viewer)).toBe(true);
    }
    const unauthenticated = await removeMember(boardId, identities.viewer.id);
    expect(unauthenticated.statusCode).toBe(401);
    expect(unauthenticated.json()).toMatchObject({ code: "AUTHENTICATION_REQUIRED" });
    expect(await membershipExists(boardId, identities.viewer.id)).toBe(true);
    expect((await revocationOutbox(boardId)).rowCount).toBe(0);

    const revoked = nextRevocation(viewer);
    const ownerRemoval = await removeMember(boardId, identities.viewer.id, tokens.owner);
    expect(ownerRemoval.statusCode).toBe(200);
    await expect(revoked).resolves.toMatchObject({ code: "ACCESS_REVOKED" });
  });

  it("protects the owner and keeps its membership, room, and outbox unchanged", async () => {
    const boardId = await createBoard();
    const owner = await connect(tokens.owner);
    await join(owner, boardId);
    const response = await removeMember(boardId, identities.owner.id, tokens.owner);
    expect(response.statusCode).toBe(409);
    expect(response.json()).toEqual({
      ok: false,
      code: "CANNOT_REMOVE_OWNER",
      message: "The board owner cannot be removed",
      retryable: false,
    });
    expect(await membershipExists(boardId, identities.owner.id)).toBe(true);
    expect(roomHas(boardId, owner)).toBe(true);
    expect((await revocationOutbox(boardId)).rowCount).toBe(0);
  });

  it("rolls back deletion and outbox insertion before performing eviction", async () => {
    const boardId = await createBoard();
    const editor = await connect(tokens.editor);
    await join(editor, boardId);
    activeMembershipHook = () => Promise.reject(new Error("forced membership rollback"));
    const response = await removeMember(boardId, identities.editor.id, tokens.owner);
    expect(response.statusCode).toBe(500);
    expect(await membershipExists(boardId, identities.editor.id)).toBe(true);
    expect((await revocationOutbox(boardId)).rowCount).toBe(0);
    expect(roomHas(boardId, editor)).toBe(true);
  });

  it("strictly rejects malformed identifiers, query fields, and request bodies", async () => {
    const boardId = await createBoard();
    const malformed = await removeMember("not-a-uuid", identities.editor.id, tokens.owner);
    expect(malformed.statusCode).toBe(400);
    const query = await removeMember(boardId, identities.editor.id, tokens.owner, {
      query: "?surprise=true",
    });
    expect(query.statusCode).toBe(400);
    const body = await removeMember(boardId, identities.editor.id, tokens.owner, {
      payload: { surprise: true },
    });
    expect(body.statusCode).toBe(400);
    expect(await membershipExists(boardId, identities.editor.id)).toBe(true);
    expect((await revocationOutbox(boardId)).rowCount).toBe(0);
  });
});

describe("board-local operation and revocation ordering", () => {
  it("permits an operation that owns the coordinator before revocation, then blocks later delivery", async () => {
    const boardId = await createBoard();
    const owner = await connect(tokens.owner);
    const editor = await connect(tokens.editor);
    await Promise.all([join(owner, boardId), join(editor, boardId)]);
    const operationEntered = deferred();
    const releaseOperation = deferred();
    activeOperationHook = async () => {
      operationEntered.resolve();
      await releaseOperation.promise;
    };
    const preRevocationLive = nextOperation(editor);
    const firstSubmission = submit(owner, createRectangleCommand(boardId));
    await operationEntered.promise;
    const revoked = nextRevocation(editor);
    const removal = removeMember(boardId, identities.editor.id, tokens.owner);
    releaseOperation.resolve();
    await expect(firstSubmission).resolves.toMatchObject({ ok: true, operation: { seq: 1 } });
    await expect(preRevocationLive).resolves.toMatchObject({ seq: 1 });
    expect((await removal).statusCode).toBe(200);
    await expect(revoked).resolves.toMatchObject({ code: "ACCESS_REVOKED" });
    expect(roomHas(boardId, editor)).toBe(false);
    await expect(submit(owner, createRectangleCommand(boardId))).resolves.toMatchObject({
      ok: true,
      operation: { seq: 2 },
    });
  });

  it("commits and evicts a revocation before allowing a later operation publication", async () => {
    const boardId = await createBoard();
    const owner = await connect(tokens.owner);
    const editor = await connect(tokens.editor);
    await Promise.all([join(owner, boardId), join(editor, boardId)]);
    const revocationEntered = deferred();
    const releaseRevocation = deferred();
    activeRevocationHook = async () => {
      revocationEntered.resolve();
      await releaseRevocation.promise;
    };
    const revoked = nextRevocation(editor);
    const removal = removeMember(boardId, identities.editor.id, tokens.owner);
    await revocationEntered.promise;
    let removedDeliveries = 0;
    editor.on("operation:committed", () => {
      removedDeliveries += 1;
    });
    const ownerLive = nextOperation(owner);
    const laterSubmission = submit(owner, createRectangleCommand(boardId));
    releaseRevocation.resolve();
    expect((await removal).statusCode).toBe(200);
    await expect(revoked).resolves.toMatchObject({ code: "ACCESS_REVOKED" });
    await expect(laterSubmission).resolves.toMatchObject({ ok: true, operation: { seq: 1 } });
    await expect(ownerLive).resolves.toMatchObject({ seq: 1 });
    expect(roomHas(boardId, editor)).toBe(false);
    expect(removedDeliveries).toBe(0);
  });

  it("cannot leave a concurrent rejoin subscribed after revocation completes", async () => {
    const boardId = await createBoard();
    const editor = await connect(tokens.editor);
    await join(editor, boardId);
    const revocationEntered = deferred();
    const releaseRevocation = deferred();
    activeRevocationHook = async () => {
      revocationEntered.resolve();
      await releaseRevocation.promise;
    };
    const revoked = nextRevocation(editor);
    const removal = removeMember(boardId, identities.editor.id, tokens.owner);
    await revocationEntered.promise;
    const rejoin = join(editor, boardId);
    releaseRevocation.resolve();
    expect((await removal).statusCode).toBe(200);
    await expect(revoked).resolves.toMatchObject({ code: "ACCESS_REVOKED" });
    await expect(rejoin).resolves.toMatchObject({ ok: false, code: "FORBIDDEN" });
    expect(roomHas(boardId, editor)).toBe(false);
  });

  it("reauthorizes exact replay after revocation and performs no publication", async () => {
    const boardId = await createBoard();
    const owner = await connect(tokens.owner);
    const editor = await connect(tokens.editor);
    await Promise.all([join(owner, boardId), join(editor, boardId)]);
    const command = createRectangleCommand(boardId);
    const originalPublication = nextOperation(owner);
    await expect(submit(editor, command)).resolves.toMatchObject({
      ok: true,
      operation: { seq: 1 },
    });
    await expect(originalPublication).resolves.toMatchObject({ seq: 1 });
    await removeMember(boardId, identities.editor.id, tokens.owner);
    let replayPublications = 0;
    owner.on("operation:committed", () => {
      replayPublications += 1;
    });
    await expect(submit(editor, command)).resolves.toMatchObject({
      ok: false,
      code: "FORBIDDEN",
      retryable: false,
    });
    expect(replayPublications).toBe(0);
    const operations = await pool.query("SELECT 1 FROM board_operations WHERE board_id = $1", [
      boardId,
    ]);
    expect(operations.rowCount).toBe(1);
  });
});
