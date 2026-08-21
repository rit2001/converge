import { execFile } from "node:child_process";
import { type AddressInfo } from "node:net";
import { promisify } from "node:util";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { buildApp, type AppContext } from "@converge/api";
import type { AuthenticatedPrincipal } from "@converge/api/auth";
import { parseEnvironment, type Environment } from "@converge/api/env";
import { boardKeys } from "@converge/api/presence-redis-transport";
import { BoardRepository, createPool, type DatabasePool } from "@converge/database";
import {
  boardPresenceSnapshotSchema,
  presenceAvailabilitySchema,
  presenceParticipantLeaveSchema,
  presenceParticipantUpsertSchema,
  type JoinBoardAck,
  type OperationAck,
  type PresenceParticipantLeave,
  type PresenceParticipantUpsert,
} from "@converge/protocol";
import { createRectangleCommand, createTestSocket, TestAuthAdapter } from "@converge/testkit";
import { createClient, type RedisClientType } from "redis";

const runFile = promisify(execFile);
const repositoryRoot = new URL("../..", import.meta.url);
const sharedDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const principal = {
  owner: { id: "10000000-0000-4000-8000-000000000091", displayName: "Presence Owner" },
  editor: { id: "10000000-0000-4000-8000-000000000092", displayName: "Presence Editor" },
  viewer: { id: "10000000-0000-4000-8000-000000000093", displayName: "Presence Viewer" },
} as const;
const token = { owner: "presence-owner", editor: "presence-editor", viewer: "presence-viewer" };
const auth = new TestAuthAdapter(
  new Map<string, AuthenticatedPrincipal>([
    [token.owner, principal.owner],
    [token.editor, principal.editor],
    [token.viewer, principal.viewer],
  ]),
);

class Journal<T> {
  readonly entries: T[] = [];
  push(value: T): void {
    this.entries.push(value);
  }
}
class Probe {
  readonly availability = new Journal<"available" | "unavailable">();
  readonly snapshots = new Journal<ReturnType<typeof boardPresenceSnapshotSchema.parse>>();
  readonly upserts = new Journal<PresenceParticipantUpsert>();
  readonly leaves = new Journal<PresenceParticipantLeave>();
  constructor(readonly socket: ReturnType<typeof createTestSocket>) {
    socket.on("presence:availability", (value) =>
      this.availability.push(presenceAvailabilitySchema.parse(value).status),
    );
    socket.on("board:presence-snapshot", (value) =>
      this.snapshots.push(boardPresenceSnapshotSchema.parse(value)),
    );
    socket.on("presence:participant-upsert", (value) =>
      this.upserts.push(presenceParticipantUpsertSchema.parse(value)),
    );
    socket.on("presence:participant-leave", (value) =>
      this.leaves.push(presenceParticipantLeaveSchema.parse(value)),
    );
  }
}
type Api = { context: AppContext; pool: DatabasePool; url: string; close(): Promise<void> };
let databaseName: string | undefined;
let databaseUrl: string | undefined;
let adminPool: DatabasePool | undefined;
let assertionPool: DatabasePool | undefined;
let redis: RedisClientType | undefined;
const apis = new Set<Api>();
const sockets = new Set<ReturnType<typeof createTestSocket>>();
const boards = new Set<string>();

function environment(): Environment {
  if (!databaseUrl) throw new Error("Missing isolated database URL");
  return parseEnvironment({
    NODE_ENV: "test",
    HOST: "127.0.0.1",
    API_PORT: "4000",
    WEB_ORIGIN: "http://127.0.0.1:3000",
    DATABASE_URL: databaseUrl,
    REDIS_URL: redisUrl,
    API_PRESENCE_ENABLED: "true",
    LOG_LEVEL: "silent",
    DEV_AUTH_USER_NAME: "Unused",
  });
}
async function createApi(): Promise<Api> {
  const pool = createPool(databaseUrl!);
  const context = await buildApp(environment(), pool, auth);
  await context.app.listen({ host: "127.0.0.1", port: 0 });
  const address = context.app.server.address() as AddressInfo;
  let closing: Promise<void> | undefined;
  const api: Api = {
    context,
    pool,
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      (closing ??= (async () => {
        try {
          await context.app.close();
        } finally {
          await pool.end();
        }
      })()),
  };
  apis.add(api);
  return api;
}
async function join(
  api: Api,
  authToken: string,
  boardId: string,
): Promise<{ probe: Probe; ack: JoinBoardAck }> {
  const socket = createTestSocket(api.url, authToken);
  sockets.add(socket);
  const probe = new Probe(socket);
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("connect_error", reject);
    socket.connect();
  });
  const ack = await new Promise<JoinBoardAck>((resolve) =>
    socket.emit(
      "board:join",
      { schemaVersion: 1, boardId, clientId: crypto.randomUUID(), lastAppliedSeq: 0 },
      resolve,
    ),
  );
  return { probe, ack };
}
async function removePresenceKeys(boardId: string): Promise<void> {
  const index = boardKeys(boardId)?.index;
  if (!index || !redis?.isOpen) return;
  const reply: unknown = await redis.sendCommand(["KEYS", `converge:presence:v1:{${boardId}}:*`]);
  if (!Array.isArray(reply) || !reply.every((value) => typeof value === "string"))
    throw new Error("Expected Redis KEYS reply");
  const keys = reply;
  if (keys.length > 0) await redis.del(keys);
}
async function redisClientIds(): Promise<Set<string>> {
  const result: unknown = await redis!.sendCommand(["CLIENT", "LIST"]);
  if (typeof result !== "string") throw new Error("Expected Redis CLIENT LIST text");
  const raw = result;
  return new Set([...raw.matchAll(/(?:^|\n)id=(\d+)/g)].map((match) => match[1]!));
}

beforeAll(async () => {
  databaseName = `converge_presence_${process.pid}_${crypto.randomUUID().replaceAll("-", "").slice(0, 10)}`;
  const adminUrl = new URL(sharedDatabaseUrl);
  adminUrl.pathname = "/postgres";
  adminUrl.searchParams.delete("options");
  adminPool = createPool(adminUrl.toString());
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const isolated = new URL(sharedDatabaseUrl);
  isolated.pathname = `/${databaseName}`;
  isolated.searchParams.delete("options");
  databaseUrl = isolated.toString();
  await runFile("pnpm", ["--filter", "@converge/database", "migrate"], {
    cwd: repositoryRoot,
    env: { ...process.env, DATABASE_URL: databaseUrl },
  });
  assertionPool = createPool(databaseUrl);
  redis = createClient({ url: redisUrl });
  redis.on("error", () => undefined);
  await redis.connect();
});
afterEach(async () => {
  for (const socket of sockets) socket.disconnect();
  sockets.clear();
  await Promise.allSettled([...apis].map((api) => api.close()));
  apis.clear();
  await Promise.all([...boards].map(removePresenceKeys));
  boards.clear();
  await assertionPool?.query("DELETE FROM boards");
});
afterAll(async () => {
  await Promise.allSettled([...apis].map((api) => api.close()));
  if (redis?.isOpen) redis.destroy();
  await assertionPool?.end();
  try {
    if (adminPool && databaseName) await adminPool.query(`DROP DATABASE "${databaseName}"`);
  } finally {
    await adminPool?.end();
  }
});

describe.sequential("M3.5B2B2 real multi-instance presence", () => {
  it("replicates bounded presence across local Socket.IO rooms and recovers API A without fencing editing", async () => {
    const repository = new BoardRepository(assertionPool!);
    const board = await repository.createBoard(
      principal.owner.id,
      `presence-${crypto.randomUUID()}`,
    );
    boards.add(board.id);
    await assertionPool!.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1,$2,'editor')",
      [board.id, principal.editor.id],
    );
    const unrelated = await repository.createBoard(
      principal.owner.id,
      `presence-other-${crypto.randomUUID()}`,
    );
    boards.add(unrelated.id);
    await assertionPool!.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1,$2,'viewer')",
      [unrelated.id, principal.viewer.id],
    );

    const beforeA = await redisClientIds();
    const apiA = await createApi();
    await vi.waitFor(async () => expect((await redisClientIds()).size).toBe(beforeA.size + 3));
    const aClientIds = [...(await redisClientIds())].filter((id) => !beforeA.has(id));
    const apiB = await createApi();
    const ownerA = await join(apiA, token.owner, board.id);
    const editorB = await join(apiB, token.editor, board.id);
    const ownerTabB = await join(apiB, token.owner, board.id);
    const unrelatedB = await join(apiB, token.viewer, unrelated.id);
    for (const joined of [ownerA, editorB, ownerTabB, unrelatedB])
      expect(joined.ack).toMatchObject({ ok: true });
    await vi.waitFor(() => expect(ownerA.probe.availability.entries).toContain("available"));
    await vi.waitFor(() => expect(editorB.probe.snapshots.entries.length).toBeGreaterThan(0));
    await vi.waitFor(() =>
      expect(ownerTabB.probe.snapshots.entries.at(-1)?.participants.length).toBeGreaterThanOrEqual(
        2,
      ),
    );
    expect(
      ownerTabB.probe.snapshots.entries
        .at(-1)
        ?.participants.filter((p) => p.userId === principal.owner.id),
    ).toHaveLength(2);
    const ownerSnapshot = ownerA.probe.snapshots.entries.at(-1);
    const ownerTabSnapshot = ownerTabB.probe.snapshots.entries.at(-1);
    expect(
      ownerSnapshot?.participants.some(
        (p) => p.presenceSessionId === ownerSnapshot.selfPresenceSessionId,
      ),
    ).toBe(true);
    expect(
      ownerTabSnapshot?.participants.some(
        (p) => p.presenceSessionId === ownerTabSnapshot.selfPresenceSessionId,
      ),
    ).toBe(true);
    expect(ownerSnapshot?.selfPresenceSessionId).not.toBe(ownerTabSnapshot?.selfPresenceSessionId);
    expect(unrelatedB.probe.upserts.entries.some((event) => event.boardId === board.id)).toBe(
      false,
    );

    ownerA.probe.socket.emit("presence:update", {
      schemaVersion: 1,
      boardId: board.id,
      cursor: { x: 123, y: -45 },
      activity: "active",
    });
    await vi.waitFor(() =>
      expect(
        editorB.probe.upserts.entries.some((event) => event.participant.cursor?.x === 123),
      ).toBe(true),
    );
    const cursor = editorB.probe.upserts.entries.find(
      (event) => event.participant.cursor?.x === 123,
    )!;
    expect(cursor.participant).toMatchObject({
      userId: principal.owner.id,
      displayName: principal.owner.displayName,
      cursor: { x: 123, y: -45 },
      activity: "active",
    });
    const priorRevision = cursor.participant.revision;
    ownerA.probe.socket.emit("presence:update", {
      schemaVersion: 1,
      boardId: unrelated.id,
      cursor: { x: 1, y: 1 },
      activity: "active",
      userId: principal.editor.id,
    });
    ownerA.probe.socket.emit("presence:update", {
      schemaVersion: 1,
      boardId: board.id,
      cursor: null,
      activity: "active",
      revision: 999,
    });
    for (let index = 0; index < 25; index += 1)
      ownerA.probe.socket.emit("presence:update", {
        schemaVersion: 1,
        boardId: board.id,
        cursor: { x: index, y: index },
        activity: "active",
      });
    await vi.waitFor(() =>
      expect(
        editorB.probe.upserts.entries.some((event) => event.participant.cursor?.x === 24),
      ).toBe(true),
    );
    const final = editorB.probe.upserts.entries
      .filter(
        (event) => event.participant.presenceSessionId === cursor.participant.presenceSessionId,
      )
      .at(-1)!;
    expect(final.participant).toMatchObject({ cursor: { x: 24, y: 24 } });
    expect(final.participant.revision).toBeGreaterThan(priorRevision);
    expect(unrelatedB.probe.upserts.entries.some((event) => event.boardId === board.id)).toBe(
      false,
    );

    const command = createRectangleCommand(board.id);
    const commandAck = await new Promise<OperationAck>((resolve) =>
      ownerA.probe.socket.emit("operation:submit", command, resolve),
    );
    expect(commandAck).toMatchObject({ ok: true, duplicate: false });
    expect(ownerA.probe.socket.connected).toBe(true);

    const apiACommandClientId = aClientIds[0];
    if (!apiACommandClientId) throw new Error("Missing API A presence client identity");
    await redis!.sendCommand(["CLIENT", "KILL", "ID", apiACommandClientId]);
    await vi.waitFor(() => expect(ownerA.probe.availability.entries).toContain("unavailable"), {
      timeout: 5_000,
    });
    expect((await fetch(`${apiA.url}/health/ready`)).status).toBe(200);
    const duringOutage = createRectangleCommand(board.id);
    await expect(
      new Promise<OperationAck>((resolve) =>
        ownerA.probe.socket.emit("operation:submit", duringOutage, resolve),
      ),
    ).resolves.toMatchObject({ ok: true });
    expect(editorB.probe.availability.entries).not.toContain("unavailable");
    await vi.waitFor(() => expect(ownerA.probe.availability.entries.at(-1)).toBe("available"), {
      timeout: 5_000,
    });
    await vi.waitFor(
      () => expect(ownerA.probe.snapshots.entries.length).toBeGreaterThanOrEqual(2),
      { timeout: 5_000 },
    );
    editorB.probe.socket.emit("presence:update", {
      schemaVersion: 1,
      boardId: board.id,
      cursor: { x: 777, y: 888 },
      activity: "active",
    });
    await vi.waitFor(
      () =>
        expect(
          ownerA.probe.upserts.entries.some((event) => event.participant.cursor?.x === 777),
        ).toBe(true),
      { timeout: 5_000 },
    );

    ownerA.probe.socket.disconnect();
    await vi.waitFor(() =>
      expect(
        editorB.probe.leaves.entries.some(
          (event) => event.presenceSessionId === final.participant.presenceSessionId,
        ),
      ).toBe(true),
    );
    expect(ownerTabB.probe.socket.connected).toBe(true);
    await expect(
      Promise.all([apiA.close(), apiA.close(), apiB.close(), apiB.close()]),
    ).resolves.toBeDefined();
  }, 30_000);
});
