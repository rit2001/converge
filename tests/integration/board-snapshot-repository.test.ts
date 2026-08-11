import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type pg from "pg";
import {
  BoardRepository,
  BoardSnapshotRepository,
  canonicalBoardSnapshot,
  createPool,
  hashBoardSnapshot,
  type BoardSnapshotProjection,
} from "@converge/database";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";
import type { DurableCommand } from "@converge/protocol";

const sharedDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@127.0.0.1:55432/converge";
const repositoryRoot = new URL("../..", import.meta.url);
const runFile = promisify(execFile);
const createdBoardIds = new Set<string>();

let databaseName: string;
let databaseUrl: string;
let adminPool: ReturnType<typeof createPool>;
let pool: ReturnType<typeof createPool>;
let boards: BoardRepository;
let snapshots: BoardSnapshotRepository;

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function createBoard(name = "snapshot-board"): Promise<string> {
  const boardId = (await boards.createBoard(fixtureIds.user, `${name}-${crypto.randomUUID()}`)).id;
  createdBoardIds.add(boardId);
  return boardId;
}

function operation(
  boardId: string,
  baseSeq: number,
  type: DurableCommand["type"],
  targetId: string,
  payload: DurableCommand["payload"],
): DurableCommand {
  return {
    schemaVersion: 1,
    opId: crypto.randomUUID(),
    boardId,
    clientId: fixtureIds.clientA,
    baseSeq,
    type,
    targetId,
    payload,
    clientTimestamp: new Date().toISOString(),
  } as DurableCommand;
}

beforeAll(async () => {
  databaseName = `converge_snapshot_${process.pid}_${crypto
    .randomUUID()
    .replaceAll("-", "")
    .slice(0, 12)}`;
  const adminUrl = new URL(sharedDatabaseUrl);
  adminUrl.pathname = "/postgres";
  adminPool = createPool(adminUrl.toString());
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);
  const isolatedUrl = new URL(sharedDatabaseUrl);
  isolatedUrl.pathname = `/${databaseName}`;
  databaseUrl = isolatedUrl.toString();
  await runFile("pnpm", ["--filter", "@converge/database", "migrate"], {
    cwd: repositoryRoot,
    env: { ...process.env, DATABASE_URL: databaseUrl },
    maxBuffer: 1024 * 1024,
  });
  pool = createPool(databaseUrl);
  boards = new BoardRepository(pool);
  snapshots = new BoardSnapshotRepository(pool);
  const migrations = await pool.query<{ name: string }>(
    "SELECT name FROM converge_migrations ORDER BY name",
  );
  expect(migrations.rows.map(({ name }) => name)).toContain("0007_verified_board_snapshots.sql");
});

afterEach(async () => {
  if (createdBoardIds.size > 0)
    await pool.query("DELETE FROM boards WHERE id = ANY($1::uuid[])", [[...createdBoardIds]]);
  createdBoardIds.clear();
});

afterAll(async () => {
  await pool?.end();
  try {
    if (adminPool && databaseName) await adminPool.query(`DROP DATABASE "${databaseName}"`);
  } finally {
    await adminPool?.end();
  }
});

describe("verified board snapshots", () => {
  it("captures and deterministically loads an empty board without advancing either head", async () => {
    const boardId = await createBoard("empty");
    const snapshot = await snapshots.create(boardId);
    expect(snapshot.projection).toMatchObject({
      boardId,
      lastSeq: 0,
      lastDeliverySeq: 0,
      objects: [],
    });
    expect(snapshot.objectCount).toBe(0);
    expect(snapshot.canonicalHash).toBe(hashBoardSnapshot(snapshot.projection));
    expect(await snapshots.loadLatest(boardId)).toEqual(snapshot);
    const heads = await pool.query("SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1", [
      boardId,
    ]);
    expect(heads.rows[0]).toEqual({ last_seq: "0", last_delivery_seq: "0" });
    await expect(snapshots.create(boardId)).rejects.toMatchObject({
      code: "DUPLICATE_SNAPSHOT_HEAD",
    });
  });

  it("stores the complete ordered projection including rotations, field clocks, and tombstones", async () => {
    const boardId = await createBoard("full");
    const rectangleId = crypto.randomUUID();
    const stickyId = crypto.randomUUID();
    const tombstoneId = crypto.randomUUID();
    await boards.commitOperation(
      fixtureIds.user,
      createRectangleCommand(boardId, rectangleId, crypto.randomUUID()),
    );
    await boards.commitOperation(
      fixtureIds.user,
      operation(boardId, 1, "object.create", stickyId, {
        id: stickyId,
        kind: "sticky",
        x: 10,
        y: 20,
        width: 120,
        height: 90,
        rotation: 17,
        fill: "#abcdef",
        text: "snapshot",
      }),
    );
    await boards.commitOperation(
      fixtureIds.user,
      operation(boardId, 2, "object.create", tombstoneId, {
        id: tombstoneId,
        kind: "rectangle",
        x: 2,
        y: 3,
        width: 80,
        height: 40,
        rotation: -12,
        fill: "#123456",
        text: "",
      }),
    );
    await boards.commitOperation(
      fixtureIds.user,
      operation(boardId, 3, "object.transform", rectangleId, { rotation: 42 }),
    );
    await boards.commitOperation(
      fixtureIds.user,
      operation(boardId, 4, "object.delete", tombstoneId, {}),
    );

    const snapshot = await snapshots.create(boardId);
    expect(snapshot.snapshotSeq).toBe(5);
    expect(snapshot.snapshotDeliverySeq).toBe(5);
    expect(snapshot.projection.objects.map(({ objectId }) => objectId)).toEqual([
      rectangleId,
      stickyId,
      tombstoneId,
    ]);
    expect(snapshot.projection.objects[0]?.value.rotation).toBe(42);
    expect(snapshot.projection.objects[1]?.value.kind).toBe("sticky");
    expect(snapshot.projection.objects[2]?.deletedSeq).toBe(5);
    expect(snapshot.projection.objects[0]?.fieldSeq.rotation).toBe(4);
  });

  it("uses deterministic domain-separated canonical hashing", () => {
    const projection: BoardSnapshotProjection = {
      schemaVersion: 1,
      boardId: "00000000-0000-4000-8000-000000000010",
      boardName: "canonical",
      lastSeq: 1,
      lastDeliverySeq: 2,
      objects: [
        {
          objectId: "10000000-0000-4000-8000-000000000010",
          stackOrder: 1,
          value: {
            id: "10000000-0000-4000-8000-000000000010",
            kind: "rectangle",
            x: 1,
            y: 2,
            width: 80,
            height: 40,
            rotation: 9,
            fill: "#abcdef",
            text: "",
          },
          fieldSeq: {
            id: 1,
            kind: 1,
            x: 1,
            y: 1,
            width: 1,
            height: 1,
            rotation: 1,
            fill: 1,
            text: 1,
          },
          createdSeq: 1,
          updatedSeq: 1,
          deletedSeq: null,
        },
      ],
    };
    expect(hashBoardSnapshot(projection)).toBe(
      "1b2e2d5856f6e39eac2a5a1f255ee33ab92a9bed6f5d8ef6ac90ddb0856117fc",
    );
    const reorderedFields = JSON.parse(JSON.stringify(projection)) as BoardSnapshotProjection;
    reorderedFields.objects[0]!.value = Object.fromEntries(
      Object.entries(reorderedFields.objects[0]!.value).reverse(),
    ) as BoardSnapshotProjection["objects"][number]["value"];
    expect(canonicalBoardSnapshot(reorderedFields)).toBe(canonicalBoardSnapshot(projection));
    expect(hashBoardSnapshot(reorderedFields)).toBe(hashBoardSnapshot(projection));
    const reorderedObjects = structuredClone(projection);
    reorderedObjects.objects.push({
      ...structuredClone(projection.objects[0]!),
      objectId: "20000000-0000-4000-8000-000000000010",
      stackOrder: 2,
      value: {
        ...structuredClone(projection.objects[0]!.value),
        id: "20000000-0000-4000-8000-000000000010",
      },
    });
    const reversed = structuredClone(reorderedObjects);
    reversed.objects.reverse();
    expect(hashBoardSnapshot(reversed)).not.toBe(hashBoardSnapshot(reorderedObjects));
  });

  it("serializes a concurrent operation entirely after a snapshot boundary", async () => {
    const boardId = await createBoard("operation-race");
    const locked = deferred();
    const release = deferred();
    const racingSnapshots = new BoardSnapshotRepository(pool, {
      afterAdvisoryLock: async () => {
        locked.resolve();
        await release.promise;
      },
    });
    const capturing = racingSnapshots.create(boardId);
    await locked.promise;
    const committing = boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    release.resolve();
    const [snapshot, committed] = await Promise.all([capturing, committing]);
    expect(snapshot.snapshotSeq).toBe(0);
    expect(committed.operation.seq).toBe(1);
    expect((await snapshots.loadLatest(boardId))?.projection.objects).toHaveLength(0);
  });

  it("serializes a concurrent revocation entirely after the captured delivery boundary", async () => {
    const boardId = await createBoard("revocation-race");
    const target = crypto.randomUUID();
    await pool.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
      [boardId, target],
    );
    const locked = deferred();
    const release = deferred();
    const racingSnapshots = new BoardSnapshotRepository(pool, {
      afterAdvisoryLock: async () => {
        locked.resolve();
        await release.promise;
      },
    });
    const capturing = racingSnapshots.create(boardId);
    await locked.promise;
    const removing = boards.removeBoardMember(fixtureIds.user, boardId, target);
    release.resolve();
    const [snapshot, removed] = await Promise.all([capturing, removing]);
    expect(snapshot.snapshotDeliverySeq).toBe(0);
    expect(removed.event?.deliverySeq).toBe(1);
  });

  it("rolls back an inserted snapshot when verification cannot complete", async () => {
    const boardId = await createBoard("rollback");
    const failing = new BoardSnapshotRepository(pool, {
      afterInsert: () => Promise.reject(new Error("injected verification failure")),
    });
    await expect(failing.create(boardId)).rejects.toThrow("injected verification failure");
    const count = await pool.query<{ count: string }>(
      "SELECT count(*) FROM board_snapshots WHERE board_id = $1",
      [boardId],
    );
    expect(count.rows[0]?.count).toBe("0");
  });

  it("rejects malformed, missing, mismatched, unknown, and mutable stored metadata", async () => {
    const boardId = await createBoard("constraints");
    const snapshot = await snapshots.create(boardId);
    const stored = await pool.query<{ projection: unknown }>(
      "SELECT projection FROM board_snapshots WHERE id = $1",
      [snapshot.id],
    );
    const projection = {
      ...(stored.rows[0]!.projection as Record<string, unknown>),
      lastSeq: 1,
      lastDeliverySeq: 1,
    };
    const insert = (
      id: string,
      candidate: unknown,
      hash = "a".repeat(64),
      snapshotSeq = 1,
      deliverySeq = 1,
    ) =>
      pool.query(
        `INSERT INTO board_snapshots(
           id, board_id, snapshot_seq, snapshot_delivery_seq, schema_version, projection,
           canonical_hash, object_count, byte_size, status, verified_at
         ) VALUES ($1,$2,$5,$6,1,$3,$4,0,1,'verified',clock_timestamp())`,
        [id, boardId, candidate, hash, snapshotSeq, deliverySeq],
      );
    await expect(
      insert(crypto.randomUUID(), { ...(projection as object), boardId: "not-a-uuid" }),
    ).rejects.toThrow();
    const missingProjection = { ...(projection as Record<string, unknown>) };
    delete missingProjection.objects;
    await expect(insert(crypto.randomUUID(), missingProjection)).rejects.toThrow();
    await expect(insert(crypto.randomUUID(), { ...projection, unknown: true })).rejects.toThrow();
    await expect(insert(crypto.randomUUID(), projection, "a".repeat(64), 2, 2)).rejects.toThrow();
    await expect(
      insert(
        crypto.randomUUID(),
        { ...projection, lastSeq: Number.MAX_SAFE_INTEGER + 1 },
        "a".repeat(64),
        Number.MAX_SAFE_INTEGER + 1,
        Number.MAX_SAFE_INTEGER + 1,
      ),
    ).rejects.toThrow();
    await expect(insert(crypto.randomUUID(), projection, "NOT-A-HASH")).rejects.toThrow();
    await expect(
      pool.query("UPDATE board_snapshots SET projection = projection WHERE id = $1", [snapshot.id]),
    ).resolves.toBeDefined();
    await expect(
      pool.query("UPDATE board_snapshots SET canonical_hash = $2 WHERE id = $1", [
        snapshot.id,
        "b".repeat(64),
      ]),
    ).rejects.toThrow(/immutable/);
    await expect(
      pool.query(
        `UPDATE board_snapshots
         SET projection = jsonb_set(projection, '{boardName}', '"mutated"'::jsonb)
         WHERE id = $1`,
        [snapshot.id],
      ),
    ).rejects.toThrow(/immutable/);
  });

  it("fails closed for corrupt payload, hash, board, head, status, and version rows", async () => {
    const boardId = await createBoard("corruption");
    const snapshot = await snapshots.create(boardId);
    const stored = await pool.query("SELECT * FROM board_snapshots WHERE id = $1", [snapshot.id]);
    const row = stored.rows[0] as Record<string, unknown>;
    const fake = (overrides: Record<string, unknown>) =>
      new BoardSnapshotRepository({
        query: () => Promise.resolve({ rows: [{ ...row, ...overrides }] }),
      } as unknown as pg.Pool);

    await expect(
      fake({ canonical_hash: "b".repeat(64) }).loadLatest(boardId),
    ).rejects.toMatchObject({ code: "SNAPSHOT_CORRUPT" });
    await expect(fake({ board_id: crypto.randomUUID() }).loadLatest(boardId)).rejects.toMatchObject(
      { code: "SNAPSHOT_CORRUPT" },
    );
    await expect(fake({ snapshot_seq: "1" }).loadLatest(boardId)).rejects.toMatchObject({
      code: "SNAPSHOT_CORRUPT",
    });
    await expect(fake({ status: "invalid" }).loadLatest(boardId)).rejects.toMatchObject({
      code: "SNAPSHOT_CORRUPT",
    });
    await expect(fake({ schema_version: 2 }).loadLatest(boardId)).rejects.toMatchObject({
      code: "UNSUPPORTED_SNAPSHOT_VERSION",
    });
    await expect(
      fake({ projection: { ...(row.projection as object), boardName: "tampered" } }).loadLatest(
        boardId,
      ),
    ).rejects.toMatchObject({ code: "SNAPSHOT_CORRUPT" });
  });

  it("selects the newest verified head deterministically", async () => {
    const boardId = await createBoard("latest");
    const first = await snapshots.create(boardId);
    await boards.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const second = await snapshots.create(boardId);
    expect(first.snapshotSeq).toBe(0);
    expect(second.snapshotSeq).toBe(1);
    expect((await snapshots.loadLatest(boardId))?.id).toBe(second.id);
  });
});
