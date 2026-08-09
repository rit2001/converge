import { readFile } from "node:fs/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  applyCommitted,
  emptyBoardState,
  hashBoardState,
  visibleObjects,
} from "@converge/canvas-engine";
import { BoardRepository, createPool } from "@converge/database";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";
import { boardSnapshotSchema, type BoardSnapshot, type DurableCommand } from "@converge/protocol";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const pool = createPool(databaseUrl);
const repository = new BoardRepository(pool);

async function board(): Promise<string> {
  return (await repository.createBoard(fixtureIds.user, `integration-${crypto.randomUUID()}`)).id;
}

async function durableState(boardId: string) {
  const result = await pool.query<{
    last_seq: string;
    operation_count: string;
    projection: unknown;
    outbox_count: string;
  }>(
    `SELECT b.last_seq,
            (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
            COALESCE(
              (SELECT jsonb_agg(
                 jsonb_build_object(
                   'objectId', o.object_id,
                   'objectData', o.object_data,
                   'fieldSeq', o.field_seq,
                   'createdSeq', o.created_seq,
                   'stackOrder', o.stack_order,
                   'updatedSeq', o.updated_seq,
                   'deletedSeq', o.deleted_seq
                 ) ORDER BY o.object_id
               ) FROM board_objects o WHERE o.board_id = b.id),
              '[]'::jsonb
            ) projection,
            (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count
     FROM boards b WHERE b.id = $1`,
    [boardId],
  );
  return result.rows[0];
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve = (): void => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function stateFromSnapshot(snapshot: BoardSnapshot) {
  const state = emptyBoardState();
  state.lastSeq = snapshot.lastSeq;
  for (const value of snapshot.objects) {
    state.objects[value.id] = {
      value,
      createdSeq: 0,
      updatedSeq: snapshot.lastSeq,
      deletedSeq: null,
      fieldSeq: {},
    };
    state.order.push(value.id);
  }
  return state;
}

beforeAll(async () => {
  await pool.query("SELECT 1");
});
afterAll(async () => {
  await pool.end();
});

describe("authoritative operation transactions", () => {
  it("backfills stack order from created sequence for existing projections", async () => {
    const migration = await readFile(
      new URL(
        "../../packages/database/migrations/0003_authoritative_stacking_order.sql",
        import.meta.url,
      ),
      "utf8",
    );
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `CREATE TEMP TABLE board_objects (
           board_id uuid NOT NULL,
           object_id uuid NOT NULL,
           created_seq bigint NOT NULL,
           PRIMARY KEY (board_id, object_id)
         )`,
      );
      await client.query("SET LOCAL search_path TO pg_temp");
      const boardId = crypto.randomUUID();
      await client.query(
        `INSERT INTO board_objects(board_id, object_id, created_seq)
         VALUES ($1, $2, 7), ($1, $3, 3)`,
        [boardId, "f0000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002"],
      );

      await client.query(migration);
      const rows = await client.query<{ created_seq: string; stack_order: string }>(
        "SELECT created_seq, stack_order FROM board_objects ORDER BY stack_order",
      );
      expect(rows.rows).toEqual([
        { created_seq: "3", stack_order: "3" },
        { created_seq: "7", stack_order: "7" },
      ]);
    } finally {
      await client.query("ROLLBACK");
      client.release();
    }
  });

  it("persists creation stacking across live replay, snapshots, mutations, and exact replay", async () => {
    const boardId = await board();
    const highId = "f0000000-0000-4000-8000-000000000001";
    const lowId = "10000000-0000-4000-8000-000000000002";
    const high = createRectangleCommand(boardId, highId);
    const low = { ...createRectangleCommand(boardId, lowId), baseSeq: 1 };
    const first = await repository.commitOperation(fixtureIds.user, high);
    const second = await repository.commitOperation(fixtureIds.user, low);
    const live = applyCommitted(
      applyCommitted(emptyBoardState(), first.operation),
      second.operation,
    );
    const afterCreate = await repository.getBoard(boardId, fixtureIds.user);

    expect(visibleObjects(live).map(({ id }) => id)).toEqual([highId, lowId]);
    expect(afterCreate.objects.map(({ id }) => id)).toEqual([highId, lowId]);
    expect(await hashBoardState(live)).toBe(await hashBoardState(stateFromSnapshot(afterCreate)));
    const ranks = await pool.query<{
      object_id: string;
      created_seq: string;
      stack_order: string;
    }>(
      `SELECT object_id, created_seq, stack_order
       FROM board_objects WHERE board_id = $1 ORDER BY stack_order, object_id`,
      [boardId],
    );
    expect(ranks.rows).toEqual([
      { object_id: highId, created_seq: "1", stack_order: "1" },
      { object_id: lowId, created_seq: "2", stack_order: "2" },
    ]);

    const beforeReplay = await durableState(boardId);
    await expect(repository.commitOperation(fixtureIds.user, low)).resolves.toMatchObject({
      duplicate: true,
      operation: { opId: low.opId, seq: 2 },
    });
    expect(await durableState(boardId)).toEqual(beforeReplay);

    const update: DurableCommand = {
      ...high,
      opId: crypto.randomUUID(),
      baseSeq: 2,
      type: "object.update",
      payload: { fill: "#ffffff" },
    };
    await repository.commitOperation(fixtureIds.user, update);
    const transform: DurableCommand = {
      ...low,
      opId: crypto.randomUUID(),
      baseSeq: 3,
      type: "object.transform",
      payload: { x: 80, y: 90 },
    };
    await repository.commitOperation(fixtureIds.user, transform);
    expect(
      (await repository.getBoard(boardId, fixtureIds.user)).objects.map(({ id }) => id),
    ).toEqual([highId, lowId]);
    expect(
      (
        await pool.query<{ object_id: string; stack_order: string }>(
          `SELECT object_id, stack_order FROM board_objects
           WHERE board_id = $1 ORDER BY stack_order, object_id`,
          [boardId],
        )
      ).rows,
    ).toEqual([
      { object_id: highId, stack_order: "1" },
      { object_id: lowId, stack_order: "2" },
    ]);

    await expect(
      pool.query(
        "UPDATE board_objects SET stack_order = 1 WHERE board_id = $1 AND object_id = $2",
        [boardId, lowId],
      ),
    ).rejects.toMatchObject({ code: "23505" });

    const removeBottom: DurableCommand = {
      ...high,
      opId: crypto.randomUUID(),
      baseSeq: 4,
      type: "object.delete",
      payload: {},
    };
    await repository.commitOperation(fixtureIds.user, removeBottom);
    expect(
      (await repository.getBoard(boardId, fixtureIds.user)).objects.map(({ id }) => id),
    ).toEqual([lowId]);
    expect(
      (
        await pool.query<{ object_id: string; stack_order: string }>(
          `SELECT object_id, stack_order FROM board_objects
           WHERE board_id = $1 ORDER BY stack_order, object_id`,
          [boardId],
        )
      ).rows,
    ).toEqual([
      { object_id: highId, stack_order: "1" },
      { object_id: lowId, stack_order: "2" },
    ]);
  });

  it("assigns monotonic board-local sequences", async () => {
    const boardId = await board();
    const first = await repository.commitOperation(
      fixtureIds.user,
      createRectangleCommand(boardId),
    );
    const second = await repository.commitOperation(
      fixtureIds.user,
      createRectangleCommand(boardId),
    );
    expect([first.operation.seq, second.operation.seq]).toEqual([1, 2]);
  });

  it("returns the original acknowledgement for duplicate opId without duplicating state", async () => {
    const boardId = await board();
    const command = createRectangleCommand(boardId);
    const first = await repository.commitOperation(fixtureIds.user, command);
    const afterFirst = await durableState(boardId);
    const duplicate = await repository.commitOperation(fixtureIds.user, command);
    expect(duplicate).toMatchObject({
      duplicate: true,
      operation: { seq: first.operation.seq, opId: command.opId },
    });
    expect(await durableState(boardId)).toEqual(afterFirst);
  });

  it("rejects a future base sequence without changing durable state", async () => {
    const boardId = await board();
    const before = await durableState(boardId);
    const command = { ...createRectangleCommand(boardId), baseSeq: 9_000 };
    await expect(repository.commitOperation(fixtureIds.user, command)).rejects.toMatchObject({
      code: "RESYNC_REQUIRED",
    });
    expect(await durableState(boardId)).toEqual(before);
  });

  it("checks a waiting command against the board head after acquiring the sequence lock", async () => {
    const boardId = await board();
    const firstCommand = createRectangleCommand(boardId);
    const secondCommand = { ...createRectangleCommand(boardId), baseSeq: 1 };
    const firstLocked = deferred();
    const releaseFirst = deferred();
    const secondAttemptingLock = deferred();
    const lockedRepository = new BoardRepository(pool, {
      beforeSequenceLock: (command) => {
        if (command.opId === secondCommand.opId) secondAttemptingLock.resolve();
        return Promise.resolve();
      },
      afterSequenceLock: async (command) => {
        if (command.opId !== firstCommand.opId) return;
        firstLocked.resolve();
        await releaseFirst.promise;
      },
    });

    const first = lockedRepository.commitOperation(fixtureIds.user, firstCommand);
    await firstLocked.promise;
    const second = lockedRepository.commitOperation(fixtureIds.user, secondCommand);
    await secondAttemptingLock.promise;
    releaseFirst.resolve();

    await expect(Promise.all([first, second])).resolves.toMatchObject([
      { operation: { seq: 1 } },
      { operation: { seq: 2 } },
    ]);
    expect(await durableState(boardId)).toMatchObject({
      last_seq: "2",
      operation_count: "2",
      outbox_count: "2",
    });
  });

  it("treats JSON key-order differences as an exact idempotent replay", async () => {
    const boardId = await board();
    const command = createRectangleCommand(boardId);
    if (command.type !== "object.create" || command.payload.kind !== "rectangle")
      throw new Error("Expected rectangle create command");
    const first = await repository.commitOperation(fixtureIds.user, command);
    const afterFirst = await durableState(boardId);
    const value = command.payload;
    const reordered: DurableCommand = {
      clientTimestamp: command.clientTimestamp,
      payload: {
        text: value.text,
        fill: value.fill,
        rotation: value.rotation,
        height: value.height,
        width: value.width,
        y: value.y,
        x: value.x,
        kind: value.kind,
        id: value.id,
      },
      targetId: command.targetId,
      type: command.type,
      baseSeq: command.baseSeq,
      clientId: command.clientId,
      boardId: command.boardId,
      opId: command.opId,
      schemaVersion: command.schemaVersion,
    };

    await expect(repository.commitOperation(fixtureIds.user, reordered)).resolves.toMatchObject({
      duplicate: true,
      operation: { opId: command.opId, seq: first.operation.seq },
    });
    expect(await durableState(boardId)).toEqual(afterFirst);
  });

  it("rejects operation-id reuse across every semantic intent dimension", async () => {
    const boardId = await board();
    const editorA = "00000000-0000-4000-8000-000000000011";
    const editorB = "00000000-0000-4000-8000-000000000012";
    await pool.query(
      `INSERT INTO board_members(board_id, user_id, role)
       VALUES ($1, $2, 'editor'), ($1, $3, 'editor')`,
      [boardId, editorA, editorB],
    );
    const firstObject = createRectangleCommand(boardId);
    const secondObject = createRectangleCommand(boardId);
    await repository.commitOperation(fixtureIds.user, firstObject);
    await repository.commitOperation(fixtureIds.user, secondObject);
    const original: DurableCommand = {
      ...firstObject,
      opId: crypto.randomUUID(),
      baseSeq: 2,
      type: "object.update",
      payload: { fill: "#ffffff" },
      clientTimestamp: "2026-08-07T12:00:00.000Z",
    };
    await repository.commitOperation(editorA, original);
    const afterOriginal = await durableState(boardId);
    const attempts: Array<{ actorId: string; command: DurableCommand }> = [
      {
        actorId: editorA,
        command: { ...original, type: "object.transform", payload: { x: 80 } },
      },
      { actorId: editorA, command: { ...original, targetId: secondObject.targetId } },
      { actorId: editorA, command: { ...original, payload: { fill: "#000000" } } },
      { actorId: editorA, command: { ...original, baseSeq: 1 } },
      { actorId: editorB, command: original },
    ];

    for (const attempt of attempts) {
      await expect(
        repository.commitOperation(attempt.actorId, attempt.command),
      ).rejects.toMatchObject({ code: "IDEMPOTENCY_CONFLICT" });
      expect(await durableState(boardId)).toEqual(afterOriginal);
    }
  });

  it("reauthorizes a removed editor before returning an exact replay", async () => {
    const boardId = await board();
    const editor = "00000000-0000-4000-8000-000000000013";
    await pool.query(
      "INSERT INTO board_members(board_id, user_id, role) VALUES ($1, $2, 'editor')",
      [boardId, editor],
    );
    const command = createRectangleCommand(boardId);
    await repository.commitOperation(editor, command);
    const beforeRemovalRetry = await durableState(boardId);
    await pool.query("DELETE FROM board_members WHERE board_id = $1 AND user_id = $2", [
      boardId,
      editor,
    ]);

    await expect(repository.commitOperation(editor, command)).rejects.toMatchObject({
      code: "FORBIDDEN",
    });
    expect(await durableState(boardId)).toEqual(beforeRemovalRetry);
  });

  it("preserves concurrent operations on separate objects", async () => {
    const boardId = await board();
    const [left, right] = await Promise.all([
      repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId)),
      repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId)),
    ]);
    expect(new Set([left.operation.seq, right.operation.seq])).toEqual(new Set([1, 2]));
    expect((await repository.getBoard(boardId, fixtureIds.user)).objects).toHaveLength(2);
  });

  it("rejects a stale update after delete", async () => {
    const boardId = await board();
    const create = createRectangleCommand(boardId);
    await repository.commitOperation(fixtureIds.user, create);
    const base = { ...create, baseSeq: 1, clientTimestamp: new Date().toISOString() };
    await repository.commitOperation(fixtureIds.user, {
      ...base,
      opId: crypto.randomUUID(),
      type: "object.delete",
      payload: {},
    });
    const stale: DurableCommand = {
      ...base,
      baseSeq: 1,
      opId: crypto.randomUUID(),
      type: "object.update",
      payload: { fill: "#ffffff" },
    };
    await expect(repository.commitOperation(fixtureIds.user, stale)).rejects.toMatchObject({
      code: "TARGET_DELETED",
    });
  });

  it("fetches a bounded missing sequence range in order", async () => {
    const boardId = await board();
    await repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    await repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    await repository.commitOperation(fixtureIds.user, createRectangleCommand(boardId));
    const operations = await repository.getOperations(boardId, fixtureIds.user, 2, 3);
    expect(operations.map((operation) => operation.seq)).toEqual([2, 3]);
  });

  it("rolls back a kind-incompatible patch and accepts the next valid mutation", async () => {
    const boardId = await board();
    const create = createRectangleCommand(boardId);
    await repository.commitOperation(fixtureIds.user, create);

    const persistedState = async () => {
      const result = await pool.query<{
        last_seq: string;
        operation_count: string;
        object_data: unknown;
        outbox_count: string;
      }>(
        `SELECT b.last_seq,
                (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
                o.object_data,
                (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count
         FROM boards b
         JOIN board_objects o ON o.board_id = b.id AND o.object_id = $2
         WHERE b.id = $1`,
        [boardId, create.targetId],
      );
      return result.rows[0];
    };

    const before = await persistedState();
    expect(before).toBeDefined();
    const invalid: DurableCommand = {
      ...create,
      opId: crypto.randomUUID(),
      baseSeq: 1,
      type: "object.update",
      payload: { text: "rectangle poisoning" },
      clientTimestamp: new Date().toISOString(),
    };
    await expect(repository.commitOperation(fixtureIds.user, invalid)).rejects.toMatchObject({
      code: "INVALID_COMMAND",
    });

    expect(await persistedState()).toEqual(before);
    expect(boardSnapshotSchema.parse(await repository.getBoard(boardId, fixtureIds.user))).toEqual(
      expect.objectContaining({ lastSeq: 1, objects: [before?.object_data] }),
    );

    const valid: DurableCommand = {
      ...invalid,
      opId: crypto.randomUUID(),
      payload: { fill: "#ffffff" },
    };
    const committed = await repository.commitOperation(fixtureIds.user, valid);
    expect(committed.operation.seq).toBe(2);
    const retrieved = boardSnapshotSchema.parse(
      await repository.getBoard(boardId, fixtureIds.user),
    );
    expect(retrieved).toMatchObject({
      lastSeq: 2,
      objects: [{ id: create.targetId, kind: "rectangle", fill: "#ffffff", text: "" }],
    });
  });
});
