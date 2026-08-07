import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { BoardRepository, createPool } from "@converge/database";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";
import { boardSnapshotSchema, type DurableCommand } from "@converge/protocol";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:5432/converge";
const pool = createPool(databaseUrl);
const repository = new BoardRepository(pool);

async function board(): Promise<string> {
  return (await repository.createBoard(fixtureIds.user, `integration-${crypto.randomUUID()}`)).id;
}

beforeAll(async () => {
  await pool.query("SELECT 1");
});
afterAll(async () => {
  await pool.end();
});

describe("authoritative operation transactions", () => {
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
    const duplicate = await repository.commitOperation(fixtureIds.user, command);
    expect(duplicate).toMatchObject({
      duplicate: true,
      operation: { seq: first.operation.seq, opId: command.opId },
    });
    const counts = await pool.query<{ operations: string; objects: string }>(
      "SELECT (SELECT count(*) FROM board_operations WHERE board_id=$1) operations, (SELECT count(*) FROM board_objects WHERE board_id=$1) objects",
      [boardId],
    );
    expect(counts.rows[0]).toEqual({ operations: "1", objects: "1" });
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
