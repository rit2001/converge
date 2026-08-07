import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { BoardRepository, createPool } from "@converge/database";
import { createRectangleCommand, fixtureIds } from "@converge/testkit";
import type { DurableCommand } from "@converge/protocol";

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
});
