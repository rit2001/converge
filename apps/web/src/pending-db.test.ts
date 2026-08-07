import { describe, expect, it } from "vitest";
import type { DurableCommand } from "@converge/protocol";
import { BoardOperationSerializer, decodePendingRows } from "./pending-db";

const boardId = "10000000-0000-4000-8000-000000000001";

function command(index: number, timestamp = `2026-08-07T12:00:0${index}.000Z`): DurableCommand {
  const suffix = index.toString(16).padStart(12, "0");
  const targetId = `30000000-0000-4000-8000-${suffix}`;
  return {
    schemaVersion: 1,
    opId: `40000000-0000-4000-8000-${suffix}`,
    boardId,
    clientId: "20000000-0000-4000-8000-000000000001",
    baseSeq: 0,
    targetId,
    clientTimestamp: timestamp,
    type: "object.create",
    payload: {
      id: targetId,
      kind: "rectangle",
      x: index,
      y: index,
      width: 100,
      height: 80,
      rotation: 0,
      fill: "#818cf8",
      text: "",
    },
  };
}

function stored(item: DurableCommand, enqueueOrdinal: number) {
  return {
    opId: item.opId,
    boardId: item.boardId,
    enqueueOrdinal,
    command: item,
  };
}

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("pending operation persistence", () => {
  it("loads explicit enqueue ordinals in deterministic order", () => {
    const first = command(1);
    const second = command(2);
    const result = decodePendingRows([stored(second, 2), stored(first, 1)], boardId);
    expect(result).toEqual({ commands: [first, second], corruptCount: 0 });
  });

  it("loads legacy commands with a deterministic timestamp and operation-id fallback", () => {
    const laterId = command(2, "2026-08-07T12:00:00.000Z");
    const earlierId = command(1, "2026-08-07T12:00:00.000Z");
    const result = decodePendingRows([laterId, earlierId], boardId);
    expect(result.commands.map(({ opId }) => opId)).toEqual([earlierId.opId, laterId.opId]);
  });

  it("keeps valid rows and reports a corrupt row without deleting the database", () => {
    const valid = command(1);
    const result = decodePendingRows(
      [stored(valid, 1), { boardId, opId: "not-valid", command: { surprise: true } }],
      boardId,
    );
    expect(result).toEqual({ commands: [valid], corruptCount: 1 });
  });

  it("orders put before delete and recovers the board chain after a failed action", async () => {
    const serializer = new BoardOperationSerializer();
    const delayedPut = deferred();
    const events: string[] = [];
    const put = serializer.run(boardId, async () => {
      events.push("put:start");
      await delayedPut.promise;
      events.push("put:end");
    });
    const remove = serializer.run(boardId, () => {
      events.push("delete");
      return Promise.resolve();
    });
    await Promise.resolve();
    expect(events).toEqual(["put:start"]);
    delayedPut.resolve();
    await Promise.all([put, remove]);
    expect(events).toEqual(["put:start", "put:end", "delete"]);

    await expect(
      serializer.run(boardId, () => Promise.reject(new Error("storage unavailable"))),
    ).rejects.toThrow("storage unavailable");
    await serializer.run(boardId, () => {
      events.push("later-put");
      return Promise.resolve();
    });
    expect(events.at(-1)).toBe("later-put");
  });
});
