import { describe, expect, it } from "vitest";
import { BoardDeliveryCoordinator } from "./board-delivery-coordinator.js";

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

describe("board delivery coordinator", () => {
  it("orders one board, allows another board through, and releases inactive keys", async () => {
    const coordinator = new BoardDeliveryCoordinator();
    const firstGate = deferred();
    const calls: string[] = [];
    const first = coordinator.run("board-a", async () => {
      calls.push("a:first:start");
      await firstGate.promise;
      calls.push("a:first:end");
    });
    const second = coordinator.run("board-a", () => {
      calls.push("a:second");
      return Promise.resolve();
    });
    const other = coordinator.run("board-b", () => {
      calls.push("b");
      return Promise.resolve();
    });
    await settle();
    expect(calls).toEqual(["a:first:start", "b"]);
    firstGate.resolve();
    await Promise.all([first, second, other]);
    expect(calls).toEqual(["a:first:start", "b", "a:first:end", "a:second"]);
    expect(coordinator.activeBoardCount).toBe(0);
  });

  it("does not poison a board queue when a task rejects", async () => {
    const coordinator = new BoardDeliveryCoordinator();
    const failed = coordinator.run("board-a", () => Promise.reject(new Error("failed")));
    const recovered = coordinator.run("board-a", () => Promise.resolve("recovered"));
    await expect(failed).rejects.toThrow("failed");
    await expect(recovered).resolves.toBe("recovered");
    expect(coordinator.activeBoardCount).toBe(0);
  });
});
