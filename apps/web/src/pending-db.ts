import { openDB } from "idb";
import type { DurableCommand } from "@converge/protocol";

const database = () =>
  openDB("converge", 1, {
    upgrade(db) {
      db.createObjectStore("pending", { keyPath: "opId" });
    },
  });

export async function loadPending(boardId: string): Promise<DurableCommand[]> {
  const values = (await (await database()).getAll("pending")) as DurableCommand[];
  return values.filter((command) => command.boardId === boardId);
}
export async function savePending(command: DurableCommand): Promise<void> {
  await (await database()).put("pending", command);
}
export async function removePending(opId: string): Promise<void> {
  await (await database()).delete("pending", opId);
}
