import { openDB } from "idb";
import { durableCommandSchema, type DurableCommand } from "@converge/protocol";

export interface PendingLoadResult {
  commands: DurableCommand[];
  corruptCount: number;
}

export interface PendingOperationStore {
  load(boardId: string): Promise<PendingLoadResult>;
  put(command: DurableCommand): Promise<void>;
  delete(boardId: string, operationId: string): Promise<void>;
}

interface StoredPendingCommand {
  opId: string;
  boardId: string;
  enqueueOrdinal: number;
  command: DurableCommand;
}

interface LoadedPendingCommand {
  command: DurableCommand;
  enqueueOrdinal: number | null;
}

const LEGACY_STORE = "pending";
const BOARD_STORE = "pending-by-board";

const database = () =>
  openDB("converge", 2, {
    upgrade(db, oldVersion) {
      if (oldVersion < 1) db.createObjectStore(LEGACY_STORE, { keyPath: "opId" });
      if (oldVersion < 2) db.createObjectStore(BOARD_STORE, { keyPath: ["boardId", "opId"] });
    },
  });

function storedCommand(value: unknown): LoadedPendingCommand | null {
  const legacy = durableCommandSchema.safeParse(value);
  if (legacy.success) return { command: legacy.data, enqueueOrdinal: null };
  if (!value || typeof value !== "object") return null;
  const record = value as Record<string, unknown>;
  if (
    Object.keys(record).some(
      (key) => !["opId", "boardId", "enqueueOrdinal", "command"].includes(key),
    ) ||
    typeof record.enqueueOrdinal !== "number" ||
    !Number.isSafeInteger(record.enqueueOrdinal) ||
    record.enqueueOrdinal < 0
  )
    return null;
  const command = durableCommandSchema.safeParse(record.command);
  if (
    !command.success ||
    record.opId !== command.data.opId ||
    record.boardId !== command.data.boardId
  )
    return null;
  return { command: command.data, enqueueOrdinal: record.enqueueOrdinal };
}

function comparePending(left: LoadedPendingCommand, right: LoadedPendingCommand): number {
  if (left.enqueueOrdinal === null && right.enqueueOrdinal !== null) return -1;
  if (left.enqueueOrdinal !== null && right.enqueueOrdinal === null) return 1;
  if (left.enqueueOrdinal !== null && right.enqueueOrdinal !== null) {
    const ordinal = left.enqueueOrdinal - right.enqueueOrdinal;
    if (ordinal !== 0) return ordinal;
  } else {
    const timestamp = left.command.clientTimestamp.localeCompare(right.command.clientTimestamp);
    if (timestamp !== 0) return timestamp;
  }
  return left.command.opId.localeCompare(right.command.opId);
}

export function decodePendingRows(values: unknown[], boardId: string): PendingLoadResult {
  const commandsByOperation = new Map<string, LoadedPendingCommand>();
  let corruptCount = 0;
  for (const value of values) {
    const loaded = storedCommand(value);
    if (!loaded) {
      if (
        value &&
        typeof value === "object" &&
        (value as Record<string, unknown>).boardId === boardId
      )
        corruptCount += 1;
      continue;
    }
    if (loaded.command.boardId === boardId) {
      const existing = commandsByOperation.get(loaded.command.opId);
      if (!existing || (existing.enqueueOrdinal === null && loaded.enqueueOrdinal !== null))
        commandsByOperation.set(loaded.command.opId, loaded);
    }
  }
  const commands = [...commandsByOperation.values()];
  commands.sort(comparePending);
  return { commands: commands.map(({ command }) => command), corruptCount };
}

export class BoardOperationSerializer {
  private readonly boardTails = new Map<string, Promise<void>>();

  run<T>(boardId: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.boardTails.get(boardId) ?? Promise.resolve();
    const result = previous.then(operation, operation);
    const settled = result.then(
      () => undefined,
      () => undefined,
    );
    this.boardTails.set(boardId, settled);
    void settled.then(() => {
      if (this.boardTails.get(boardId) === settled) this.boardTails.delete(boardId);
    });
    return result;
  }
}

export class IndexedDbPendingOperationStore implements PendingOperationStore {
  private readonly serializer = new BoardOperationSerializer();

  load(boardId: string): Promise<PendingLoadResult> {
    return this.serialize(boardId, async () => {
      const db = await database();
      const [legacy, boardScoped] = await Promise.all([
        db.getAll(LEGACY_STORE) as Promise<unknown[]>,
        db.getAll(BOARD_STORE) as Promise<unknown[]>,
      ]);
      const values = [...legacy, ...boardScoped];
      return decodePendingRows(values, boardId);
    });
  }

  put(command: DurableCommand): Promise<void> {
    const normalized = durableCommandSchema.parse(command);
    return this.serialize(normalized.boardId, async () => {
      const db = await database();
      const transaction = db.transaction([LEGACY_STORE, BOARD_STORE], "readwrite");
      const legacyStore = transaction.objectStore(LEGACY_STORE);
      const boardStore = transaction.objectStore(BOARD_STORE);
      const [legacyValue, boardValue, values] = await Promise.all([
        legacyStore.get(normalized.opId) as Promise<unknown>,
        boardStore.get([normalized.boardId, normalized.opId]) as Promise<unknown>,
        boardStore.getAll() as Promise<unknown[]>,
      ]);
      const existing = storedCommand(boardValue) ?? storedCommand(legacyValue);
      if (existing?.command.boardId === normalized.boardId) {
        if (JSON.stringify(existing.command) !== JSON.stringify(normalized))
          throw new Error("Pending operation identity conflict");
        await transaction.done;
        return;
      }
      let maxOrdinal = 0;
      for (const value of values) {
        const loaded = storedCommand(value);
        if (
          loaded?.command.boardId === normalized.boardId &&
          loaded.enqueueOrdinal !== null &&
          loaded.enqueueOrdinal > maxOrdinal
        )
          maxOrdinal = loaded.enqueueOrdinal;
      }
      const stored: StoredPendingCommand = {
        opId: normalized.opId,
        boardId: normalized.boardId,
        enqueueOrdinal: maxOrdinal + 1,
        command: normalized,
      };
      await boardStore.put(stored);
      await transaction.done;
    });
  }

  delete(boardId: string, operationId: string): Promise<void> {
    return this.serialize(boardId, async () => {
      const db = await database();
      const transaction = db.transaction([LEGACY_STORE, BOARD_STORE], "readwrite");
      const legacyStore = transaction.objectStore(LEGACY_STORE);
      const boardStore = transaction.objectStore(BOARD_STORE);
      const legacyValue: unknown = await legacyStore.get(operationId);
      const loaded = storedCommand(legacyValue);
      if (loaded?.command.boardId === boardId) await legacyStore.delete(operationId);
      await boardStore.delete([boardId, operationId]);
      await transaction.done;
    });
  }

  private serialize<T>(boardId: string, operation: () => Promise<T>): Promise<T> {
    return this.serializer.run(boardId, operation);
  }
}

export const indexedDbPendingOperationStore = new IndexedDbPendingOperationStore();
