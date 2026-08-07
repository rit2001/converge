import { create } from "zustand";
import {
  applyCommitted,
  emptyBoardState,
  hashBoardState,
  optimisticState,
  visibleObjects,
  type BoardState,
  type ProjectedObject,
} from "@converge/canvas-engine";
import type {
  BoardSnapshot,
  CanvasObject,
  CommittedOperation,
  DurableCommand,
  OperationAck,
} from "@converge/protocol";
import { removePending, savePending } from "./pending-db";

export type SynchronizationStatus =
  | "disconnected"
  | "connecting"
  | "joining"
  | "catching-up"
  | "ready"
  | "authorization-failed"
  | "error";
export type IngestResult = "applied" | "buffered" | "duplicate" | "conflict";

interface BoardStore {
  boardId: string | null;
  name: string;
  committed: BoardState;
  pending: DurableCommand[];
  buffered: Record<number, CommittedOperation>;
  appliedOpIds: Record<string, number>;
  appliedSeqOpIds: Record<number, string>;
  objects: CanvasObject[];
  selectedId: string | null;
  connection: SynchronizationStatus;
  hash: string;
  error: string | null;
  initialize(snapshot: BoardSnapshot, pending: DurableCommand[]): void;
  setConnection(status: SynchronizationStatus): void;
  setSynchronizationError(message: string, authorizationFailed?: boolean): void;
  select(id: string | null): void;
  enqueue(command: DurableCommand): void;
  acknowledge(opId: string, ack: OperationAck): void;
  ingest(operation: CommittedOperation): IngestResult;
}

function stateFromSnapshot(snapshot: BoardSnapshot): BoardState {
  const state = emptyBoardState();
  state.lastSeq = snapshot.lastSeq;
  for (const value of snapshot.objects) {
    const projected: ProjectedObject = {
      value,
      createdSeq: 0,
      updatedSeq: snapshot.lastSeq,
      deletedSeq: null,
      fieldSeq: {},
    };
    state.objects[value.id] = projected;
    state.order.push(value.id);
  }
  return state;
}

function derive(committed: BoardState, pending: DurableCommand[]): CanvasObject[] {
  return visibleObjects(optimisticState(committed, pending));
}

function scheduleHash(state: BoardState): void {
  void hashBoardState(state).then((hash) => useBoardStore.setState({ hash }));
}

export const useBoardStore = create<BoardStore>((set, get) => ({
  boardId: null,
  name: "Untitled board",
  committed: emptyBoardState(),
  pending: [],
  buffered: {},
  appliedOpIds: {},
  appliedSeqOpIds: {},
  objects: [],
  selectedId: null,
  connection: "disconnected",
  hash: "calculating…",
  error: null,
  initialize(snapshot, pending) {
    const committed = stateFromSnapshot(snapshot);
    set({
      boardId: snapshot.id,
      name: snapshot.name,
      committed,
      pending,
      objects: derive(committed, pending),
      buffered: {},
      appliedOpIds: {},
      appliedSeqOpIds: {},
      error: null,
    });
    scheduleHash(committed);
  },
  setConnection(connection) {
    set({ connection });
  },
  setSynchronizationError(error, authorizationFailed = false) {
    set({
      connection: authorizationFailed ? "authorization-failed" : "error",
      error,
    });
  },
  select(selectedId) {
    set({ selectedId });
  },
  enqueue(command) {
    const pending = [...get().pending, command];
    set({ pending, objects: derive(get().committed, pending), error: null });
    void savePending(command);
  },
  acknowledge(opId, ack) {
    if (ack.ok) {
      const pending = get().pending.filter((command) => command.opId !== opId);
      set({ pending, objects: derive(get().committed, pending) });
      void removePending(opId);
      get().ingest(ack.operation);
    } else if (!ack.retryable) {
      const pending = get().pending.filter((command) => command.opId !== opId);
      set({
        pending,
        objects: derive(get().committed, pending),
        error: `${ack.code}: ${ack.message}`,
      });
      void removePending(opId);
    } else set({ error: `${ack.code}: ${ack.message}` });
  },
  ingest(operation) {
    const current = get();
    const appliedOpId = current.appliedSeqOpIds[operation.seq];
    if (appliedOpId !== undefined && appliedOpId !== operation.opId) {
      set({ error: "Conflicting operations share an applied sequence", connection: "error" });
      return "conflict";
    }
    const appliedSeq = current.appliedOpIds[operation.opId];
    if (appliedSeq !== undefined) {
      if (appliedSeq !== operation.seq) {
        set({ error: "Conflicting sequence for committed operation", connection: "error" });
        return "conflict";
      }
      return "duplicate";
    }
    if (operation.seq <= current.committed.lastSeq) return "duplicate";
    const existing = current.buffered[operation.seq];
    if (existing) {
      if (existing.opId === operation.opId) return "duplicate";
      set({ error: "Conflicting operations share a sequence", connection: "error" });
      return "conflict";
    }

    let committed = current.committed;
    const buffered = { ...current.buffered, [operation.seq]: operation };
    const appliedOpIds = { ...current.appliedOpIds };
    const appliedSeqOpIds = { ...current.appliedSeqOpIds };
    let next = buffered[committed.lastSeq + 1];
    while (next) {
      committed = applyCommitted(committed, next);
      appliedOpIds[next.opId] = next.seq;
      appliedSeqOpIds[next.seq] = next.opId;
      delete buffered[next.seq];
      next = buffered[committed.lastSeq + 1];
    }
    set({
      committed,
      buffered,
      appliedOpIds,
      appliedSeqOpIds,
      objects: derive(committed, get().pending),
    });
    scheduleHash(committed);
    return operation.seq <= committed.lastSeq ? "applied" : "buffered";
  },
}));

export function firstBufferedGap(): { from: number; to: number } | null {
  const { committed, buffered } = useBoardStore.getState();
  const sequences = Object.keys(buffered)
    .map(Number)
    .sort((a, b) => a - b);
  const first = sequences[0];
  return first && first > committed.lastSeq + 1
    ? { from: committed.lastSeq + 1, to: first - 1 }
    : null;
}
