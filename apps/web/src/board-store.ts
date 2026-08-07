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

type ConnectionStatus = "connecting" | "connected" | "reconnecting" | "offline";
interface BoardStore {
  boardId: string | null;
  name: string;
  committed: BoardState;
  pending: DurableCommand[];
  buffered: Record<number, CommittedOperation>;
  objects: CanvasObject[];
  selectedId: string | null;
  connection: ConnectionStatus;
  hash: string;
  error: string | null;
  initialize(snapshot: BoardSnapshot, pending: DurableCommand[]): void;
  setConnection(status: ConnectionStatus): void;
  select(id: string | null): void;
  enqueue(command: DurableCommand): void;
  acknowledge(opId: string, ack: OperationAck): void;
  ingest(operation: CommittedOperation): void;
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
  objects: [],
  selectedId: null,
  connection: "connecting",
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
      error: null,
    });
    scheduleHash(committed);
  },
  setConnection(connection) {
    set({ connection });
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
    let committed = get().committed;
    if (operation.seq <= committed.lastSeq) return;
    const buffered = { ...get().buffered, [operation.seq]: operation };
    let next = buffered[committed.lastSeq + 1];
    while (next) {
      committed = applyCommitted(committed, next);
      delete buffered[next.seq];
      next = buffered[committed.lastSeq + 1];
    }
    set({ committed, buffered, objects: derive(committed, get().pending) });
    scheduleHash(committed);
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
