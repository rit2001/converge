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
} from "@converge/protocol";
import type { BoardSessionToken } from "./board-session";
import type { PendingRecoveryStatus } from "./pending-command-queue";

export type SynchronizationStatus =
  | "disconnected"
  | "connecting"
  | "joining"
  | "catching-up"
  | "retry-wait"
  | "ready"
  | "authorization-failed"
  | "error";
export type IngestResult = "applied" | "buffered" | "duplicate" | "conflict";
export interface SynchronizationDiagnostics {
  attempt: number;
  retryCode: string | null;
  retryScheduled: boolean;
  retryDelayMs: number | null;
  bufferedCount: number;
  bufferedBytes: number;
  bufferCountLimit: number;
  bufferByteLimit: number;
}
export type AuthoritativeHash =
  | {
      status: "idle";
      boardId: null;
      sessionGeneration: null;
      seq: null;
      value: null;
    }
  | {
      status: "pending" | "ready" | "error";
      boardId: string;
      sessionGeneration: number;
      seq: number;
      value: string | null;
    };

export interface BoardStore {
  sessionToken: BoardSessionToken | null;
  sessionGeneration: number | null;
  boardId: string | null;
  name: string;
  committed: BoardState;
  pending: DurableCommand[];
  buffered: Record<number, CommittedOperation>;
  appliedOpIds: Record<string, number>;
  appliedSeqOpIds: Record<number, string>;
  objects: CanvasObject[];
  selectedId: string | null;
  selectionNotice: string | null;
  connection: SynchronizationStatus;
  pendingStatus: PendingRecoveryStatus;
  synchronizationDiagnostics: SynchronizationDiagnostics;
  authoritativeHash: AuthoritativeHash;
  error: string | null;
  beginSession(token: BoardSessionToken, boardId: string | null): void;
  bindSessionBoard(token: BoardSessionToken, boardId: string): boolean;
  isCurrentSession(token: BoardSessionToken, boardId?: string): boolean;
  initializeSession(
    token: BoardSessionToken,
    snapshot: BoardSnapshot,
    pending: DurableCommand[],
  ): boolean;
  rebaseSession(token: BoardSessionToken, snapshot: BoardSnapshot): boolean;
  rebaseRecoveredSession(
    token: BoardSessionToken,
    boardId: string,
    boardName: string,
    committed: BoardState,
  ): boolean;
  revokeSession(token: BoardSessionToken, boardId: string, message: string): boolean;
  failSession(token: BoardSessionToken, message: string): void;
  endSession(token: BoardSessionToken): void;
  setConnection(token: BoardSessionToken, status: SynchronizationStatus): void;
  setSynchronizationError(
    token: BoardSessionToken,
    message: string,
    authorizationFailed?: boolean,
  ): void;
  setSynchronizationDiagnostics(
    token: BoardSessionToken,
    diagnostics: Partial<SynchronizationDiagnostics>,
  ): void;
  setPendingStatus(
    token: BoardSessionToken,
    status: PendingRecoveryStatus,
    message?: string | null,
  ): void;
  select(id: string | null): void;
  addPersistedPending(token: BoardSessionToken, command: DurableCommand): boolean;
  removePending(token: BoardSessionToken, opId: string, error?: string): void;
  ingest(token: BoardSessionToken, operation: CommittedOperation): IngestResult | "stale";
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

function currentSelection(objects: CanvasObject[], selectedId: string | null) {
  if (!selectedId || objects.some((object) => object.id === selectedId))
    return { selectedId, selectionNotice: null };
  return { selectedId: null, selectionNotice: "Selected object was removed from the board." };
}

function cloneBoardState(state: BoardState): BoardState {
  return {
    lastSeq: state.lastSeq,
    order: [...state.order],
    objects: Object.fromEntries(
      Object.entries(state.objects).map(([id, projected]) => [
        id,
        {
          ...projected,
          value: { ...projected.value },
          fieldSeq: { ...projected.fieldSeq },
        },
      ]),
    ),
  };
}

const idleHash = (): AuthoritativeHash => ({
  status: "idle",
  boardId: null,
  sessionGeneration: null,
  seq: null,
  value: null,
});

const idleSynchronizationDiagnostics = (): SynchronizationDiagnostics => ({
  attempt: 0,
  retryCode: null,
  retryScheduled: false,
  retryDelayMs: null,
  bufferedCount: 0,
  bufferedBytes: 0,
  bufferCountLimit: 0,
  bufferByteLimit: 0,
});

export function createBoardStore(
  calculateHash: (state: BoardState) => Promise<string> = hashBoardState,
) {
  const store = create<BoardStore>((set, get) => {
    const isCurrent = (token: BoardSessionToken, boardId?: string): boolean => {
      const current = get();
      return (
        current.sessionToken === token && (boardId === undefined || current.boardId === boardId)
      );
    };
    const scheduleHash = (token: BoardSessionToken, boardId: string, state: BoardState): void => {
      const seq = state.lastSeq;
      const sessionGeneration = token.generation;
      void calculateHash(state)
        .then((value) => {
          const current = get();
          if (
            current.sessionToken !== token ||
            current.boardId !== boardId ||
            current.committed.lastSeq !== seq
          )
            return;
          set({
            authoritativeHash: {
              status: "ready",
              boardId,
              sessionGeneration,
              seq,
              value,
            },
          });
        })
        .catch(() => {
          const current = get();
          if (
            current.sessionToken !== token ||
            current.boardId !== boardId ||
            current.committed.lastSeq !== seq
          )
            return;
          set({
            authoritativeHash: {
              status: "error",
              boardId,
              sessionGeneration,
              seq,
              value: null,
            },
          });
        });
    };

    return {
      sessionToken: null,
      sessionGeneration: null,
      boardId: null,
      name: "Untitled board",
      committed: emptyBoardState(),
      pending: [],
      buffered: {},
      appliedOpIds: {},
      appliedSeqOpIds: {},
      objects: [],
      selectedId: null,
      selectionNotice: null,
      connection: "disconnected",
      pendingStatus: "idle",
      synchronizationDiagnostics: idleSynchronizationDiagnostics(),
      authoritativeHash: idleHash(),
      error: null,
      beginSession(sessionToken, boardId) {
        set({
          sessionToken,
          sessionGeneration: sessionToken.generation,
          boardId,
          name: "Untitled board",
          committed: emptyBoardState(),
          pending: [],
          buffered: {},
          appliedOpIds: {},
          appliedSeqOpIds: {},
          objects: [],
          selectedId: null,
          selectionNotice: null,
          connection: "connecting",
          pendingStatus: "idle",
          synchronizationDiagnostics: idleSynchronizationDiagnostics(),
          authoritativeHash: idleHash(),
          error: null,
        });
      },
      bindSessionBoard(sessionToken, boardId) {
        if (!isCurrent(sessionToken)) return false;
        set({ boardId });
        return true;
      },
      isCurrentSession: isCurrent,
      initializeSession(sessionToken, snapshot, pending) {
        if (!isCurrent(sessionToken, snapshot.id)) return false;
        if (pending.some((command) => command.boardId !== snapshot.id)) return false;
        const committed = stateFromSnapshot(snapshot);
        const objects = derive(committed, pending);
        set({
          name: snapshot.name,
          committed,
          pending,
          objects,
          ...currentSelection(objects, get().selectedId),
          buffered: {},
          appliedOpIds: {},
          appliedSeqOpIds: {},
          authoritativeHash: {
            status: "pending",
            boardId: snapshot.id,
            sessionGeneration: sessionToken.generation,
            seq: committed.lastSeq,
            value: null,
          },
          error: null,
        });
        scheduleHash(sessionToken, snapshot.id, committed);
        return true;
      },
      rebaseSession(sessionToken, snapshot) {
        if (!isCurrent(sessionToken, snapshot.id)) return false;
        const current = get();
        const committed = stateFromSnapshot(snapshot);
        const objects = derive(committed, current.pending);
        set({
          name: snapshot.name,
          committed,
          objects,
          ...currentSelection(objects, current.selectedId),
          buffered: {},
          appliedOpIds: {},
          appliedSeqOpIds: {},
          authoritativeHash: {
            status: "pending",
            boardId: snapshot.id,
            sessionGeneration: sessionToken.generation,
            seq: committed.lastSeq,
            value: null,
          },
          error: null,
        });
        scheduleHash(sessionToken, snapshot.id, committed);
        return true;
      },
      rebaseRecoveredSession(sessionToken, recoveredBoardId, boardName, recovered) {
        if (!isCurrent(sessionToken, recoveredBoardId)) return false;
        const current = get();
        const committed = cloneBoardState(recovered);
        const objects = derive(committed, current.pending);
        set({
          name: boardName,
          committed,
          objects,
          ...currentSelection(objects, current.selectedId),
          buffered: {},
          appliedOpIds: {},
          appliedSeqOpIds: {},
          authoritativeHash: {
            status: "pending",
            boardId: recoveredBoardId,
            sessionGeneration: sessionToken.generation,
            seq: committed.lastSeq,
            value: null,
          },
          error: null,
        });
        scheduleHash(sessionToken, recoveredBoardId, committed);
        return true;
      },
      revokeSession(sessionToken, revokedBoardId, error) {
        if (!isCurrent(sessionToken, revokedBoardId)) return false;
        set({
          name: "Access revoked",
          committed: emptyBoardState(),
          buffered: {},
          appliedOpIds: {},
          appliedSeqOpIds: {},
          objects: [],
          selectedId: null,
          selectionNotice: null,
          connection: "authorization-failed",
          synchronizationDiagnostics: {
            ...get().synchronizationDiagnostics,
            retryCode: "ACCESS_REVOKED",
            retryScheduled: false,
            retryDelayMs: null,
            bufferedCount: 0,
            bufferedBytes: 0,
          },
          authoritativeHash: idleHash(),
          error,
        });
        return true;
      },
      failSession(sessionToken, error) {
        if (!isCurrent(sessionToken)) return;
        set({ connection: "error", error });
      },
      endSession(sessionToken) {
        if (!isCurrent(sessionToken)) return;
        set({
          sessionToken: null,
          sessionGeneration: null,
          boardId: null,
          name: "Untitled board",
          committed: emptyBoardState(),
          pending: [],
          buffered: {},
          appliedOpIds: {},
          appliedSeqOpIds: {},
          objects: [],
          selectedId: null,
          selectionNotice: null,
          connection: "disconnected",
          pendingStatus: "idle",
          synchronizationDiagnostics: idleSynchronizationDiagnostics(),
          authoritativeHash: idleHash(),
          error: null,
        });
      },
      setConnection(sessionToken, connection) {
        if (!isCurrent(sessionToken)) return;
        set({ connection });
      },
      setSynchronizationError(sessionToken, error, authorizationFailed = false) {
        if (!isCurrent(sessionToken)) return;
        set({
          connection: authorizationFailed ? "authorization-failed" : "error",
          error,
        });
      },
      setSynchronizationDiagnostics(sessionToken, diagnostics) {
        if (!isCurrent(sessionToken)) return;
        set({
          synchronizationDiagnostics: {
            ...get().synchronizationDiagnostics,
            ...diagnostics,
          },
        });
      },
      setPendingStatus(sessionToken, pendingStatus, message) {
        if (!isCurrent(sessionToken)) return;
        set({ pendingStatus, ...(message === undefined ? {} : { error: message }) });
      },
      select(selectedId) {
        const current = get();
        const selection = currentSelection(current.objects, selectedId);
        set({ ...selection, selectionNotice: selectedId ? null : selection.selectionNotice });
      },
      addPersistedPending(sessionToken, command) {
        const current = get();
        if (!isCurrent(sessionToken, command.boardId)) return false;
        if (current.pending.some(({ opId }) => opId === command.opId)) return true;
        const pending = [...current.pending, command];
        const objects = derive(current.committed, pending);
        set({
          pending,
          objects,
          ...currentSelection(objects, current.selectedId),
          error: null,
        });
        return true;
      },
      removePending(sessionToken, opId, error) {
        if (!isCurrent(sessionToken)) return;
        const pending = get().pending.filter((command) => command.opId !== opId);
        const current = get();
        const objects = derive(current.committed, pending);
        set({
          pending,
          objects,
          ...currentSelection(objects, current.selectedId),
          ...(error === undefined ? {} : { error }),
        });
      },
      ingest(sessionToken, operation) {
        if (!isCurrent(sessionToken)) return "stale";
        const current = get();
        if (operation.boardId !== current.boardId) {
          set({ error: "Committed operation belongs to another board", connection: "error" });
          return "conflict";
        }
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
        const advanced = committed.lastSeq !== current.committed.lastSeq;
        const objects = derive(committed, get().pending);
        set({
          committed,
          buffered,
          appliedOpIds,
          appliedSeqOpIds,
          objects,
          ...currentSelection(objects, current.selectedId),
          ...(advanced && current.boardId
            ? {
                authoritativeHash: {
                  status: "pending" as const,
                  boardId: current.boardId,
                  sessionGeneration: sessionToken.generation,
                  seq: committed.lastSeq,
                  value: null,
                },
              }
            : {}),
        });
        if (advanced && current.boardId) scheduleHash(sessionToken, current.boardId, committed);
        return operation.seq <= committed.lastSeq ? "applied" : "buffered";
      },
    };
  });
  return store;
}

export const useBoardStore = createBoardStore();

export function firstBufferedGap(token: BoardSessionToken): { from: number; to: number } | null {
  const state = useBoardStore.getState();
  if (!state.isCurrentSession(token)) return null;
  const { committed, buffered } = state;
  const sequences = Object.keys(buffered)
    .map(Number)
    .sort((a, b) => a - b);
  const first = sequences[0];
  return first && first > committed.lastSeq + 1
    ? { from: committed.lastSeq + 1, to: first - 1 }
    : null;
}
