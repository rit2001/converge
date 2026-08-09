import { boardSnapshotSchema, type BoardSnapshot, type DurableCommand } from "@converge/protocol";
import type { PendingRecoveryStatus } from "./pending-command-queue";
import type { PendingLoadResult } from "./pending-db";

export type BoardSessionToken = Readonly<{ generation: number; nonce: symbol }>;

export interface BoardSessionStoreBoundary {
  beginSession(token: BoardSessionToken, boardId: string | null): void;
  bindSessionBoard(token: BoardSessionToken, boardId: string): boolean;
  isCurrentSession(token: BoardSessionToken, boardId?: string): boolean;
  initializeSession(
    token: BoardSessionToken,
    snapshot: BoardSnapshot,
    pending: DurableCommand[],
  ): boolean;
  failSession(token: BoardSessionToken, message: string): void;
  setPendingStatus(
    token: BoardSessionToken,
    status: PendingRecoveryStatus,
    message?: string | null,
  ): void;
  endSession(token: BoardSessionToken): void;
}

export interface OwnedBoardTransport {
  connect(): void;
  disconnect(): void;
  submit(command: DurableCommand): Promise<boolean>;
}

export interface BoardSessionDependencies {
  store: BoardSessionStoreBoundary;
  createBoard(signal: AbortSignal): Promise<BoardSnapshot>;
  loadSnapshot(boardId: string, signal: AbortSignal): Promise<BoardSnapshot>;
  loadPending(boardId: string): Promise<PendingLoadResult>;
  updateBoardLocation(boardId: string): void;
  createTransport(boardId: string, token: BoardSessionToken): OwnedBoardTransport;
}

export interface BoardSessionHandle {
  readonly token: BoardSessionToken;
  completion: Promise<void>;
  submit(command: DurableCommand): Promise<boolean>;
  cancel(): void;
}

let nextGeneration = 0;

function sessionToken(): BoardSessionToken {
  nextGeneration += 1;
  return Object.freeze({ generation: nextGeneration, nonce: Symbol("board-session") });
}

function isCancellation(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

export class BoardSessionController {
  private current: BoardSessionHandle | null = null;

  constructor(private readonly dependencies: BoardSessionDependencies) {}

  start(requestedBoardId: string | null): BoardSessionHandle {
    this.current?.cancel();
    const token = sessionToken();
    const abortController = new AbortController();
    let cancelled = false;
    let transport: OwnedBoardTransport | null = null;
    this.dependencies.store.beginSession(token, requestedBoardId);

    const isActive = (boardId?: string): boolean =>
      !cancelled && this.dependencies.store.isCurrentSession(token, boardId);
    const handle: BoardSessionHandle = {
      token,
      completion: Promise.resolve(),
      submit: async (command) => {
        await handle.completion;
        if (cancelled || !transport) return false;
        return transport.submit(command);
      },
      cancel: () => {
        if (cancelled) return;
        cancelled = true;
        abortController.abort();
        transport?.disconnect();
        transport = null;
        this.dependencies.store.endSession(token);
      },
    };
    this.current = handle;
    handle.completion = (async () => {
      try {
        let boardId = requestedBoardId;
        if (boardId === null) {
          const created = boardSnapshotSchema.parse(
            await this.dependencies.createBoard(abortController.signal),
          );
          if (!isActive()) return;
          boardId = created.id;
          if (!this.dependencies.store.bindSessionBoard(token, boardId)) return;
          if (!isActive(boardId)) return;
          this.dependencies.updateBoardLocation(boardId);
        }

        const snapshot = boardSnapshotSchema.parse(
          await this.dependencies.loadSnapshot(boardId, abortController.signal),
        );
        if (!isActive(boardId)) return;
        if (snapshot.id !== boardId) throw new Error("Board snapshot does not match session");
        const pending = await this.dependencies.loadPending(boardId);
        if (!isActive(boardId)) return;
        if (!this.dependencies.store.initializeSession(token, snapshot, pending.commands)) return;
        if (pending.corruptCount > 0)
          this.dependencies.store.setPendingStatus(
            token,
            "persistence-error",
            `LOCAL_PERSISTENCE_WARNING: Ignored ${pending.corruptCount} invalid pending row${pending.corruptCount === 1 ? "" : "s"}`,
          );
        if (!isActive(boardId)) return;
        transport = this.dependencies.createTransport(boardId, token);
        if (!isActive(boardId)) {
          transport.disconnect();
          transport = null;
          return;
        }
        transport.connect();
      } catch (error) {
        if (!isActive() || isCancellation(error)) return;
        this.dependencies.store.failSession(
          token,
          error instanceof Error ? error.message : "Startup failed",
        );
      }
    })();
    return handle;
  }

  stop(handle: BoardSessionHandle): void {
    handle.cancel();
    if (this.current === handle) this.current = null;
  }
}
