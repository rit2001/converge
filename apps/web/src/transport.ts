import { io, type Socket } from "socket.io-client";
import {
  joinBoardAckSchema,
  operationRangeResponseSchema,
  protocolErrorSchema,
  type ClientToServerEvents,
  type CommittedOperation,
  type DurableCommand,
  type JoinBoardAck,
  type ServerToClientEvents,
} from "@converge/protocol";
import type { BoardSessionToken } from "./board-session";
import { firstBufferedGap, useBoardStore } from "./board-store";
import {
  PendingCommandQueue,
  retryableSubmission,
  timedSubmission,
  type RetryScheduler,
  type SubmissionAttempt,
} from "./pending-command-queue";
import { indexedDbPendingOperationStore, type PendingOperationStore } from "./pending-db";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:4000";
const ACK_TIMEOUT_MS = 10_000;
const transportScheduler: RetryScheduler = {
  setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
  random: () => Math.random(),
};

export interface BoardTransportOptions {
  pendingStore?: PendingOperationStore;
  scheduler?: RetryScheduler;
}

class SynchronizationError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
  }
}

export class BoardTransport {
  private socket: Socket<ServerToClientEvents, ClientToServerEvents> | null = null;
  private synchronizationGeneration = 0;
  private synchronizing = false;
  private ready = false;
  private liveConflict = false;
  private readonly liveBuffer = new Map<number, CommittedOperation>();
  private readonly scheduler: RetryScheduler;
  private readonly pendingQueue: PendingCommandQueue;

  constructor(
    private readonly boardId: string,
    private readonly clientId: string,
    private readonly sessionToken: BoardSessionToken,
    options: BoardTransportOptions = {},
  ) {
    this.scheduler = options.scheduler ?? transportScheduler;
    const state = useBoardStore.getState();
    this.pendingQueue = new PendingCommandQueue({
      boardId,
      initialCommands: state.pending,
      persistence: options.pendingStore ?? indexedDbPendingOperationStore,
      scheduler: this.scheduler,
      isActive: () => this.isSessionActive(),
      addPersisted: (command) =>
        useBoardStore.getState().addPersistedPending(this.sessionToken, command),
      removePending: (operationId, error) =>
        useBoardStore.getState().removePending(this.sessionToken, operationId, error),
      ingest: (operation) => useBoardStore.getState().ingest(this.sessionToken, operation),
      setStatus: (status, message) =>
        useBoardStore.getState().setPendingStatus(this.sessionToken, status, message),
      submit: (command) => this.createSubmissionAttempt(command),
      requestSynchronization: () => {
        if (!this.isSessionActive()) return;
        this.ready = false;
        this.pendingQueue.setReady(false);
        if (this.socket?.connected) this.beginSynchronization(this.socket);
      },
    });
  }

  connect(): void {
    if (!this.isSessionActive()) return;
    useBoardStore.getState().setConnection(this.sessionToken, "connecting");
    const socket = io(API_URL, { auth: {}, reconnection: true });
    this.socket = socket;
    socket.on("connect", () => this.beginSynchronization(socket));
    socket.on("disconnect", () => {
      this.ready = false;
      this.pendingQueue.setReady(false);
      this.synchronizing = false;
      this.synchronizationGeneration += 1;
      this.liveBuffer.clear();
      this.liveConflict = false;
      useBoardStore.getState().setConnection(this.sessionToken, "disconnected");
    });
    socket.on("operation:committed", (operation: CommittedOperation) => {
      if (!this.isSessionActive()) return;
      if (!this.ready || this.synchronizing) {
        this.bufferLive(operation);
        return;
      }
      const result = this.pendingQueue.observeCommitted(operation);
      if (result === "buffered") this.beginSynchronization(socket);
      if (result === "conflict") this.ready = false;
    });
    socket.io.on("reconnect_attempt", () =>
      useBoardStore.getState().setConnection(this.sessionToken, "connecting"),
    );
    socket.io.on("reconnect_failed", () =>
      useBoardStore
        .getState()
        .setSynchronizationError(this.sessionToken, "Socket reconnection failed"),
    );
  }

  submit(command: DurableCommand): Promise<boolean> {
    return this.pendingQueue.enqueue(command);
  }

  disconnect(): void {
    this.ready = false;
    this.pendingQueue.cancel();
    this.synchronizing = false;
    this.synchronizationGeneration += 1;
    this.liveBuffer.clear();
    this.liveConflict = false;
    this.socket?.disconnect();
    this.socket = null;
    useBoardStore.getState().setConnection(this.sessionToken, "disconnected");
  }

  private beginSynchronization(socket: Socket<ServerToClientEvents, ClientToServerEvents>): void {
    if (!this.isSessionActive() || !socket.connected || this.synchronizing) return;
    this.ready = false;
    this.pendingQueue.setReady(false);
    this.synchronizing = true;
    const generation = ++this.synchronizationGeneration;
    void this.synchronize(socket, generation)
      .catch((error: unknown) => {
        if (generation !== this.synchronizationGeneration || !this.isSessionActive()) return;
        const failure =
          error instanceof SynchronizationError
            ? error
            : new SynchronizationError("INTERNAL_ERROR", "Board synchronization failed", true);
        const authorizationFailed =
          failure.code === "FORBIDDEN" ||
          failure.code === "BOARD_NOT_FOUND" ||
          failure.code === "AUTHENTICATION_REQUIRED";
        useBoardStore
          .getState()
          .setSynchronizationError(
            this.sessionToken,
            `${failure.code}: ${failure.message}`,
            authorizationFailed,
          );
      })
      .finally(() => {
        if (generation === this.synchronizationGeneration) this.synchronizing = false;
      });
  }

  private async synchronize(
    socket: Socket<ServerToClientEvents, ClientToServerEvents>,
    generation: number,
  ): Promise<void> {
    while (this.isCurrent(socket, generation)) {
      const store = useBoardStore.getState();
      store.setConnection(this.sessionToken, "joining");
      const acknowledgement = await this.requestJoin(socket);
      this.assertCurrent(socket, generation);
      if (!acknowledgement.ok)
        throw new SynchronizationError(
          acknowledgement.code,
          acknowledgement.message,
          acknowledgement.retryable,
        );
      if (acknowledgement.boardId !== this.boardId)
        throw new SynchronizationError("RESYNC_REQUIRED", "Join board mismatch", true);

      const localSequence = useBoardStore.getState().committed.lastSeq;
      if (localSequence > acknowledgement.joinWatermark)
        throw new SynchronizationError(
          "RESYNC_REQUIRED",
          "Local sequence exceeds join watermark",
          true,
        );
      if (localSequence < acknowledgement.joinWatermark) {
        useBoardStore.getState().setConnection(this.sessionToken, "catching-up");
        await this.catchUp(acknowledgement.joinWatermark, socket, generation);
      }

      this.drainLiveBuffer();
      if (firstBufferedGap(this.sessionToken)) continue;
      if (useBoardStore.getState().committed.lastSeq < acknowledgement.joinWatermark)
        throw new SynchronizationError(
          "RESYNC_REQUIRED",
          "Catch-up did not reach the join watermark",
          true,
        );

      this.ready = true;
      useBoardStore.getState().setConnection(this.sessionToken, "ready");
      this.pendingQueue.setReady(true);
      return;
    }
  }

  private async catchUp(
    watermark: number,
    socket: Socket<ServerToClientEvents, ClientToServerEvents>,
    generation: number,
  ): Promise<void> {
    while (useBoardStore.getState().committed.lastSeq < watermark) {
      this.assertCurrent(socket, generation);
      const after = useBoardStore.getState().committed.lastSeq;
      const response = await fetch(
        `${API_URL}/v1/boards/${this.boardId}/operations?after=${after}&watermark=${watermark}`,
      );
      this.assertCurrent(socket, generation);
      const raw: unknown = await response.json();
      this.assertCurrent(socket, generation);
      if (!response.ok) {
        const failure = protocolErrorSchema.safeParse(raw);
        if (failure.success)
          throw new SynchronizationError(
            failure.data.code,
            failure.data.message,
            failure.data.retryable,
          );
        throw new SynchronizationError("INTERNAL_ERROR", "Catch-up request failed", true);
      }
      const batch = operationRangeResponseSchema.parse(raw);
      if (
        batch.boardId !== this.boardId ||
        batch.afterSeq !== after ||
        batch.watermark !== watermark
      )
        throw new SynchronizationError("RESYNC_REQUIRED", "Catch-up response mismatch", true);
      for (const operation of batch.operations) {
        const result = this.pendingQueue.observeCommitted(operation);
        if (result === "conflict")
          throw new SynchronizationError("RESYNC_REQUIRED", "Conflicting operation delivery", true);
      }
      if (useBoardStore.getState().committed.lastSeq <= after)
        throw new SynchronizationError("RESYNC_REQUIRED", "Catch-up made no progress", true);
      if (!batch.hasMore && useBoardStore.getState().committed.lastSeq < watermark)
        throw new SynchronizationError("RESYNC_REQUIRED", "Catch-up ended before watermark", true);
    }
  }

  private requestJoin(
    socket: Socket<ServerToClientEvents, ClientToServerEvents>,
  ): Promise<JoinBoardAck> {
    return new Promise((resolve, reject) => {
      const timeout = globalThis.setTimeout(
        () =>
          reject(
            new SynchronizationError("INTERNAL_ERROR", "Join acknowledgement timed out", true),
          ),
        ACK_TIMEOUT_MS,
      );
      const state = useBoardStore.getState();
      socket.emit(
        "board:join",
        {
          schemaVersion: 1,
          boardId: this.boardId,
          clientId: this.clientId,
          lastAppliedSeq: state.committed.lastSeq,
        },
        (raw) => {
          globalThis.clearTimeout(timeout);
          const acknowledgement = joinBoardAckSchema.safeParse(raw);
          if (!acknowledgement.success) {
            reject(
              new SynchronizationError("INTERNAL_ERROR", "Invalid join acknowledgement", true),
            );
            return;
          }
          resolve(acknowledgement.data);
        },
      );
    });
  }

  private bufferLive(operation: CommittedOperation): void {
    const existing = this.liveBuffer.get(operation.seq);
    if (existing && existing.opId !== operation.opId) {
      this.liveConflict = true;
      useBoardStore
        .getState()
        .setSynchronizationError(this.sessionToken, "RESYNC_REQUIRED: Conflicting live operations");
      return;
    }
    this.liveBuffer.set(operation.seq, operation);
  }

  private drainLiveBuffer(): void {
    if (this.liveConflict) {
      this.liveConflict = false;
      throw new SynchronizationError("RESYNC_REQUIRED", "Conflicting live operations", true);
    }
    const operations = [...this.liveBuffer.values()].sort((left, right) => left.seq - right.seq);
    this.liveBuffer.clear();
    for (const operation of operations) {
      const result = this.pendingQueue.observeCommitted(operation);
      if (result === "conflict")
        throw new SynchronizationError("RESYNC_REQUIRED", "Conflicting operation delivery", true);
    }
  }

  private isCurrent(
    socket: Socket<ServerToClientEvents, ClientToServerEvents>,
    generation: number,
  ): boolean {
    return (
      socket.connected && generation === this.synchronizationGeneration && this.isSessionActive()
    );
  }

  private assertCurrent(
    socket: Socket<ServerToClientEvents, ClientToServerEvents>,
    generation: number,
  ): void {
    if (!this.isCurrent(socket, generation))
      throw new SynchronizationError("INTERNAL_ERROR", "Synchronization interrupted", true);
  }

  private isSessionActive(): boolean {
    return useBoardStore.getState().isCurrentSession(this.sessionToken, this.boardId);
  }

  private createSubmissionAttempt(command: DurableCommand): SubmissionAttempt {
    if (!this.isSessionActive() || !this.socket?.connected || !this.ready)
      return retryableSubmission("Operation transport is not ready");
    return timedSubmission(this.scheduler, ACK_TIMEOUT_MS, (acknowledge) =>
      this.socket?.emit("operation:submit", command, acknowledge),
    );
  }
}

export { API_URL };
