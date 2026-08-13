import { io, type Socket } from "socket.io-client";
import { hashBoardState, reduceCommand, type BoardState } from "@converge/canvas-engine";
import {
  boardAccessRevokedEventSchema,
  boardRecoveryMaterialSchema,
  committedOperationSchema,
  joinBoardAckSchema,
  operationRangeResponseSchema,
  protocolErrorSchema,
  type BoardRecoveryMaterial,
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
export const SYNC_ACK_TIMEOUT_MS = 10_000;
export const SYNC_RETRY_BASE_MS = 500;
export const SYNC_RETRY_CAP_MS = 10_000;
export const LIVE_BUFFER_MAX_COUNT = 1_000;
export const LIVE_BUFFER_MAX_BYTES = 2 * 1024 * 1024;

type BoardSocket = Socket<ServerToClientEvents, ClientToServerEvents>;

const transportScheduler: RetryScheduler = {
  setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
  random: () => Math.random(),
};

const SNAPSHOT_HASH_DOMAIN = "converge.snapshot.v1";

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, item]) => item !== undefined)
        .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
        .map(([key, item]) => [key, canonicalValue(item)]),
    );
  }
  if (typeof value === "number" && Object.is(value, -0)) return 0;
  return value;
}

async function hashRecoverySnapshot(
  snapshot: BoardRecoveryMaterial["snapshotState"],
): Promise<string> {
  const canonical = JSON.stringify(canonicalValue(snapshot));
  const bytes = new TextEncoder().encode(`${SNAPSHOT_HASH_DOMAIN}\0${canonical}`);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function recoverySnapshotState(material: BoardRecoveryMaterial): BoardState {
  return {
    lastSeq: material.snapshotCanvasSeq,
    objects: Object.fromEntries(
      material.snapshotState.objects.map((object) => [
        object.objectId,
        {
          value: { ...object.value },
          fieldSeq: { ...object.fieldSeq },
          createdSeq: object.createdSeq,
          updatedSeq: object.updatedSeq,
          deletedSeq: object.deletedSeq,
        },
      ]),
    ),
    order: material.snapshotState.objects
      .filter((object) => object.deletedSeq === null)
      .map((object) => object.objectId),
  };
}

function invalidRecovery(message: string): SynchronizationError {
  return new SynchronizationError("INVALID_COMMAND", message, false);
}

async function verifyRecoveryMaterial(
  material: BoardRecoveryMaterial,
  activeBoardId: string,
): Promise<BoardState> {
  const heads = [
    material.snapshotCanvasSeq,
    material.snapshotDeliverySeq,
    material.capturedCanvasSeq,
    material.capturedDeliverySeq,
  ];
  if (
    material.boardId !== activeBoardId ||
    heads.some((head) => !Number.isSafeInteger(head) || head < 0) ||
    material.snapshotCanvasSeq > material.capturedCanvasSeq ||
    material.snapshotDeliverySeq > material.capturedDeliverySeq ||
    material.capturedDeliverySeq < material.capturedCanvasSeq ||
    material.snapshotState.boardId !== material.boardId ||
    material.snapshotState.lastSeq !== material.snapshotCanvasSeq ||
    material.snapshotState.lastDeliverySeq !== material.snapshotDeliverySeq
  )
    throw invalidRecovery("Recovery metadata does not match the active board");

  if ((await hashRecoverySnapshot(material.snapshotState)) !== material.snapshotCanonicalHash)
    throw invalidRecovery("Recovery snapshot hash verification failed");

  let expectedSequence = material.snapshotCanvasSeq + 1;
  let committed = recoverySnapshotState(material);
  const snapshotObjectIds = new Set<string>();
  for (const object of material.snapshotState.objects) {
    if (snapshotObjectIds.has(object.objectId))
      throw invalidRecovery("Recovery snapshot contains duplicate object identity");
    snapshotObjectIds.add(object.objectId);
  }
  const operationIds = new Set<string>();
  for (const operation of material.operationTail) {
    if (
      operation.boardId !== activeBoardId ||
      !Number.isSafeInteger(operation.seq) ||
      operation.seq !== expectedSequence ||
      operation.seq > material.capturedCanvasSeq
    )
      throw invalidRecovery("Recovery operation tail is not contiguous");
    if (operationIds.has(operation.opId))
      throw invalidRecovery("Recovery operation tail contains duplicate identity");
    operationIds.add(operation.opId);
    const reduced = reduceCommand(committed, operation, operation.seq);
    if (!reduced.ok) throw invalidRecovery("Recovery operation replay failed");
    committed = reduced.state;
    expectedSequence += 1;
  }
  if (
    committed.lastSeq !== material.capturedCanvasSeq ||
    expectedSequence - 1 !== material.capturedCanvasSeq ||
    (material.snapshotCanvasSeq === material.capturedCanvasSeq &&
      material.operationTail.length !== 0)
  )
    throw invalidRecovery("Recovery operation tail does not reach the captured head");

  if ((await hashBoardState(committed)) !== material.reconstructedCanonicalHash)
    throw invalidRecovery("Reconstructed board hash verification failed");
  return committed;
}

interface SynchronizationAttempt {
  id: number;
  connectionGeneration: number;
  startingSeq: number;
  retryNumber: number;
  joinWatermark: number | null;
  cancelled: boolean;
  readonly abortController: AbortController;
  readonly cancelers: Set<() => void>;
}

interface BufferedLiveOperation {
  operation: CommittedOperation;
  serialized: string;
  bytes: number;
}

export interface SynchronizationLimits {
  acknowledgementTimeoutMs: number;
  retryBaseMs: number;
  retryCapMs: number;
  liveBufferMaxCount: number;
  liveBufferMaxBytes: number;
}

export interface BoardTransportOptions {
  pendingStore?: PendingOperationStore;
  scheduler?: RetryScheduler;
  apiUrl?: string;
  socketFactory?: (apiUrl: string) => BoardSocket;
  fetcher?: typeof fetch;
  loadRecovery?: (boardId: string, signal: AbortSignal) => Promise<unknown>;
  synchronization?: Partial<SynchronizationLimits>;
}

export class SynchronizationError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
  }
}

const terminalAuthorizationCodes = new Set([
  "AUTHENTICATION_REQUIRED",
  "INVALID_AUTH_INPUT",
  "FORBIDDEN",
  "BOARD_NOT_FOUND",
]);

export class BoardTransport {
  private socket: BoardSocket | null = null;
  private connectionGeneration = 0;
  private nextAttemptId = 0;
  private activeAttempt: SynchronizationAttempt | null = null;
  private retryTimer: unknown = null;
  private synchronizationFailures = 0;
  private ready = false;
  private terminal = false;
  private readonly liveBuffer = new Map<number, BufferedLiveOperation>();
  private readonly liveBufferOpSequences = new Map<string, number>();
  private liveBufferBytes = 0;
  private readonly scheduler: RetryScheduler;
  private readonly pendingQueue: PendingCommandQueue;
  private readonly apiUrl: string;
  private readonly socketFactory: (apiUrl: string) => BoardSocket;
  private readonly fetcher: typeof fetch;
  private readonly loadRecovery: (boardId: string, signal: AbortSignal) => Promise<unknown>;
  private readonly limits: SynchronizationLimits;

  constructor(
    private readonly boardId: string,
    private readonly clientId: string,
    private readonly sessionToken: BoardSessionToken,
    options: BoardTransportOptions = {},
  ) {
    this.scheduler = options.scheduler ?? transportScheduler;
    this.apiUrl = options.apiUrl ?? API_URL;
    this.socketFactory =
      options.socketFactory ?? ((url) => io(url, { auth: {}, reconnection: true }));
    this.fetcher = options.fetcher ?? globalThis.fetch.bind(globalThis);
    this.loadRecovery = options.loadRecovery ?? ((id, signal) => this.fetchRecovery(id, signal));
    this.limits = {
      acknowledgementTimeoutMs: SYNC_ACK_TIMEOUT_MS,
      retryBaseMs: SYNC_RETRY_BASE_MS,
      retryCapMs: SYNC_RETRY_CAP_MS,
      liveBufferMaxCount: LIVE_BUFFER_MAX_COUNT,
      liveBufferMaxBytes: LIVE_BUFFER_MAX_BYTES,
      ...options.synchronization,
    };
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
      requestSynchronization: (reason) => {
        if (!this.isSessionActive()) return;
        if (reason) {
          const socket = this.socket;
          if (socket) this.recoverFromIngestResult(socket, reason, "acknowledgement");
          return;
        }
        this.ready = false;
        this.pendingQueue.setReady(false);
        if (this.socket?.connected) this.beginSynchronization(this.socket);
      },
    });
    this.updateDiagnostics({
      bufferCountLimit: this.limits.liveBufferMaxCount,
      bufferByteLimit: this.limits.liveBufferMaxBytes,
    });
  }

  connect(): void {
    if (!this.isSessionActive()) return;
    useBoardStore.getState().setConnection(this.sessionToken, "connecting");
    const socket = this.socketFactory(this.apiUrl);
    this.socket = socket;
    socket.on("connect", () => {
      if (!this.isSessionActive() || socket !== this.socket) return;
      this.connectionGeneration += 1;
      this.cancelRetry();
      this.invalidateAttempt();
      this.beginSynchronization(socket);
    });
    socket.on("connect_error", (error: unknown) => this.receiveConnectionError(socket, error));
    socket.on("disconnect", () => {
      if (this.terminal || socket !== this.socket) return;
      this.ready = false;
      this.pendingQueue.setReady(false);
      this.connectionGeneration += 1;
      this.cancelRetry();
      this.invalidateAttempt();
      this.clearLiveBuffer();
      if (!this.terminal) useBoardStore.getState().setConnection(this.sessionToken, "disconnected");
    });
    socket.on("operation:committed", (raw: unknown) => this.receiveLive(socket, raw));
    socket.on("board:access-revoked", (raw: unknown) => this.receiveAccessRevoked(socket, raw));
    socket.io.on("reconnect_attempt", () => {
      if (!this.isSessionActive() || socket !== this.socket) return;
      useBoardStore.getState().setConnection(this.sessionToken, "connecting");
    });
    socket.io.on("reconnect_failed", () => {
      if (!this.isSessionActive() || socket !== this.socket) return;
      useBoardStore
        .getState()
        .setSynchronizationError(this.sessionToken, "Socket reconnection failed");
    });
    if (!socket.connected) socket.connect();
  }

  submit(command: DurableCommand): Promise<boolean> {
    return this.pendingQueue.enqueue(command);
  }

  disconnect(): void {
    this.ready = false;
    this.pendingQueue.cancel();
    this.connectionGeneration += 1;
    this.cancelRetry();
    this.invalidateAttempt();
    this.clearLiveBuffer();
    this.socket?.disconnect();
    this.socket = null;
    if (!this.terminal) useBoardStore.getState().setConnection(this.sessionToken, "disconnected");
  }

  private beginSynchronization(socket: BoardSocket): void {
    if (!this.isSessionActive() || !socket.connected || this.activeAttempt || this.retryTimer)
      return;
    this.ready = false;
    this.pendingQueue.setReady(false);
    this.clearLiveBuffer();
    const attempt: SynchronizationAttempt = {
      id: ++this.nextAttemptId,
      connectionGeneration: this.connectionGeneration,
      startingSeq: useBoardStore.getState().committed.lastSeq,
      retryNumber: this.synchronizationFailures,
      joinWatermark: null,
      cancelled: false,
      abortController: new AbortController(),
      cancelers: new Set(),
    };
    this.activeAttempt = attempt;
    this.updateDiagnostics({
      attempt: attempt.id,
      retryCode: null,
      retryScheduled: false,
      retryDelayMs: null,
    });
    void this.synchronize(socket, attempt).catch((error: unknown) => {
      if (!this.isCurrentAttempt(socket, attempt)) return;
      const failure =
        error instanceof SynchronizationError
          ? error
          : new SynchronizationError("INTERNAL_ERROR", "Board synchronization failed", true);
      this.failAttempt(socket, attempt, failure);
    });
  }

  private async synchronize(socket: BoardSocket, attempt: SynchronizationAttempt): Promise<void> {
    useBoardStore.getState().setConnection(this.sessionToken, "joining");
    const acknowledgement = await this.requestJoin(socket, attempt);
    this.assertCurrent(socket, attempt);
    if (!acknowledgement.ok) {
      if (acknowledgement.code === "RESYNC_REQUIRED") {
        await this.rebaseFromRecovery(socket, attempt);
        throw new SynchronizationError(
          "RESYNC_REQUIRED",
          "Authoritative recovery material applied; joining again",
          true,
        );
      }
      throw new SynchronizationError(
        acknowledgement.code,
        acknowledgement.message,
        acknowledgement.retryable,
      );
    }
    if (acknowledgement.boardId !== this.boardId)
      throw new SynchronizationError("INVALID_COMMAND", "Join board mismatch", false);
    attempt.joinWatermark = acknowledgement.joinWatermark;

    const localSequence = useBoardStore.getState().committed.lastSeq;
    if (localSequence > acknowledgement.joinWatermark) {
      await this.rebaseFromRecovery(socket, attempt);
      throw new SynchronizationError(
        "RESYNC_REQUIRED",
        "Local sequence exceeded the join watermark; joining again",
        true,
      );
    }
    if (localSequence < acknowledgement.joinWatermark) {
      useBoardStore.getState().setConnection(this.sessionToken, "catching-up");
      await this.catchUp(acknowledgement.joinWatermark, socket, attempt);
    }

    this.assertCurrent(socket, attempt);
    this.drainLiveBuffer();
    if (firstBufferedGap(this.sessionToken))
      throw new SynchronizationError("RESYNC_REQUIRED", "A live sequence gap remains", true);
    if (useBoardStore.getState().committed.lastSeq < acknowledgement.joinWatermark)
      throw new SynchronizationError(
        "RESYNC_REQUIRED",
        "Catch-up did not reach the join watermark",
        true,
      );

    this.completeAttempt(socket, attempt);
  }

  private async catchUp(
    watermark: number,
    socket: BoardSocket,
    attempt: SynchronizationAttempt,
  ): Promise<void> {
    while (useBoardStore.getState().committed.lastSeq < watermark) {
      this.assertCurrent(socket, attempt);
      const after = useBoardStore.getState().committed.lastSeq;
      const { response, raw } = await this.fetchRange(after, watermark, socket, attempt);
      this.assertCurrent(socket, attempt);
      if (!response.ok) {
        const failure = protocolErrorSchema.safeParse(raw);
        if (failure.success) {
          if (failure.data.code === "RESYNC_REQUIRED") {
            await this.rebaseFromRecovery(socket, attempt);
            throw new SynchronizationError(
              "RESYNC_REQUIRED",
              "Operation range was unavailable; authoritative snapshot reloaded",
              true,
            );
          }
          throw new SynchronizationError(
            failure.data.code,
            failure.data.message,
            failure.data.retryable,
          );
        }
        throw new SynchronizationError("INTERNAL_ERROR", "Catch-up request failed", true);
      }
      const parsed = operationRangeResponseSchema.safeParse(raw);
      if (!parsed.success)
        throw new SynchronizationError("INVALID_COMMAND", "Invalid catch-up response", false);
      const batch = parsed.data;
      if (
        batch.boardId !== this.boardId ||
        batch.afterSeq !== after ||
        batch.watermark !== watermark
      )
        throw new SynchronizationError("RESYNC_REQUIRED", "Catch-up response mismatch", true);
      for (const operation of batch.operations) {
        this.assertCurrent(socket, attempt);
        const result = this.pendingQueue.observeCommitted(operation);
        if (result === "conflict")
          throw new SynchronizationError("RESYNC_REQUIRED", "Conflicting operation delivery", true);
      }
      if (useBoardStore.getState().committed.lastSeq <= after) {
        await this.rebaseFromRecovery(socket, attempt);
        throw new SynchronizationError("RESYNC_REQUIRED", "Catch-up made no progress", true);
      }
      if (!batch.hasMore && useBoardStore.getState().committed.lastSeq < watermark) {
        await this.rebaseFromRecovery(socket, attempt);
        throw new SynchronizationError("RESYNC_REQUIRED", "Catch-up ended before watermark", true);
      }
    }
  }

  private requestJoin(socket: BoardSocket, attempt: SynchronizationAttempt): Promise<JoinBoardAck> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const finish = (callback: () => void): void => {
        if (settled) return;
        settled = true;
        this.scheduler.clearTimeout(timeout);
        attempt.cancelers.delete(cancel);
        callback();
      };
      const cancel = (): void =>
        finish(() =>
          reject(new SynchronizationError("INTERNAL_ERROR", "Join attempt was cancelled", true)),
        );
      const timeout = this.scheduler.setTimeout(
        () =>
          finish(() =>
            reject(
              new SynchronizationError("INTERNAL_ERROR", "Join acknowledgement timed out", true),
            ),
          ),
        this.limits.acknowledgementTimeoutMs,
      );
      attempt.cancelers.add(cancel);
      const state = useBoardStore.getState();
      socket.emit(
        "board:join",
        {
          schemaVersion: 1,
          boardId: this.boardId,
          clientId: this.clientId,
          lastAppliedSeq: state.committed.lastSeq,
        },
        (raw) =>
          finish(() => {
            if (!this.isCurrentAttempt(socket, attempt)) return;
            const acknowledgement = joinBoardAckSchema.safeParse(raw);
            if (!acknowledgement.success) {
              reject(
                new SynchronizationError("INVALID_COMMAND", "Invalid join acknowledgement", false),
              );
              return;
            }
            resolve(acknowledgement.data);
          }),
      );
    });
  }

  private async fetchRange(
    after: number,
    watermark: number,
    socket: BoardSocket,
    attempt: SynchronizationAttempt,
  ): Promise<{ response: Response; raw: unknown }> {
    const controller = new AbortController();
    let timedOut = false;
    let rejectInterruption!: (error: SynchronizationError) => void;
    const interruption = new Promise<never>((_resolve, reject) => {
      rejectInterruption = reject;
    });
    const interrupt = (error: SynchronizationError): void => {
      controller.abort();
      rejectInterruption(error);
    };
    const cancel = (): void =>
      interrupt(new SynchronizationError("INTERNAL_ERROR", "Catch-up attempt was cancelled", true));
    attempt.cancelers.add(cancel);
    const timeout = this.scheduler.setTimeout(() => {
      timedOut = true;
      interrupt(
        new SynchronizationError("INTERNAL_ERROR", "Catch-up acknowledgement timed out", true),
      );
    }, this.limits.acknowledgementTimeoutMs);
    try {
      let response: Response;
      try {
        response = await Promise.race([
          this.fetcher(
            `${this.apiUrl}/v1/boards/${this.boardId}/operations?after=${after}&watermark=${watermark}`,
            { signal: controller.signal },
          ),
          interruption,
        ]);
      } catch (error) {
        if (error instanceof SynchronizationError) throw error;
        if (timedOut)
          throw new SynchronizationError(
            "INTERNAL_ERROR",
            "Catch-up acknowledgement timed out",
            true,
          );
        if (attempt.cancelled) throw error;
        throw new SynchronizationError("INTERNAL_ERROR", "Catch-up request failed", true);
      }
      this.assertCurrent(socket, attempt);
      let raw: unknown;
      try {
        raw = await Promise.race([response.json(), interruption]);
      } catch (error) {
        if (error instanceof SynchronizationError) throw error;
        if (timedOut)
          throw new SynchronizationError(
            "INTERNAL_ERROR",
            "Catch-up acknowledgement timed out",
            true,
          );
        if (attempt.cancelled) throw error;
        throw new SynchronizationError("INTERNAL_ERROR", "Catch-up response was unreadable", true);
      }
      this.assertCurrent(socket, attempt);
      return { response, raw };
    } finally {
      this.scheduler.clearTimeout(timeout);
      attempt.cancelers.delete(cancel);
    }
  }

  private async rebaseFromRecovery(
    socket: BoardSocket,
    attempt: SynchronizationAttempt,
  ): Promise<void> {
    const controller = new AbortController();
    let timedOut = false;
    let rejectInterruption!: (error: SynchronizationError) => void;
    const interruption = new Promise<never>((_resolve, reject) => {
      rejectInterruption = reject;
    });
    const interrupt = (error: SynchronizationError): void => {
      controller.abort();
      rejectInterruption(error);
    };
    const cancel = (): void =>
      interrupt(new SynchronizationError("INTERNAL_ERROR", "Recovery attempt was cancelled", true));
    attempt.cancelers.add(cancel);
    const timeout = this.scheduler.setTimeout(() => {
      timedOut = true;
      interrupt(new SynchronizationError("INTERNAL_ERROR", "Recovery request timed out", true));
    }, this.limits.acknowledgementTimeoutMs);
    try {
      let raw: unknown;
      try {
        raw = await Promise.race([
          this.loadRecovery(this.boardId, controller.signal),
          interruption,
        ]);
      } catch (error) {
        if (error instanceof SynchronizationError) throw error;
        if (timedOut)
          throw new SynchronizationError("INTERNAL_ERROR", "Recovery request timed out", true);
        if (attempt.cancelled) throw error;
        throw new SynchronizationError("INTERNAL_ERROR", "Recovery request failed", true);
      }
      this.assertCurrent(socket, attempt);
      const parsed = boardRecoveryMaterialSchema.safeParse(raw);
      if (!parsed.success)
        throw new SynchronizationError(
          "INVALID_COMMAND",
          "Invalid authoritative recovery material",
          false,
        );
      const committed = await Promise.race([
        verifyRecoveryMaterial(parsed.data, this.boardId),
        interruption,
      ]);
      this.assertCurrent(socket, attempt);
      if (
        !useBoardStore
          .getState()
          .rebaseRecoveredSession(
            this.sessionToken,
            this.boardId,
            parsed.data.snapshotState.boardName,
            committed,
          )
      )
        throw new SynchronizationError("INTERNAL_ERROR", "Recovery session was superseded", true);
    } finally {
      this.scheduler.clearTimeout(timeout);
      attempt.cancelers.delete(cancel);
    }
  }

  private async fetchRecovery(boardId: string, signal: AbortSignal): Promise<unknown> {
    const response = await this.fetcher(`${this.apiUrl}/v1/boards/${boardId}/recovery`, { signal });
    let raw: unknown;
    try {
      raw = await response.json();
    } catch {
      throw new SynchronizationError("INTERNAL_ERROR", "Recovery response was unreadable", true);
    }
    if (!response.ok) {
      const failure = protocolErrorSchema.safeParse(raw);
      if (failure.success)
        throw new SynchronizationError(
          failure.data.code,
          failure.data.message,
          failure.data.retryable,
        );
      throw new SynchronizationError("INTERNAL_ERROR", "Recovery request failed", true);
    }
    return raw;
  }

  private receiveLive(socket: BoardSocket, raw: unknown): void {
    if (!this.isSessionActive() || socket !== this.socket) return;
    const parsed = committedOperationSchema.safeParse(raw);
    if (!parsed.success) {
      this.failCurrentOrReady(
        socket,
        new SynchronizationError("INVALID_COMMAND", "Invalid live operation", false),
      );
      return;
    }
    const operation = parsed.data;
    if (operation.boardId !== this.boardId) {
      this.failCurrentOrReady(
        socket,
        new SynchronizationError("INVALID_COMMAND", "Live operation board mismatch", false),
      );
      return;
    }
    if (this.activeAttempt) {
      this.bufferLive(socket, operation);
      return;
    }
    if (!this.ready || this.retryTimer) return;
    const result = this.pendingQueue.observeCommitted(operation);
    if (result === "buffered" || result === "conflict")
      this.recoverFromIngestResult(socket, result, "live operation");
  }

  private recoverFromIngestResult(
    socket: BoardSocket,
    result: "buffered" | "conflict",
    source: "acknowledgement" | "live operation",
  ): void {
    if (result === "buffered") {
      if (!this.ready || this.retryTimer || socket !== this.socket) return;
      this.ready = false;
      this.pendingQueue.setReady(false);
      this.beginSynchronization(socket);
    } else {
      this.failCurrentOrReady(
        socket,
        new SynchronizationError("RESYNC_REQUIRED", `Conflicting ${source} operation`, true),
      );
    }
  }

  private receiveAccessRevoked(socket: BoardSocket, raw: unknown): void {
    if (!this.isSessionActive() || socket !== this.socket) return;
    const parsed = boardAccessRevokedEventSchema.safeParse(raw);
    if (!parsed.success) {
      this.stopTransport(socket, () =>
        useBoardStore
          .getState()
          .setSynchronizationError(
            this.sessionToken,
            "INVALID_COMMAND: Invalid board-access-revoked event",
          ),
      );
      return;
    }
    if (parsed.data.boardId !== this.boardId) return;
    this.stopTransport(socket, () =>
      useBoardStore
        .getState()
        .revokeSession(this.sessionToken, this.boardId, "ACCESS_REVOKED: Board access was revoked"),
    );
  }

  private receiveConnectionError(socket: BoardSocket, error: unknown): void {
    if (!this.isSessionActive() || socket !== this.socket) return;
    if (error === null || typeof error !== "object" || !Object.hasOwn(error, "data")) return;
    const failure = protocolErrorSchema.safeParse((error as { data: unknown }).data);
    if (
      !failure.success ||
      failure.data.retryable ||
      !terminalAuthorizationCodes.has(failure.data.code)
    )
      return;
    this.setTerminalFailure(
      socket,
      new SynchronizationError(failure.data.code, failure.data.message, failure.data.retryable),
    );
  }

  private stopTransport(socket: BoardSocket, updateState: () => void): void {
    if (socket !== this.socket || this.terminal) return;
    this.ready = false;
    this.pendingQueue.cancel();
    this.cancelRetry();
    this.invalidateAttempt();
    this.clearLiveBuffer();
    this.connectionGeneration += 1;
    this.terminal = true;
    updateState();
    socket.disconnect();
    this.socket = null;
  }

  private bufferLive(socket: BoardSocket, operation: CommittedOperation): void {
    const serialized = JSON.stringify(operation);
    const bytes = new TextEncoder().encode(serialized).byteLength;
    const existing = this.liveBuffer.get(operation.seq);
    if (existing) {
      if (existing.serialized !== serialized)
        this.failCurrentOrReady(
          socket,
          new SynchronizationError("RESYNC_REQUIRED", "Conflicting live operations", true),
        );
      return;
    }
    const existingSequence = this.liveBufferOpSequences.get(operation.opId);
    if (existingSequence !== undefined && existingSequence !== operation.seq) {
      this.failCurrentOrReady(
        socket,
        new SynchronizationError("RESYNC_REQUIRED", "Conflicting live operation identity", true),
      );
      return;
    }
    if (
      this.liveBuffer.size + 1 > this.limits.liveBufferMaxCount ||
      this.liveBufferBytes + bytes > this.limits.liveBufferMaxBytes
    ) {
      this.failCurrentOrReady(
        socket,
        new SynchronizationError(
          "BUFFER_LIMIT_EXCEEDED",
          "Live synchronization buffer limit exceeded",
          true,
        ),
      );
      return;
    }
    this.liveBuffer.set(operation.seq, { operation, serialized, bytes });
    this.liveBufferOpSequences.set(operation.opId, operation.seq);
    this.liveBufferBytes += bytes;
    this.updateBufferDiagnostics();
  }

  private drainLiveBuffer(): void {
    const operations = [...this.liveBuffer.values()]
      .map(({ operation }) => operation)
      .sort((left, right) => left.seq - right.seq);
    this.clearLiveBuffer();
    for (const operation of operations) {
      const result = this.pendingQueue.observeCommitted(operation);
      if (result === "conflict")
        throw new SynchronizationError("RESYNC_REQUIRED", "Conflicting operation delivery", true);
    }
  }

  private completeAttempt(socket: BoardSocket, attempt: SynchronizationAttempt): void {
    this.assertCurrent(socket, attempt);
    attempt.cancelled = true;
    attempt.cancelers.clear();
    this.activeAttempt = null;
    this.synchronizationFailures = 0;
    this.ready = true;
    this.updateDiagnostics({
      retryCode: null,
      retryScheduled: false,
      retryDelayMs: null,
    });
    useBoardStore.getState().setConnection(this.sessionToken, "ready");
    this.pendingQueue.setReady(true);
  }

  private failCurrentOrReady(socket: BoardSocket, failure: SynchronizationError): void {
    const attempt = this.activeAttempt;
    if (attempt) {
      this.failAttempt(socket, attempt, failure);
      return;
    }
    if (!this.ready || socket !== this.socket) return;
    this.ready = false;
    this.pendingQueue.setReady(false);
    if (failure.retryable) this.scheduleRetry(socket, failure);
    else this.setTerminalFailure(socket, failure);
  }

  private failAttempt(
    socket: BoardSocket,
    attempt: SynchronizationAttempt,
    failure: SynchronizationError,
  ): void {
    if (!this.isCurrentAttempt(socket, attempt)) return;
    this.invalidateAttempt();
    this.ready = false;
    this.pendingQueue.setReady(false);
    if (failure.retryable) this.scheduleRetry(socket, failure);
    else this.setTerminalFailure(socket, failure);
  }

  private scheduleRetry(socket: BoardSocket, failure: SynchronizationError): void {
    if (!this.isSessionActive() || !socket.connected || socket !== this.socket || this.retryTimer)
      return;
    this.clearLiveBuffer();
    this.synchronizationFailures += 1;
    const exponent = Math.min(this.synchronizationFailures - 1, 20);
    const baseDelay = Math.min(this.limits.retryCapMs, this.limits.retryBaseMs * 2 ** exponent);
    const delay = Math.min(
      this.limits.retryCapMs,
      Math.round(baseDelay * (1 + this.scheduler.random() * 0.2)),
    );
    useBoardStore.getState().setConnection(this.sessionToken, "retry-wait");
    this.updateDiagnostics({
      retryCode: failure.code,
      retryScheduled: true,
      retryDelayMs: delay,
    });
    const connectionGeneration = this.connectionGeneration;
    this.retryTimer = this.scheduler.setTimeout(() => {
      this.retryTimer = null;
      if (
        !this.isSessionActive() ||
        socket !== this.socket ||
        !socket.connected ||
        connectionGeneration !== this.connectionGeneration
      )
        return;
      this.updateDiagnostics({ retryScheduled: false, retryDelayMs: null });
      this.beginSynchronization(socket);
    }, delay);
  }

  private setTerminalFailure(socket: BoardSocket, failure: SynchronizationError): void {
    const authorizationFailed = terminalAuthorizationCodes.has(failure.code);
    this.stopTransport(socket, () => {
      useBoardStore
        .getState()
        .setSynchronizationError(
          this.sessionToken,
          `${failure.code}: ${failure.message}`,
          authorizationFailed,
        );
      this.updateDiagnostics({
        retryCode: failure.code,
        retryScheduled: false,
        retryDelayMs: null,
      });
    });
  }

  private invalidateAttempt(): void {
    const attempt = this.activeAttempt;
    if (!attempt) return;
    this.activeAttempt = null;
    attempt.cancelled = true;
    attempt.abortController.abort();
    for (const cancel of [...attempt.cancelers]) cancel();
    attempt.cancelers.clear();
    this.clearLiveBuffer();
  }

  private cancelRetry(): void {
    if (this.retryTimer !== null) this.scheduler.clearTimeout(this.retryTimer);
    this.retryTimer = null;
    this.updateDiagnostics({ retryScheduled: false, retryDelayMs: null });
  }

  private clearLiveBuffer(): void {
    this.liveBuffer.clear();
    this.liveBufferOpSequences.clear();
    this.liveBufferBytes = 0;
    this.updateBufferDiagnostics();
  }

  private updateBufferDiagnostics(): void {
    this.updateDiagnostics({
      bufferedCount: this.liveBuffer.size,
      bufferedBytes: this.liveBufferBytes,
    });
  }

  private updateDiagnostics(
    diagnostics: Parameters<
      ReturnType<typeof useBoardStore.getState>["setSynchronizationDiagnostics"]
    >[1],
  ): void {
    useBoardStore.getState().setSynchronizationDiagnostics(this.sessionToken, diagnostics);
  }

  private isCurrentAttempt(socket: BoardSocket, attempt: SynchronizationAttempt): boolean {
    return (
      !attempt.cancelled &&
      socket.connected &&
      socket === this.socket &&
      attempt === this.activeAttempt &&
      attempt.connectionGeneration === this.connectionGeneration &&
      this.isSessionActive()
    );
  }

  private assertCurrent(socket: BoardSocket, attempt: SynchronizationAttempt): void {
    if (!this.isCurrentAttempt(socket, attempt))
      throw new SynchronizationError("INTERNAL_ERROR", "Synchronization interrupted", true);
  }

  private isSessionActive(): boolean {
    return (
      !this.terminal && useBoardStore.getState().isCurrentSession(this.sessionToken, this.boardId)
    );
  }

  private createSubmissionAttempt(command: DurableCommand): SubmissionAttempt {
    if (!this.isSessionActive() || !this.socket?.connected || !this.ready)
      return retryableSubmission("Operation transport is not ready");
    return timedSubmission(this.scheduler, SYNC_ACK_TIMEOUT_MS, (acknowledge) =>
      this.socket?.emit("operation:submit", command, acknowledge),
    );
  }
}

export { API_URL };
