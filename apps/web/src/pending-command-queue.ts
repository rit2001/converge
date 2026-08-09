import {
  durableCommandSchema,
  operationAckSchema,
  type CommittedOperation,
  type DurableCommand,
  type OperationAck,
} from "@converge/protocol";
import type { IngestResult } from "./board-store";
import type { PendingOperationStore } from "./pending-db";

export const PENDING_RETRY_BASE_MS = 500;
export const PENDING_RETRY_CAP_MS = 10_000;
const RETRY_JITTER_RATIO = 0.25;

export type PendingRecoveryStatus =
  | "idle"
  | "saving-locally"
  | "pending-retry"
  | "persistence-error"
  | "cleanup-warning";

export interface RetryScheduler {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
  random(): number;
}

export type SubmissionOutcome =
  | { kind: "ack"; acknowledgement: OperationAck }
  | { kind: "retryable-transport"; message: string };

export interface SubmissionAttempt {
  result: Promise<SubmissionOutcome>;
  cancel(): void;
}

export function retryableSubmission(message: string): SubmissionAttempt {
  return {
    result: Promise.resolve({ kind: "retryable-transport", message }),
    cancel: () => undefined,
  };
}

export function timedSubmission(
  scheduler: RetryScheduler,
  timeoutMs: number,
  emit: (acknowledge: (raw: unknown) => void) => void,
): SubmissionAttempt {
  let settled = false;
  let timeout: unknown = null;
  let settle!: (outcome: SubmissionOutcome) => void;
  const result = new Promise<SubmissionOutcome>((resolve) => {
    settle = resolve;
  });
  const finish = (outcome: SubmissionOutcome): void => {
    if (settled) return;
    settled = true;
    if (timeout !== null) scheduler.clearTimeout(timeout);
    settle(outcome);
  };
  timeout = scheduler.setTimeout(
    () =>
      finish({
        kind: "retryable-transport",
        message: "Operation acknowledgement timed out",
      }),
    timeoutMs,
  );
  try {
    emit((raw) => {
      const acknowledgement = operationAckSchema.safeParse(raw);
      if (!acknowledgement.success) {
        finish({ kind: "retryable-transport", message: "Invalid operation acknowledgement" });
        return;
      }
      finish({ kind: "ack", acknowledgement: acknowledgement.data });
    });
  } catch {
    finish({ kind: "retryable-transport", message: "Operation submission failed" });
  }
  return {
    result,
    cancel: () =>
      finish({ kind: "retryable-transport", message: "Operation submission cancelled" }),
  };
}

export interface PendingCommandQueueDependencies {
  boardId: string;
  initialCommands: DurableCommand[];
  persistence: PendingOperationStore;
  scheduler?: RetryScheduler;
  isActive(): boolean;
  addPersisted(command: DurableCommand): boolean;
  removePending(operationId: string, error?: string): void;
  ingest(operation: CommittedOperation): IngestResult | "stale";
  setStatus(status: PendingRecoveryStatus, message?: string | null): void;
  submit(command: DurableCommand): SubmissionAttempt;
  requestSynchronization(reason?: "buffered" | "conflict"): void;
}

const browserScheduler: RetryScheduler = {
  setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
  random: () => Math.random(),
};

export class PendingCommandQueue {
  private readonly commands: DurableCommand[];
  private readonly scheduler: RetryScheduler;
  private readonly retryAttempts = new Map<string, number>();
  private readonly cleanupCommands = new Map<string, DurableCommand>();
  private readonly cleanupAttempts = new Map<string, number>();
  private inFlight: { command: DurableCommand; attempt: SubmissionAttempt } | null = null;
  private retryTimer: unknown = null;
  private cleanupTimer: unknown = null;
  private awaitingSynchronization: string | null = null;
  private ready = false;
  private cancelled = false;

  constructor(private readonly dependencies: PendingCommandQueueDependencies) {
    this.commands = [...dependencies.initialCommands];
    this.scheduler = dependencies.scheduler ?? browserScheduler;
  }

  async enqueue(raw: DurableCommand): Promise<boolean> {
    const parsed = durableCommandSchema.safeParse(raw);
    if (!parsed.success || parsed.data.boardId !== this.dependencies.boardId) {
      if (this.isActive())
        this.dependencies.setStatus(
          "persistence-error",
          "INVALID_COMMAND: Pending command validation failed",
        );
      return false;
    }
    const command = parsed.data;
    if (!this.isActive()) return false;
    this.dependencies.setStatus("saving-locally", null);
    try {
      await this.dependencies.persistence.put(command);
    } catch {
      if (this.isActive())
        this.dependencies.setStatus(
          "persistence-error",
          "LOCAL_PERSISTENCE_ERROR: Pending command was not saved",
        );
      return false;
    }
    if (!this.isActive()) return false;
    if (!this.commands.some(({ opId }) => opId === command.opId)) this.commands.push(command);
    if (!this.dependencies.addPersisted(command)) return false;
    this.dependencies.setStatus("idle", null);
    this.drain();
    return true;
  }

  setReady(ready: boolean): void {
    if (this.cancelled) return;
    this.ready = ready;
    if (!ready) {
      this.cancelRetryTimer();
      this.cancelCleanupTimer();
      return;
    }
    const command = this.commands[0];
    if (command && this.awaitingSynchronization === command.opId) {
      this.awaitingSynchronization = null;
      this.scheduleRetry(command, "Synchronization completed");
      this.drainCleanup();
      return;
    }
    this.drain();
    this.drainCleanup();
  }

  cancel(): void {
    if (this.cancelled) return;
    this.cancelled = true;
    this.ready = false;
    this.cancelRetryTimer();
    this.cancelCleanupTimer();
    this.inFlight?.attempt.cancel();
    this.inFlight = null;
    this.awaitingSynchronization = null;
  }

  observeCommitted(operation: CommittedOperation): IngestResult | "stale" {
    const result = this.dependencies.ingest(operation);
    if (result === "conflict" || result === "stale") return result;
    const command = this.commands.find(({ opId }) => opId === operation.opId);
    if (command) void this.finishCommand(command);
    return result;
  }

  private isActive(): boolean {
    return !this.cancelled && this.dependencies.isActive();
  }

  private drain(): void {
    if (!this.isActive() || !this.ready || this.inFlight || this.retryTimer !== null) return;
    const command = this.commands[0];
    if (!command) return;
    let attempt: SubmissionAttempt;
    try {
      attempt = this.dependencies.submit(command);
    } catch {
      attempt = retryableSubmission("Operation submission failed");
    }
    this.inFlight = { command, attempt };
    void attempt.result.then(
      (outcome) => this.handleOutcome(command, attempt, outcome),
      () =>
        this.handleOutcome(command, attempt, {
          kind: "retryable-transport",
          message: "Operation submission failed",
        }),
    );
  }

  private async handleOutcome(
    command: DurableCommand,
    attempt: SubmissionAttempt,
    outcome: SubmissionOutcome,
  ): Promise<void> {
    if (!this.isActive() || this.inFlight?.attempt !== attempt) return;
    this.inFlight = null;
    if (outcome.kind === "retryable-transport") {
      this.scheduleRetry(command, outcome.message);
      return;
    }

    const acknowledgement = outcome.acknowledgement;
    if (acknowledgement.ok) {
      const result = this.dependencies.ingest(acknowledgement.operation);
      if (result === "buffered" || result === "conflict") {
        this.setReady(false);
        this.dependencies.requestSynchronization(result);
      }
      await this.finishCommand(command);
      this.drain();
      return;
    }
    if (acknowledgement.retryable) {
      if (acknowledgement.code === "RESYNC_REQUIRED") {
        this.ready = false;
        this.awaitingSynchronization = command.opId;
        this.dependencies.setStatus(
          "pending-retry",
          "RESYNC_REQUIRED: Synchronizing before pending retry",
        );
        this.dependencies.requestSynchronization();
        return;
      }
      this.scheduleRetry(command, `${acknowledgement.code}: ${acknowledgement.message}`);
      return;
    }

    this.dependencies.removePending(
      command.opId,
      `${acknowledgement.code}: ${acknowledgement.message}`,
    );
    this.removeCommand(command.opId);
    await this.deleteOrScheduleCleanup(command);
    this.drain();
  }

  private scheduleRetry(command: DurableCommand, message: string): void {
    if (!this.isActive() || !this.ready) return;
    const attempt = (this.retryAttempts.get(command.opId) ?? 0) + 1;
    this.retryAttempts.set(command.opId, attempt);
    const baseDelay = Math.min(PENDING_RETRY_BASE_MS * 2 ** (attempt - 1), PENDING_RETRY_CAP_MS);
    const jitterWindow = Math.min(baseDelay * RETRY_JITTER_RATIO, PENDING_RETRY_CAP_MS - baseDelay);
    const jitter = Math.floor(jitterWindow * this.scheduler.random());
    this.dependencies.setStatus("pending-retry", `PENDING_RETRY: ${message}`);
    this.retryTimer = this.scheduler.setTimeout(() => {
      this.retryTimer = null;
      if (!this.isActive() || !this.ready) return;
      this.drain();
    }, baseDelay + jitter);
  }

  private async finishCommand(command: DurableCommand): Promise<void> {
    this.cancelRetryTimer();
    const current = this.inFlight;
    if (current?.command.opId === command.opId) {
      current.attempt.cancel();
      this.inFlight = null;
    }
    if (!this.commands.some(({ opId }) => opId === command.opId)) return;
    this.dependencies.removePending(command.opId);
    this.removeCommand(command.opId);
    await this.deleteOrScheduleCleanup(command);
    if (this.isActive() && !this.cleanupCommands.has(command.opId))
      this.dependencies.setStatus("idle", null);
    this.drain();
  }

  private removeCommand(operationId: string): void {
    const index = this.commands.findIndex(({ opId }) => opId === operationId);
    if (index >= 0) this.commands.splice(index, 1);
    this.retryAttempts.delete(operationId);
    if (this.awaitingSynchronization === operationId) this.awaitingSynchronization = null;
  }

  private async deleteOrScheduleCleanup(command: DurableCommand): Promise<void> {
    try {
      await this.dependencies.persistence.delete(command.boardId, command.opId);
      this.cleanupCommands.delete(command.opId);
      this.cleanupAttempts.delete(command.opId);
      if (this.isActive() && this.cleanupCommands.size === 0) this.dependencies.setStatus("idle");
    } catch {
      this.cleanupCommands.set(command.opId, command);
      if (this.isActive()) {
        this.dependencies.setStatus(
          "cleanup-warning",
          "LOCAL_PERSISTENCE_CLEANUP: Committed pending row will be retried",
        );
        this.scheduleCleanup();
      }
    }
  }

  private scheduleCleanup(): void {
    if (!this.isActive() || !this.ready || this.cleanupTimer !== null) return;
    const command = this.cleanupCommands.values().next().value;
    if (!command) return;
    const attempt = (this.cleanupAttempts.get(command.opId) ?? 0) + 1;
    this.cleanupAttempts.set(command.opId, attempt);
    const delay = Math.min(PENDING_RETRY_BASE_MS * 2 ** (attempt - 1), PENDING_RETRY_CAP_MS);
    this.cleanupTimer = this.scheduler.setTimeout(() => {
      this.cleanupTimer = null;
      this.drainCleanup();
    }, delay);
  }

  private drainCleanup(): void {
    if (!this.isActive() || !this.ready || this.cleanupTimer !== null) return;
    const command = this.cleanupCommands.values().next().value;
    if (!command) return;
    void this.deleteOrScheduleCleanup(command).then(() => this.drainCleanup());
  }

  private cancelRetryTimer(): void {
    if (this.retryTimer === null) return;
    this.scheduler.clearTimeout(this.retryTimer);
    this.retryTimer = null;
  }

  private cancelCleanupTimer(): void {
    if (this.cleanupTimer === null) return;
    this.scheduler.clearTimeout(this.cleanupTimer);
    this.cleanupTimer = null;
  }
}
