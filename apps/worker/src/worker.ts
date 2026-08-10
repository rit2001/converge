import type {
  ClaimedOutboxEvent,
  ClaimOutboxOptions,
  LeaseMutationResult,
  MarkOutboxPublishedInput,
  RecordOutboxFailureInput,
} from "@converge/database";
import type { StructuredLogger } from "@converge/observability";
import {
  deliveryEnvelopeSchema,
  encodeDeliveryStreamFields,
  redisStreamEntryIdSchema,
  validateDeliveryStreamEntrySize,
} from "@converge/protocol";
import {
  RedisXaddAmbiguousError,
  RedisXaddRejectedError,
  type DeliveryStream,
} from "./redis-stream.js";

export interface OutboxPublicationRepository {
  claimAvailable(options: ClaimOutboxOptions): Promise<ClaimedOutboxEvent[]>;
  markPublished(input: MarkOutboxPublishedInput): Promise<LeaseMutationResult>;
  recordFailure(input: RecordOutboxFailureInput): Promise<LeaseMutationResult>;
}

export interface WorkerScheduler {
  sleep(delayMs: number, signal?: AbortSignal): Promise<void>;
  random(): number;
}

export const systemWorkerScheduler: WorkerScheduler = {
  sleep: (delayMs, signal) =>
    new Promise((resolve) => {
      if (signal?.aborted) {
        resolve();
        return;
      }
      let settled = false;
      const settle = (): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        signal?.removeEventListener("abort", settle);
        resolve();
      };
      const timeout = setTimeout(settle, delayMs);
      signal?.addEventListener("abort", settle, { once: true });
      if (signal?.aborted) settle();
    }),
  random: Math.random,
};

export interface OutboxWorkerConfiguration {
  owner: string;
  claimBatchSize: number;
  publishConcurrency: number;
  leaseDurationMs: number;
  idlePollMs: number;
  pollJitterRatio: number;
  publicationTimeoutMs: number;
  maximumEnvelopeBytes: number;
}

export interface OutboxWorkerHooks {
  afterClaimsCommitted?: (claims: readonly ClaimedOutboxEvent[]) => Promise<void>;
  beforeXadd?: (claim: ClaimedOutboxEvent) => Promise<void>;
  afterXadd?: (claim: ClaimedOutboxEvent, redisEntryId: string) => Promise<void>;
  beforeMarkPublished?: (claim: ClaimedOutboxEvent, redisEntryId: string) => Promise<void>;
}

export type PublicationOutcome =
  | "published"
  | "stale"
  | "retry_scheduled"
  | "blocked"
  | "unexpected_failure";

export interface WorkerCycleResult {
  state: "stopping" | "redis_unavailable" | "empty" | "processed";
  claimed: number;
  outcomes: PublicationOutcome[];
}

export class SimulatedWorkerCrash extends Error {}

class PublicationTimeoutError extends Error {}

class DeliveryPreparationError extends Error {
  constructor(
    readonly code:
      | "INVALID_DELIVERY_ENVELOPE"
      | "DELIVERY_ENVELOPE_NOT_SERIALIZABLE"
      | "DELIVERY_ENVELOPE_TOO_LARGE",
    message: string,
  ) {
    super(message);
  }
}

async function withTimeout<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number,
  scheduler: WorkerScheduler,
  recoverAfterTimeout: () => Promise<void>,
): Promise<T> {
  const operationController = new AbortController();
  const timeoutController = new AbortController();
  const operationResult = Promise.resolve()
    .then(() => operation(operationController.signal))
    .then(
      (value) => ({ state: "fulfilled" as const, value }),
      (error: unknown) => ({ state: "rejected" as const, error }),
    );
  const timeoutResult = scheduler
    .sleep(timeoutMs, timeoutController.signal)
    .then(() => ({ state: "timed_out" as const }));
  try {
    const result = await Promise.race([operationResult, timeoutResult]);
    if (result.state === "timed_out") {
      operationController.abort();
      await Promise.allSettled([recoverAfterTimeout(), operationResult]);
      throw new PublicationTimeoutError("Redis operation timed out");
    }
    if (result.state === "rejected") throw result.error;
    return result.value;
  } finally {
    timeoutController.abort();
  }
}

function prepareClaim(claim: ClaimedOutboxEvent, maximumEnvelopeBytes: number) {
  const envelope = deliveryEnvelopeSchema.safeParse(claim.envelope);
  if (
    !envelope.success ||
    envelope.data.eventId !== claim.eventId ||
    envelope.data.boardId !== claim.boardId ||
    envelope.data.deliverySeq !== claim.deliverySeq ||
    envelope.data.eventType !== claim.eventType ||
    envelope.data.schemaVersion !== claim.schemaVersion
  )
    throw new DeliveryPreparationError(
      "INVALID_DELIVERY_ENVELOPE",
      "Delivery envelope failed strict validation.",
    );
  let fields;
  try {
    fields = encodeDeliveryStreamFields(envelope.data);
  } catch {
    throw new DeliveryPreparationError(
      "DELIVERY_ENVELOPE_NOT_SERIALIZABLE",
      "Delivery envelope could not be serialized.",
    );
  }
  const entrySize = validateDeliveryStreamEntrySize(fields, maximumEnvelopeBytes);
  if (!entrySize.valid)
    throw new DeliveryPreparationError(
      "DELIVERY_ENVELOPE_TOO_LARGE",
      "Delivery stream entry exceeds the configured byte limit.",
    );
  return fields;
}

function eventLogFields(claim: ClaimedOutboxEvent): Record<string, unknown> {
  return {
    eventId: claim.eventId,
    boardId: claim.boardId,
    deliverySeq: claim.deliverySeq,
    canvasSeq: claim.canvasSeq,
    eventType: claim.eventType,
    attemptCount: claim.attemptCount,
  };
}

export class OutboxWorker {
  private readonly active = new Set<Promise<PublicationOutcome>>();
  private readonly cycles = new Set<Promise<WorkerCycleResult>>();
  private retentionTask: Promise<void> | undefined;
  private retentionController: AbortController | undefined;
  private acceptingClaims = true;
  private abandoningActiveLeases = false;

  constructor(
    private readonly repository: OutboxPublicationRepository,
    private readonly stream: DeliveryStream,
    private readonly configuration: OutboxWorkerConfiguration,
    private readonly logger: StructuredLogger,
    private readonly scheduler: WorkerScheduler = systemWorkerScheduler,
    private readonly hooks: OutboxWorkerHooks = {},
  ) {}

  stopTakingClaims(): void {
    this.acceptingClaims = false;
  }

  abandonActiveLeases(): void {
    this.abandoningActiveLeases = true;
    this.retentionController?.abort();
  }

  async drain(gracePeriodMs: number): Promise<boolean> {
    if (this.cycles.size === 0 && this.retentionTask === undefined) return true;
    const graceController = new AbortController();
    try {
      return await Promise.race([
        this.waitForLifecycleWork().then(() => true),
        this.scheduler.sleep(gracePeriodMs, graceController.signal).then(() => false),
      ]);
    } finally {
      graceController.abort();
    }
  }

  runCycle(): Promise<WorkerCycleResult> {
    const cycle = this.executeCycle();
    this.cycles.add(cycle);
    void cycle.then(
      () => this.cycles.delete(cycle),
      () => this.cycles.delete(cycle),
    );
    return cycle;
  }

  private async executeCycle(): Promise<WorkerCycleResult> {
    if (!this.acceptingClaims) return { state: "stopping", claimed: 0, outcomes: [] };
    if (!this.stream.isReady()) return { state: "redis_unavailable", claimed: 0, outcomes: [] };
    const freeSlots = this.configuration.publishConcurrency - this.active.size;
    if (freeSlots <= 0) return { state: "processed", claimed: 0, outcomes: [] };
    const claims = await this.repository.claimAvailable({
      owner: this.configuration.owner,
      batchSize: Math.min(this.configuration.claimBatchSize, freeSlots),
      leaseDurationMs: this.configuration.leaseDurationMs,
    });
    if (this.abandoningActiveLeases)
      return { state: "stopping", claimed: claims.length, outcomes: [] };
    await this.hooks.afterClaimsCommitted?.(claims);
    if (this.abandoningActiveLeases)
      return { state: "stopping", claimed: claims.length, outcomes: [] };
    if (claims.length === 0) return { state: "empty", claimed: 0, outcomes: [] };

    const tasks = claims.map((claim) => {
      const task = this.publishSafely(claim);
      this.active.add(task);
      void task.then(
        () => this.active.delete(task),
        () => this.active.delete(task),
      );
      return task;
    });
    return {
      state: "processed",
      claimed: claims.length,
      outcomes: await Promise.all(tasks),
    };
  }

  async run(signal: AbortSignal): Promise<void> {
    while (this.acceptingClaims && !signal.aborted) {
      try {
        const result = await this.runCycle();
        if (result.state === "processed" && result.claimed > 0) continue;
      } catch (error) {
        if (error instanceof SimulatedWorkerCrash) throw error;
        this.logger.error(
          { component: "worker", code: "WORKER_CYCLE_FAILED" },
          "Worker cycle failed",
        );
      }
      if (!this.acceptingClaims || signal.aborted) break;
      const jitter = this.configuration.idlePollMs * this.configuration.pollJitterRatio;
      const delay = Math.floor(this.configuration.idlePollMs + jitter * this.scheduler.random());
      await this.scheduler.sleep(delay, signal);
    }
  }

  private async waitForLifecycleWork(): Promise<void> {
    while (this.cycles.size > 0 || this.retentionTask !== undefined) {
      const work: Promise<unknown>[] = [...this.cycles];
      if (this.retentionTask) work.push(this.retentionTask);
      await Promise.allSettled(work);
    }
  }

  private async publishSafely(claim: ClaimedOutboxEvent): Promise<PublicationOutcome> {
    try {
      return await this.publish(claim);
    } catch (error) {
      if (error instanceof SimulatedWorkerCrash) throw error;
      this.logger.error(
        { ...eventLogFields(claim), code: "EVENT_PUBLICATION_FAILED" },
        "Event publication failed unexpectedly",
      );
      return "unexpected_failure";
    }
  }

  private async publish(claim: ClaimedOutboxEvent): Promise<PublicationOutcome> {
    let fields;
    try {
      fields = prepareClaim(claim, this.configuration.maximumEnvelopeBytes);
    } catch (error) {
      if (!(error instanceof DeliveryPreparationError)) throw error;
      return this.recordFailure(claim, false, error.code, error.message);
    }

    if (this.abandoningActiveLeases) return "unexpected_failure";
    await this.hooks.beforeXadd?.(claim);
    if (this.abandoningActiveLeases) return "unexpected_failure";
    let redisResult: unknown;
    try {
      redisResult = await withTimeout(
        (signal) => this.stream.append(fields, signal),
        this.configuration.publicationTimeoutMs,
        this.scheduler,
        () => this.stream.resetAfterCommandTimeout(),
      );
    } catch (error) {
      if (this.abandoningActiveLeases) return "unexpected_failure";
      const ambiguous =
        error instanceof PublicationTimeoutError ||
        error instanceof RedisXaddAmbiguousError ||
        !(error instanceof RedisXaddRejectedError);
      return this.recordFailure(
        claim,
        true,
        ambiguous ? "REDIS_XADD_AMBIGUOUS" : "REDIS_XADD_FAILED",
        ambiguous
          ? "Redis publication outcome is ambiguous."
          : "Redis did not acknowledge publication.",
      );
    }

    const parsedEntryId = redisStreamEntryIdSchema.safeParse(redisResult);
    if (!parsedEntryId.success)
      return this.recordFailure(
        claim,
        true,
        "INVALID_REDIS_ENTRY_ID",
        "Redis returned an invalid publication identifier.",
      );
    const redisEntryId = parsedEntryId.data;
    await this.hooks.afterXadd?.(claim, redisEntryId);
    if (this.abandoningActiveLeases) return "unexpected_failure";

    await this.hooks.beforeMarkPublished?.(claim, redisEntryId);
    if (this.abandoningActiveLeases) return "unexpected_failure";
    let finalized: LeaseMutationResult;
    try {
      finalized = await this.repository.markPublished({
        eventId: claim.eventId,
        leaseToken: claim.leaseToken,
        publicationId: redisEntryId,
      });
    } catch {
      this.logger.error(
        { ...eventLogFields(claim), redisEntryId, code: "DATABASE_FINALIZATION_FAILED" },
        "Redis publication could not be finalized",
      );
      return "unexpected_failure";
    }
    this.scheduleRetentionMaintenance(claim, redisEntryId);
    if (finalized.outcome === "stale") {
      this.logger.warn(
        { ...eventLogFields(claim), redisEntryId, code: "STALE_LEASE_AFTER_XADD" },
        "Stale lease could not finalize Redis publication",
      );
      return "stale";
    }
    this.logger.info(
      { ...eventLogFields(claim), redisEntryId, outcome: "published" },
      "Outbox event published",
    );
    return "published";
  }

  private scheduleRetentionMaintenance(claim: ClaimedOutboxEvent, redisEntryId: string): void {
    if (this.retentionTask || this.abandoningActiveLeases) return;
    const controller = new AbortController();
    this.retentionController = controller;
    const task = this.runRetentionMaintenance(claim, redisEntryId, controller.signal).finally(
      () => {
        if (this.retentionTask === task) {
          this.retentionTask = undefined;
          this.retentionController = undefined;
        }
      },
    );
    this.retentionTask = task;
  }

  private async runRetentionMaintenance(
    claim: ClaimedOutboxEvent,
    redisEntryId: string,
    shutdownSignal: AbortSignal,
  ): Promise<void> {
    if (shutdownSignal.aborted) return;
    try {
      await withTimeout(
        (signal) => {
          const controller = new AbortController();
          const abort = (): void => controller.abort();
          signal.addEventListener("abort", abort, { once: true });
          shutdownSignal.addEventListener("abort", abort, { once: true });
          return this.stream.trimByAge(controller.signal).finally(() => {
            signal.removeEventListener("abort", abort);
            shutdownSignal.removeEventListener("abort", abort);
          });
        },
        this.configuration.publicationTimeoutMs,
        this.scheduler,
        () => this.stream.resetAfterCommandTimeout(),
      );
    } catch {
      this.logger.warn(
        { ...eventLogFields(claim), redisEntryId, code: "REDIS_AGE_TRIM_FAILED" },
        "Redis age retention maintenance failed",
      );
    }
  }

  private async recordFailure(
    claim: ClaimedOutboxEvent,
    retryable: boolean,
    errorCode: string,
    errorMessage: string,
  ): Promise<PublicationOutcome> {
    if (this.abandoningActiveLeases) return "unexpected_failure";
    let result: LeaseMutationResult;
    try {
      result = await this.repository.recordFailure({
        eventId: claim.eventId,
        leaseToken: claim.leaseToken,
        retryable,
        errorCode,
        errorMessage,
        ...(retryable ? { retryJitter: this.scheduler.random() } : {}),
      });
    } catch {
      this.logger.error(
        { ...eventLogFields(claim), code: "DATABASE_FAILURE_TRANSITION_FAILED" },
        "Publication failure could not be recorded",
      );
      return "unexpected_failure";
    }
    if (result.outcome === "stale") {
      this.logger.warn(
        { ...eventLogFields(claim), code: "STALE_LEASE_FAILURE_TRANSITION" },
        "Stale lease could not record publication failure",
      );
      return "stale";
    }
    const outcome = result.status === "blocked" ? "blocked" : "retry_scheduled";
    this.logger.warn(
      { ...eventLogFields(claim), code: errorCode, retryable, outcome },
      "Outbox publication attempt failed",
    );
    return outcome;
  }
}
