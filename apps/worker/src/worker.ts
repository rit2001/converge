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
      const timeout = setTimeout(resolve, delayMs);
      signal?.addEventListener(
        "abort",
        () => {
          clearTimeout(timeout);
          resolve();
        },
        { once: true },
      );
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

const textEncoder = new TextEncoder();

async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  scheduler: WorkerScheduler,
): Promise<T> {
  const timeoutController = new AbortController();
  try {
    return await Promise.race([
      operation,
      scheduler.sleep(timeoutMs, timeoutController.signal).then(() => {
        throw new PublicationTimeoutError("Redis operation timed out");
      }),
    ]);
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
  if (textEncoder.encode(fields.event).byteLength > maximumEnvelopeBytes)
    throw new DeliveryPreparationError(
      "DELIVERY_ENVELOPE_TOO_LARGE",
      "Delivery envelope exceeds the configured byte limit.",
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
  }

  async drain(gracePeriodMs: number): Promise<boolean> {
    if (this.active.size === 0) return true;
    const graceController = new AbortController();
    try {
      return await Promise.race([
        Promise.allSettled([...this.active]).then(() => true),
        this.scheduler.sleep(gracePeriodMs, graceController.signal).then(() => false),
      ]);
    } finally {
      graceController.abort();
    }
  }

  async runCycle(): Promise<WorkerCycleResult> {
    if (!this.acceptingClaims) return { state: "stopping", claimed: 0, outcomes: [] };
    if (!this.stream.isReady()) return { state: "redis_unavailable", claimed: 0, outcomes: [] };
    const freeSlots = this.configuration.publishConcurrency - this.active.size;
    if (freeSlots <= 0) return { state: "processed", claimed: 0, outcomes: [] };
    const claims = await this.repository.claimAvailable({
      owner: this.configuration.owner,
      batchSize: Math.min(this.configuration.claimBatchSize, freeSlots),
      leaseDurationMs: this.configuration.leaseDurationMs,
    });
    await this.hooks.afterClaimsCommitted?.(claims);
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
        this.stream.append(fields),
        this.configuration.publicationTimeoutMs,
        this.scheduler,
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

    try {
      await withTimeout(
        this.stream.trimByAge(),
        this.configuration.publicationTimeoutMs,
        this.scheduler,
      );
    } catch {
      this.logger.warn(
        { ...eventLogFields(claim), redisEntryId, code: "REDIS_AGE_TRIM_FAILED" },
        "Redis age retention maintenance failed",
      );
    }

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
