import { createHash, randomUUID } from "node:crypto";
import {
  decodeDeliveryStreamFieldPairs,
  redisStreamEntryIdSchema,
  type DeliveryEnvelope,
  type DeliveryStreamFieldPair,
} from "@converge/protocol";

export const REDIS_DELIVERY_READ_COUNT = 100 as const;
export const REDIS_DELIVERY_BLOCK_MS = 5_000 as const;
export const REDIS_DELIVERY_MAX_BOARD_STATES_UPPER_BOUND = 100_000 as const;

const ZERO_STREAM_ID = "0-0";
const STREAM_ID_PATTERN = /^(0|[1-9]\d*)-(0|[1-9]\d*)$/;
const REDIS_UINT64_MAXIMUM = 18_446_744_073_709_551_615n;
const textEncoder = new TextEncoder();

export interface RawDeliveryStreamEntry {
  id: string;
  fields: readonly DeliveryStreamFieldPair[];
}

export interface DeliveryStreamMetadata {
  exists: boolean;
  length: string;
  firstEntryId: string | null;
  lastEntryId: string | null;
  lastGeneratedId: string;
  maxDeletedEntryId: string;
  entriesAdded: string;
  incarnation: string;
}

export interface DeliveryStreamInitialization {
  created: boolean;
  sentinelId: string | null;
  generationToken: string | null;
}

export interface DeliveryConsumerTransport {
  connect(): Promise<void>;
  initializeStream(input: {
    generationToken: string;
    signal: AbortSignal;
  }): Promise<DeliveryStreamInitialization>;
  verifyInitialization(input: {
    sentinelId: string;
    generationToken: string;
    signal: AbortSignal;
  }): Promise<boolean>;
  inspect(input: { signal: AbortSignal }): Promise<DeliveryStreamMetadata>;
  readAfter(input: {
    cursor: string;
    count: typeof REDIS_DELIVERY_READ_COUNT;
    blockMs: typeof REDIS_DELIVERY_BLOCK_MS;
    signal: AbortSignal;
    onIssued: () => void;
  }): Promise<readonly RawDeliveryStreamEntry[]>;
  cancelRead(): Promise<void>;
  close(): Promise<void>;
}

export interface DeliveryConsumerConfiguration {
  maximumEnvelopeBytes: number;
  globalQueueMaximumEvents: number;
  globalQueueMaximumBytes: number;
  boardQuarantineMaximumEvents: number;
  boardQuarantineMaximumBytes: number;
  boardDedupeMaximumEvents: number;
  boardDedupeMaximumBytes: number;
  maximumBoardStates: number;
  reconnectDelayMs: number;
}

export const defaultDeliveryConsumerConfiguration: Readonly<DeliveryConsumerConfiguration> = {
  maximumEnvelopeBytes: 128 * 1024,
  globalQueueMaximumEvents: 1_000,
  globalQueueMaximumBytes: 16 * 1024 * 1024,
  boardQuarantineMaximumEvents: 100,
  boardQuarantineMaximumBytes: 2 * 1024 * 1024,
  boardDedupeMaximumEvents: 256,
  boardDedupeMaximumBytes: 2 * 1024 * 1024,
  maximumBoardStates: 1_000,
  reconnectDelayMs: 250,
};

export type DeliveryConsumerErrorCode =
  | "INVALID_CONFIGURATION"
  | "INVALID_STREAM_METADATA"
  | "STREAM_INITIALIZATION_FAILED"
  | "INVALID_REDIS_ENTRY_ID"
  | "NON_MONOTONIC_REDIS_ENTRY_ID"
  | "INVALID_STREAM_ENTRY"
  | "GLOBAL_QUEUE_OVERFLOW"
  | "BOARD_STATE_CAPACITY_EXCEEDED"
  | "DELIVERY_CALLBACK_FAILED"
  | "QUARANTINE_CALLBACK_FAILED";

export type CursorLossReason =
  | "SERVER_INCARNATION_CHANGED"
  | "STREAM_MISSING"
  | "STREAM_RECREATED"
  | "STREAM_BEHIND_CURSOR"
  | "TRIMMED_BEYOND_CURSOR"
  | "CONTINUITY_UNCERTAIN";

export type DeliveryConsumerLifecycleEvent =
  | { state: "starting" }
  | { state: "established"; cursor: string; initialTail: string }
  | {
      state: "unavailable";
      cursor: string;
      code: "REDIS_UNAVAILABLE" | "BOARD_STATE_CAPACITY_EXCEEDED";
    }
  | { state: "recovering"; cursor: string }
  | { state: "recovered"; cursor: string; recoveryTail: string }
  | { state: "cursor_lost"; cursor: string; reason: CursorLossReason }
  | { state: "error"; cursor: string; entryId?: string; code: DeliveryConsumerErrorCode }
  | { state: "stopping"; cursor: string }
  | { state: "stopped"; cursor: string };

export interface DeliveryContext {
  redisEntryId: string;
  envelope: DeliveryEnvelope;
}

export type BoardQuarantineReason =
  | "DELIVERY_SEQUENCE_GAP"
  | "CONFLICTING_DELIVERY_SEQUENCE"
  | "EVENT_ID_REUSED"
  | "BOARD_ALREADY_QUARANTINED"
  | "QUARANTINE_OVERFLOW";

export interface BoardQuarantineEvent {
  redisEntryId: string;
  boardId: string;
  eventId: string;
  deliverySeq: number;
  reason: BoardQuarantineReason;
  retainedEvents: number;
  retainedBytes: number;
  overflowed: boolean;
}

export interface DeliveryConsumerCallbacks {
  deliver(context: DeliveryContext): Promise<void>;
  quarantine(event: BoardQuarantineEvent): Promise<void>;
  lifecycle(event: DeliveryConsumerLifecycleEvent): Promise<void> | void;
}

export interface DeliveryConsumerHooks {
  beforeEntryValidation?: (entryId: string) => Promise<void>;
  beforeCursorAdvance?: (entryId: string) => Promise<void>;
  afterCursorAdvance?: (entryId: string) => Promise<void>;
}

export interface DeliveryConsumerScheduler {
  wait(delayMs: number, signal: AbortSignal): Promise<void>;
}

export const systemDeliveryConsumerScheduler: DeliveryConsumerScheduler = {
  wait: (delayMs, signal) =>
    new Promise((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      let settled = false;
      const settle = (): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        signal.removeEventListener("abort", settle);
        resolve();
      };
      const timeout = setTimeout(settle, delayMs);
      signal.addEventListener("abort", settle, { once: true });
      if (signal.aborted) settle();
    }),
};

interface DedupeRecord {
  eventId: string;
  deliverySeq: number;
  envelopeDigest: string;
  bytes: number;
}

interface QuarantinedRecord {
  eventId: string;
  deliverySeq: number;
  bytes: number;
}

interface BoardState {
  cursor: number;
  quarantined: boolean;
  dedupe: DedupeRecord[];
  dedupeBytes: number;
  quarantine: QuarantinedRecord[];
  quarantineBytes: number;
  callbackInFlight: boolean;
  pendingCommitEntryId: string | undefined;
  committed: boolean;
  ownerGeneration: number;
  touchOrdinal: bigint;
}

export interface BoardStateCapacityDiagnostics {
  currentCount: number;
  configuredLimit: number;
  evictionCount: number;
  capacityFailureCount: number;
}

interface ContinuityWitness {
  metadata: DeliveryStreamMetadata;
  observedStream: boolean;
  minimumEntriesAdded: bigint;
  emptyBoundary: EmptyStreamBoundary | undefined;
}

interface EmptyStreamBoundary {
  lastGeneratedId: string;
  entriesAdded: bigint;
  maxDeletedEntryId: string;
  incarnation: string;
}

class FatalConsumerError extends Error {
  constructor(
    readonly code: DeliveryConsumerErrorCode,
    readonly entryId?: string,
  ) {
    super(code);
  }
}

class CursorLossError extends Error {
  constructor(readonly reason: CursorLossReason) {
    super(reason);
  }
}

class StaleGenerationError extends Error {}

function parseStreamId(value: string, allowZero: boolean): readonly [bigint, bigint] {
  const match = STREAM_ID_PATTERN.exec(value);
  const milliseconds = match?.[1];
  const sequence = match?.[2];
  if (
    milliseconds === undefined ||
    sequence === undefined ||
    (!allowZero && value === ZERO_STREAM_ID) ||
    BigInt(milliseconds) > REDIS_UINT64_MAXIMUM ||
    BigInt(sequence) > REDIS_UINT64_MAXIMUM
  )
    throw new Error("Invalid Redis stream ID");
  return [BigInt(milliseconds), BigInt(sequence)];
}

export function compareRedisStreamIds(left: string, right: string): number {
  const [leftMilliseconds, leftSequence] = parseStreamId(left, true);
  const [rightMilliseconds, rightSequence] = parseStreamId(right, true);
  if (leftMilliseconds !== rightMilliseconds) return leftMilliseconds < rightMilliseconds ? -1 : 1;
  if (leftSequence === rightSequence) return 0;
  return leftSequence < rightSequence ? -1 : 1;
}

function parseCounter(value: string): bigint {
  if (!/^(0|[1-9]\d*)$/.test(value)) throw new Error("Invalid Redis stream counter");
  const counter = BigInt(value);
  if (counter > REDIS_UINT64_MAXIMUM) throw new Error("Invalid Redis stream counter");
  return counter;
}

function captureEmptyBoundary(metadata: DeliveryStreamMetadata): EmptyStreamBoundary | undefined {
  if (!metadata.exists || metadata.length !== "0") return undefined;
  return {
    lastGeneratedId: metadata.lastGeneratedId,
    entriesAdded: parseCounter(metadata.entriesAdded),
    maxDeletedEntryId: metadata.maxDeletedEntryId,
    incarnation: metadata.incarnation,
  };
}

function validateMetadata(metadata: DeliveryStreamMetadata): void {
  parseCounter(metadata.length);
  parseCounter(metadata.entriesAdded);
  parseStreamId(metadata.lastGeneratedId, true);
  parseStreamId(metadata.maxDeletedEntryId, true);
  if (metadata.incarnation.length === 0) throw new Error("Missing Redis incarnation");
  if (!metadata.exists) {
    if (
      metadata.length !== "0" ||
      metadata.firstEntryId !== null ||
      metadata.lastEntryId !== null ||
      metadata.lastGeneratedId !== ZERO_STREAM_ID ||
      metadata.maxDeletedEntryId !== ZERO_STREAM_ID ||
      metadata.entriesAdded !== "0"
    )
      throw new Error("Absent Redis stream metadata is inconsistent");
    return;
  }
  if (metadata.length === "0") {
    if (metadata.firstEntryId !== null || metadata.lastEntryId !== null)
      throw new Error("Empty Redis stream metadata is inconsistent");
    return;
  }
  if (metadata.firstEntryId === null || metadata.lastEntryId === null)
    throw new Error("Non-empty Redis stream metadata is incomplete");
  parseStreamId(metadata.firstEntryId, false);
  parseStreamId(metadata.lastEntryId, false);
  if (compareRedisStreamIds(metadata.firstEntryId, metadata.lastEntryId) > 0)
    throw new Error("Redis stream metadata is reversed");
  if (compareRedisStreamIds(metadata.lastEntryId, metadata.lastGeneratedId) > 0)
    throw new Error("Redis stream tail exceeds last-generated ID");
}

function validateConfiguration(configuration: DeliveryConsumerConfiguration): void {
  for (const value of Object.values(configuration))
    if (!Number.isSafeInteger(value) || value < 0)
      throw new FatalConsumerError("INVALID_CONFIGURATION");
  if (
    configuration.maximumEnvelopeBytes === 0 ||
    configuration.globalQueueMaximumEvents === 0 ||
    configuration.globalQueueMaximumBytes === 0 ||
    configuration.boardQuarantineMaximumEvents === 0 ||
    configuration.boardQuarantineMaximumBytes === 0 ||
    configuration.boardDedupeMaximumEvents === 0 ||
    configuration.boardDedupeMaximumBytes === 0 ||
    configuration.maximumBoardStates === 0 ||
    configuration.maximumBoardStates > REDIS_DELIVERY_MAX_BOARD_STATES_UPPER_BOUND
  )
    throw new FatalConsumerError("INVALID_CONFIGURATION");
}

function entryBytes(entry: RawDeliveryStreamEntry): number {
  let bytes = textEncoder.encode(entry.id).byteLength;
  for (const [name, value] of entry.fields)
    bytes += textEncoder.encode(name).byteLength + textEncoder.encode(value).byteLength;
  return bytes;
}

function envelopeDigest(rawEnvelope: string): string {
  return createHash("sha256").update(rawEnvelope).digest("hex");
}

function rawEnvelope(entry: RawDeliveryStreamEntry): string {
  const pair = entry.fields.find(([name]) => name === "event");
  return pair?.[1] ?? "";
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

export class RedisDeliveryConsumer {
  private readonly abortController = new AbortController();
  private readonly boards = new Map<string, BoardState>();
  private cursor = ZERO_STREAM_ID;
  private witness: ContinuityWitness | undefined;
  private loopPromise: Promise<void> | undefined;
  private startPromise: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private stopping = false;
  private generation = 0;
  private boardTouchOrdinal = 0n;
  private boardStateEvictionCount = 0;
  private boardStateCapacityFailureCount = 0;
  private availabilityState: "unknown" | "available" | "unavailable" = "unknown";

  constructor(
    private readonly transport: DeliveryConsumerTransport,
    private readonly callbacks: DeliveryConsumerCallbacks,
    private readonly configuration: DeliveryConsumerConfiguration = {
      ...defaultDeliveryConsumerConfiguration,
    },
    private readonly scheduler: DeliveryConsumerScheduler = systemDeliveryConsumerScheduler,
    private readonly hooks: DeliveryConsumerHooks = {},
  ) {}

  get lastHandledCursor(): string {
    return this.cursor;
  }

  get activeBoardStateCount(): number {
    return this.boards.size;
  }

  get boardStateCapacityDiagnostics(): Readonly<BoardStateCapacityDiagnostics> {
    return {
      currentCount: this.boards.size,
      configuredLimit: this.configuration.maximumBoardStates,
      evictionCount: this.boardStateEvictionCount,
      capacityFailureCount: this.boardStateCapacityFailureCount,
    };
  }

  getBoardDiagnostics(boardId: string):
    | {
        cursor: number;
        quarantined: boolean;
        dedupeEvents: number;
        dedupeBytes: number;
        quarantineEvents: number;
        quarantineBytes: number;
      }
    | undefined {
    const board = this.boards.get(boardId);
    if (!board) return undefined;
    return {
      cursor: board.cursor,
      quarantined: board.quarantined,
      dedupeEvents: board.dedupe.length,
      dedupeBytes: board.dedupeBytes,
      quarantineEvents: board.quarantine.length,
      quarantineBytes: board.quarantineBytes,
    };
  }

  start(): Promise<void> {
    this.startPromise ??= this.startOnce();
    return this.startPromise;
  }

  stop(): Promise<void> {
    this.stopPromise ??= this.stopOnce();
    return this.stopPromise;
  }

  private async startOnce(): Promise<void> {
    const generation = ++this.generation;
    try {
      validateConfiguration(this.configuration);
      await this.notifyLifecycle({ state: "starting" });
      this.assertGeneration(generation);
      await this.transport.connect();
      this.assertGeneration(generation);
      const startupGenerationToken = randomUUID();
      const initialization = await this.transport.initializeStream({
        generationToken: startupGenerationToken,
        signal: this.abortController.signal,
      });
      this.assertGeneration(generation);
      const metadata = await this.inspectStrict(generation);
      const sentinelId = initialization.sentinelId;
      const generationToken = initialization.generationToken;
      if (initialization.created && generationToken !== startupGenerationToken)
        throw new FatalConsumerError("STREAM_INITIALIZATION_FAILED");
      if ((sentinelId === null) !== (generationToken === null))
        throw new FatalConsumerError("STREAM_INITIALIZATION_FAILED");
      if (sentinelId !== null && generationToken !== null) {
        if (
          !redisStreamEntryIdSchema.safeParse(sentinelId).success ||
          generationToken.length === 0 ||
          !metadata.exists
        )
          throw new FatalConsumerError("STREAM_INITIALIZATION_FAILED");
        const verified = await this.transport.verifyInitialization({
          sentinelId,
          generationToken,
          signal: this.abortController.signal,
        });
        this.assertGeneration(generation);
        if (!verified) throw new FatalConsumerError("STREAM_INITIALIZATION_FAILED");
      } else if (initialization.created) {
        throw new FatalConsumerError("STREAM_INITIALIZATION_FAILED");
      } else if (!metadata.exists) {
        // The atomic initializer observed an existing stream. Its disappearance before strict
        // inspection is therefore a recreation boundary, not a fresh absent-stream startup.
        throw new FatalConsumerError("STREAM_INITIALIZATION_FAILED");
      }
      this.cursor = metadata.lastGeneratedId;
      this.witness = {
        metadata,
        observedStream: metadata.exists,
        minimumEntriesAdded: parseCounter(metadata.entriesAdded),
        emptyBoundary: captureEmptyBoundary(metadata),
      };

      const established = deferred<void>();
      this.loopPromise = this.run(metadata.lastGeneratedId, established, generation);
      void this.loopPromise.catch(() => undefined);
      await established.promise;
    } catch (error) {
      if (this.stopping || error instanceof StaleGenerationError) return;
      if (error instanceof FatalConsumerError)
        await this.notifyLifecycle({ state: "error", cursor: this.cursor, code: error.code });
      else
        await this.notifyLifecycle({
          state: "unavailable",
          cursor: this.cursor,
          code: "REDIS_UNAVAILABLE",
        });
      throw error;
    }
  }

  private async run(
    initialTail: string,
    established: ReturnType<typeof deferred<void>>,
    initialGeneration: number,
  ): Promise<void> {
    let generation = initialGeneration;
    let firstRead = true;
    while (!this.stopping) {
      try {
        const entries = await this.issueRead(generation, async () => {
          if (!firstRead) return;
          firstRead = false;
          await this.notifyLifecycle({
            state: "established",
            cursor: this.cursor,
            initialTail,
          });
          established.resolve();
        });
        if (this.stopping) return;
        const metadata = await this.inspectStrict(generation);
        this.validateReadContinuity(metadata, entries);
        await this.processBatch(entries, generation);
      } catch (error) {
        if (this.stopping || error instanceof StaleGenerationError) return;
        if (error instanceof FatalConsumerError) {
          await this.reportFatalError(error);
          established.reject(error);
          return;
        }
        if (error instanceof CursorLossError) {
          await this.notifyLifecycle({
            state: "cursor_lost",
            cursor: this.cursor,
            reason: error.reason,
          });
          established.reject(error);
          return;
        }
        await this.notifyLifecycle({
          state: "unavailable",
          cursor: this.cursor,
          code: "REDIS_UNAVAILABLE",
        });
        generation = ++this.generation;
        await this.transport.cancelRead().catch(() => undefined);
        try {
          await this.recover(generation);
        } catch (recoveryError) {
          if (this.stopping || recoveryError instanceof StaleGenerationError) return;
          if (recoveryError instanceof FatalConsumerError) {
            await this.reportFatalError(recoveryError);
            established.reject(recoveryError);
            return;
          }
          if (recoveryError instanceof CursorLossError) {
            await this.notifyLifecycle({
              state: "cursor_lost",
              cursor: this.cursor,
              reason: recoveryError.reason,
            });
            established.reject(recoveryError);
            return;
          }
          throw recoveryError;
        }
      }
    }
  }

  private async issueRead(
    generation: number,
    onIssued: () => Promise<void>,
  ): Promise<readonly RawDeliveryStreamEntry[]> {
    this.assertGeneration(generation);
    const issued = deferred<void>();
    const result = this.transport.readAfter({
      cursor: this.cursor,
      count: REDIS_DELIVERY_READ_COUNT,
      blockMs: REDIS_DELIVERY_BLOCK_MS,
      signal: this.abortController.signal,
      onIssued: () => {
        if (!this.isActiveGeneration(generation)) {
          issued.reject(new StaleGenerationError());
          return;
        }
        void onIssued().then(() => {
          try {
            this.assertGeneration(generation);
            issued.resolve();
          } catch (error) {
            issued.reject(error);
          }
        }, issued.reject);
      },
    });
    await Promise.race([
      issued.promise,
      result.then(
        () => Promise.reject(new Error("Redis XREAD completed before it was issued")),
        (error: unknown) =>
          Promise.reject(error instanceof Error ? error : new Error("Redis XREAD failed")),
      ),
    ]);
    const entries = await result;
    this.assertGeneration(generation);
    return entries;
  }

  private async recover(generation: number): Promise<void> {
    await this.notifyLifecycle({ state: "recovering", cursor: this.cursor });
    let availabilityAnnounced = false;
    while (this.isActiveGeneration(generation)) {
      try {
        await this.transport.connect();
        this.assertGeneration(generation);
        const metadata = await this.inspectStrict(generation);
        this.validateContinuity(metadata);
        this.adoptBoardStates(generation);
        const recoveryTail = metadata.lastGeneratedId;
        while (
          this.isActiveGeneration(generation) &&
          compareRedisStreamIds(this.cursor, recoveryTail) < 0
        ) {
          const entries = await this.issueRead(generation, () => Promise.resolve());
          const readMetadata = await this.inspectStrict(generation);
          this.validateReadContinuity(readMetadata, entries);
          if (entries.length === 0) throw new CursorLossError("CONTINUITY_UNCERTAIN");
          await this.processBatch(entries, generation, recoveryTail);
        }
        this.assertGeneration(generation);
        const pendingRead = this.issueRead(generation, () =>
          Promise.resolve().then(async () => {
            availabilityAnnounced = true;
            await this.notifyLifecycle({ state: "recovered", cursor: this.cursor, recoveryTail });
          }),
        );
        const entries = await pendingRead;
        const liveMetadata = await this.inspectStrict(generation);
        this.validateReadContinuity(liveMetadata, entries);
        await this.processBatch(entries, generation);
        return;
      } catch (error) {
        if (this.stopping || error instanceof StaleGenerationError) return;
        if (error instanceof FatalConsumerError || error instanceof CursorLossError) throw error;
        if (availabilityAnnounced) {
          availabilityAnnounced = false;
          await this.notifyLifecycle({
            state: "unavailable",
            cursor: this.cursor,
            code: "REDIS_UNAVAILABLE",
          });
          if (!this.isActiveGeneration(generation)) return;
          await this.notifyLifecycle({ state: "recovering", cursor: this.cursor });
          if (!this.isActiveGeneration(generation)) return;
        }
        await this.transport.cancelRead().catch(() => undefined);
        await this.scheduler.wait(this.configuration.reconnectDelayMs, this.abortController.signal);
      }
    }
  }

  private async inspectStrict(generation: number): Promise<DeliveryStreamMetadata> {
    const metadata = await this.transport.inspect({ signal: this.abortController.signal });
    this.assertGeneration(generation);
    try {
      validateMetadata(metadata);
      return metadata;
    } catch {
      throw new FatalConsumerError("INVALID_STREAM_METADATA");
    }
  }

  private async reportFatalError(error: FatalConsumerError): Promise<void> {
    if (error.code === "BOARD_STATE_CAPACITY_EXCEEDED")
      await this.notifyLifecycle({
        state: "unavailable",
        cursor: this.cursor,
        code: error.code,
      });
    await this.notifyLifecycle({
      state: "error",
      cursor: this.cursor,
      ...(error.entryId === undefined ? {} : { entryId: error.entryId }),
      code: error.code,
    });
  }

  private validateContinuity(metadata: DeliveryStreamMetadata): void {
    const witness = this.witness;
    if (!witness) throw new CursorLossError("CONTINUITY_UNCERTAIN");
    if (metadata.incarnation !== witness.metadata.incarnation)
      throw new CursorLossError("SERVER_INCARNATION_CHANGED");
    if (!metadata.exists) {
      if (witness.observedStream || this.cursor !== ZERO_STREAM_ID)
        throw new CursorLossError("STREAM_MISSING");
      return;
    }
    if (compareRedisStreamIds(metadata.lastGeneratedId, this.cursor) < 0)
      throw new CursorLossError("STREAM_BEHIND_CURSOR");
    if (compareRedisStreamIds(metadata.maxDeletedEntryId, this.cursor) > 0)
      throw new CursorLossError("TRIMMED_BEYOND_CURSOR");
    const entriesAdded = parseCounter(metadata.entriesAdded);
    if (entriesAdded < witness.minimumEntriesAdded) throw new CursorLossError("STREAM_RECREATED");
    if (metadata.length === "0") {
      const boundary = witness.emptyBoundary;
      if (
        compareRedisStreamIds(metadata.lastGeneratedId, this.cursor) > 0 ||
        (boundary === undefined && entriesAdded !== witness.minimumEntriesAdded) ||
        (boundary !== undefined &&
          (metadata.lastGeneratedId !== boundary.lastGeneratedId ||
            entriesAdded !== boundary.entriesAdded ||
            metadata.maxDeletedEntryId !== boundary.maxDeletedEntryId ||
            metadata.incarnation !== boundary.incarnation))
      )
        throw new CursorLossError("TRIMMED_BEYOND_CURSOR");
      this.witness = {
        ...witness,
        metadata,
        observedStream: true,
        emptyBoundary: captureEmptyBoundary(metadata),
      };
      return;
    }
    if (witness.emptyBoundary !== undefined) {
      if (this.provesCompleteAppendAfterEmptyBoundary(witness.emptyBoundary, metadata)) {
        this.witness = {
          ...witness,
          metadata,
          observedStream: true,
        };
        return;
      }
      if (
        entriesAdded < witness.emptyBoundary.entriesAdded ||
        metadata.firstEntryId === null ||
        compareRedisStreamIds(metadata.firstEntryId, witness.emptyBoundary.lastGeneratedId) <= 0
      )
        throw new CursorLossError("STREAM_RECREATED");
      throw new CursorLossError("TRIMMED_BEYOND_CURSOR");
    }
    if (
      witness.observedStream &&
      metadata.firstEntryId !== null &&
      compareRedisStreamIds(metadata.firstEntryId, this.cursor) > 0 &&
      metadata.maxDeletedEntryId === ZERO_STREAM_ID
    ) {
      if (parseCounter(metadata.entriesAdded) > witness.minimumEntriesAdded)
        throw new CursorLossError("TRIMMED_BEYOND_CURSOR");
      throw new CursorLossError("STREAM_RECREATED");
    }
    this.witness = { ...witness, metadata, observedStream: true };
  }

  private provesCompleteAppendAfterEmptyBoundary(
    boundary: EmptyStreamBoundary,
    metadata: DeliveryStreamMetadata,
  ): boolean {
    if (
      metadata.incarnation !== boundary.incarnation ||
      metadata.firstEntryId === null ||
      metadata.lastEntryId === null ||
      compareRedisStreamIds(metadata.firstEntryId, boundary.lastGeneratedId) <= 0 ||
      compareRedisStreamIds(metadata.maxDeletedEntryId, boundary.lastGeneratedId) > 0 ||
      metadata.lastGeneratedId !== metadata.lastEntryId
    )
      return false;
    const entriesAdded = parseCounter(metadata.entriesAdded);
    if (entriesAdded <= boundary.entriesAdded) return false;
    return parseCounter(metadata.length) === entriesAdded - boundary.entriesAdded;
  }

  private validateReadContinuity(
    metadata: DeliveryStreamMetadata,
    entries: readonly RawDeliveryStreamEntry[],
  ): void {
    this.validateContinuity(metadata);
    const lastReturnedId = entries.at(-1)?.id;
    if (lastReturnedId === undefined) return;
    if (!redisStreamEntryIdSchema.safeParse(lastReturnedId).success) return;
    if (!metadata.exists) throw new CursorLossError("STREAM_MISSING");
    if (compareRedisStreamIds(metadata.lastGeneratedId, lastReturnedId) < 0)
      throw new CursorLossError("CONTINUITY_UNCERTAIN");
    if (
      metadata.lastEntryId === null ||
      compareRedisStreamIds(metadata.lastEntryId, lastReturnedId) < 0
    )
      throw new CursorLossError("TRIMMED_BEYOND_CURSOR");
  }

  private async processBatch(
    entries: readonly RawDeliveryStreamEntry[],
    generation: number,
    stopAt?: string,
  ): Promise<void> {
    if (entries.length > this.configuration.globalQueueMaximumEvents)
      throw new FatalConsumerError("GLOBAL_QUEUE_OVERFLOW", entries[0]?.id);
    let batchBytes = 0;
    for (const entry of entries) {
      batchBytes += entryBytes(entry);
      if (batchBytes > this.configuration.globalQueueMaximumBytes)
        throw new FatalConsumerError("GLOBAL_QUEUE_OVERFLOW", entry.id);
    }

    let previousId = this.cursor;
    for (const entry of entries) {
      if (
        !this.isActiveGeneration(generation) ||
        (stopAt !== undefined && compareRedisStreamIds(previousId, stopAt) >= 0)
      )
        return;
      await this.hooks.beforeEntryValidation?.(entry.id);
      this.assertGeneration(generation);
      if (!redisStreamEntryIdSchema.safeParse(entry.id).success)
        throw new FatalConsumerError("INVALID_REDIS_ENTRY_ID", entry.id);
      if (compareRedisStreamIds(entry.id, previousId) <= 0)
        throw new FatalConsumerError("NON_MONOTONIC_REDIS_ENTRY_ID", entry.id);
      previousId = entry.id;

      let envelope: DeliveryEnvelope;
      try {
        envelope = decodeDeliveryStreamFieldPairs(
          entry.fields,
          this.configuration.maximumEnvelopeBytes,
        );
      } catch {
        throw new FatalConsumerError("INVALID_STREAM_ENTRY", entry.id);
      }
      const board = await this.handleBoardEntry(entry, envelope, generation);
      this.assertGeneration(generation);
      await this.hooks.beforeCursorAdvance?.(entry.id);
      this.assertGeneration(generation);
      this.cursor = entry.id;
      if (this.witness) {
        this.witness.minimumEntriesAdded += 1n;
        if (
          this.witness.emptyBoundary !== undefined &&
          compareRedisStreamIds(this.cursor, this.witness.emptyBoundary.lastGeneratedId) > 0
        )
          this.witness.emptyBoundary = undefined;
      }
      this.commitBoardState(envelope.boardId, board, entry.id, generation);
      await this.hooks.afterCursorAdvance?.(entry.id);
      if (stopAt !== undefined && compareRedisStreamIds(this.cursor, stopAt) >= 0) return;
    }
  }

  private async handleBoardEntry(
    entry: RawDeliveryStreamEntry,
    envelope: DeliveryEnvelope,
    generation: number,
  ): Promise<BoardState> {
    let board = this.boards.get(envelope.boardId);
    if (!board) {
      this.ensureBoardStateCapacity(generation, entry.id);
      board = {
        cursor: envelope.deliverySeq,
        quarantined: false,
        dedupe: [],
        dedupeBytes: 0,
        quarantine: [],
        quarantineBytes: 0,
        callbackInFlight: false,
        pendingCommitEntryId: undefined,
        committed: false,
        ownerGeneration: generation,
        touchOrdinal: 0n,
      };
      this.boards.set(envelope.boardId, board);
      try {
        await this.runBoardCallback(board, () =>
          this.callbacks.deliver({ redisEntryId: entry.id, envelope }),
        );
      } catch {
        throw new FatalConsumerError("DELIVERY_CALLBACK_FAILED", entry.id);
      }
      this.remember(board, entry, envelope);
      this.markBoardPending(envelope.boardId, board, entry.id, generation);
      return board;
    }

    if (board.quarantined) {
      await this.quarantine(board, entry, envelope, "BOARD_ALREADY_QUARANTINED");
      this.markBoardPending(envelope.boardId, board, entry.id, generation);
      return board;
    }

    const raw = rawEnvelope(entry);
    const digest = envelopeDigest(raw);
    const bySequence = board.dedupe.find((record) => record.deliverySeq === envelope.deliverySeq);
    const byEvent = board.dedupe.find((record) => record.eventId === envelope.eventId);
    if (byEvent && byEvent.deliverySeq !== envelope.deliverySeq) {
      await this.quarantine(board, entry, envelope, "EVENT_ID_REUSED");
      this.markBoardPending(envelope.boardId, board, entry.id, generation);
      return board;
    }
    if (envelope.deliverySeq <= board.cursor) {
      if (
        bySequence &&
        (bySequence.eventId !== envelope.eventId || bySequence.envelopeDigest !== digest)
      ) {
        await this.quarantine(board, entry, envelope, "CONFLICTING_DELIVERY_SEQUENCE");
        this.markBoardPending(envelope.boardId, board, entry.id, generation);
        return board;
      }
      if (byEvent && byEvent.envelopeDigest !== digest) {
        await this.quarantine(board, entry, envelope, "CONFLICTING_DELIVERY_SEQUENCE");
      }
      this.markBoardPending(envelope.boardId, board, entry.id, generation);
      return board;
    }
    if (envelope.deliverySeq !== board.cursor + 1) {
      await this.quarantine(board, entry, envelope, "DELIVERY_SEQUENCE_GAP");
      this.markBoardPending(envelope.boardId, board, entry.id, generation);
      return board;
    }

    try {
      await this.runBoardCallback(board, () =>
        this.callbacks.deliver({ redisEntryId: entry.id, envelope }),
      );
    } catch {
      throw new FatalConsumerError("DELIVERY_CALLBACK_FAILED", entry.id);
    }
    board.cursor = envelope.deliverySeq;
    this.remember(board, entry, envelope);
    this.markBoardPending(envelope.boardId, board, entry.id, generation);
    return board;
  }

  private ensureBoardStateCapacity(generation: number, entryId: string): void {
    this.assertGeneration(generation);
    if (this.boards.size < this.configuration.maximumBoardStates) return;

    let candidate: { boardId: string; board: BoardState } | undefined;
    for (const [boardId, board] of this.boards) {
      if (!this.isBoardStateEvictable(board, generation)) continue;
      if (
        candidate === undefined ||
        board.touchOrdinal < candidate.board.touchOrdinal ||
        (board.touchOrdinal === candidate.board.touchOrdinal && boardId < candidate.boardId)
      )
        candidate = { boardId, board };
    }

    if (!candidate) {
      this.boardStateCapacityFailureCount += 1;
      throw new FatalConsumerError("BOARD_STATE_CAPACITY_EXCEEDED", entryId);
    }

    // The global stream cursor has validated every intervening entry before this point. Evicting a
    // fully committed healthy state therefore drops only process-local duplicate history, not an
    // unresolved operation or ordering evidence. A later event may establish a new baseline, but
    // delivery remains at least once and downstream stable-event-ID deduplication is still required.
    candidate.board.dedupe.length = 0;
    candidate.board.dedupeBytes = 0;
    candidate.board.quarantine.length = 0;
    candidate.board.quarantineBytes = 0;
    candidate.board.pendingCommitEntryId = undefined;
    this.boards.delete(candidate.boardId);
    this.boardStateEvictionCount += 1;
  }

  private isBoardStateEvictable(board: BoardState, generation: number): boolean {
    return (
      board.ownerGeneration === generation &&
      board.committed &&
      !board.quarantined &&
      !board.callbackInFlight &&
      board.pendingCommitEntryId === undefined &&
      board.quarantine.length === 0 &&
      board.quarantineBytes === 0
    );
  }

  private adoptBoardStates(generation: number): void {
    this.assertGeneration(generation);
    for (const board of this.boards.values()) board.ownerGeneration = generation;
  }

  private async runBoardCallback(board: BoardState, callback: () => Promise<void>): Promise<void> {
    board.callbackInFlight = true;
    try {
      await callback();
    } finally {
      board.callbackInFlight = false;
    }
  }

  private markBoardPending(
    boardId: string,
    board: BoardState,
    entryId: string,
    generation: number,
  ): void {
    this.assertGeneration(generation);
    if (this.boards.get(boardId) !== board) throw new StaleGenerationError();
    board.ownerGeneration = generation;
    board.pendingCommitEntryId = entryId;
  }

  private commitBoardState(
    boardId: string,
    board: BoardState,
    entryId: string,
    generation: number,
  ): void {
    this.assertGeneration(generation);
    if (this.boards.get(boardId) !== board || board.pendingCommitEntryId !== entryId)
      throw new StaleGenerationError();
    board.pendingCommitEntryId = undefined;
    board.committed = true;
    board.ownerGeneration = generation;
    board.touchOrdinal = ++this.boardTouchOrdinal;
  }

  private remember(
    board: BoardState,
    entry: RawDeliveryStreamEntry,
    envelope: DeliveryEnvelope,
  ): void {
    const raw = rawEnvelope(entry);
    const record: DedupeRecord = {
      eventId: envelope.eventId,
      deliverySeq: envelope.deliverySeq,
      envelopeDigest: envelopeDigest(raw),
      bytes: textEncoder.encode(envelope.eventId).byteLength + 8 + 64,
    };
    board.dedupe.push(record);
    board.dedupeBytes += record.bytes;
    while (
      board.dedupe.length > this.configuration.boardDedupeMaximumEvents ||
      board.dedupeBytes > this.configuration.boardDedupeMaximumBytes
    ) {
      const removed = board.dedupe.shift();
      if (!removed) break;
      board.dedupeBytes -= removed.bytes;
    }
  }

  private async quarantine(
    board: BoardState,
    entry: RawDeliveryStreamEntry,
    envelope: DeliveryEnvelope,
    reason: BoardQuarantineReason,
  ): Promise<void> {
    board.quarantined = true;
    const record = {
      eventId: envelope.eventId,
      deliverySeq: envelope.deliverySeq,
      bytes: entryBytes(entry),
    };
    const overflowed =
      board.quarantine.length + 1 > this.configuration.boardQuarantineMaximumEvents ||
      board.quarantineBytes + record.bytes > this.configuration.boardQuarantineMaximumBytes;
    if (overflowed) {
      board.quarantine = [];
      board.quarantineBytes = 0;
      reason = "QUARANTINE_OVERFLOW";
    } else {
      board.quarantine.push(record);
      board.quarantineBytes += record.bytes;
    }
    try {
      await this.runBoardCallback(board, () =>
        this.callbacks.quarantine({
          redisEntryId: entry.id,
          boardId: envelope.boardId,
          eventId: envelope.eventId,
          deliverySeq: envelope.deliverySeq,
          reason,
          retainedEvents: board.quarantine.length,
          retainedBytes: board.quarantineBytes,
          overflowed,
        }),
      );
    } catch {
      throw new FatalConsumerError("QUARANTINE_CALLBACK_FAILED", entry.id);
    }
  }

  private async stopOnce(): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    await this.notifyLifecycle({ state: "stopping", cursor: this.cursor });
    this.abortController.abort();
    await this.transport.cancelRead().catch(() => undefined);
    await this.transport.close();
    await this.loopPromise?.catch(() => undefined);
    for (const board of this.boards.values()) {
      board.dedupe.length = 0;
      board.dedupeBytes = 0;
      board.quarantine.length = 0;
      board.quarantineBytes = 0;
      board.pendingCommitEntryId = undefined;
    }
    this.boards.clear();
    this.boardTouchOrdinal = 0n;
    await this.notifyLifecycle({ state: "stopped", cursor: this.cursor });
  }

  private isActiveGeneration(generation: number): boolean {
    return !this.stopping && generation === this.generation;
  }

  private assertGeneration(generation: number): void {
    if (!this.isActiveGeneration(generation)) throw new StaleGenerationError();
  }

  private async notifyLifecycle(event: DeliveryConsumerLifecycleEvent): Promise<void> {
    if (event.state === "unavailable") {
      if (this.availabilityState === "unavailable") return;
      this.availabilityState = "unavailable";
    } else if (event.state === "established" || event.state === "recovered") {
      this.availabilityState = "available";
    }
    try {
      await this.callbacks.lifecycle(event);
    } catch {
      // Diagnostics callbacks cannot weaken cursor, recovery, or shutdown invariants.
    }
  }
}
