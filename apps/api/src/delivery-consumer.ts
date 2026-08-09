import { createHash } from "node:crypto";
import {
  decodeDeliveryStreamFieldPairs,
  redisStreamEntryIdSchema,
  type DeliveryEnvelope,
  type DeliveryStreamFieldPair,
} from "@converge/protocol";

export const REDIS_DELIVERY_READ_COUNT = 100 as const;
export const REDIS_DELIVERY_BLOCK_MS = 5_000 as const;

const ZERO_STREAM_ID = "0-0";
const STREAM_ID_PATTERN = /^(0|[1-9]\d*)-(0|[1-9]\d*)$/;
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

export interface DeliveryConsumerTransport {
  connect(): Promise<void>;
  inspect(): Promise<DeliveryStreamMetadata>;
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
  reconnectDelayMs: 250,
};

export type DeliveryConsumerErrorCode =
  | "INVALID_CONFIGURATION"
  | "INVALID_STREAM_METADATA"
  | "INVALID_REDIS_ENTRY_ID"
  | "NON_MONOTONIC_REDIS_ENTRY_ID"
  | "INVALID_STREAM_ENTRY"
  | "GLOBAL_QUEUE_OVERFLOW"
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
  | { state: "unavailable"; cursor: string; code: "REDIS_UNAVAILABLE" }
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
}

interface ContinuityWitness {
  metadata: DeliveryStreamMetadata;
  observedStream: boolean;
  minimumEntriesAdded: bigint;
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

function parseStreamId(value: string, allowZero: boolean): readonly [bigint, bigint] {
  const match = STREAM_ID_PATTERN.exec(value);
  const milliseconds = match?.[1];
  const sequence = match?.[2];
  const maximum = 18_446_744_073_709_551_615n;
  if (
    milliseconds === undefined ||
    sequence === undefined ||
    (!allowZero && value === ZERO_STREAM_ID) ||
    BigInt(milliseconds) > maximum ||
    BigInt(sequence) > maximum
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
  return BigInt(value);
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
    configuration.boardDedupeMaximumBytes === 0
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
    try {
      validateConfiguration(this.configuration);
      await this.notifyLifecycle({ state: "starting" });
      await this.transport.connect();
      const metadata = await this.inspectStrict();
      this.cursor = metadata.lastEntryId ?? ZERO_STREAM_ID;
      this.witness = {
        metadata,
        observedStream: metadata.exists,
        minimumEntriesAdded: parseCounter(metadata.entriesAdded),
      };

      const established = deferred<void>();
      this.loopPromise = this.run(metadata.lastEntryId ?? ZERO_STREAM_ID, established);
      void this.loopPromise.catch(() => undefined);
      await established.promise;
    } catch (error) {
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
  ): Promise<void> {
    let firstRead = true;
    while (!this.stopping) {
      try {
        const entries = await this.issueRead(async () => {
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
        await this.processBatch(entries);
      } catch (error) {
        if (this.stopping) return;
        if (error instanceof FatalConsumerError) {
          await this.notifyLifecycle({
            state: "error",
            cursor: this.cursor,
            ...(error.entryId === undefined ? {} : { entryId: error.entryId }),
            code: error.code,
          });
          established.reject(error);
          return;
        }
        await this.notifyLifecycle({
          state: "unavailable",
          cursor: this.cursor,
          code: "REDIS_UNAVAILABLE",
        });
        await this.transport.cancelRead().catch(() => undefined);
        try {
          await this.recover();
        } catch (recoveryError) {
          if (this.stopping) return;
          if (recoveryError instanceof FatalConsumerError) {
            await this.notifyLifecycle({
              state: "error",
              cursor: this.cursor,
              ...(recoveryError.entryId === undefined ? {} : { entryId: recoveryError.entryId }),
              code: recoveryError.code,
            });
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
          await this.scheduler.wait(
            this.configuration.reconnectDelayMs,
            this.abortController.signal,
          );
        }
      }
    }
  }

  private async issueRead(
    onIssued: () => Promise<void>,
  ): Promise<readonly RawDeliveryStreamEntry[]> {
    const issued = deferred<void>();
    const result = this.transport.readAfter({
      cursor: this.cursor,
      count: REDIS_DELIVERY_READ_COUNT,
      blockMs: REDIS_DELIVERY_BLOCK_MS,
      signal: this.abortController.signal,
      onIssued: () => {
        void onIssued().then(issued.resolve, issued.reject);
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
    return result;
  }

  private async recover(): Promise<void> {
    await this.notifyLifecycle({ state: "recovering", cursor: this.cursor });
    await this.transport.connect();
    const metadata = await this.inspectStrict();
    this.validateContinuity(metadata);
    const recoveryTail = metadata.lastEntryId ?? ZERO_STREAM_ID;
    while (!this.stopping && compareRedisStreamIds(this.cursor, recoveryTail) < 0) {
      const entries = await this.issueRead(() => Promise.resolve());
      if (entries.length === 0) throw new CursorLossError("CONTINUITY_UNCERTAIN");
      await this.processBatch(entries, recoveryTail);
    }
    if (this.stopping) return;
    const pendingRead = this.issueRead(() =>
      Promise.resolve(
        this.notifyLifecycle({ state: "recovered", cursor: this.cursor, recoveryTail }),
      ),
    );
    const entries = await pendingRead;
    if (!this.stopping) await this.processBatch(entries);
  }

  private async inspectStrict(): Promise<DeliveryStreamMetadata> {
    const metadata = await this.transport.inspect();
    try {
      validateMetadata(metadata);
      return metadata;
    } catch {
      throw new FatalConsumerError("INVALID_STREAM_METADATA");
    }
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
    if (
      compareRedisStreamIds(metadata.lastGeneratedId, this.cursor) < 0 ||
      (metadata.lastEntryId !== null &&
        compareRedisStreamIds(metadata.lastEntryId, this.cursor) < 0)
    )
      throw new CursorLossError("STREAM_BEHIND_CURSOR");
    if (compareRedisStreamIds(metadata.maxDeletedEntryId, this.cursor) > 0)
      throw new CursorLossError("TRIMMED_BEYOND_CURSOR");
    if (parseCounter(metadata.entriesAdded) < witness.minimumEntriesAdded)
      throw new CursorLossError("STREAM_RECREATED");
    if (
      witness.observedStream &&
      metadata.firstEntryId !== null &&
      compareRedisStreamIds(metadata.firstEntryId, this.cursor) > 0 &&
      metadata.maxDeletedEntryId === ZERO_STREAM_ID
    )
      throw new CursorLossError("STREAM_RECREATED");
    this.witness = { ...witness, metadata, observedStream: true };
  }

  private async processBatch(
    entries: readonly RawDeliveryStreamEntry[],
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
      if (this.stopping || (stopAt !== undefined && compareRedisStreamIds(previousId, stopAt) >= 0))
        return;
      await this.hooks.beforeEntryValidation?.(entry.id);
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
      await this.handleBoardEntry(entry, envelope);
      await this.hooks.beforeCursorAdvance?.(entry.id);
      if (this.stopping) return;
      this.cursor = entry.id;
      if (this.witness) this.witness.minimumEntriesAdded += 1n;
      await this.hooks.afterCursorAdvance?.(entry.id);
      if (stopAt !== undefined && compareRedisStreamIds(this.cursor, stopAt) >= 0) return;
    }
  }

  private async handleBoardEntry(
    entry: RawDeliveryStreamEntry,
    envelope: DeliveryEnvelope,
  ): Promise<void> {
    let board = this.boards.get(envelope.boardId);
    if (!board) {
      board = {
        cursor: envelope.deliverySeq,
        quarantined: false,
        dedupe: [],
        dedupeBytes: 0,
        quarantine: [],
        quarantineBytes: 0,
      };
      try {
        await this.callbacks.deliver({ redisEntryId: entry.id, envelope });
      } catch {
        throw new FatalConsumerError("DELIVERY_CALLBACK_FAILED", entry.id);
      }
      this.remember(board, entry, envelope);
      this.boards.set(envelope.boardId, board);
      return;
    }

    if (board.quarantined) {
      await this.quarantine(board, entry, envelope, "BOARD_ALREADY_QUARANTINED");
      return;
    }

    const raw = rawEnvelope(entry);
    const digest = envelopeDigest(raw);
    const bySequence = board.dedupe.find((record) => record.deliverySeq === envelope.deliverySeq);
    const byEvent = board.dedupe.find((record) => record.eventId === envelope.eventId);
    if (byEvent && byEvent.deliverySeq !== envelope.deliverySeq) {
      await this.quarantine(board, entry, envelope, "EVENT_ID_REUSED");
      return;
    }
    if (envelope.deliverySeq <= board.cursor) {
      if (
        bySequence &&
        (bySequence.eventId !== envelope.eventId || bySequence.envelopeDigest !== digest)
      ) {
        await this.quarantine(board, entry, envelope, "CONFLICTING_DELIVERY_SEQUENCE");
        return;
      }
      if (byEvent && byEvent.envelopeDigest !== digest) {
        await this.quarantine(board, entry, envelope, "CONFLICTING_DELIVERY_SEQUENCE");
      }
      return;
    }
    if (envelope.deliverySeq !== board.cursor + 1) {
      await this.quarantine(board, entry, envelope, "DELIVERY_SEQUENCE_GAP");
      return;
    }

    try {
      await this.callbacks.deliver({ redisEntryId: entry.id, envelope });
    } catch {
      throw new FatalConsumerError("DELIVERY_CALLBACK_FAILED", entry.id);
    }
    board.cursor = envelope.deliverySeq;
    this.remember(board, entry, envelope);
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
      await this.callbacks.quarantine({
        redisEntryId: entry.id,
        boardId: envelope.boardId,
        eventId: envelope.eventId,
        deliverySeq: envelope.deliverySeq,
        reason,
        retainedEvents: board.quarantine.length,
        retainedBytes: board.quarantineBytes,
        overflowed,
      });
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
    await this.loopPromise?.catch(() => undefined);
    await this.transport.close();
    await this.notifyLifecycle({ state: "stopped", cursor: this.cursor });
  }

  private async notifyLifecycle(event: DeliveryConsumerLifecycleEvent): Promise<void> {
    try {
      await this.callbacks.lifecycle(event);
    } catch {
      // Diagnostics callbacks cannot weaken cursor, recovery, or shutdown invariants.
    }
  }
}
