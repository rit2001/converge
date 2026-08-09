import { describe, expect, it } from "vitest";
import {
  encodeDeliveryStreamFields,
  membershipRevokedDeliveryEnvelopeSchema,
  type DeliveryEnvelope,
  type DeliveryStreamFieldPair,
} from "@converge/protocol";
import {
  REDIS_DELIVERY_BLOCK_MS,
  REDIS_DELIVERY_MAX_BOARD_STATES_UPPER_BOUND,
  REDIS_DELIVERY_READ_COUNT,
  RedisDeliveryConsumer,
  compareRedisStreamIds,
  defaultDeliveryConsumerConfiguration,
  type DeliveryConsumerCallbacks,
  type DeliveryConsumerConfiguration,
  type DeliveryConsumerHooks,
  type DeliveryConsumerLifecycleEvent,
  type DeliveryConsumerScheduler,
  type DeliveryConsumerTransport,
  type DeliveryStreamInitialization,
  type DeliveryStreamMetadata,
  type RawDeliveryStreamEntry,
} from "./delivery-consumer.js";

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

const ids = {
  boardA: "10000000-0000-4000-8000-000000000001",
  boardB: "10000000-0000-4000-8000-000000000002",
  revoked: "20000000-0000-4000-8000-000000000001",
  actor: "30000000-0000-4000-8000-000000000001",
};

function eventId(sequence: number, suffix = 1): string {
  return `40000000-0000-4000-8000-${String(sequence * 10 + suffix).padStart(12, "0")}`;
}

function boardId(sequence: number): string {
  return `10000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`;
}

function configurationWithBoardStateLimit(
  maximumBoardStates: number,
): DeliveryConsumerConfiguration {
  return {
    ...defaultDeliveryConsumerConfiguration,
    maximumBoardStates,
  } as DeliveryConsumerConfiguration;
}

function envelope(
  deliverySeq: number,
  boardId = ids.boardA,
  stableEventId = eventId(deliverySeq),
  actor = ids.actor,
): DeliveryEnvelope {
  return membershipRevokedDeliveryEnvelopeSchema.parse({
    schemaVersion: 1,
    eventId: stableEventId,
    boardId,
    deliverySeq,
    eventType: "board.membership.revoked",
    occurredAt: `2026-08-09T12:00:${String(deliverySeq % 60).padStart(2, "0")}.000Z`,
    payload: { revokedUserId: ids.revoked, initiatedByUserId: actor },
  });
}

function entry(id: string, value: DeliveryEnvelope): RawDeliveryStreamEntry {
  const fields = encodeDeliveryStreamFields(value);
  return { id, fields: Object.entries(fields) as DeliveryStreamFieldPair[] };
}

function metadata(input: Partial<DeliveryStreamMetadata> = {}): DeliveryStreamMetadata {
  return {
    exists: true,
    length: "1",
    firstEntryId: "10-0",
    lastEntryId: "10-0",
    lastGeneratedId: "10-0",
    maxDeletedEntryId: "0-0",
    entriesAdded: "1",
    incarnation: "run-1",
    ...input,
  };
}

function emptyMetadata(): DeliveryStreamMetadata {
  return {
    exists: false,
    length: "0",
    firstEntryId: null,
    lastEntryId: null,
    lastGeneratedId: "0-0",
    maxDeletedEntryId: "0-0",
    entriesAdded: "0",
    incarnation: "run-1",
  };
}

function initializedSentinelMetadata(): DeliveryStreamMetadata {
  return metadata({
    length: "1",
    firstEntryId: "0-1",
    lastEntryId: "0-1",
    lastGeneratedId: "0-1",
    maxDeletedEntryId: "0-0",
    entriesAdded: "1",
  });
}

function existingEmptyMetadata(
  input: Partial<DeliveryStreamMetadata> = {},
): DeliveryStreamMetadata {
  return metadata({
    length: "0",
    firstEntryId: null,
    lastEntryId: null,
    lastGeneratedId: "10-0",
    maxDeletedEntryId: "0-0",
    entriesAdded: "2",
    ...input,
  });
}

class FakeTransport implements DeliveryConsumerTransport {
  readonly readCalls: { cursor: string; count: number; blockMs: number }[] = [];
  readonly inspections: (DeliveryStreamMetadata | Error)[] = [];
  connectCalls = 0;
  inspectCalls = 0;
  cancelCalls = 0;
  closeCalls = 0;
  initializeCalls = 0;
  verifyCalls = 0;
  verificationResult = true;
  initializeBarrier: Promise<void> | undefined;
  activeReads = 0;
  maximumActiveReads = 0;
  onRead: ((cursor: string) => void) | undefined;
  private readonly pendingReads: ReturnType<typeof deferred<readonly RawDeliveryStreamEntry[]>>[] =
    [];
  private inspectIndex = 0;
  private lastMetadata: DeliveryStreamMetadata | undefined;
  private resolvedEntries: readonly RawDeliveryStreamEntry[] = [];

  constructor(...metadataValues: (DeliveryStreamMetadata | Error)[]) {
    this.inspections.push(...metadataValues);
  }

  connect(): Promise<void> {
    this.connectCalls += 1;
    return Promise.resolve();
  }

  async initializeStream(input: {
    generationToken: string;
  }): Promise<DeliveryStreamInitialization> {
    this.initializeCalls += 1;
    await this.initializeBarrier;
    const startupMetadata = this.inspections[this.inspectIndex];
    if (!(startupMetadata instanceof Error) && startupMetadata?.exists === false) {
      this.inspections[this.inspectIndex] = initializedSentinelMetadata();
      return {
        created: true,
        sentinelId: "0-1",
        generationToken: input.generationToken,
      };
    }
    return { created: false, sentinelId: null, generationToken: null };
  }

  verifyInitialization(): Promise<boolean> {
    this.verifyCalls += 1;
    return Promise.resolve(this.verificationResult);
  }

  inspect(): Promise<DeliveryStreamMetadata> {
    this.inspectCalls += 1;
    const value = this.inspections[this.inspectIndex];
    this.inspectIndex += 1;
    if (value instanceof Error) return Promise.reject(value);
    if (value) {
      this.lastMetadata = value;
      this.resolvedEntries = [];
      return Promise.resolve(value);
    }
    const previous = this.lastMetadata;
    if (!previous) return Promise.reject(new Error("missing fake metadata"));
    const appended = this.resolvedEntries.filter(({ id }) => {
      try {
        return compareRedisStreamIds(id, previous.lastGeneratedId) > 0;
      } catch {
        return false;
      }
    });
    this.resolvedEntries = [];
    if (appended.length === 0) return Promise.resolve(previous);
    const firstAppendedId = appended.reduce(
      (first, { id }) => (compareRedisStreamIds(id, first) < 0 ? id : first),
      appended[0]!.id,
    );
    const lastId = appended.reduce(
      (last, { id }) => (compareRedisStreamIds(id, last) > 0 ? id : last),
      appended[0]!.id,
    );
    const derived: DeliveryStreamMetadata = {
      ...previous,
      exists: true,
      length: String(BigInt(previous.length) + BigInt(appended.length)),
      firstEntryId: previous.firstEntryId ?? firstAppendedId,
      lastEntryId: lastId,
      lastGeneratedId: lastId,
      entriesAdded: String(BigInt(previous.entriesAdded) + BigInt(appended.length)),
    };
    this.lastMetadata = derived;
    return Promise.resolve(derived);
  }

  readAfter(input: {
    cursor: string;
    count: 100;
    blockMs: 5_000;
    signal: AbortSignal;
    onIssued: () => void;
  }): Promise<readonly RawDeliveryStreamEntry[]> {
    this.readCalls.push({ cursor: input.cursor, count: input.count, blockMs: input.blockMs });
    const pending = deferred<readonly RawDeliveryStreamEntry[]>();
    this.pendingReads.push(pending);
    this.activeReads += 1;
    this.maximumActiveReads = Math.max(this.maximumActiveReads, this.activeReads);
    input.signal.addEventListener("abort", () => pending.reject(new Error("aborted")), {
      once: true,
    });
    input.onIssued();
    this.onRead?.(input.cursor);
    return pending.promise.finally(() => {
      this.activeReads -= 1;
    });
  }

  resolveRead(entries: readonly RawDeliveryStreamEntry[]): void {
    const read = this.pendingReads.shift();
    if (!read) throw new Error("No pending XREAD");
    this.resolvedEntries = entries;
    read.resolve(entries);
  }

  rejectRead(error = new Error("Redis disconnected")): void {
    const read = this.pendingReads.shift();
    if (!read) throw new Error("No pending XREAD");
    read.reject(error);
  }

  cancelRead(): Promise<void> {
    this.cancelCalls += 1;
    this.pendingReads.shift()?.reject(new Error("read cancelled"));
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }
}

interface Harness {
  consumer: RedisDeliveryConsumer;
  delivered: DeliveryEnvelope[];
  quarantined: { boardId: string; reason: string; overflowed: boolean }[];
  lifecycle: DeliveryConsumerLifecycleEvent[];
}

function harness(
  transport: FakeTransport,
  overrides: Partial<DeliveryConsumerCallbacks> = {},
  configuration = defaultDeliveryConsumerConfiguration,
  scheduler?: DeliveryConsumerScheduler,
  hooks: DeliveryConsumerHooks = {},
): Harness {
  const delivered: DeliveryEnvelope[] = [];
  const quarantined: { boardId: string; reason: string; overflowed: boolean }[] = [];
  const lifecycle: DeliveryConsumerLifecycleEvent[] = [];
  const callbacks: DeliveryConsumerCallbacks = {
    deliver: ({ envelope: value }) => {
      delivered.push(value);
      return Promise.resolve();
    },
    quarantine: (value) => {
      quarantined.push(value);
      return Promise.resolve();
    },
    lifecycle: (value) => {
      lifecycle.push(value);
    },
    ...overrides,
  };
  return {
    consumer: new RedisDeliveryConsumer(
      transport,
      callbacks,
      { ...configuration },
      scheduler,
      hooks,
    ),
    delivered,
    quarantined,
    lifecycle,
  };
}

function retainedEmptyBoundary(consumer: RedisDeliveryConsumer): string | undefined {
  return (
    consumer as unknown as {
      witness?: { emptyBoundary?: { lastGeneratedId: string } };
    }
  ).witness?.emptyBoundary?.lastGeneratedId;
}

async function eventually(assertion: () => void): Promise<void> {
  let failure: unknown;
  for (let attempt = 0; attempt < 1_000; attempt += 1) {
    try {
      assertion();
      return;
    } catch (error) {
      failure = error;
      await Promise.resolve();
    }
  }
  throw failure;
}

describe("RedisDeliveryConsumer", () => {
  it("uses a verified initialization sentinel as the absent-stream startup cursor and tail", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);

    await test.consumer.start();

    expect(transport.initializeCalls).toBe(1);
    expect(transport.verifyCalls).toBe(1);
    expect(test.consumer.lastHandledCursor).toBe("0-1");
    expect(test.delivered).toEqual([]);
    expect(test.quarantined).toEqual([]);
    expect(test.lifecycle).toContainEqual({
      state: "established",
      cursor: "0-1",
      initialTail: "0-1",
    });
    await test.consumer.stop();
  });

  it("fails startup when the created initialization sentinel disappears before validation", async () => {
    const transport = new FakeTransport(emptyMetadata());
    transport.verificationResult = false;
    const test = harness(transport);

    await expect(test.consumer.start()).rejects.toThrow("STREAM_INITIALIZATION_FAILED");

    expect(test.consumer.lastHandledCursor).toBe("0-0");
    expect(test.delivered).toEqual([]);
    expect(test.lifecycle).toEqual([
      { state: "starting" },
      { state: "error", cursor: "0-0", code: "STREAM_INITIALIZATION_FAILED" },
    ]);
    await test.consumer.stop();
  });

  it("ignores a late initializer result after shutdown invalidates startup", async () => {
    const initializationGate = deferred<void>();
    const transport = new FakeTransport(emptyMetadata());
    transport.initializeBarrier = initializationGate.promise;
    const test = harness(transport);

    const startup = test.consumer.start();
    await eventually(() => expect(transport.initializeCalls).toBe(1));
    await test.consumer.stop();
    initializationGate.resolve();
    await expect(startup).resolves.toBeUndefined();

    expect(transport.inspectCalls).toBe(0);
    expect(transport.verifyCalls).toBe(0);
    expect(test.consumer.lastHandledCursor).toBe("0-0");
    expect(test.lifecycle.map(({ state }) => state)).toEqual(["starting", "stopping", "stopped"]);
  });

  it("fails closed when an absent startup stream is created, deleted, and recreated before inspection", async () => {
    const replacement = metadata({
      firstEntryId: "2-0",
      lastEntryId: "2-0",
      lastGeneratedId: "2-0",
      entriesAdded: "1",
    });
    const transport = new FakeTransport(emptyMetadata(), replacement);
    const test = harness(transport);
    await test.consumer.start();

    // XREAD has already observed A from the initialized incarnation, but XINFO sees only B from the
    // replacement stream. Neither the stale A result nor replacement metadata may be accepted.
    transport.resolveRead([entry("1-0", envelope(1))]);
    await eventually(() =>
      expect(
        test.consumer.lastHandledCursor === "2-0" ||
          test.lifecycle.some(({ state }) => state === "cursor_lost"),
      ).toBe(true),
    );

    expect(test.lifecycle.at(-1)).toMatchObject({ state: "cursor_lost" });
    expect(test.delivered).toEqual([]);
    expect(test.consumer.lastHandledCursor).toBe("0-1");
    await test.consumer.stop();
  });

  it("does not emit unavailable after stop wins an in-flight startup", async () => {
    const startingGate = deferred<void>();
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport, {
      lifecycle: async (event) => {
        test.lifecycle.push(event);
        if (event.state === "starting") await startingGate.promise;
      },
    });

    const startupOutcome = test.consumer.start().then(
      () => undefined,
      (error: unknown) => error,
    );
    await eventually(() => expect(test.lifecycle.map(({ state }) => state)).toEqual(["starting"]));
    await test.consumer.stop();
    startingGate.resolve();
    await startupOutcome;

    expect(test.lifecycle.map(({ state }) => state)).toEqual(["starting", "stopping", "stopped"]);
  });

  it("uses the conservative default global board-state capacity", () => {
    expect(defaultDeliveryConsumerConfiguration.maximumBoardStates).toBe(1_000);
  });

  it("never retains more distinct healthy board states than the configured limit", async () => {
    const transport = new FakeTransport(metadata());
    const test = harness(transport, {}, configurationWithBoardStateLimit(2));
    await test.consumer.start();

    transport.resolveRead([
      entry("11-0", envelope(1, boardId(1))),
      entry("12-0", envelope(1, boardId(2))),
      entry("13-0", envelope(1, boardId(3))),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("13-0"));

    expect(test.consumer.activeBoardStateCount).toBeLessThanOrEqual(2);
    await test.consumer.stop();
  });

  it("evicts the oldest healthy board state when the configured limit is reached", async () => {
    const transport = new FakeTransport(metadata());
    const test = harness(transport, {}, configurationWithBoardStateLimit(2));
    await test.consumer.start();

    transport.resolveRead([
      entry("11-0", envelope(1, boardId(1))),
      entry("12-0", envelope(1, boardId(2))),
      entry("13-0", envelope(1, boardId(3))),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("13-0"));

    expect(test.consumer.getBoardDiagnostics(boardId(1))).toBeUndefined();
    expect(test.consumer.getBoardDiagnostics(boardId(2))).toBeDefined();
    expect(test.consumer.getBoardDiagnostics(boardId(3))).toBeDefined();
    await test.consumer.stop();
  });

  it("fails closed instead of retaining quarantined boards beyond the configured limit", async () => {
    const transport = new FakeTransport(metadata());
    const test = harness(transport, {}, configurationWithBoardStateLimit(2));
    await test.consumer.start();

    transport.resolveRead([
      entry("11-0", envelope(1, boardId(1))),
      entry("12-0", envelope(3, boardId(1))),
      entry("13-0", envelope(1, boardId(2))),
      entry("14-0", envelope(3, boardId(2))),
      entry("15-0", envelope(1, boardId(3))),
      entry("16-0", envelope(3, boardId(3))),
      entry("17-0", envelope(1, boardId(4))),
    ]);
    await eventually(() =>
      expect(test.lifecycle.at(-1)).toMatchObject({
        state: "error",
        code: "BOARD_STATE_CAPACITY_EXCEEDED",
      }),
    );

    expect(test.consumer.lastHandledCursor).toBe("14-0");
    expect(test.consumer.activeBoardStateCount).toBe(2);
    expect(test.consumer.boardStateCapacityDiagnostics).toEqual({
      currentCount: 2,
      configuredLimit: 2,
      evictionCount: 0,
      capacityFailureCount: 1,
    });
    expect(test.delivered.map(({ boardId: deliveredBoard }) => deliveredBoard)).toEqual([
      boardId(1),
      boardId(2),
    ]);
    expect(test.lifecycle).toEqual([
      { state: "starting" },
      { state: "established", cursor: "10-0", initialTail: "10-0" },
      {
        state: "unavailable",
        cursor: "14-0",
        code: "BOARD_STATE_CAPACITY_EXCEEDED",
      },
      {
        state: "error",
        cursor: "14-0",
        entryId: "15-0",
        code: "BOARD_STATE_CAPACITY_EXCEEDED",
      },
    ]);
    expect(transport.readCalls).toHaveLength(1);
    await test.consumer.stop();
  });

  it("retains a recently touched healthy state over an older healthy state", async () => {
    const transport = new FakeTransport(metadata());
    const test = harness(transport, {}, configurationWithBoardStateLimit(2));
    await test.consumer.start();
    const firstBoardEvent = envelope(1, boardId(1), eventId(1));

    transport.resolveRead([
      entry("11-0", firstBoardEvent),
      entry("12-0", envelope(1, boardId(2), eventId(2))),
      entry("13-0", firstBoardEvent),
      entry("14-0", envelope(1, boardId(3), eventId(3))),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("14-0"));

    expect(test.consumer.getBoardDiagnostics(boardId(1))).toBeDefined();
    expect(test.consumer.getBoardDiagnostics(boardId(2))).toBeUndefined();
    expect(test.consumer.getBoardDiagnostics(boardId(3))).toBeDefined();
    expect(test.consumer.boardStateCapacityDiagnostics.evictionCount).toBe(1);
    await test.consumer.stop();
  });

  it("re-establishes an evicted board baseline and preserves its later contiguous ordering", async () => {
    const transport = new FakeTransport(metadata());
    const test = harness(transport, {}, configurationWithBoardStateLimit(2));
    await test.consumer.start();

    transport.resolveRead([
      entry("11-0", envelope(10, boardId(1), eventId(10))),
      entry("12-0", envelope(1, boardId(2), eventId(20))),
      entry("13-0", envelope(1, boardId(3), eventId(30))),
      entry("14-0", envelope(11, boardId(1), eventId(11))),
      entry("15-0", envelope(12, boardId(1), eventId(12))),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("15-0"));

    expect(
      test.delivered
        .filter(({ boardId: deliveredBoard }) => deliveredBoard === boardId(1))
        .map(({ deliverySeq }) => deliverySeq),
    ).toEqual([10, 11, 12]);
    expect(test.consumer.getBoardDiagnostics(boardId(1))?.cursor).toBe(12);
    expect(test.consumer.activeBoardStateCount).toBe(2);
    expect(test.consumer.boardStateCapacityDiagnostics.evictionCount).toBe(2);
    await test.consumer.stop();
  });

  it("never evicts a quarantined state when a healthy state is available", async () => {
    const transport = new FakeTransport(metadata());
    const test = harness(transport, {}, configurationWithBoardStateLimit(2));
    await test.consumer.start();

    transport.resolveRead([
      entry("11-0", envelope(1, boardId(1), eventId(1))),
      entry("12-0", envelope(3, boardId(1), eventId(3))),
      entry("13-0", envelope(1, boardId(2), eventId(20))),
      entry("14-0", envelope(1, boardId(3), eventId(30))),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("14-0"));

    expect(test.consumer.getBoardDiagnostics(boardId(1))?.quarantined).toBe(true);
    expect(test.consumer.getBoardDiagnostics(boardId(2))).toBeUndefined();
    expect(test.consumer.getBoardDiagnostics(boardId(3))).toBeDefined();
    expect(test.consumer.activeBoardStateCount).toBe(2);
    await test.consumer.stop();
  });

  it("makes a healthy state evictable only after its callback and cursor commit complete", async () => {
    const callbackEntered = deferred<void>();
    const callbackGate = deferred<void>();
    const cursorEntered = deferred<void>();
    const cursorGate = deferred<void>();
    const transport = new FakeTransport(metadata());
    const test = harness(
      transport,
      {
        deliver: async () => {
          callbackEntered.resolve();
          await callbackGate.promise;
        },
      },
      configurationWithBoardStateLimit(1),
      undefined,
      {
        beforeCursorAdvance: async () => {
          cursorEntered.resolve();
          await cursorGate.promise;
        },
      },
    );
    const internal = test.consumer as unknown as {
      boards: Map<string, unknown>;
      generation: number;
      isBoardStateEvictable(board: unknown, generation: number): boolean;
    };
    await test.consumer.start();

    transport.resolveRead([entry("11-0", envelope(1, boardId(1), eventId(1)))]);
    await callbackEntered.promise;
    const board = internal.boards.get(boardId(1));
    expect(board).toBeDefined();
    expect(internal.isBoardStateEvictable(board, internal.generation)).toBe(false);
    expect(test.consumer.lastHandledCursor).toBe("10-0");

    callbackGate.resolve();
    await cursorEntered.promise;
    expect(internal.isBoardStateEvictable(board, internal.generation)).toBe(false);
    expect(test.consumer.lastHandledCursor).toBe("10-0");

    cursorGate.resolve();
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("11-0"));
    expect(internal.isBoardStateEvictable(board, internal.generation)).toBe(true);
    await test.consumer.stop();
  });

  it("does not let a stale commit resurrect an evicted state", async () => {
    const transport = new FakeTransport(metadata());
    const test = harness(transport, {}, configurationWithBoardStateLimit(2));
    const internal = test.consumer as unknown as {
      boards: Map<string, unknown>;
      generation: number;
      markBoardPending(boardId: string, board: unknown, entryId: string, generation: number): void;
    };
    await test.consumer.start();

    transport.resolveRead([
      entry("11-0", envelope(1, boardId(1), eventId(1))),
      entry("12-0", envelope(1, boardId(2), eventId(2))),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("12-0"));
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    const evictedState = internal.boards.get(boardId(1));
    expect(evictedState).toBeDefined();

    transport.resolveRead([entry("13-0", envelope(1, boardId(3), eventId(3)))]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("13-0"));
    expect(test.consumer.getBoardDiagnostics(boardId(1))).toBeUndefined();
    expect(() =>
      internal.markBoardPending(boardId(1), evictedState, "11-0", internal.generation),
    ).toThrow();
    expect(test.consumer.getBoardDiagnostics(boardId(1))).toBeUndefined();
    expect(test.consumer.activeBoardStateCount).toBe(2);
    await test.consumer.stop();
  });

  it("keeps high-cardinality healthy delivery bounded by deterministic eviction", async () => {
    const maximumBoardStates = 5;
    const eventCount = 100;
    const transport = new FakeTransport(metadata());
    const test = harness(transport, {}, configurationWithBoardStateLimit(maximumBoardStates));
    await test.consumer.start();

    transport.resolveRead(
      Array.from({ length: eventCount }, (_, index) =>
        entry(`${11 + index}-0`, envelope(1, boardId(index + 1), eventId(index + 1))),
      ),
    );
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("110-0"));

    expect(test.delivered).toHaveLength(eventCount);
    expect(test.consumer.activeBoardStateCount).toBe(maximumBoardStates);
    expect(test.consumer.boardStateCapacityDiagnostics).toEqual({
      currentCount: maximumBoardStates,
      configuredLimit: maximumBoardStates,
      evictionCount: eventCount - maximumBoardStates,
      capacityFailureCount: 0,
    });
    await test.consumer.stop();
  });

  it.each([0, -1, 1.5, REDIS_DELIVERY_MAX_BOARD_STATES_UPPER_BOUND + 1])(
    "rejects an invalid maximum board-state count of %s before connecting",
    async (maximumBoardStates) => {
      const transport = new FakeTransport(emptyMetadata());
      const test = harness(transport, {}, configurationWithBoardStateLimit(maximumBoardStates));

      await expect(test.consumer.start()).rejects.toThrow("INVALID_CONFIGURATION");
      expect(transport.connectCalls).toBe(0);
      expect(test.consumer.activeBoardStateCount).toBe(0);
      expect(test.lifecycle).toEqual([
        { state: "error", cursor: "0-0", code: "INVALID_CONFIGURATION" },
      ]);
      await test.consumer.stop();
    },
  );

  it("captures the initial tail and never replays it", async () => {
    const transport = new FakeTransport(metadata());
    const test = harness(transport);
    await test.consumer.start();

    expect(transport.readCalls[0]).toEqual({
      cursor: "10-0",
      count: REDIS_DELIVERY_READ_COUNT,
      blockMs: REDIS_DELIVERY_BLOCK_MS,
    });
    expect(test.delivered).toEqual([]);
    transport.resolveRead([entry("11-0", envelope(8))]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("11-0"));
    expect(test.delivered).toHaveLength(1);
    await test.consumer.stop();
  });

  it("returns an entry appended after tail capture but before the first XREAD", async () => {
    const transport = new FakeTransport(metadata());
    const test = harness(transport);
    let observedCursor = "";
    transport.onRead = (cursor) => {
      observedCursor = cursor;
      transport.onRead = undefined;
      transport.resolveRead([entry("10-1", envelope(20))]);
    };

    await test.consumer.start();
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("10-1"));
    expect(observedCursor).toBe("10-0");
    expect(test.delivered).toHaveLength(1);
    await test.consumer.stop();
  });

  it.each([
    [
      "newest entry deleted",
      metadata({
        length: "1",
        firstEntryId: "10-0",
        lastEntryId: "10-0",
        lastGeneratedId: "11-0",
        maxDeletedEntryId: "11-0",
        entriesAdded: "2",
      }),
    ],
    [
      "all entries deleted",
      metadata({
        length: "0",
        firstEntryId: null,
        lastEntryId: null,
        lastGeneratedId: "11-0",
        maxDeletedEntryId: "11-0",
        entriesAdded: "1",
      }),
    ],
  ])(
    "starts from lastGeneratedId when %s and reconnects without false cursor loss",
    async (_name, startup) => {
      const transport = new FakeTransport(startup, startup, startup);
      const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
        wait: () => Promise.resolve(),
      });
      await test.consumer.start();

      expect(test.consumer.lastHandledCursor).toBe("11-0");
      expect(transport.readCalls[0]?.cursor).toBe("11-0");
      expect(test.lifecycle).toContainEqual({
        state: "established",
        cursor: "11-0",
        initialTail: "11-0",
      });

      transport.rejectRead();
      await eventually(() => expect(transport.readCalls).toHaveLength(2));
      expect(transport.readCalls[1]?.cursor).toBe("11-0");
      expect(test.lifecycle.some(({ state }) => state === "cursor_lost")).toBe(false);
      expect(test.consumer.lastHandledCursor).toBe("11-0");
      await test.consumer.stop();
    },
  );

  it.each([
    ["XTRIM", existingEmptyMetadata()],
    ["XDEL", existingEmptyMetadata({ maxDeletedEntryId: "10-0" })],
  ])(
    "delivers the first valid post-start entry after an existing stream was emptied by %s",
    async (_name, startup) => {
      const transport = new FakeTransport(startup);
      const test = harness(transport);
      await test.consumer.start();

      expect(test.consumer.lastHandledCursor).toBe("10-0");
      transport.resolveRead([entry("11-0", envelope(1))]);
      await eventually(() => expect(test.consumer.lastHandledCursor).toBe("11-0"));
      expect(test.delivered.map(({ deliverySeq }) => deliverySeq)).toEqual([1]);
      expect(test.lifecycle.some(({ state }) => state === "cursor_lost")).toBe(false);
      await test.consumer.stop();
    },
  );

  it("delivers multiple valid entries in order after a validated empty boundary", async () => {
    const transport = new FakeTransport(existingEmptyMetadata());
    const test = harness(transport);
    await test.consumer.start();

    transport.resolveRead([entry("11-0", envelope(1)), entry("12-0", envelope(2))]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("12-0"));
    expect(test.delivered.map(({ deliverySeq }) => deliverySeq)).toEqual([1, 2]);
    expect(test.lifecycle.some(({ state }) => state === "cursor_lost")).toBe(false);
    await test.consumer.stop();
  });

  it("captures a validated empty boundary during recovery", async () => {
    const emptiedDuringFailure = existingEmptyMetadata({ entriesAdded: "1" });
    const transport = new FakeTransport(metadata(), emptiedDuringFailure);
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
      wait: () => Promise.resolve(),
    });
    await test.consumer.start();

    transport.rejectRead();
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    transport.resolveRead([entry("11-0", envelope(1))]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("11-0"));
    expect(test.delivered.map(({ deliverySeq }) => deliverySeq)).toEqual([1]);
    expect(test.lifecycle.some(({ state }) => state === "cursor_lost")).toBe(false);
    await test.consumer.stop();
  });

  it("retains the empty boundary when recovery inspection precedes recovery consumption", async () => {
    const afterEntry = metadata({
      firstEntryId: "11-0",
      lastEntryId: "11-0",
      lastGeneratedId: "11-0",
      entriesAdded: "3",
    });
    const transport = new FakeTransport(existingEmptyMetadata(), afterEntry, afterEntry);
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
      wait: () => Promise.resolve(),
    });
    await test.consumer.start();

    transport.rejectRead();
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    transport.resolveRead([entry("11-0", envelope(1))]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("11-0"));

    expect(test.delivered.map(({ deliverySeq }) => deliverySeq)).toEqual([1]);
    expect(test.lifecycle.some(({ state }) => state === "cursor_lost")).toBe(false);
    expect(retainedEmptyBoundary(test.consumer)).toBeUndefined();
    await test.consumer.stop();
  });

  it("retains the empty boundary across an empty-read inspection race", async () => {
    const afterEntry = metadata({
      firstEntryId: "11-0",
      lastEntryId: "11-0",
      lastGeneratedId: "11-0",
      entriesAdded: "3",
    });
    const transport = new FakeTransport(existingEmptyMetadata(), afterEntry, afterEntry);
    const test = harness(transport);
    await test.consumer.start();

    transport.resolveRead([]);
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.delivered).toEqual([]);
    transport.resolveRead([entry("11-0", envelope(1))]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("11-0"));

    expect(test.delivered.map(({ deliverySeq }) => deliverySeq)).toEqual([1]);
    expect(test.lifecycle.some(({ state }) => state === "cursor_lost")).toBe(false);
    expect(retainedEmptyBoundary(test.consumer)).toBeUndefined();
    await test.consumer.stop();
  });

  it("retains the empty boundary when handling fails before cursor advancement", async () => {
    const transport = new FakeTransport(existingEmptyMetadata());
    const test = harness(transport, {
      deliver: () => Promise.reject(new Error("injected handler failure")),
    });
    await test.consumer.start();

    transport.resolveRead([entry("11-0", envelope(1))]);
    await eventually(() =>
      expect(test.lifecycle.at(-1)).toMatchObject({
        state: "error",
        code: "DELIVERY_CALLBACK_FAILED",
      }),
    );

    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(retainedEmptyBoundary(test.consumer)).toBe("10-0");
    await test.consumer.stop();
  });

  it("clears the empty boundary only after the owned cursor advancement", async () => {
    const beforeAdvance = deferred<void>();
    const allowAdvance = deferred<void>();
    const transport = new FakeTransport(existingEmptyMetadata());
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, undefined, {
      beforeCursorAdvance: async () => {
        beforeAdvance.resolve();
        await allowAdvance.promise;
      },
    });
    await test.consumer.start();

    transport.resolveRead([entry("11-0", envelope(1))]);
    await beforeAdvance.promise;
    expect(test.delivered.map(({ deliverySeq }) => deliverySeq)).toEqual([1]);
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(retainedEmptyBoundary(test.consumer)).toBe("10-0");

    allowAdvance.resolve();
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("11-0"));
    expect(retainedEmptyBoundary(test.consumer)).toBeUndefined();
    await test.consumer.stop();
  });

  it("does not let a stale handler completion clear the active empty boundary", async () => {
    const beforeAdvance = deferred<void>();
    const allowCompletion = deferred<void>();
    const transport = new FakeTransport(existingEmptyMetadata());
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, undefined, {
      beforeCursorAdvance: async () => {
        beforeAdvance.resolve();
        await allowCompletion.promise;
      },
    });
    await test.consumer.start();

    transport.resolveRead([entry("11-0", envelope(1))]);
    await beforeAdvance.promise;
    const stopping = test.consumer.stop();
    allowCompletion.resolve();
    await stopping;

    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(retainedEmptyBoundary(test.consumer)).toBe("10-0");
    expect(test.lifecycle.map(({ state }) => state).slice(-2)).toEqual(["stopping", "stopped"]);
  });

  it.each([
    [
      "XDEL",
      existingEmptyMetadata({
        length: "1",
        firstEntryId: "12-0",
        lastEntryId: "12-0",
        lastGeneratedId: "12-0",
        maxDeletedEntryId: "11-0",
        entriesAdded: "4",
      }),
    ],
    [
      "XTRIM",
      existingEmptyMetadata({
        length: "1",
        firstEntryId: "12-0",
        lastEntryId: "12-0",
        lastGeneratedId: "12-0",
        entriesAdded: "4",
      }),
    ],
  ])("fails closed when post-boundary %s removes an unread entry", async (_name, afterLoss) => {
    const transport = new FakeTransport(existingEmptyMetadata(), afterLoss);
    const test = harness(transport);
    await test.consumer.start();

    transport.resolveRead([entry("12-0", envelope(2))]);
    await eventually(() => expect(test.lifecycle.at(-1)?.state).toBe("cursor_lost"));
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.delivered).toEqual([]);
    await test.consumer.stop();
  });

  it("fails closed when an initially empty stream is deleted and recreated", async () => {
    const replacement = metadata({
      firstEntryId: "20-0",
      lastEntryId: "20-0",
      lastGeneratedId: "20-0",
      maxDeletedEntryId: "0-0",
      entriesAdded: "1",
    });
    const transport = new FakeTransport(existingEmptyMetadata(), replacement);
    const test = harness(transport);
    await test.consumer.start();

    transport.resolveRead([entry("20-0", envelope(1))]);
    await eventually(() =>
      expect(test.lifecycle.at(-1)).toMatchObject({
        state: "cursor_lost",
        reason: "STREAM_RECREATED",
      }),
    );
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.delivered).toEqual([]);
    await test.consumer.stop();
  });

  it.each([
    [
      "XDEL",
      metadata({
        length: "1",
        firstEntryId: "12-0",
        lastEntryId: "12-0",
        lastGeneratedId: "12-0",
        maxDeletedEntryId: "11-0",
        entriesAdded: "3",
      }),
      "TRIMMED_BEYOND_CURSOR",
    ],
    [
      "XTRIM",
      metadata({
        length: "1",
        firstEntryId: "12-0",
        lastEntryId: "12-0",
        lastGeneratedId: "12-0",
        maxDeletedEntryId: "0-0",
        entriesAdded: "3",
      }),
      "TRIMMED_BEYOND_CURSOR",
    ],
    [
      "stream recreation",
      metadata({
        length: "1",
        firstEntryId: "20-0",
        lastEntryId: "20-0",
        lastGeneratedId: "20-0",
        maxDeletedEntryId: "0-0",
        entriesAdded: "1",
      }),
      "STREAM_RECREATED",
    ],
  ])(
    "fails closed on healthy-read %s before delivering or advancing",
    async (_name, healthyMetadata, reason) => {
      const transport = new FakeTransport(metadata(), healthyMetadata);
      const test = harness(transport);
      await test.consumer.start();

      transport.resolveRead([entry(healthyMetadata.lastGeneratedId, envelope(11))]);
      await eventually(() =>
        expect(test.lifecycle.at(-1)).toMatchObject({ state: "cursor_lost", reason }),
      );
      expect(test.delivered).toEqual([]);
      expect(test.consumer.lastHandledCursor).toBe("10-0");
      expect(transport.readCalls).toHaveLength(1);
      await test.consumer.stop();
    },
  );

  it("gives two independent consumers every post-start entry", async () => {
    const firstTransport = new FakeTransport(metadata());
    const secondTransport = new FakeTransport(metadata());
    const first = harness(firstTransport);
    const second = harness(secondTransport);
    await Promise.all([first.consumer.start(), second.consumer.start()]);
    const published = entry("11-0", envelope(4));
    firstTransport.resolveRead([published]);
    secondTransport.resolveRead([published]);

    await eventually(() => expect(first.consumer.lastHandledCursor).toBe("11-0"));
    await eventually(() => expect(second.consumer.lastHandledCursor).toBe("11-0"));
    expect(first.delivered).toHaveLength(1);
    expect(second.delivered).toHaveLength(1);
    await Promise.all([first.consumer.stop(), second.consumer.stop()]);
  });

  it("processes Redis IDs monotonically and fails globally on reversal", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);
    await test.consumer.start();
    transport.resolveRead([entry("2-0", envelope(1)), entry("1-0", envelope(2))]);

    await eventually(() =>
      expect(test.lifecycle.at(-1)).toMatchObject({
        state: "error",
        code: "NON_MONOTONIC_REDIS_ENTRY_ID",
      }),
    );
    expect(test.delivered.map((value) => value.deliverySeq)).toEqual([1]);
    expect(test.consumer.lastHandledCursor).toBe("2-0");
    await test.consumer.stop();
  });

  it("suppresses exact ambiguous-publication duplicates while advancing transport", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);
    const duplicate = envelope(7);
    await test.consumer.start();
    transport.resolveRead([entry("1-0", duplicate), entry("2-0", duplicate)]);

    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("2-0"));
    expect(test.delivered).toEqual([duplicate]);
    expect(test.quarantined).toEqual([]);
    expect(test.consumer.activeBoardStateCount).toBe(1);
    await test.consumer.stop();
  });

  it("quarantines conflicting duplicates and gaps without blocking unrelated boards", async () => {
    const quarantineGate = deferred<void>();
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport, {
      quarantine: async (value) => {
        test.quarantined.push(value);
        await quarantineGate.promise;
      },
    });
    await test.consumer.start();
    const first = envelope(1);
    const conflict = envelope(1, ids.boardA, eventId(1, 9));
    transport.resolveRead([
      entry("1-0", first),
      entry("2-0", conflict),
      entry("3-0", envelope(3, ids.boardA)),
      entry("4-0", envelope(50, ids.boardB)),
    ]);

    await eventually(() => expect(test.quarantined).toHaveLength(1));
    expect(test.consumer.lastHandledCursor).toBe("1-0");
    expect(test.delivered.map((value) => value.boardId)).toEqual([ids.boardA]);
    quarantineGate.resolve();
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("4-0"));
    expect(test.quarantined.map((value) => value.reason)).toEqual([
      "CONFLICTING_DELIVERY_SEQUENCE",
      "BOARD_ALREADY_QUARANTINED",
    ]);
    expect(test.delivered.map((value) => value.boardId)).toEqual([ids.boardA, ids.boardB]);
    await test.consumer.stop();
  });

  it("quarantines event-ID reuse at a different sequence", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);
    const repeatedId = eventId(1);
    await test.consumer.start();
    transport.resolveRead([
      entry("1-0", envelope(1, ids.boardA, repeatedId)),
      entry("2-0", envelope(2, ids.boardA, repeatedId)),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("2-0"));
    expect(test.delivered).toHaveLength(1);
    expect(test.quarantined[0]?.reason).toBe("EVENT_ID_REUSED");
    await test.consumer.stop();
  });

  it("quarantines a repeated sequence and event ID with a conflicting envelope", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);
    const stableId = eventId(1);
    await test.consumer.start();
    transport.resolveRead([
      entry("1-0", envelope(1, ids.boardA, stableId)),
      entry("2-0", envelope(1, ids.boardA, stableId, "30000000-0000-4000-8000-000000000009")),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("2-0"));
    expect(test.delivered).toHaveLength(1);
    expect(test.quarantined[0]?.reason).toBe("CONFLICTING_DELIVERY_SEQUENCE");
    await test.consumer.stop();
  });

  it("quarantines a delivery gap and never delivers later events for that board", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);
    await test.consumer.start();
    transport.resolveRead([
      entry("1-0", envelope(10)),
      entry("2-0", envelope(12)),
      entry("3-0", envelope(11)),
      entry("4-0", envelope(1, ids.boardB)),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("4-0"));
    expect(test.delivered.map((value) => [value.boardId, value.deliverySeq])).toEqual([
      [ids.boardA, 10],
      [ids.boardB, 1],
    ]);
    expect(test.quarantined).toHaveLength(2);
    await test.consumer.stop();
  });

  it.each([
    [
      "unknown field",
      (valid: RawDeliveryStreamEntry) => ({
        ...valid,
        fields: [...valid.fields, ["surprise", "1"] as const],
      }),
    ],
    [
      "duplicate field",
      (valid: RawDeliveryStreamEntry) => ({
        ...valid,
        fields: [...valid.fields, valid.fields[0]!],
      }),
    ],
    [
      "mismatched metadata",
      (valid: RawDeliveryStreamEntry) => ({
        ...valid,
        fields: valid.fields.map(
          ([name, value]) => [name, name === "boardId" ? ids.boardB : value] as const,
        ),
      }),
    ],
    ["malformed ID", (valid: RawDeliveryStreamEntry) => ({ ...valid, id: "not-an-id" })],
  ])("fails closed on %s without cursor advancement", async (_name, poison) => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);
    await test.consumer.start();
    transport.resolveRead([poison(entry("1-0", envelope(1)))]);
    await eventually(() => expect(test.lifecycle.at(-1)?.state).toBe("error"));
    expect(test.delivered).toEqual([]);
    expect(test.consumer.lastHandledCursor).toBe("0-1");
    expect(test.consumer.activeBoardStateCount).toBe(0);
    await test.consumer.stop();
  });

  it("rejects oversized entries without advancing", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(
      transport,
      {},
      { ...defaultDeliveryConsumerConfiguration, maximumEnvelopeBytes: 4 },
    );
    await test.consumer.start();
    transport.resolveRead([entry("1-0", envelope(1))]);
    await eventually(() => expect(test.lifecycle.at(-1)?.state).toBe("error"));
    expect(test.consumer.lastHandledCursor).toBe("0-1");
    await test.consumer.stop();
  });

  it("stops a poisoned batch at the failed entry and never lets later entries pass", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);
    const poison = entry("2-0", envelope(2));
    poison.fields = poison.fields.map(([name, value]) =>
      name === "schemaVersion" ? ([name, "2"] as const) : ([name, value] as const),
    );
    await test.consumer.start();
    transport.resolveRead([
      entry("1-0", envelope(1)),
      poison,
      entry("3-0", envelope(1, ids.boardB)),
    ]);
    await eventually(() => expect(test.lifecycle.at(-1)?.state).toBe("error"));
    expect(test.consumer.lastHandledCursor).toBe("1-0");
    expect(test.delivered.map((value) => value.boardId)).toEqual([ids.boardA]);
    await test.consumer.stop();
  });

  it("fails closed before processing when the bounded global queue overflows", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(
      transport,
      {},
      { ...defaultDeliveryConsumerConfiguration, globalQueueMaximumEvents: 1 },
    );
    await test.consumer.start();
    transport.resolveRead([entry("1-0", envelope(1)), entry("2-0", envelope(2))]);
    await eventually(() =>
      expect(test.lifecycle.at(-1)).toMatchObject({
        state: "error",
        code: "GLOBAL_QUEUE_OVERFLOW",
      }),
    );
    expect(test.consumer.lastHandledCursor).toBe("0-1");
    expect(test.delivered).toEqual([]);
    await test.consumer.stop();
  });

  it("does not advance before awaited delivery and keeps the cursor on callback failure", async () => {
    const gate = deferred<void>();
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport, { deliver: () => gate.promise });
    await test.consumer.start();
    transport.resolveRead([entry("1-0", envelope(1))]);
    await eventually(() => expect(transport.readCalls).toHaveLength(1));
    expect(test.consumer.lastHandledCursor).toBe("0-1");
    gate.reject(new Error("local handling failed"));
    await eventually(() =>
      expect(test.lifecycle.at(-1)).toMatchObject({
        state: "error",
        code: "DELIVERY_CALLBACK_FAILED",
      }),
    );
    expect(test.consumer.lastHandledCursor).toBe("0-1");
    await test.consumer.stop();
  });

  it("retains its cursor and drains through a valid recovery tail before recovered", async () => {
    const transport = new FakeTransport(
      metadata(),
      metadata({
        length: "3",
        lastEntryId: "12-0",
        lastGeneratedId: "12-0",
        entriesAdded: "3",
      }),
    );
    const test = harness(transport);
    await test.consumer.start();
    transport.rejectRead();
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    expect(transport.readCalls[1]?.cursor).toBe("10-0");
    transport.resolveRead([entry("11-0", envelope(11)), entry("12-0", envelope(12))]);
    await eventually(() => expect(transport.readCalls).toHaveLength(3));
    expect(test.consumer.lastHandledCursor).toBe("12-0");
    expect(test.lifecycle.at(-1)).toMatchObject({ state: "recovered", recoveryTail: "12-0" });
    expect(transport.readCalls[2]?.cursor).toBe("12-0");
    await test.consumer.stop();
  });

  it("retains quarantine through recovery and still enforces the global cap", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport, {}, configurationWithBoardStateLimit(1), {
      wait: () => Promise.resolve(),
    });
    await test.consumer.start();

    transport.resolveRead([
      entry("1-0", envelope(1, boardId(1), eventId(1))),
      entry("2-0", envelope(3, boardId(1), eventId(3))),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("2-0"));
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    expect(test.consumer.getBoardDiagnostics(boardId(1))?.quarantined).toBe(true);

    transport.rejectRead();
    await eventually(() => expect(transport.readCalls).toHaveLength(3));
    await eventually(() => expect(test.lifecycle.at(-1)?.state).toBe("recovered"));
    expect(test.consumer.getBoardDiagnostics(boardId(1))?.quarantined).toBe(true);

    transport.resolveRead([entry("3-0", envelope(1, boardId(2), eventId(20)))]);
    await eventually(() =>
      expect(test.lifecycle.at(-1)).toMatchObject({
        state: "error",
        code: "BOARD_STATE_CAPACITY_EXCEEDED",
      }),
    );
    expect(test.consumer.lastHandledCursor).toBe("2-0");
    expect(test.consumer.getBoardDiagnostics(boardId(1))?.quarantined).toBe(true);
    expect(test.consumer.getBoardDiagnostics(boardId(2))).toBeUndefined();
    expect(test.consumer.boardStateCapacityDiagnostics).toEqual({
      currentCount: 1,
      configuredLimit: 1,
      evictionCount: 0,
      capacityFailureCount: 1,
    });
    expect(test.lifecycle.map(({ state }) => state)).toEqual([
      "starting",
      "established",
      "unavailable",
      "recovering",
      "recovered",
      "unavailable",
      "error",
    ]);
    await test.consumer.stop();
  });

  it("stays in recovery when metadata inspection fails after reconnect", async () => {
    const retryGate = deferred<void>();
    const recoveredMetadata = metadata({
      length: "3",
      lastEntryId: "12-0",
      lastGeneratedId: "12-0",
      entriesAdded: "3",
    });
    const transport = new FakeTransport(
      metadata(),
      new Error("transient XINFO failure"),
      recoveredMetadata,
    );
    let waits = 0;
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
      wait: () => {
        waits += 1;
        return retryGate.promise;
      },
    });
    await test.consumer.start();

    transport.rejectRead();
    await eventually(() => expect(waits).toBe(1));
    expect(test.lifecycle.at(-1)).toEqual({ state: "recovering", cursor: "10-0" });
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(transport.inspectCalls).toBe(2);
    expect(transport.readCalls).toHaveLength(1);

    retryGate.resolve();
    await eventually(() => expect(transport.inspectCalls).toBe(3));
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    expect(transport.readCalls[1]?.cursor).toBe("10-0");
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.lifecycle.some(({ state }) => state === "recovered")).toBe(false);
    await test.consumer.stop();
  });

  it("uses one recovery loop across repeated inspection failures and recovers only after draining", async () => {
    const recoveredMetadata = metadata({
      length: "3",
      lastEntryId: "12-0",
      lastGeneratedId: "12-0",
      entriesAdded: "3",
    });
    const transport = new FakeTransport(
      metadata(),
      new Error("first transient XINFO failure"),
      new Error("second transient XINFO failure"),
      recoveredMetadata,
      recoveredMetadata,
    );
    let waits = 0;
    const cursorsDuringFailures: string[] = [];
    const observed: { consumer?: RedisDeliveryConsumer } = {};
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
      wait: () => {
        waits += 1;
        if (!observed.consumer) throw new Error("consumer not initialized");
        cursorsDuringFailures.push(observed.consumer.lastHandledCursor);
        return Promise.resolve();
      },
    });
    observed.consumer = test.consumer;
    await test.consumer.start();

    transport.rejectRead();
    await eventually(() => expect(transport.inspectCalls).toBe(4));
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    expect(waits).toBe(2);
    expect(cursorsDuringFailures).toEqual(["10-0", "10-0"]);
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.lifecycle.filter(({ state }) => state === "recovering")).toHaveLength(1);
    expect(test.lifecycle.filter(({ state }) => state === "recovered")).toHaveLength(0);
    expect(transport.maximumActiveReads).toBe(1);

    transport.resolveRead([entry("11-0", envelope(11)), entry("12-0", envelope(12))]);
    await eventually(() => expect(transport.readCalls).toHaveLength(3));
    expect(test.consumer.lastHandledCursor).toBe("12-0");
    expect(test.lifecycle.filter(({ state }) => state === "recovered")).toEqual([
      { state: "recovered", cursor: "12-0", recoveryTail: "12-0" },
    ]);
    expect(transport.readCalls[2]?.cursor).toBe("12-0");
    expect(transport.maximumActiveReads).toBe(1);
    await test.consumer.stop();
  });

  it("revokes availability when the pending read fails after recovered", async () => {
    const transport = new FakeTransport(metadata(), metadata(), metadata());
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
      wait: () => Promise.resolve(),
    });
    await test.consumer.start();

    transport.rejectRead();
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    await eventually(() => expect(test.lifecycle.at(-1)?.state).toBe("recovered"));
    transport.rejectRead(new Error("post-recovery XREAD failed"));
    await eventually(() => expect(transport.readCalls).toHaveLength(3));

    expect(test.lifecycle.map(({ state }) => state)).toEqual([
      "starting",
      "established",
      "unavailable",
      "recovering",
      "recovered",
      "unavailable",
      "recovering",
      "recovered",
    ]);
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.delivered).toEqual([]);
    expect(transport.maximumActiveReads).toBe(1);
    await test.consumer.stop();
  });

  it("revokes availability when first post-recovery metadata inspection fails", async () => {
    const afterEntry = metadata({
      length: "2",
      lastEntryId: "11-0",
      lastGeneratedId: "11-0",
      entriesAdded: "2",
    });
    const transport = new FakeTransport(
      metadata(),
      metadata(),
      new Error("post-recovery XINFO failed"),
      afterEntry,
    );
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
      wait: () => Promise.resolve(),
    });
    await test.consumer.start();

    transport.rejectRead();
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    transport.resolveRead([entry("11-0", envelope(1))]);
    await eventually(() => expect(transport.readCalls).toHaveLength(3));
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.delivered).toEqual([]);
    transport.resolveRead([entry("11-0", envelope(1))]);
    await eventually(() => expect(transport.readCalls).toHaveLength(4));

    expect(test.lifecycle.map(({ state }) => state)).toEqual([
      "starting",
      "established",
      "unavailable",
      "recovering",
      "recovered",
      "unavailable",
      "recovering",
      "recovered",
    ]);
    expect(test.consumer.lastHandledCursor).toBe("11-0");
    expect(test.delivered.map(({ deliverySeq }) => deliverySeq)).toEqual([1]);
    expect(transport.maximumActiveReads).toBe(1);
    await test.consumer.stop();
  });

  it("never emits recovered twice without unavailable across repeated post-recovery failures", async () => {
    const transport = new FakeTransport(metadata(), metadata());
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
      wait: () => Promise.resolve(),
    });
    await test.consumer.start();

    transport.rejectRead();
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    transport.rejectRead(new Error("first post-recovery failure"));
    await eventually(() => expect(transport.readCalls).toHaveLength(3));
    transport.rejectRead(new Error("second post-recovery failure"));
    await eventually(() => expect(transport.readCalls).toHaveLength(4));

    expect(test.lifecycle.map(({ state }) => state)).toEqual([
      "starting",
      "established",
      "unavailable",
      "recovering",
      "recovered",
      "unavailable",
      "recovering",
      "recovered",
      "unavailable",
      "recovering",
      "recovered",
    ]);
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.delivered).toEqual([]);
    expect(transport.maximumActiveReads).toBe(1);
    await test.consumer.stop();
  });

  it("emits unavailable once while one post-recovery failure episode keeps retrying", async () => {
    const transport = new FakeTransport(
      metadata(),
      metadata(),
      new Error("first retry inspection failed"),
      new Error("second retry inspection failed"),
      metadata(),
    );
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
      wait: () => Promise.resolve(),
    });
    await test.consumer.start();

    transport.rejectRead();
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    transport.rejectRead(new Error("post-recovery read failed"));
    await eventually(() => expect(transport.inspectCalls).toBe(5));
    await eventually(() => expect(transport.readCalls).toHaveLength(3));

    expect(test.lifecycle.map(({ state }) => state)).toEqual([
      "starting",
      "established",
      "unavailable",
      "recovering",
      "recovered",
      "unavailable",
      "recovering",
      "recovered",
    ]);
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.delivered).toEqual([]);
    expect(transport.maximumActiveReads).toBe(1);
    await test.consumer.stop();
  });

  it("shutdown after recovered emits no spurious availability transition", async () => {
    const transport = new FakeTransport(metadata(), metadata());
    const test = harness(transport, {}, defaultDeliveryConsumerConfiguration, {
      wait: () => Promise.resolve(),
    });
    await test.consumer.start();

    transport.rejectRead();
    await eventually(() => expect(transport.readCalls).toHaveLength(2));
    await eventually(() => expect(test.lifecycle.at(-1)?.state).toBe("recovered"));
    await test.consumer.stop();

    expect(test.lifecycle.map(({ state }) => state)).toEqual([
      "starting",
      "established",
      "unavailable",
      "recovering",
      "recovered",
      "stopping",
      "stopped",
    ]);
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    expect(test.delivered).toEqual([]);
  });

  it.each([
    [
      "trim",
      metadata({
        firstEntryId: "12-0",
        lastEntryId: "12-0",
        lastGeneratedId: "12-0",
        maxDeletedEntryId: "11-0",
        entriesAdded: "3",
      }),
      "TRIMMED_BEYOND_CURSOR",
    ],
    ["deletion", emptyMetadata(), "STREAM_MISSING"],
    [
      "recreation",
      metadata({
        firstEntryId: "20-0",
        lastEntryId: "20-0",
        lastGeneratedId: "20-0",
        entriesAdded: "1",
      }),
      "STREAM_RECREATED",
    ],
  ])("detects stream %s during reconnect", async (_name, recoveredMetadata, reason) => {
    const transport = new FakeTransport(metadata(), recoveredMetadata);
    const test = harness(transport);
    await test.consumer.start();
    transport.rejectRead();
    await eventually(() =>
      expect(test.lifecycle.at(-1)).toMatchObject({ state: "cursor_lost", reason }),
    );
    expect(test.consumer.lastHandledCursor).toBe("10-0");
    await test.consumer.stop();
  });

  it("bounds dedupe and quarantine state and drops quarantine on overflow", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(
      transport,
      {},
      {
        ...defaultDeliveryConsumerConfiguration,
        boardDedupeMaximumEvents: 3,
        boardDedupeMaximumBytes: 10_000,
        boardQuarantineMaximumEvents: 2,
        boardQuarantineMaximumBytes: 1_000_000,
      },
    );
    await test.consumer.start();
    transport.resolveRead([
      entry("1-0", envelope(1)),
      entry("2-0", envelope(2)),
      entry("3-0", envelope(3)),
      entry("4-0", envelope(4)),
      entry("5-0", envelope(6)),
      entry("6-0", envelope(7)),
      entry("7-0", envelope(8)),
    ]);
    await eventually(() => expect(test.consumer.lastHandledCursor).toBe("7-0"));
    const diagnostics = test.consumer.getBoardDiagnostics(ids.boardA);
    expect(diagnostics?.dedupeEvents).toBe(3);
    expect(diagnostics?.dedupeBytes).toBeLessThanOrEqual(10_000);
    expect(diagnostics?.quarantineEvents).toBe(0);
    expect(diagnostics?.quarantineBytes).toBe(0);
    expect(test.quarantined.at(-1)).toMatchObject({
      reason: "QUARANTINE_OVERFLOW",
      overflowed: true,
    });
    await test.consumer.stop();
  });

  it("cancels a blocked XREAD and closes owned connections exactly once", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);
    await test.consumer.start();
    const firstStop = test.consumer.stop();
    expect(test.consumer.stop()).toBe(firstStop);
    await firstStop;
    expect(transport.cancelCalls).toBe(1);
    expect(transport.closeCalls).toBe(1);
    expect(test.lifecycle.map(({ state }) => state)).toContain("stopped");
  });

  it("releases all retained board accounting on shutdown and a fresh restart starts clean", async () => {
    const firstTransport = new FakeTransport(emptyMetadata());
    const first = harness(firstTransport, {}, configurationWithBoardStateLimit(2));
    await first.consumer.start();
    firstTransport.resolveRead([
      entry("1-0", envelope(1, boardId(1), eventId(1))),
      entry("2-0", envelope(1, boardId(2), eventId(2))),
      entry("3-0", envelope(1, boardId(3), eventId(3))),
    ]);
    await eventually(() => expect(first.consumer.lastHandledCursor).toBe("3-0"));
    expect(first.consumer.boardStateCapacityDiagnostics.currentCount).toBe(2);
    await first.consumer.stop();
    expect(first.consumer.boardStateCapacityDiagnostics.currentCount).toBe(0);

    const restartedTransport = new FakeTransport(emptyMetadata());
    const restarted = harness(restartedTransport, {}, configurationWithBoardStateLimit(2));
    expect(restarted.consumer.boardStateCapacityDiagnostics).toEqual({
      currentCount: 0,
      configuredLimit: 2,
      evictionCount: 0,
      capacityFailureCount: 0,
    });
    await restarted.consumer.start();
    expect(restarted.consumer.activeBoardStateCount).toBe(0);
    await restarted.consumer.stop();
  });

  it("uses only independent XREAD parameters and no group, Pub/Sub, or adapter operation", async () => {
    const transport = new FakeTransport(emptyMetadata());
    const test = harness(transport);
    await test.consumer.start();
    expect(transport.readCalls).toEqual([{ cursor: "0-1", count: 100, blockMs: 5_000 }]);
    expect(Object.keys(transport)).not.toContain("group");
    await test.consumer.stop();
  });
});
