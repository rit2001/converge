import { describe, expect, it, vi } from "vitest";
import type {
  ClaimedOutboxEvent,
  ClaimOutboxOptions,
  LeaseMutationResult,
  MarkOutboxPublishedInput,
  RecordOutboxFailureInput,
} from "@converge/database";
import type { StructuredLogger } from "@converge/observability";
import {
  DELIVERY_ENVELOPE_MAX_BYTES,
  DELIVERY_STREAM_ENTRY_MAX_BYTES,
  decodeDeliveryStreamFields,
  encodeDeliveryStreamFields,
  membershipRevokedDeliveryEnvelopeSchema,
  validateDeliveryStreamEntrySize,
  type DeliveryStreamFields,
} from "@converge/protocol";
import { RedisXaddRejectedError, type DeliveryStream } from "./redis-stream.js";
import { WorkerProcessLifecycle, type WorkerLoop } from "./lifecycle.js";
import {
  OutboxWorker,
  systemWorkerScheduler,
  type OutboxPublicationRepository,
  type OutboxWorkerConfiguration,
  type WorkerScheduler,
} from "./worker.js";

function claim(sequence = 1, boardId = crypto.randomUUID()): ClaimedOutboxEvent {
  const eventId = crypto.randomUUID();
  const envelope = membershipRevokedDeliveryEnvelopeSchema.parse({
    schemaVersion: 1,
    eventId,
    boardId,
    deliverySeq: sequence,
    eventType: "board.membership.revoked",
    occurredAt: "2026-08-09T12:00:00.000Z",
    payload: {
      revokedUserId: crypto.randomUUID(),
      initiatedByUserId: crypto.randomUUID(),
    },
  });
  return {
    eventId,
    boardId,
    deliverySeq: sequence,
    canvasSeq: null,
    eventType: envelope.eventType,
    schemaVersion: 1,
    envelope,
    attemptCount: 1,
    leaseOwner: "worker-test",
    leaseToken: crypto.randomUUID(),
    leasedUntil: new Date("2026-08-09T12:01:00.000Z"),
  };
}

function claimWithEnvelopeBytes(targetBytes: number): ClaimedOutboxEvent {
  const value = claim(Number.MAX_SAFE_INTEGER);
  const baseTimestamp = "2026-08-09T12:00:00.0Z";
  const baseline = JSON.stringify({ ...value.envelope, occurredAt: baseTimestamp }).length;
  const additionalDigits = targetBytes - baseline;
  if (additionalDigits < 0) throw new Error("Target envelope is smaller than the test fixture");
  const occurredAt = `2026-08-09T12:00:00.${"0".repeat(additionalDigits + 1)}Z`;
  const envelope = membershipRevokedDeliveryEnvelopeSchema.parse({
    ...value.envelope,
    occurredAt,
  });
  if (new TextEncoder().encode(JSON.stringify(envelope)).byteLength !== targetBytes)
    throw new Error("Test fixture did not reach the requested encoded envelope size");
  return { ...value, envelope };
}

const applied = (eventId: string, status: "published" | "retry_wait" | "blocked") =>
  ({
    outcome: "applied",
    eventId,
    status,
    attemptCount: 1,
    leasedUntil: null,
    nextAttemptAt: null,
    publishedAt: status === "published" ? new Date() : null,
  }) satisfies LeaseMutationResult;

class FakeRepository implements OutboxPublicationRepository {
  claims: ClaimedOutboxEvent[] = [];
  claimOptions: ClaimOutboxOptions[] = [];
  published: MarkOutboxPublishedInput[] = [];
  failures: RecordOutboxFailureInput[] = [];
  publishResult: LeaseMutationResult | undefined;

  claimAvailable(options: ClaimOutboxOptions): Promise<ClaimedOutboxEvent[]> {
    this.claimOptions.push(options);
    return Promise.resolve(this.claims.splice(0, options.batchSize ?? this.claims.length));
  }

  markPublished(input: MarkOutboxPublishedInput): Promise<LeaseMutationResult> {
    this.published.push(input);
    return Promise.resolve(this.publishResult ?? applied(input.eventId, "published"));
  }

  recordFailure(input: RecordOutboxFailureInput): Promise<LeaseMutationResult> {
    this.failures.push(input);
    return Promise.resolve(applied(input.eventId, input.retryable ? "retry_wait" : "blocked"));
  }
}

class FakeStream implements DeliveryStream {
  ready = true;
  appended: DeliveryStreamFields[] = [];
  results: unknown[] = [];
  trimCalls = 0;
  trimFailure = false;
  resetCalls = 0;

  connect(): Promise<void> {
    this.ready = true;
    return Promise.resolve();
  }

  isReady(): boolean {
    return this.ready;
  }

  append(fields: DeliveryStreamFields, signal: AbortSignal): Promise<unknown> {
    void signal;
    this.appended.push(fields);
    const result = this.results.shift() ?? `${this.appended.length}-0`;
    return result instanceof Error ? Promise.reject(result) : Promise.resolve(result);
  }

  trimByAge(signal: AbortSignal): Promise<void> {
    void signal;
    this.trimCalls += 1;
    return this.trimFailure ? Promise.reject(new Error("trim details")) : Promise.resolve();
  }

  resetAfterCommandTimeout(): Promise<void> {
    this.resetCalls += 1;
    return Promise.resolve();
  }

  close(force?: boolean): Promise<void> {
    void force;
    this.ready = false;
    return Promise.resolve();
  }
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

const scheduler: WorkerScheduler = {
  random: () => 0.5,
  sleep: (_delay, signal) =>
    new Promise((resolve) => signal?.addEventListener("abort", () => resolve(), { once: true })),
};

const logger: StructuredLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const configuration: OutboxWorkerConfiguration = {
  owner: "worker-test",
  claimBatchSize: 32,
  publishConcurrency: 8,
  leaseDurationMs: 60_000,
  idlePollMs: 250,
  pollJitterRatio: 0.2,
  publicationTimeoutMs: 5_000,
  maximumEnvelopeBytes: 128 * 1024,
};

describe("outbox worker", () => {
  it("claims before XADD and finalizes only the returned strict stream entry ID", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const order: string[] = [];
    const claimAvailable = repository.claimAvailable.bind(repository);
    repository.claimAvailable = async (options) => {
      order.push("claim");
      return claimAvailable(options);
    };
    const append = stream.append.bind(stream);
    stream.append = (fields, signal) => {
      order.push("xadd");
      return append(fields, signal);
    };
    const markPublished = repository.markPublished.bind(repository);
    repository.markPublished = (input) => {
      order.push("mark-published");
      return markPublished(input);
    };
    const event = claim();
    repository.claims.push(event);
    stream.results.push("1000-7");
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    await expect(worker.runCycle()).resolves.toEqual({
      state: "processed",
      claimed: 1,
      outcomes: ["published"],
    });
    expect(repository.claimOptions).toHaveLength(1);
    expect(decodeDeliveryStreamFields(stream.appended[0])).toEqual(event.envelope);
    expect(stream.appended[0]).toMatchObject({
      schemaVersion: "1",
      eventId: event.eventId,
      boardId: event.boardId,
      deliverySeq: "1",
      eventType: event.eventType,
    });
    expect(repository.published).toEqual([
      { eventId: event.eventId, leaseToken: event.leaseToken, publicationId: "1000-7" },
    ]);
    expect(order).toEqual(["claim", "xadd", "mark-published"]);
  });

  it("records XADD rejection as retryable without publishing", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const logs: string[] = [];
    const capturingLogger: StructuredLogger = {
      info: (fields, message) => logs.push(JSON.stringify({ fields, message })),
      warn: (fields, message) => logs.push(JSON.stringify({ fields, message })),
      error: (fields, message) => logs.push(JSON.stringify({ fields, message })),
    };
    const event = claim();
    repository.claims.push(event);
    stream.results.push(
      new RedisXaddRejectedError("redis://user:private-password@private-host connection details"),
    );
    const worker = new OutboxWorker(repository, stream, configuration, capturingLogger, scheduler);

    expect((await worker.runCycle()).outcomes).toEqual(["retry_scheduled"]);
    expect(repository.published).toEqual([]);
    expect(repository.failures).toEqual([
      expect.objectContaining({
        eventId: event.eventId,
        retryable: true,
        errorCode: "REDIS_XADD_FAILED",
        errorMessage: "Redis did not acknowledge publication.",
      }),
    ]);
    expect(logs.join("\n")).toContain("REDIS_XADD_FAILED");
    expect(logs.join("\n")).not.toMatch(
      /private-password|private-host|connection details|payload/i,
    );
  });

  it("records a timed-out possible acceptance as ambiguous and retryable", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const event = claim();
    const append = deferred<unknown>();
    const timeout = deferred<void>();
    repository.claims.push(event);
    let cancelled = false;
    stream.append = (fields, signal) => {
      stream.appended.push(fields);
      signal.addEventListener(
        "abort",
        () => {
          cancelled = true;
          append.reject(new Error("cancelled command"));
        },
        { once: true },
      );
      return append.promise;
    };
    const timeoutScheduler: WorkerScheduler = {
      random: () => 0.25,
      sleep: () => timeout.promise,
    };
    const worker = new OutboxWorker(repository, stream, configuration, logger, timeoutScheduler);

    const cycle = worker.runCycle();
    await Promise.resolve();
    timeout.resolve();
    expect((await cycle).outcomes).toEqual(["retry_scheduled"]);
    expect(cancelled).toBe(true);
    expect(stream.resetCalls).toBe(1);
    expect(repository.failures[0]).toMatchObject({
      retryable: true,
      retryJitter: 0.25,
      errorCode: "REDIS_XADD_AMBIGUOUS",
      errorMessage: "Redis publication outcome is ambiguous.",
    });
    expect(repository.published).toEqual([]);
    append.resolve("late-accepted-id");
    await Promise.resolve();
    expect(repository.published).toEqual([]);

    const recovered = claim();
    repository.claims.push(recovered);
    stream.append = (fields) => {
      stream.appended.push(fields);
      return Promise.resolve("1501-0");
    };
    const recoveredWorker = new OutboxWorker(repository, stream, configuration, logger, scheduler);
    expect((await recoveredWorker.runCycle()).outcomes).toEqual(["published"]);
    expect(repository.published.at(-1)?.eventId).toBe(recovered.eventId);
  });

  it("cancels stalled commands before releasing bounded publication slots", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const timeout = deferred<void>();
    let inFlight = 0;
    let maximumInFlight = 0;
    let cancelled = 0;
    repository.claims.push(...Array.from({ length: 10 }, () => claim()));
    stream.append = (fields, signal) => {
      stream.appended.push(fields);
      inFlight += 1;
      maximumInFlight = Math.max(maximumInFlight, inFlight);
      return new Promise((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            inFlight -= 1;
            cancelled += 1;
            reject(new Error("cancelled stalled command"));
          },
          { once: true },
        );
      });
    };
    const timeoutScheduler: WorkerScheduler = {
      random: () => 0,
      sleep: () => timeout.promise,
    };
    const worker = new OutboxWorker(repository, stream, configuration, logger, timeoutScheduler);

    const cycle = worker.runCycle();
    while (inFlight < configuration.publishConcurrency) await Promise.resolve();
    timeout.resolve();
    const result = await cycle;

    expect(result.claimed).toBe(8);
    expect(result.outcomes).toEqual(Array.from({ length: 8 }, () => "retry_scheduled"));
    expect(maximumInFlight).toBe(8);
    expect(inFlight).toBe(0);
    expect(cancelled).toBe(8);
    expect(repository.published).toEqual([]);
    expect(repository.failures).toHaveLength(8);
    expect(repository.claims).toHaveLength(2);
  });

  it("treats connection loss during XADD as an ambiguous retryable outcome", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    repository.claims.push(claim());
    stream.results.push(new Error("socket closed after write"));
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    expect((await worker.runCycle()).outcomes).toEqual(["retry_scheduled"]);
    expect(repository.failures[0]).toMatchObject({
      retryable: true,
      errorCode: "REDIS_XADD_AMBIGUOUS",
      errorMessage: "Redis publication outcome is ambiguous.",
    });
    expect(repository.published).toEqual([]);
  });

  it("never finalizes malformed Redis results", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    repository.claims.push(claim());
    stream.results.push("not-a-stream-id");
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    expect((await worker.runCycle()).outcomes).toEqual(["retry_scheduled"]);
    expect(repository.published).toEqual([]);
    expect(repository.failures[0]).toMatchObject({
      retryable: true,
      errorCode: "INVALID_REDIS_ENTRY_ID",
    });
  });

  it("blocks an oversized strict envelope without calling XADD", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    repository.claims.push(claim());
    const worker = new OutboxWorker(
      repository,
      stream,
      { ...configuration, maximumEnvelopeBytes: 1 },
      logger,
      scheduler,
    );

    expect((await worker.runCycle()).outcomes).toEqual(["blocked"]);
    expect(stream.appended).toEqual([]);
    expect(repository.failures[0]).toMatchObject({
      retryable: false,
      errorCode: "DELIVERY_ENVELOPE_TOO_LARGE",
      errorMessage: "Delivery stream entry exceeds the configured byte limit.",
    });
  });

  it("accepts the maximum complete valid worker stream entry", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const event = claimWithEnvelopeBytes(DELIVERY_ENVELOPE_MAX_BYTES);
    repository.claims.push(event);
    stream.results.push("2000-1");
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    expect((await worker.runCycle()).outcomes).toEqual(["published"]);
    const fields = encodeDeliveryStreamFields(event.envelope);
    expect(validateDeliveryStreamEntrySize(fields)).toEqual({
      valid: true,
      entryBytes: DELIVERY_STREAM_ENTRY_MAX_BYTES,
    });
    expect(stream.appended).toEqual([fields]);
    expect(repository.published).toHaveLength(1);
  });

  it("rejects a complete entry one byte over its configured limit before XADD", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    repository.claims.push(claimWithEnvelopeBytes(DELIVERY_ENVELOPE_MAX_BYTES));
    const worker = new OutboxWorker(
      repository,
      stream,
      { ...configuration, maximumEnvelopeBytes: DELIVERY_ENVELOPE_MAX_BYTES - 1 },
      logger,
      scheduler,
    );

    expect((await worker.runCycle()).outcomes).toEqual(["blocked"]);
    expect(stream.appended).toEqual([]);
    expect(repository.published).toEqual([]);
    expect(repository.failures[0]).toMatchObject({
      retryable: false,
      errorCode: "DELIVERY_ENVELOPE_TOO_LARGE",
    });
  });

  it("rejects an oversized individual event field before XADD or publication finalization", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    repository.claims.push(claimWithEnvelopeBytes(DELIVERY_ENVELOPE_MAX_BYTES));
    const worker = new OutboxWorker(
      repository,
      stream,
      { ...configuration, maximumEnvelopeBytes: DELIVERY_ENVELOPE_MAX_BYTES - 1024 },
      logger,
      scheduler,
    );

    expect((await worker.runCycle()).outcomes).toEqual(["blocked"]);
    expect(stream.appended).toEqual([]);
    expect(repository.published).toEqual([]);
  });

  it("keeps a valid XADD positive when age trimming fails", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    repository.claims.push(claim());
    stream.results.push("2000-0");
    stream.trimFailure = true;
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    expect((await worker.runCycle()).outcomes).toEqual(["published"]);
    await worker.drain(10_000);
    expect(stream.trimCalls).toBe(1);
    expect(repository.published[0]?.publicationId).toBe("2000-0");
    expect(repository.failures).toEqual([]);
  });

  it("finalizes immediately after XADD without waiting for stalled age trimming", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const order: string[] = [];
    const trimStarted = deferred<void>();
    const trimFinished = deferred<void>();
    repository.claims.push(claim());
    stream.append = () => {
      order.push("xadd");
      return Promise.resolve("2001-0");
    };
    repository.markPublished = (input) => {
      order.push("mark-published");
      repository.published.push(input);
      return Promise.resolve(applied(input.eventId, "published"));
    };
    stream.trimByAge = () => {
      order.push("trim");
      trimStarted.resolve();
      return trimFinished.promise;
    };
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    await expect(worker.runCycle()).resolves.toMatchObject({ outcomes: ["published"] });
    await trimStarted.promise;
    expect(order).toEqual(["xadd", "mark-published", "trim"]);
    expect(repository.published).toHaveLength(1);
    trimFinished.resolve();
    await expect(worker.drain(10_000)).resolves.toBe(true);
  });

  it("allows only one retention-maintenance task to accumulate", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const trimFinished = deferred<void>();
    repository.claims.push(claim(), claim());
    stream.trimByAge = () => {
      stream.trimCalls += 1;
      return trimFinished.promise;
    };
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    await expect(worker.runCycle()).resolves.toMatchObject({
      claimed: 2,
      outcomes: ["published", "published"],
    });
    expect(stream.trimCalls).toBe(1);
    trimFinished.resolve();
    await expect(worker.drain(10_000)).resolves.toBe(true);
  });

  it("honors stale lease fencing after XADD", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const event = claim();
    repository.claims.push(event);
    repository.publishResult = { outcome: "stale", eventId: event.eventId };
    stream.results.push("3000-0");
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    expect((await worker.runCycle()).outcomes).toEqual(["stale"]);
    expect(repository.failures).toEqual([]);
  });

  it("caps claims and active Redis publications at eight", async () => {
    const repository = new FakeRepository();
    const gates = Array.from({ length: 8 }, () => deferred<unknown>());
    const allStarted = deferred<void>();
    let active = 0;
    let maximumActive = 0;
    const stream = new FakeStream();
    stream.append = (fields) => {
      stream.appended.push(fields);
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      if (stream.appended.length === 8) allStarted.resolve();
      const gate = gates[stream.appended.length - 1];
      if (!gate) throw new Error("Missing publication gate");
      return gate.promise.finally(() => {
        active -= 1;
      });
    };
    repository.claims.push(...Array.from({ length: 10 }, () => claim()));
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    const cycle = worker.runCycle();
    await allStarted.promise;
    expect(repository.claimOptions[0]?.batchSize).toBe(8);
    expect(stream.appended).toHaveLength(8);
    for (const [index, gate] of gates.entries()) gate.resolve(`${4000 + index}-0`);
    expect((await cycle).claimed).toBe(8);
    expect(maximumActive).toBe(8);
    expect(repository.claims).toHaveLength(2);
  });

  it("pauses claims while Redis is unavailable and resumes after readiness returns", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    stream.ready = false;
    repository.claims.push(claim());
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    await expect(worker.runCycle()).resolves.toMatchObject({
      state: "redis_unavailable",
      claimed: 0,
    });
    expect(repository.claimOptions).toEqual([]);
    stream.ready = true;
    await expect(worker.runCycle()).resolves.toMatchObject({ state: "processed", claimed: 1 });
  });

  it("uses bounded cancellable empty polling jitter", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const delays: number[] = [];
    const controller = new AbortController();
    const pollScheduler: WorkerScheduler = {
      random: () => 0.5,
      sleep: (delay) => {
        delays.push(delay);
        controller.abort();
        return Promise.resolve();
      },
    };
    const worker = new OutboxWorker(repository, stream, configuration, logger, pollScheduler);

    await worker.run(controller.signal);
    expect(repository.claimOptions).toHaveLength(1);
    expect(delays).toEqual([275]);
  });

  it("removes abort listeners and timers after repeated normal idle sleeps", async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const signal = controller.signal;
    const addEventListener = signal.addEventListener.bind(signal);
    const removeEventListener = signal.removeEventListener.bind(signal);
    let listenerCount = 0;
    signal.addEventListener = ((...arguments_: Parameters<AbortSignal["addEventListener"]>) => {
      listenerCount += 1;
      return addEventListener(...arguments_);
    }) as AbortSignal["addEventListener"];
    signal.removeEventListener = ((
      ...arguments_: Parameters<AbortSignal["removeEventListener"]>
    ) => {
      listenerCount -= 1;
      return removeEventListener(...arguments_);
    }) as AbortSignal["removeEventListener"];

    try {
      for (let index = 0; index < 100; index += 1) {
        const sleeping = systemWorkerScheduler.sleep(10, signal);
        await vi.advanceTimersByTimeAsync(10);
        await sleeping;
        expect(listenerCount).toBe(0);
        expect(vi.getTimerCount()).toBe(0);
      }
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops new claims and abandons unfinished work after the grace boundary", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const append = deferred<unknown>();
    const appendStarted = deferred<void>();
    const grace = deferred<void>();
    const publicationTimeout = deferred<void>();
    stream.append = (fields) => {
      stream.appended.push(fields);
      appendStarted.resolve();
      return append.promise;
    };
    repository.claims.push(claim(), claim());
    const shutdownScheduler: WorkerScheduler = {
      random: () => 0,
      sleep: (delay) => (delay === 10_000 ? grace.promise : publicationTimeout.promise),
    };
    const worker = new OutboxWorker(
      repository,
      stream,
      { ...configuration, publishConcurrency: 1 },
      logger,
      shutdownScheduler,
    );
    const cycle = worker.runCycle();
    await appendStarted.promise;
    worker.stopTakingClaims();
    const draining = worker.drain(10_000);
    grace.resolve();

    await expect(draining).resolves.toBe(false);
    worker.abandonActiveLeases();
    await expect(worker.runCycle()).resolves.toMatchObject({ state: "stopping", claimed: 0 });
    expect(repository.claimOptions).toHaveLength(1);
    expect(repository.failures).toEqual([]);
    append.resolve("5000-0");
    await cycle;
    expect(repository.published).toEqual([]);
  });

  it("waits for a pending claim that resolves inside shutdown grace and drains it", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const claimStarted = deferred<void>();
    const pendingClaim = deferred<ClaimedOutboxEvent[]>();
    const event = claim();
    const order: string[] = [];
    repository.claimAvailable = (options) => {
      repository.claimOptions.push(options);
      claimStarted.resolve();
      return pendingClaim.promise;
    };
    repository.markPublished = (input) => {
      order.push("mark-published");
      repository.published.push(input);
      return Promise.resolve(applied(input.eventId, "published"));
    };
    stream.close = () => {
      order.push("close-redis");
      stream.ready = false;
      return Promise.resolve();
    };
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);
    const lifecycle = new WorkerProcessLifecycle(
      worker,
      stream,
      { end: () => Promise.resolve(order.push("close-database")).then(() => undefined) },
      10_000,
      logger,
    );

    const cycle = worker.runCycle();
    await claimStarted.promise;
    const shutdown = lifecycle.shutdown("SIGTERM");
    pendingClaim.resolve([event]);

    await expect(cycle).resolves.toMatchObject({ claimed: 1, outcomes: ["published"] });
    await shutdown;
    expect(order).toEqual(["mark-published", "close-redis", "close-database"]);
    expect(repository.published[0]?.eventId).toBe(event.eventId);
  });

  it("fences a pending claim that resolves after grace and makes repeated shutdown idempotent", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    const claimStarted = deferred<void>();
    const pendingClaim = deferred<ClaimedOutboxEvent[]>();
    const graceExpired = deferred<void>();
    const event = claim();
    let closed = false;
    repository.claimAvailable = (options) => {
      repository.claimOptions.push(options);
      claimStarted.resolve();
      return pendingClaim.promise;
    };
    stream.append = () => {
      if (closed) throw new Error("used closed Redis dependency");
      return Promise.resolve("9000-0");
    };
    stream.close = () => {
      closed = true;
      stream.ready = false;
      return Promise.resolve();
    };
    const graceScheduler: WorkerScheduler = {
      random: () => 0,
      sleep: () => graceExpired.promise,
    };
    const worker = new OutboxWorker(repository, stream, configuration, logger, graceScheduler);
    const lifecycle = new WorkerProcessLifecycle(
      worker,
      stream,
      { end: () => Promise.resolve() },
      10_000,
      logger,
    );

    const cycle = worker.runCycle();
    await claimStarted.promise;
    const firstShutdown = lifecycle.shutdown("SIGINT");
    expect(lifecycle.shutdown("SIGTERM")).toBe(firstShutdown);
    graceExpired.resolve();
    await firstShutdown;
    pendingClaim.resolve([event]);

    await expect(cycle).resolves.toMatchObject({ state: "stopping", claimed: 1, outcomes: [] });
    expect(stream.appended).toEqual([]);
    expect(repository.published).toEqual([]);
    expect(repository.failures).toEqual([]);
    expect(repository.claimOptions).toHaveLength(1);
    await expect(worker.runCycle()).resolves.toMatchObject({ state: "stopping", claimed: 0 });
  });
});

describe("worker process lifecycle", () => {
  it("stops claims and force-closes dependencies when the shutdown grace expires", async () => {
    const stream = new FakeStream();
    let forceClose: boolean | undefined;
    stream.close = (force) => {
      forceClose = force;
      return Promise.resolve();
    };
    let stopped = false;
    let abandoned = false;
    let databaseClosed = false;
    const loop: WorkerLoop = {
      run: () => Promise.resolve(),
      stopTakingClaims: () => {
        stopped = true;
      },
      abandonActiveLeases: () => {
        abandoned = true;
      },
      drain: () => Promise.resolve(false),
    };
    const lifecycle = new WorkerProcessLifecycle(
      loop,
      stream,
      {
        end: () => {
          databaseClosed = true;
          return Promise.resolve();
        },
      },
      10_000,
      logger,
    );

    await lifecycle.shutdown("SIGTERM");
    expect(stopped).toBe(true);
    expect(abandoned).toBe(true);
    expect(forceClose).toBe(true);
    expect(databaseClosed).toBe(true);
  });
});
