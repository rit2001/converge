import { describe, expect, it } from "vitest";
import type {
  ClaimedOutboxEvent,
  ClaimOutboxOptions,
  LeaseMutationResult,
  MarkOutboxPublishedInput,
  RecordOutboxFailureInput,
} from "@converge/database";
import type { StructuredLogger } from "@converge/observability";
import {
  decodeDeliveryStreamFields,
  membershipRevokedDeliveryEnvelopeSchema,
  type DeliveryStreamFields,
} from "@converge/protocol";
import { RedisXaddRejectedError, type DeliveryStream } from "./redis-stream.js";
import { WorkerProcessLifecycle, type WorkerLoop } from "./lifecycle.js";
import {
  OutboxWorker,
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

  connect(): Promise<void> {
    this.ready = true;
    return Promise.resolve();
  }

  isReady(): boolean {
    return this.ready;
  }

  append(fields: DeliveryStreamFields): Promise<unknown> {
    this.appended.push(fields);
    const result = this.results.shift() ?? `${this.appended.length}-0`;
    return result instanceof Error ? Promise.reject(result) : Promise.resolve(result);
  }

  trimByAge(): Promise<void> {
    this.trimCalls += 1;
    return this.trimFailure ? Promise.reject(new Error("trim details")) : Promise.resolve();
  }

  close(force?: boolean): Promise<void> {
    void force;
    this.ready = false;
    return Promise.resolve();
  }
}

function deferred<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
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
    stream.append = (fields) => {
      order.push("xadd");
      return append(fields);
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
    stream.append = (fields) => {
      stream.appended.push(fields);
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
    expect(repository.failures[0]).toMatchObject({
      retryable: true,
      retryJitter: 0.25,
      errorCode: "REDIS_XADD_AMBIGUOUS",
      errorMessage: "Redis publication outcome is ambiguous.",
    });
    expect(repository.published).toEqual([]);
    append.resolve("1500-0");
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
      errorMessage: "Delivery envelope exceeds the configured byte limit.",
    });
  });

  it("keeps a valid XADD positive when age trimming fails", async () => {
    const repository = new FakeRepository();
    const stream = new FakeStream();
    repository.claims.push(claim());
    stream.results.push("2000-0");
    stream.trimFailure = true;
    const worker = new OutboxWorker(repository, stream, configuration, logger, scheduler);

    expect((await worker.runCycle()).outcomes).toEqual(["published"]);
    expect(stream.trimCalls).toBe(1);
    expect(repository.published[0]?.publicationId).toBe("2000-0");
    expect(repository.failures).toEqual([]);
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
