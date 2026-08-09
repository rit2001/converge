import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  RedisDeliveryConsumer,
  type DeliveryConsumerTransport,
  type DeliveryConsumerLifecycleEvent,
  type DeliveryContext,
} from "@converge/api/delivery-consumer";
import { RedisDeliveryConsumerTransport } from "@converge/api/redis-delivery-transport";
import {
  encodeDeliveryStreamFields,
  membershipRevokedDeliveryEnvelopeSchema,
} from "@converge/protocol";
import { createClient, type RedisClientType } from "redis";

const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const streamKeys = new Set<string>();
const consumers = new Set<RedisDeliveryConsumer>();
let publisher: RedisClientType;

class FailFirstReadTransport implements DeliveryConsumerTransport {
  private failNextRead = true;

  constructor(private readonly delegate: RedisDeliveryConsumerTransport) {}

  connect(): Promise<void> {
    return this.delegate.connect();
  }

  inspect(input: Parameters<DeliveryConsumerTransport["inspect"]>[0]) {
    return this.delegate.inspect(input);
  }

  readAfter(input: Parameters<DeliveryConsumerTransport["readAfter"]>[0]) {
    if (!this.failNextRead) return this.delegate.readAfter(input);
    this.failNextRead = false;
    input.onIssued();
    return new Promise<never>((_resolve, reject) => {
      queueMicrotask(() => queueMicrotask(() => reject(new Error("injected read disconnect"))));
    });
  }

  cancelRead(): Promise<void> {
    return this.delegate.cancelRead();
  }

  close(): Promise<void> {
    return this.delegate.close();
  }
}

function uniqueStreamKey(): string {
  const key = `converge:test:m24a:${randomUUID()}`;
  streamKeys.add(key);
  return key;
}

function envelope(deliverySeq: number) {
  return membershipRevokedDeliveryEnvelopeSchema.parse({
    schemaVersion: 1,
    eventId: randomUUID(),
    boardId: randomUUID(),
    deliverySeq,
    eventType: "board.membership.revoked",
    occurredAt: "2026-08-09T12:00:00.000Z",
    payload: { revokedUserId: randomUUID(), initiatedByUserId: randomUUID() },
  });
}

async function append(key: string, value: ReturnType<typeof envelope>): Promise<string> {
  return appendAt(key, "*", value);
}

function streamFieldArguments(value: ReturnType<typeof envelope>): string[] {
  const fields = encodeDeliveryStreamFields(value);
  return [
    "schemaVersion",
    fields.schemaVersion,
    "eventId",
    fields.eventId,
    "boardId",
    fields.boardId,
    "deliverySeq",
    fields.deliverySeq,
    "eventType",
    fields.eventType,
    "event",
    fields.event,
  ];
}

async function appendAt(
  key: string,
  id: string,
  value: ReturnType<typeof envelope>,
): Promise<string> {
  const result = await publisher.sendCommand(["XADD", key, id, ...streamFieldArguments(value)]);
  if (typeof result !== "string") throw new Error("Redis XADD did not return an entry ID");
  return result;
}

function deliveryGate(): {
  promise: Promise<DeliveryContext>;
  deliver: (context: DeliveryContext) => Promise<void>;
} {
  let resolve!: (context: DeliveryContext) => void;
  const promise = new Promise<DeliveryContext>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, deliver: (context) => Promise.resolve(resolve(context)) };
}

function cursorLossGate(): {
  promise: Promise<Extract<DeliveryConsumerLifecycleEvent, { state: "cursor_lost" }>>;
  lifecycle: (event: DeliveryConsumerLifecycleEvent) => void;
} {
  let resolve!: (event: Extract<DeliveryConsumerLifecycleEvent, { state: "cursor_lost" }>) => void;
  const promise = new Promise<Extract<DeliveryConsumerLifecycleEvent, { state: "cursor_lost" }>>(
    (resolvePromise) => {
      resolve = resolvePromise;
    },
  );
  return {
    promise,
    lifecycle: (event) => {
      if (event.state === "cursor_lost") resolve(event);
    },
  };
}

async function withDeadline<T>(promise: Promise<T>): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(
          () => reject(new Error("Real Redis consumer deadline expired")),
          5_000,
        );
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
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

beforeAll(async () => {
  publisher = createClient({ url: redisUrl });
  publisher.on("error", () => undefined);
  await publisher.connect();
});

afterEach(async () => {
  await Promise.all([...consumers].map((consumer) => consumer.stop()));
  consumers.clear();
  if (streamKeys.size > 0) await publisher.sendCommand(["DEL", ...streamKeys]);
  streamKeys.clear();
});

afterAll(() => {
  if (publisher?.isOpen) publisher.destroy();
});

describe("real Redis independent delivery consumers", () => {
  it("delivers the same post-tail entry to two independent plain-XREAD consumers", async () => {
    const key = uniqueStreamKey();
    const historical = envelope(1);
    const historicalId = await append(key, historical);
    const firstGate = deliveryGate();
    const secondGate = deliveryGate();
    const first = new RedisDeliveryConsumer(new RedisDeliveryConsumerTransport(redisUrl, key), {
      deliver: firstGate.deliver,
      quarantine: () => Promise.resolve(),
      lifecycle: () => undefined,
    });
    const second = new RedisDeliveryConsumer(new RedisDeliveryConsumerTransport(redisUrl, key), {
      deliver: secondGate.deliver,
      quarantine: () => Promise.resolve(),
      lifecycle: () => undefined,
    });
    consumers.add(first);
    consumers.add(second);

    await Promise.all([first.start(), second.start()]);
    expect(first.lastHandledCursor).toBe(historicalId);
    expect(second.lastHandledCursor).toBe(historicalId);

    const published = envelope(2);
    const publishedId = await append(key, published);
    const [firstDelivery, secondDelivery] = await Promise.all([
      withDeadline(firstGate.promise),
      withDeadline(secondGate.promise),
    ]);

    expect(firstDelivery).toEqual({ redisEntryId: publishedId, envelope: published });
    expect(secondDelivery).toEqual({ redisEntryId: publishedId, envelope: published });
    expect(firstDelivery.envelope.eventId).not.toBe(historical.eventId);
    expect(secondDelivery.envelope.eventId).not.toBe(historical.eventId);
  });

  it("preserves duplicate Redis fields long enough to fail closed", async () => {
    const key = uniqueStreamKey();
    let resolveError!: (event: DeliveryConsumerLifecycleEvent) => void;
    const lifecycleError = new Promise<DeliveryConsumerLifecycleEvent>((resolve) => {
      resolveError = resolve;
    });
    const delivered: DeliveryContext[] = [];
    const consumer = new RedisDeliveryConsumer(new RedisDeliveryConsumerTransport(redisUrl, key), {
      deliver: (context) => {
        delivered.push(context);
        return Promise.resolve();
      },
      quarantine: () => Promise.resolve(),
      lifecycle: (event) => {
        if (event.state === "error") resolveError(event);
      },
    });
    consumers.add(consumer);
    await consumer.start();
    const value = envelope(1);
    const fields = encodeDeliveryStreamFields(value);

    const entryId = await publisher.sendCommand([
      "XADD",
      key,
      "*",
      "schemaVersion",
      fields.schemaVersion,
      "eventId",
      fields.eventId,
      "eventId",
      fields.eventId,
      "boardId",
      fields.boardId,
      "deliverySeq",
      fields.deliverySeq,
      "eventType",
      fields.eventType,
      "event",
      fields.event,
    ]);
    const failure = await withDeadline(lifecycleError);

    expect(failure).toMatchObject({
      state: "error",
      entryId,
      code: "INVALID_STREAM_ENTRY",
      cursor: "0-0",
    });
    expect(consumer.lastHandledCursor).toBe("0-0");
    expect(delivered).toEqual([]);
  });

  it.each(["xdel", "xtrim", "recreate"] as const)(
    "detects healthy-stream %s continuity loss before delivering the later entry",
    async (operation) => {
      const key = uniqueStreamKey();
      await appendAt(key, "1-0", envelope(1));
      const delivered: DeliveryContext[] = [];
      const loss = cursorLossGate();
      const consumer = new RedisDeliveryConsumer(
        new RedisDeliveryConsumerTransport(redisUrl, key),
        {
          deliver: (context) => {
            delivered.push(context);
            return Promise.resolve();
          },
          quarantine: () => Promise.resolve(),
          lifecycle: loss.lifecycle,
        },
      );
      consumers.add(consumer);
      await consumer.start();
      expect(consumer.lastHandledCursor).toBe("1-0");

      if (operation === "xdel") {
        await publisher.sendCommand([
          "EVAL",
          "redis.call('XADD', KEYS[1], ARGV[1], unpack(ARGV, 3, 14)); redis.call('XDEL', KEYS[1], ARGV[1]); return redis.call('XADD', KEYS[1], ARGV[2], unpack(ARGV, 15, 26))",
          "1",
          key,
          "2-0",
          "3-0",
          ...streamFieldArguments(envelope(2)),
          ...streamFieldArguments(envelope(3)),
        ]);
      } else if (operation === "xtrim") {
        await publisher.sendCommand([
          "EVAL",
          "redis.call('XADD', KEYS[1], ARGV[1], unpack(ARGV, 3, 14)); redis.call('XADD', KEYS[1], ARGV[2], unpack(ARGV, 15, 26)); redis.call('XTRIM', KEYS[1], 'MINID', ARGV[2]); return ARGV[2]",
          "1",
          key,
          "2-0",
          "3-0",
          ...streamFieldArguments(envelope(2)),
          ...streamFieldArguments(envelope(3)),
        ]);
      } else {
        await publisher.sendCommand([
          "EVAL",
          "redis.call('DEL', KEYS[1]); return redis.call('XADD', KEYS[1], ARGV[1], unpack(ARGV, 2, 13))",
          "1",
          key,
          "3-0",
          ...streamFieldArguments(envelope(3)),
        ]);
      }

      const failure = await withDeadline(loss.promise);
      expect(failure.cursor).toBe("1-0");
      expect(failure.reason).toBe(
        operation === "recreate" ? "STREAM_RECREATED" : "TRIMMED_BEYOND_CURSOR",
      );
      expect(consumer.lastHandledCursor).toBe("1-0");
      expect(delivered).toEqual([]);
    },
  );

  it.each([
    ["newest entry", false],
    ["all entries", true],
  ] as const)("starts from last-generated ID after deleting %s", async (_name, deleteAll) => {
    const key = uniqueStreamKey();
    await appendAt(key, "1-0", envelope(1));
    await appendAt(key, "2-0", envelope(2));
    await publisher.sendCommand(["XDEL", key, ...(deleteAll ? ["1-0", "2-0"] : ["2-0"])]);
    let resolveRecovered!: (
      event: Extract<DeliveryConsumerLifecycleEvent, { state: "recovered" }>,
    ) => void;
    const recovered = new Promise<Extract<DeliveryConsumerLifecycleEvent, { state: "recovered" }>>(
      (resolve) => {
        resolveRecovered = resolve;
      },
    );
    const lifecycle: DeliveryConsumerLifecycleEvent[] = [];
    const consumer = new RedisDeliveryConsumer(
      new FailFirstReadTransport(new RedisDeliveryConsumerTransport(redisUrl, key)),
      {
        deliver: () => Promise.resolve(),
        quarantine: () => Promise.resolve(),
        lifecycle: (event) => {
          lifecycle.push(event);
          if (event.state === "recovered") resolveRecovered(event);
        },
      },
    );
    consumers.add(consumer);

    await consumer.start();
    expect(consumer.lastHandledCursor).toBe("2-0");
    expect(await withDeadline(recovered)).toEqual({
      state: "recovered",
      cursor: "2-0",
      recoveryTail: "2-0",
    });
    expect(lifecycle.some(({ state }) => state === "cursor_lost")).toBe(false);
    expect(consumer.lastHandledCursor).toBe("2-0");
  });

  it.each(["xtrim", "xdel"] as const)(
    "delivers the first post-start entry after an existing stream was emptied with %s",
    async (operation) => {
      const key = uniqueStreamKey();
      await appendAt(key, "1-0", envelope(1));
      await appendAt(key, "2-0", envelope(2));
      if (operation === "xtrim") await publisher.sendCommand(["XTRIM", key, "MAXLEN", "0"]);
      else await publisher.sendCommand(["XDEL", key, "1-0", "2-0"]);

      let resolveOutcome!: (outcome: "delivered" | "cursor_lost") => void;
      const outcome = new Promise<"delivered" | "cursor_lost">((resolve) => {
        resolveOutcome = resolve;
      });
      const delivered: DeliveryContext[] = [];
      const consumer = new RedisDeliveryConsumer(
        new RedisDeliveryConsumerTransport(redisUrl, key),
        {
          deliver: (context) => {
            delivered.push(context);
            resolveOutcome("delivered");
            return Promise.resolve();
          },
          quarantine: () => Promise.resolve(),
          lifecycle: (event) => {
            if (event.state === "cursor_lost") resolveOutcome("cursor_lost");
          },
        },
      );
      consumers.add(consumer);
      await consumer.start();
      expect(consumer.lastHandledCursor).toBe("2-0");

      const value = envelope(3);
      const id = await appendAt(key, "3-0", value);
      expect(await withDeadline(outcome)).toBe("delivered");
      expect(delivered).toEqual([{ redisEntryId: id, envelope: value }]);
      await eventually(() => expect(consumer.lastHandledCursor).toBe("3-0"));
    },
  );

  it("delivers multiple post-start entries in order after a completely trimmed stream", async () => {
    const key = uniqueStreamKey();
    await appendAt(key, "1-0", envelope(1));
    await appendAt(key, "2-0", envelope(2));
    await publisher.sendCommand(["XTRIM", key, "MAXLEN", "0"]);
    let resolveOutcome!: (outcome: "delivered" | "cursor_lost") => void;
    const outcome = new Promise<"delivered" | "cursor_lost">((resolve) => {
      resolveOutcome = resolve;
    });
    const delivered: DeliveryContext[] = [];
    const consumer = new RedisDeliveryConsumer(new RedisDeliveryConsumerTransport(redisUrl, key), {
      deliver: (context) => {
        delivered.push(context);
        if (delivered.length === 2) resolveOutcome("delivered");
        return Promise.resolve();
      },
      quarantine: () => Promise.resolve(),
      lifecycle: (event) => {
        if (event.state === "cursor_lost") resolveOutcome("cursor_lost");
      },
    });
    consumers.add(consumer);
    await consumer.start();

    const first = envelope(3);
    const second = envelope(4);
    await publisher.sendCommand([
      "EVAL",
      "redis.call('XADD', KEYS[1], ARGV[1], unpack(ARGV, 3, 14)); return redis.call('XADD', KEYS[1], ARGV[2], unpack(ARGV, 15, 26))",
      "1",
      key,
      "3-0",
      "4-0",
      ...streamFieldArguments(first),
      ...streamFieldArguments(second),
    ]);

    expect(await withDeadline(outcome)).toBe("delivered");
    expect(delivered.map(({ envelope: value }) => value.eventId)).toEqual([
      first.eventId,
      second.eventId,
    ]);
    await eventually(() => expect(consumer.lastHandledCursor).toBe("4-0"));
  });

  it.each(["xdel", "xtrim", "recreate"] as const)(
    "fails closed when an unread post-boundary entry is lost through %s",
    async (operation) => {
      const key = uniqueStreamKey();
      await appendAt(key, "1-0", envelope(1));
      await appendAt(key, "2-0", envelope(2));
      await publisher.sendCommand(["XTRIM", key, "MAXLEN", "0"]);
      const delivered: DeliveryContext[] = [];
      const loss = cursorLossGate();
      const consumer = new RedisDeliveryConsumer(
        new RedisDeliveryConsumerTransport(redisUrl, key),
        {
          deliver: (context) => {
            delivered.push(context);
            return Promise.resolve();
          },
          quarantine: () => Promise.resolve(),
          lifecycle: loss.lifecycle,
        },
      );
      consumers.add(consumer);
      await consumer.start();

      if (operation === "xdel") {
        await publisher.sendCommand([
          "EVAL",
          "redis.call('XADD', KEYS[1], ARGV[1], unpack(ARGV, 3, 14)); redis.call('XDEL', KEYS[1], ARGV[1]); return redis.call('XADD', KEYS[1], ARGV[2], unpack(ARGV, 15, 26))",
          "1",
          key,
          "3-0",
          "4-0",
          ...streamFieldArguments(envelope(3)),
          ...streamFieldArguments(envelope(4)),
        ]);
      } else if (operation === "xtrim") {
        await publisher.sendCommand([
          "EVAL",
          "redis.call('XADD', KEYS[1], ARGV[1], unpack(ARGV, 3, 14)); redis.call('XADD', KEYS[1], ARGV[2], unpack(ARGV, 15, 26)); redis.call('XTRIM', KEYS[1], 'MINID', ARGV[2]); return ARGV[2]",
          "1",
          key,
          "3-0",
          "4-0",
          ...streamFieldArguments(envelope(3)),
          ...streamFieldArguments(envelope(4)),
        ]);
      } else {
        await publisher.sendCommand([
          "EVAL",
          "redis.call('DEL', KEYS[1]); return redis.call('XADD', KEYS[1], ARGV[1], unpack(ARGV, 2, 13))",
          "1",
          key,
          "4-0",
          ...streamFieldArguments(envelope(4)),
        ]);
      }

      const failure = await withDeadline(loss.promise);
      expect(failure.cursor).toBe("2-0");
      expect(consumer.lastHandledCursor).toBe("2-0");
      expect(delivered).toEqual([]);
    },
  );
});
