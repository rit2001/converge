import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  RedisDeliveryConsumer,
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
  const fields = encodeDeliveryStreamFields(value);
  const result = await publisher.sendCommand([
    "XADD",
    key,
    "*",
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
  ]);
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
});
