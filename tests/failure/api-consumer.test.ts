import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  RedisDeliveryConsumer,
  type DeliveryConsumerTransport,
  type DeliveryConsumerLifecycleEvent,
  type DeliveryContext,
} from "@converge/api/delivery-consumer";
import {
  DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
  DELIVERY_STREAM_INITIALIZATION_TYPE,
  DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
  RedisDeliveryConsumerTransport,
} from "@converge/api/redis-delivery-transport";
import {
  encodeDeliveryStreamFields,
  membershipRevokedDeliveryEnvelopeSchema,
} from "@converge/protocol";
import { createClient, type RedisClientType } from "redis";

const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const streamKeys = new Set<string>();
const aclUsers = new Set<string>();
const consumers = new Set<RedisDeliveryConsumer>();
let publisher: RedisClientType;

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

class FailFirstReadTransport implements DeliveryConsumerTransport {
  private failNextRead = true;

  constructor(private readonly delegate: RedisDeliveryConsumerTransport) {}

  connect(): Promise<void> {
    return this.delegate.connect();
  }

  initializeStream(input: Parameters<DeliveryConsumerTransport["initializeStream"]>[0]) {
    return this.delegate.initializeStream(input);
  }

  verifyInitialization(input: Parameters<DeliveryConsumerTransport["verifyInitialization"]>[0]) {
    return this.delegate.verifyInitialization(input);
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

class ControlledFirstReadFailureTransport implements DeliveryConsumerTransport {
  private readonly firstRead = deferred<readonly never[]>();
  private useInjectedRead = true;

  constructor(private readonly delegate: RedisDeliveryConsumerTransport) {}

  connect(): Promise<void> {
    return this.delegate.connect();
  }

  initializeStream(input: Parameters<DeliveryConsumerTransport["initializeStream"]>[0]) {
    return this.delegate.initializeStream(input);
  }

  verifyInitialization(input: Parameters<DeliveryConsumerTransport["verifyInitialization"]>[0]) {
    return this.delegate.verifyInitialization(input);
  }

  inspect(input: Parameters<DeliveryConsumerTransport["inspect"]>[0]) {
    return this.delegate.inspect(input);
  }

  readAfter(input: Parameters<DeliveryConsumerTransport["readAfter"]>[0]) {
    if (!this.useInjectedRead) return this.delegate.readAfter(input);
    this.useInjectedRead = false;
    input.onIssued();
    return this.firstRead.promise;
  }

  failFirstRead(): void {
    this.firstRead.reject(new Error("injected read disconnect"));
  }

  cancelRead(): Promise<void> {
    return this.delegate.cancelRead();
  }

  close(): Promise<void> {
    return this.delegate.close();
  }
}

class FirstReadBarrierTransport implements DeliveryConsumerTransport {
  private readonly entered = deferred<void>();
  private readonly released = deferred<void>();
  private firstRead = true;

  constructor(private readonly delegate: RedisDeliveryConsumerTransport) {}

  connect(): Promise<void> {
    return this.delegate.connect();
  }

  initializeStream(input: Parameters<DeliveryConsumerTransport["initializeStream"]>[0]) {
    return this.delegate.initializeStream(input);
  }

  verifyInitialization(input: Parameters<DeliveryConsumerTransport["verifyInitialization"]>[0]) {
    return this.delegate.verifyInitialization(input);
  }

  inspect(input: Parameters<DeliveryConsumerTransport["inspect"]>[0]) {
    return this.delegate.inspect(input);
  }

  async readAfter(input: Parameters<DeliveryConsumerTransport["readAfter"]>[0]) {
    if (this.firstRead) {
      this.firstRead = false;
      this.entered.resolve();
      await this.released.promise;
    }
    return this.delegate.readAfter(input);
  }

  waitUntilEntered(): Promise<void> {
    return this.entered.promise;
  }

  release(): void {
    this.released.resolve();
  }

  cancelRead(): Promise<void> {
    return this.delegate.cancelRead();
  }

  close(): Promise<void> {
    this.release();
    return this.delegate.close();
  }
}

class InitializationVerificationBarrierTransport implements DeliveryConsumerTransport {
  private readonly entered = deferred<void>();
  private readonly released = deferred<void>();

  constructor(private readonly delegate: RedisDeliveryConsumerTransport) {}

  connect(): Promise<void> {
    return this.delegate.connect();
  }

  initializeStream(input: Parameters<DeliveryConsumerTransport["initializeStream"]>[0]) {
    return this.delegate.initializeStream(input);
  }

  async verifyInitialization(
    input: Parameters<DeliveryConsumerTransport["verifyInitialization"]>[0],
  ) {
    this.entered.resolve();
    await this.released.promise;
    return this.delegate.verifyInitialization(input);
  }

  inspect(input: Parameters<DeliveryConsumerTransport["inspect"]>[0]) {
    return this.delegate.inspect(input);
  }

  readAfter(input: Parameters<DeliveryConsumerTransport["readAfter"]>[0]) {
    return this.delegate.readAfter(input);
  }

  waitUntilEntered(): Promise<void> {
    return this.entered.promise;
  }

  release(): void {
    this.released.resolve();
  }

  cancelRead(): Promise<void> {
    return this.delegate.cancelRead();
  }

  close(): Promise<void> {
    this.release();
    return this.delegate.close();
  }
}

class FirstPostReadInspectionBarrierTransport implements DeliveryConsumerTransport {
  private readonly entered = deferred<void>();
  private readonly released = deferred<void>();
  private inspectionCount = 0;

  constructor(private readonly delegate: RedisDeliveryConsumerTransport) {}

  connect(): Promise<void> {
    return this.delegate.connect();
  }

  initializeStream(input: Parameters<DeliveryConsumerTransport["initializeStream"]>[0]) {
    return this.delegate.initializeStream(input);
  }

  verifyInitialization(input: Parameters<DeliveryConsumerTransport["verifyInitialization"]>[0]) {
    return this.delegate.verifyInitialization(input);
  }

  async inspect(input: Parameters<DeliveryConsumerTransport["inspect"]>[0]) {
    this.inspectionCount += 1;
    if (this.inspectionCount === 2) {
      this.entered.resolve();
      await this.released.promise;
    }
    return this.delegate.inspect(input);
  }

  readAfter(input: Parameters<DeliveryConsumerTransport["readAfter"]>[0]) {
    return this.delegate.readAfter(input);
  }

  waitUntilEntered(): Promise<void> {
    return this.entered.promise;
  }

  release(): void {
    this.released.resolve();
  }

  cancelRead(): Promise<void> {
    return this.delegate.cancelRead();
  }

  close(): Promise<void> {
    this.release();
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

async function rawStreamEntries(
  key: string,
): Promise<readonly { id: string; fields: readonly string[] }[]> {
  const response = await publisher.sendCommand(["XRANGE", key, "-", "+"]);
  if (!Array.isArray(response)) throw new Error("Redis XRANGE returned an invalid response");
  return response.map((raw) => {
    if (!Array.isArray(raw) || raw.length !== 2 || typeof raw[0] !== "string")
      throw new Error("Redis XRANGE returned an invalid entry");
    if (!Array.isArray(raw[1]) || raw[1].some((field) => typeof field !== "string"))
      throw new Error("Redis XRANGE returned invalid fields");
    return { id: raw[0], fields: raw[1] as string[] };
  });
}

async function createRestrictedApiRedisUrl(
  key: string,
  options: { allowSentinelWrite?: boolean } = {},
): Promise<string> {
  const username = `converge_m24a_${randomUUID().replaceAll("-", "")}`;
  const password = randomUUID().replaceAll("-", "");
  aclUsers.add(username);
  await publisher.sendCommand([
    "ACL",
    "SETUSER",
    username,
    "reset",
    "on",
    `>${password}`,
    `~${key}`,
    "+ping",
    "+hello",
    "+client|setinfo",
    "+eval",
    "+info",
    "+exists",
    "+xinfo",
    "+xrange",
    "+xread",
    options.allowSentinelWrite === false ? "-xadd" : "+xadd",
    "-xgroup",
  ]);
  const restricted = new URL(redisUrl);
  restricted.username = username;
  restricted.password = password;
  return restricted.toString();
}

async function consumerGroups(key: string): Promise<unknown[]> {
  const groups = await publisher.sendCommand(["XINFO", "GROUPS", key]);
  if (!Array.isArray(groups)) throw new Error("Redis XINFO GROUPS returned invalid evidence");
  return groups;
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
  if (aclUsers.size > 0) await publisher.sendCommand(["ACL", "DELUSER", ...aclUsers]);
  aclUsers.clear();
});

afterAll(() => {
  if (publisher?.isOpen) publisher.destroy();
});

describe("real Redis independent delivery consumers", () => {
  it("rejects oversized malformed first/last XINFO evidence with a bounded diagnostic", async () => {
    const key = uniqueStreamKey();
    const oversized = `must-not-cross-js:${"x".repeat(512 * 1024)}`;
    await publisher.sendCommand([
      "XADD",
      key,
      "1-0",
      DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
      DELIVERY_STREAM_INITIALIZATION_TYPE,
      DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
      oversized,
    ]);
    await publisher.sendCommand(["XADD", key, "2-0", "malformed", oversized]);
    const transport = new RedisDeliveryConsumerTransport(redisUrl, key);

    try {
      await transport.connect();
      let initializationError: unknown;
      try {
        await transport.initializeStream({
          generationToken: "10000000-0000-4000-8000-000000000001",
          signal: new AbortController().signal,
        });
      } catch (error) {
        initializationError = error;
      }
      expect(initializationError).toBeInstanceOf(Error);
      expect((initializationError as Error).message).toBe(
        "Redis stream initializer returned invalid sentinel evidence",
      );
      expect((initializationError as Error).message.length).toBeLessThan(100);

      let inspectionError: unknown;
      try {
        await transport.inspect({ signal: new AbortController().signal });
      } catch (error) {
        inspectionError = error;
      }
      expect(inspectionError).toBeInstanceOf(Error);
      expect((inspectionError as Error).message).toBe(
        "Redis XINFO STREAM returned invalid bounded evidence",
      );
      expect((inspectionError as Error).message.length).toBeLessThan(100);
      expect((inspectionError as Error).message).not.toContain("must-not-cross-js");
    } finally {
      await transport.close();
    }
  });

  it("preserves XINFO entries-added above Number.MAX_SAFE_INTEGER", async () => {
    const key = uniqueStreamKey();
    await appendAt(key, "1-0", envelope(1));
    await publisher.sendCommand(["XSETID", key, "1-0", "ENTRIESADDED", "9007199254740993"]);
    const transport = new RedisDeliveryConsumerTransport(redisUrl, key);

    try {
      await transport.connect();
      const metadata = await transport.inspect({ signal: new AbortController().signal });
      expect(metadata.entriesAdded).toBe("9007199254740993");
    } finally {
      await transport.close();
    }
  });

  it("keeps exact high-counter inspection compatible with an API ACL that denies XGROUP", async () => {
    const key = uniqueStreamKey();
    await appendAt(key, "1-0", envelope(1));
    await publisher.sendCommand(["XSETID", key, "1-0", "ENTRIESADDED", "9007199254740993"]);
    const restrictedRedisUrl = await createRestrictedApiRedisUrl(key, {
      allowSentinelWrite: false,
    });
    const transport = new RedisDeliveryConsumerTransport(restrictedRedisUrl, key);

    expect(await consumerGroups(key)).toEqual([]);
    try {
      await transport.connect();
      await expect(
        transport.inspect({ signal: new AbortController().signal }),
      ).resolves.toMatchObject({ entriesAdded: "9007199254740993" });
    } finally {
      await transport.close();
    }
    expect(await consumerGroups(key)).toEqual([]);
  });

  it("issues no XGROUP mutation while inspecting an exact high counter", async () => {
    const key = uniqueStreamKey();
    await appendAt(key, "1-0", envelope(1));
    await publisher.sendCommand(["XSETID", key, "1-0", "ENTRIESADDED", "9007199254740993"]);
    const monitored: string[] = [];
    const monitor = createClient({ url: redisUrl });
    monitor.on("error", () => undefined);
    const transport = new RedisDeliveryConsumerTransport(redisUrl, key);

    try {
      await monitor.connect();
      await monitor.monitor((reply) => monitored.push(String(reply)));
      await transport.connect();
      await transport.inspect({ signal: new AbortController().signal });
      await new Promise<void>((resolve) => setImmediate(resolve));
      await monitor.reset();
    } finally {
      if (monitor.isOpen) monitor.destroy();
      await transport.close();
    }

    const commandsForKey = monitored.filter((command) => command.includes(key));
    expect(
      commandsForKey.filter((command) => /"XGROUP" "(?:CREATE|DESTROY)"/i.test(command)),
    ).toEqual([]);
    expect(await consumerGroups(key)).toEqual([]);
  });

  it("uses only the fixed sentinel write when a restricted API initializes an absent stream", async () => {
    const key = uniqueStreamKey();
    const restrictedRedisUrl = await createRestrictedApiRedisUrl(key);
    const monitored: string[] = [];
    const monitor = createClient({ url: redisUrl });
    monitor.on("error", () => undefined);
    const consumer = new RedisDeliveryConsumer(
      new RedisDeliveryConsumerTransport(restrictedRedisUrl, key),
      {
        deliver: () => Promise.resolve(),
        quarantine: () => Promise.resolve(),
        lifecycle: () => undefined,
      },
    );
    consumers.add(consumer);

    try {
      await monitor.connect();
      await monitor.monitor((reply) => monitored.push(String(reply)));
      await consumer.start();
      await new Promise<void>((resolve) => setImmediate(resolve));
      await monitor.reset();
    } finally {
      if (monitor.isOpen) monitor.destroy();
    }

    const entries = await rawStreamEntries(key);
    expect(entries).toHaveLength(1);
    expect(entries[0]?.fields).toEqual([
      DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
      DELIVERY_STREAM_INITIALIZATION_TYPE,
      DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
      expect.stringMatching(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      ),
    ]);
    const luaWrites = monitored.filter(
      (command) =>
        command.includes(key) &&
        command.includes("[0 lua]") &&
        /"(?:XADD|XGROUP|XSETID|XDEL|XTRIM|DEL|SET)"/i.test(command),
    );
    expect(luaWrites).toHaveLength(1);
    expect(luaWrites[0]).toMatch(/"XADD"/i);
    expect(await consumerGroups(key)).toEqual([]);
  });

  it("starts, recovers, and resumes delivery with XGROUP denied", async () => {
    const key = uniqueStreamKey();
    const historicalId = await appendAt(key, "1-0", envelope(1));
    const restrictedRedisUrl = await createRestrictedApiRedisUrl(key, {
      allowSentinelWrite: false,
    });
    const transport = new ControlledFirstReadFailureTransport(
      new RedisDeliveryConsumerTransport(restrictedRedisUrl, key),
    );
    const delivered = deliveryGate();
    const recovered = deferred<void>();
    const lifecycle: DeliveryConsumerLifecycleEvent[] = [];
    const consumer = new RedisDeliveryConsumer(transport, {
      deliver: delivered.deliver,
      quarantine: () => Promise.resolve(),
      lifecycle: (event) => {
        lifecycle.push(event);
        if (event.state === "recovered") recovered.resolve();
      },
    });
    consumers.add(consumer);

    await consumer.start();
    expect(consumer.lastHandledCursor).toBe(historicalId);
    transport.failFirstRead();
    await withDeadline(recovered.promise);
    const value = envelope(2);
    const id = await append(key, value);
    await expect(withDeadline(delivered.promise)).resolves.toEqual({
      redisEntryId: id,
      envelope: value,
    });

    expect(lifecycle.map(({ state }) => state)).toEqual([
      "starting",
      "established",
      "unavailable",
      "recovering",
      "recovered",
    ]);
    expect(await consumerGroups(key)).toEqual([]);
  });

  it("atomically initializes an absent stream once for two independent consumers", async () => {
    const key = uniqueStreamKey();
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
    const initialized = await rawStreamEntries(key);
    expect(initialized).toHaveLength(1);
    expect(initialized[0]?.fields).toEqual([
      DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
      DELIVERY_STREAM_INITIALIZATION_TYPE,
      DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
      expect.stringMatching(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      ),
    ]);
    expect(first.lastHandledCursor).toBe(initialized[0]?.id);
    expect(second.lastHandledCursor).toBe(initialized[0]?.id);

    const published = envelope(1);
    const publishedId = await append(key, published);
    const [firstDelivery, secondDelivery] = await Promise.all([
      withDeadline(firstGate.promise),
      withDeadline(secondGate.promise),
    ]);
    expect(firstDelivery).toEqual({ redisEntryId: publishedId, envelope: published });
    expect(secondDelivery).toEqual({ redisEntryId: publishedId, envelope: published });
  });

  it.each([
    [
      "non-UUID",
      [
        DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
        DELIVERY_STREAM_INITIALIZATION_TYPE,
        DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
        "not-a-uuid",
      ],
    ],
    [
      "uppercase UUID",
      [
        DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
        DELIVERY_STREAM_INITIALIZATION_TYPE,
        DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
        "10000000-0000-4000-8000-00000000000A",
      ],
    ],
    [
      "empty generation",
      [
        DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
        DELIVERY_STREAM_INITIALIZATION_TYPE,
        DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
        "",
      ],
    ],
    [
      "whitespace generation",
      [
        DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
        DELIVERY_STREAM_INITIALIZATION_TYPE,
        DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
        " 10000000-0000-4000-8000-000000000001",
      ],
    ],
    [
      "oversized generation",
      [
        DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
        DELIVERY_STREAM_INITIALIZATION_TYPE,
        DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
        "a".repeat(100_000),
      ],
    ],
    [
      "duplicate generation",
      [
        DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
        DELIVERY_STREAM_INITIALIZATION_TYPE,
        DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
        "10000000-0000-4000-8000-000000000001",
        DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
        "10000000-0000-4000-8000-000000000001",
      ],
    ],
    [
      "reordered fields",
      [
        DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
        "10000000-0000-4000-8000-000000000001",
        DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
        DELIVERY_STREAM_INITIALIZATION_TYPE,
      ],
    ],
    [
      "unknown sentinel field",
      [
        DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
        DELIVERY_STREAM_INITIALIZATION_TYPE,
        DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
        "10000000-0000-4000-8000-000000000001",
        "unexpected",
        "value",
      ],
    ],
  ] as const)("fails startup on Redis sentinel evidence with %s", async (_name, fields) => {
    const key = uniqueStreamKey();
    await publisher.sendCommand(["XADD", key, "*", ...fields]);
    const delivered: DeliveryContext[] = [];
    const quarantined: unknown[] = [];
    const lifecycle: DeliveryConsumerLifecycleEvent[] = [];
    const consumer = new RedisDeliveryConsumer(new RedisDeliveryConsumerTransport(redisUrl, key), {
      deliver: (context) => {
        delivered.push(context);
        return Promise.resolve();
      },
      quarantine: (event) => {
        quarantined.push(event);
        return Promise.resolve();
      },
      lifecycle: (event) => {
        lifecycle.push(event);
      },
    });
    consumers.add(consumer);

    await expect(consumer.start()).rejects.toThrow("invalid sentinel evidence");
    expect(delivered).toEqual([]);
    expect(quarantined).toEqual([]);
    expect(lifecycle.some(({ state }) => state === "established")).toBe(false);
  });

  it("fails startup when its initialization sentinel is deleted before validation", async () => {
    const key = uniqueStreamKey();
    const transport = new InitializationVerificationBarrierTransport(
      new RedisDeliveryConsumerTransport(redisUrl, key),
    );
    const lifecycle: DeliveryConsumerLifecycleEvent[] = [];
    const consumer = new RedisDeliveryConsumer(transport, {
      deliver: () => Promise.resolve(),
      quarantine: () => Promise.resolve(),
      lifecycle: (event) => {
        lifecycle.push(event);
      },
    });
    consumers.add(consumer);

    const startup = consumer.start();
    await withDeadline(transport.waitUntilEntered());
    await publisher.sendCommand(["DEL", key]);
    transport.release();

    await expect(startup).rejects.toThrow("STREAM_INITIALIZATION_FAILED");
    expect(lifecycle.some(({ state }) => state === "established")).toBe(false);
    expect(lifecycle.at(-1)).toMatchObject({
      state: "error",
      code: "STREAM_INITIALIZATION_FAILED",
    });
  });

  it("rejects a stale first-read result when the initialized stream is recreated before inspection", async () => {
    const key = uniqueStreamKey();
    const transport = new FirstPostReadInspectionBarrierTransport(
      new RedisDeliveryConsumerTransport(redisUrl, key),
    );
    const delivered: DeliveryContext[] = [];
    const loss = cursorLossGate();
    const consumer = new RedisDeliveryConsumer(transport, {
      deliver: (context) => {
        delivered.push(context);
        return Promise.resolve();
      },
      quarantine: () => Promise.resolve(),
      lifecycle: loss.lifecycle,
    });
    consumers.add(consumer);

    await consumer.start();
    const startupCursor = consumer.lastHandledCursor;
    await append(key, envelope(1));
    await withDeadline(transport.waitUntilEntered());
    await publisher.sendCommand(["DEL", key]);
    await append(key, envelope(2));
    transport.release();

    await expect(withDeadline(loss.promise)).resolves.toMatchObject({
      state: "cursor_lost",
      cursor: startupCursor,
      reason: "STREAM_RECREATED",
    });
    expect(delivered).toEqual([]);
    expect(consumer.lastHandledCursor).toBe(startupCursor);
  });

  it("accepts later delivery after the initialization sentinel is trimmed behind the cursor", async () => {
    const key = uniqueStreamKey();
    const firstObserved = deferred<DeliveryContext>();
    const secondObserved = deferred<DeliveryContext>();
    const delivered: DeliveryContext[] = [];
    const lifecycle: DeliveryConsumerLifecycleEvent[] = [];
    const consumer = new RedisDeliveryConsumer(new RedisDeliveryConsumerTransport(redisUrl, key), {
      deliver: (context) => {
        delivered.push(context);
        (delivered.length === 1 ? firstObserved : secondObserved).resolve(context);
        return Promise.resolve();
      },
      quarantine: () => Promise.resolve(),
      lifecycle: (event) => {
        lifecycle.push(event);
      },
    });
    consumers.add(consumer);

    await consumer.start();
    const sentinelId = consumer.lastHandledCursor;
    const first = envelope(1);
    const firstId = await append(key, first);
    await withDeadline(firstObserved.promise);
    await eventually(() => expect(consumer.lastHandledCursor).toBe(firstId));

    await publisher.sendCommand(["XTRIM", key, "MINID", firstId]);
    const second = envelope(2);
    const secondId = await append(key, second);
    await withDeadline(secondObserved.promise);
    await eventually(() => expect(consumer.lastHandledCursor).toBe(secondId));

    expect(delivered).toEqual([
      { redisEntryId: firstId, envelope: first },
      { redisEntryId: secondId, envelope: second },
    ]);
    expect((await rawStreamEntries(key)).some(({ id }) => id === sentinelId)).toBe(false);
    expect(lifecycle.some(({ state }) => state === "cursor_lost")).toBe(false);
  });

  it("delivers an append between final startup inspection and first XREAD delegation exactly once", async () => {
    const key = uniqueStreamKey();
    const transport = new FirstReadBarrierTransport(
      new RedisDeliveryConsumerTransport(redisUrl, key),
    );
    const observed = deferred<DeliveryContext>();
    const delivered: DeliveryContext[] = [];
    const quarantined: unknown[] = [];
    const lifecycle: DeliveryConsumerLifecycleEvent[] = [];
    const consumer = new RedisDeliveryConsumer(transport, {
      deliver: (context) => {
        delivered.push(context);
        observed.resolve(context);
        return Promise.resolve();
      },
      quarantine: (event) => {
        quarantined.push(event);
        return Promise.resolve();
      },
      lifecycle: (event) => {
        lifecycle.push(event);
      },
    });
    consumers.add(consumer);

    const startup = consumer.start();
    await withDeadline(transport.waitUntilEntered());
    const value = envelope(1);
    const id = await append(key, value);
    transport.release();
    await startup;
    await withDeadline(observed.promise);
    await eventually(() => expect(consumer.lastHandledCursor).toBe(id));

    expect(delivered).toEqual([{ redisEntryId: id, envelope: value }]);
    expect(consumer.lastHandledCursor).toBe(id);
    expect(quarantined).toEqual([]);
    expect(lifecycle.some(({ state }) => state === "cursor_lost")).toBe(false);
  });

  it("delivers the same post-tail entry to two independent plain-XREAD consumers", async () => {
    const key = uniqueStreamKey();
    const historical = envelope(1);
    const historicalId = await append(key, historical);
    const restrictedRedisUrl = await createRestrictedApiRedisUrl(key, {
      allowSentinelWrite: false,
    });
    const firstGate = deliveryGate();
    const secondGate = deliveryGate();
    const first = new RedisDeliveryConsumer(
      new RedisDeliveryConsumerTransport(restrictedRedisUrl, key),
      {
        deliver: firstGate.deliver,
        quarantine: () => Promise.resolve(),
        lifecycle: () => undefined,
      },
    );
    const second = new RedisDeliveryConsumer(
      new RedisDeliveryConsumerTransport(restrictedRedisUrl, key),
      {
        deliver: secondGate.deliver,
        quarantine: () => Promise.resolve(),
        lifecycle: () => undefined,
      },
    );
    consumers.add(first);
    consumers.add(second);

    await Promise.all([first.start(), second.start()]);
    expect(first.lastHandledCursor).toBe(historicalId);
    expect(second.lastHandledCursor).toBe(historicalId);
    expect((await rawStreamEntries(key)).map(({ id }) => id)).toEqual([historicalId]);

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
    expect(await consumerGroups(key)).toEqual([]);
  });

  it("fails closed when XINFO materializes duplicate Redis fields", async () => {
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
        if (event.state === "unavailable") resolveError(event);
      },
    });
    consumers.add(consumer);
    await consumer.start();
    const startupCursor = consumer.lastHandledCursor;
    const value = envelope(1);
    const fields = encodeDeliveryStreamFields(value);

    await publisher.sendCommand([
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
      state: "unavailable",
      code: "REDIS_UNAVAILABLE",
      cursor: startupCursor,
    });
    expect(consumer.lastHandledCursor).toBe(startupCursor);
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

  it("retains the trimmed-empty witness from recovery inspection through consumption", async () => {
    const key = uniqueStreamKey();
    await appendAt(key, "1-0", envelope(1));
    await appendAt(key, "2-0", envelope(2));
    await publisher.sendCommand(["XTRIM", key, "MAXLEN", "0"]);
    const transport = new ControlledFirstReadFailureTransport(
      new RedisDeliveryConsumerTransport(redisUrl, key),
    );
    let resolveOutcome!: (outcome: "delivered" | "cursor_lost") => void;
    const outcome = new Promise<"delivered" | "cursor_lost">((resolve) => {
      resolveOutcome = resolve;
    });
    const delivered: DeliveryContext[] = [];
    const consumer = new RedisDeliveryConsumer(transport, {
      deliver: (context) => {
        delivered.push(context);
        resolveOutcome("delivered");
        return Promise.resolve();
      },
      quarantine: () => Promise.resolve(),
      lifecycle: (event) => {
        if (event.state === "cursor_lost") resolveOutcome("cursor_lost");
      },
    });
    consumers.add(consumer);
    await consumer.start();
    expect(consumer.lastHandledCursor).toBe("2-0");

    const value = envelope(3);
    const id = await appendAt(key, "3-0", value);
    transport.failFirstRead();

    expect(await withDeadline(outcome)).toBe("delivered");
    expect(delivered).toEqual([{ redisEntryId: id, envelope: value }]);
    await eventually(() => expect(consumer.lastHandledCursor).toBe("3-0"));
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
