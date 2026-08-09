import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  createPool,
  OutboxRepository,
  type ClaimedOutboxEvent,
  type DatabasePool,
} from "@converge/database";
import type { StructuredLogger } from "@converge/observability";
import {
  decodeDeliveryStreamFields,
  deliveryStreamFieldsSchema,
  membershipRevokedDeliveryEnvelopeSchema,
  type DeliveryStreamFields,
} from "@converge/protocol";
import {
  OutboxWorker,
  RedisDeliveryStream,
  RedisXaddRejectedError,
  SimulatedWorkerCrash,
  systemWorkerScheduler,
  type DeliveryStream,
  type OutboxPublicationRepository,
  type OutboxWorkerConfiguration,
  type OutboxWorkerHooks,
  type WorkerScheduler,
} from "@converge/worker";
import { createClient, type RedisClientType } from "redis";

const sharedDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const repositoryRoot = new URL("../..", import.meta.url);
const runFile = promisify(execFile);
const testUserId = "00000000-0000-4000-8000-000000000001";

let databaseName: string | undefined;
let adminPool: DatabasePool | undefined;
let pool: DatabasePool;
let repository: OutboxRepository;
let redis: RedisClientType;
const boardIds = new Set<string>();
const streamKeys = new Set<string>();
const deliveryStreams = new Set<DeliveryStream>();

const logger: StructuredLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const configuration: OutboxWorkerConfiguration = {
  owner: "failure-worker",
  claimBatchSize: 32,
  publishConcurrency: 8,
  leaseDurationMs: 60_000,
  idlePollMs: 250,
  pollJitterRatio: 0.2,
  publicationTimeoutMs: 5_000,
  maximumEnvelopeBytes: 128 * 1024,
};

interface SeededEvent {
  eventId: string;
  boardId: string;
  deliverySeq: number;
}

interface OutboxState {
  id: string;
  status: string;
  attempt_count: number;
  lease_token: string | null;
  redis_entry_id: string | null;
  published_at: Date | null;
  last_error_code: string | null;
}

interface StreamEntry {
  id: string;
  fields: DeliveryStreamFields;
}

beforeAll(async () => {
  databaseName = `converge_m23_worker_${process.pid}_${crypto
    .randomUUID()
    .replaceAll("-", "")
    .slice(0, 12)}`;
  const adminUrl = new URL(sharedDatabaseUrl);
  adminUrl.pathname = "/postgres";
  adminUrl.searchParams.delete("options");
  adminPool = createPool(adminUrl.toString());
  await adminPool.query(`CREATE DATABASE "${databaseName}"`);

  const isolatedUrl = new URL(sharedDatabaseUrl);
  isolatedUrl.pathname = `/${databaseName}`;
  isolatedUrl.searchParams.delete("options");
  await runFile("pnpm", ["--filter", "@converge/database", "migrate"], {
    cwd: repositoryRoot,
    env: { ...process.env, DATABASE_URL: isolatedUrl.toString() },
    maxBuffer: 1024 * 1024,
  });
  pool = createPool(isolatedUrl.toString());
  repository = new OutboxRepository(pool);
  redis = createClient({ url: redisUrl });
  redis.on("error", () => undefined);
  await redis.connect();
});

afterEach(async () => {
  await Promise.all([...deliveryStreams].map((stream) => stream.close(true)));
  deliveryStreams.clear();
  if (streamKeys.size > 0) await redis.sendCommand(["DEL", ...streamKeys]);
  streamKeys.clear();
  if (boardIds.size > 0)
    await pool.query("DELETE FROM boards WHERE id = ANY($1::uuid[])", [[...boardIds]]);
  boardIds.clear();
});

afterAll(async () => {
  if (redis?.isOpen) redis.destroy();
  if (pool) await pool.end();
  try {
    if (adminPool && databaseName) await adminPool.query(`DROP DATABASE "${databaseName}"`);
  } finally {
    await adminPool?.end();
  }
});

function uniqueStreamKey(): string {
  const key = `converge:test:m23:${crypto.randomUUID()}`;
  streamKeys.add(key);
  return key;
}

async function seedBoard(eventCount = 1): Promise<SeededEvent[]> {
  const boardId = crypto.randomUUID();
  boardIds.add(boardId);
  const events: SeededEvent[] = [];
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO boards(id, name, created_by, last_delivery_seq)
       VALUES ($1, $2, $3, $4)`,
      [boardId, `worker-${boardId}`, testUserId, eventCount],
    );
    for (let deliverySeq = 1; deliverySeq <= eventCount; deliverySeq += 1) {
      const eventId = crypto.randomUUID();
      const envelope = membershipRevokedDeliveryEnvelopeSchema.parse({
        schemaVersion: 1,
        eventId,
        boardId,
        deliverySeq,
        eventType: "board.membership.revoked",
        occurredAt: `2026-08-09T12:00:${String(deliverySeq).padStart(2, "0")}.000Z`,
        payload: {
          revokedUserId: crypto.randomUUID(),
          initiatedByUserId: testUserId,
        },
      });
      await client.query(
        `INSERT INTO outbox_events(
           id, board_id, delivery_seq, canvas_seq, event_type, schema_version, payload
         ) VALUES ($1,$2,$3,NULL,'board.membership.revoked',1,$4)`,
        [eventId, boardId, deliverySeq, envelope],
      );
      events.push({ eventId, boardId, deliverySeq });
    }
    await client.query("COMMIT");
    return events;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function stateFor(eventId: string): Promise<OutboxState> {
  const result = await pool.query<OutboxState>(
    `SELECT id, status, attempt_count, lease_token, redis_entry_id, published_at, last_error_code
     FROM outbox_events WHERE id = $1`,
    [eventId],
  );
  const state = result.rows[0];
  if (!state) throw new Error("Expected outbox state");
  return state;
}

async function expireLease(eventId: string): Promise<void> {
  await pool.query(
    `UPDATE outbox_events
     SET leased_until = statement_timestamp() - interval '1 second'
     WHERE id = $1 AND status = 'leased'`,
    [eventId],
  );
}

function createStream(
  key: string,
  maximumLength = 100_000,
  maximumAgeMs = 24 * 60 * 60 * 1000,
): RedisDeliveryStream {
  const stream = new RedisDeliveryStream(redisUrl, key, maximumLength, maximumAgeMs, logger);
  deliveryStreams.add(stream);
  return stream;
}

async function createWorker(
  key: string,
  hooks: OutboxWorkerHooks = {},
  options: {
    repository?: OutboxPublicationRepository;
    stream?: DeliveryStream;
    scheduler?: WorkerScheduler;
    configuration?: Partial<OutboxWorkerConfiguration>;
  } = {},
): Promise<OutboxWorker> {
  const stream = options.stream ?? createStream(key);
  await stream.connect();
  return new OutboxWorker(
    options.repository ?? repository,
    stream,
    { ...configuration, ...options.configuration },
    logger,
    options.scheduler ?? systemWorkerScheduler,
    hooks,
  );
}

function parseEntries(input: unknown): StreamEntry[] {
  if (!Array.isArray(input)) throw new Error("Expected Redis stream entries");
  return input.map((entry) => {
    if (!Array.isArray(entry)) throw new Error("Expected Redis stream entry");
    const tuple = entry as unknown[];
    if (tuple.length !== 2 || typeof tuple[0] !== "string")
      throw new Error("Expected Redis stream entry");
    const rawFields: unknown = tuple[1];
    if (!Array.isArray(rawFields)) throw new Error("Expected Redis stream fields");
    const fieldList = rawFields as unknown[];
    if (fieldList.some((field) => typeof field !== "string"))
      throw new Error("Expected Redis stream fields");
    const pairs: Array<[string, string]> = [];
    for (let index = 0; index < fieldList.length; index += 2) {
      const name = fieldList[index];
      const value = fieldList[index + 1];
      if (typeof name !== "string" || typeof value !== "string")
        throw new Error("Expected field pair");
      pairs.push([name, value]);
    }
    return { id: tuple[0], fields: deliveryStreamFieldsSchema.parse(Object.fromEntries(pairs)) };
  });
}

async function entriesFor(key: string): Promise<StreamEntry[]> {
  return parseEntries(await redis.sendCommand(["XRANGE", key, "-", "+"]));
}

function deferred<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("worker outbox publication crash recovery", () => {
  it("publishes strict fields to real Redis and stores the exact XADD ID", async () => {
    const key = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    const worker = await createWorker(key);

    await expect(worker.runCycle()).resolves.toMatchObject({
      state: "processed",
      outcomes: ["published"],
    });
    const entries = await entriesFor(key);
    expect(entries).toHaveLength(1);
    expect(entries[0]?.fields).toMatchObject({
      schemaVersion: "1",
      eventId: event.eventId,
      boardId: event.boardId,
      deliverySeq: "1",
      eventType: "board.membership.revoked",
    });
    expect(decodeDeliveryStreamFields(entries[0]?.fields)).toMatchObject({
      eventId: event.eventId,
      boardId: event.boardId,
      deliverySeq: 1,
    });
    expect(await stateFor(event.eventId)).toMatchObject({
      status: "published",
      redis_entry_id: entries[0]?.id,
    });
  });

  it("isolates an XADD rejection while an unrelated board publishes", async () => {
    const key = uniqueStreamKey();
    const [rejected] = await seedBoard();
    const [unrelated] = await seedBoard();
    if (!rejected || !unrelated) throw new Error("Expected events");
    const stream: DeliveryStream = {
      connect: () => Promise.resolve(),
      isReady: () => true,
      append: (fields) =>
        fields.eventId === rejected.eventId
          ? Promise.reject(new RedisXaddRejectedError("private connection details"))
          : Promise.resolve("6000-0"),
      trimByAge: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const worker = await createWorker(key, {}, { stream });

    expect(new Set((await worker.runCycle()).outcomes)).toEqual(
      new Set(["retry_scheduled", "published"]),
    );
    expect(await stateFor(rejected.eventId)).toMatchObject({
      status: "retry_wait",
      published_at: null,
      redis_entry_id: null,
      last_error_code: "REDIS_XADD_FAILED",
    });
    expect(await stateFor(unrelated.eventId)).toMatchObject({
      status: "published",
      redis_entry_id: "6000-0",
    });
  });

  it("blocks an oversized envelope without calling real Redis", async () => {
    const key = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    const worker = await createWorker(
      key,
      {},
      {
        configuration: { maximumEnvelopeBytes: 1 },
      },
    );

    await expect(worker.runCycle()).resolves.toMatchObject({ outcomes: ["blocked"] });
    expect(await entriesFor(key)).toEqual([]);
    expect(await stateFor(event.eventId)).toMatchObject({
      status: "blocked",
      published_at: null,
      redis_entry_id: null,
      last_error_code: "DELIVERY_ENVELOPE_TOO_LARGE",
    });
  });

  it("does not publish when Redis returns a malformed XADD result", async () => {
    const key = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    const stream: DeliveryStream = {
      connect: () => Promise.resolve(),
      isReady: () => true,
      append: () => Promise.resolve("malformed"),
      trimByAge: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const worker = await createWorker(key, {}, { stream });

    await expect(worker.runCycle()).resolves.toMatchObject({ outcomes: ["retry_scheduled"] });
    expect(await stateFor(event.eventId)).toMatchObject({
      status: "retry_wait",
      published_at: null,
      redis_entry_id: null,
      last_error_code: "INVALID_REDIS_ENTRY_ID",
    });
  });

  it("finalizes an acknowledged XADD even when age trimming fails", async () => {
    const key = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    const realStream = createStream(key);
    await realStream.connect();
    const trimFailureStream: DeliveryStream = {
      connect: () => Promise.resolve(),
      isReady: () => realStream.isReady(),
      append: (fields) => realStream.append(fields),
      trimByAge: () => Promise.reject(new Error("private trim details")),
      close: () => Promise.resolve(),
    };
    const worker = await createWorker(key, {}, { stream: trimFailureStream });

    await expect(worker.runCycle()).resolves.toMatchObject({ outcomes: ["published"] });
    const entries = await entriesFor(key);
    expect(entries).toHaveLength(1);
    expect(await stateFor(event.eventId)).toMatchObject({
      status: "published",
      redis_entry_id: entries[0]?.id,
      last_error_code: null,
    });
  });

  it("recovers a crash after claim and before XADD through lease expiry", async () => {
    const key = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    const crashed = await createWorker(key, {
      beforeXadd: () => Promise.reject(new SimulatedWorkerCrash("before XADD")),
    });

    await expect(crashed.runCycle()).rejects.toThrow(SimulatedWorkerCrash);
    expect(await entriesFor(key)).toEqual([]);
    expect(await stateFor(event.eventId)).toMatchObject({ status: "leased", attempt_count: 1 });
    await expireLease(event.eventId);
    const recovered = await createWorker(key);
    await expect(recovered.runCycle()).resolves.toMatchObject({ outcomes: ["published"] });
    expect(await entriesFor(key)).toHaveLength(1);
    expect(await stateFor(event.eventId)).toMatchObject({ status: "published", attempt_count: 2 });
  });

  it("republishes the same stable event after a crash between XADD and database success", async () => {
    const key = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    const crashed = await createWorker(key, {
      afterXadd: () => Promise.reject(new SimulatedWorkerCrash("after XADD")),
    });

    await expect(crashed.runCycle()).rejects.toThrow(SimulatedWorkerCrash);
    const firstEntries = await entriesFor(key);
    expect(firstEntries).toHaveLength(1);
    expect(await stateFor(event.eventId)).toMatchObject({ status: "leased", redis_entry_id: null });
    await expireLease(event.eventId);
    const recovered = await createWorker(key);
    await expect(recovered.runCycle()).resolves.toMatchObject({ outcomes: ["published"] });

    const entries = await entriesFor(key);
    expect(entries).toHaveLength(2);
    expect(new Set(entries.map((entry) => entry.fields.eventId))).toEqual(new Set([event.eventId]));
    expect(new Set(entries.map((entry) => entry.fields.deliverySeq))).toEqual(new Set(["1"]));
    expect(entries[0]?.id).not.toBe(entries[1]?.id);
    expect(await stateFor(event.eventId)).toMatchObject({
      status: "published",
      attempt_count: 2,
      redis_entry_id: entries[1]?.id,
    });
  });

  it("allows a timeout after real Redis acceptance to produce a stable duplicate", async () => {
    const key = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    const realStream = createStream(key);
    await realStream.connect();
    const accepted = deferred<void>();
    const neverReturn = deferred<unknown>();
    const timeout = deferred<void>();
    const ambiguousStream: DeliveryStream = {
      connect: () => Promise.resolve(),
      isReady: () => realStream.isReady(),
      append: async (fields) => {
        await realStream.append(fields);
        accepted.resolve();
        return neverReturn.promise;
      },
      trimByAge: () => realStream.trimByAge(),
      close: () => Promise.resolve(),
    };
    const timeoutScheduler: WorkerScheduler = {
      random: () => 0,
      sleep: () => timeout.promise,
    };
    const ambiguousWorker = await createWorker(
      key,
      {},
      {
        stream: ambiguousStream,
        scheduler: timeoutScheduler,
      },
    );
    const attempt = ambiguousWorker.runCycle();
    await accepted.promise;
    timeout.resolve();
    await expect(attempt).resolves.toMatchObject({ outcomes: ["retry_scheduled"] });
    expect(await entriesFor(key)).toHaveLength(1);
    await pool.query(
      `UPDATE outbox_events
       SET next_attempt_at = statement_timestamp() - interval '1 second'
       WHERE id = $1`,
      [event.eventId],
    );
    const retry = await createWorker(key);
    await expect(retry.runCycle()).resolves.toMatchObject({ outcomes: ["published"] });
    const entries = await entriesFor(key);
    expect(entries).toHaveLength(2);
    expect(entries.map((entry) => entry.fields.eventId)).toEqual([event.eventId, event.eventId]);
  });

  it("cannot finalize with a stale token after XADD", async () => {
    const key = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    let replacement: ClaimedOutboxEvent | undefined;
    const stale = await createWorker(key, {
      afterXadd: async () => {
        await expireLease(event.eventId);
        [replacement] = await repository.claimAvailable({
          owner: "replacement-worker",
          batchSize: 1,
          leaseDurationMs: 60_000,
        });
      },
    });

    await expect(stale.runCycle()).resolves.toMatchObject({ outcomes: ["stale"] });
    expect(replacement?.eventId).toBe(event.eventId);
    expect(await stateFor(event.eventId)).toMatchObject({
      status: "leased",
      attempt_count: 2,
      lease_token: replacement?.leaseToken,
      redis_entry_id: null,
    });
    expect(await entriesFor(key)).toHaveLength(1);
  });

  it("publishes one same-board head at a time in delivery order", async () => {
    const key = uniqueStreamKey();
    const [first, second] = await seedBoard(2);
    if (!first || !second) throw new Error("Expected events");
    const worker = await createWorker(key);

    await expect(worker.runCycle()).resolves.toMatchObject({ claimed: 1, outcomes: ["published"] });
    expect(await stateFor(second.eventId)).toMatchObject({ status: "pending", attempt_count: 0 });
    await expect(worker.runCycle()).resolves.toMatchObject({ claimed: 1, outcomes: ["published"] });
    expect((await entriesFor(key)).map((entry) => entry.fields.deliverySeq)).toEqual(["1", "2"]);
  });

  it("applies approximate MAXLEN and explicit server-time MINID retention", async () => {
    const maxLengthKey = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    const [claim] = await repository.claimAvailable({
      owner: "retention-fixture",
      batchSize: 1,
      leaseDurationMs: 60_000,
    });
    if (!claim) throw new Error("Expected claim");
    const fields = deliveryStreamFieldsSchema.parse({
      schemaVersion: "1",
      eventId: claim.eventId,
      boardId: claim.boardId,
      deliverySeq: String(claim.deliverySeq),
      eventType: claim.eventType,
      event: JSON.stringify(claim.envelope),
    });
    const lengthStream = createStream(maxLengthKey, 10, 24 * 60 * 60 * 1000);
    await lengthStream.connect();
    for (let index = 0; index < 250; index += 1) await lengthStream.append(fields);
    const length = Number(await redis.sendCommand(["XLEN", maxLengthKey]));
    expect(length).toBeLessThan(250);

    const ageKey = uniqueStreamKey();
    const redisTime = await redis.sendCommand(["TIME"]);
    if (!Array.isArray(redisTime) || typeof redisTime[0] !== "string")
      throw new Error("Expected Redis server time");
    const oldMilliseconds = Number(redisTime[0]) * 1_000 - 60_000;
    await redis.sendCommand(["XADD", ageKey, `${oldMilliseconds}-0`, "fixture", "old"]);
    const ageStream = createStream(ageKey, 100_000, 1_000);
    await ageStream.connect();
    await ageStream.append(fields);
    await ageStream.trimByAge();
    const retained = await redis.sendCommand(["XRANGE", ageKey, "-", "+"]);
    if (!Array.isArray(retained)) throw new Error("Expected retained entries");
    expect(retained).toHaveLength(1);
  });
});
