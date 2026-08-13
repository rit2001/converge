import { execFile } from "node:child_process";
import { createServer as createNetServer, type AddressInfo } from "node:net";
import { promisify } from "node:util";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  BoardRepository,
  BoardSnapshotCandidateRepository,
  BoardSnapshotRepository,
  createPool,
  OutboxRepository,
  type ClaimedOutboxEvent,
  type DatabasePool,
} from "@converge/database";
import {
  InMemoryTelemetryRecorder,
  METRIC_CATALOG,
  PROMETHEUS_CONTENT_TYPE,
  type StructuredLogger,
} from "@converge/observability";
import {
  decodeDeliveryStreamFields,
  deliveryStreamFieldsSchema,
  membershipRevokedDeliveryEnvelopeSchema,
  type DeliveryStreamFields,
} from "@converge/protocol";
import {
  CompactionCoordinator,
  OutboxWorker,
  RedisDeliveryStream,
  RedisXaddRejectedError,
  SimulatedWorkerCrash,
  SnapshotCoordinator,
  createWorkerApplication,
  createNodeWorkerOperationalListener,
  InstanceWorkerOperationalState,
  parseWorkerEnvironment,
  systemWorkerScheduler,
  type CompactionCoordinatorScheduler,
  type DeliveryStream,
  type OutboxPublicationRepository,
  type OutboxWorkerConfiguration,
  type OutboxWorkerHooks,
  type SnapshotCoordinatorScheduler,
  type WorkerScheduler,
} from "@converge/worker";
import { createRectangleCommand } from "@converge/testkit";
import { createClient, type RedisClientType } from "redis";

const sharedDatabaseUrl =
  process.env.DATABASE_URL ?? "postgresql://converge:converge@localhost:55432/converge";
const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const repositoryRoot = new URL("../..", import.meta.url);
const runFile = promisify(execFile);
const testUserId = "00000000-0000-4000-8000-000000000001";

let databaseName: string | undefined;
let isolatedDatabaseUrl: string | undefined;
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
  isolatedDatabaseUrl = isolatedUrl.toString();
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

function uniqueWorkerObservabilityStreamKey(): string {
  const key = `converge:test:m28-worker:${crypto.randomUUID()}`;
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
    telemetry?: InMemoryTelemetryRecorder;
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
    options.telemetry,
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

class ManualTimerScheduler implements SnapshotCoordinatorScheduler, CompactionCoordinatorScheduler {
  private nextId = 1;
  readonly tasks = new Map<number, { callback: () => void; delayMs: number }>();

  setTimeout(callback: () => void, delayMs: number): number {
    const id = this.nextId++;
    this.tasks.set(id, { callback, delayMs });
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.tasks.delete(handle as number);
  }
}

class ControlledSleepScheduler implements WorkerScheduler {
  private nextId = 1;
  readonly sleeps = new Map<
    number,
    { delayMs: number; resolve(): void; signal: AbortSignal | undefined }
  >();

  random(): number {
    return 0;
  }

  sleep(delayMs: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const id = this.nextId++;
      const finish = (): void => {
        signal?.removeEventListener("abort", finish);
        this.sleeps.delete(id);
        resolve();
      };
      this.sleeps.set(id, { delayMs, resolve: finish, signal });
      signal?.addEventListener("abort", finish, { once: true });
    });
  }

  release(delayMs: number): void {
    const task = [...this.sleeps.values()].find((candidate) => candidate.delayMs === delayMs);
    if (!task) throw new Error(`No controlled sleep is waiting for ${delayMs}ms`);
    task.resolve();
  }
}

class ControlledRealStream implements DeliveryStream {
  allowConnection = false;
  failNextAppend = false;
  private available = false;

  constructor(readonly delegate: RedisDeliveryStream) {}

  async connect(): Promise<void> {
    if (!this.allowConnection) throw new Error("controlled Redis unavailability");
    await this.delegate.connect();
    this.available = true;
  }

  isReady(): boolean {
    return this.available && this.delegate.isReady();
  }

  append(fields: DeliveryStreamFields, signal: AbortSignal): Promise<unknown> {
    if (this.failNextAppend) {
      this.failNextAppend = false;
      this.available = false;
      this.allowConnection = false;
      return Promise.reject(new Error("controlled ambiguous Redis failure"));
    }
    return this.delegate.append(fields, signal);
  }

  trimByAge(signal: AbortSignal): Promise<void> {
    return this.delegate.trimByAge(signal);
  }

  resetAfterCommandTimeout(): Promise<void> {
    return this.delegate.resetAfterCommandTimeout();
  }

  async close(force = false): Promise<void> {
    this.available = false;
    await this.delegate.close(force);
  }
}

async function allocatePort(): Promise<number> {
  const server = createNetServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const port = (server.address() as AddressInfo).port;
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return port;
}

async function operationalRequest(
  port: number,
  path: string,
  authorization?: string,
): Promise<{ status: number; contentType: string; body: string }> {
  const response = await fetch(
    `http://127.0.0.1:${port}${path}`,
    authorization === undefined ? undefined : { headers: { authorization } },
  );
  return {
    status: response.status,
    contentType: response.headers.get("content-type") ?? "",
    body: await response.text(),
  };
}

function metricValue(body: string, series: string): number {
  const line = body.split("\n").find((candidate) => candidate.startsWith(`${series} `));
  return line === undefined ? 0 : Number(line.slice(series.length + 1));
}

async function waitFor<T>(read: () => Promise<T>, predicate: (value: T) => boolean): Promise<T> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const value = await read();
    if (predicate(value)) return value;
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("Bounded worker observability assertion did not converge");
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
      resetAfterCommandTimeout: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const telemetry = new InMemoryTelemetryRecorder();
    const worker = await createWorker(key, {}, { stream, telemetry });

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
    expect(
      telemetry
        .snapshot()
        .counters.filter(({ name }) => name === "converge_outbox_publications_total"),
    ).toEqual([
      expect.objectContaining({ labels: { outcome: "published" }, value: 1 }),
      expect.objectContaining({ labels: { outcome: "retry" }, value: 1 }),
    ]);
  });

  it("blocks an oversized envelope without calling real Redis", async () => {
    const key = uniqueStreamKey();
    const [event] = await seedBoard();
    if (!event) throw new Error("Expected event");
    const telemetry = new InMemoryTelemetryRecorder();
    const worker = await createWorker(
      key,
      {},
      {
        configuration: { maximumEnvelopeBytes: 1 },
        telemetry,
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
    expect(
      telemetry
        .snapshot()
        .counters.find(
          ({ name, labels }) =>
            name === "converge_outbox_publications_total" && labels.outcome === "blocked",
        )?.value,
    ).toBe(1);
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
      resetAfterCommandTimeout: () => Promise.resolve(),
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
      append: (fields, signal) => realStream.append(fields, signal),
      trimByAge: () => Promise.reject(new Error("private trim details")),
      resetAfterCommandTimeout: () => realStream.resetAfterCommandTimeout(),
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
      append: async (fields, signal) => {
        await realStream.append(fields, signal);
        accepted.resolve();
        signal.addEventListener("abort", () => neverReturn.reject(new Error("timed out")), {
          once: true,
        });
        return neverReturn.promise;
      },
      trimByAge: (signal) => realStream.trimByAge(signal),
      resetAfterCommandTimeout: () => realStream.resetAfterCommandTimeout(),
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
    const telemetry = new InMemoryTelemetryRecorder();
    const stale = await createWorker(
      key,
      {
        afterXadd: async () => {
          await expireLease(event.eventId);
          [replacement] = await repository.claimAvailable({
            owner: "replacement-worker",
            batchSize: 1,
            leaseDurationMs: 60_000,
          });
        },
      },
      { telemetry },
    );

    await expect(stale.runCycle()).resolves.toMatchObject({ outcomes: ["stale"] });
    expect(replacement?.eventId).toBe(event.eventId);
    expect(await stateFor(event.eventId)).toMatchObject({
      status: "leased",
      attempt_count: 2,
      lease_token: replacement?.leaseToken,
      redis_entry_id: null,
    });
    expect(await entriesFor(key)).toHaveLength(1);
    expect(
      telemetry
        .snapshot()
        .counters.find(
          ({ name, labels }) =>
            name === "converge_outbox_publications_total" && labels.outcome === "stale",
        )?.value,
    ).toBe(1);
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
    for (let index = 0; index < 250; index += 1)
      await lengthStream.append(fields, new AbortController().signal);
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
    await ageStream.append(fields, new AbortController().signal);
    await ageStream.trimByAge(new AbortController().signal);
    const retained = await redis.sendCommand(["XRANGE", ageKey, "-", "+"]);
    if (!Array.isArray(retained)) throw new Error("Expected retained entries");
    expect(retained).toHaveLength(1);
  });
});

describe("M2.8 worker observability acceptance under real PostgreSQL and Redis failures", () => {
  it("reports independent readiness and bounded publication, snapshot, compaction, and shutdown evidence", async () => {
    if (!isolatedDatabaseUrl) throw new Error("Isolated database URL is unavailable");
    const metricsToken = "m28-worker-observability-acceptance-token";
    const port = await allocatePort();
    const disabledPort = await allocatePort();
    const disabledState = new InstanceWorkerOperationalState();
    const disabledListener = createNodeWorkerOperationalListener({
      configuration: {
        host: "127.0.0.1",
        port: disabledPort,
        metricsEnabled: false,
        metricsBearerToken: "",
      },
      state: disabledState,
    });
    await disabledListener.start();
    expect((await operationalRequest(disabledPort, "/metrics")).status).toBe(404);
    await disabledListener.close();

    const key = uniqueWorkerObservabilityStreamKey();
    const workerPool = createPool(isolatedDatabaseUrl);
    const recorder = new InMemoryTelemetryRecorder(1_000);
    const operationalState = new InstanceWorkerOperationalState();
    const realStream = new RedisDeliveryStream(
      redisUrl,
      key,
      100_000,
      24 * 60 * 60 * 1_000,
      logger,
    );
    const controlledStream = new ControlledRealStream(realStream);
    const reconnectScheduler = new ControlledSleepScheduler();
    const outboxScheduler = new ControlledSleepScheduler();
    const snapshotScheduler = new ManualTimerScheduler();
    const compactionScheduler = new ManualTimerScheduler();
    const coreStartEntered = deferred<void>();
    const allowCoreStart = deferred<void>();
    const targetBoards: { snapshot?: string; compaction?: string } = {};
    let holdSnapshot = false;
    let snapshotStarted = deferred<void>();
    let releaseSnapshot = deferred<void>();
    let holdCompaction = false;
    let compactionStarted = deferred<void>();
    let releaseCompaction = deferred<void>();
    let holdPublication = false;
    let publicationStarted = deferred<void>();
    let releasePublication = deferred<void>();
    let failMetricsSnapshot = false;
    let telemetryTime = 0;
    const telemetryClock = { now: () => (telemetryTime += 100) };
    let snapshotCoordinator: SnapshotCoordinator | undefined;
    let compactionCoordinator: CompactionCoordinator | undefined;
    let outboxWorker: OutboxWorker | undefined;
    const environment = parseWorkerEnvironment({
      NODE_ENV: "test",
      DATABASE_URL: isolatedDatabaseUrl,
      REDIS_URL: redisUrl,
      REDIS_STREAM_KEY: key,
      WORKER_ID: "m28-worker-observability",
      COMPACTION_ENABLED: "true",
      WORKER_OPERATIONS_ENABLED: "true",
      WORKER_OPERATIONS_HOST: "127.0.0.1",
      WORKER_OPERATIONS_PORT: String(port),
      WORKER_METRICS_ENABLED: "true",
      WORKER_METRICS_BEARER_TOKEN: metricsToken,
      OUTBOX_IDLE_POLL_MS: "250",
      OUTBOX_POLL_JITTER_RATIO: "0",
      SNAPSHOT_OPERATION_THRESHOLD: "1000",
      SNAPSHOT_POLL_INTERVAL_MS: "30000",
      SNAPSHOT_POLL_JITTER_PERCENT: "1",
      COMPACTION_POLL_INTERVAL_MS: "300000",
      COMPACTION_POLL_JITTER_PERCENT: "1",
      LOG_LEVEL: "silent",
    });
    const application = await createWorkerApplication(
      environment,
      {
        createLogger: () => logger,
        createDatabase: () => workerPool,
        createStream: () => controlledStream,
        reconnectScheduler,
        createSnapshotCoordinator: ({ database, configuration, telemetry, telemetryClock }) => {
          const repository = new BoardSnapshotCandidateRepository(database as DatabasePool);
          snapshotCoordinator = new SnapshotCoordinator({
            repository: {
              discover: async (options) => {
                const result = await repository.discover(options);
                return {
                  ...result,
                  candidates: result.candidates.filter(
                    ({ boardId }) => boardId === targetBoards.snapshot,
                  ),
                };
              },
              capture: async (boardId, options) => {
                if (holdSnapshot && boardId === targetBoards.snapshot) {
                  snapshotStarted.resolve(undefined);
                  await releaseSnapshot.promise;
                }
                return repository.capture(boardId, options);
              },
            },
            configuration,
            scheduler: snapshotScheduler,
            random: { next: () => 0.5 },
            telemetry,
            telemetryClock,
          });
          return {
            start: async () => {
              coreStartEntered.resolve(undefined);
              await allowCoreStart.promise;
              await snapshotCoordinator!.start();
            },
            stop: () => snapshotCoordinator!.stop(),
          };
        },
        createCompactionCoordinator: ({
          candidates,
          compaction,
          configuration,
          telemetry,
          telemetryClock,
        }) => {
          compactionCoordinator = new CompactionCoordinator({
            candidates: {
              discover: async (options) => {
                const result = await candidates.discover(options);
                return {
                  ...result,
                  candidates: result.candidates.filter(
                    ({ boardId }) => boardId === targetBoards.compaction,
                  ),
                };
              },
            },
            compaction: {
              compact: async (boardId) => {
                if (holdCompaction && boardId === targetBoards.compaction) {
                  compactionStarted.resolve(undefined);
                  await releaseCompaction.promise;
                }
                return compaction.compact(boardId);
              },
            },
            configuration,
            scheduler: compactionScheduler,
            random: { next: () => 0.5 },
            telemetry,
            telemetryClock,
          });
          return compactionCoordinator;
        },
        createOutboxPublisher: ({ database, stream, configuration, telemetry, telemetryClock }) => {
          outboxWorker = new OutboxWorker(
            new OutboxRepository(database as DatabasePool),
            stream,
            configuration,
            logger,
            outboxScheduler,
            {
              beforeXadd: async () => {
                if (!holdPublication) return;
                publicationStarted.resolve(undefined);
                await releasePublication.promise;
              },
            },
            telemetry,
            telemetryClock,
          );
          return outboxWorker;
        },
      },
      {
        telemetry: recorder,
        telemetryClock,
        operationalState,
        telemetrySnapshot: () => {
          if (failMetricsSnapshot) throw new Error("controlled metrics failure");
          return recorder.snapshot();
        },
      },
    );

    const start = application.start();
    await coreStartEntered.promise;
    expect((await operationalRequest(port, "/health/live")).status).toBe(200);
    expect((await operationalRequest(port, "/health/ready")).status).toBe(503);
    expect((await operationalRequest(port, "/health/delivery-ready")).status).toBe(503);
    const missingToken = await operationalRequest(port, "/metrics");
    const wrongToken = await operationalRequest(port, "/metrics", "Bearer incorrect-token-value");
    expect(missingToken).toEqual(wrongToken);
    allowCoreStart.resolve(undefined);
    await start;
    await snapshotCoordinator?.runCycle();
    await compactionCoordinator?.runCycle();
    expect((await operationalRequest(port, "/health/ready")).status).toBe(200);
    expect((await operationalRequest(port, "/health/delivery-ready")).status).toBe(503);
    const scrape = () => operationalRequest(port, "/metrics", `Bearer ${metricsToken}`);
    const initialMetrics = await scrape();
    expect(initialMetrics).toMatchObject({ status: 200, contentType: PROMETHEUS_CONTENT_TYPE });
    for (const gauge of [
      "converge_outbox_active_work",
      "converge_snapshot_active_work",
      "converge_compaction_active_work",
    ])
      expect(metricValue(initialMetrics.body, gauge)).toBe(0);
    expect(await scrape()).toEqual(initialMetrics);

    controlledStream.allowConnection = true;
    reconnectScheduler.release(250);
    await waitFor(
      () => operationalRequest(port, "/health/delivery-ready"),
      ({ status }) => status === 200,
    );
    expect((await operationalRequest(port, "/health/ready")).status).toBe(200);

    const [publishedEvent] = await seedBoard();
    if (!publishedEvent) throw new Error("Expected publication fixture");
    holdPublication = true;
    outboxScheduler.release(250);
    await publicationStarted.promise;
    expect(metricValue((await scrape()).body, "converge_outbox_active_work")).toBe(1);
    releasePublication.resolve(undefined);
    const publishedState = await waitFor(
      () => stateFor(publishedEvent.eventId),
      ({ status }) => status === "published",
    );
    const publishedEntries = await entriesFor(key);
    expect(publishedState.redis_entry_id).toBe(publishedEntries[0]?.id);
    const publishedMetrics = await scrape();
    expect(
      metricValue(publishedMetrics.body, 'converge_outbox_publications_total{outcome="published"}'),
    ).toBe(1);
    expect(
      metricValue(publishedMetrics.body, "converge_outbox_publication_duration_seconds_count"),
    ).toBe(1);
    expect(metricValue(publishedMetrics.body, "converge_outbox_active_work")).toBe(0);

    holdPublication = false;
    publicationStarted = deferred<void>();
    releasePublication = deferred<void>();
    const [retryEvent] = await seedBoard();
    if (!retryEvent) throw new Error("Expected retry fixture");
    controlledStream.failNextAppend = true;
    outboxScheduler.release(250);
    const retryState = await waitFor(
      () => stateFor(retryEvent.eventId),
      ({ status }) => status === "retry_wait",
    );
    expect(retryState.redis_entry_id).toBeNull();
    reconnectScheduler.release(250);
    await waitFor(
      () => operationalRequest(port, "/health/delivery-ready"),
      ({ status }) => status === 503,
    );
    expect((await operationalRequest(port, "/health/ready")).status).toBe(200);
    const retryMetrics = await scrape();
    expect(
      metricValue(retryMetrics.body, 'converge_outbox_publications_total{outcome="retry"}'),
    ).toBe(1);
    expect(
      metricValue(retryMetrics.body, "converge_outbox_publication_duration_seconds_count"),
    ).toBe(2);
    expect(metricValue(retryMetrics.body, "converge_outbox_active_work")).toBe(0);

    const boards = new BoardRepository(pool);
    const snapshotBoard = await boards.createBoard(
      testUserId,
      `m28-snapshot-${crypto.randomUUID()}`,
    );
    boardIds.add(snapshotBoard.id);
    targetBoards.snapshot = snapshotBoard.id;
    const snapshotHeadsBefore = await pool.query(
      "SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1",
      [snapshotBoard.id],
    );
    holdSnapshot = true;
    snapshotStarted = deferred<void>();
    releaseSnapshot = deferred<void>();
    const snapshotCycle = snapshotCoordinator!.runCycle();
    await snapshotStarted.promise;
    expect(metricValue((await scrape()).body, "converge_snapshot_active_work")).toBe(1);
    expect((await operationalRequest(port, "/health/ready")).status).toBe(200);
    expect((await operationalRequest(port, "/health/delivery-ready")).status).toBe(503);
    releaseSnapshot.resolve(undefined);
    await snapshotCycle;
    expect(
      await pool.query("SELECT count(*)::int count FROM board_snapshots WHERE board_id = $1", [
        snapshotBoard.id,
      ]),
    ).toMatchObject({ rows: [{ count: 1 }] });
    expect(
      await pool.query("SELECT last_seq, last_delivery_seq FROM boards WHERE id = $1", [
        snapshotBoard.id,
      ]),
    ).toMatchObject({ rows: snapshotHeadsBefore.rows });
    const snapshotMetrics = await scrape();
    expect(
      metricValue(snapshotMetrics.body, 'converge_snapshot_runs_total{outcome="captured"}'),
    ).toBe(1);
    expect(metricValue(snapshotMetrics.body, "converge_snapshot_duration_seconds_count")).toBe(1);
    expect(metricValue(snapshotMetrics.body, "converge_snapshot_active_work")).toBe(0);

    const compactionBoard = await boards.createBoard(
      testUserId,
      `m28-compaction-${crypto.randomUUID()}`,
    );
    boardIds.add(compactionBoard.id);
    const snapshotRepository = new BoardSnapshotRepository(pool);
    const commitAndPublish = async (baseSeq: number) => {
      const command = { ...createRectangleCommand(compactionBoard.id), baseSeq };
      const committed = await boards.commitOperation(testUserId, command);
      const claims = await repository.claimAvailable({
        owner: "m28-compaction-fixture",
        batchSize: 16,
        leaseDurationMs: 60_000,
      });
      const claim = claims.find(({ eventId }) => eventId === committed.event.eventId);
      if (!claim) throw new Error("Compaction outbox evidence was not claimable");
      await repository.markPublished({
        eventId: claim.eventId,
        leaseToken: claim.leaseToken,
        publicationId: `${baseSeq + 10}-0`,
      });
    };
    await commitAndPublish(0);
    await snapshotRepository.create(compactionBoard.id);
    await commitAndPublish(1);
    await snapshotRepository.create(compactionBoard.id);
    await commitAndPublish(2);
    targetBoards.compaction = compactionBoard.id;
    holdCompaction = true;
    compactionStarted = deferred<void>();
    releaseCompaction = deferred<void>();
    const compactionCycle = compactionCoordinator!.runCycle();
    await compactionStarted.promise;
    expect(metricValue((await scrape()).body, "converge_compaction_active_work")).toBe(1);
    releaseCompaction.resolve(undefined);
    await compactionCycle;
    const compacted = await pool.query<{
      operation_recovery_floor: string;
      delivery_recovery_floor: string;
      operation_count: string;
      outbox_count: string;
    }>(
      `SELECT operation_recovery_floor, delivery_recovery_floor,
              (SELECT count(*) FROM board_operations WHERE board_id = b.id) operation_count,
              (SELECT count(*) FROM outbox_events WHERE board_id = b.id) outbox_count
       FROM boards b WHERE id = $1`,
      [compactionBoard.id],
    );
    expect(compacted.rows[0]).toEqual({
      operation_recovery_floor: "1",
      delivery_recovery_floor: "1",
      operation_count: "2",
      outbox_count: "2",
    });
    const compactionMetrics = await scrape();
    expect(
      metricValue(compactionMetrics.body, 'converge_compaction_runs_total{outcome="compacted"}'),
    ).toBe(1);
    expect(metricValue(compactionMetrics.body, "converge_compaction_duration_seconds_count")).toBe(
      1,
    );
    expect(metricValue(compactionMetrics.body, "converge_compaction_active_work")).toBe(0);
    expect((await operationalRequest(port, "/health/ready")).status).toBe(200);
    expect((await operationalRequest(port, "/health/delivery-ready")).status).toBe(503);

    const beforeRenderFailure = recorder.snapshot();
    failMetricsSnapshot = true;
    expect(await scrape()).toMatchObject({
      status: 503,
      contentType: "text/plain; charset=utf-8",
      body: "Metrics unavailable\n",
    });
    expect(recorder.snapshot()).toEqual(beforeRenderFailure);
    failMetricsSnapshot = false;
    const finalMetrics = await scrape();
    expect(finalMetrics.body).not.toMatch(
      /outbox\.publication\.result|snapshot\.capture\.result|compaction\.result|worker\.lifecycle/,
    );
    const knownNames = new Set(Object.keys(METRIC_CATALOG));
    for (const line of finalMetrics.body.split("\n")) {
      if (line === "" || line.startsWith("#")) continue;
      const name = (line.split(/[ {]/, 1)[0] ?? "").replace(/_(bucket|sum|count)$/, "");
      expect(knownNames.has(name)).toBe(true);
    }
    const privateValues = [
      isolatedDatabaseUrl,
      redisUrl,
      metricsToken,
      publishedEvent.boardId,
      publishedEvent.eventId,
      retryEvent.boardId,
      retryEvent.eventId,
      snapshotBoard.id,
      compactionBoard.id,
      publishedState.redis_entry_id ?? "missing-publication-id",
    ];
    const eventEvidence = JSON.stringify(recorder.snapshot().events);
    for (const value of privateValues) {
      expect(finalMetrics.body).not.toContain(value);
      expect(eventEvidence).not.toContain(value);
    }

    const shutdown = application.shutdown("SIGTERM");
    expect(application.shutdown("SIGINT")).toBe(shutdown);
    expect(operationalState.isLive()).toBe(false);
    expect(operationalState.isCoreReady()).toBe(false);
    expect(operationalState.isDeliveryReady()).toBe(false);
    await shutdown;
    operationalState.setCoreReady(true);
    operationalState.setRedisReady(true);
    operationalState.setOutboxAccepting(true);
    expect(operationalState.isCoreReady()).toBe(false);
    expect(operationalState.isDeliveryReady()).toBe(false);
    expect(snapshotScheduler.tasks.size).toBe(0);
    expect(compactionScheduler.tasks.size).toBe(0);
    expect(outboxScheduler.sleeps.size).toBe(0);
    expect(reconnectScheduler.sleeps.size).toBe(0);
    expect(
      recorder
        .snapshot()
        .events.filter(({ eventName }) => eventName === "worker.lifecycle")
        .map(({ code }) => code),
    ).toEqual(["STARTING", "READY", "STOPPING", "STOPPED"]);
    await redis.sendCommand(["DEL", key]);
    streamKeys.delete(key);
    expect(await redis.sendCommand(["EXISTS", key])).toBe(0);
  });
});
