import { createClient } from "redis";
import { z } from "zod";
import {
  PRESENCE_SESSION_TTL_MS,
  PRESENCE_SNAPSHOT_MAX_SESSIONS,
  boardPresenceSnapshotSchema,
  idSchema,
  presenceAvailabilitySchema,
  presenceParticipantLeaveSchema,
  presenceParticipantUpsertSchema,
  presenceUpdateSchema,
  type BoardPresenceSnapshot,
  type PresenceAvailability,
  type PresenceParticipant,
  type PresenceParticipantLeave,
  type PresenceParticipantUpsert,
  type PresenceUpdate,
} from "@converge/protocol";

export const PRESENCE_REDIS_CHANNEL = "converge:presence:v1:pubsub";
export const PRESENCE_REDIS_PREFIX = "converge:presence:v1";
export type PresenceAvailabilityCode = "PRESENCE_REDIS_UNAVAILABLE" | "PRESENCE_MESSAGE_INVALID";
export type PresenceOutcome<T> =
  | { kind: "ok"; value: T }
  | { kind: "capacity" }
  | { kind: "missing" }
  | { kind: "identity" }
  | { kind: "unavailable"; code: PresenceAvailabilityCode }
  | { kind: "invalid" };

export interface PresencePrincipalEvidence {
  userId: string;
  displayName: string;
}
export interface PresenceAdmission {
  boardId: string;
  presenceSessionId: string;
  principal: PresencePrincipalEvidence;
  cursor: PresenceUpdate["cursor"];
  activity: PresenceUpdate["activity"];
}
export interface PresenceRefresh {
  boardId: string;
  presenceSessionId: string;
  principal: PresencePrincipalEvidence;
  cursor: PresenceUpdate["cursor"];
  activity: PresenceUpdate["activity"];
}
export interface PresenceRedisTransport {
  start(): Promise<PresenceOutcome<void>>;
  admit(input: PresenceAdmission): Promise<PresenceOutcome<PresenceParticipant>>;
  refresh(input: PresenceRefresh): Promise<PresenceOutcome<PresenceParticipant>>;
  snapshot(boardId: string): Promise<PresenceOutcome<BoardPresenceSnapshot>>;
  leave(
    boardId: string,
    presenceSessionId: string,
    principal: PresencePrincipalEvidence,
  ): Promise<PresenceOutcome<PresenceParticipantLeave | null>>;
  onDelta(
    callback: (delta: PresenceParticipantUpsert | PresenceParticipantLeave) => void,
  ): () => void;
  onAvailability(
    callback: (availability: PresenceAvailability, code?: PresenceAvailabilityCode) => void,
  ): () => void;
  stop(): Promise<void>;
}

const internalRecordSchema = z
  .object({
    presenceSessionId: idSchema,
    userId: idSchema,
    displayName: z.string().min(1).max(100),
    revision: z.string().regex(/^(0|[1-9]\d*)$/),
    activity: z.enum(["active", "idle"]),
    cursor: z
      .object({
        x: z.number().finite().min(-1_000_000).max(1_000_000),
        y: z.number().finite().min(-1_000_000).max(1_000_000),
      })
      .strict()
      .nullable(),
    observedAtMs: z.string().regex(/^(0|[1-9]\d*)$/),
    expiresAtMs: z.string().regex(/^(0|[1-9]\d*)$/),
  })
  .strict();
const pubSubEnvelopeSchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("upsert"), event: presenceParticipantUpsertSchema }).strict(),
  z.object({ type: z.literal("leave"), event: presenceParticipantLeaveSchema }).strict(),
]);

const ADMIT_SCRIPT = `
local time=redis.call('TIME'); local now=time[1]*1000+math.floor(time[2]/1000); local expiry=now+tonumber(ARGV[5]);
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('EXISTS', KEYS[2]) == 1 then return {'EXISTING'} end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[6]) then return {'CAPACITY'} end
local r={presenceSessionId=ARGV[1],userId=ARGV[2],displayName=ARGV[3],revision='1',activity=ARGV[4],cursor=cjson.decode(ARGV[7]),observedAtMs=tostring(now),expiresAtMs=tostring(expiry)}
local encoded=cjson.encode(r); redis.call('SET', KEYS[2], encoded, 'PX', ARGV[5]); redis.call('ZADD', KEYS[1], expiry, ARGV[1]); redis.call('PEXPIRE', KEYS[1], ARGV[5]); redis.call('DEL', KEYS[3]); return {'OK',encoded}`;
const REFRESH_SCRIPT = `
local time=redis.call('TIME'); local now=time[1]*1000+math.floor(time[2]/1000); local raw=redis.call('GET',KEYS[2]); if not raw then return {'MISSING'} end
local ok,r=pcall(cjson.decode,raw); if not ok or r.presenceSessionId ~= ARGV[1] or r.userId ~= ARGV[2] or r.displayName ~= ARGV[3] then return {'IDENTITY'} end
if not r.revision or tonumber(r.revision) == nil or tonumber(r.revision) >= 9007199254740991 then return {'INVALID'} end
local expiry=now+tonumber(ARGV[5]); r.revision=tostring(tonumber(r.revision)+1); r.activity=ARGV[4]; r.cursor=cjson.decode(ARGV[6]); r.observedAtMs=tostring(now); r.expiresAtMs=tostring(expiry); local encoded=cjson.encode(r); redis.call('SET',KEYS[2],encoded,'PX',ARGV[5]); redis.call('ZADD',KEYS[1],expiry,ARGV[1]); redis.call('PEXPIRE',KEYS[1],ARGV[5]); return {'OK',encoded}`;
const LEAVE_SCRIPT = `
local time=redis.call('TIME'); local now=time[1]*1000+math.floor(time[2]/1000); local raw=redis.call('GET',KEYS[2]); if not raw then return {'NOOP'} end
local ok,r=pcall(cjson.decode,raw); if not ok or r.presenceSessionId ~= ARGV[1] or r.userId ~= ARGV[2] or r.displayName ~= ARGV[3] or tonumber(r.revision) == nil then return {'IDENTITY'} end
local revision=tostring(tonumber(r.revision)+1); local tomb=cjson.encode({presenceSessionId=ARGV[1],revision=revision,observedAtMs=tostring(now)}); redis.call('DEL',KEYS[2]); redis.call('ZREM',KEYS[1],ARGV[1]); redis.call('SET',KEYS[3],tomb,'PX',ARGV[4]); if redis.call('ZCARD',KEYS[1]) == 0 then redis.call('DEL',KEYS[1]) end; return {'OK',tomb}`;
const SNAPSHOT_SCRIPT = `
local time=redis.call('TIME'); local now=time[1]*1000+math.floor(time[2]/1000); redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now); local ids=redis.call('ZRANGEBYSCORE',KEYS[1],now,'+inf','LIMIT',0,tonumber(ARGV[1])); local out={}; for _,id in ipairs(ids) do local raw=redis.call('GET',ARGV[2]..id); if raw then table.insert(out,raw) else redis.call('ZREM',KEYS[1],id) end end; if redis.call('ZCARD',KEYS[1]) == 0 then redis.call('DEL',KEYS[1]) end; return {tostring(now),unpack(out)}`;

export function boardKeys(boardId: string, presenceSessionId?: string) {
  if (
    !idSchema.safeParse(boardId).success ||
    (presenceSessionId && !idSchema.safeParse(presenceSessionId).success)
  )
    return null;
  const base = `${PRESENCE_REDIS_PREFIX}:{${boardId}}`;
  return {
    index: `${base}:sessions`,
    session: presenceSessionId ? `${base}:session:${presenceSessionId}` : "",
    tombstone: presenceSessionId ? `${base}:tombstone:${presenceSessionId}` : "",
    prefix: `${base}:session:`,
  };
}
function iso(milliseconds: string): string | null {
  const value = Number(milliseconds);
  return Number.isSafeInteger(value) && value >= 0 ? new Date(value).toISOString() : null;
}
function participant(raw: unknown): PresenceParticipant | null {
  if (typeof raw !== "string") return null;
  try {
    const value = internalRecordSchema.parse(JSON.parse(raw));
    const observedAt = iso(value.observedAtMs),
      expiresAt = iso(value.expiresAtMs),
      revision = Number(value.revision);
    if (!observedAt || !expiresAt || !Number.isSafeInteger(revision)) return null;
    return {
      presenceSessionId: value.presenceSessionId,
      userId: value.userId,
      displayName: value.displayName,
      revision,
      activity: value.activity,
      cursor: value.cursor,
      observedAt,
      expiresAt,
    };
  } catch {
    return null;
  }
}
function scriptReply(value: unknown): string[] | null {
  return Array.isArray(value) && value.every((part) => typeof part === "string") ? value : null;
}

type RedisClient = ReturnType<typeof createClient>;
export const PRESENCE_RECONNECT_BASE_DELAY_MS = 250;
export const PRESENCE_RECONNECT_MAX_DELAY_MS = 10_000;
export interface PresenceRedisTransportScheduler {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}
export interface PresenceRedisTransportOptions {
  createClient?: (url: string) => RedisClient;
  scheduler?: PresenceRedisTransportScheduler;
  random?: () => number;
}
const presenceScheduler: PresenceRedisTransportScheduler = {
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
  clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};
type ConnectionCycle = {
  generation: number;
  command: RedisClient;
  publisher: RedisClient;
  subscriber: RedisClient;
  failed: boolean;
  available: boolean;
};
export class RedisPresenceTransport implements PresenceRedisTransport {
  private cycle: ConnectionCycle | undefined;
  private started = false;
  private stopped = false;
  private attempting = false;
  private generation = 0;
  private retryAttempt = 0;
  private retryTimer: unknown;
  private startPromise: Promise<PresenceOutcome<void>> | undefined;
  private resolveStart: ((outcome: PresenceOutcome<void>) => void) | undefined;
  private stopPromise: Promise<void> | undefined;
  private readonly deltas = new Set<
    (delta: PresenceParticipantUpsert | PresenceParticipantLeave) => void
  >();
  private readonly availability = new Set<
    (value: PresenceAvailability, code?: PresenceAvailabilityCode) => void
  >();
  constructor(
    private readonly redisUrl: string,
    private readonly options: PresenceRedisTransportOptions = {},
  ) {}
  onDelta(
    callback: (delta: PresenceParticipantUpsert | PresenceParticipantLeave) => void,
  ): () => void {
    this.deltas.add(callback);
    return () => this.deltas.delete(callback);
  }
  onAvailability(
    callback: (availability: PresenceAvailability, code?: PresenceAvailabilityCode) => void,
  ): () => void {
    this.availability.add(callback);
    return () => this.availability.delete(callback);
  }
  async start(): Promise<PresenceOutcome<void>> {
    if (this.stopped) return { kind: "unavailable", code: "PRESENCE_REDIS_UNAVAILABLE" };
    if (this.cycle?.available) return { kind: "ok", value: undefined };
    if (this.startPromise) return this.startPromise;
    this.startPromise = new Promise((resolve) => {
      this.resolveStart = resolve;
    });
    this.connect();
    return this.startPromise;
  }
  private create(): RedisClient {
    return (
      this.options.createClient ??
      ((url: string) => createClient({ url, socket: { reconnectStrategy: false } }))
    )(this.redisUrl);
  }
  private connect(): void {
    if (this.stopped || this.attempting || this.cycle?.available) return;
    this.attempting = true;
    const command = this.create();
    const cycle: ConnectionCycle = {
      generation: ++this.generation,
      command,
      publisher: command.duplicate(),
      subscriber: command.duplicate(),
      failed: false,
      available: false,
    };
    this.cycle = cycle;
    const failed = () => this.fail(cycle);
    for (const client of [cycle.command, cycle.publisher, cycle.subscriber]) {
      client.on("error", failed);
      client.on("end", failed);
    }
    void this.connectCycle(cycle);
  }
  private async connectCycle(cycle: ConnectionCycle): Promise<void> {
    try {
      const connected = await Promise.allSettled([
        cycle.command.connect(),
        cycle.publisher.connect(),
        cycle.subscriber.connect(),
      ]);
      if (connected.some((result) => result.status === "rejected"))
        throw new Error("Redis unavailable");
      if (!this.current(cycle)) return;
      await cycle.subscriber.subscribe(PRESENCE_REDIS_CHANNEL, (message) =>
        this.receive(cycle, message),
      );
      if (!this.current(cycle)) return;
      cycle.available = true;
      this.started = true;
      this.retryAttempt = 0;
      this.settleStart({ kind: "ok", value: undefined });
      this.emitAvailability("available");
    } catch {
      this.fail(cycle);
    } finally {
      this.attempting = false;
      if (cycle.failed && !this.stopped) this.scheduleRetry();
    }
  }
  private current(cycle: ConnectionCycle): boolean {
    return !this.stopped && this.cycle?.generation === cycle.generation && !cycle.failed;
  }
  private fail(cycle: ConnectionCycle): void {
    if (!this.current(cycle)) return;
    cycle.failed = true;
    if (this.cycle?.generation === cycle.generation) {
      this.cycle = undefined;
      this.started = false;
    }
    this.destroy(cycle);
    this.settleStart({ kind: "unavailable", code: "PRESENCE_REDIS_UNAVAILABLE" });
    this.emitAvailability("unavailable", "PRESENCE_REDIS_UNAVAILABLE");
    if (!this.attempting) this.scheduleRetry();
  }
  private scheduleRetry(): void {
    if (this.stopped || this.attempting || this.retryTimer !== undefined) return;
    const maximum = Math.min(
      PRESENCE_RECONNECT_MAX_DELAY_MS,
      PRESENCE_RECONNECT_BASE_DELAY_MS * 2 ** Math.min(this.retryAttempt, 5),
    );
    const random = this.options.random ?? Math.random;
    const sampled = random();
    const boundedRandom = Number.isFinite(sampled) ? Math.min(1, Math.max(0, sampled)) : 0.5;
    const delay = Math.floor(boundedRandom * maximum);
    if (!Number.isSafeInteger(delay) || delay < 0 || delay > PRESENCE_RECONNECT_MAX_DELAY_MS)
      throw new Error("Invalid presence retry delay");
    this.retryAttempt += 1;
    this.retryTimer = (this.options.scheduler ?? presenceScheduler).setTimeout(() => {
      this.retryTimer = undefined;
      this.connect();
    }, delay);
  }
  private settleStart(outcome: PresenceOutcome<void>): void {
    const resolve = this.resolveStart;
    this.resolveStart = undefined;
    this.startPromise = undefined;
    resolve?.(outcome);
  }
  private emitAvailability(
    status: "available" | "unavailable",
    code?: PresenceAvailabilityCode,
  ): void {
    if (this.stopped) return;
    const value = presenceAvailabilitySchema.parse({
      schemaVersion: 1,
      boardId: "00000000-0000-4000-8000-000000000000",
      status,
    });
    for (const callback of this.availability) {
      try {
        callback(value, code);
      } catch {
        // Presence observers are best-effort and cannot destabilize transport cleanup.
      }
    }
  }
  private receive(cycle: ConnectionCycle, message: string): void {
    if (!this.current(cycle)) return;
    if (message.length > 32_768)
      return this.emitAvailability("unavailable", "PRESENCE_MESSAGE_INVALID");
    let raw: unknown;
    try {
      raw = JSON.parse(message);
    } catch {
      return this.emitAvailability("unavailable", "PRESENCE_MESSAGE_INVALID");
    }
    const parsed = pubSubEnvelopeSchema.safeParse(raw);
    if (!parsed.success) return this.emitAvailability("unavailable", "PRESENCE_MESSAGE_INVALID");
    for (const callback of this.deltas) {
      try {
        callback(parsed.data.event);
      } catch {
        // A consumer callback is isolated from Redis subscription ownership.
      }
    }
  }
  private ready(): RedisClient | null {
    return this.started && this.cycle?.available && this.cycle.command.isReady
      ? this.cycle.command
      : null;
  }
  private async eval(script: string, keys: string[], args: string[]): Promise<string[] | null> {
    const client = this.ready();
    if (!client) return null;
    try {
      return scriptReply(await client.eval(script, { keys, arguments: args }));
    } catch {
      if (this.cycle) this.fail(this.cycle);
      return null;
    }
  }
  async admit(input: PresenceAdmission): Promise<PresenceOutcome<PresenceParticipant>> {
    const keys = boardKeys(input.boardId, input.presenceSessionId);
    const client = presenceUpdateSchema.safeParse({
      schemaVersion: 1,
      boardId: input.boardId,
      cursor: input.cursor,
      activity: input.activity,
    });
    if (
      !keys ||
      !client.success ||
      !idSchema.safeParse(input.principal.userId).success ||
      input.principal.displayName.length === 0 ||
      input.principal.displayName.length > 100
    )
      return { kind: "invalid" };
    const reply = await this.eval(
      ADMIT_SCRIPT,
      [keys.index, keys.session, keys.tombstone],
      [
        input.presenceSessionId,
        input.principal.userId,
        input.principal.displayName,
        input.activity,
        String(PRESENCE_SESSION_TTL_MS),
        String(PRESENCE_SNAPSHOT_MAX_SESSIONS),
        JSON.stringify(input.cursor),
      ],
    );
    if (!reply) return { kind: "unavailable", code: "PRESENCE_REDIS_UNAVAILABLE" };
    if (reply[0] === "CAPACITY") return { kind: "capacity" };
    const value = reply[0] === "OK" ? participant(reply[1]) : null;
    if (!value) return { kind: "invalid" };
    await this.publish({
      type: "upsert",
      event: presenceParticipantUpsertSchema.parse({
        schemaVersion: 1,
        boardId: input.boardId,
        participant: value,
      }),
    });
    return { kind: "ok", value };
  }
  async refresh(input: PresenceRefresh): Promise<PresenceOutcome<PresenceParticipant>> {
    const keys = boardKeys(input.boardId, input.presenceSessionId);
    const update = presenceUpdateSchema.safeParse({
      schemaVersion: 1,
      boardId: input.boardId,
      cursor: input.cursor,
      activity: input.activity,
    });
    if (!keys || !update.success) return { kind: "invalid" };
    const reply = await this.eval(
      REFRESH_SCRIPT,
      [keys.index, keys.session, keys.tombstone],
      [
        input.presenceSessionId,
        input.principal.userId,
        input.principal.displayName,
        input.activity,
        String(PRESENCE_SESSION_TTL_MS),
        JSON.stringify(input.cursor),
      ],
    );
    if (!reply) return { kind: "unavailable", code: "PRESENCE_REDIS_UNAVAILABLE" };
    if (reply[0] === "MISSING") return { kind: "missing" };
    if (reply[0] === "IDENTITY") return { kind: "identity" };
    const value = reply[0] === "OK" ? participant(reply[1]) : null;
    if (!value) return { kind: "invalid" };
    await this.publish({
      type: "upsert",
      event: presenceParticipantUpsertSchema.parse({
        schemaVersion: 1,
        boardId: input.boardId,
        participant: value,
      }),
    });
    return { kind: "ok", value };
  }
  async snapshot(boardId: string): Promise<PresenceOutcome<BoardPresenceSnapshot>> {
    const keys = boardKeys(boardId);
    if (!keys) return { kind: "invalid" };
    const reply = await this.eval(
      SNAPSHOT_SCRIPT,
      [keys.index],
      [String(PRESENCE_SNAPSHOT_MAX_SESSIONS), keys.prefix],
    );
    if (!reply || reply.length === 0)
      return { kind: "unavailable", code: "PRESENCE_REDIS_UNAVAILABLE" };
    const observedAt = iso(reply[0]!);
    const participants = reply.slice(1).map(participant);
    if (!observedAt || participants.some((item) => !item)) return { kind: "invalid" };
    try {
      return {
        kind: "ok",
        value: boardPresenceSnapshotSchema.parse({
          schemaVersion: 1,
          boardId,
          observedAt,
          participants,
        }),
      };
    } catch {
      return { kind: "invalid" };
    }
  }
  async leave(
    boardId: string,
    presenceSessionId: string,
    principal: PresencePrincipalEvidence,
  ): Promise<PresenceOutcome<PresenceParticipantLeave | null>> {
    const keys = boardKeys(boardId, presenceSessionId);
    if (!keys || !idSchema.safeParse(principal.userId).success) return { kind: "invalid" };
    const reply = await this.eval(
      LEAVE_SCRIPT,
      [keys.index, keys.session, keys.tombstone],
      [presenceSessionId, principal.userId, principal.displayName, String(PRESENCE_SESSION_TTL_MS)],
    );
    if (!reply) return { kind: "unavailable", code: "PRESENCE_REDIS_UNAVAILABLE" };
    if (reply[0] === "NOOP") return { kind: "ok", value: null };
    if (reply[0] !== "OK" || typeof reply[1] !== "string") return { kind: "identity" };
    try {
      const tomb = z
        .object({
          presenceSessionId: idSchema,
          revision: z.string().regex(/^(0|[1-9]\d*)$/),
          observedAtMs: z.string().regex(/^(0|[1-9]\d*)$/),
        })
        .strict()
        .parse(JSON.parse(reply[1]));
      const observedAt = iso(tomb.observedAtMs),
        revision = Number(tomb.revision);
      if (!observedAt || !Number.isSafeInteger(revision)) return { kind: "invalid" };
      const value = presenceParticipantLeaveSchema.parse({
        schemaVersion: 1,
        boardId,
        presenceSessionId: tomb.presenceSessionId,
        revision,
        reason: "left",
        observedAt,
      });
      await this.publish({ type: "leave", event: value });
      return { kind: "ok", value };
    } catch {
      return { kind: "invalid" };
    }
  }
  private async publish(value: z.infer<typeof pubSubEnvelopeSchema>): Promise<void> {
    const cycle = this.cycle;
    if (!cycle?.available || !cycle.publisher.isReady) {
      if (cycle) this.fail(cycle);
      return;
    }
    try {
      await cycle.publisher.publish(PRESENCE_REDIS_CHANNEL, JSON.stringify(value));
    } catch {
      this.fail(cycle);
    }
  }
  async stop(): Promise<void> {
    this.stopPromise ??= this.stopOnce();
    return this.stopPromise;
  }
  private stopOnce(): Promise<void> {
    this.stopped = true;
    this.started = false;
    if (this.retryTimer !== undefined)
      (this.options.scheduler ?? presenceScheduler).clearTimeout(this.retryTimer);
    this.retryTimer = undefined;
    const cycle = this.cycle;
    this.cycle = undefined;
    if (cycle) {
      cycle.failed = true;
      this.destroy(cycle);
    }
    this.settleStart({ kind: "unavailable", code: "PRESENCE_REDIS_UNAVAILABLE" });
    this.deltas.clear();
    this.availability.clear();
    return Promise.resolve();
  }
  private destroy(cycle: ConnectionCycle): void {
    const clients = [cycle.subscriber, cycle.publisher, cycle.command];
    for (const client of clients)
      try {
        client.removeAllListeners("error");
        client.removeAllListeners("end");
        client.destroy();
      } catch {
        // A partially constructed node-redis client may already be terminal.
      }
  }
}
