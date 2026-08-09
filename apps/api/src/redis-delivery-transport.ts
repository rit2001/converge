import { createClient, RESP_TYPES } from "redis";
import type {
  REDIS_DELIVERY_BLOCK_MS,
  REDIS_DELIVERY_READ_COUNT,
  DeliveryConsumerTransport,
  DeliveryStreamInitialization,
  DeliveryStreamMetadata,
  RawDeliveryStreamEntry,
} from "./delivery-consumer.js";
import {
  DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
  DELIVERY_STREAM_INITIALIZATION_TYPE,
  DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
  isCanonicalDeliveryStreamGeneration,
} from "./delivery-stream-sentinel.js";

export {
  DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
  DELIVERY_STREAM_INITIALIZATION_TYPE,
  DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
} from "./delivery-stream-sentinel.js";

const UINT64_MAXIMUM = 18_446_744_073_709_551_615n;
const INITIALIZE_STREAM_SCRIPT = `
local function is_canonical_generation(value)
  if type(value) ~= 'string' or string.len(value) ~= 36 then
    return false
  end
  if string.sub(value, 9, 9) ~= '-' or
     string.sub(value, 14, 14) ~= '-' or
     string.sub(value, 19, 19) ~= '-' or
     string.sub(value, 24, 24) ~= '-' then
    return false
  end
  if string.sub(value, 15, 15) ~= '4' or
     not string.match(string.sub(value, 20, 20), '^[89ab]$') then
    return false
  end
  local compact = string.gsub(value, '-', '')
  return string.len(compact) == 32 and string.match(compact, '^[0-9a-f]+$') ~= nil
end

if not is_canonical_generation(ARGV[1]) then
  return {2, '', ''}
end

if redis.call('EXISTS', KEYS[1]) == 1 then
  local first = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', 1)
  if #first == 1 then
    local fields = first[1][2]
    if #fields == 4 and
       fields[1] == '${DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD}' and
       fields[2] == '${DELIVERY_STREAM_INITIALIZATION_TYPE}' and
       fields[3] == '${DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD}' and
       is_canonical_generation(fields[4]) then
      return {0, first[1][1], fields[4]}
    end
    for index = 1, #fields, 2 do
      if fields[index] == '${DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD}' or
         fields[index] == '${DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD}' then
        return {2, '', ''}
      end
    end
  end
  return {0, '', ''}
end
local sentinel_id = redis.call(
  'XADD',
  KEYS[1],
  '*',
  '${DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD}',
  '${DELIVERY_STREAM_INITIALIZATION_TYPE}',
  '${DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD}',
  ARGV[1]
)
return {1, sentinel_id, ARGV[1]}
`;

function createResp2Client(redisUrl: string) {
  return createClient({
    url: redisUrl,
    // RESP2 preserves stream field lists as ordered pairs. RESP3 represents a stream message as a
    // map and would erase duplicate field names before the consumer can reject them.
    RESP: 2,
    socket: { reconnectStrategy: false },
  }).withTypeMapping({
    // XINFO counters are RESP integers. Mapping them at the decoder boundary prevents values above
    // Number.MAX_SAFE_INTEGER from being rounded before continuity validation can use BigInt.
    [RESP_TYPES.NUMBER]: String,
  });
}

type RedisClient = ReturnType<typeof createResp2Client>;

const ZERO_STREAM_ID = "0-0";

function asArray(value: unknown, message: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(message);
  return value;
}

function asString(value: unknown, message: string): string {
  if (typeof value !== "string") throw new Error(message);
  return value;
}

export function parseRedisUint64Reply(value: unknown, message: string): string {
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error(message);
    return String(value);
  }
  if (typeof value === "bigint") {
    if (value < 0n || value > UINT64_MAXIMUM) throw new Error(message);
    return value.toString();
  }
  if (typeof value !== "string" || !/^(0|[1-9]\d*)$/.test(value)) throw new Error(message);
  if (BigInt(value) > UINT64_MAXIMUM) throw new Error(message);
  return value;
}

function asBulkString(value: unknown, message: string): string {
  if (typeof value !== "string") throw new Error(message);
  return value;
}

function flatRecord(value: unknown, message: string): Map<string, unknown> {
  const values = asArray(value, message);
  if (values.length % 2 !== 0) throw new Error(message);
  const result = new Map<string, unknown>();
  for (let index = 0; index < values.length; index += 2) {
    const key = asString(values[index], message);
    if (result.has(key)) throw new Error(message);
    result.set(key, values[index + 1]);
  }
  return result;
}

function parseEntry(value: unknown): RawDeliveryStreamEntry {
  const tuple = asArray(value, "Redis returned an invalid stream entry");
  if (tuple.length !== 2) throw new Error("Redis returned an invalid stream entry");
  const id = asBulkString(tuple[0], "Redis returned an invalid stream entry ID");
  const rawFields = asArray(tuple[1], "Redis returned invalid stream fields");
  if (rawFields.length % 2 !== 0) throw new Error("Redis returned invalid stream fields");
  const fields: [string, string][] = [];
  for (let index = 0; index < rawFields.length; index += 2)
    fields.push([
      asBulkString(rawFields[index], "Redis returned an invalid stream field name"),
      asBulkString(rawFields[index + 1], "Redis returned an invalid stream field value"),
    ]);
  return { id, fields };
}

function parseEntryId(value: unknown): string | null {
  if (value === null) return null;
  return parseEntry(value).id;
}

function parseRunId(info: unknown): string {
  if (typeof info !== "string") throw new Error("Redis INFO SERVER returned an invalid response");
  const match = /^run_id:([^\r\n]+)$/m.exec(info);
  if (!match?.[1]) throw new Error("Redis INFO SERVER omitted run_id");
  return match[1];
}

function absentMetadata(incarnation: string): DeliveryStreamMetadata {
  return {
    exists: false,
    length: "0",
    firstEntryId: null,
    lastEntryId: null,
    lastGeneratedId: ZERO_STREAM_ID,
    maxDeletedEntryId: ZERO_STREAM_ID,
    entriesAdded: "0",
    incarnation,
  };
}

export class RedisDeliveryConsumerTransport implements DeliveryConsumerTransport {
  private control: RedisClient | undefined;
  private reader: RedisClient | undefined;
  private connectPromise: Promise<void> | undefined;
  private closePromise: Promise<void> | undefined;
  private closed = false;
  private readonly errorListener = (): void => {
    // Command promises are the lifecycle signal consumed by RedisDeliveryConsumer.
  };

  constructor(
    private readonly redisUrl: string,
    private readonly streamKey: string,
  ) {}

  connect(): Promise<void> {
    if (this.closed) return Promise.reject(new Error("Redis delivery transport is closed"));
    this.connectPromise ??= this.connectOnce().finally(() => {
      this.connectPromise = undefined;
    });
    return this.connectPromise;
  }

  async initializeStream(input: {
    generationToken: string;
    signal: AbortSignal;
  }): Promise<DeliveryStreamInitialization> {
    if (!isCanonicalDeliveryStreamGeneration(input.generationToken))
      throw new Error("Redis stream initializer received an invalid generation token");
    const response = asArray(
      await this.requireControl().sendCommand(
        ["EVAL", INITIALIZE_STREAM_SCRIPT, "1", this.streamKey, input.generationToken],
        { abortSignal: input.signal },
      ),
      "Redis stream initializer returned an invalid response",
    );
    if (response.length !== 3)
      throw new Error("Redis stream initializer returned an invalid response");
    const created = parseRedisUint64Reply(
      response[0],
      "Redis stream initializer returned an invalid creation flag",
    );
    const sentinelId = asBulkString(
      response[1],
      "Redis stream initializer returned an invalid sentinel ID",
    );
    const generationToken = asBulkString(
      response[2],
      "Redis stream initializer returned an invalid generation token",
    );
    if (created === "2")
      throw new Error("Redis stream initializer returned invalid sentinel evidence");
    if (created === "0") {
      if (sentinelId === "" && generationToken === "")
        return { created: false, sentinelId: null, generationToken: null };
      if (sentinelId !== "" && isCanonicalDeliveryStreamGeneration(generationToken))
        return { created: false, sentinelId, generationToken };
      throw new Error("Redis stream initializer returned an invalid generation token");
    }
    if (
      created !== "1" ||
      sentinelId === "" ||
      !isCanonicalDeliveryStreamGeneration(generationToken) ||
      generationToken !== input.generationToken
    )
      throw new Error("Redis stream initializer returned inconsistent evidence");
    return { created: true, sentinelId, generationToken };
  }

  async verifyInitialization(input: {
    sentinelId: string;
    generationToken: string;
    signal: AbortSignal;
  }): Promise<boolean> {
    if (!isCanonicalDeliveryStreamGeneration(input.generationToken))
      throw new Error("Redis initialization verification received an invalid generation token");
    const response = asArray(
      await this.requireControl().sendCommand(
        ["XRANGE", this.streamKey, input.sentinelId, input.sentinelId, "COUNT", "1"],
        { abortSignal: input.signal },
      ),
      "Redis initialization verification returned an invalid response",
    );
    if (response.length !== 1) return false;
    const sentinel = parseEntry(response[0]);
    return (
      sentinel.id === input.sentinelId &&
      sentinel.fields.length === 2 &&
      sentinel.fields[0]?.[0] === DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD &&
      sentinel.fields[0]?.[1] === DELIVERY_STREAM_INITIALIZATION_TYPE &&
      sentinel.fields[1]?.[0] === DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD &&
      sentinel.fields[1]?.[1] === input.generationToken
    );
  }

  async inspect(input: { signal: AbortSignal }): Promise<DeliveryStreamMetadata> {
    const control = this.requireControl();
    const incarnation = parseRunId(
      await control.sendCommand(["INFO", "SERVER"], { abortSignal: input.signal }),
    );
    let response: unknown;
    try {
      response = await control.sendCommand(["XINFO", "STREAM", this.streamKey], {
        abortSignal: input.signal,
      });
    } catch (error) {
      const exists = await control.sendCommand(["EXISTS", this.streamKey], {
        abortSignal: input.signal,
      });
      if (parseRedisUint64Reply(exists, "Redis EXISTS returned an invalid response") === "0")
        return absentMetadata(incarnation);
      throw error;
    }
    const info = flatRecord(response, "Redis XINFO STREAM returned an invalid response");
    return {
      exists: true,
      length: parseRedisUint64Reply(info.get("length"), "Redis XINFO STREAM omitted length"),
      firstEntryId: parseEntryId(info.get("first-entry")),
      lastEntryId: parseEntryId(info.get("last-entry")),
      lastGeneratedId: asString(
        info.get("last-generated-id"),
        "Redis XINFO STREAM omitted last-generated-id",
      ),
      maxDeletedEntryId: asString(
        info.get("max-deleted-entry-id") ?? ZERO_STREAM_ID,
        "Redis XINFO STREAM returned an invalid max-deleted-entry-id",
      ),
      entriesAdded: parseRedisUint64Reply(
        info.get("entries-added"),
        "Redis XINFO STREAM omitted entries-added",
      ),
      incarnation,
    };
  }

  async readAfter(input: {
    cursor: string;
    count: typeof REDIS_DELIVERY_READ_COUNT;
    blockMs: typeof REDIS_DELIVERY_BLOCK_MS;
    signal: AbortSignal;
    onIssued: () => void;
  }): Promise<readonly RawDeliveryStreamEntry[]> {
    const reader = this.requireReader();
    const command = reader.sendCommand(
      [
        "XREAD",
        "COUNT",
        String(input.count),
        "BLOCK",
        String(input.blockMs),
        "STREAMS",
        this.streamKey,
        input.cursor,
      ],
      { abortSignal: input.signal },
    );
    input.onIssued();
    const response = await command;
    if (response === null) return [];
    const streams = asArray(response, "Redis XREAD returned an invalid response");
    if (streams.length !== 1) throw new Error("Redis XREAD returned an unexpected stream count");
    const stream = asArray(streams[0], "Redis XREAD returned an invalid stream tuple");
    if (
      stream.length !== 2 ||
      asString(stream[0], "Redis XREAD omitted stream key") !== this.streamKey
    )
      throw new Error("Redis XREAD returned an unexpected stream");
    return asArray(stream[1], "Redis XREAD returned invalid entries").map(parseEntry);
  }

  cancelRead(): Promise<void> {
    const reader = this.reader;
    this.reader = undefined;
    if (!reader) return Promise.resolve();
    reader.removeListener("error", this.errorListener);
    if (reader.isOpen) reader.destroy();
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closePromise ??= this.closeOnce();
    return this.closePromise;
  }

  private createRedisClient(): RedisClient {
    const client = createResp2Client(this.redisUrl);
    client.on("error", this.errorListener);
    return client;
  }

  private async connectOnce(): Promise<void> {
    await this.cancelRead();
    const oldControl = this.control;
    this.control = undefined;
    if (oldControl) {
      oldControl.removeListener("error", this.errorListener);
      if (oldControl.isOpen) oldControl.destroy();
    }
    const control = this.createRedisClient();
    const reader = this.createRedisClient();
    this.control = control;
    this.reader = reader;
    try {
      await Promise.all([control.connect(), reader.connect()]);
    } catch (error) {
      await this.destroyClients();
      throw error;
    }
  }

  private requireControl(): RedisClient {
    if (!this.control?.isReady) throw new Error("Redis delivery control connection is unavailable");
    return this.control;
  }

  private requireReader(): RedisClient {
    if (!this.reader?.isReady) throw new Error("Redis delivery read connection is unavailable");
    return this.reader;
  }

  private destroyClients(): Promise<void> {
    const control = this.control;
    const reader = this.reader;
    this.control = undefined;
    this.reader = undefined;
    for (const client of [reader, control]) {
      if (!client) continue;
      client.removeListener("error", this.errorListener);
      if (client.isOpen) client.destroy();
    }
    return Promise.resolve();
  }

  private async closeOnce(): Promise<void> {
    this.closed = true;
    await this.destroyClients();
  }
}
