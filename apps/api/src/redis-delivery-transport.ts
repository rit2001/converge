import { createClient, RESP_TYPES } from "redis";
import {
  DELIVERY_ENVELOPE_MAX_BYTES,
  DELIVERY_STREAM_DECODED_ENTRY_MAX_BYTES,
  DELIVERY_STREAM_ENTRY_MAX_BYTES,
  REDIS_STREAM_ENTRY_ID_MAX_BYTES,
  decodeDeliveryStreamFieldPairs,
  deliveryStreamFieldsSchema,
  redisStreamEntryIdSchema,
  validateDeliveryStreamEntrySize,
} from "@converge/protocol";
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
  DELIVERY_STREAM_INITIALIZATION_ENTRY_MAX_BYTES,
  isCanonicalDeliveryStreamGeneration,
} from "./delivery-stream-sentinel.js";

export {
  DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD,
  DELIVERY_STREAM_INITIALIZATION_TYPE,
  DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD,
} from "./delivery-stream-sentinel.js";

const UINT64_MAXIMUM = 18_446_744_073_709_551_615n;
const XINFO_STREAM_FIELD_NAMES = Object.freeze([
  "length",
  "radix-tree-keys",
  "radix-tree-nodes",
  "last-generated-id",
  "max-deleted-entry-id",
  "entries-added",
  "recorded-first-entry-id",
  "groups",
  "first-entry",
  "last-entry",
] as const);
const XINFO_STREAM_COUNTER_FIELDS = Object.freeze([
  "length",
  "radix-tree-keys",
  "radix-tree-nodes",
  "entries-added",
  "groups",
] as const);
const XINFO_STREAM_ID_FIELDS = Object.freeze([
  "last-generated-id",
  "max-deleted-entry-id",
  "recorded-first-entry-id",
] as const);
const REDIS_UINT64_MAX_DECIMAL_BYTES = 20;
export const REDIS_DELIVERY_XINFO_FIXED_SCALAR_MAX_BYTES =
  XINFO_STREAM_FIELD_NAMES.reduce((total, field) => total + field.length, 0) +
  XINFO_STREAM_COUNTER_FIELDS.length * REDIS_UINT64_MAX_DECIMAL_BYTES +
  XINFO_STREAM_ID_FIELDS.length * REDIS_STREAM_ENTRY_ID_MAX_BYTES;
export const REDIS_DELIVERY_XINFO_RESP_OVERHEAD_MAX_BYTES = 2_048;
export const REDIS_DELIVERY_XINFO_AUTHORIZED_RESPONSE_MAX_BYTES =
  2 * DELIVERY_STREAM_DECODED_ENTRY_MAX_BYTES +
  REDIS_DELIVERY_XINFO_FIXED_SCALAR_MAX_BYTES +
  REDIS_DELIVERY_XINFO_RESP_OVERHEAD_MAX_BYTES;
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

const VERIFY_INITIALIZATION_SCRIPT = `
local function is_canonical_generation(value)
  if type(value) ~= 'string' or string.len(value) ~= 36 then return false end
  if string.sub(value, 9, 9) ~= '-' or
     string.sub(value, 14, 14) ~= '-' or
     string.sub(value, 19, 19) ~= '-' or
     string.sub(value, 24, 24) ~= '-' then return false end
  if string.sub(value, 15, 15) ~= '4' or
     not string.match(string.sub(value, 20, 20), '^[89ab]$') then return false end
  local compact = string.gsub(value, '-', '')
  return string.len(compact) == 32 and string.match(compact, '^[0-9a-f]+$') ~= nil
end

local function is_bounded_id(value)
  if type(value) ~= 'string' or string.len(value) < 3 or string.len(value) > 41 then return false end
  local milliseconds, sequence = string.match(value, '^(%d+)%-(%d+)$')
  if not milliseconds or not sequence then return false end
  if (#milliseconds > 1 and string.sub(milliseconds, 1, 1) == '0') or
     (#sequence > 1 and string.sub(sequence, 1, 1) == '0') then return false end
  return #milliseconds <= 20 and #sequence <= 20
end

if not is_bounded_id(ARGV[1]) or not is_canonical_generation(ARGV[2]) then return {0} end
local entry = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1], 'COUNT', 1)
if #entry ~= 1 or entry[1][1] ~= ARGV[1] then return {0} end
local fields = entry[1][2]
if #fields ~= 4 or
   fields[1] ~= '${DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD}' or
   fields[2] ~= '${DELIVERY_STREAM_INITIALIZATION_TYPE}' or
   fields[3] ~= '${DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD}' or
   fields[4] ~= ARGV[2] then return {0} end
return {1}
`;

function createResp2Client(redisUrl: string) {
  return createClient({
    url: redisUrl,
    // RESP2 preserves stream field lists as ordered pairs. RESP3 represents a stream message as a
    // map and would erase duplicate field names before the consumer can reject them.
    RESP: 2,
    socket: { reconnectStrategy: false },
  }).withTypeMapping({
    // Preserve script statuses and direct XINFO counters as exact canonical decimal strings.
    // No metadata counter passes through JavaScript Number or Redis Lua's double representation.
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

function parseRunId(info: unknown): string {
  if (typeof info !== "string") throw new Error("Redis INFO SERVER returned invalid evidence");
  const runId = /^run_id:([0-9a-f]{40})$/m.exec(info)?.[1];
  if (!runId) throw new Error("Redis INFO SERVER returned invalid evidence");
  return runId;
}

interface BoundedMetadataEntry {
  entry: RawDeliveryStreamEntry;
  encodedBytes: number;
}

function invalidXinfoEvidence(): Error {
  return new Error("Redis XINFO STREAM returned invalid bounded evidence");
}

function isInitializationSentinel(entry: RawDeliveryStreamEntry): boolean {
  return (
    entry.fields.length === 2 &&
    entry.fields[0]?.[0] === DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD &&
    entry.fields[0]?.[1] === DELIVERY_STREAM_INITIALIZATION_TYPE &&
    entry.fields[1]?.[0] === DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD &&
    isCanonicalDeliveryStreamGeneration(entry.fields[1]?.[1])
  );
}

function parseBoundedMetadataEntry(value: unknown): BoundedMetadataEntry | null {
  if (value === null) return null;
  let entry: RawDeliveryStreamEntry;
  try {
    entry = parseEntry(value);
  } catch {
    throw invalidXinfoEvidence();
  }
  if (!redisStreamEntryIdSchema.safeParse(entry.id).success) throw invalidXinfoEvidence();
  const idBytes = Buffer.byteLength(entry.id, "utf8");
  if (isInitializationSentinel(entry))
    return {
      entry,
      encodedBytes: idBytes + DELIVERY_STREAM_INITIALIZATION_ENTRY_MAX_BYTES,
    };

  try {
    decodeDeliveryStreamFieldPairs(entry.fields, DELIVERY_ENVELOPE_MAX_BYTES);
  } catch {
    throw invalidXinfoEvidence();
  }
  const fields = deliveryStreamFieldsSchema.safeParse(Object.fromEntries(entry.fields));
  if (!fields.success) throw invalidXinfoEvidence();
  const size = validateDeliveryStreamEntrySize(fields.data);
  if (!size.valid || size.entryBytes > DELIVERY_STREAM_ENTRY_MAX_BYTES)
    throw invalidXinfoEvidence();
  return { entry, encodedBytes: idBytes + size.entryBytes };
}

function entriesEqual(left: RawDeliveryStreamEntry, right: RawDeliveryStreamEntry): boolean {
  return (
    left.id === right.id &&
    left.fields.length === right.fields.length &&
    left.fields.every(
      ([name, value], index) =>
        right.fields[index]?.[0] === name && right.fields[index]?.[1] === value,
    )
  );
}

function requiredXinfoValue(info: Map<string, unknown>, field: string): unknown {
  if (!info.has(field)) throw invalidXinfoEvidence();
  return info.get(field);
}

function parseXinfoMetadata(response: unknown, incarnation: string): DeliveryStreamMetadata {
  let info: Map<string, unknown>;
  try {
    info = flatRecord(response, "Redis XINFO STREAM returned invalid bounded evidence");
  } catch {
    throw invalidXinfoEvidence();
  }
  if (
    info.size !== XINFO_STREAM_FIELD_NAMES.length ||
    XINFO_STREAM_FIELD_NAMES.some((field) => !info.has(field))
  )
    throw invalidXinfoEvidence();

  const counters = new Map<string, string>();
  for (const field of XINFO_STREAM_COUNTER_FIELDS)
    counters.set(
      field,
      parseRedisUint64Reply(requiredXinfoValue(info, field), "Redis XINFO counter is invalid"),
    );
  if (counters.get("groups") !== "0") throw invalidXinfoEvidence();

  const ids = new Map<string, string>();
  for (const field of XINFO_STREAM_ID_FIELDS) {
    const id = asBulkString(requiredXinfoValue(info, field), "Redis XINFO stream ID is invalid");
    if (!redisStreamEntryIdSchema.safeParse(id).success && id !== ZERO_STREAM_ID)
      throw invalidXinfoEvidence();
    ids.set(field, id);
  }

  const first = parseBoundedMetadataEntry(requiredXinfoValue(info, "first-entry"));
  const last = parseBoundedMetadataEntry(requiredXinfoValue(info, "last-entry"));
  const length = counters.get("length");
  if (!length || (length === "0") !== (first === null && last === null))
    throw invalidXinfoEvidence();
  if (length !== "0" && (first === null || last === null)) throw invalidXinfoEvidence();
  if (first && last && first.entry.id === last.entry.id && !entriesEqual(first.entry, last.entry))
    throw invalidXinfoEvidence();

  // Account a single retained entry once when Redis repeats it as both first-entry and last-entry.
  const uniqueEntryBytes =
    (first?.encodedBytes ?? 0) +
    (last && last.entry.id !== first?.entry.id ? last.encodedBytes : 0);
  if (uniqueEntryBytes > 2 * DELIVERY_STREAM_DECODED_ENTRY_MAX_BYTES) throw invalidXinfoEvidence();

  return {
    exists: true,
    length,
    firstEntryId: first?.entry.id ?? null,
    lastEntryId: last?.entry.id ?? null,
    lastGeneratedId: ids.get("last-generated-id") ?? ZERO_STREAM_ID,
    maxDeletedEntryId: ids.get("max-deleted-entry-id") ?? ZERO_STREAM_ID,
    entriesAdded: counters.get("entries-added") ?? "0",
    incarnation,
  };
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
    if (!redisStreamEntryIdSchema.safeParse(input.sentinelId).success)
      throw new Error("Redis initialization verification received an invalid sentinel ID");
    const response = asArray(
      await this.requireControl().sendCommand(
        [
          "EVAL",
          VERIFY_INITIALIZATION_SCRIPT,
          "1",
          this.streamKey,
          input.sentinelId,
          input.generationToken,
        ],
        { abortSignal: input.signal },
      ),
      "Redis initialization verification returned an invalid response",
    );
    return (
      response.length === 1 &&
      parseRedisUint64Reply(
        response[0],
        "Redis initialization verification returned an invalid status",
      ) === "1"
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
      const exists = parseRedisUint64Reply(
        await control.sendCommand(["EXISTS", this.streamKey], { abortSignal: input.signal }),
        "Redis EXISTS returned invalid evidence",
      );
      if (exists !== "0") throw error;
      const confirmedIncarnation = parseRunId(
        await control.sendCommand(["INFO", "SERVER"], { abortSignal: input.signal }),
      );
      if (confirmedIncarnation !== incarnation)
        throw new Error("Redis metadata commands observed conflicting server incarnations");
      return absentMetadata(incarnation);
    }
    const confirmedIncarnation = parseRunId(
      await control.sendCommand(["INFO", "SERVER"], { abortSignal: input.signal }),
    );
    if (confirmedIncarnation !== incarnation)
      throw new Error("Redis metadata commands observed conflicting server incarnations");
    return parseXinfoMetadata(response, incarnation);
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
