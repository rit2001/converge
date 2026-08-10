import { createClient, RESP_TYPES } from "redis";
import { redisStreamEntryIdSchema } from "@converge/protocol";
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
const INVALID_PROJECTION_STATUS = "2";
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

const INSPECT_STREAM_SCRIPT = `
local function invalid() return {2} end

local function is_bounded_id(value)
  if type(value) ~= 'string' or string.len(value) < 3 or string.len(value) > 41 then return false end
  local milliseconds, sequence = string.match(value, '^(%d+)%-(%d+)$')
  if not milliseconds or not sequence then return false end
  if (#milliseconds > 1 and string.sub(milliseconds, 1, 1) == '0') or
     (#sequence > 1 and string.sub(sequence, 1, 1) == '0') then return false end
  if #milliseconds > 20 or #sequence > 20 then return false end
  local maximum = '18446744073709551615'
  if (#milliseconds == 20 and milliseconds > maximum) or
     (#sequence == 20 and sequence > maximum) then return false end
  return true
end

local function add_small(decimal, addition)
  local output = {}
  local carry = addition
  for index = #decimal, 1, -1 do
    local sum = string.byte(decimal, index) - 48 + (carry % 10)
    carry = math.floor(carry / 10)
    if sum >= 10 then
      sum = sum - 10
      carry = carry + 1
    end
    output[index] = string.char(48 + sum)
  end
  while carry > 0 do
    table.insert(output, 1, string.char(48 + (carry % 10)))
    carry = math.floor(carry / 10)
  end
  return table.concat(output)
end

local function exact_entries_added(stream_info, last_generated_id, group_suffix)
  local entries_added
  for index = 1, #stream_info, 2 do
    if stream_info[index] == 'entries-added' then entries_added = stream_info[index + 1] end
  end
  if type(entries_added) ~= 'number' or entries_added < 0 then return nil end
  if entries_added <= 9007199254740991 then return string.format('%.0f', entries_added) end

  -- Redis Lua 5.1 receives RESP integers as doubles. Recover the exact low bits from a temporary
  -- group lag whose ENTRIESREAD baseline is deliberately below the rounded double value.
  local baseline = string.format('%.0f', entries_added - 8192)
  local group = 'converge:metadata:' .. group_suffix
  local created = redis.pcall(
    'XGROUP', 'CREATE', KEYS[1], group, last_generated_id, 'ENTRIESREAD', baseline
  )
  if type(created) ~= 'table' or created.err then return nil end
  local groups = redis.pcall('XINFO', 'GROUPS', KEYS[1])
  local lag
  if type(groups) == 'table' and not groups.err then
    for _, candidate in ipairs(groups) do
      local name
      local candidate_lag
      for index = 1, #candidate, 2 do
        if candidate[index] == 'name' then name = candidate[index + 1] end
        if candidate[index] == 'lag' then candidate_lag = candidate[index + 1] end
      end
      if name == group then lag = candidate_lag end
    end
  end
  local destroyed = redis.pcall('XGROUP', 'DESTROY', KEYS[1], group)
  if destroyed ~= 1 or type(lag) ~= 'number' or lag < 0 or lag > 16384 or lag % 1 ~= 0 then
    return nil
  end
  return add_small(baseline, lag)
end

if type(ARGV[1]) ~= 'string' or
   string.len(ARGV[1]) ~= 36 or
   not string.match(ARGV[1], '^[0-9a-f%-]+$') then return invalid() end

local server = redis.pcall('INFO', 'SERVER')
if type(server) ~= 'string' then return invalid() end
local incarnation = string.match(server, '\\nrun_id:([0-9a-f]+)\\r?\\n') or
                    string.match(server, '^run_id:([0-9a-f]+)\\r?\\n')
if not incarnation or #incarnation ~= 40 then return invalid() end
if redis.call('EXISTS', KEYS[1]) == 0 then return {0, incarnation} end

local info = redis.pcall('XINFO', 'STREAM', KEYS[1])
if type(info) ~= 'table' or info.err then return invalid() end
local length
local first_entry
local last_entry
local last_generated_id
local max_deleted_entry_id = '0-0'
for index = 1, #info, 2 do
  local name = info[index]
  local value = info[index + 1]
  if name == 'length' then length = value end
  if name == 'first-entry' then first_entry = value end
  if name == 'last-entry' then last_entry = value end
  if name == 'last-generated-id' then last_generated_id = value end
  if name == 'max-deleted-entry-id' then max_deleted_entry_id = value end
end
if type(length) ~= 'number' or length < 0 or length > 9007199254740991 or length % 1 ~= 0 or
   not is_bounded_id(last_generated_id) or not is_bounded_id(max_deleted_entry_id) then
  return invalid()
end

local first_id = ''
local last_id = ''
if length == 0 then
  if first_entry or last_entry then return invalid() end
else
  if type(first_entry) ~= 'table' or type(last_entry) ~= 'table' or
     not is_bounded_id(first_entry[1]) or not is_bounded_id(last_entry[1]) then return invalid() end
  first_id = first_entry[1]
  last_id = last_entry[1]
end
local entries_added = exact_entries_added(info, last_generated_id, ARGV[1])
if not entries_added or #entries_added > 20 then return invalid() end
return {
  1,
  incarnation,
  string.format('%.0f', length),
  first_id,
  last_id,
  last_generated_id,
  max_deleted_entry_id,
  entries_added
}
`;

function createResp2Client(redisUrl: string) {
  return createClient({
    url: redisUrl,
    // RESP2 preserves stream field lists as ordered pairs. RESP3 represents a stream message as a
    // map and would erase duplicate field names before the consumer can reject them.
    RESP: 2,
    socket: { reconnectStrategy: false },
  }).withTypeMapping({
    // Preserve script statuses as strings. The metadata script itself emits counters as bounded
    // decimal bulk strings because Redis Lua receives XINFO integer replies as doubles.
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
    const response = asArray(
      await control.sendCommand(
        ["EVAL", INSPECT_STREAM_SCRIPT, "1", this.streamKey, crypto.randomUUID()],
        {
          abortSignal: input.signal,
        },
      ),
      "Redis stream metadata projection returned an invalid response",
    );
    const status = parseRedisUint64Reply(
      response[0],
      "Redis stream metadata projection returned an invalid status",
    );
    if (status === INVALID_PROJECTION_STATUS)
      throw new Error("Redis stream metadata projection returned invalid evidence");
    const incarnation = asBulkString(
      response[1],
      "Redis stream metadata projection omitted the incarnation",
    );
    if (status === "0") {
      if (response.length !== 2)
        throw new Error("Redis stream metadata projection returned inconsistent absence evidence");
      return absentMetadata(incarnation);
    }
    if (status !== "1" || response.length !== 8)
      throw new Error("Redis stream metadata projection returned invalid evidence");
    const length = parseRedisUint64Reply(
      response[2],
      "Redis stream metadata projection omitted length",
    );
    const firstEntryId = asBulkString(
      response[3],
      "Redis stream metadata projection omitted the first entry ID",
    );
    const lastEntryId = asBulkString(
      response[4],
      "Redis stream metadata projection omitted the last entry ID",
    );
    return {
      exists: true,
      length,
      firstEntryId: firstEntryId === "" ? null : firstEntryId,
      lastEntryId: lastEntryId === "" ? null : lastEntryId,
      lastGeneratedId: asBulkString(
        response[5],
        "Redis stream metadata projection omitted last-generated-id",
      ),
      maxDeletedEntryId: asBulkString(
        response[6],
        "Redis stream metadata projection omitted max-deleted-entry-id",
      ),
      entriesAdded: parseRedisUint64Reply(
        response[7],
        "Redis stream metadata projection omitted entries-added",
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
