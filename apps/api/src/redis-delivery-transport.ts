import { createClient } from "redis";
import type {
  REDIS_DELIVERY_BLOCK_MS,
  REDIS_DELIVERY_READ_COUNT,
  DeliveryConsumerTransport,
  DeliveryStreamMetadata,
  RawDeliveryStreamEntry,
} from "./delivery-consumer.js";

function createResp2Client(redisUrl: string) {
  return createClient({
    url: redisUrl,
    // RESP2 preserves stream field lists as ordered pairs. RESP3 represents a stream message as a
    // map and would erase duplicate field names before the consumer can reject them.
    RESP: 2,
    socket: { reconnectStrategy: false },
  });
}

type RedisClient = ReturnType<typeof createResp2Client>;

const ZERO_STREAM_ID = "0-0";

function asArray(value: unknown, message: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(message);
  return value;
}

function asString(value: unknown, message: string): string {
  if (typeof value !== "string" && typeof value !== "number") throw new Error(message);
  return String(value);
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

  async inspect(): Promise<DeliveryStreamMetadata> {
    const control = this.requireControl();
    const incarnation = parseRunId(await control.sendCommand(["INFO", "SERVER"]));
    let response: unknown;
    try {
      response = await control.sendCommand(["XINFO", "STREAM", this.streamKey]);
    } catch (error) {
      const exists = await control.sendCommand(["EXISTS", this.streamKey]);
      if (asString(exists, "Redis EXISTS returned an invalid response") === "0")
        return absentMetadata(incarnation);
      throw error;
    }
    const info = flatRecord(response, "Redis XINFO STREAM returned an invalid response");
    return {
      exists: true,
      length: asString(info.get("length"), "Redis XINFO STREAM omitted length"),
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
      entriesAdded: asString(info.get("entries-added"), "Redis XINFO STREAM omitted entries-added"),
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
