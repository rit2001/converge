import type { StructuredLogger } from "@converge/observability";
import type { DeliveryStreamFields } from "@converge/protocol";
import { createClient, type RedisClientType } from "redis";

export interface DeliveryStream {
  connect(): Promise<void>;
  isReady(): boolean;
  append(fields: DeliveryStreamFields): Promise<unknown>;
  trimByAge(): Promise<void>;
  close(force?: boolean): Promise<void>;
}

export class RedisXaddRejectedError extends Error {}
export class RedisXaddAmbiguousError extends Error {}

function parseRedisTime(value: unknown): number {
  if (
    !Array.isArray(value) ||
    value.length !== 2 ||
    typeof value[0] !== "string" ||
    typeof value[1] !== "string"
  )
    throw new Error("Redis TIME returned an invalid response");
  const seconds = Number(value[0]);
  const microseconds = Number(value[1]);
  if (!Number.isSafeInteger(seconds) || !Number.isSafeInteger(microseconds))
    throw new Error("Redis TIME returned an invalid response");
  return seconds * 1_000 + Math.floor(microseconds / 1_000);
}

export class RedisDeliveryStream implements DeliveryStream {
  private readonly client: RedisClientType;

  constructor(
    redisUrl: string,
    private readonly streamKey: string,
    private readonly maximumLength: number,
    private readonly maximumAgeMs: number,
    logger: StructuredLogger,
  ) {
    this.client = createClient({ url: redisUrl });
    this.client.on("error", () =>
      logger.warn({ component: "redis", code: "REDIS_CLIENT_ERROR" }, "Redis client error"),
    );
  }

  async connect(): Promise<void> {
    if (!this.client.isOpen) await this.client.connect();
  }

  isReady(): boolean {
    return this.client.isReady;
  }

  async append(fields: DeliveryStreamFields): Promise<unknown> {
    if (!this.client.isReady) throw new RedisXaddRejectedError("Redis was unavailable before XADD");
    try {
      return await this.client.sendCommand([
        "XADD",
        this.streamKey,
        "MAXLEN",
        "~",
        String(this.maximumLength),
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
    } catch {
      throw new RedisXaddAmbiguousError("Redis XADD acceptance is unknown");
    }
  }

  async trimByAge(): Promise<void> {
    const redisTime = parseRedisTime(await this.client.sendCommand(["TIME"]));
    const minimumMilliseconds = Math.max(0, redisTime - this.maximumAgeMs);
    await this.client.sendCommand(["XTRIM", this.streamKey, "MINID", `${minimumMilliseconds}-0`]);
  }

  async close(force = false): Promise<void> {
    if (!this.client.isOpen) return;
    if (force) this.client.destroy();
    else await this.client.close();
  }
}
