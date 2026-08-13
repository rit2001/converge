import type { StructuredLogger } from "@converge/observability";
import {
  deliveryStreamFieldsSchema,
  validateDeliveryStreamEntrySize,
  type DeliveryStreamFields,
} from "@converge/protocol";
import { createClient, type RedisClientType } from "redis";

export interface DeliveryStream {
  connect(): Promise<void>;
  isReady(): boolean;
  append(fields: DeliveryStreamFields, signal: AbortSignal): Promise<unknown>;
  trimByAge(signal: AbortSignal): Promise<void>;
  resetAfterCommandTimeout(): Promise<void>;
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
  private client: RedisClientType;
  private resetPromise: Promise<void> | undefined;
  private closing = false;

  constructor(
    private readonly redisUrl: string,
    private readonly streamKey: string,
    private readonly maximumLength: number,
    private readonly maximumAgeMs: number,
    private readonly logger: StructuredLogger,
  ) {
    this.client = this.createRedisClient();
  }

  async connect(): Promise<void> {
    if (!this.client.isOpen) await this.client.connect();
  }

  isReady(): boolean {
    return this.client.isReady;
  }

  async append(fields: DeliveryStreamFields, signal: AbortSignal): Promise<unknown> {
    if (!this.client.isReady) throw new RedisXaddRejectedError("Redis was unavailable before XADD");
    const validatedFields = deliveryStreamFieldsSchema.safeParse(fields);
    if (!validatedFields.success || !validateDeliveryStreamEntrySize(validatedFields.data).valid)
      throw new RedisXaddRejectedError("Redis delivery entry was rejected before XADD");
    const client = this.client;
    try {
      return await client.sendCommand(
        [
          "XADD",
          this.streamKey,
          "MAXLEN",
          "~",
          String(this.maximumLength),
          "*",
          "schemaVersion",
          validatedFields.data.schemaVersion,
          "eventId",
          validatedFields.data.eventId,
          "boardId",
          validatedFields.data.boardId,
          "deliverySeq",
          validatedFields.data.deliverySeq,
          "eventType",
          validatedFields.data.eventType,
          "event",
          validatedFields.data.event,
        ],
        { abortSignal: signal },
      );
    } catch {
      throw new RedisXaddAmbiguousError("Redis XADD acceptance is unknown");
    }
  }

  async trimByAge(signal: AbortSignal): Promise<void> {
    const client = this.client;
    const redisTime = parseRedisTime(await client.sendCommand(["TIME"], { abortSignal: signal }));
    const minimumMilliseconds = Math.max(0, redisTime - this.maximumAgeMs);
    await client.sendCommand(["XTRIM", this.streamKey, "MINID", `${minimumMilliseconds}-0`], {
      abortSignal: signal,
    });
  }

  resetAfterCommandTimeout(): Promise<void> {
    this.resetPromise ??= this.resetConnection().finally(() => {
      this.resetPromise = undefined;
    });
    return this.resetPromise;
  }

  async close(force = false): Promise<void> {
    this.closing = true;
    if (!this.client.isOpen) return;
    if (force) this.client.destroy();
    else await this.client.close();
  }

  private createRedisClient(): RedisClientType {
    const client = createClient({ url: this.redisUrl });
    client.on("error", () =>
      this.logger.warn({ component: "redis", code: "REDIS_CLIENT_ERROR" }, "Redis client error"),
    );
    return client;
  }

  private async resetConnection(): Promise<void> {
    const previous = this.client;
    if (previous.isOpen) previous.destroy();
    if (this.closing) return;
    const replacement = this.createRedisClient();
    this.client = replacement;
    try {
      await replacement.connect();
    } catch (error) {
      if (replacement.isOpen) replacement.destroy();
      throw error;
    }
  }
}
