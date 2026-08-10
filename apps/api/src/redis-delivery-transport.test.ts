import { describe, expect, it, vi } from "vitest";

const redisMock = vi.hoisted(() => {
  let numberReply: ((value: string) => unknown) | undefined;
  let initializerReply: unknown[] | undefined;
  const commands: string[][] = [];
  return {
    reset(): void {
      numberReply = undefined;
      initializerReply = undefined;
      commands.length = 0;
    },
    mapNumber(mapping: Record<number, (value: string) => unknown>): void {
      numberReply = mapping[58];
    },
    reply(value: string): unknown {
      return numberReply ? numberReply(value) : Number(value);
    },
    setInitializerReply(value: unknown[]): void {
      initializerReply = value;
    },
    initializerReply(): unknown[] | undefined {
      return initializerReply;
    },
    record(command: readonly string[]): void {
      commands.push([...command]);
    },
    commands(): readonly (readonly string[])[] {
      return commands;
    },
  };
});

vi.mock("redis", () => ({
  RESP_TYPES: { NUMBER: 58 },
  createClient: () => {
    const client = {
      isOpen: false,
      isReady: false,
      on: () => client,
      removeListener: () => client,
      withTypeMapping: (mapping: Record<number, (value: string) => unknown>) => {
        redisMock.mapNumber(mapping);
        return client;
      },
      connect: async () => {
        await Promise.resolve();
        client.isOpen = true;
        client.isReady = true;
      },
      destroy: () => {
        client.isOpen = false;
        client.isReady = false;
      },
      sendCommand: async (command: readonly string[]) => {
        await Promise.resolve();
        redisMock.record(command);
        if (command[0] === "EVAL" && command[1]?.includes("exact_entries_added"))
          return [
            redisMock.reply("1"),
            "a".repeat(40),
            "0",
            "",
            "",
            "1-0",
            "1-0",
            "9007199254740993",
          ];
        if (command[0] === "EVAL" && command.length === 6) return [redisMock.reply("1")];
        if (command[0] === "EVAL")
          return redisMock.initializerReply() ?? [redisMock.reply("1"), "5-0", command.at(-1)];
        throw new Error(`Unexpected Redis command ${command[0]}`);
      },
    };
    return client;
  },
}));

import {
  RedisDeliveryConsumerTransport,
  parseRedisUint64Reply,
} from "./redis-delivery-transport.js";
import { DELIVERY_STREAM_INITIALIZATION_ENTRY_MAX_BYTES } from "./delivery-stream-sentinel.js";

describe("RedisDeliveryConsumerTransport", () => {
  const canonicalGeneration = "10000000-0000-4000-8000-000000000001";

  it("keeps the initialization sentinel fixed and byte bounded", () => {
    const encodedBytes = new TextEncoder().encode(
      "controlType" + "converge.stream.initialized.v1" + "generation" + canonicalGeneration,
    ).byteLength;

    expect(DELIVERY_STREAM_INITIALIZATION_ENTRY_MAX_BYTES).toBe(87);
    expect(encodedBytes).toBe(DELIVERY_STREAM_INITIALIZATION_ENTRY_MAX_BYTES);
  });

  it("uses one atomic Redis-side initializer and verifies the exact sentinel", async () => {
    redisMock.reset();
    const transport = new RedisDeliveryConsumerTransport(
      "redis://test",
      "converge:test:m24a:initializer",
    );
    await transport.connect();

    const initialization = await transport.initializeStream({
      generationToken: canonicalGeneration,
      signal: new AbortController().signal,
    });
    const verified = await transport.verifyInitialization({
      sentinelId: "5-0",
      generationToken: canonicalGeneration,
      signal: new AbortController().signal,
    });

    expect(initialization).toEqual({
      created: true,
      sentinelId: "5-0",
      generationToken: canonicalGeneration,
    });
    expect(verified).toBe(true);
    const initializer = redisMock.commands().find(([name]) => name === "EVAL");
    expect(initializer).toMatchObject([
      "EVAL",
      expect.stringContaining("redis.call('EXISTS', KEYS[1])"),
      "1",
      "converge:test:m24a:initializer",
      canonicalGeneration,
    ]);
    expect(redisMock.commands().filter(([name]) => name === "EVAL")).toHaveLength(2);
    expect(redisMock.commands().some(([name]) => name === "XRANGE")).toBe(false);
    await transport.close();
  });

  it.each([
    "not-a-uuid",
    "10000000-0000-4000-8000-00000000000A",
    "",
    " 10000000-0000-4000-8000-000000000001",
    "10000000-0000-4000-8000-000000000001 ",
    "a".repeat(100_000),
  ])("rejects a noncanonical locally supplied sentinel generation", async (generationToken) => {
    redisMock.reset();
    const transport = new RedisDeliveryConsumerTransport(
      "redis://test",
      "converge:test:m24a:invalid-local-generation",
    );
    await transport.connect();

    await expect(
      transport.initializeStream({ generationToken, signal: new AbortController().signal }),
    ).rejects.toThrow("invalid generation token");
    expect(redisMock.commands().some(([name]) => name === "EVAL")).toBe(false);
    await transport.close();
  });

  it("rejects a noncanonical generation before initialization verification", async () => {
    redisMock.reset();
    const transport = new RedisDeliveryConsumerTransport(
      "redis://test",
      "converge:test:m24a:invalid-verification-generation",
    );
    await transport.connect();

    await expect(
      transport.verifyInitialization({
        sentinelId: "5-0",
        generationToken: "not-a-uuid",
        signal: new AbortController().signal,
      }),
    ).rejects.toThrow("invalid generation token");
    expect(redisMock.commands().some(([name]) => name === "EVAL")).toBe(false);
    await transport.close();
  });

  it.each([
    "not-a-uuid",
    "10000000-0000-4000-8000-00000000000A",
    "",
    " 10000000-0000-4000-8000-000000000001",
    "10000000-0000-4000-8000-000000000001 ",
    "a".repeat(100_000),
  ])("rejects a noncanonical observed sentinel generation", async (generationToken) => {
    redisMock.reset();
    redisMock.setInitializerReply([redisMock.reply("0"), "5-0", generationToken]);
    const transport = new RedisDeliveryConsumerTransport(
      "redis://test",
      "converge:test:m24a:invalid-observed-generation",
    );
    await transport.connect();

    await expect(
      transport.initializeStream({
        generationToken: canonicalGeneration,
        signal: new AbortController().signal,
      }),
    ).rejects.toThrow("invalid generation token");
    await transport.close();
  });

  it.each(["9007199254740991", "9007199254740992", "9007199254740993", "18446744073709551615"])(
    "accepts the exact canonical uint64 reply %s",
    (value) => {
      expect(parseRedisUint64Reply(value, "invalid counter")).toBe(value);
    },
  );

  it.each([
    -1,
    1.5,
    9_007_199_254_740_992,
    NaN,
    Infinity,
    "-1",
    "1.5",
    "1e3",
    " 1",
    "1 ",
    "01",
    "18446744073709551616",
    "NaN",
    "Infinity",
  ])("rejects a non-canonical or unsafe uint64 reply %s", (value) => {
    expect(() => parseRedisUint64Reply(value, "invalid counter")).toThrow("invalid counter");
  });

  it("preserves an unsafe RESP integer exactly through production reply mapping", async () => {
    redisMock.reset();
    const transport = new RedisDeliveryConsumerTransport("redis://test", "converge:test:m24a:map");
    await transport.connect();

    const metadata = await transport.inspect({ signal: new AbortController().signal });

    expect(metadata.entriesAdded).toBe("9007199254740993");
    const inspection = redisMock
      .commands()
      .find(([name, script]) => name === "EVAL" && script?.includes("exact_entries_added"));
    expect(inspection?.[1]).toContain("first_id = first_entry[1]");
    expect(inspection?.[1]).not.toContain("first_entry[2]");
    expect(redisMock.commands().some(([name]) => name === "XINFO")).toBe(false);
    await transport.close();
  });
});
