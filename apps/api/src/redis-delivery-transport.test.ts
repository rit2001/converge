import { describe, expect, it, vi } from "vitest";

const redisMock = vi.hoisted(() => {
  let numberReply: ((value: string) => unknown) | undefined;
  const commands: string[][] = [];
  return {
    reset(): void {
      numberReply = undefined;
      commands.length = 0;
    },
    mapNumber(mapping: Record<number, (value: string) => unknown>): void {
      numberReply = mapping[58];
    },
    reply(value: string): unknown {
      return numberReply ? numberReply(value) : Number(value);
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
        if (command[0] === "INFO") return "run_id:test-run\r\n";
        if (command[0] === "XINFO")
          return [
            "length",
            redisMock.reply("0"),
            "last-generated-id",
            "1-0",
            "max-deleted-entry-id",
            "1-0",
            "entries-added",
            redisMock.reply("9007199254740993"),
            "first-entry",
            null,
            "last-entry",
            null,
          ];
        if (command[0] === "EVAL") return [redisMock.reply("1"), "5-0", command.at(-1)];
        if (command[0] === "XRANGE")
          return [
            [
              "5-0",
              ["controlType", "converge.stream.initialized.v1", "generation", "test-generation"],
            ],
          ];
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

describe("RedisDeliveryConsumerTransport", () => {
  it("uses one atomic Redis-side initializer and verifies the exact sentinel", async () => {
    redisMock.reset();
    const transport = new RedisDeliveryConsumerTransport(
      "redis://test",
      "converge:test:m24a:initializer",
    );
    await transport.connect();

    const initialization = await transport.initializeStream({
      generationToken: "test-generation",
      signal: new AbortController().signal,
    });
    const verified = await transport.verifyInitialization({
      sentinelId: "5-0",
      generationToken: "test-generation",
      signal: new AbortController().signal,
    });

    expect(initialization).toEqual({
      created: true,
      sentinelId: "5-0",
      generationToken: "test-generation",
    });
    expect(verified).toBe(true);
    const initializer = redisMock.commands().find(([name]) => name === "EVAL");
    expect(initializer).toMatchObject([
      "EVAL",
      expect.stringContaining("redis.call('EXISTS', KEYS[1])"),
      "1",
      "converge:test:m24a:initializer",
      "test-generation",
    ]);
    expect(redisMock.commands().filter(([name]) => name === "EVAL")).toHaveLength(1);
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
    await transport.close();
  });
});
