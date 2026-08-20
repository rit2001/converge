import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";
import type { PresenceAvailability } from "@converge/protocol";
import {
  PRESENCE_RECONNECT_BASE_DELAY_MS,
  RedisPresenceTransport,
  type PresenceRedisTransportScheduler,
} from "./presence-redis-transport.js";

class Scheduler implements PresenceRedisTransportScheduler {
  readonly timers = new Map<number, () => void>();
  readonly delays: number[] = [];
  private next = 1;
  setTimeout(callback: () => void, delayMs: number): number {
    const handle = this.next++;
    this.timers.set(handle, callback);
    this.delays.push(delayMs);
    return handle;
  }
  clearTimeout(handle: unknown): void {
    this.timers.delete(handle as number);
  }
  runNext(): void {
    const [handle, callback] = this.timers.entries().next().value as [number, () => void];
    this.timers.delete(handle);
    callback();
  }
}

class Client extends EventEmitter {
  isOpen = false;
  isReady = false;
  readonly connect = vi.fn(() => {
    if (this.failConnect) return Promise.reject(new Error("redis unavailable"));
    this.isOpen = true;
    this.isReady = true;
    return Promise.resolve();
  });
  readonly subscribe = vi.fn((_channel: string, callback: (message: string) => void) => {
    this.message = callback;
    return Promise.resolve();
  });
  readonly destroy = vi.fn(() => {
    this.isOpen = false;
    this.isReady = false;
  });
  message: ((message: string) => void) | undefined;
  failConnect = false;
  duplicate!: () => Client;
}

function clients(failFirst = false): { create: (url: string) => never; all: Client[] } {
  const all: Client[] = [];
  const create = (): Client => {
    const command = new Client();
    command.failConnect = failFirst && all.length === 0;
    command.duplicate = () => {
      const duplicate = new Client();
      all.push(duplicate);
      return duplicate;
    };
    all.push(command);
    return command;
  };
  return { create: create as never, all };
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("RedisPresenceTransport reconnection supervisor", () => {
  it("reproduces an initial unavailable attempt then automatically establishes a fresh generation", async () => {
    const scheduler = new Scheduler();
    const source = clients(true);
    const transport = new RedisPresenceTransport("redis://unused", {
      createClient: source.create,
      scheduler,
      random: () => 1,
    });
    const availability: PresenceAvailability["status"][] = [];
    transport.onAvailability((event) => availability.push(event.status));

    await expect(transport.start()).resolves.toMatchObject({ kind: "unavailable" });
    expect(scheduler.timers.size).toBe(1);
    expect(scheduler.delays).toEqual([PRESENCE_RECONNECT_BASE_DELAY_MS]);
    scheduler.runNext();
    await settle();

    expect(availability).toEqual(["unavailable", "available"]);
    expect(source.all).toHaveLength(6);
    expect(source.all.slice(0, 3).every((client) => client.destroy.mock.calls.length === 1)).toBe(
      true,
    );
    await transport.stop();
  });

  it("fences repeated loss callbacks and recovers an established command generation once", async () => {
    const scheduler = new Scheduler();
    const source = clients();
    const transport = new RedisPresenceTransport("redis://unused", {
      createClient: source.create,
      scheduler,
      random: () => 0,
    });
    const availability: PresenceAvailability["status"][] = [];
    transport.onAvailability((event) => availability.push(event.status));
    await expect(transport.start()).resolves.toMatchObject({ kind: "ok" });

    source.all[0]!.emit("error", new Error("lost"));
    source.all[0]!.emit("end");
    expect(availability).toEqual(["available", "unavailable"]);
    expect(scheduler.timers.size).toBe(1);
    scheduler.runNext();
    await settle();

    expect(availability).toEqual(["available", "unavailable", "available"]);
    expect(source.all).toHaveLength(6);
    await transport.stop();
    expect(scheduler.timers.size).toBe(0);
  });
});
