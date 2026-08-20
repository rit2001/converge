import { createClient } from "redis";
import { afterEach, describe, expect, it } from "vitest";
import { PRESENCE_SNAPSHOT_MAX_SESSIONS } from "@converge/protocol";
import { RedisPresenceTransport, boardKeys } from "./presence-redis-transport.js";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const board = "10000000-0000-4000-8000-000000000001";
const principal = { userId: "20000000-0000-4000-8000-000000000002", displayName: "Ada" };
const transports: RedisPresenceTransport[] = [];
const session = (value: number) => `${String(value).padStart(8, "0")}-0000-4000-8000-000000000003`;

afterEach(async () => {
  await Promise.all(transports.splice(0).map((transport) => transport.stop()));
  const client = createClient({ url: redisUrl });
  await client.connect();
  const keys = [
    boardKeys(board)?.index,
    ...Array.from({ length: 101 }, (_, index) => boardKeys(board, session(index + 1))).flatMap(
      (value) => [value?.session, value?.tombstone],
    ),
  ].filter((value): value is string => Boolean(value));
  await client.del(keys);
  client.destroy();
});

describe("RedisPresenceTransport", () => {
  it("derives isolated hash-tagged presence keys", () => {
    expect(boardKeys(board, session(1))).toMatchObject({
      index: `converge:presence:v1:{${board}}:sessions`,
    });
    expect(boardKeys("not-a-uuid")).toBeNull();
  });

  it("atomically admits, refreshes, snapshots, leaves, and exchanges validated deltas", async () => {
    const first = new RedisPresenceTransport(redisUrl);
    const second = new RedisPresenceTransport(redisUrl);
    transports.push(first, second);
    expect((await first.start()).kind).toBe("ok");
    expect((await second.start()).kind).toBe("ok");
    const deltas: string[] = [];
    second.onDelta((delta) =>
      deltas.push("participant" in delta ? `u${delta.participant.revision}` : `l${delta.revision}`),
    );
    const admitted = await first.admit({
      boardId: board,
      presenceSessionId: session(1),
      principal,
      cursor: { x: 1, y: 2 },
      activity: "active",
    });
    expect(admitted).toMatchObject({ kind: "ok", value: { revision: 1 } });
    const refreshed = await first.refresh({
      boardId: board,
      presenceSessionId: session(1),
      principal,
      cursor: null,
      activity: "idle",
    });
    expect(refreshed).toMatchObject({ kind: "ok", value: { revision: 2, activity: "idle" } });
    expect(await first.snapshot(board)).toMatchObject({
      kind: "ok",
      value: { participants: [expect.objectContaining({ revision: 2 })] },
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(deltas).toContain("u1");
    expect(deltas).toContain("u2");
    expect(await first.leave(board, session(1), principal)).toMatchObject({
      kind: "ok",
      value: { revision: 3 },
    });
    expect(await first.leave(board, session(1), principal)).toMatchObject({
      kind: "ok",
      value: null,
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(deltas).toContain("l3");
  });

  it("enforces capacity while permitting an admitted session to refresh", async () => {
    const transport = new RedisPresenceTransport(redisUrl);
    transports.push(transport);
    expect((await transport.start()).kind).toBe("ok");
    for (let index = 1; index <= PRESENCE_SNAPSHOT_MAX_SESSIONS; index++)
      expect(
        (
          await transport.admit({
            boardId: board,
            presenceSessionId: session(index),
            principal,
            cursor: null,
            activity: "active",
          })
        ).kind,
      ).toBe("ok");
    expect(
      (
        await transport.admit({
          boardId: board,
          presenceSessionId: session(101),
          principal,
          cursor: null,
          activity: "active",
        })
      ).kind,
    ).toBe("capacity");
    expect(
      await transport.refresh({
        boardId: board,
        presenceSessionId: session(1),
        principal,
        cursor: null,
        activity: "idle",
      }),
    ).toMatchObject({ kind: "ok", value: { revision: 2 } });
  }, 15_000);

  it("keeps concurrent admissions bounded and cannot resurrect after a concurrent leave", async () => {
    const transport = new RedisPresenceTransport(redisUrl);
    transports.push(transport);
    expect((await transport.start()).kind).toBe("ok");
    const outcomes = await Promise.all(
      Array.from({ length: PRESENCE_SNAPSHOT_MAX_SESSIONS + 20 }, (_, index) =>
        transport.admit({
          boardId: board,
          presenceSessionId: session(index + 1),
          principal,
          cursor: null,
          activity: "active",
        }),
      ),
    );
    expect(outcomes.filter((outcome) => outcome.kind === "ok")).toHaveLength(
      PRESENCE_SNAPSHOT_MAX_SESSIONS,
    );
    expect(outcomes.filter((outcome) => outcome.kind === "capacity")).toHaveLength(20);
    await Promise.all([
      transport.refresh({
        boardId: board,
        presenceSessionId: session(1),
        principal,
        cursor: { x: 2, y: 3 },
        activity: "active",
      }),
      transport.leave(board, session(1), principal),
    ]);
    const snapshot = await transport.snapshot(board);
    expect(snapshot).toMatchObject({ kind: "ok" });
    if (snapshot.kind === "ok")
      expect(
        snapshot.value.participants.some(
          ({ presenceSessionId }) => presenceSessionId === session(1),
        ),
      ).toBe(false);
  }, 15_000);
});
