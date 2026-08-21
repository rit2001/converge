import { describe, expect, it, vi } from "vitest";
import { PresenceStore } from "./presence-store";

const boardId = "10000000-0000-4000-8000-000000000001";
const user = "20000000-0000-4000-8000-000000000002";
const other = "30000000-0000-4000-8000-000000000003";
const session = "40000000-0000-4000-8000-000000000004";
const tab = "50000000-0000-4000-8000-000000000005";
const token = Object.freeze({ generation: 1, nonce: Symbol("presence") });
class Clock {
  value = 0;
  timers = new Map<number, () => void>();
  next = 1;
  now = () => this.value;
  setTimeout = (callback: () => void, delay: number) => {
    void delay;
    const id = this.next++;
    this.timers.set(id, callback);
    return id;
  };
  clearTimeout = (id: unknown) => {
    this.timers.delete(id as number);
  };
  advance(ms: number) {
    this.value += ms;
    for (const callback of [...this.timers.values()]) callback();
  }
}
const participant = (
  presenceSessionId = session,
  userId = user,
  revision = 1,
  activity: "active" | "idle" = "active",
  cursor: { x: number; y: number } | null = { x: 1, y: 2 },
) => ({
  presenceSessionId,
  userId,
  displayName: userId === user ? "Same name" : "Same name",
  revision,
  activity,
  cursor,
  observedAt: "2026-08-20T12:00:00.000Z",
  expiresAt: "2026-08-20T12:00:45.000Z",
});
const snapshot = (participants = [participant()], selfPresenceSessionId = session) => ({
  schemaVersion: 1 as const,
  boardId,
  observedAt: "2026-08-20T12:00:00.000Z",
  selfPresenceSessionId,
  participants,
});

describe("PresenceStore", () => {
  it("derives self only from the self session and groups same-user tabs", () => {
    const store = new PresenceStore(boardId, token);
    store.receiveSnapshot(
      snapshot([
        participant(),
        participant(tab, user, 2, "idle", null),
        participant("60000000-0000-4000-8000-000000000006", other),
      ]),
    );
    const state = store.snapshot();
    expect(state.selfUserId).toBe(user);
    expect(state.collaborators).toHaveLength(2);
    expect(state.collaborators.find((item) => item.self)).toMatchObject({
      label: "You",
      activity: "active",
      cursor: { x: 1, y: 2 },
    });
    expect(state.collaborators.find((item) => !item.self)?.displayName).toBe("Same name");
  });
  it("keeps presentation referentially stable until presence evidence changes", () => {
    const store = new PresenceStore(boardId, token);
    store.receiveSnapshot(snapshot());
    expect(store.snapshot()).toBe(store.snapshot());
    store.receiveAvailability({ schemaVersion: 1, boardId, status: "unavailable" });
    expect(store.snapshot().availability).toBe("unavailable");
  });
  it("fails closed for contradictory self evidence and tombstones prevent resurrection", () => {
    const store = new PresenceStore(boardId, token);
    store.receiveSnapshot(snapshot([participant()], tab));
    expect(store.snapshot().availability).toBe("unavailable");
    store.receiveSnapshot(snapshot());
    store.receiveLeave({
      schemaVersion: 1,
      boardId,
      presenceSessionId: session,
      revision: 2,
      reason: "left",
      observedAt: "2026-08-20T12:00:01.000Z",
    });
    store.receiveUpsert({ schemaVersion: 1, boardId, participant: participant(session, user, 2) });
    expect(store.snapshot().collaborators).toEqual([]);
  });
  it("uses one expiry timer and drops silent expired evidence", () => {
    const clock = new Clock();
    const store = new PresenceStore(boardId, token, clock);
    store.receiveSnapshot(snapshot());
    expect(clock.timers.size).toBeLessThanOrEqual(2);
    clock.advance(45_000);
    expect(store.snapshot().collaborators).toEqual([]);
    expect(clock.timers.size).toBeLessThanOrEqual(1);
  });
  it("coalesces outbound updates at 20Hz and never includes identity", () => {
    const clock = new Clock();
    const store = new PresenceStore(boardId, token, clock);
    const send = vi.fn();
    store.setPublisher(send);
    store.receiveSnapshot(snapshot());
    store.publish({ x: 1, y: 2 }, "active");
    store.publish({ x: 3, y: 4 }, "active");
    expect(send).toHaveBeenCalledTimes(1);
    expect(clock.timers.size).toBeLessThanOrEqual(2);
    clock.advance(50);
    expect(send).toHaveBeenLastCalledWith({
      schemaVersion: 1,
      boardId,
      cursor: { x: 3, y: 4 },
      activity: "active",
    });
    const emitted = send.mock.calls[0]?.[0] as unknown as Record<string, unknown>;
    expect(Object.keys(emitted)).toEqual(["schemaVersion", "boardId", "cursor", "activity"]);
  });
});
