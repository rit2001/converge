import { describe, expect, it, vi } from "vitest";
import type { PresenceAvailability } from "@converge/protocol";
import type { PresenceRedisTransport } from "./presence-redis-transport.js";
import { PresenceRuntime } from "./presence-runtime.js";

const board = "10000000-0000-4000-8000-000000000001";
const principal = { userId: "20000000-0000-4000-8000-000000000002", displayName: "Ada" };
const participant = {
  presenceSessionId: "30000000-0000-4000-8000-000000000003",
  userId: principal.userId,
  displayName: "Ada",
  revision: 1,
  activity: "active" as const,
  cursor: null,
  observedAt: "2026-08-20T12:00:00.000Z",
  expiresAt: "2026-08-20T12:00:45.000Z",
};

function transport(): PresenceRedisTransport & {
  refresh: ReturnType<typeof vi.fn>;
  admit: ReturnType<typeof vi.fn>;
  leave: ReturnType<typeof vi.fn>;
  stop: ReturnType<typeof vi.fn>;
  availability: Set<(event: PresenceAvailability) => void>;
} {
  const availability = new Set<(event: PresenceAvailability) => void>();
  return {
    start: vi.fn(() => Promise.resolve({ kind: "ok" as const, value: undefined })),
    admit: vi.fn(() => Promise.resolve({ kind: "ok" as const, value: participant })),
    refresh: vi.fn(() =>
      Promise.resolve({ kind: "ok" as const, value: { ...participant, revision: 2 } }),
    ),
    snapshot: vi.fn(() =>
      Promise.resolve({
        kind: "ok" as const,
        value: {
          schemaVersion: 1 as const,
          boardId: board,
          observedAt: participant.observedAt,
          participants: [participant],
        },
      }),
    ),
    leave: vi.fn(() => Promise.resolve({ kind: "ok" as const, value: null })),
    stop: vi.fn(() => Promise.resolve()),
    onDelta: vi.fn(() => () => undefined),
    onAvailability: vi.fn((callback: (event: PresenceAvailability) => void) => {
      availability.add(callback);
      return () => availability.delete(callback);
    }),
    availability,
  };
}
describe("PresenceRuntime", () => {
  it("binds after join evidence, ignores wrong-board updates, and leaves without touching durable IO", async () => {
    const source = transport();
    const socket = { id: "socket", emit: vi.fn(), on: vi.fn() };
    const local = { to: vi.fn(() => ({ emit: vi.fn() })) };
    const runtime = new PresenceRuntime(source, { local });
    runtime.start();
    runtime.bind(socket, board, principal);
    await vi.waitFor(() => expect(source.admit).toHaveBeenCalledOnce());
    await vi.waitFor(() =>
      expect(socket.emit).toHaveBeenCalledWith("board:presence-snapshot", expect.any(Object)),
    );
    const update = socket.on.mock.calls.find(([event]) => event === "presence:update")?.[1] as (
      raw: unknown,
    ) => void;
    update({
      schemaVersion: 1,
      boardId: "40000000-0000-4000-8000-000000000004",
      cursor: null,
      activity: "active",
    });
    expect(source.refresh).not.toHaveBeenCalled();
    runtime.unbind(socket.id);
    await Promise.resolve();
    expect(source.leave).toHaveBeenCalledOnce();
    await runtime.stop();
    expect(source.stop).toHaveBeenCalledOnce();
  });

  it("re-admits only the current binding and sends a fresh snapshot after transport recovery", async () => {
    const source = transport();
    const socket = { id: "socket", emit: vi.fn(), on: vi.fn() };
    const runtime = new PresenceRuntime(source, {
      local: { to: vi.fn(() => ({ emit: vi.fn() })) },
    });
    runtime.start();
    runtime.bind(socket, board, principal);
    await vi.waitFor(() => expect(source.admit).toHaveBeenCalledOnce());
    await vi.waitFor(() =>
      expect(
        socket.emit.mock.calls.filter(([event]) => event === "board:presence-snapshot"),
      ).toHaveLength(1),
    );
    for (const callback of source.availability)
      callback({ schemaVersion: 1, boardId: board, status: "available" });
    await vi.waitFor(() => expect(source.admit).toHaveBeenCalledTimes(2));
    await vi.waitFor(() =>
      expect(
        socket.emit.mock.calls.filter(([event]) => event === "board:presence-snapshot"),
      ).toHaveLength(2),
    );
    const first = source.admit.mock.calls[0]?.[0] as unknown as { presenceSessionId: string };
    const second = source.admit.mock.calls[1]?.[0] as unknown as { presenceSessionId: string };
    expect(first.presenceSessionId).not.toBe(second.presenceSessionId);
    await runtime.stop();
  });
});
