import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import type { AuthAdapter } from "./auth.js";
import { buildApp, type BuildAppOptions } from "./app.js";
import type {
  DeliveryRuntimeEventHandlers,
  DeliveryRuntimeFactory,
  DeliveryRuntimeOwner,
} from "./delivery-runtime.js";
import type { Environment } from "./env.js";
import type { DatabasePool } from "@converge/database";
import {
  membershipRevokedDeliveryEnvelopeSchema,
  operationCommittedDeliveryEnvelopeSchema,
} from "@converge/protocol";

const ids = {
  board: "10000000-0000-4000-8000-000000000001",
  otherBoard: "10000000-0000-4000-8000-000000000002",
  client: "20000000-0000-4000-8000-000000000001",
  object: "30000000-0000-4000-8000-000000000001",
  operation: "40000000-0000-4000-8000-000000000001",
  operationEvent: "50000000-0000-4000-8000-000000000001",
  membershipEvent: "50000000-0000-4000-8000-000000000002",
  revoked: "60000000-0000-4000-8000-000000000001",
  actor: "70000000-0000-4000-8000-000000000001",
} as const;

const environment: Environment = {
  NODE_ENV: "test",
  HOST: "127.0.0.1",
  API_PORT: 4000,
  WEB_ORIGIN: "http://127.0.0.1:3000",
  DATABASE_URL: "postgresql://unused",
  REDIS_URL: "redis://unused",
  LOG_LEVEL: "silent",
  DEV_AUTH_USER_NAME: "Unused",
};

const pool = {} as DatabasePool;
const auth = {
  authenticateHttp: vi.fn(() => Promise.resolve(null)),
  authenticateSocket: vi.fn(() => Promise.resolve(null)),
} satisfies AuthAdapter;

function operationEnvelope(boardId = ids.board) {
  return operationCommittedDeliveryEnvelopeSchema.parse({
    schemaVersion: 1,
    eventId: ids.operationEvent,
    boardId,
    deliverySeq: 1,
    eventType: "operation.committed",
    occurredAt: "2026-08-11T10:00:01.000Z",
    payload: {
      operation: {
        schemaVersion: 1,
        opId: ids.operation,
        boardId,
        clientId: ids.client,
        baseSeq: 0,
        targetId: ids.object,
        clientTimestamp: "2026-08-11T10:00:00.000Z",
        type: "object.create",
        payload: {
          id: ids.object,
          kind: "rectangle",
          x: 0,
          y: 0,
          width: 100,
          height: 80,
          rotation: 0,
          fill: "#818cf8",
          text: "",
        },
        seq: 1,
        committedAt: "2026-08-11T10:00:01.000Z",
      },
    },
  });
}

const membershipEnvelope = membershipRevokedDeliveryEnvelopeSchema.parse({
  schemaVersion: 1,
  eventId: ids.membershipEvent,
  boardId: ids.board,
  deliverySeq: 2,
  eventType: "board.membership.revoked",
  occurredAt: "2026-08-11T10:00:02.000Z",
  payload: { revokedUserId: ids.revoked, initiatedByUserId: ids.actor },
});

function runtimeHarness(options: { startError?: Error } = {}) {
  let handlers: DeliveryRuntimeEventHandlers | undefined;
  const startRuntime = vi.fn(() =>
    options.startError === undefined ? Promise.resolve() : Promise.reject(options.startError),
  );
  const stopRuntime = vi.fn(() => Promise.resolve());
  const runtime: DeliveryRuntimeOwner = {
    start: startRuntime,
    stop: stopRuntime,
  };
  const createRuntime: DeliveryRuntimeFactory = vi.fn(
    (createdHandlers: DeliveryRuntimeEventHandlers) => {
      handlers = createdHandlers;
      return runtime;
    },
  );
  return {
    startRuntime,
    stopRuntime,
    createRuntime,
    get handlers(): DeliveryRuntimeEventHandlers {
      if (!handlers) throw new Error("Expected distributed delivery handlers");
      return handlers;
    },
  };
}

describe("distributed-delivery application composition", () => {
  it("keeps local delivery as the default and production startup Redis-free", () => {
    const appSource = readFileSync(new URL("./app.ts", import.meta.url), "utf8");
    const serverSource = readFileSync(new URL("./server.ts", import.meta.url), "utf8");

    expect(appSource).toContain('options.deliveryMode ?? { mode: "local" as const }');
    expect(appSource).toContain('if (deliveryMode.mode === "local")');
    expect(serverSource).not.toContain("deliveryMode");
    expect(serverSource).not.toContain("RedisDelivery");
    expect(serverSource).not.toContain("createRuntime");
    expect(serverSource).not.toContain("io.close");
  });

  it("starts and stops one runtime and lets app.close own one Socket.IO close", async () => {
    const state = runtimeHarness();
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: {
        mode: "distributed",
        createRuntime: state.createRuntime,
        membershipRevoked: vi.fn(() => Promise.resolve()),
      },
    });
    const closeSocketIo = vi.spyOn(context.io, "close");

    await context.app.ready();
    await context.app.ready();
    expect(state.createRuntime).toHaveBeenCalledOnce();
    expect(state.startRuntime).toHaveBeenCalledOnce();
    await context.app.close();
    await context.app.close();
    expect(state.stopRuntime).toHaveBeenCalledOnce();
    expect(closeSocketIo).toHaveBeenCalledOnce();
  });

  it("stops a failed startup once before Socket.IO closes", async () => {
    const calls: string[] = [];
    const state = runtimeHarness({ startError: new Error("runtime startup failed") });
    state.startRuntime.mockImplementation(() => {
      calls.push("runtime:start");
      return Promise.reject(new Error("runtime startup failed"));
    });
    state.stopRuntime.mockImplementation(() => {
      calls.push("runtime:stop");
      return Promise.resolve();
    });
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: {
        mode: "distributed",
        createRuntime: state.createRuntime,
        membershipRevoked: vi.fn(() => Promise.resolve()),
      },
    });
    vi.spyOn(context.io, "close").mockImplementation(() => {
      calls.push("socket:close");
      return Promise.resolve();
    });

    await expect(context.app.ready()).rejects.toThrow("runtime startup failed");
    await context.app.close();
    expect(state.startRuntime).toHaveBeenCalledOnce();
    expect(state.stopRuntime).toHaveBeenCalledOnce();
    expect(calls).toEqual(["runtime:start", "runtime:stop", "socket:close"]);
  });

  it("routes a consumed operation only through the matching API-local board room", async () => {
    const state = runtimeHarness();
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: {
        mode: "distributed",
        createRuntime: state.createRuntime,
        membershipRevoked: vi.fn(() => Promise.resolve()),
      },
    });
    const broadcast = vi.spyOn(context.io.of("/").adapter, "broadcast");
    try {
      await context.app.ready();
      const envelope = operationEnvelope();
      await state.handlers.operationCommitted(envelope);

      expect(broadcast).toHaveBeenCalledOnce();
      const [packet, broadcastOptions] = broadcast.mock.calls[0] as unknown as readonly [
        { data: unknown[] },
        { rooms: Set<string>; flags: Record<string, unknown> },
      ];
      expect(packet.data).toEqual(["operation:committed", envelope.payload.operation]);
      expect(broadcastOptions.rooms).toEqual(new Set([`board:${ids.board}`]));
      expect(broadcastOptions.flags).toMatchObject({ local: true });
    } finally {
      await context.app.close();
    }
  });

  it("forwards membership revocation to its required injected handler", async () => {
    const state = runtimeHarness();
    const membershipRevoked = vi.fn(() => Promise.resolve());
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: state.createRuntime, membershipRevoked },
    });
    try {
      await context.app.ready();
      await state.handlers.membershipRevoked(membershipEnvelope);
      expect(membershipRevoked).toHaveBeenCalledOnce();
      expect(membershipRevoked).toHaveBeenCalledWith(membershipEnvelope);
    } finally {
      await context.app.close();
    }
  });

  it("rejects distributed composition without the membership handler", async () => {
    const state = runtimeHarness();
    const deliveryMode = {
      mode: "distributed",
      createRuntime: state.createRuntime,
    } as unknown as NonNullable<BuildAppOptions["deliveryMode"]>;

    await expect(buildApp(environment, pool, auth, { deliveryMode })).rejects.toThrow(
      "requires a runtime and every event handler",
    );
    expect(state.createRuntime).not.toHaveBeenCalled();
  });

  it("fences operation and membership callbacks as soon as application shutdown starts", async () => {
    const state = runtimeHarness();
    const membershipRevoked = vi.fn(() => Promise.resolve());
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: state.createRuntime, membershipRevoked },
    });
    const broadcast = vi.spyOn(context.io.of("/").adapter, "broadcast");
    await context.app.ready();
    await context.app.close();
    broadcast.mockClear();

    await state.handlers.operationCommitted(operationEnvelope());
    await state.handlers.membershipRevoked(membershipEnvelope);
    expect(broadcast).not.toHaveBeenCalled();
    expect(membershipRevoked).not.toHaveBeenCalled();
  });

  it("rejects a handler payload whose operation targets another board", async () => {
    const state = runtimeHarness();
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: {
        mode: "distributed",
        createRuntime: state.createRuntime,
        membershipRevoked: vi.fn(() => Promise.resolve()),
      },
    });
    try {
      await context.app.ready();
      const envelope = operationEnvelope();
      const mismatched = {
        ...envelope,
        boardId: ids.otherBoard,
      } as typeof envelope;
      await expect(state.handlers.operationCommitted(mismatched)).rejects.toThrow(
        "does not match its delivery envelope",
      );
    } finally {
      await context.app.close();
    }
  });
});
