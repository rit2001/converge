import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import { InMemoryTelemetryRecorder } from "@converge/observability";
import type { AuthAdapter, AuthenticatedPrincipal } from "./auth.js";
import { buildApp, type BuildAppOptions } from "./app.js";
import type {
  DeliveryRuntimeEventHandlers,
  DeliveryRuntimeFactory,
  DeliveryRuntimeLifecycleEvent,
  DeliveryRuntimeObserver,
  DeliveryRuntimeOwner,
} from "./delivery-runtime.js";
import { parseEnvironment } from "./env.js";
import type { DatabasePool } from "@converge/database";
import {
  membershipRevokedDeliveryEnvelopeSchema,
  operationCommittedDeliveryEnvelopeSchema,
  protocolErrorSchema,
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

const environment = parseEnvironment({
  NODE_ENV: "test",
  HOST: "127.0.0.1",
  API_PORT: "4000",
  WEB_ORIGIN: "http://127.0.0.1:3000",
  DATABASE_URL: "postgresql://unused",
  REDIS_URL: "redis://unused",
  LOG_LEVEL: "silent",
  DEV_AUTH_USER_NAME: "Unused",
});

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

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

function runtimeHarness(options: { startError?: Error; establishOnStart?: boolean } = {}) {
  let handlers: DeliveryRuntimeEventHandlers | undefined;
  let observer: DeliveryRuntimeObserver | undefined;
  const startRuntime = vi.fn(async () => {
    if (options.startError !== undefined) throw options.startError;
    if (options.establishOnStart !== false) await observer?.lifecycle({ state: "established" });
  });
  const stopRuntime = vi.fn(() => Promise.resolve());
  const runtime: DeliveryRuntimeOwner = {
    start: startRuntime,
    stop: stopRuntime,
  };
  const createRuntime: DeliveryRuntimeFactory = vi.fn(
    (createdHandlers: DeliveryRuntimeEventHandlers, createdObserver: DeliveryRuntimeObserver) => {
      handlers = createdHandlers;
      observer = createdObserver;
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
    lifecycle(event: DeliveryRuntimeLifecycleEvent): Promise<void> {
      if (!observer) throw new Error("Expected a distributed delivery observer");
      return Promise.resolve(observer.lifecycle(event));
    },
  };
}

const authorizedHttpAuthentication = vi.fn(() =>
  Promise.resolve({ id: ids.actor, displayName: "Readiness actor" }),
);
const authorizedSocketAuthentication = vi.fn(() =>
  Promise.resolve({ id: ids.actor, displayName: "Readiness actor" }),
);
const authorizedAuth: AuthAdapter = {
  authenticateHttp: authorizedHttpAuthentication,
  authenticateSocket: authorizedSocketAuthentication,
};

async function runSocketMiddlewares(context: Awaited<ReturnType<typeof buildApp>>) {
  const namespace = context.io.of("/") as unknown as {
    _fns: Array<
      (
        socket: { handshake: { auth: Record<string, unknown> }; data: Record<string, unknown> },
        next: (error?: Error) => void,
      ) => void
    >;
  };
  const socket = { handshake: { auth: {} }, data: {} };
  for (const middleware of namespace._fns) {
    const error = await new Promise<Error | undefined>((resolve) => middleware(socket, resolve));
    if (error) return error;
  }
  return undefined;
}

function protocolData(error: Error | undefined) {
  return protocolErrorSchema.parse((error as Error & { data: unknown }).data);
}

function attachFakeSocket(context: Awaited<ReturnType<typeof buildApp>>) {
  const listeners = new Map<string, (...args: unknown[]) => unknown>();
  const socket = {
    id: "readiness-socket",
    data: {
      principal: { id: ids.actor, displayName: "Readiness actor" },
      revokedBoards: new Set<string>(),
    },
    rooms: new Set(["readiness-socket"]),
    on: vi.fn((event: string, listener: (...args: unknown[]) => unknown) => {
      listeners.set(event, listener);
      return socket;
    }),
    disconnect: vi.fn(),
    join: vi.fn(() => Promise.resolve()),
    leave: vi.fn(() => Promise.resolve()),
    emit: vi.fn(),
  };
  const connection = context.io.of("/").listeners("connection")[0];
  if (!connection) throw new Error("Expected the Socket.IO connection handler");
  connection(socket as never);
  return { socket, listeners };
}

function healthyHttpPool(): DatabasePool {
  const query = vi.fn((sql: string) => {
    if (sql.includes("SELECT b.id, b.name, b.last_seq"))
      return Promise.resolve({
        rows: [{ id: ids.board, name: "Ready board", last_seq: "0", object_data: null }],
        rowCount: 1,
      });
    if (sql.includes("SELECT b.last_seq"))
      return Promise.resolve({ rows: [{ last_seq: "0" }], rowCount: 1 });
    if (sql.includes("FROM board_operations")) return Promise.resolve({ rows: [], rowCount: 0 });
    throw new Error(`Unexpected pool query: ${sql}`);
  });
  const connect = vi.fn(() => {
    const client = {
      query: vi.fn((sql: string) => {
        if (sql === "BEGIN" || sql === "COMMIT" || sql.includes("pg_advisory_xact_lock"))
          return Promise.resolve({ rows: [], rowCount: 0 });
        if (sql.includes("SELECT b.last_seq, b.last_delivery_seq"))
          return Promise.resolve({
            rows: [
              {
                last_seq: "0",
                last_delivery_seq: "0",
                operation_recovery_floor: "0",
                delivery_recovery_floor: "0",
              },
            ],
            rowCount: 1,
          });
        if (sql.includes("FROM board_operations"))
          return Promise.resolve({ rows: [], rowCount: 0 });
        if (sql.includes("SELECT b.created_by"))
          return Promise.resolve({
            rows: [{ created_by: ids.actor, actor_role: "owner", last_delivery_seq: "0" }],
            rowCount: 1,
          });
        if (sql.includes("SELECT role FROM board_members"))
          return Promise.resolve({ rows: [], rowCount: 0 });
        throw new Error(`Unexpected client query: ${sql}`);
      }),
      release: vi.fn(),
    };
    return Promise.resolve(client);
  });
  return { query, connect } as unknown as DatabasePool;
}

describe("distributed-delivery application composition", () => {
  it("accepts HTTP health checks while distributed consumer establishment is pending", async () => {
    const startEntered = deferred();
    const establish = deferred();
    const start = vi.fn(() => {
      startEntered.resolve();
      return establish.promise;
    });
    const stop = vi.fn(() => Promise.resolve());
    const context = await buildApp(environment, healthyHttpPool(), authorizedAuth, {
      deliveryMode: { mode: "distributed", createRuntime: () => ({ start, stop }) },
      healthProbe: { check: () => Promise.resolve() },
    });
    const readyWork = context.app.ready();
    try {
      await startEntered.promise;
      await expect(
        Promise.race([
          readyWork.then(() => true),
          new Promise<false>((resolve) => setTimeout(() => resolve(false), 100)),
        ]),
      ).resolves.toBe(true);
      expect((await context.app.inject({ method: "GET", url: "/health/ready" })).statusCode).toBe(
        200,
      );
      expect(
        (await context.app.inject({ method: "GET", url: "/health/socket-ready" })).statusCode,
      ).toBe(503);
    } finally {
      establish.resolve();
      await readyWork;
      await context.app.close();
    }
    expect(start).toHaveBeenCalledOnce();
    expect(stop).toHaveBeenCalledOnce();
  });

  it("starts socket-unready until the distributed consumer is established", async () => {
    const state = runtimeHarness({ establishOnStart: false });
    authorizedSocketAuthentication.mockClear();
    const context = await buildApp(environment, pool, authorizedAuth, {
      deliveryMode: { mode: "distributed", createRuntime: state.createRuntime },
    });
    try {
      await context.app.ready();
      const startupError = await runSocketMiddlewares(context);
      expect(protocolData(startupError)).toEqual({
        ok: false,
        code: "INTERNAL_ERROR",
        message: "Realtime delivery is temporarily unavailable",
        retryable: true,
      });
      expect(startupError?.message).toBe("Realtime delivery is temporarily unavailable");
      expect(startupError?.message).not.toMatch(/auth|board|redis|cursor/i);
      expect(authorizedSocketAuthentication).not.toHaveBeenCalled();
      await state.lifecycle({ state: "established" });
      await expect(runSocketMiddlewares(context)).resolves.toBeUndefined();
    } finally {
      await context.app.close();
    }
  });

  it("records real readiness and API lifecycle transitions once and fences shutdown", async () => {
    const telemetry = new InMemoryTelemetryRecorder();
    const state = runtimeHarness({ establishOnStart: false });
    const context = await buildApp(environment, pool, authorizedAuth, {
      deliveryMode: { mode: "distributed", createRuntime: state.createRuntime },
      telemetry,
    });

    const gauge = (name: "converge_delivery_consumer_ready" | "converge_socket_ready") =>
      telemetry.snapshot().gauges.find((entry) => entry.name === name)?.value;
    const transitionCount = (source: "consumer" | "socket_readiness", stateName: string) =>
      telemetry
        .snapshot()
        .counters.find(
          (entry) =>
            entry.name === "converge_delivery_state_transitions_total" &&
            entry.labels.source === source &&
            entry.labels.state === stateName,
        )?.value ?? 0;

    expect(gauge("converge_delivery_consumer_ready")).toBe(0);
    expect(gauge("converge_socket_ready")).toBe(0);
    await context.app.ready();

    await state.lifecycle({ state: "established" });
    await state.lifecycle({ state: "established" });
    expect(gauge("converge_delivery_consumer_ready")).toBe(1);
    expect(gauge("converge_socket_ready")).toBe(1);
    expect(transitionCount("consumer", "established")).toBe(1);
    expect(transitionCount("socket_readiness", "established")).toBe(1);

    await state.lifecycle({ state: "unavailable", code: "REDIS_UNAVAILABLE" });
    await state.lifecycle({ state: "unavailable", code: "REDIS_UNAVAILABLE" });
    expect(gauge("converge_delivery_consumer_ready")).toBe(0);
    expect(gauge("converge_socket_ready")).toBe(0);
    expect(transitionCount("consumer", "unavailable")).toBe(1);
    expect(transitionCount("socket_readiness", "unavailable")).toBe(1);

    await state.lifecycle({ state: "recovered" });
    await state.lifecycle({ state: "recovered" });
    expect(gauge("converge_socket_ready")).toBe(1);
    expect(transitionCount("consumer", "recovered")).toBe(1);
    expect(transitionCount("socket_readiness", "established")).toBe(2);

    await context.app.close();
    await context.app.close();
    const closed = telemetry.snapshot();
    expect(gauge("converge_delivery_consumer_ready")).toBe(0);
    expect(gauge("converge_socket_ready")).toBe(0);
    expect(transitionCount("socket_readiness", "unavailable")).toBe(2);
    expect(
      closed.events
        .filter(({ eventName }) => eventName === "api.lifecycle")
        .map(({ code }) => code),
    ).toEqual(["STARTING", "READY", "STOPPING", "STOPPED"]);

    await state.lifecycle({ state: "unavailable", code: "REDIS_UNAVAILABLE" });
    await state.lifecycle({ state: "recovered" });
    expect(telemetry.snapshot()).toEqual(closed);
  });

  it("keeps local readiness immediately usable while exposing its current gauge", async () => {
    const telemetry = new InMemoryTelemetryRecorder();
    const context = await buildApp(environment, pool, authorizedAuth, { telemetry });
    try {
      expect(
        telemetry.snapshot().gauges.find(({ name }) => name === "converge_socket_ready")?.value,
      ).toBe(1);
      await context.app.ready();
      await expect(runSocketMiddlewares(context)).resolves.toBeUndefined();
    } finally {
      await context.app.close();
    }
  });

  it("disconnects and fences sockets until the current runtime recovers", async () => {
    const state = runtimeHarness();
    const context = await buildApp(environment, pool, authorizedAuth, {
      deliveryMode: { mode: "distributed", createRuntime: state.createRuntime },
    });
    const disconnectSockets = vi.spyOn(context.io.of("/").adapter, "disconnectSockets");
    const broadcast = vi.spyOn(context.io.of("/").adapter, "broadcast");
    try {
      await context.app.ready();
      const active = attachFakeSocket(context);

      await state.lifecycle({ state: "unavailable", code: "REDIS_UNAVAILABLE" });
      expect(disconnectSockets).toHaveBeenCalledOnce();
      expect(disconnectSockets.mock.calls[0]?.[0]).toMatchObject({ flags: { local: true } });
      expect(disconnectSockets.mock.calls[0]?.[1]).toBe(true);
      expect(protocolData(await runSocketMiddlewares(context))).toMatchObject({
        code: "INTERNAL_ERROR",
        retryable: true,
      });

      const joinAck = vi.fn();
      await active.listeners.get("board:join")?.(
        { schemaVersion: 1, boardId: ids.board, lastAppliedSeq: 0 },
        joinAck,
      );
      const operationAck = vi.fn();
      await active.listeners.get("operation:submit")?.(
        operationEnvelope().payload.operation,
        operationAck,
      );
      await state.handlers.operationCommitted(operationEnvelope());
      expect(joinAck).not.toHaveBeenCalled();
      expect(operationAck).not.toHaveBeenCalled();
      expect(broadcast).not.toHaveBeenCalled();

      await state.lifecycle({ state: "unavailable", code: "BOARD_STATE_CAPACITY_EXCEEDED" });
      await state.lifecycle({ state: "recovering" });
      await state.lifecycle({ state: "recovering" });
      expect(disconnectSockets).toHaveBeenCalledOnce();
      expect(protocolData(await runSocketMiddlewares(context))).toMatchObject({ retryable: true });

      await state.lifecycle({ state: "recovered" });
      await state.lifecycle({ state: "recovered" });
      await expect(runSocketMiddlewares(context)).resolves.toBeUndefined();

      await state.lifecycle({ state: "terminal", source: "cursor", code: "STREAM_BEHIND_CURSOR" });
      expect(disconnectSockets).toHaveBeenCalledTimes(2);
      await state.lifecycle({ state: "recovered" });
      expect(protocolData(await runSocketMiddlewares(context))).toMatchObject({
        code: "INTERNAL_ERROR",
      });
    } finally {
      await context.app.close();
    }

    await state.lifecycle({ state: "established" });
    await state.lifecycle({ state: "recovered" });
    expect(protocolData(await runSocketMiddlewares(context))).toMatchObject({
      code: "INTERNAL_ERROR",
      retryable: true,
    });
  });

  it("fails closed when readiness is lost during asynchronous socket authentication", async () => {
    const state = runtimeHarness();
    let finishAuthentication: (() => void) | undefined;
    const delayedSocketAuthentication: AuthAdapter["authenticateSocket"] = vi.fn(
      () =>
        new Promise<AuthenticatedPrincipal | null>((resolve) => {
          finishAuthentication = () => resolve({ id: ids.actor, displayName: "Readiness actor" });
        }),
    );
    const delayedAuth: AuthAdapter = {
      authenticateHttp: () => authorizedHttpAuthentication(),
      authenticateSocket: delayedSocketAuthentication,
    };
    const context = await buildApp(environment, pool, delayedAuth, {
      deliveryMode: { mode: "distributed", createRuntime: state.createRuntime },
    });
    try {
      await context.app.ready();
      const pendingHandshake = runSocketMiddlewares(context);
      await vi.waitFor(() => expect(delayedSocketAuthentication).toHaveBeenCalledOnce());
      await state.lifecycle({ state: "unavailable", code: "REDIS_UNAVAILABLE" });
      finishAuthentication?.();
      expect(protocolData(await pendingHandshake)).toEqual({
        ok: false,
        code: "INTERNAL_ERROR",
        message: "Realtime delivery is temporarily unavailable",
        retryable: true,
      });
    } finally {
      await context.app.close();
    }
  });

  it("keeps local sockets immediately usable and leaves PostgreSQL HTTP routes ungated", async () => {
    const local = await buildApp(environment, pool, authorizedAuth);
    try {
      await local.app.ready();
      await expect(runSocketMiddlewares(local)).resolves.toBeUndefined();
    } finally {
      await local.app.close();
    }

    const state = runtimeHarness({ establishOnStart: false });
    const distributed = await buildApp(environment, healthyHttpPool(), authorizedAuth, {
      deliveryMode: { mode: "distributed", createRuntime: state.createRuntime },
    });
    try {
      await distributed.app.ready();
      expect(await runSocketMiddlewares(distributed)).toBeInstanceOf(Error);
      const snapshot = await distributed.app.inject({
        method: "GET",
        url: `/v1/boards/${ids.board}`,
      });
      const operationRange = await distributed.app.inject({
        method: "GET",
        url: `/v1/boards/${ids.board}/operations?after=0&watermark=0`,
      });
      const membership = await distributed.app.inject({
        method: "DELETE",
        url: `/v1/boards/${ids.board}/members/${ids.revoked}`,
        payload: {},
      });
      expect(snapshot.statusCode).toBe(200);
      expect(operationRange.statusCode).toBe(200);
      expect(membership.statusCode).toBe(200);
      expect(membership.json()).toMatchObject({ ok: true, removed: false, eventId: null });
    } finally {
      await distributed.app.close();
    }
  });

  it("keeps local delivery as the application default and delegates server activation", () => {
    const appSource = readFileSync(new URL("./app.ts", import.meta.url), "utf8");
    const serverSource = readFileSync(new URL("./server.ts", import.meta.url), "utf8");

    expect(appSource).toContain('options.deliveryMode ?? { mode: "local" as const }');
    expect(appSource).toContain('if (deliveryMode.mode === "local")');
    expect(serverSource).toContain("createApiServer");
    expect(serverSource).not.toContain("RedisDelivery");
    expect(serverSource).not.toContain("OutboxWorker");
    expect(serverSource).not.toContain("io.close");
  });

  it("starts and stops one runtime and lets app.close own one Socket.IO close", async () => {
    const state = runtimeHarness();
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: {
        mode: "distributed",
        createRuntime: state.createRuntime,
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

  it("keeps HTTP ready and stops a failed delivery startup once before Socket.IO closes", async () => {
    const calls: string[] = [];
    const telemetry = new InMemoryTelemetryRecorder();
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
      },
      telemetry,
      healthProbe: { check: () => Promise.resolve() },
    });
    vi.spyOn(context.io, "close").mockImplementation(() => {
      calls.push("socket:close");
      return Promise.resolve();
    });

    await context.app.ready();
    expect((await context.app.inject({ method: "GET", url: "/health/ready" })).statusCode).toBe(
      200,
    );
    expect(
      (await context.app.inject({ method: "GET", url: "/health/socket-ready" })).statusCode,
    ).toBe(503);
    await vi.waitFor(() => expect(state.stopRuntime).toHaveBeenCalledOnce());
    await context.app.close();
    expect(state.startRuntime).toHaveBeenCalledOnce();
    expect(state.stopRuntime).toHaveBeenCalledOnce();
    expect(calls).toEqual(["runtime:start", "runtime:stop", "socket:close"]);
    expect(
      telemetry
        .snapshot()
        .events.filter(({ eventName }) => eventName === "api.lifecycle")
        .map(({ code }) => code),
    ).toEqual(["STARTING", "READY", "STOPPING", "STOPPED"]);
  });

  it("routes a consumed operation only through the matching API-local board room", async () => {
    const state = runtimeHarness();
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: {
        mode: "distributed",
        createRuntime: state.createRuntime,
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

  it("injects the required membership-revocation handler into the runtime", async () => {
    const state = runtimeHarness();
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: state.createRuntime },
    });
    try {
      await context.app.ready();
      await state.handlers.membershipRevoked(membershipEnvelope);
      expect(state.createRuntime).toHaveBeenCalledOnce();
    } finally {
      await context.app.close();
    }
  });

  it("rejects distributed composition without a runtime factory", async () => {
    const state = runtimeHarness();
    const deliveryMode = {
      mode: "distributed",
    } as unknown as NonNullable<BuildAppOptions["deliveryMode"]>;

    await expect(buildApp(environment, pool, auth, { deliveryMode })).rejects.toThrow(
      "requires a runtime factory",
    );
    expect(state.createRuntime).not.toHaveBeenCalled();
  });

  it("fences operation and membership callbacks as soon as application shutdown starts", async () => {
    const state = runtimeHarness();
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: state.createRuntime },
    });
    const broadcast = vi.spyOn(context.io.of("/").adapter, "broadcast");
    await context.app.ready();
    await context.app.close();
    broadcast.mockClear();

    await state.handlers.operationCommitted(operationEnvelope());
    await state.handlers.membershipRevoked(membershipEnvelope);
    expect(broadcast).not.toHaveBeenCalled();
  });

  it("rejects a handler payload whose operation targets another board", async () => {
    const state = runtimeHarness();
    const context = await buildApp(environment, pool, auth, {
      deliveryMode: {
        mode: "distributed",
        createRuntime: state.createRuntime,
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
