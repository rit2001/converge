import { describe, expect, it, vi } from "vitest";
import type { AuthAdapter } from "./auth.js";
import { buildApp, type AppContext } from "./app.js";
import {
  BoardDeliveryHeadWatchdog,
  defaultBoardDeliveryHeadWatchdogConfiguration,
  type BoardDeliveryHeadWatchdogFactory,
  type BoardDeliveryHeadWatchdogFactoryInput,
  type BoardDeliveryHeadWatchdogLifecycleEvent,
  type BoardDeliveryHeadWatchdogOwner,
  type BoardDeliveryHeadWatchdogScheduler,
} from "./board-delivery-head-watchdog.js";
import type {
  DeliveryRuntimeEventHandlers,
  DeliveryRuntimeFactory,
  DeliveryRuntimeLifecycleEvent,
  DeliveryRuntimeObserver,
} from "./delivery-runtime.js";
import { parseEnvironment } from "./env.js";
import type { DatabasePool } from "@converge/database";
import {
  membershipRevokedDeliveryEnvelopeSchema,
  operationCommittedDeliveryEnvelopeSchema,
  protocolErrorSchema,
  type JoinBoardAck,
} from "@converge/protocol";

const ids = {
  board: "10000000-0000-4000-8000-000000000001",
  client: "20000000-0000-4000-8000-000000000001",
  object: "30000000-0000-4000-8000-000000000001",
  operation: "40000000-0000-4000-8000-000000000001",
  event: "50000000-0000-4000-8000-000000000001",
  membershipEvent: "50000000-0000-4000-8000-000000000002",
  actor: "60000000-0000-4000-8000-000000000001",
  revoked: "60000000-0000-4000-8000-000000000002",
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

const auth: AuthAdapter = {
  authenticateHttp: vi.fn(() => Promise.resolve({ id: ids.actor, displayName: "Actor" })),
  authenticateSocket: vi.fn(() => Promise.resolve({ id: ids.actor, displayName: "Actor" })),
};

function operationEnvelope(deliverySeq: number) {
  return operationCommittedDeliveryEnvelopeSchema.parse({
    schemaVersion: 1,
    eventId: ids.event,
    boardId: ids.board,
    deliverySeq,
    eventType: "operation.committed",
    occurredAt: "2026-08-11T10:00:01.000Z",
    payload: {
      operation: {
        schemaVersion: 1,
        opId: ids.operation,
        boardId: ids.board,
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
  deliverySeq: 11,
  eventType: "board.membership.revoked",
  occurredAt: "2026-08-11T10:00:02.000Z",
  payload: { revokedUserId: ids.revoked, initiatedByUserId: ids.actor },
});

function runtimeHarness() {
  let handlers: DeliveryRuntimeEventHandlers | undefined;
  let observer: DeliveryRuntimeObserver | undefined;
  const start = vi.fn(async () => observer?.lifecycle({ state: "established" }));
  const stop = vi.fn(() => Promise.resolve());
  const createRuntime: DeliveryRuntimeFactory = vi.fn(
    (createdHandlers: DeliveryRuntimeEventHandlers, createdObserver: DeliveryRuntimeObserver) => {
      handlers = createdHandlers;
      observer = createdObserver;
      return { start, stop };
    },
  );
  return {
    createRuntime,
    start,
    stop,
    get handlers(): DeliveryRuntimeEventHandlers {
      if (!handlers) throw new Error("Expected runtime handlers");
      return handlers;
    },
    lifecycle(event: DeliveryRuntimeLifecycleEvent): Promise<void> {
      if (!observer) throw new Error("Expected runtime observer");
      return Promise.resolve(observer.lifecycle(event));
    },
  };
}

function watchdogHarness() {
  let input: BoardDeliveryHeadWatchdogFactoryInput | undefined;
  const start = vi.fn(() => Promise.resolve());
  const stop = vi.fn(() => Promise.resolve());
  const owner: BoardDeliveryHeadWatchdogOwner = { start, stop };
  const createWatchdog: BoardDeliveryHeadWatchdogFactory = vi.fn(
    (createdInput: BoardDeliveryHeadWatchdogFactoryInput) => {
      input = createdInput;
      return owner;
    },
  );
  return {
    createWatchdog,
    start,
    stop,
    activeBoards(): string[] {
      if (!input) throw new Error("Expected watchdog input");
      return [...input.activeBoards.activeBoardIds()];
    },
    progress(boardId: string): number {
      if (!input) throw new Error("Expected watchdog input");
      return input.deliveryProgress.handledDeliverySequence(boardId);
    },
    lifecycle(event: BoardDeliveryHeadWatchdogLifecycleEvent): Promise<void> {
      if (!input) throw new Error("Expected watchdog input");
      return Promise.resolve(input.observer.lifecycle(event));
    },
  };
}

function databaseHarness() {
  let deliveryHead = 10;
  let canvasHead = 4;
  const deliveryHeadQueries: readonly string[][] = [];
  const recordedDeliveryHeadQueries = deliveryHeadQueries as string[][];
  const query = vi.fn((sql: string, values?: readonly unknown[]) => {
    if (sql.includes("SELECT role FROM board_members"))
      return Promise.resolve({ rows: [{ role: "editor" }], rowCount: 1 });
    if (sql.includes("SELECT id, last_delivery_seq")) {
      const boardIds = (values?.[0] ?? []) as readonly string[];
      recordedDeliveryHeadQueries.push([...boardIds]);
      return Promise.resolve({
        rows: boardIds.map((boardId) => ({
          id: boardId,
          last_delivery_seq: String(deliveryHead),
        })),
        rowCount: boardIds.length,
      });
    }
    if (sql.includes("SELECT b.last_seq"))
      return Promise.resolve({ rows: [{ last_seq: String(canvasHead) }], rowCount: 1 });
    throw new Error(`Unexpected query: ${sql}`);
  });
  return {
    pool: { query } as unknown as DatabasePool,
    deliveryHeadQueries,
    set deliveryHead(value: number) {
      deliveryHead = value;
    },
    set canvasHead(value: number) {
      canvasHead = value;
    },
  };
}

function attachSocket(context: AppContext, socketId: string, principalId: string = ids.actor) {
  const listeners = new Map<string, (...args: unknown[]) => unknown>();
  const rooms = new Set([socketId]);
  const socket = {
    id: socketId,
    data: {
      principal: { id: principalId, displayName: "Socket actor" },
      revokedBoards: new Set<string>(),
    },
    rooms,
    on: vi.fn((event: string, listener: (...args: unknown[]) => unknown) => {
      listeners.set(event, listener);
      return socket;
    }),
    disconnect: vi.fn(),
    join: vi.fn((room: string) => {
      rooms.add(room);
      return Promise.resolve();
    }),
    leave: vi.fn((room: string) => {
      rooms.delete(room);
      return Promise.resolve();
    }),
    emit: vi.fn(),
  };
  const connection = context.io.of("/").listeners("connection")[0];
  if (!connection) throw new Error("Expected Socket.IO connection handler");
  connection(socket as never);
  return {
    socket,
    listeners,
    async joinBoard(lastAppliedSeq = 0) {
      let acknowledged: JoinBoardAck | undefined;
      await listeners.get("board:join")?.(
        { schemaVersion: 1, boardId: ids.board, clientId: ids.client, lastAppliedSeq },
        (ack: JoinBoardAck) => {
          acknowledged = ack;
        },
      );
      return acknowledged;
    },
    async disconnect() {
      await listeners.get("disconnect")?.("client namespace disconnect");
    },
  };
}

async function runSocketMiddlewares(context: AppContext): Promise<Error | undefined> {
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

class ManualScheduler implements BoardDeliveryHeadWatchdogScheduler {
  currentTime = 0;
  private readonly waits = new Set<{ signal: AbortSignal; settle: () => void }>();
  now = (): number => this.currentTime;
  random = (): number => 0.5;

  wait(_delayMs: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      const wait = {
        signal,
        settle: (): void => {
          this.waits.delete(wait);
          signal.removeEventListener("abort", wait.settle);
          resolve();
        },
      };
      this.waits.add(wait);
      signal.addEventListener("abort", wait.settle, { once: true });
    });
  }
}

function realWatchdogFactory(scheduler: ManualScheduler) {
  let watchdog: BoardDeliveryHeadWatchdog | undefined;
  const createWatchdog: BoardDeliveryHeadWatchdogFactory = (input) => {
    watchdog = new BoardDeliveryHeadWatchdog(
      input.repository,
      input.activeBoards,
      input.deliveryProgress,
      input.observer,
      { ...defaultBoardDeliveryHeadWatchdogConfiguration, jitterRatio: 0 },
      scheduler,
    );
    return watchdog;
  };
  return {
    createWatchdog,
    get watchdog(): BoardDeliveryHeadWatchdog {
      if (!watchdog) throw new Error("Expected real watchdog");
      return watchdog;
    },
  };
}

describe("board-head watchdog application composition", () => {
  it("uses the first authoritative delivery watermark and bounded socket reference counts", async () => {
    const database = databaseHarness();
    const runtime = runtimeHarness();
    const watchdog = watchdogHarness();
    const context = await buildApp(environment, database.pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: runtime.createRuntime },
      createBoardDeliveryHeadWatchdog: watchdog.createWatchdog,
    });
    try {
      await context.app.ready();
      const first = attachSocket(context, "socket-a");
      expect(await first.joinBoard()).toMatchObject({ joinWatermark: 4 });
      expect(watchdog.activeBoards()).toEqual([ids.board]);
      expect(watchdog.progress(ids.board)).toBe(10);

      database.deliveryHead = 99;
      const second = attachSocket(context, "socket-b");
      await second.joinBoard();
      expect(database.deliveryHeadQueries).toEqual([[ids.board]]);
      expect(watchdog.activeBoards()).toEqual([ids.board]);
      expect(watchdog.progress(ids.board)).toBe(10);

      await first.disconnect();
      await first.disconnect();
      expect(watchdog.activeBoards()).toEqual([ids.board]);
      await second.disconnect();
      expect(watchdog.activeBoards()).toEqual([]);
      expect(watchdog.progress(ids.board)).toBe(0);
    } finally {
      await context.app.close();
    }
  });

  it("decrements revocation leaves without removing another local socket", async () => {
    const database = databaseHarness();
    const runtime = runtimeHarness();
    const watchdog = watchdogHarness();
    const context = await buildApp(environment, database.pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: runtime.createRuntime },
      createBoardDeliveryHeadWatchdog: watchdog.createWatchdog,
    });
    try {
      await context.app.ready();
      const target = attachSocket(context, "revoked-socket", ids.revoked);
      const other = attachSocket(context, "other-socket", ids.actor);
      await target.joinBoard();
      await other.joinBoard();
      context.io.sockets.sockets.set(target.socket.id, target.socket as never);
      context.io.sockets.sockets.set(other.socket.id, other.socket as never);

      await runtime.handlers.membershipRevoked(membershipEnvelope);
      expect(target.socket.leave).toHaveBeenCalledWith(`board:${ids.board}`);
      expect(other.socket.leave).not.toHaveBeenCalled();
      expect(watchdog.activeBoards()).toEqual([ids.board]);
      expect(watchdog.progress(ids.board)).toBe(11);
      await other.disconnect();
      expect(watchdog.activeBoards()).toEqual([]);
    } finally {
      context.io.sockets.sockets.delete("revoked-socket");
      context.io.sockets.sockets.delete("other-socket");
      await context.app.close();
    }
  });

  it("stays ready when delivery catches up inside grace and never resets progress on later joins", async () => {
    const database = databaseHarness();
    const runtime = runtimeHarness();
    const scheduler = new ManualScheduler();
    const watchdog = realWatchdogFactory(scheduler);
    const context = await buildApp(environment, database.pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: runtime.createRuntime },
      createBoardDeliveryHeadWatchdog: watchdog.createWatchdog,
    });
    const disconnectSockets = vi.spyOn(context.io.of("/").adapter, "disconnectSockets");
    try {
      await context.app.ready();
      const first = attachSocket(context, "first-socket");
      await first.joinBoard();
      database.deliveryHead = 11;
      await watchdog.watchdog.runCheck();
      expect(await runSocketMiddlewares(context)).toBeUndefined();

      await runtime.handlers.operationCommitted(operationEnvelope(11));
      scheduler.currentTime = 4_999;
      await watchdog.watchdog.runCheck();
      expect(disconnectSockets).not.toHaveBeenCalled();
      expect(await runSocketMiddlewares(context)).toBeUndefined();
    } finally {
      await context.app.close();
    }
  });

  it("fails readiness after persistent divergence and a later join cannot mask it", async () => {
    const database = databaseHarness();
    const runtime = runtimeHarness();
    const scheduler = new ManualScheduler();
    const watchdog = realWatchdogFactory(scheduler);
    const context = await buildApp(environment, database.pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: runtime.createRuntime },
      createBoardDeliveryHeadWatchdog: watchdog.createWatchdog,
    });
    const disconnectSockets = vi.spyOn(context.io.of("/").adapter, "disconnectSockets");
    try {
      await context.app.ready();
      const first = attachSocket(context, "first-socket");
      await first.joinBoard();
      database.deliveryHead = 11;
      await watchdog.watchdog.runCheck();
      expect(await runSocketMiddlewares(context)).toBeUndefined();

      database.deliveryHead = 20;
      const later = attachSocket(context, "later-socket");
      await later.joinBoard();
      scheduler.currentTime = 5_000;
      await watchdog.watchdog.runCheck();

      expect(database.deliveryHeadQueries).toEqual([[ids.board], [ids.board], [ids.board]]);
      expect(disconnectSockets).toHaveBeenCalledOnce();
      const error = await runSocketMiddlewares(context);
      expect(protocolErrorSchema.parse((error as Error & { data: unknown }).data)).toEqual({
        ok: false,
        code: "INTERNAL_ERROR",
        message: "Realtime delivery is temporarily unavailable",
        retryable: true,
      });
      expect(JSON.stringify((error as Error & { data: unknown }).data)).not.toContain(ids.board);

      await first.disconnect();
      await later.disconnect();
      await watchdog.watchdog.runCheck();
      expect(await runSocketMiddlewares(context)).toBeUndefined();
    } finally {
      await context.app.close();
    }
  });

  it("requires both consumer and watchdog recovery before reopening sockets", async () => {
    const database = databaseHarness();
    const runtime = runtimeHarness();
    const watchdog = watchdogHarness();
    const context = await buildApp(environment, database.pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: runtime.createRuntime },
      createBoardDeliveryHeadWatchdog: watchdog.createWatchdog,
    });
    try {
      await context.app.ready();
      await watchdog.lifecycle({
        state: "unavailable",
        code: "DELIVERY_HEAD_DIVERGED",
        boardIds: [ids.board],
      });
      await runtime.lifecycle({ state: "unavailable", code: "REDIS_UNAVAILABLE" });
      await watchdog.lifecycle({ state: "recovered" });
      expect(await runSocketMiddlewares(context)).toBeInstanceOf(Error);
      await runtime.lifecycle({ state: "recovered" });
      expect(await runSocketMiddlewares(context)).toBeUndefined();

      await watchdog.lifecycle({
        state: "unavailable",
        code: "DATABASE_CHECK_FAILED",
        boardIds: [],
      });
      await runtime.lifecycle({ state: "unavailable", code: "REDIS_UNAVAILABLE" });
      await runtime.lifecycle({ state: "recovered" });
      expect(await runSocketMiddlewares(context)).toBeInstanceOf(Error);
      await watchdog.lifecycle({ state: "recovered" });
      expect(await runSocketMiddlewares(context)).toBeUndefined();
    } finally {
      await context.app.close();
    }
  });

  it("advances only after successful current handling and remains monotonic", async () => {
    const database = databaseHarness();
    const runtime = runtimeHarness();
    const watchdog = watchdogHarness();
    const context = await buildApp(environment, database.pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: runtime.createRuntime },
      createBoardDeliveryHeadWatchdog: watchdog.createWatchdog,
    });
    const broadcast = vi.spyOn(context.io.of("/").adapter, "broadcast");
    try {
      await context.app.ready();
      await attachSocket(context, "delivery-socket").joinBoard();
      broadcast.mockImplementation(() => {
        throw new Error("local publication failed");
      });
      await expect(runtime.handlers.operationCommitted(operationEnvelope(11))).rejects.toThrow(
        "local publication failed",
      );
      expect(watchdog.progress(ids.board)).toBe(10);

      broadcast.mockRestore();
      await runtime.handlers.operationCommitted(operationEnvelope(12));
      await runtime.handlers.operationCommitted(operationEnvelope(11));
      expect(watchdog.progress(ids.board)).toBe(12);
      await context.app.close();
      await runtime.handlers.operationCommitted(operationEnvelope(13));
      expect(watchdog.progress(ids.board)).toBe(0);
    } finally {
      await context.app.close();
    }
  });

  it("does not create a watchdog in local mode and stops it once in distributed shutdown", async () => {
    const database = databaseHarness();
    const localWatchdog = watchdogHarness();
    const local = await buildApp(environment, database.pool, auth, {
      createBoardDeliveryHeadWatchdog: localWatchdog.createWatchdog,
    });
    await local.app.ready();
    await local.app.close();
    expect(localWatchdog.createWatchdog).not.toHaveBeenCalled();

    const runtime = runtimeHarness();
    const distributedWatchdog = watchdogHarness();
    const distributed = await buildApp(environment, database.pool, auth, {
      deliveryMode: { mode: "distributed", createRuntime: runtime.createRuntime },
      createBoardDeliveryHeadWatchdog: distributedWatchdog.createWatchdog,
    });
    await distributed.app.ready();
    await distributed.app.close();
    await distributed.app.close();
    await distributedWatchdog.lifecycle({ state: "recovered" });
    expect(distributedWatchdog.start).toHaveBeenCalledOnce();
    expect(distributedWatchdog.stop).toHaveBeenCalledOnce();
    expect(await runSocketMiddlewares(distributed)).toBeInstanceOf(Error);
  });
});
