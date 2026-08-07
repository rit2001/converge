import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoardSnapshot,
  CommittedOperation,
  DurableCommand,
  JoinBoardAck,
} from "@converge/protocol";
import type { BoardSessionToken } from "./board-session";
import { useBoardStore } from "./board-store";
import type { RetryScheduler } from "./pending-command-queue";
import type { PendingLoadResult, PendingOperationStore } from "./pending-db";
import {
  BoardTransport,
  LIVE_BUFFER_MAX_BYTES,
  LIVE_BUFFER_MAX_COUNT,
  SYNC_ACK_TIMEOUT_MS,
  SYNC_RETRY_CAP_MS,
} from "./transport";

const boardId = "10000000-0000-4000-8000-000000000001";
const clientId = "20000000-0000-4000-8000-000000000001";
let generation = 0;

function uuid(group: string, index: number): string {
  return `${group}0000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`;
}

function operation(seq: number): CommittedOperation {
  return {
    ...pending(seq),
    seq,
    committedAt: `2026-08-07T12:01:${String(seq).padStart(2, "0")}.000Z`,
  };
}

function pending(index: number): DurableCommand {
  const targetId = uuid("3", index);
  return {
    schemaVersion: 1,
    opId: uuid("4", index),
    boardId,
    clientId,
    baseSeq: index - 1,
    targetId,
    clientTimestamp: `2026-08-07T12:00:${String(index).padStart(2, "0")}.000Z`,
    type: "object.create",
    payload: {
      id: targetId,
      kind: "rectangle",
      x: index,
      y: index,
      width: 100,
      height: 80,
      rotation: 0,
      fill: "#818cf8",
      text: "",
    },
  };
}

function snapshot(lastSeq = 0): BoardSnapshot {
  return { id: boardId, name: "Recovery", lastSeq, objects: [] };
}

async function settle(): Promise<void> {
  for (let turn = 0; turn < 20; turn += 1) await Promise.resolve();
}

class FakeScheduler implements RetryScheduler {
  private nextId = 1;
  readonly tasks = new Map<number, { delay: number; callback: () => void }>();

  setTimeout(callback: () => void, delay: number): number {
    const id = this.nextId++;
    this.tasks.set(id, { delay, callback });
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.tasks.delete(handle as number);
  }

  random(): number {
    return 0;
  }

  runDelay(delay: number): void {
    const entry = [...this.tasks].find(([, task]) => task.delay === delay);
    if (!entry) throw new Error(`No task scheduled for ${delay}ms`);
    this.tasks.delete(entry[0]);
    entry[1].callback();
  }

  delays(): number[] {
    return [...this.tasks.values()].map(({ delay }) => delay);
  }
}

type Listener = (...values: unknown[]) => void;

class FakeSocket {
  connected = false;
  readonly joins: Array<{ request: unknown; acknowledge: (value: JoinBoardAck) => void }> = [];
  readonly submissions: DurableCommand[] = [];
  private readonly listeners = new Map<string, Listener[]>();
  readonly io = {
    on: (event: string, listener: Listener): void => this.addListener(`io:${event}`, listener),
  };

  on(event: string, listener: Listener): this {
    this.addListener(event, listener);
    return this;
  }

  emit(event: string, ...values: unknown[]): this {
    if (event === "board:join") {
      const acknowledge = values[1];
      if (typeof acknowledge !== "function") throw new Error("Join acknowledgement is required");
      this.joins.push({
        request: values[0],
        acknowledge: acknowledge as (value: JoinBoardAck) => void,
      });
    } else if (event === "operation:submit") {
      this.submissions.push(values[0] as DurableCommand);
    }
    return this;
  }

  serverConnect(): void {
    this.connected = true;
    this.dispatch("connect");
  }

  connect(): this {
    return this;
  }

  serverDisconnect(): void {
    this.connected = false;
    this.dispatch("disconnect", "transport close");
  }

  deliver(value: unknown): void {
    this.dispatch("operation:committed", value);
  }

  disconnect(): this {
    if (this.connected) this.serverDisconnect();
    return this;
  }

  private addListener(event: string, listener: Listener): void {
    this.listeners.set(event, [...(this.listeners.get(event) ?? []), listener]);
  }

  private dispatch(event: string, ...values: unknown[]): void {
    for (const listener of this.listeners.get(event) ?? []) listener(...values);
  }
}

class MemoryPendingStore implements PendingOperationStore {
  readonly rows = new Map<string, DurableCommand>();

  load(requestedBoardId: string): Promise<PendingLoadResult> {
    return Promise.resolve({
      commands: [...this.rows.values()].filter((item) => item.boardId === requestedBoardId),
      corruptCount: 0,
    });
  }

  put(command: DurableCommand): Promise<void> {
    this.rows.set(command.opId, command);
    return Promise.resolve();
  }

  delete(_boardId: string, operationId: string): Promise<void> {
    this.rows.delete(operationId);
    return Promise.resolve();
  }
}

function response(after: number, watermark: number, operations: CommittedOperation[]): Response {
  return new Response(
    JSON.stringify({
      boardId,
      afterSeq: after,
      watermark,
      operations,
      nextSeq: operations.at(-1)?.seq ?? after,
      hasMore: (operations.at(-1)?.seq ?? after) < watermark,
    }),
    { status: 200, headers: { "content-type": "application/json" } },
  );
}

function harness(
  options: {
    initialSeq?: number;
    initialPending?: DurableCommand[];
    fetcher?: typeof fetch;
    loadSnapshot?: (id: string, signal: AbortSignal) => Promise<unknown>;
    countLimit?: number;
    byteLimit?: number;
    retryCapMs?: number;
  } = {},
) {
  generation += 1;
  const token: BoardSessionToken = { generation, nonce: Symbol("sync-test") };
  const initialPending = options.initialPending ?? [];
  useBoardStore.getState().beginSession(token, boardId);
  useBoardStore.getState().initializeSession(token, snapshot(options.initialSeq), initialPending);
  const socket = new FakeSocket();
  const scheduler = new FakeScheduler();
  const persistence = new MemoryPendingStore();
  for (const command of initialPending) persistence.rows.set(command.opId, command);
  const fetcher = options.fetcher ?? (() => Promise.resolve(response(0, 0, [])));
  const transport = new BoardTransport(boardId, clientId, token, {
    scheduler,
    pendingStore: persistence,
    socketFactory: () => socket as never,
    fetcher,
    loadSnapshot: options.loadSnapshot ?? (() => Promise.resolve(snapshot())),
    synchronization: {
      liveBufferMaxCount: options.countLimit ?? LIVE_BUFFER_MAX_COUNT,
      liveBufferMaxBytes: options.byteLimit ?? LIVE_BUFFER_MAX_BYTES,
      retryCapMs: options.retryCapMs ?? SYNC_RETRY_CAP_MS,
    },
  });
  transport.connect();
  socket.serverConnect();
  return { transport, socket, scheduler, persistence, token };
}

function succeedJoin(socket: FakeSocket, index: number, watermark: number): void {
  socket.joins[index]?.acknowledge({ ok: true, boardId, joinWatermark: watermark });
}

beforeEach(() => {
  useBoardStore.getState().endSession(useBoardStore.getState().sessionToken as BoardSessionToken);
});

describe("self-healing synchronization", () => {
  it("retries a timed-out join, ignores its late acknowledgement, and reaches READY", async () => {
    const test = harness();
    expect(test.socket.joins).toHaveLength(1);
    test.scheduler.runDelay(SYNC_ACK_TIMEOUT_MS);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      synchronizationDiagnostics: {
        attempt: 1,
        retryScheduled: true,
        retryDelayMs: 500,
      },
    });
    expect(test.scheduler.delays()).toEqual([500]);

    test.scheduler.runDelay(500);
    expect(test.socket.joins).toHaveLength(2);
    succeedJoin(test.socket, 1, 0);
    await settle();
    expect(useBoardStore.getState().connection).toBe("ready");

    succeedJoin(test.socket, 0, 99);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "ready",
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: { attempt: 2, retryScheduled: false },
    });
  });

  it("recovers automatically after a catch-up range timeout", async () => {
    let fetchAttempt = 0;
    const fetcher: typeof fetch = (_input, init) => {
      fetchAttempt += 1;
      if (fetchAttempt === 1)
        return new Promise<Response>((_resolve, reject) =>
          init?.signal?.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          ),
        );
      return Promise.resolve(response(0, 1, [operation(1)]));
    };
    const test = harness({ fetcher });
    succeedJoin(test.socket, 0, 1);
    await settle();
    test.scheduler.runDelay(SYNC_ACK_TIMEOUT_MS);
    await settle();
    expect(useBoardStore.getState().connection).toBe("retry-wait");
    test.scheduler.runDelay(500);
    succeedJoin(test.socket, 1, 1);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "ready",
      committed: { lastSeq: 1 },
    });
  });

  it("backs off retryable errors, resets only after READY, and stops for authorization", async () => {
    const test = harness();
    test.socket.joins[0]?.acknowledge({
      ok: false,
      code: "INTERNAL_ERROR",
      message: "temporary",
      retryable: true,
    });
    await settle();
    expect(test.scheduler.delays()).toEqual([500]);
    test.scheduler.runDelay(500);
    test.socket.joins[1]?.acknowledge({
      ok: false,
      code: "INTERNAL_ERROR",
      message: "temporary",
      retryable: true,
    });
    await settle();
    expect(test.scheduler.delays()).toEqual([1_000]);
    test.scheduler.runDelay(1_000);
    succeedJoin(test.socket, 2, 0);
    await settle();
    test.socket.deliver(operation(2));
    expect(test.socket.joins).toHaveLength(4);
    test.socket.joins[3]?.acknowledge({
      ok: false,
      code: "FORBIDDEN",
      message: "Board not available",
      retryable: false,
    });
    await settle();
    expect(useBoardStore.getState().connection).toBe("authorization-failed");
    expect(test.scheduler.tasks.size).toBe(0);
  });

  it("cancels retry on disconnect, reconnects immediately, and retains failure backoff", async () => {
    const test = harness();
    test.socket.joins[0]?.acknowledge({
      ok: false,
      code: "INTERNAL_ERROR",
      message: "temporary",
      retryable: true,
    });
    await settle();
    expect(test.scheduler.tasks.size).toBe(1);
    test.socket.serverDisconnect();
    expect(test.scheduler.tasks.size).toBe(0);
    test.socket.serverConnect();
    expect(test.socket.joins).toHaveLength(2);
    test.socket.joins[1]?.acknowledge({
      ok: false,
      code: "INTERNAL_ERROR",
      message: "still temporary",
      retryable: true,
    });
    await settle();
    expect(test.scheduler.delays()).toEqual([1_000]);
    test.scheduler.runDelay(1_000);
    succeedJoin(test.socket, 2, 0);
    await settle();
    expect(useBoardStore.getState().connection).toBe("ready");
  });

  it("does not let an obsolete session timer or acknowledgement mutate its replacement", async () => {
    const first = harness();
    first.scheduler.runDelay(SYNC_ACK_TIMEOUT_MS);
    await settle();
    first.transport.disconnect();
    const second = harness();
    succeedJoin(second.socket, 0, 0);
    await settle();
    first.socket.joins[0]?.acknowledge({ ok: true, boardId, joinWatermark: 500 });
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      sessionToken: second.token,
      connection: "ready",
      committed: { lastSeq: 0 },
    });
  });
});

describe("bounded live buffering", () => {
  it("charges duplicate operations once and recovers when count would exceed the limit", async () => {
    const test = harness({ countLimit: 2 });
    test.socket.deliver(operation(1));
    test.socket.deliver(operation(1));
    test.socket.deliver(operation(2));
    expect(useBoardStore.getState().synchronizationDiagnostics.bufferedCount).toBe(2);
    test.socket.deliver(operation(3));
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      synchronizationDiagnostics: {
        retryCode: "BUFFER_LIMIT_EXCEEDED",
        bufferedCount: 0,
        bufferedBytes: 0,
      },
    });
    expect(test.scheduler.delays()).toEqual([500]);
  });

  it("never exceeds the configured byte limit and rejects conflicting sequence data", async () => {
    const first = operation(1);
    const firstBytes = new TextEncoder().encode(JSON.stringify(first)).byteLength;
    const test = harness({ byteLimit: firstBytes });
    test.socket.deliver(first);
    expect(useBoardStore.getState().synchronizationDiagnostics.bufferedBytes).toBe(firstBytes);
    test.socket.deliver(operation(2));
    await settle();
    expect(useBoardStore.getState().synchronizationDiagnostics.bufferedBytes).toBe(0);

    test.scheduler.runDelay(500);
    const conflict = harness({ countLimit: 5 });
    conflict.socket.deliver(first);
    conflict.socket.deliver({ ...first, opId: uuid("4", 99) });
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      synchronizationDiagnostics: { retryCode: "RESYNC_REQUIRED", bufferedCount: 0 },
    });
  });

  it("recovers discarded overflow operations from the fixed-watermark durable log", async () => {
    const test = harness({
      countLimit: 1,
      fetcher: () => Promise.resolve(response(0, 2, [operation(1), operation(2)])),
    });
    test.socket.deliver(operation(1));
    test.socket.deliver(operation(2));
    await settle();
    test.scheduler.runDelay(500);
    succeedJoin(test.socket, 1, 2);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "ready",
      committed: { lastSeq: 2 },
      synchronizationDiagnostics: { bufferedCount: 0, bufferedBytes: 0 },
    });
  });

  it("keeps memory bounded and backoff capped across repeated overflow", async () => {
    const test = harness({ countLimit: 1, retryCapMs: 1_000 });
    for (const expectedDelay of [500, 1_000, 1_000]) {
      test.socket.deliver(operation(1));
      test.socket.deliver(operation(2));
      await settle();
      expect(useBoardStore.getState().synchronizationDiagnostics.bufferedCount).toBe(0);
      expect(test.scheduler.delays()).toEqual([expectedDelay]);
      test.scheduler.runDelay(expectedDelay);
    }
    expect(useBoardStore.getState().connection).toBe("joining");
  });

  it("leaves READY on a gap and catches up without applying the later event out of order", async () => {
    const test = harness({
      fetcher: () => Promise.resolve(response(0, 2, [operation(1), operation(2)])),
    });
    succeedJoin(test.socket, 0, 0);
    await settle();
    test.socket.deliver(operation(2));
    expect(useBoardStore.getState()).toMatchObject({
      committed: { lastSeq: 0 },
      connection: "joining",
    });
    succeedJoin(test.socket, 1, 2);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "ready",
      committed: { lastSeq: 2 },
    });
  });
});

describe("authoritative resynchronization and pending interaction", () => {
  it("reloads the snapshot when the durable operation range is unavailable", async () => {
    let snapshotLoads = 0;
    const test = harness({
      fetcher: () =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              ok: false,
              code: "RESYNC_REQUIRED",
              message: "Operation range is unavailable",
              retryable: true,
            }),
            { status: 409, headers: { "content-type": "application/json" } },
          ),
        ),
      loadSnapshot: () => {
        snapshotLoads += 1;
        return Promise.resolve(snapshot(0));
      },
    });
    succeedJoin(test.socket, 0, 1);
    await settle();
    expect(snapshotLoads).toBe(1);
    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: { retryCode: "RESYNC_REQUIRED" },
    });
    test.scheduler.runDelay(500);
    succeedJoin(test.socket, 1, 0);
    await settle();
    expect(useBoardStore.getState().connection).toBe("ready");
  });

  it("reloads an authoritative snapshot for client-ahead RESYNC_REQUIRED and preserves pending", async () => {
    const item = pending(1);
    let snapshotLoads = 0;
    const test = harness({
      initialSeq: 5,
      initialPending: [item],
      loadSnapshot: () => {
        snapshotLoads += 1;
        return Promise.resolve(snapshot(0));
      },
    });
    test.socket.joins[0]?.acknowledge({
      ok: false,
      code: "RESYNC_REQUIRED",
      message: "Client sequence exceeds authoritative board head",
      retryable: true,
    });
    await settle();
    expect(snapshotLoads).toBe(1);
    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      committed: { lastSeq: 0 },
      pending: [{ opId: item.opId }],
    });
    expect(test.socket.submissions).toHaveLength(0);
    test.scheduler.runDelay(500);
    expect(test.socket.joins[1]?.request).toMatchObject({ lastAppliedSeq: 0 });
    succeedJoin(test.socket, 1, 0);
    await settle();
    expect(useBoardStore.getState().connection).toBe("ready");
    expect(test.socket.submissions).toEqual([item]);
  });

  it("keeps pending commands paused through retry wait and resumes one ordered drain at READY", async () => {
    const first = pending(1);
    const second = pending(2);
    const test = harness({ initialPending: [first, second] });
    test.scheduler.runDelay(SYNC_ACK_TIMEOUT_MS);
    await settle();
    expect(test.socket.submissions).toHaveLength(0);
    test.scheduler.runDelay(500);
    succeedJoin(test.socket, 1, 0);
    await settle();
    expect(test.socket.submissions).toEqual([first]);
  });
});
