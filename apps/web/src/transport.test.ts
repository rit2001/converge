import { beforeEach, describe, expect, it } from "vitest";
import {
  emptyBoardState,
  hashBoardState,
  reduceCommand,
  type BoardState,
} from "@converge/canvas-engine";
import type {
  BoardRecoveryMaterial,
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

function transformOperation(
  seq: number,
  targetId: string,
  payload: { x?: number; rotation?: number },
): CommittedOperation {
  return {
    schemaVersion: 1,
    opId: uuid("6", seq),
    boardId,
    clientId,
    baseSeq: seq - 1,
    targetId,
    clientTimestamp: `2026-08-07T12:02:${String(seq).padStart(2, "0")}.000Z`,
    committedAt: `2026-08-07T12:03:${String(seq).padStart(2, "0")}.000Z`,
    type: "object.transform",
    payload,
    seq,
  };
}

function snapshot(lastSeq = 0): BoardSnapshot {
  return { id: boardId, name: "Recovery", lastSeq, objects: [] };
}

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
        .map(([key, item]) => [key, canonicalValue(item)]),
    );
  }
  if (typeof value === "number" && Object.is(value, -0)) return 0;
  return value;
}

async function sha256(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function replay(operations: CommittedOperation[]): BoardState {
  return operations.reduce((state, item) => {
    const result = reduceCommand(state, item, item.seq);
    if (!result.ok) throw new Error(`Invalid recovery fixture: ${result.code}`);
    return result.state;
  }, emptyBoardState());
}

async function recoveryMaterial(
  snapshotOperations: CommittedOperation[] = [],
  operationTail: CommittedOperation[] = [],
): Promise<BoardRecoveryMaterial> {
  const snapshotStateValue = replay(snapshotOperations);
  const orderedIds = [
    ...snapshotStateValue.order,
    ...Object.keys(snapshotStateValue.objects)
      .filter((id) => !snapshotStateValue.order.includes(id))
      .sort(),
  ];
  const snapshotState: BoardRecoveryMaterial["snapshotState"] = {
    schemaVersion: 1 as const,
    boardId,
    boardName: "Recovery",
    lastSeq: snapshotStateValue.lastSeq,
    lastDeliverySeq: snapshotStateValue.lastSeq,
    objects: orderedIds.map((id, index) => {
      const projected = snapshotStateValue.objects[id];
      if (!projected) throw new Error("Missing recovery fixture object");
      return {
        objectId: id,
        stackOrder: index + 1,
        value: projected.value,
        fieldSeq: {
          id: projected.fieldSeq.id ?? projected.createdSeq,
          kind: projected.fieldSeq.kind ?? projected.createdSeq,
          x: projected.fieldSeq.x ?? projected.createdSeq,
          y: projected.fieldSeq.y ?? projected.createdSeq,
          width: projected.fieldSeq.width ?? projected.createdSeq,
          height: projected.fieldSeq.height ?? projected.createdSeq,
          rotation: projected.fieldSeq.rotation ?? projected.createdSeq,
          fill: projected.fieldSeq.fill ?? projected.createdSeq,
          text: projected.fieldSeq.text ?? projected.createdSeq,
        },
        createdSeq: projected.createdSeq,
        updatedSeq: projected.updatedSeq,
        deletedSeq: projected.deletedSeq,
      };
    }),
  };
  const reconstructed = operationTail.reduce((state, item) => {
    const result = reduceCommand(state, item, item.seq);
    if (!result.ok) throw new Error(`Invalid recovery fixture tail: ${result.code}`);
    return result.state;
  }, snapshotStateValue);
  const snapshotCanonicalHash = await sha256(
    `converge.snapshot.v1\0${JSON.stringify(canonicalValue(snapshotState))}`,
  );
  return {
    boardId,
    snapshotId: "50000000-0000-4000-8000-000000000001",
    snapshotSchemaVersion: 1,
    snapshotCanvasSeq: snapshotState.lastSeq,
    snapshotDeliverySeq: snapshotState.lastDeliverySeq,
    capturedCanvasSeq: reconstructed.lastSeq,
    capturedDeliverySeq: reconstructed.lastSeq,
    snapshotState,
    snapshotCanonicalHash,
    operationTail,
    reconstructedCanonicalHash: await hashBoardState(reconstructed),
  };
}

function deferred<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function settle(): Promise<void> {
  for (let turn = 0; turn < 8; turn += 1) {
    await crypto.subtle.digest("SHA-256", new Uint8Array());
    await Promise.resolve();
  }
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
  connectCalls = 0;
  disconnectCalls = 0;
  readonly joins: Array<{ request: unknown; acknowledge: (value: JoinBoardAck) => void }> = [];
  readonly submissions: DurableCommand[] = [];
  readonly submissionAcknowledgements: Array<(value: unknown) => void> = [];
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
      const acknowledge = values[1];
      if (typeof acknowledge !== "function")
        throw new Error("Operation acknowledgement is required");
      this.submissionAcknowledgements.push(acknowledge as (value: unknown) => void);
    }
    return this;
  }

  serverConnect(): void {
    this.connected = true;
    this.dispatch("connect");
  }

  connect(): this {
    this.connectCalls += 1;
    return this;
  }

  rejectConnection(data: unknown, message = "Socket connection rejected"): void {
    this.dispatch("connect_error", Object.assign(new Error(message), { data }));
  }

  serverDisconnect(): void {
    this.connected = false;
    this.dispatch("disconnect", "transport close");
  }

  reconnectAttempt(): void {
    this.dispatch("io:reconnect_attempt");
  }

  reconnectFailed(): void {
    this.dispatch("io:reconnect_failed");
  }

  deliver(value: unknown): void {
    this.dispatch("operation:committed", value);
  }

  acknowledgeSubmission(index: number, value: unknown): void {
    this.submissionAcknowledgements[index]?.(value);
  }

  revoke(value: unknown): void {
    this.dispatch("board:access-revoked", value);
  }

  disconnect(): this {
    this.disconnectCalls += 1;
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
    loadRecovery?: (id: string, signal: AbortSignal) => Promise<unknown>;
    useDefaultRecovery?: boolean;
    countLimit?: number;
    byteLimit?: number;
    retryCapMs?: number;
    connectSocket?: boolean;
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
    ...(options.loadRecovery
      ? { loadRecovery: options.loadRecovery }
      : options.useDefaultRecovery
        ? {}
        : { loadRecovery: () => recoveryMaterial() }),
    synchronization: {
      liveBufferMaxCount: options.countLimit ?? LIVE_BUFFER_MAX_COUNT,
      liveBufferMaxBytes: options.byteLimit ?? LIVE_BUFFER_MAX_BYTES,
      retryCapMs: options.retryCapMs ?? SYNC_RETRY_CAP_MS,
    },
  });
  transport.connect();
  if (options.connectSocket !== false) socket.serverConnect();
  return { transport, socket, scheduler, persistence, token };
}

function succeedJoin(socket: FakeSocket, index: number, watermark: number): void {
  socket.joins[index]?.acknowledge({ ok: true, boardId, joinWatermark: watermark });
}

function requireRecovery(socket: FakeSocket, index = 0): void {
  socket.joins[index]?.acknowledge({
    ok: false,
    code: "RESYNC_REQUIRED",
    message: "Authoritative recovery is required",
    retryable: true,
  });
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

  it("times out a stalled range body after headers and ignores its late completion", async () => {
    const body = deferred<unknown>();
    const rangeCapture: { signal: AbortSignal | null } = {
      signal: null,
    };
    const fetcher: typeof fetch = (_input, init) => {
      rangeCapture.signal = init?.signal ?? null;
      return Promise.resolve({
        ok: true,
        json: () => body.promise,
      } as Response);
    };
    const test = harness({ fetcher });
    succeedJoin(test.socket, 0, 1);
    await settle();

    expect(useBoardStore.getState().connection).toBe("catching-up");
    test.scheduler.runDelay(SYNC_ACK_TIMEOUT_MS);
    await settle();
    expect(rangeCapture.signal?.aborted).toBe(true);
    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: { attempt: 1, retryScheduled: true },
    });

    body.resolve({
      boardId,
      afterSeq: 0,
      watermark: 1,
      operations: [operation(1)],
      nextSeq: 1,
      hasMore: false,
    });
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: { attempt: 1, retryScheduled: true },
    });
    expect(test.socket.joins).toHaveLength(1);
  });

  it("consumes an ordinary timely range body before becoming READY", async () => {
    const test = harness({
      fetcher: () => Promise.resolve(response(0, 1, [operation(1)])),
    });
    succeedJoin(test.socket, 0, 1);
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      connection: "ready",
      committed: { lastSeq: 1 },
    });
    expect(test.scheduler.tasks.size).toBe(0);
  });

  it("retains retry recovery for an unreadable timely range body", async () => {
    const test = harness({
      fetcher: () =>
        Promise.resolve(
          new Response("{", { status: 200, headers: { "content-type": "application/json" } }),
        ),
    });
    succeedJoin(test.socket, 0, 1);
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: {
        retryCode: "INTERNAL_ERROR",
        retryScheduled: true,
        retryDelayMs: 500,
      },
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

describe("terminal synchronization failures", () => {
  it("terminally fences an initial Socket.IO authentication rejection and preserves pending", async () => {
    const item = pending(1);
    let rangeRequests = 0;
    let snapshotLoads = 0;
    const failed = harness({
      initialPending: [item],
      connectSocket: false,
      fetcher: () => {
        rangeRequests += 1;
        return Promise.resolve(response(0, 0, []));
      },
      loadRecovery: () => {
        snapshotLoads += 1;
        return recoveryMaterial();
      },
    });

    failed.socket.rejectConnection({
      ok: false,
      code: "AUTHENTICATION_REQUIRED",
      message: "Authentication required",
      retryable: false,
    });
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      connection: "authorization-failed",
      pending: [{ opId: item.opId }],
      synchronizationDiagnostics: {
        retryCode: "AUTHENTICATION_REQUIRED",
        retryScheduled: false,
        bufferedCount: 0,
        bufferedBytes: 0,
      },
      error: "AUTHENTICATION_REQUIRED: Authentication required",
    });
    expect(failed.persistence.rows.has(item.opId)).toBe(true);
    expect(failed.scheduler.tasks.size).toBe(0);
    expect(failed.socket).toMatchObject({
      connected: false,
      connectCalls: 1,
      disconnectCalls: 1,
      joins: [],
      submissions: [],
    });
    expect({ rangeRequests, snapshotLoads }).toEqual({ rangeRequests: 0, snapshotLoads: 0 });

    failed.transport.connect();
    failed.socket.reconnectAttempt();
    failed.socket.reconnectFailed();
    failed.socket.serverDisconnect();
    failed.socket.serverConnect();
    failed.socket.deliver(operation(1));
    failed.socket.revoke({
      schemaVersion: 1,
      boardId,
      code: "ACCESS_REVOKED",
      message: "Board access was revoked",
    });
    const lateCommand = pending(2);
    expect(await failed.transport.submit(lateCommand)).toBe(false);
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      connection: "authorization-failed",
      committed: { lastSeq: 0 },
      pending: [{ opId: item.opId }],
      synchronizationDiagnostics: { retryCode: "AUTHENTICATION_REQUIRED" },
    });
    expect(failed.persistence.rows.has(item.opId)).toBe(true);
    expect(failed.persistence.rows.has(lateCommand.opId)).toBe(false);
    expect(failed.socket).toMatchObject({ connectCalls: 1, disconnectCalls: 1, joins: [] });
    expect({
      rangeRequests,
      snapshotLoads,
      submissions: failed.socket.submissions.length,
    }).toEqual({ rangeRequests: 0, snapshotLoads: 0, submissions: 0 });

    const replacement = harness();
    succeedJoin(replacement.socket, 0, 0);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      sessionToken: replacement.token,
      connection: "ready",
      committed: { lastSeq: 0 },
    });
  });

  it("does not trust malformed Socket.IO connection-error data", async () => {
    const test = harness({ connectSocket: false });
    test.socket.rejectConnection(
      {
        code: "AUTHENTICATION_REQUIRED",
        message: "Forged authentication failure",
        retryable: false,
      },
      "Authentication required",
    );
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      connection: "connecting",
      error: null,
      synchronizationDiagnostics: { retryCode: null, retryScheduled: false },
    });
    expect(test.socket.disconnectCalls).toBe(0);
    expect(test.scheduler.tasks.size).toBe(0);

    test.socket.serverConnect();
    succeedJoin(test.socket, 0, 0);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "ready",
      committed: { lastSeq: 0 },
    });
  });

  it("fences a non-retryable authentication failure through disconnect and reconnect", async () => {
    const item = pending(1);
    let rangeRequests = 0;
    let snapshotLoads = 0;
    const test = harness({
      initialPending: [item],
      fetcher: () => {
        rangeRequests += 1;
        return Promise.resolve(
          new Response(
            JSON.stringify({
              ok: false,
              code: "AUTHENTICATION_REQUIRED",
              message: "Authentication required",
              retryable: false,
            }),
            { status: 401, headers: { "content-type": "application/json" } },
          ),
        );
      },
      loadRecovery: () => {
        snapshotLoads += 1;
        return recoveryMaterial();
      },
    });
    succeedJoin(test.socket, 0, 1);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "authorization-failed",
      pending: [{ opId: item.opId }],
      synchronizationDiagnostics: {
        retryCode: "AUTHENTICATION_REQUIRED",
        retryScheduled: false,
      },
    });
    expect(test.persistence.rows.has(item.opId)).toBe(true);
    expect(test.scheduler.tasks.size).toBe(0);
    expect(test.socket.connected).toBe(false);
    expect(test.socket.joins).toHaveLength(1);
    expect(test.socket.joins[0]?.request).toBeDefined();
    expect({ rangeRequests, snapshotLoads, submissions: test.socket.submissions.length }).toEqual({
      rangeRequests: 1,
      snapshotLoads: 0,
      submissions: 0,
    });

    test.socket.reconnectAttempt();
    test.socket.reconnectFailed();
    test.socket.serverDisconnect();
    test.socket.serverConnect();
    test.socket.deliver(operation(1));
    expect(await test.transport.submit(pending(2))).toBe(false);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "authorization-failed",
      committed: { lastSeq: 0 },
      pending: [{ opId: item.opId }],
      synchronizationDiagnostics: { retryCode: "AUTHENTICATION_REQUIRED" },
    });
    expect(test.persistence.rows.has(item.opId)).toBe(true);
    expect(test.socket.joins).toHaveLength(1);
    expect({ rangeRequests, snapshotLoads, submissions: test.socket.submissions.length }).toEqual({
      rangeRequests: 1,
      snapshotLoads: 0,
      submissions: 0,
    });
  });

  it("fences protocol-integrity failures and allows a replacement transport", async () => {
    let rangeRequests = 0;
    let snapshotLoads = 0;
    const failed = harness({
      fetcher: () => {
        rangeRequests += 1;
        return Promise.resolve(response(0, 0, []));
      },
      loadRecovery: () => {
        snapshotLoads += 1;
        return recoveryMaterial();
      },
    });
    failed.socket.deliver({ malformed: true });
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "error",
      error: "INVALID_COMMAND: Invalid live operation",
      synchronizationDiagnostics: {
        retryCode: "INVALID_COMMAND",
        retryScheduled: false,
      },
    });
    expect(failed.socket.connected).toBe(false);

    succeedJoin(failed.socket, 0, 9);
    failed.socket.reconnectAttempt();
    failed.socket.reconnectFailed();
    failed.socket.serverConnect();
    failed.socket.deliver(operation(1));
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "error",
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: { retryCode: "INVALID_COMMAND" },
    });
    expect(failed.socket.joins).toHaveLength(1);
    expect({ rangeRequests, snapshotLoads, submissions: failed.socket.submissions.length }).toEqual(
      {
        rangeRequests: 0,
        snapshotLoads: 0,
        submissions: 0,
      },
    );

    const replacement = harness();
    succeedJoin(replacement.socket, 0, 0);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      sessionToken: replacement.token,
      connection: "ready",
      committed: { lastSeq: 0 },
    });
    expect(replacement.socket.joins).toHaveLength(1);
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
  it("recovers a gap-revealing acknowledgement before draining the next command", async () => {
    const first = pending(2);
    const second = pending(3);
    const acknowledged = { ...first, seq: 2, committedAt: "2026-08-07T12:01:02.000Z" };
    let rangeRequests = 0;
    const test = harness({
      initialPending: [first, second],
      fetcher: () => {
        rangeRequests += 1;
        return Promise.resolve(response(0, 2, [operation(1), acknowledged]));
      },
    });
    succeedJoin(test.socket, 0, 0);
    await settle();
    expect(test.socket.submissions).toEqual([first]);

    test.socket.acknowledgeSubmission(0, {
      ok: true,
      duplicate: false,
      operation: acknowledged,
    });
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "joining",
      committed: { lastSeq: 0 },
      pending: [{ opId: second.opId }],
    });
    expect(test.socket.joins).toHaveLength(2);
    expect(test.socket.submissions).toEqual([first]);
    expect(rangeRequests).toBe(0);
    expect(test.persistence.rows.has(first.opId)).toBe(false);

    succeedJoin(test.socket, 1, 2);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "ready",
      committed: { lastSeq: 2 },
      pending: [{ opId: second.opId }],
    });
    expect(rangeRequests).toBe(1);
    expect(test.socket.submissions).toEqual([first, second]);
  });

  it("routes a conflicting acknowledgement through retry recovery", async () => {
    const first = pending(2);
    const second = pending(3);
    const test = harness({ initialPending: [first, second] });
    succeedJoin(test.socket, 0, 0);
    await settle();
    test.socket.deliver(operation(1));
    test.socket.acknowledgeSubmission(0, {
      ok: true,
      duplicate: false,
      operation: { ...first, seq: 1, committedAt: "2026-08-07T12:01:01.000Z" },
    });
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      committed: { lastSeq: 1 },
      pending: [{ opId: second.opId }],
      synchronizationDiagnostics: {
        retryCode: "RESYNC_REQUIRED",
        retryScheduled: true,
        retryDelayMs: 500,
      },
    });
    expect(test.socket.submissions).toEqual([first]);
    expect(test.persistence.rows.has(first.opId)).toBe(false);
    expect(test.scheduler.delays()).toEqual([500]);
  });

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
      loadRecovery: () => {
        snapshotLoads += 1;
        return recoveryMaterial();
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
      loadRecovery: () => {
        snapshotLoads += 1;
        return recoveryMaterial();
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

describe("verified snapshot-plus-tail recovery", () => {
  it("applies an exact-head snapshot with an empty tail", async () => {
    const first = operation(1);
    const material = await recoveryMaterial([first]);
    const test = harness({ loadRecovery: () => Promise.resolve(material) });

    requireRecovery(test.socket);
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      committed: { lastSeq: 1, order: [first.targetId] },
      objects: [{ id: first.targetId }],
    });
    expect(material.operationTail).toEqual([]);
  });

  it("replays one and multiple contiguous operations while preserving stacking and rotation", async () => {
    const first = operation(1);
    const second = operation(2);
    const rotated = transformOperation(3, first.targetId, { x: 77, rotation: 35 });
    const oneTailMaterial = await recoveryMaterial([first], [second]);
    const oneTail = harness({ loadRecovery: () => Promise.resolve(oneTailMaterial) });
    requireRecovery(oneTail.socket);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      committed: { lastSeq: 2, order: [first.targetId, second.targetId] },
    });

    const material = await recoveryMaterial([first], [second, rotated]);
    const test = harness({ loadRecovery: () => Promise.resolve(material) });

    requireRecovery(test.socket);
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      committed: { lastSeq: 3, order: [first.targetId, second.targetId] },
      objects: [
        { id: first.targetId, x: 77, rotation: 35 },
        { id: second.targetId, rotation: 0 },
      ],
    });
  });

  it("publishes only the fully replayed state and retains durable pending commands until READY", async () => {
    const first = operation(1);
    const second = operation(2);
    const third = operation(3);
    const pendingCommand = pending(9);
    const material = await recoveryMaterial([first], [second, third]);
    const test = harness({
      initialPending: [pendingCommand],
      loadRecovery: () => Promise.resolve(material),
    });
    let prior = useBoardStore.getState().committed;
    const committedSequences: number[] = [];
    const unsubscribe = useBoardStore.subscribe((state) => {
      if (state.committed === prior) return;
      prior = state.committed;
      committedSequences.push(state.committed.lastSeq);
    });

    requireRecovery(test.socket);
    await settle();

    expect(committedSequences).toEqual([3]);
    expect(test.persistence.rows.has(pendingCommand.opId)).toBe(true);
    expect(test.socket.submissions).toEqual([]);
    test.scheduler.runDelay(500);
    succeedJoin(test.socket, 1, 3);
    await settle();
    expect(useBoardStore.getState().connection).toBe("ready");
    expect(test.socket.submissions).toEqual([pendingCommand]);
    expect(test.persistence.rows.has(pendingCommand.opId)).toBe(true);
    unsubscribe();
  });

  it("treats RECOVERY_BLOCKED as terminal without retry or legacy snapshot fallback", async () => {
    const repairedMaterial = await recoveryMaterial([], [operation(1)]);
    const urls: string[] = [];
    const failed = harness({
      useDefaultRecovery: true,
      fetcher: (input) => {
        urls.push(
          typeof input === "string" ? input : input instanceof URL ? input.href : input.url,
        );
        return Promise.resolve(
          new Response(
            JSON.stringify({
              ok: false,
              code: "RECOVERY_BLOCKED",
              message: "Authoritative board recovery is unavailable",
              retryable: false,
            }),
            { status: 409, headers: { "content-type": "application/json" } },
          ),
        );
      },
    });

    requireRecovery(failed.socket);
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      connection: "error",
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: { retryCode: "RECOVERY_BLOCKED", retryScheduled: false },
    });
    expect(failed.socket.connected).toBe(false);
    expect(failed.scheduler.tasks.size).toBe(0);
    expect(urls).toEqual([`http://localhost:4000/v1/boards/${boardId}/recovery`]);

    const replacement = harness({ loadRecovery: () => Promise.resolve(repairedMaterial) });
    requireRecovery(replacement.socket);
    await settle();
    replacement.scheduler.runDelay(500);
    succeedJoin(replacement.socket, 1, 1);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      sessionToken: replacement.token,
      connection: "ready",
      committed: { lastSeq: 1 },
      authoritativeHash: {
        status: "ready",
        seq: 1,
        value: repairedMaterial.reconstructedCanonicalHash,
      },
    });
  });

  it("retries transient recovery failures and keeps authorization failures terminal", async () => {
    const material = await recoveryMaterial();
    let recoveryRequests = 0;
    const retried = harness({
      useDefaultRecovery: true,
      fetcher: () => {
        recoveryRequests += 1;
        const failure = recoveryRequests === 1;
        return Promise.resolve(
          new Response(
            JSON.stringify(
              failure
                ? {
                    ok: false,
                    code: "INTERNAL_ERROR",
                    message: "An internal server error occurred.",
                    retryable: true,
                  }
                : material,
            ),
            { status: failure ? 500 : 200, headers: { "content-type": "application/json" } },
          ),
        );
      },
    });
    requireRecovery(retried.socket);
    await settle();
    expect(useBoardStore.getState().connection).toBe("retry-wait");
    retried.scheduler.runDelay(500);
    requireRecovery(retried.socket, 1);
    await settle();
    retried.scheduler.runDelay(1_000);
    succeedJoin(retried.socket, 2, 0);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({ connection: "ready", error: null });
    expect(recoveryRequests).toBe(2);

    const unauthorized = harness({
      useDefaultRecovery: true,
      fetcher: () =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              ok: false,
              code: "AUTHENTICATION_REQUIRED",
              message: "Authentication required",
              retryable: false,
            }),
            { status: 401, headers: { "content-type": "application/json" } },
          ),
        ),
    });
    requireRecovery(unauthorized.socket);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({ connection: "authorization-failed" });
  });

  it.each([
    ["wrong board", async () => ({ ...(await recoveryMaterial()), boardId: uuid("9", 1) })],
    [
      "unsafe head",
      async () => ({
        ...(await recoveryMaterial()),
        capturedCanvasSeq: Number.MAX_SAFE_INTEGER + 1,
      }),
    ],
    [
      "snapshot hash mismatch",
      async () => ({ ...(await recoveryMaterial()), snapshotCanonicalHash: "f".repeat(64) }),
    ],
    [
      "tail gap",
      async () => {
        const material = await recoveryMaterial([], [operation(1), operation(2)]);
        return { ...material, operationTail: [material.operationTail[1]] };
      },
    ],
    [
      "duplicate sequence",
      async () => {
        const material = await recoveryMaterial([], [operation(1), operation(2)]);
        return {
          ...material,
          operationTail: [material.operationTail[0], { ...material.operationTail[1], seq: 1 }],
        };
      },
    ],
    [
      "malformed operation",
      async () => {
        const material = await recoveryMaterial([], [operation(1)]);
        return {
          ...material,
          operationTail: [{ ...material.operationTail[0], privateField: true }],
        };
      },
    ],
    [
      "reducer failure",
      async () => ({
        ...(await recoveryMaterial()),
        capturedCanvasSeq: 1,
        capturedDeliverySeq: 1,
        operationTail: [transformOperation(1, uuid("8", 1), { x: 10 })],
      }),
    ],
    [
      "reconstructed hash mismatch",
      async () => ({ ...(await recoveryMaterial()), reconstructedCanonicalHash: "e".repeat(64) }),
    ],
    ["unknown field", async () => ({ ...(await recoveryMaterial()), privateField: true })],
  ])("terminally rejects %s evidence before store mutation", async (_name, buildMaterial) => {
    const raw = await buildMaterial();
    const test = harness({ loadRecovery: () => Promise.resolve(raw) });

    requireRecovery(test.socket);
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      connection: "error",
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: { retryCode: "INVALID_COMMAND", retryScheduled: false },
    });
    expect(test.socket.connected).toBe(false);
    expect(test.scheduler.tasks.size).toBe(0);
  });

  it("keeps the timeout active through stalled response-body consumption", async () => {
    const body = deferred<unknown>();
    const test = harness({
      useDefaultRecovery: true,
      fetcher: () => Promise.resolve({ ok: true, json: () => body.promise } as unknown as Response),
    });
    requireRecovery(test.socket);
    await settle();

    test.scheduler.runDelay(SYNC_ACK_TIMEOUT_MS);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "retry-wait",
      committed: { lastSeq: 0 },
      synchronizationDiagnostics: { retryCode: "INTERNAL_ERROR" },
    });

    body.resolve(await recoveryMaterial([], [operation(1)]));
    await settle();
    expect(useBoardStore.getState().committed.lastSeq).toBe(0);
  });

  it("ignores cancelled recovery material after a replacement session owns the store", async () => {
    const late = deferred<unknown>();
    const stale = harness({ loadRecovery: () => late.promise });
    requireRecovery(stale.socket);
    await settle();
    stale.transport.disconnect();

    const replacement = harness();
    succeedJoin(replacement.socket, 0, 0);
    await settle();
    late.resolve(await recoveryMaterial([], [operation(1)]));
    await settle();

    expect(useBoardStore.getState()).toMatchObject({
      sessionToken: replacement.token,
      connection: "ready",
      committed: { lastSeq: 0 },
    });
  });
});

describe("board access revocation", () => {
  const event = {
    schemaVersion: 1 as const,
    boardId,
    code: "ACCESS_REVOKED" as const,
    message: "Board access was revoked",
  };

  it("terminates the matching session, preserves pending work, and rejects stale restoration", async () => {
    const item = pending(1);
    const test = harness({ initialPending: [item] });
    test.socket.deliver(operation(2));
    expect(useBoardStore.getState().synchronizationDiagnostics.bufferedCount).toBe(1);
    test.socket.revoke(event);
    expect(test.socket.connected).toBe(false);
    expect(test.scheduler.tasks.size).toBe(0);
    expect(useBoardStore.getState()).toMatchObject({
      boardId,
      connection: "authorization-failed",
      committed: { lastSeq: 0 },
      pending: [{ opId: item.opId }],
      objects: [],
      synchronizationDiagnostics: {
        retryCode: "ACCESS_REVOKED",
        retryScheduled: false,
        bufferedCount: 0,
        bufferedBytes: 0,
      },
      error: "ACCESS_REVOKED: Board access was revoked",
    });
    succeedJoin(test.socket, 0, 2);
    await settle();
    expect(useBoardStore.getState()).toMatchObject({
      connection: "authorization-failed",
      committed: { lastSeq: 0 },
    });
    expect(test.socket.submissions).toHaveLength(0);
  });

  it("ignores a different board or obsolete session and rejects malformed active events", () => {
    const current = harness();
    current.socket.revoke({ ...event, boardId: uuid("1", 99) });
    expect(current.socket.connected).toBe(true);
    expect(useBoardStore.getState().connection).toBe("joining");

    const obsolete = current;
    const replacement = harness();
    obsolete.socket.revoke(event);
    expect(useBoardStore.getState()).toMatchObject({
      sessionToken: replacement.token,
      connection: "joining",
    });

    replacement.socket.revoke({ ...event, surprise: true });
    expect(replacement.socket.connected).toBe(false);
    expect(useBoardStore.getState()).toMatchObject({
      connection: "error",
      error: "INVALID_COMMAND: Invalid board-access-revoked event",
    });
  });
});
