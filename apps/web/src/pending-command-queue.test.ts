import { describe, expect, it } from "vitest";
import type {
  BoardSnapshot,
  CommittedOperation,
  DurableCommand,
  OperationAck,
} from "@converge/protocol";
import type { BoardSessionToken } from "./board-session";
import { createBoardStore } from "./board-store";
import {
  PendingCommandQueue,
  PENDING_RETRY_CAP_MS,
  timedSubmission,
  type RetryScheduler,
  type SubmissionAttempt,
  type SubmissionOutcome,
} from "./pending-command-queue";
import type { PendingLoadResult, PendingOperationStore } from "./pending-db";

const boardId = "10000000-0000-4000-8000-000000000001";
const token: BoardSessionToken = { generation: 1, nonce: Symbol("queue-session") };

function id(prefix: string, index: number): string {
  return `${prefix}0000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`;
}

function command(index: number): DurableCommand {
  const targetId = id("3", index);
  return {
    schemaVersion: 1,
    opId: id("4", index),
    boardId,
    clientId: "20000000-0000-4000-8000-000000000001",
    baseSeq: 0,
    targetId,
    clientTimestamp: `2026-08-07T12:${String(Math.floor(index / 60) % 60).padStart(2, "0")}:${String(index % 60).padStart(2, "0")}.000Z`,
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

function committed(item: DurableCommand, seq: number): CommittedOperation {
  return { ...item, seq, committedAt: "2026-08-07T13:00:00.000Z" } as CommittedOperation;
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: Error): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

class FakeScheduler implements RetryScheduler {
  readonly delays: number[] = [];
  private nextId = 1;
  private readonly tasks = new Map<number, () => void>();

  setTimeout(callback: () => void, delayMs: number): number {
    const id = this.nextId++;
    this.delays.push(delayMs);
    this.tasks.set(id, callback);
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.tasks.delete(handle as number);
  }

  random(): number {
    return 0;
  }

  runNext(): void {
    const entry = this.tasks.entries().next().value as [number, () => void] | undefined;
    if (!entry) throw new Error("No scheduled task");
    this.tasks.delete(entry[0]);
    entry[1]();
  }

  get size(): number {
    return this.tasks.size;
  }
}

class MemoryPendingStore implements PendingOperationStore {
  readonly rows = new Map<string, DurableCommand>();
  readonly events: string[] = [];
  putHook: ((item: DurableCommand) => Promise<void>) | null = null;
  deleteFailures = 0;

  load(requestedBoardId: string): Promise<PendingLoadResult> {
    return Promise.resolve({
      commands: [...this.rows.values()].filter((item) => item.boardId === requestedBoardId),
      corruptCount: 0,
    });
  }

  async put(item: DurableCommand): Promise<void> {
    this.events.push(`put:start:${item.opId}`);
    await this.putHook?.(item);
    this.rows.set(item.opId, item);
    this.events.push(`put:end:${item.opId}`);
  }

  delete(requestedBoardId: string, operationId: string): Promise<void> {
    this.events.push(`delete:${operationId}`);
    if (this.deleteFailures > 0) {
      this.deleteFailures -= 1;
      return Promise.reject(new Error("delete failed"));
    }
    if (this.rows.get(operationId)?.boardId === requestedBoardId) this.rows.delete(operationId);
    return Promise.resolve();
  }
}

function controllableAttempt() {
  const result = deferred<SubmissionOutcome>();
  let cancelled = false;
  const attempt: SubmissionAttempt = {
    result: result.promise,
    cancel: () => {
      cancelled = true;
    },
  };
  return {
    attempt,
    result,
    get cancelled() {
      return cancelled;
    },
  };
}

function success(item: DurableCommand, seq: number, duplicate = false): OperationAck {
  return { ok: true, duplicate, operation: committed(item, seq) };
}

function harness(initialCommands: DurableCommand[] = [], persistence = new MemoryPendingStore()) {
  const scheduler = new FakeScheduler();
  const store = createBoardStore(() => Promise.resolve("hash"));
  const snapshot: BoardSnapshot = { id: boardId, name: "Queue", lastSeq: 0, objects: [] };
  store.getState().beginSession(token, boardId);
  store.getState().initializeSession(token, snapshot, initialCommands);
  for (const item of initialCommands) persistence.rows.set(item.opId, item);
  let active = true;
  let synchronizationRequests = 0;
  const submissions: Array<{
    command: DurableCommand;
    controlled: ReturnType<typeof controllableAttempt>;
  }> = [];
  let submitOverride: ((item: DurableCommand) => SubmissionAttempt) | null = null;
  const queue = new PendingCommandQueue({
    boardId,
    initialCommands,
    persistence,
    scheduler,
    isActive: () => active && store.getState().isCurrentSession(token, boardId),
    addPersisted: (item) => store.getState().addPersistedPending(token, item),
    removePending: (operationId, error) =>
      store.getState().removePending(token, operationId, error),
    ingest: (operation) => store.getState().ingest(token, operation),
    setStatus: (status, message) => store.getState().setPendingStatus(token, status, message),
    submit: (item) => {
      if (submitOverride) return submitOverride(item);
      const controlled = controllableAttempt();
      submissions.push({ command: item, controlled });
      return controlled.attempt;
    },
    requestSynchronization: () => {
      synchronizationRequests += 1;
    },
  });
  return {
    queue,
    store,
    persistence,
    scheduler,
    submissions,
    setActive(value: boolean): void {
      active = value;
    },
    setSubmit(implementation: (item: DurableCommand) => SubmissionAttempt): void {
      submitOverride = implementation;
    },
    get synchronizationRequests(): number {
      return synchronizationRequests;
    },
  };
}

describe("pending command recovery", () => {
  it("persists before optimistic state and network submission", async () => {
    const test = harness();
    const waiting = deferred<void>();
    test.persistence.putHook = () => waiting.promise;
    test.queue.setReady(true);
    const saving = test.queue.enqueue(command(1));
    await settle();
    expect(test.store.getState()).toMatchObject({
      pending: [],
      objects: [],
      pendingStatus: "saving-locally",
    });
    expect(test.submissions).toHaveLength(0);

    waiting.resolve();
    await expect(saving).resolves.toBe(true);
    expect(test.store.getState().pending).toHaveLength(1);
    expect(test.store.getState().objects).toHaveLength(1);
    expect(test.submissions).toHaveLength(1);
  });

  it("keeps authoritative and optimistic state unchanged on put failure and accepts a later put", async () => {
    const test = harness();
    let fail = true;
    test.persistence.putHook = () =>
      fail ? Promise.reject(new Error("quota")) : Promise.resolve();
    test.queue.setReady(true);
    await expect(test.queue.enqueue(command(1))).resolves.toBe(false);
    expect(test.store.getState()).toMatchObject({
      committed: { lastSeq: 0 },
      pending: [],
      objects: [],
      pendingStatus: "persistence-error",
      error: "LOCAL_PERSISTENCE_ERROR: Pending command was not saved",
    });
    expect(test.submissions).toHaveLength(0);

    fail = false;
    await expect(test.queue.enqueue(command(2))).resolves.toBe(true);
    expect(test.submissions).toHaveLength(1);
    expect(test.submissions[0]?.command.opId).toBe(command(2).opId);
  });

  it("orders deletion after put even on the fastest completion path", async () => {
    const test = harness();
    test.queue.setReady(true);
    const item = command(1);
    await test.queue.enqueue(item);
    test.submissions[0]?.controlled.result.resolve({
      kind: "ack",
      acknowledgement: success(item, 1),
    });
    await settle();
    expect(test.persistence.events).toEqual([
      `put:start:${item.opId}`,
      `put:end:${item.opId}`,
      `delete:${item.opId}`,
    ]);
    expect(test.persistence.rows.has(item.opId)).toBe(false);
  });

  it("retries retryable acknowledgements with capped backoff and exact identity", async () => {
    const test = harness();
    const item = command(1);
    test.queue.setReady(true);
    await test.queue.enqueue(item);
    for (let attempt = 0; attempt < 7; attempt += 1) {
      test.submissions.at(-1)?.controlled.result.resolve({
        kind: "ack",
        acknowledgement: {
          ok: false,
          code: "RATE_LIMITED",
          message: "slow down",
          retryable: true,
        },
      });
      await settle();
      test.scheduler.runNext();
      await settle();
    }
    expect(test.scheduler.delays).toEqual([500, 1_000, 2_000, 4_000, 8_000, 10_000, 10_000]);
    expect(test.submissions).toHaveLength(8);
    expect(
      test.submissions.every(
        ({ command: submitted }) => JSON.stringify(submitted) === JSON.stringify(item),
      ),
    ).toBe(true);
    expect(test.submissions.every(({ command: submitted }) => submitted.opId === item.opId)).toBe(
      true,
    );
    expect(
      test.submissions.every(
        ({ command: submitted }) => submitted.clientTimestamp === item.clientTimestamp,
      ),
    ).toBe(true);
    expect(Math.max(...test.scheduler.delays)).toBe(PENDING_RETRY_CAP_MS);
  });

  it("drops a non-retryable command durably and continues without scheduling retry", async () => {
    const first = command(1);
    const second = command(2);
    const test = harness([first, second]);
    test.queue.setReady(true);
    test.submissions[0]?.controlled.result.resolve({
      kind: "ack",
      acknowledgement: {
        ok: false,
        code: "IDEMPOTENCY_CONFLICT",
        message: "operation id conflict",
        retryable: false,
      },
    });
    await settle();
    expect(test.scheduler.size).toBe(0);
    expect(test.persistence.rows.has(first.opId)).toBe(false);
    expect(test.store.getState().pending.map(({ opId }) => opId)).toEqual([second.opId]);
    expect(test.submissions[1]?.command.opId).toBe(second.opId);
    expect(test.store.getState().error).toBe("IDEMPOTENCY_CONFLICT: operation id conflict");
  });

  it("waits for synchronization after RESYNC_REQUIRED before resubmitting", async () => {
    const item = command(1);
    const test = harness([item]);
    test.queue.setReady(true);
    test.submissions[0]?.controlled.result.resolve({
      kind: "ack",
      acknowledgement: {
        ok: false,
        code: "RESYNC_REQUIRED",
        message: "catch up",
        retryable: true,
      },
    });
    await settle();
    expect(test.synchronizationRequests).toBe(1);
    expect(test.submissions).toHaveLength(1);
    expect(test.scheduler.size).toBe(0);

    test.queue.setReady(true);
    expect(test.submissions).toHaveLength(1);
    expect(test.scheduler.size).toBe(1);
    test.scheduler.runNext();
    expect(test.submissions).toHaveLength(2);
    expect(test.submissions[1]?.command).toBe(item);
  });

  it("pauses backoff while disconnected and resumes the same command on READY", async () => {
    const item = command(1);
    const test = harness([item]);
    test.queue.setReady(true);
    test.submissions[0]?.controlled.result.resolve({
      kind: "retryable-transport",
      message: "ack timeout",
    });
    await settle();
    expect(test.scheduler.size).toBe(1);
    test.queue.setReady(false);
    expect(test.scheduler.size).toBe(0);
    test.queue.setReady(true);
    expect(test.submissions).toHaveLength(2);
    expect(test.submissions[1]?.command).toBe(item);
  });

  it("fences stale retry, acknowledgement, and put completion after session replacement", async () => {
    const item = command(1);
    const test = harness([item]);
    test.queue.setReady(true);
    test.submissions[0]?.controlled.result.resolve({
      kind: "retryable-transport",
      message: "retry",
    });
    await settle();
    test.setActive(false);
    test.queue.cancel();
    expect(test.scheduler.size).toBe(0);
    test.submissions[0]?.controlled.result.resolve({
      kind: "ack",
      acknowledgement: success(item, 1),
    });
    await settle();
    expect(test.store.getState().committed.lastSeq).toBe(0);
  });

  it("does not apply a delayed put result to a replacement board session", async () => {
    const test = harness();
    const waiting = deferred<void>();
    test.persistence.putHook = () => waiting.promise;
    test.queue.setReady(true);
    const saving = test.queue.enqueue(command(1));
    await settle();
    const boardB = "10000000-0000-4000-8000-000000000002";
    const tokenB: BoardSessionToken = { generation: 2, nonce: Symbol("board-b") };
    test.setActive(false);
    test.queue.cancel();
    test.store.getState().beginSession(tokenB, boardB);
    test.store
      .getState()
      .initializeSession(tokenB, { id: boardB, name: "B", lastSeq: 0, objects: [] }, []);
    waiting.resolve();
    await expect(saving).resolves.toBe(false);
    expect(test.store.getState()).toMatchObject({ boardId: boardB, pending: [], objects: [] });
    expect(test.submissions).toHaveLength(0);
  });

  it("uses a matching live commit to cancel an outstanding acknowledgement and delete pending", async () => {
    const test = harness();
    const item = command(1);
    test.queue.setReady(true);
    await test.queue.enqueue(item);
    const attempt = test.submissions[0]?.controlled;
    expect(attempt?.cancelled).toBe(false);
    expect(test.queue.observeCommitted(committed(item, 1))).toBe("applied");
    await settle();
    expect(attempt?.cancelled).toBe(true);
    expect(test.store.getState()).toMatchObject({ pending: [], committed: { lastSeq: 1 } });
    expect(test.persistence.rows.has(item.opId)).toBe(false);
  });

  it("recovers an ack-lost commit through an exact replay with one logical server mutation", async () => {
    const item = command(1);
    const test = harness();
    let serverSequence = 0;
    let serverOperations = 0;
    let serverProjectionMutations = 0;
    let serverOutboxRows = 0;
    const received: DurableCommand[] = [];
    test.setSubmit((submitted) => {
      received.push(submitted);
      if (serverSequence === 0) {
        serverSequence = 1;
        serverOperations = 1;
        serverProjectionMutations = 1;
        serverOutboxRows = 1;
        return timedSubmission(test.scheduler, 10, () => undefined);
      }
      const replay = controllableAttempt();
      replay.result.resolve({ kind: "ack", acknowledgement: success(submitted, 1, true) });
      return replay.attempt;
    });
    test.queue.setReady(true);
    await test.queue.enqueue(item);
    test.scheduler.runNext();
    await settle();
    test.scheduler.runNext();
    await settle();

    expect(received).toHaveLength(2);
    expect(received[0]).toEqual(received[1]);
    expect({
      serverSequence,
      serverOperations,
      serverProjectionMutations,
      serverOutboxRows,
    }).toEqual({
      serverSequence: 1,
      serverOperations: 1,
      serverProjectionMutations: 1,
      serverOutboxRows: 1,
    });
    expect(test.store.getState()).toMatchObject({ pending: [], committed: { lastSeq: 1 } });
    expect(test.persistence.rows.has(item.opId)).toBe(false);
  });

  it("keeps committed state after delete failure and cleans an exact replay after reload", async () => {
    const item = command(1);
    const persistence = new MemoryPendingStore();
    persistence.deleteFailures = 1;
    const first = harness([], persistence);
    first.queue.setReady(true);
    await first.queue.enqueue(item);
    first.submissions[0]?.controlled.result.resolve({
      kind: "ack",
      acknowledgement: success(item, 1),
    });
    await settle();
    expect(first.store.getState()).toMatchObject({
      committed: { lastSeq: 1 },
      pending: [],
      pendingStatus: "cleanup-warning",
    });
    expect(persistence.rows.has(item.opId)).toBe(true);

    first.queue.cancel();
    if (item.type !== "object.create") throw new Error("Expected object creation command");
    const reloadedStore = createBoardStore(() => Promise.resolve("hash"));
    const reloadToken: BoardSessionToken = { generation: 2, nonce: Symbol("reload") };
    reloadedStore.getState().beginSession(reloadToken, boardId);
    reloadedStore
      .getState()
      .initializeSession(
        reloadToken,
        { id: boardId, name: "Reload", lastSeq: 1, objects: [item.payload] },
        [item],
      );
    const scheduler = new FakeScheduler();
    let submissions = 0;
    const replayQueue = new PendingCommandQueue({
      boardId,
      initialCommands: [item],
      persistence,
      scheduler,
      isActive: () => reloadedStore.getState().isCurrentSession(reloadToken, boardId),
      addPersisted: (pending) => reloadedStore.getState().addPersistedPending(reloadToken, pending),
      removePending: (opId, error) =>
        reloadedStore.getState().removePending(reloadToken, opId, error),
      ingest: (operation) => reloadedStore.getState().ingest(reloadToken, operation),
      setStatus: (status, message) =>
        reloadedStore.getState().setPendingStatus(reloadToken, status, message),
      submit: (submitted) => {
        submissions += 1;
        const replay = controllableAttempt();
        replay.result.resolve({ kind: "ack", acknowledgement: success(submitted, 1, true) });
        return replay.attempt;
      },
      requestSynchronization: () => undefined,
    });
    replayQueue.setReady(true);
    await settle();
    expect(submissions).toBe(1);
    expect(reloadedStore.getState()).toMatchObject({ committed: { lastSeq: 1 }, pending: [] });
    expect(reloadedStore.getState().objects).toHaveLength(1);
    expect(persistence.rows.has(item.opId)).toBe(false);
  });

  it("drains 1,001 loaded commands with one bounded in-flight submission", () => {
    const pending = Array.from({ length: 1_001 }, (_, index) => command(index + 1));
    const test = harness(pending);
    test.queue.setReady(true);
    expect(test.submissions).toHaveLength(1);
    expect(test.submissions[0]?.command.opId).toBe(pending[0]?.opId);
  });
});
