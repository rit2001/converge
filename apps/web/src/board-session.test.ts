import { describe, expect, it } from "vitest";
import type { BoardSnapshot, CommittedOperation, DurableCommand } from "@converge/protocol";
import {
  BoardSessionController,
  type BoardSessionDependencies,
  type BoardSessionToken,
  type OwnedBoardTransport,
} from "./board-session";
import { createBoardStore } from "./board-store";

const boardA = "10000000-0000-4000-8000-000000000011";
const boardB = "10000000-0000-4000-8000-000000000012";

function snapshot(boardId: string, name: string): BoardSnapshot {
  return { id: boardId, name, lastSeq: 0, objects: [] };
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function settlePromises(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

class FakeTransport implements OwnedBoardTransport {
  connectCount = 0;
  disconnectCount = 0;
  submitted: DurableCommand[] = [];

  constructor(
    readonly boardId: string,
    readonly token: BoardSessionToken,
    private readonly store: ReturnType<typeof createBoardStore>,
  ) {}

  connect(): void {
    this.connectCount += 1;
    this.store.getState().setConnection(this.token, "joining");
  }

  disconnect(): void {
    this.disconnectCount += 1;
    this.store.getState().setConnection(this.token, "disconnected");
  }

  submit(command: DurableCommand): Promise<boolean> {
    this.submitted.push(command);
    return Promise.resolve(true);
  }

  lateReady(): void {
    this.store.getState().setConnection(this.token, "ready");
  }

  lateError(): void {
    this.store.getState().setSynchronizationError(this.token, "late transport error");
  }

  lateAcknowledgement(): void {
    this.store
      .getState()
      .removePending(this.token, "stale-operation", "INTERNAL_ERROR: late acknowledgement");
  }

  lateOperation(operation: CommittedOperation): void {
    this.store.getState().ingest(this.token, operation);
  }
}

function harness() {
  const store = createBoardStore(() => Promise.resolve("hash"));
  const snapshotQueues = new Map<string, Array<Promise<BoardSnapshot>>>();
  const pendingQueues = new Map<
    string,
    Array<Promise<{ commands: DurableCommand[]; corruptCount: number }>>
  >();
  const createQueue: Array<Promise<BoardSnapshot>> = [];
  const transports: FakeTransport[] = [];
  const locations: string[] = [];
  const take = <T>(
    queues: Map<string, Array<Promise<T>>>,
    boardId: string,
    fallback: T,
  ): Promise<T> => queues.get(boardId)?.shift() ?? Promise.resolve(fallback);
  const dependencies: BoardSessionDependencies = {
    store: store.getState(),
    createBoard: () => createQueue.shift() ?? Promise.resolve(snapshot(boardA, "created A")),
    loadSnapshot: (boardId) =>
      take(snapshotQueues, boardId, snapshot(boardId, boardId === boardA ? "A" : "B")),
    loadPending: (boardId) => take(pendingQueues, boardId, { commands: [], corruptCount: 0 }),
    updateBoardLocation: (boardId) => locations.push(boardId),
    createTransport: (boardId, token) => {
      const transport = new FakeTransport(boardId, token, store);
      transports.push(transport);
      return transport;
    },
  };
  const controller = new BoardSessionController(dependencies);
  return {
    controller,
    store,
    transports,
    locations,
    queueSnapshot(boardId: string, promise: Promise<BoardSnapshot>): void {
      snapshotQueues.set(boardId, [...(snapshotQueues.get(boardId) ?? []), promise]);
    },
    queuePending(
      boardId: string,
      promise: Promise<{ commands: DurableCommand[]; corruptCount: number }>,
    ): void {
      pendingQueues.set(boardId, [...(pendingQueues.get(boardId) ?? []), promise]);
    },
    queueCreate(promise: Promise<BoardSnapshot>): void {
      createQueue.push(promise);
    },
  };
}

describe("board session lifecycle", () => {
  it("discards a delayed pending load after session B initializes", async () => {
    const test = harness();
    const pendingA = deferred<{ commands: DurableCommand[]; corruptCount: number }>();
    test.queuePending(boardA, pendingA.promise);
    const sessionA = test.controller.start(boardA);
    await settlePromises();
    const sessionB = test.controller.start(boardB);
    await sessionB.completion;
    pendingA.resolve({ commands: [], corruptCount: 0 });
    await sessionA.completion;

    expect(test.store.getState()).toMatchObject({
      boardId: boardB,
      name: "B",
      connection: "joining",
      sessionGeneration: sessionB.token.generation,
    });
    expect(test.transports.map((transport) => transport.boardId)).toEqual([boardB]);
  });

  it("keeps valid startup state while surfacing ignored corrupt pending rows", async () => {
    const test = harness();
    test.queuePending(boardA, Promise.resolve({ commands: [], corruptCount: 2 }));
    const session = test.controller.start(boardA);
    await session.completion;

    expect(test.store.getState()).toMatchObject({
      boardId: boardA,
      connection: "joining",
      pending: [],
      pendingStatus: "persistence-error",
      error: "LOCAL_PERSISTENCE_WARNING: Ignored 2 invalid pending rows",
    });
    expect(test.transports).toHaveLength(1);
  });

  it("holds startup-time submissions until the owned transport exists", async () => {
    const test = harness();
    const pending = deferred<{ commands: DurableCommand[]; corruptCount: number }>();
    test.queuePending(boardA, pending.promise);
    const session = test.controller.start(boardA);
    const command: DurableCommand = {
      schemaVersion: 1,
      opId: "40000000-0000-4000-8000-000000000021",
      boardId: boardA,
      clientId: "20000000-0000-4000-8000-000000000021",
      baseSeq: 0,
      targetId: "30000000-0000-4000-8000-000000000021",
      clientTimestamp: "2026-08-07T12:00:00.000Z",
      type: "object.delete",
      payload: {},
    };
    const submitted = session.submit(command);
    await settlePromises();
    expect(test.transports).toHaveLength(0);

    pending.resolve({ commands: [], corruptCount: 0 });
    await expect(submitted).resolves.toBe(true);
    expect(test.transports[0]?.submitted).toEqual([command]);
  });

  it("discards a delayed snapshot after a newer board initializes", async () => {
    const test = harness();
    const snapshotA = deferred<BoardSnapshot>();
    test.queueSnapshot(boardA, snapshotA.promise);
    const sessionA = test.controller.start(boardA);
    const sessionB = test.controller.start(boardB);
    await sessionB.completion;
    snapshotA.resolve(snapshot(boardA, "stale A"));
    await sessionA.completion;

    expect(test.store.getState()).toMatchObject({
      boardId: boardB,
      name: "B",
      connection: "joining",
      sessionGeneration: sessionB.token.generation,
    });
    expect(test.transports).toHaveLength(1);
    expect(test.transports[0]?.boardId).toBe(boardB);
  });

  it("disconnects the superseded transport without disconnecting the replacement", async () => {
    const test = harness();
    const sessionA = test.controller.start(boardA);
    await sessionA.completion;
    const transportA = test.transports[0];
    const sessionB = test.controller.start(boardB);
    await sessionB.completion;
    const transportB = test.transports[1];

    expect(transportA).toMatchObject({ connectCount: 1, disconnectCount: 1 });
    expect(transportB).toMatchObject({ connectCount: 1, disconnectCount: 0 });
    expect(test.store.getState()).toMatchObject({ boardId: boardB, connection: "joining" });
  });

  it("ignores late status, error, and operation callbacks from an old transport", async () => {
    const test = harness();
    const sessionA = test.controller.start(boardA);
    await sessionA.completion;
    const transportA = test.transports[0];
    const sessionB = test.controller.start(boardB);
    await sessionB.completion;
    test.transports[1]?.lateReady();
    const before = test.store.getState();
    const staleOperation: CommittedOperation = {
      schemaVersion: 1,
      opId: "40000000-0000-4000-8000-000000000011",
      boardId: boardA,
      clientId: "20000000-0000-4000-8000-000000000011",
      baseSeq: 0,
      targetId: "30000000-0000-4000-8000-000000000011",
      clientTimestamp: "2026-08-07T12:00:00.000Z",
      type: "object.create",
      payload: {
        id: "30000000-0000-4000-8000-000000000011",
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
      committedAt: "2026-08-07T12:00:01.000Z",
    };

    transportA?.lateReady();
    transportA?.lateError();
    transportA?.lateAcknowledgement();
    transportA?.lateOperation(staleOperation);
    transportA?.disconnect();
    expect(test.store.getState()).toMatchObject({
      boardId: before.boardId,
      name: before.name,
      connection: before.connection,
      committed: { lastSeq: 0 },
      error: null,
    });
  });

  it("distinguishes the original A generation from a later A session", async () => {
    const test = harness();
    const firstSnapshotA = deferred<BoardSnapshot>();
    test.queueSnapshot(boardA, firstSnapshotA.promise);
    test.queueSnapshot(boardA, Promise.resolve(snapshot(boardA, "new A")));
    const firstA = test.controller.start(boardA);
    const sessionB = test.controller.start(boardB);
    await sessionB.completion;
    const secondA = test.controller.start(boardA);
    await secondA.completion;
    firstSnapshotA.resolve(snapshot(boardA, "stale original A"));
    await firstA.completion;

    expect(test.store.getState()).toMatchObject({
      boardId: boardA,
      name: "new A",
      connection: "joining",
      sessionGeneration: secondA.token.generation,
    });
    expect(secondA.token.generation).not.toBe(firstA.token.generation);
  });

  it("survives Strict Mode style start-cleanup-remount with one active transport", async () => {
    const test = harness();
    const staleSnapshot = deferred<BoardSnapshot>();
    test.queueSnapshot(boardA, staleSnapshot.promise);
    test.queueSnapshot(boardA, Promise.resolve(snapshot(boardA, "remounted A")));
    const firstMount = test.controller.start(boardA);
    test.controller.stop(firstMount);
    const remount = test.controller.start(boardA);
    await remount.completion;
    staleSnapshot.resolve(snapshot(boardA, "stale strict A"));
    await firstMount.completion;

    expect(test.transports).toHaveLength(1);
    expect(test.transports[0]).toMatchObject({ connectCount: 1, disconnectCount: 0 });
    expect(test.store.getState()).toMatchObject({
      boardId: boardA,
      name: "remounted A",
      connection: "joining",
    });
  });

  it("cancels safely during board creation, snapshot loading, and pending loading", async () => {
    const creationTest = harness();
    const created = deferred<BoardSnapshot>();
    creationTest.queueCreate(created.promise);
    const creating = creationTest.controller.start(null);
    creationTest.controller.stop(creating);
    created.resolve(snapshot(boardA, "created after unmount"));
    await creating.completion;
    expect(creationTest.store.getState()).toMatchObject({
      boardId: null,
      connection: "disconnected",
    });
    expect(creationTest.locations).toEqual([]);
    expect(creationTest.transports).toEqual([]);

    const snapshotTest = harness();
    const loadingSnapshot = deferred<BoardSnapshot>();
    snapshotTest.queueSnapshot(boardA, loadingSnapshot.promise);
    const snapshotSession = snapshotTest.controller.start(boardA);
    snapshotTest.controller.stop(snapshotSession);
    loadingSnapshot.resolve(snapshot(boardA, "loaded after unmount"));
    await snapshotSession.completion;
    expect(snapshotTest.transports).toEqual([]);
    expect(snapshotTest.store.getState().boardId).toBeNull();

    const pendingTest = harness();
    const loadingPending = deferred<{ commands: DurableCommand[]; corruptCount: number }>();
    pendingTest.queuePending(boardA, loadingPending.promise);
    const pendingSession = pendingTest.controller.start(boardA);
    await settlePromises();
    pendingTest.controller.stop(pendingSession);
    loadingPending.resolve({ commands: [], corruptCount: 0 });
    await pendingSession.completion;
    expect(pendingTest.transports).toEqual([]);
    expect(pendingTest.store.getState().boardId).toBeNull();

    const transportTest = harness();
    const connectedSession = transportTest.controller.start(boardA);
    await connectedSession.completion;
    const ownedTransport = transportTest.transports[0];
    transportTest.controller.stop(connectedSession);
    ownedTransport?.lateReady();
    expect(ownedTransport).toMatchObject({ connectCount: 1, disconnectCount: 1 });
    expect(transportTest.store.getState()).toMatchObject({
      boardId: null,
      connection: "disconnected",
    });
  });
});
