import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CommittedOperation, DurableCommand } from "@converge/protocol";
import { emptyBoardState } from "@converge/canvas-engine";
import type { BoardSessionToken } from "./board-session";
import { createBoardStore, useBoardStore } from "./board-store";

vi.mock("./pending-db", () => ({
  savePending: vi.fn(() => Promise.resolve()),
  removePending: vi.fn(() => Promise.resolve()),
}));

const boardId = "10000000-0000-4000-8000-000000000001";
const clientId = "20000000-0000-4000-8000-000000000001";
const objectId = "30000000-0000-4000-8000-000000000001";
const common = {
  schemaVersion: 1 as const,
  boardId,
  clientId,
  baseSeq: 0,
  targetId: objectId,
  clientTimestamp: "2026-08-06T12:00:00.000Z",
  committedAt: "2026-08-06T12:00:01.000Z",
};
const created: CommittedOperation = {
  ...common,
  opId: "40000000-0000-4000-8000-000000000001",
  type: "object.create",
  payload: {
    id: objectId,
    kind: "rectangle",
    x: 10,
    y: 20,
    width: 100,
    height: 80,
    rotation: 0,
    fill: "#818cf8",
    text: "",
  },
  seq: 1,
};
const transformed: CommittedOperation = {
  ...common,
  opId: "40000000-0000-4000-8000-000000000002",
  type: "object.transform",
  payload: { x: 90 },
  seq: 2,
};

let generation = 0;
let token: BoardSessionToken;

function nextToken(): BoardSessionToken {
  generation += 1;
  return { generation, nonce: Symbol("test-session") };
}

async function settlePromises(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

beforeEach(() => {
  token = nextToken();
  useBoardStore.getState().beginSession(token, boardId);
});

describe("committed sequence reconciliation", () => {
  it("buffers an out-of-order event and drains it after the gap arrives", () => {
    useBoardStore.getState().ingest(token, transformed);
    expect(useBoardStore.getState().committed.lastSeq).toBe(0);
    expect(useBoardStore.getState().buffered[2]).toBeDefined();
    useBoardStore.getState().ingest(token, created);
    const state = useBoardStore.getState();
    expect(state.committed.lastSeq).toBe(2);
    expect(state.objects[0]).toMatchObject({ id: objectId, x: 90 });
    expect(state.buffered).toEqual({});
  });

  it("ignores an at-least-once duplicate", () => {
    useBoardStore.getState().ingest(token, created);
    useBoardStore.getState().ingest(token, created);
    expect(useBoardStore.getState().committed.lastSeq).toBe(1);
    expect(useBoardStore.getState().objects).toHaveLength(1);
  });

  it("rejects conflicting operation identities for one sequence", () => {
    useBoardStore.getState().ingest(token, transformed);
    const conflict = useBoardStore.getState().ingest(token, {
      ...transformed,
      opId: "40000000-0000-4000-8000-000000000099",
    });
    expect(conflict).toBe("conflict");
    expect(useBoardStore.getState()).toMatchObject({
      connection: "error",
      committed: { lastSeq: 0 },
    });
  });

  it("rejects a conflicting identity for an already applied sequence", () => {
    useBoardStore.getState().ingest(token, created);
    const conflict = useBoardStore.getState().ingest(token, {
      ...created,
      opId: "40000000-0000-4000-8000-000000000098",
    });
    expect(conflict).toBe("conflict");
    expect(useBoardStore.getState()).toMatchObject({
      connection: "error",
      committed: { lastSeq: 1 },
      objects: [{ id: objectId }],
    });
  });
});

describe("sequence-specific authoritative hashing", () => {
  it("discards an older sequence hash that resolves after the current sequence", async () => {
    const requests: Array<{ seq: number; resolve: (hash: string) => void }> = [];
    const store = createBoardStore(
      (state) => new Promise<string>((resolve) => requests.push({ seq: state.lastSeq, resolve })),
    );
    const session = nextToken();
    store.getState().beginSession(session, boardId);
    store
      .getState()
      .initializeSession(session, { id: boardId, name: "A", lastSeq: 0, objects: [] }, []);
    store.getState().ingest(session, created);
    expect(requests.map((request) => request.seq)).toEqual([0, 1]);

    requests[1]?.resolve("sequence-one");
    await settlePromises();
    expect(store.getState().authoritativeHash).toEqual({
      status: "ready",
      boardId,
      sessionGeneration: session.generation,
      seq: 1,
      value: "sequence-one",
    });
    requests[0]?.resolve("stale-sequence-zero");
    await settlePromises();
    expect(store.getState().authoritativeHash).toMatchObject({
      status: "ready",
      seq: 1,
      value: "sequence-one",
    });
  });

  it("discards hashes from an older board session, including A to B to A", async () => {
    const requests: Array<{
      boardId: string;
      generation: number;
      resolve: (hash: string) => void;
    }> = [];
    let activeBoardId = "";
    let activeGeneration = 0;
    const store = createBoardStore(
      () =>
        new Promise<string>((resolve) =>
          requests.push({
            boardId: activeBoardId,
            generation: activeGeneration,
            resolve,
          }),
        ),
    );
    const boardB = "10000000-0000-4000-8000-000000000002";
    const firstA = nextToken();
    activeBoardId = boardId;
    activeGeneration = firstA.generation;
    store.getState().beginSession(firstA, boardId);
    store
      .getState()
      .initializeSession(firstA, { id: boardId, name: "first A", lastSeq: 0, objects: [] }, []);
    const sessionB = nextToken();
    activeBoardId = boardB;
    activeGeneration = sessionB.generation;
    store.getState().beginSession(sessionB, boardB);
    store
      .getState()
      .initializeSession(sessionB, { id: boardB, name: "B", lastSeq: 0, objects: [] }, []);
    const secondA = nextToken();
    activeBoardId = boardId;
    activeGeneration = secondA.generation;
    store.getState().beginSession(secondA, boardId);
    store
      .getState()
      .initializeSession(secondA, { id: boardId, name: "second A", lastSeq: 0, objects: [] }, []);

    requests[2]?.resolve("second-a");
    await settlePromises();
    requests[1]?.resolve("stale-b");
    requests[0]?.resolve("stale-first-a");
    await settlePromises();
    expect(store.getState().authoritativeHash).toEqual({
      status: "ready",
      boardId,
      sessionGeneration: secondA.generation,
      seq: 0,
      value: "second-a",
    });
  });

  it("keeps board B diagnostics when board A hashing resolves late", async () => {
    const requests: Array<{ resolve: (hash: string) => void }> = [];
    const store = createBoardStore(
      () => new Promise<string>((resolve) => requests.push({ resolve })),
    );
    const boardB = "10000000-0000-4000-8000-000000000002";
    const sessionA = nextToken();
    store.getState().beginSession(sessionA, boardId);
    store
      .getState()
      .initializeSession(sessionA, { id: boardId, name: "A", lastSeq: 0, objects: [] }, []);
    const sessionB = nextToken();
    store.getState().beginSession(sessionB, boardB);
    store
      .getState()
      .initializeSession(sessionB, { id: boardB, name: "B", lastSeq: 0, objects: [] }, []);

    requests[1]?.resolve("board-b");
    await settlePromises();
    requests[0]?.resolve("stale-board-a");
    await settlePromises();
    expect(store.getState().authoritativeHash).toEqual({
      status: "ready",
      boardId: boardB,
      sessionGeneration: sessionB.generation,
      seq: 0,
      value: "board-b",
    });
  });

  it("does not relabel the committed hash when optimistic pending state changes", async () => {
    const store = createBoardStore(() => Promise.resolve("empty-authoritative-state"));
    const session = nextToken();
    store.getState().beginSession(session, boardId);
    store
      .getState()
      .initializeSession(session, { id: boardId, name: "A", lastSeq: 0, objects: [] }, []);
    await settlePromises();
    const pending: DurableCommand = {
      schemaVersion: created.schemaVersion,
      opId: created.opId,
      boardId: created.boardId,
      clientId: created.clientId,
      baseSeq: created.baseSeq,
      targetId: created.targetId,
      clientTimestamp: created.clientTimestamp,
      type: created.type,
      payload: created.payload,
    };
    store.getState().enqueue(pending);

    expect(store.getState().objects).toHaveLength(1);
    expect(store.getState().committed).toEqual(emptyBoardState());
    expect(store.getState().authoritativeHash).toEqual({
      status: "ready",
      boardId,
      sessionGeneration: session.generation,
      seq: 0,
      value: "empty-authoritative-state",
    });
  });
});
