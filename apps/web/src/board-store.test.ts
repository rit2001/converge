import { beforeEach, describe, expect, it } from "vitest";
import type { CommittedOperation, DurableCommand } from "@converge/protocol";
import { emptyBoardState } from "@converge/canvas-engine";
import type { BoardSessionToken } from "./board-session";
import { createBoardStore, useBoardStore, visibleInLocalView } from "./board-store";

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
const deleted: CommittedOperation = {
  ...common,
  opId: "40000000-0000-4000-8000-000000000003",
  type: "object.delete",
  payload: {},
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

describe("local controlled selection", () => {
  it("keeps selection local and does not create a pending command", () => {
    useBoardStore.getState().ingest(token, created);

    useBoardStore.getState().select(objectId);

    expect(useBoardStore.getState()).toMatchObject({ selectedId: objectId, pending: [] });
  });

  it("clears a selected object after local optimistic deletion without adding another command", () => {
    useBoardStore.getState().ingest(token, created);
    useBoardStore.getState().select(objectId);
    const pending: DurableCommand = {
      schemaVersion: 1,
      opId: "40000000-0000-4000-8000-000000000004",
      boardId,
      clientId,
      baseSeq: 1,
      targetId: objectId,
      clientTimestamp: "2026-08-06T12:01:00.000Z",
      type: "object.delete",
      payload: {},
    };

    useBoardStore.getState().addPersistedPending(token, pending);

    expect(useBoardStore.getState().selectedId).toBeNull();
    expect(useBoardStore.getState().pending).toEqual([pending]);
  });

  it("clears stale selection and announces an authoritative remote deletion", () => {
    useBoardStore.getState().ingest(token, created);
    useBoardStore.getState().select(objectId);

    useBoardStore.getState().ingest(token, deleted);

    expect(useBoardStore.getState()).toMatchObject({
      selectedId: null,
      selectionNotice: "Selected object was removed from the board.",
      objects: [],
    });
  });

  it("clears local selection when a replacement session begins", () => {
    useBoardStore.getState().ingest(token, created);
    useBoardStore.getState().select(objectId);
    const replacement = nextToken();

    useBoardStore.getState().beginSession(replacement, "10000000-0000-4000-8000-000000000002");

    expect(useBoardStore.getState()).toMatchObject({ selectedId: null, objects: [] });
  });

  it("keeps hide and lock identities local, clears hidden selection, and never adds pending work", () => {
    useBoardStore.getState().ingest(token, created);
    useBoardStore.getState().select(objectId);

    useBoardStore.getState().setObjectHidden(objectId, true);
    useBoardStore.getState().setObjectLocked(objectId, true);

    expect(useBoardStore.getState()).toMatchObject({ selectedId: null, pending: [] });
    expect(useBoardStore.getState().hiddenObjectIds).toEqual(new Set([objectId]));
    expect(useBoardStore.getState().lockedObjectIds).toEqual(new Set([objectId]));
    expect(useBoardStore.getState().objects).toHaveLength(1);
  });

  it("keeps remote authoritative updates while an object is hidden and locked", () => {
    useBoardStore.getState().ingest(token, created);
    useBoardStore.getState().setObjectHidden(objectId, true);
    useBoardStore.getState().setObjectLocked(objectId, true);

    useBoardStore.getState().ingest(token, transformed);

    expect(useBoardStore.getState().objects[0]).toMatchObject({ id: objectId, x: 90 });
    expect(useBoardStore.getState().hiddenObjectIds).toEqual(new Set([objectId]));
    expect(useBoardStore.getState().lockedObjectIds).toEqual(new Set([objectId]));
    expect(
      visibleInLocalView(
        useBoardStore.getState().objects,
        useBoardStore.getState().hiddenObjectIds,
      ),
    ).toEqual([]);

    useBoardStore.getState().setObjectHidden(objectId, false);
    expect(
      visibleInLocalView(
        useBoardStore.getState().objects,
        useBoardStore.getState().hiddenObjectIds,
      )[0],
    ).toMatchObject({ id: objectId, x: 90 });
  });

  it("prunes local view identities after a remote deletion and replacement session", () => {
    useBoardStore.getState().ingest(token, created);
    useBoardStore.getState().setObjectHidden(objectId, true);
    useBoardStore.getState().setObjectLocked(objectId, true);

    useBoardStore.getState().ingest(token, deleted);

    expect(useBoardStore.getState().hiddenObjectIds).toEqual(new Set());
    expect(useBoardStore.getState().lockedObjectIds).toEqual(new Set());
    const replacement = nextToken();
    useBoardStore.getState().beginSession(replacement, "10000000-0000-4000-8000-000000000003");
    expect(useBoardStore.getState().hiddenObjectIds).toEqual(new Set());
    expect(useBoardStore.getState().lockedObjectIds).toEqual(new Set());
  });

  it("can clear local view controls without mutating authoritative or pending state", () => {
    useBoardStore.getState().ingest(token, created);
    useBoardStore.getState().setObjectHidden(objectId, true);
    useBoardStore.getState().setObjectLocked(objectId, true);

    useBoardStore.getState().clearLocalViewControls();

    expect(useBoardStore.getState()).toMatchObject({ pending: [], objects: [{ id: objectId }] });
    expect(useBoardStore.getState().hiddenObjectIds).toEqual(new Set());
    expect(useBoardStore.getState().lockedObjectIds).toEqual(new Set());
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
    store.getState().addPersistedPending(session, pending);

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

describe("atomic verified recovery rebasing", () => {
  it("replaces committed state once, preserves pending work, and clones recovery material", () => {
    const store = createBoardStore(() => Promise.resolve("recovered-hash"));
    const session = nextToken();
    const pendingObjectId = "30000000-0000-4000-8000-000000000099";
    const pending: DurableCommand = {
      schemaVersion: 1,
      opId: "40000000-0000-4000-8000-000000000099",
      boardId,
      clientId,
      baseSeq: 1,
      targetId: pendingObjectId,
      clientTimestamp: "2026-08-06T12:02:00.000Z",
      type: "object.create",
      payload: { ...created.payload, id: pendingObjectId },
    };
    const recovered = {
      lastSeq: 1,
      objects: {
        [objectId]: {
          value: { ...created.payload, rotation: 25 },
          createdSeq: 1,
          updatedSeq: 1,
          deletedSeq: null,
          fieldSeq: Object.fromEntries(Object.keys(created.payload).map((field) => [field, 1])),
        },
      },
      order: [objectId],
    };
    store.getState().beginSession(session, boardId);
    store
      .getState()
      .initializeSession(session, { id: boardId, name: "Before", lastSeq: 0, objects: [] }, [
        pending,
      ]);
    let prior = store.getState().committed;
    const committedSequences: number[] = [];
    const unsubscribe = store.subscribe((state) => {
      if (state.committed === prior) return;
      prior = state.committed;
      committedSequences.push(state.committed.lastSeq);
    });

    expect(store.getState().rebaseRecoveredSession(session, boardId, "Recovered", recovered)).toBe(
      true,
    );

    expect(committedSequences).toEqual([1]);
    expect(store.getState()).toMatchObject({
      name: "Recovered",
      committed: { lastSeq: 1, order: [objectId] },
      pending: [{ opId: pending.opId }],
      objects: [{ id: objectId, rotation: 25 }, { id: pendingObjectId }],
    });
    recovered.order.length = 0;
    recovered.objects[objectId].value.rotation = 90;
    expect(store.getState().committed).toMatchObject({
      order: [objectId],
      objects: { [objectId]: { value: { rotation: 25 } } },
    });
    unsubscribe();
  });

  it("rejects recovery material owned by a stale board-session token", () => {
    const store = createBoardStore(() => Promise.resolve("hash"));
    const stale = nextToken();
    const current = nextToken();
    store.getState().beginSession(stale, boardId);
    store.getState().beginSession(current, boardId);

    expect(
      store.getState().rebaseRecoveredSession(stale, boardId, "Stale", {
        lastSeq: 1,
        objects: {},
        order: [],
      }),
    ).toBe(false);
    expect(store.getState()).toMatchObject({
      sessionToken: current,
      name: "Untitled board",
      committed: { lastSeq: 0 },
    });
  });
});
