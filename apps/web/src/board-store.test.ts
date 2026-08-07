import { beforeEach, describe, expect, it } from "vitest";
import type { CommittedOperation } from "@converge/protocol";
import { emptyBoardState } from "@converge/canvas-engine";
import { useBoardStore } from "./board-store";

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

beforeEach(() => {
  useBoardStore.setState({
    committed: emptyBoardState(),
    pending: [],
    buffered: {},
    appliedOpIds: {},
    appliedSeqOpIds: {},
    objects: [],
    hash: "calculating…",
    connection: "disconnected",
    error: null,
  });
});

describe("committed sequence reconciliation", () => {
  it("buffers an out-of-order event and drains it after the gap arrives", () => {
    useBoardStore.getState().ingest(transformed);
    expect(useBoardStore.getState().committed.lastSeq).toBe(0);
    expect(useBoardStore.getState().buffered[2]).toBeDefined();
    useBoardStore.getState().ingest(created);
    const state = useBoardStore.getState();
    expect(state.committed.lastSeq).toBe(2);
    expect(state.objects[0]).toMatchObject({ id: objectId, x: 90 });
    expect(state.buffered).toEqual({});
  });

  it("ignores an at-least-once duplicate", () => {
    useBoardStore.getState().ingest(created);
    useBoardStore.getState().ingest(created);
    expect(useBoardStore.getState().committed.lastSeq).toBe(1);
    expect(useBoardStore.getState().objects).toHaveLength(1);
  });

  it("rejects conflicting operation identities for one sequence", () => {
    useBoardStore.getState().ingest(transformed);
    const conflict = useBoardStore.getState().ingest({
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
    useBoardStore.getState().ingest(created);
    const conflict = useBoardStore.getState().ingest({
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
