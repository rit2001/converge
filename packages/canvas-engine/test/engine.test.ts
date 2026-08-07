import { describe, expect, it } from "vitest";
import type { DurableCommand } from "@converge/protocol";
import { canonicalBoard, emptyBoardState, hashBoardState, reduceCommand } from "../src/index.js";

const ids = {
  board: "10000000-0000-4000-8000-000000000001",
  client: "20000000-0000-4000-8000-000000000001",
  object: "30000000-0000-4000-8000-000000000001",
};
const base = {
  schemaVersion: 1 as const,
  boardId: ids.board,
  clientId: ids.client,
  baseSeq: 0,
  targetId: ids.object,
  clientTimestamp: "2026-08-06T12:00:00.000Z",
};
const create: DurableCommand = {
  ...base,
  opId: "40000000-0000-4000-8000-000000000001",
  type: "object.create",
  payload: {
    id: ids.object,
    kind: "sticky",
    x: 1,
    y: 2,
    width: 180,
    height: 140,
    rotation: 0,
    fill: "#fde68a",
    text: "hello",
  },
};

const createRectangle: DurableCommand = {
  ...create,
  payload: {
    ...create.payload,
    kind: "rectangle",
    text: "",
  },
};

describe("canvas engine", () => {
  it("merges patches to independent fields", () => {
    const created = reduceCommand(emptyBoardState(), create, 1);
    expect(created.ok).toBe(true);
    if (!created.ok) return;
    const fill = reduceCommand(
      created.state,
      {
        ...base,
        opId: "40000000-0000-4000-8000-000000000002",
        type: "object.update",
        payload: { fill: "#ffffff" },
      },
      2,
    );
    expect(fill.ok).toBe(true);
    if (!fill.ok) return;
    const text = reduceCommand(
      fill.state,
      {
        ...base,
        opId: "40000000-0000-4000-8000-000000000003",
        type: "object.update",
        payload: { text: "world" },
      },
      3,
    );
    expect(text.ok && text.state.objects[ids.object]?.value).toMatchObject({
      fill: "#ffffff",
      text: "world",
    });
  });

  it("accepts valid rectangle updates", () => {
    const created = reduceCommand(emptyBoardState(), createRectangle, 1);
    if (!created.ok) throw new Error("fixture failed");
    const updated = reduceCommand(
      created.state,
      {
        ...base,
        opId: "40000000-0000-4000-8000-000000000006",
        type: "object.update",
        payload: { fill: "#ffffff" },
      },
      2,
    );
    expect(updated.ok && updated.state.objects[ids.object]?.value).toMatchObject({
      kind: "rectangle",
      fill: "#ffffff",
      text: "",
    });
  });

  it("rejects rectangle text poisoning without returning or applying a partial state", () => {
    const created = reduceCommand(emptyBoardState(), createRectangle, 1);
    if (!created.ok) throw new Error("fixture failed");
    const before = canonicalBoard(created.state);
    const poisoned = reduceCommand(
      created.state,
      {
        ...base,
        opId: "40000000-0000-4000-8000-000000000007",
        type: "object.update",
        payload: { text: "poisoned" },
      },
      2,
    );
    expect(poisoned).toMatchObject({ ok: false, code: "INVALID_COMMAND" });
    expect("state" in poisoned).toBe(false);
    expect(canonicalBoard(created.state)).toBe(before);
    expect(created.state.objects[ids.object]?.value.text).toBe("");
  });

  it("makes delete win over stale updates", () => {
    const created = reduceCommand(emptyBoardState(), create, 1);
    if (!created.ok) throw new Error("fixture failed");
    const deleted = reduceCommand(
      created.state,
      { ...base, opId: "40000000-0000-4000-8000-000000000004", type: "object.delete", payload: {} },
      2,
    );
    if (!deleted.ok) throw new Error("fixture failed");
    const stale = reduceCommand(
      deleted.state,
      {
        ...base,
        opId: "40000000-0000-4000-8000-000000000005",
        type: "object.update",
        payload: { text: "lost" },
      },
      3,
    );
    expect(stale).toMatchObject({ ok: false, code: "TARGET_DELETED" });
  });

  it("canonicalizes object and key order and produces a stable SHA-256", async () => {
    const created = reduceCommand(emptyBoardState(), create, 1);
    if (!created.ok) throw new Error("fixture failed");
    expect(canonicalBoard(created.state)).toBe(
      '{"objects":[{"fill":"#fde68a","height":140,"id":"30000000-0000-4000-8000-000000000001","kind":"sticky","rotation":0,"text":"hello","width":180,"x":1,"y":2}]}',
    );
    expect(await hashBoardState(created.state)).toMatch(/^[a-f0-9]{64}$/);
    expect(await hashBoardState(created.state)).toBe(await hashBoardState(created.state));
  });

  it("keeps canonical hashing unchanged for equivalent valid objects", async () => {
    const original = reduceCommand(emptyBoardState(), create, 1);
    const reordered = reduceCommand(
      emptyBoardState(),
      {
        ...create,
        payload: {
          text: "hello",
          fill: "#fde68a",
          rotation: 0,
          height: 140,
          width: 180,
          y: 2,
          x: 1,
          kind: "sticky",
          id: ids.object,
        },
      },
      1,
    );
    if (!original.ok || !reordered.ok) throw new Error("fixture failed");
    expect(canonicalBoard(reordered.state)).toBe(canonicalBoard(original.state));
    expect(await hashBoardState(reordered.state)).toBe(await hashBoardState(original.state));
  });
});
