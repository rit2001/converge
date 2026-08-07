import * as React from "react";
import { createElement, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Group, Transformer } from "react-konva";
import type { BoardSnapshot, CanvasObject, CommittedOperation } from "@converge/protocol";
import type { BoardSessionToken } from "../board-session";
import { createBoardStore } from "../board-store";
import { Canvas } from "./canvas";

vi.mock("react-konva", async () => {
  const React = await import("react");
  const Container = React.forwardRef<unknown, { children?: ReactNode }>(function Container(
    { children },
    ref,
  ) {
    void ref;
    return React.createElement(React.Fragment, null, children);
  });
  return {
    Group: vi.fn(({ children }: { children?: ReactNode }) =>
      React.createElement(React.Fragment, null, children),
    ),
    Layer: Container,
    Rect: vi.fn(() => null),
    Stage: Container,
    Text: vi.fn(() => null),
    Transformer: vi.fn(() => null),
  };
});

const boardId = "10000000-0000-4000-8000-000000000001";
const clientId = "20000000-0000-4000-8000-000000000001";
const rectangle: CanvasObject = {
  id: "30000000-0000-4000-8000-000000000001",
  kind: "rectangle",
  x: 40,
  y: 50,
  width: 160,
  height: 100,
  rotation: 32,
  fill: "#818cf8",
  text: "",
};
const sticky: CanvasObject = {
  id: "30000000-0000-4000-8000-000000000002",
  kind: "sticky",
  x: 220,
  y: 120,
  width: 180,
  height: 140,
  rotation: -17,
  fill: "#fde68a",
  text: "rotated",
};

let generation = 0;

function token(): BoardSessionToken {
  generation += 1;
  return { generation, nonce: Symbol("canvas-test") };
}

function createOperation(object: CanvasObject, seq: number): CommittedOperation {
  return {
    schemaVersion: 1,
    opId: `40000000-0000-4000-8000-${String(seq).padStart(12, "0")}`,
    boardId,
    clientId,
    baseSeq: seq - 1,
    type: "object.create",
    targetId: object.id,
    payload: object,
    clientTimestamp: "2026-08-07T10:00:00.000Z",
    seq,
    committedAt: "2026-08-07T10:00:01.000Z",
  };
}

interface RenderedGroupProps {
  id: string;
  x: number;
  y: number;
  width: number;
  height: number;
  rotation?: number;
  onTransformEnd: (event: {
    target: {
      x: () => number;
      y: () => number;
      scaleX: ((value?: number) => number) & ((value: number) => void);
      scaleY: ((value?: number) => number) & ((value: number) => void);
    };
  }) => void;
}

function renderCanvas(
  objects: CanvasObject[],
  selectedId: string | null = null,
  onTransform: (id: string, patch: object) => void = vi.fn(),
): RenderedGroupProps[] {
  vi.mocked(Group).mockClear();
  vi.mocked(Transformer).mockClear();
  renderToStaticMarkup(
    createElement(Canvas, {
      objects,
      selectedId,
      tool: "select",
      onSelect: vi.fn(),
      onTransform,
    }),
  );
  return vi.mocked(Group).mock.calls.map(([props]) => props as unknown as RenderedGroupProps);
}

function snapshot(objects: CanvasObject[]): BoardSnapshot {
  return { id: boardId, name: "Rotations", lastSeq: objects.length, objects };
}

beforeEach(() => {
  vi.stubGlobal("React", React);
  vi.clearAllMocks();
});

describe("authoritative canvas rotation", () => {
  it("renders snapshot reload and reconnect catch-up rotations identically", () => {
    const snapshotStore = createBoardStore(() => Promise.resolve("hash"));
    const snapshotToken = token();
    snapshotStore.getState().beginSession(snapshotToken, boardId);
    snapshotStore.getState().initializeSession(snapshotToken, snapshot([rectangle, sticky]), []);
    const snapshotGroups = renderCanvas(snapshotStore.getState().objects);

    const reconnectStore = createBoardStore(() => Promise.resolve("hash"));
    const reconnectToken = token();
    reconnectStore.getState().beginSession(reconnectToken, boardId);
    reconnectStore.getState().initializeSession(reconnectToken, snapshot([]), []);
    reconnectStore.getState().ingest(reconnectToken, createOperation(rectangle, 1));
    reconnectStore.getState().ingest(reconnectToken, createOperation(sticky, 2));
    const reconnectGroups = renderCanvas(reconnectStore.getState().objects);

    const sceneProps = (groups: RenderedGroupProps[]) =>
      groups.map(({ id, x, y, width, height, rotation }) => ({
        id,
        x,
        y,
        width,
        height,
        rotation,
      }));
    expect(sceneProps(snapshotGroups)).toEqual([
      { id: `object-${rectangle.id}`, x: 40, y: 50, width: 160, height: 100, rotation: 32 },
      { id: `object-${sticky.id}`, x: 220, y: 120, width: 180, height: 140, rotation: -17 },
    ]);
    expect(sceneProps(reconnectGroups)).toEqual(sceneProps(snapshotGroups));
  });

  it("passes zero rotation through without changing geometry", () => {
    const groups = renderCanvas([{ ...rectangle, rotation: 0 }]);
    expect(groups[0]).toMatchObject({
      x: rectangle.x,
      y: rectangle.y,
      width: rectangle.width,
      height: rectangle.height,
      rotation: 0,
    });
  });

  it("keeps rotation when an authoritative object is selected and transformed", () => {
    const onTransform = vi.fn();
    const [group] = renderCanvas([rectangle], rectangle.id, onTransform);
    expect(group).toMatchObject({ rotation: rectangle.rotation });
    expect(vi.mocked(Transformer).mock.calls[0]?.[0]).toMatchObject({ rotateEnabled: false });

    const scaleX = vi.fn().mockReturnValueOnce(1.5);
    const scaleY = vi.fn().mockReturnValueOnce(2);
    group?.onTransformEnd({
      target: { x: () => 70, y: () => 80, scaleX, scaleY },
    });
    expect(onTransform).toHaveBeenCalledWith(rectangle.id, {
      x: 70,
      y: 80,
      width: 240,
      height: 200,
    });
    expect(onTransform.mock.calls[0]?.[1]).not.toHaveProperty("rotation");

    const store = createBoardStore(() => Promise.resolve("hash"));
    const sessionToken = token();
    store.getState().beginSession(sessionToken, boardId);
    store.getState().initializeSession(sessionToken, snapshot([]), []);
    store.getState().ingest(sessionToken, createOperation(rectangle, 1));
    store.getState().ingest(sessionToken, {
      ...createOperation(rectangle, 2),
      opId: "40000000-0000-4000-8000-000000000099",
      type: "object.transform",
      payload: { x: 70, y: 80, width: 240, height: 200 },
    });
    expect(renderCanvas(store.getState().objects, rectangle.id)[0]).toMatchObject({
      x: 70,
      y: 80,
      width: 240,
      height: 200,
      rotation: rectangle.rotation,
    });
  });
});
