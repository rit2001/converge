// @vitest-environment jsdom

import * as React from "react";
import { act, createElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Group, Line, Stage, Text, Transformer } from "react-konva";
import type { BoardSnapshot, CanvasObject, CommittedOperation } from "@converge/protocol";
import type { BoardSessionToken } from "../board-session";
import { createBoardStore } from "../board-store";
import { AlignmentGuides, Canvas } from "./canvas";

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
    Line: vi.fn(() => React.createElement("i", { "data-alignment-guide": "" })),
    Rect: vi.fn(() => null),
    Stage: vi.fn(({ children }: { children?: ReactNode }) =>
      React.createElement(React.Fragment, null, children),
    ),
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
let root: ReturnType<typeof createRoot> | null = null;

const reactTestEnvironment = globalThis as typeof globalThis & {
  IS_REACT_ACT_ENVIRONMENT: boolean;
};
reactTestEnvironment.IS_REACT_ACT_ENVIRONMENT = true;

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
  draggable: boolean;
  onClick: () => void;
  onDragStart: () => void;
  onDragMove: (event: {
    evt: { altKey: boolean };
    target: {
      x: () => number;
      y: () => number;
      position: (value: { x: number; y: number }) => void;
    };
  }) => void;
  onDragEnd: (event: { target: { x: () => number; y: () => number } }) => void;
  onTransform: (event: {
    evt: { shiftKey: boolean };
    target: { rotation: ((value?: number) => number) & ((value: number) => void) };
  }) => void;
  onTransformEnd: (event: {
    target: {
      x: () => number;
      y: () => number;
      rotation: ((value?: number) => number) & ((value: number) => void);
      scaleX: ((value?: number) => number) & ((value: number) => void);
      scaleY: ((value?: number) => number) & ((value: number) => void);
    };
  }) => void;
}

function renderCanvas(
  objects: CanvasObject[],
  selectedId: string | null = null,
  onTransform: (id: string, patch: object) => void = vi.fn(),
  onSelect: (id: string | null) => void = vi.fn(),
  lockedObjectIds: ReadonlySet<string> = new Set(),
  hiddenObjectIds: ReadonlySet<string> = new Set(),
  rotationEnabled = false,
  rotationFence = "test-session",
): RenderedGroupProps[] {
  vi.mocked(Group).mockClear();
  vi.mocked(Transformer).mockClear();
  vi.mocked(Stage).mockClear();
  vi.mocked(Line).mockClear();
  vi.mocked(Text).mockClear();
  renderToStaticMarkup(
    createElement(Canvas, {
      objects,
      selectedId,
      hiddenObjectIds,
      lockedObjectIds,
      tool: "select",
      onSelect,
      onTransform,
      rotationEnabled,
      rotationFence,
    }),
  );
  return vi.mocked(Group).mock.calls.map(([props]) => props as unknown as RenderedGroupProps);
}

function snapshot(objects: CanvasObject[]): BoardSnapshot {
  return { id: boardId, name: "Rotations", lastSeq: objects.length, objects };
}

beforeEach(() => {
  vi.stubGlobal("React", React);
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe(): void {}
      disconnect(): void {}
    },
  );
  vi.stubGlobal("matchMedia", () => ({ addEventListener: vi.fn(), removeEventListener: vi.fn() }));
  vi.clearAllMocks();
});

afterEach(() => {
  void act(() => root?.unmount());
  root = null;
  document.body.replaceChildren();
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
      target: { x: () => 70, y: () => 80, rotation: vi.fn(), scaleX, scaleY },
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

  it("keeps pointer rotation local until transform end and commits its exact preview", () => {
    const onTransform = vi.fn();
    const [group] = renderCanvas(
      [rectangle],
      rectangle.id,
      onTransform,
      vi.fn(),
      new Set(),
      new Set(),
      true,
    );
    let angle = 47;
    const rotation = vi.fn((next?: number) => {
      if (next !== undefined) angle = next;
      return angle;
    }) as unknown as ((value?: number) => number) & ((value: number) => void);

    group?.onTransform({ evt: { shiftKey: false }, target: { rotation } });
    expect(onTransform).not.toHaveBeenCalled();
    group?.onTransformEnd({
      target: {
        x: () => rectangle.x,
        y: () => rectangle.y,
        rotation,
        scaleX: vi.fn().mockReturnValue(1),
        scaleY: vi.fn().mockReturnValue(1),
      },
    });

    expect(vi.mocked(Transformer).mock.calls[0]?.[0]).toMatchObject({
      rotateEnabled: true,
      rotateAnchorCursor: "crosshair",
    });
    expect(onTransform).toHaveBeenCalledTimes(1);
    expect(onTransform).toHaveBeenCalledWith(rectangle.id, { rotation: 47 });
  });

  it("snaps Shift pointer rotation to 15 degrees without a movement command", () => {
    const onTransform = vi.fn();
    const [group] = renderCanvas(
      [rectangle],
      rectangle.id,
      onTransform,
      vi.fn(),
      new Set(),
      new Set(),
      true,
    );
    let angle = 53;
    const rotation = vi.fn((next?: number) => {
      if (next !== undefined) angle = next;
      return angle;
    }) as unknown as ((value?: number) => number) & ((value: number) => void);

    group?.onTransform({ evt: { shiftKey: true }, target: { rotation } });
    expect(angle).toBe(60);
    expect(onTransform).not.toHaveBeenCalled();
    group?.onTransformEnd({
      target: {
        x: () => rectangle.x,
        y: () => rectangle.y,
        rotation,
        scaleX: vi.fn().mockReturnValue(1),
        scaleY: vi.fn().mockReturnValue(1),
      },
    });
    expect(onTransform).toHaveBeenCalledWith(rectangle.id, { rotation: 60 });
  });

  it("renders exact remote authoritative rotation updates", () => {
    const store = createBoardStore(() => Promise.resolve("hash"));
    const sessionToken = token();
    store.getState().beginSession(sessionToken, boardId);
    store.getState().initializeSession(sessionToken, snapshot([]), []);
    store.getState().ingest(sessionToken, createOperation(rectangle, 1));
    store.getState().ingest(sessionToken, {
      ...createOperation(rectangle, 2),
      opId: "40000000-0000-4000-8000-000000000097",
      type: "object.transform",
      payload: { rotation: 345 },
    });

    expect(renderCanvas(store.getState().objects, rectangle.id)[0]).toMatchObject({
      rotation: 345,
    });
  });
});

describe("canvas selection boundary", () => {
  it("clears the shared local selection when the empty stage is clicked", () => {
    const onSelect = vi.fn();
    renderCanvas([rectangle], rectangle.id, vi.fn(), onSelect);
    const props = vi.mocked(Stage).mock.calls[0]?.[0] as {
      onPointerDown: (event: { target: { getStage: () => unknown } }) => void;
    };
    const onPointerDown = (event: { target: { getStage: () => unknown } }): void =>
      props.onPointerDown(event);
    const stage = {} as { getStage: () => unknown };
    stage.getStage = () => stage;

    onPointerDown({ target: stage });

    expect(onSelect).toHaveBeenCalledWith(null);
  });

  it("blocks locked-object selection, dragging, and transforms", () => {
    const onSelect = vi.fn();
    const onTransform = vi.fn();
    const [group] = renderCanvas(
      [rectangle],
      rectangle.id,
      onTransform,
      onSelect,
      new Set([rectangle.id]),
    );

    expect(group).toMatchObject({ draggable: false });
    expect(vi.mocked(Transformer).mock.calls[0]?.[0]).toMatchObject({ rotateEnabled: false });
    group?.onClick();
    group?.onDragEnd({ target: { x: () => 70, y: () => 80 } });
    const scaleX = vi.fn().mockReturnValue(1.5);
    const scaleY = vi.fn().mockReturnValue(2);
    group?.onTransformEnd({
      target: { x: () => 70, y: () => 80, rotation: vi.fn(), scaleX, scaleY },
    });

    expect(onSelect).not.toHaveBeenCalled();
    expect(onTransform).not.toHaveBeenCalled();
  });

  it("omits rotation capability for hidden and locked selections", () => {
    renderCanvas(
      [rectangle],
      rectangle.id,
      vi.fn(),
      vi.fn(),
      new Set(),
      new Set([rectangle.id]),
      true,
    );
    expect(vi.mocked(Group)).not.toHaveBeenCalled();
    expect(vi.mocked(Transformer).mock.calls[0]?.[0]).toMatchObject({ rotateEnabled: false });

    renderCanvas(
      [rectangle],
      rectangle.id,
      vi.fn(),
      vi.fn(),
      new Set([rectangle.id]),
      new Set(),
      true,
    );
    expect(vi.mocked(Transformer).mock.calls[0]?.[0]).toMatchObject({ rotateEnabled: false });
  });

  it("keeps snapped drag previews local and commits their exact final position once", () => {
    const reference = { ...sticky, id: "30000000-0000-4000-8000-000000000099", x: 300, y: 210 };
    const onTransform = vi.fn();
    const [group] = renderCanvas(
      [rectangle, reference],
      rectangle.id,
      onTransform,
      vi.fn(),
      new Set([reference.id]),
    );
    let position = { x: 137, y: 107 };
    const target = {
      x: () => position.x,
      y: () => position.y,
      position: (next: { x: number; y: number }) => {
        position = next;
      },
    };

    group?.onDragStart();
    group?.onDragMove({ evt: { altKey: false }, target });
    group?.onDragMove({ evt: { altKey: false }, target });

    expect(position).toEqual({ x: 140, y: 110 });
    expect(onTransform).not.toHaveBeenCalled();
    group?.onDragEnd({ target });
    expect(onTransform).toHaveBeenCalledTimes(1);
    expect(onTransform).toHaveBeenCalledWith(rectangle.id, { x: 140, y: 110 });
  });

  it("bypasses snapping while Option/Alt is held", () => {
    const reference = { ...sticky, id: "30000000-0000-4000-8000-000000000099", x: 300, y: 210 };
    const onTransform = vi.fn();
    const [group] = renderCanvas([rectangle, reference], rectangle.id, onTransform);
    const position = { x: 137, y: 107 };
    const target = {
      x: () => position.x,
      y: () => position.y,
      position: vi.fn(),
    };

    group?.onDragStart();
    group?.onDragMove({ evt: { altKey: true }, target });
    group?.onDragEnd({ target });

    expect(target.position).not.toHaveBeenCalled();
    expect(onTransform).toHaveBeenCalledWith(rectangle.id, position);
  });

  it("renders guide lines as non-interactive canvas controls without identifiers", () => {
    renderToStaticMarkup(
      createElement(AlignmentGuides, {
        guides: [{ axis: "vertical", coordinate: 300, from: 10, to: 200 }],
      }),
    );

    expect(vi.mocked(Line).mock.calls[0]?.[0]).toMatchObject({
      points: [300, 10, 300, 200],
      listening: false,
      perfectDrawEnabled: false,
    });
    expect(vi.mocked(Line).mock.calls[0]?.[0]).not.toHaveProperty("id");
  });

  it("clears local guides at every drag termination and replacement boundary", () => {
    const reference = { ...sticky, id: "30000000-0000-4000-8000-000000000099", x: 300, y: 210 };
    const host = document.createElement("div");
    document.body.append(host);
    root = createRoot(host);
    const render = (objects: CanvasObject[], hiddenObjectIds = new Set<string>()): void => {
      void act(() => {
        root?.render(
          <Canvas
            objects={objects}
            selectedId={rectangle.id}
            hiddenObjectIds={hiddenObjectIds}
            tool="select"
            onSelect={vi.fn()}
            onTransform={vi.fn()}
          />,
        );
      });
    };
    const currentGroup = (): RenderedGroupProps =>
      vi
        .mocked(Group)
        .mock.calls.map(([props]) => props as unknown as RenderedGroupProps)
        .reverse()
        .find((props) => props.id === `object-${rectangle.id}`)!;
    const drag = (): void => {
      let position = { x: 137, y: 107 };
      const target = {
        x: () => position.x,
        y: () => position.y,
        position: (next: { x: number; y: number }) => {
          position = next;
        },
      };
      void act(() => {
        currentGroup().onDragStart();
        currentGroup().onDragMove({ evt: { altKey: false }, target });
      });
    };

    render([rectangle, reference]);
    drag();
    expect(host.querySelectorAll("[data-alignment-guide]")).toHaveLength(2);

    void act(() => currentGroup().onDragEnd({ target: { x: () => 140, y: () => 110 } }));
    expect(host.querySelectorAll("[data-alignment-guide]")).toHaveLength(0);

    drag();
    const stage = vi.mocked(Stage).mock.calls.at(-1)?.[0] as {
      onPointerCancel: () => void;
    };
    void act(() => stage.onPointerCancel());
    expect(host.querySelectorAll("[data-alignment-guide]")).toHaveLength(0);

    drag();
    render([reference]);
    expect(host.querySelectorAll("[data-alignment-guide]")).toHaveLength(0);

    render([rectangle, reference]);
    drag();
    render([rectangle, reference], new Set([reference.id]));
    expect(host.querySelectorAll("[data-alignment-guide]")).toHaveLength(0);

    render([rectangle, reference]);
    drag();
    render([]);
    expect(host.querySelectorAll("[data-alignment-guide]")).toHaveLength(0);

    render([rectangle, reference]);
    drag();
    void act(() => root?.unmount());
    root = null;
    expect(host.querySelectorAll("[data-alignment-guide]")).toHaveLength(0);
  });

  it("fences a stale rotation callback after session replacement or terminal clearing", () => {
    const onTransform = vi.fn();
    const host = document.createElement("div");
    document.body.append(host);
    root = createRoot(host);
    const render = (objects: CanvasObject[], fence: string): void => {
      void act(() => {
        root?.render(
          <Canvas
            objects={objects}
            selectedId={rectangle.id}
            tool="select"
            rotationEnabled
            rotationFence={fence}
            onSelect={vi.fn()}
            onTransform={onTransform}
          />,
        );
      });
    };
    render([rectangle], "session-1");
    const staleGroup = vi
      .mocked(Group)
      .mock.calls.map(([props]) => props as unknown as RenderedGroupProps)
      .at(-1)!;
    let angle = 47;
    const rotation = vi.fn((next?: number) => {
      if (next !== undefined) angle = next;
      return angle;
    }) as unknown as ((value?: number) => number) & ((value: number) => void);
    void act(() => staleGroup.onTransform({ evt: { shiftKey: false }, target: { rotation } }));
    expect(onTransform).not.toHaveBeenCalled();
    expect(
      vi
        .mocked(Text)
        .mock.calls.some(([props]) =>
          Boolean(
            (props as { text?: string; listening?: boolean }).text === "47°" &&
              (props as { listening?: boolean }).listening === false,
          ),
        ),
    ).toBe(true);

    render([], "terminal-session-2");
    void act(() =>
      staleGroup.onTransformEnd({
        target: {
          x: () => rectangle.x,
          y: () => rectangle.y,
          rotation,
          scaleX: vi.fn().mockReturnValue(1),
          scaleY: vi.fn().mockReturnValue(1),
        },
      }),
    );

    expect(onTransform).not.toHaveBeenCalled();
  });
});
