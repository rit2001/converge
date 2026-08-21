// @vitest-environment jsdom

import * as React from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { CanvasObject } from "@converge/protocol";
import { LayersPanel } from "./layers-panel";

const reactTestEnvironment = globalThis as typeof globalThis & {
  IS_REACT_ACT_ENVIRONMENT: boolean;
};
reactTestEnvironment.IS_REACT_ACT_ENVIRONMENT = true;

const rectangle: CanvasObject = {
  id: "30000000-0000-4000-8000-000000000001",
  kind: "rectangle",
  x: 0,
  y: 0,
  width: 120,
  height: 80,
  rotation: 0,
  fill: "#818cf8",
  text: "",
};

let root: ReturnType<typeof createRoot> | null = null;

afterEach(() => {
  void act(() => root?.unmount());
  root = null;
  document.body.replaceChildren();
});

describe("LayersPanel local controls", () => {
  it("activates a toggle without selecting its row", () => {
    const onSelect = vi.fn();
    const onToggleHidden = vi.fn();
    const host = document.createElement("div");
    document.body.append(host);
    root = createRoot(host);

    void act(() => {
      root?.render(
        <LayersPanel
          objects={[rectangle]}
          selectedId={null}
          hiddenObjectIds={new Set()}
          lockedObjectIds={new Set()}
          onSelect={onSelect}
          onToggleHidden={onToggleHidden}
          onToggleLocked={vi.fn()}
          onClose={vi.fn()}
        />,
      );
    });
    const hide = host.querySelector('button[aria-label="Hide Rectangle"]');
    expect(hide).not.toBeNull();

    void act(() => hide?.dispatchEvent(new MouseEvent("click", { bubbles: true })));

    expect(onToggleHidden).toHaveBeenCalledWith(rectangle.id);
    expect(onSelect).not.toHaveBeenCalled();
  });
});
