// @vitest-environment jsdom

import * as React from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { CanvasObject } from "@converge/protocol";
import { RotationControls } from "./rotation-controls";

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
  rotation: 345,
  fill: "#818cf8",
  text: "",
};

let root: ReturnType<typeof createRoot> | null = null;

afterEach(() => {
  void act(() => root?.unmount());
  root = null;
  document.body.replaceChildren();
});

describe("shared rotation controls", () => {
  it("uses generated accessible labels and emits one desired angle per activation", () => {
    const onRotate = vi.fn();
    const onFocusWithinChange = vi.fn();
    const host = document.createElement("div");
    document.body.append(host);
    root = createRoot(host);
    void act(() =>
      root?.render(
        <RotationControls
          object={rectangle}
          onRotate={onRotate}
          onFocusWithinChange={onFocusWithinChange}
        />,
      ),
    );

    const counterclockwise = host.querySelector(
      'button[aria-label="Rotate Rectangle 15° counterclockwise"]',
    );
    const clockwise = host.querySelector('button[aria-label="Rotate Rectangle 15° clockwise"]');
    const reset = host.querySelector('button[aria-label="Reset Rectangle rotation to 0°"]');
    expect(host.textContent).toContain("Shared rotation");
    expect(counterclockwise?.getAttribute("aria-describedby")).toBeTruthy();
    expect(clockwise?.getAttribute("aria-describedby")).toBeTruthy();
    expect(reset?.getAttribute("aria-describedby")).toBeTruthy();

    void act(() => (counterclockwise as HTMLButtonElement | null)?.focus());
    void act(() => (counterclockwise as HTMLButtonElement | null)?.blur());
    expect(onFocusWithinChange).toHaveBeenCalledWith(true);
    expect(onFocusWithinChange).toHaveBeenCalledWith(false);

    void act(() => counterclockwise?.dispatchEvent(new MouseEvent("click", { bubbles: true })));
    void act(() => clockwise?.dispatchEvent(new MouseEvent("click", { bubbles: true })));
    void act(() => reset?.dispatchEvent(new MouseEvent("click", { bubbles: true })));

    expect(onRotate.mock.calls).toEqual([[330], [360], [0]]);
  });
});
