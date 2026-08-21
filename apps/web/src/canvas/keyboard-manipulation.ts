import type { CanvasObject } from "@converge/protocol";

export const KEYBOARD_MOVE_STEP = 1;
export const KEYBOARD_LARGE_STEP = 10;
export const MIN_CANVAS_COORDINATE = -1_000_000;
export const MAX_CANVAS_COORDINATE = 1_000_000;
export const MIN_OBJECT_SIZE = 8;
export const MAX_OBJECT_SIZE = 100_000;

export type KeyboardObjectPatch = Readonly<{
  x?: number;
  y?: number;
  width?: number;
  height?: number;
}>;

export function viewportCenter(
  stage: { x: number; y: number; scale: number },
  viewport: { width: number; height: number },
) {
  return {
    x: (viewport.width / 2 - stage.x) / stage.scale,
    y: (viewport.height / 2 - stage.y) / stage.scale,
  };
}

export function keyboardObjectPatch(
  object: CanvasObject,
  key: "ArrowLeft" | "ArrowRight" | "ArrowUp" | "ArrowDown",
  options: { altKey: boolean; shiftKey: boolean },
): KeyboardObjectPatch | null {
  const step = options.shiftKey ? KEYBOARD_LARGE_STEP : KEYBOARD_MOVE_STEP;
  if (options.altKey) {
    const value =
      key === "ArrowLeft"
        ? object.width - step
        : key === "ArrowRight"
          ? object.width + step
          : key === "ArrowUp"
            ? object.height - step
            : object.height + step;
    if (!Number.isFinite(value) || value < MIN_OBJECT_SIZE || value > MAX_OBJECT_SIZE) return null;
    return key === "ArrowLeft" || key === "ArrowRight" ? { width: value } : { height: value };
  }
  const value =
    key === "ArrowLeft"
      ? object.x - step
      : key === "ArrowRight"
        ? object.x + step
        : key === "ArrowUp"
          ? object.y - step
          : object.y + step;
  if (!Number.isFinite(value) || value < MIN_CANVAS_COORDINATE || value > MAX_CANVAS_COORDINATE)
    return null;
  return key === "ArrowLeft" || key === "ArrowRight" ? { x: value } : { y: value };
}
