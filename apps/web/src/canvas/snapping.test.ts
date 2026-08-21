import { describe, expect, it } from "vitest";
import type { CanvasObject } from "@converge/protocol";
import { CANVAS_GRID_SPACING, SNAP_THRESHOLD_SCREEN_PX, snapObjectPosition } from "./snapping";

const object = (id: string, x: number, y: number, width = 100, height = 80): CanvasObject => ({
  id,
  kind: "rectangle",
  x,
  y,
  width,
  height,
  rotation: 0,
  fill: "#818cf8",
  text: "",
});

const dragged = object("dragged", 0, 0);

function snap(
  position: { x: number; y: number },
  objects: CanvasObject[],
  options: Partial<Parameters<typeof snapObjectPosition>[0]> = {},
) {
  return snapObjectPosition({
    object: dragged,
    position,
    objects,
    viewportScale: 1,
    gridSpacing: null,
    ...options,
  });
}

describe("object drag snapping", () => {
  it("aligns leading/trailing edges and centers on both axes", () => {
    const reference = object("reference", 200, 300, 100, 80);

    expect(snap({ x: 97, y: 217 }, [dragged, reference]).position).toEqual({ x: 100, y: 220 });
    expect(snap({ x: 203, y: 303 }, [dragged, reference]).position).toEqual({ x: 200, y: 300 });
    expect(snap({ x: 297, y: 377 }, [dragged, reference]).position).toEqual({ x: 300, y: 380 });
  });

  it("keeps x and y resolution independent", () => {
    const reference = object("reference", 200, 500);

    expect(snap({ x: 97, y: 470 }, [dragged, reference]).position).toEqual({ x: 100, y: 470 });
  });

  it("uses an eight-screen-pixel tolerance at every viewport scale", () => {
    const reference = object("reference", 200, 500);
    const atOneTimes = snap({ x: 108, y: 0 }, [dragged, reference], { gridSpacing: null });
    const outsideOneTimes = snap({ x: 108.1, y: 0 }, [dragged, reference], {
      gridSpacing: null,
    });
    const atTwoTimes = snap({ x: 104, y: 0 }, [dragged, reference], {
      gridSpacing: null,
      viewportScale: 2,
    });
    const outsideTwoTimes = snap({ x: 104.1, y: 0 }, [dragged, reference], {
      gridSpacing: null,
      viewportScale: 2,
    });

    expect(SNAP_THRESHOLD_SCREEN_PX).toBe(8);
    expect(atOneTimes.position.x).toBe(100);
    expect(outsideOneTimes.position.x).toBe(108.1);
    expect(atTwoTimes.position.x).toBe(100);
    expect(outsideTwoTimes.position.x).toBe(104.1);
  });

  it("uses the documented world grid for leading-edge snapping", () => {
    expect(CANVAS_GRID_SPACING).toBe(22);
    expect(
      snap({ x: 19, y: 25 }, [dragged], { gridSpacing: CANVAS_GRID_SPACING }).position,
    ).toEqual({
      x: 22,
      y: 22,
    });
  });

  it("chooses the nearest reference with stable lexical-id tie breaking", () => {
    const first = object("alpha", 196, 500);
    const second = object("zulu", 204, 500);

    expect(snap({ x: 100, y: 0 }, [dragged, second, first]).position.x).toBe(96);
    expect(snap({ x: 100, y: 0 }, [dragged, first, second]).position.x).toBe(96);
  });

  it("excludes the dragged and hidden objects while retaining locked visible references", () => {
    const hidden = object("hidden", 200, 500);
    const locked = object("locked", 300, 500);
    const hiddenResult = snap({ x: 97, y: 0 }, [dragged, hidden], {
      hiddenObjectIds: new Set([hidden.id]),
    });
    const lockedResult = snap({ x: 197, y: 0 }, [dragged, locked], {
      hiddenObjectIds: new Set(),
    });

    expect(hiddenResult.position.x).toBe(97);
    expect(lockedResult.position.x).toBe(200);
    expect(lockedResult.guides).toEqual([{ axis: "vertical", coordinate: 300, from: 0, to: 580 }]);
  });
});
