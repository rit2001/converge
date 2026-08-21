import { expect, it } from "vitest";
import { keyboardObjectPatch, viewportCenter } from "./keyboard-manipulation";

const object = {
  id: "00000000-0000-4000-8000-000000000001",
  kind: "rectangle" as const,
  x: 120,
  y: 80,
  width: 180,
  height: 110,
  rotation: 15,
  fill: "#818cf8",
  text: "" as const,
};

it("calculates a zoom-independent visible world center", () => {
  expect(viewportCenter({ x: -100, y: -50, scale: 2 }, { width: 900, height: 600 })).toEqual({
    x: 275,
    y: 175,
  });
});

it("returns exact one and ten unit move and resize patches without changing rotation", () => {
  expect(keyboardObjectPatch(object, "ArrowRight", { altKey: false, shiftKey: false })).toEqual({
    x: 121,
  });
  expect(keyboardObjectPatch(object, "ArrowUp", { altKey: false, shiftKey: true })).toEqual({
    y: 70,
  });
  expect(keyboardObjectPatch(object, "ArrowRight", { altKey: true, shiftKey: false })).toEqual({
    width: 181,
  });
  expect(keyboardObjectPatch(object, "ArrowDown", { altKey: true, shiftKey: true })).toEqual({
    height: 120,
  });
});

it("fails closed at coordinate and size bounds", () => {
  expect(
    keyboardObjectPatch({ ...object, x: -1_000_000 }, "ArrowLeft", {
      altKey: false,
      shiftKey: false,
    }),
  ).toBeNull();
  expect(
    keyboardObjectPatch({ ...object, width: 8 }, "ArrowLeft", { altKey: true, shiftKey: false }),
  ).toBeNull();
});
