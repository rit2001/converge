import { describe, expect, it } from "vitest";
import {
  ROTATION_STEP_DEGREES,
  normalizeRotation,
  rotationLabel,
  rotationPreviewAngle,
  snapRotation,
} from "./rotation";

describe("authoritative rotation contract", () => {
  it("normalizes wrap-around values without negative zero or non-finite output", () => {
    expect(normalizeRotation(-0)).toBe(0);
    expect(Object.is(normalizeRotation(-0), -0)).toBe(false);
    expect(normalizeRotation(-15)).toBe(345);
    expect(normalizeRotation(360)).toBe(0);
    expect(normalizeRotation(735)).toBe(15);
    expect(normalizeRotation(Number.NaN)).toBe(0);
    expect(normalizeRotation(Number.POSITIVE_INFINITY)).toBe(0);
  });

  it("keeps free rotation by default and snaps Shift previews to 15-degree increments", () => {
    expect(ROTATION_STEP_DEGREES).toBe(15);
    expect(rotationPreviewAngle(17, false)).toBe(17);
    expect(rotationPreviewAngle(17, true)).toBe(15);
    expect(rotationPreviewAngle(353, true)).toBe(0);
    expect(snapRotation(-8)).toBe(345);
    expect(rotationLabel(-15)).toBe("345°");
  });
});
