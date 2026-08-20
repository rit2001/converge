export const ROTATION_STEP_DEGREES = 15;

/**
 * Durable UI-originated rotations use the half-open [0, 360) range. Non-finite
 * input is made safe as zero, and the explicit zero branch prevents -0 from
 * entering an authoritative transform payload.
 */
export function normalizeRotation(angle: number): number {
  if (!Number.isFinite(angle)) return 0;
  const normalized = ((angle % 360) + 360) % 360;
  return normalized === 0 ? 0 : normalized;
}

/** Shift snapping rounds to the nearest 15-degree increment before normalization. */
export function snapRotation(angle: number, step = ROTATION_STEP_DEGREES): number {
  if (!Number.isFinite(angle) || !Number.isFinite(step) || step <= 0) return 0;
  return normalizeRotation(Math.round(angle / step) * step);
}

export function rotationPreviewAngle(angle: number, shiftKey: boolean): number {
  return shiftKey ? snapRotation(angle) : normalizeRotation(angle);
}

export function rotationLabel(angle: number): string {
  return `${normalizeRotation(angle)}°`;
}
