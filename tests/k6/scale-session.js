export const SCALE_TOTAL_DURATION_MS = 225_000;
export const SCALE_RAMP_DOWN_START_MS = 210_000;
export const SCALE_RAMP_DOWN_DURATION_MS = 15_000;
export const SCALE_COMMAND_INTERVAL_MS = 250;
export const SCALE_PARK_PADDING_MS = 60_000;

export function scaleSessionCloseAtMs({
  vu,
  maximumVus,
  rampDownStartMs,
  rampDownDurationMs,
  closeMarginMs = 1_000,
}) {
  if (!Number.isInteger(vu) || vu < 1 || !Number.isInteger(maximumVus) || vu > maximumVus)
    throw new Error("Invalid persistent scale VU boundary");
  if (
    !Number.isInteger(rampDownStartMs) ||
    rampDownStartMs < 1 ||
    !Number.isInteger(rampDownDurationMs) ||
    rampDownDurationMs < 1 ||
    !Number.isInteger(closeMarginMs) ||
    closeMarginMs < 1 ||
    closeMarginMs >= rampDownDurationMs
  )
    throw new Error("Invalid persistent scale close boundary");
  return rampDownStartMs - closeMarginMs;
}

export function shouldCloseScaleSession(elapsedMs, boundary) {
  return Number.isFinite(elapsedMs) && elapsedMs >= boundary;
}

export function scaleParkDurationSeconds(elapsedMs, totalDurationMs) {
  if (!Number.isFinite(elapsedMs) || elapsedMs < 0 || !Number.isInteger(totalDurationMs))
    throw new Error("Invalid persistent scale park boundary");
  return Math.max(1, (totalDurationMs - elapsedMs + SCALE_PARK_PADDING_MS) / 1_000);
}

export function shouldClassifySocketFailure(sessionComplete) {
  return sessionComplete !== true;
}

export function createScaleSessionOwnership() {
  let started = false;
  let snapshots = 0;
  let secondInvocations = 0;
  return Object.freeze({
    start() {
      if (started) {
        secondInvocations += 1;
        return false;
      }
      started = true;
      return true;
    },
    snapshotRequested() {
      if (!started || snapshots !== 0)
        throw new Error("Persistent scale snapshot ownership violated");
      snapshots += 1;
    },
    snapshot() {
      return Object.freeze({ started, snapshots, secondInvocations });
    },
  });
}
