export interface WorkerTelemetryClock {
  now(): number;
}

export const systemWorkerTelemetryClock: WorkerTelemetryClock = {
  now: () => performance.now(),
};

export function telemetryNow(clock: WorkerTelemetryClock): number | undefined {
  try {
    const value = clock.now();
    return Number.isFinite(value) ? value : undefined;
  } catch {
    return undefined;
  }
}

export function telemetryDurationSeconds(
  clock: WorkerTelemetryClock,
  startedAt: number | undefined,
): number {
  const finishedAt = telemetryNow(clock);
  if (startedAt === undefined || finishedAt === undefined) return 0;
  const elapsed = finishedAt - startedAt;
  return Number.isFinite(elapsed) && elapsed >= 0 ? elapsed / 1_000 : 0;
}
