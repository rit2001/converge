import {
  SNAPSHOT_CANDIDATE_BATCH_SIZE_DEFAULT,
  SNAPSHOT_CANDIDATE_SCAN_LIMIT_DEFAULT,
  SNAPSHOT_CHANGED_AGE_MS_DEFAULT,
  SNAPSHOT_MAX_PAYLOAD_BYTES_DEFAULT,
  SNAPSHOT_OPERATION_BYTES_THRESHOLD_DEFAULT,
  SNAPSHOT_OPERATION_THRESHOLD_DEFAULT,
  type SnapshotCandidate,
  type SnapshotCandidateDiscoveryOptions,
  type SnapshotCandidateDiscoveryResult,
  type SnapshotCaptureOptions,
  type SnapshotCaptureOutcome,
} from "@converge/database";
import {
  noOpTelemetryRecorder,
  safeTelemetryRecorder,
  type TelemetryRecorder,
} from "@converge/observability";
import {
  systemWorkerTelemetryClock,
  telemetryDurationSeconds,
  telemetryNow,
  type WorkerTelemetryClock,
} from "./telemetry.js";

const TIMER_MAXIMUM_MS = 2_147_483_647;

export const SNAPSHOT_POLL_INTERVAL_MS_DEFAULT = 30_000 as const;
export const SNAPSHOT_POLL_JITTER_PERCENT_DEFAULT = 20 as const;
export const SNAPSHOT_MAX_CONCURRENCY_DEFAULT = 2 as const;
export const SNAPSHOT_RETRY_BASE_MS_DEFAULT = 5_000 as const;
export const SNAPSHOT_RETRY_CAP_MS_DEFAULT = 300_000 as const;
export const SNAPSHOT_BUSY_RETRY_MS_DEFAULT = 5_000 as const;
export const SNAPSHOT_FAILURE_FINGERPRINT_LIMIT_DEFAULT = 1_000 as const;
export const SNAPSHOT_SHUTDOWN_GRACE_MS_DEFAULT = 10_000 as const;

export interface SnapshotCoordinatorRepository {
  discover(options: SnapshotCandidateDiscoveryOptions): Promise<SnapshotCandidateDiscoveryResult>;
  capture(boardId: string, options: SnapshotCaptureOptions): Promise<SnapshotCaptureOutcome>;
}

export interface SnapshotCoordinatorClock {
  now(): number;
}

export interface SnapshotCoordinatorRandomSource {
  next(): number;
}

export interface SnapshotCoordinatorScheduler {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

export const systemSnapshotCoordinatorClock: SnapshotCoordinatorClock = { now: Date.now };
export const systemSnapshotCoordinatorRandomSource: SnapshotCoordinatorRandomSource = {
  next: Math.random,
};
export const systemSnapshotCoordinatorScheduler: SnapshotCoordinatorScheduler = {
  setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
};

export interface SnapshotCoordinatorConfiguration {
  pollIntervalMs: number;
  pollJitterPercent: number;
  candidateScanLimit: number;
  candidateLimit: number;
  maximumConcurrency: number;
  operationThreshold: number;
  changedAgeMs: number;
  operationBytesThreshold: number;
  maximumPayloadBytes: number;
  retryBaseMs: number;
  retryCapMs: number;
  busyRetryMs: number;
  failureFingerprintLimit: number;
  shutdownGraceMs: number;
}

export const defaultSnapshotCoordinatorConfiguration: Readonly<SnapshotCoordinatorConfiguration> = {
  pollIntervalMs: SNAPSHOT_POLL_INTERVAL_MS_DEFAULT,
  pollJitterPercent: SNAPSHOT_POLL_JITTER_PERCENT_DEFAULT,
  candidateScanLimit: SNAPSHOT_CANDIDATE_SCAN_LIMIT_DEFAULT,
  candidateLimit: SNAPSHOT_CANDIDATE_BATCH_SIZE_DEFAULT,
  maximumConcurrency: SNAPSHOT_MAX_CONCURRENCY_DEFAULT,
  operationThreshold: SNAPSHOT_OPERATION_THRESHOLD_DEFAULT,
  changedAgeMs: SNAPSHOT_CHANGED_AGE_MS_DEFAULT,
  operationBytesThreshold: SNAPSHOT_OPERATION_BYTES_THRESHOLD_DEFAULT,
  maximumPayloadBytes: SNAPSHOT_MAX_PAYLOAD_BYTES_DEFAULT,
  retryBaseMs: SNAPSHOT_RETRY_BASE_MS_DEFAULT,
  retryCapMs: SNAPSHOT_RETRY_CAP_MS_DEFAULT,
  busyRetryMs: SNAPSHOT_BUSY_RETRY_MS_DEFAULT,
  failureFingerprintLimit: SNAPSHOT_FAILURE_FINGERPRINT_LIMIT_DEFAULT,
  shutdownGraceMs: SNAPSHOT_SHUTDOWN_GRACE_MS_DEFAULT,
};

export interface SnapshotCapturedNotification {
  boardId: string;
  snapshotId: string;
  canvasHead: number;
  deliveryHead: number;
}

export interface SnapshotDeterministicFailureNotification {
  boardId: string;
  canvasHead: number;
  deliveryHead: number;
  code: Extract<SnapshotCaptureOutcome, { status: "deterministic_failure" }>["code"];
}

export interface SnapshotCoordinatorHooks {
  captured?(notification: SnapshotCapturedNotification): Promise<void> | void;
  deterministicFailure?(
    notification: SnapshotDeterministicFailureNotification,
  ): Promise<void> | void;
}

export interface SnapshotCoordinatorDependencies {
  repository: SnapshotCoordinatorRepository;
  configuration?: SnapshotCoordinatorConfiguration;
  clock?: SnapshotCoordinatorClock;
  random?: SnapshotCoordinatorRandomSource;
  scheduler?: SnapshotCoordinatorScheduler;
  hooks?: SnapshotCoordinatorHooks;
  telemetry?: TelemetryRecorder;
  telemetryClock?: WorkerTelemetryClock;
}

export class SnapshotCoordinatorConfigurationError extends Error {
  constructor() {
    super("Snapshot coordinator configuration is invalid");
  }
}

interface RetryState {
  fingerprint: string;
  attempt: number;
  notBefore: number;
  ordinal: number;
}

interface CooldownState {
  fingerprint: string;
  notBefore: number;
  ordinal: number;
}

interface SuppressionState {
  fingerprint: string;
  ordinal: number;
}

function positiveSafeInteger(value: number, timerSafe = false): boolean {
  return Number.isSafeInteger(value) && value > 0 && (!timerSafe || value <= TIMER_MAXIMUM_MS);
}

function validateConfiguration(configuration: SnapshotCoordinatorConfiguration): void {
  if (
    !positiveSafeInteger(configuration.pollIntervalMs, true) ||
    !positiveSafeInteger(configuration.pollJitterPercent) ||
    configuration.pollJitterPercent > SNAPSHOT_POLL_JITTER_PERCENT_DEFAULT ||
    !positiveSafeInteger(configuration.candidateScanLimit) ||
    configuration.candidateScanLimit > SNAPSHOT_CANDIDATE_SCAN_LIMIT_DEFAULT ||
    !positiveSafeInteger(configuration.candidateLimit) ||
    configuration.candidateLimit > SNAPSHOT_CANDIDATE_BATCH_SIZE_DEFAULT ||
    configuration.candidateLimit > configuration.candidateScanLimit ||
    !positiveSafeInteger(configuration.maximumConcurrency) ||
    configuration.maximumConcurrency > SNAPSHOT_MAX_CONCURRENCY_DEFAULT ||
    configuration.maximumConcurrency > configuration.candidateLimit ||
    !positiveSafeInteger(configuration.operationThreshold) ||
    !positiveSafeInteger(configuration.changedAgeMs, true) ||
    !positiveSafeInteger(configuration.operationBytesThreshold) ||
    !positiveSafeInteger(configuration.maximumPayloadBytes) ||
    configuration.maximumPayloadBytes > SNAPSHOT_MAX_PAYLOAD_BYTES_DEFAULT ||
    !positiveSafeInteger(configuration.retryBaseMs, true) ||
    !positiveSafeInteger(configuration.retryCapMs, true) ||
    configuration.retryCapMs > SNAPSHOT_RETRY_CAP_MS_DEFAULT ||
    configuration.retryBaseMs > configuration.retryCapMs ||
    !positiveSafeInteger(configuration.busyRetryMs, true) ||
    !positiveSafeInteger(configuration.failureFingerprintLimit) ||
    configuration.failureFingerprintLimit > SNAPSHOT_FAILURE_FINGERPRINT_LIMIT_DEFAULT ||
    !positiveSafeInteger(configuration.shutdownGraceMs, true) ||
    configuration.pollIntervalMs * (1 + configuration.pollJitterPercent / 100) > TIMER_MAXIMUM_MS ||
    configuration.busyRetryMs * (1 + configuration.pollJitterPercent / 100) > TIMER_MAXIMUM_MS
  )
    throw new SnapshotCoordinatorConfigurationError();
}

function fingerprint(candidate: SnapshotCandidate): string {
  return `${candidate.boardId}:${String(candidate.canvasHead)}:${String(candidate.deliveryHead)}`;
}

export class SnapshotCoordinator {
  private readonly repository: SnapshotCoordinatorRepository;
  private readonly configuration: SnapshotCoordinatorConfiguration;
  private readonly clock: SnapshotCoordinatorClock;
  private readonly random: SnapshotCoordinatorRandomSource;
  private readonly scheduler: SnapshotCoordinatorScheduler;
  private readonly hooks: SnapshotCoordinatorHooks;
  private readonly telemetry: TelemetryRecorder;
  private readonly telemetryClock: WorkerTelemetryClock;
  private readonly retries = new Map<string, RetryState>();
  private readonly cooldowns = new Map<string, CooldownState>();
  private readonly suppressions = new Map<string, SuppressionState>();
  private readonly activeCaptures = new Set<Promise<void>>();
  private cursor: string | null = null;
  private pollTimer: unknown;
  private graceTimer: unknown;
  private cyclePromise: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private finishStop: (() => void) | undefined;
  private lifecycle: "idle" | "running" | "stopping" | "stopped" = "idle";
  private generation = 0;
  private ordinal = 0;
  private resultsFenced = false;
  private activeTelemetryWork = 0;
  private telemetryFenced = false;

  constructor(dependencies: SnapshotCoordinatorDependencies) {
    this.repository = dependencies.repository;
    this.configuration =
      dependencies.configuration ?? ({ ...defaultSnapshotCoordinatorConfiguration } as const);
    this.clock = dependencies.clock ?? systemSnapshotCoordinatorClock;
    this.random = dependencies.random ?? systemSnapshotCoordinatorRandomSource;
    this.scheduler = dependencies.scheduler ?? systemSnapshotCoordinatorScheduler;
    this.hooks = dependencies.hooks ?? {};
    this.telemetry = safeTelemetryRecorder(dependencies.telemetry ?? noOpTelemetryRecorder);
    this.telemetryClock = dependencies.telemetryClock ?? systemWorkerTelemetryClock;
    validateConfiguration(this.configuration);
    this.telemetry.setGauge("converge_snapshot_active_work", {}, 0);
  }

  get diagnostics(): Readonly<{
    lifecycle: "idle" | "running" | "stopping" | "stopped";
    cursor: string | null;
    cycleInFlight: boolean;
    activeCaptures: number;
    retryBoards: number;
    cooldownBoards: number;
    suppressedFingerprints: number;
    pollScheduled: boolean;
    graceScheduled: boolean;
  }> {
    return {
      lifecycle: this.lifecycle,
      cursor: this.cursor,
      cycleInFlight: this.cyclePromise !== undefined,
      activeCaptures: this.activeCaptures.size,
      retryBoards: this.retries.size,
      cooldownBoards: this.cooldowns.size,
      suppressedFingerprints: this.suppressions.size,
      pollScheduled: this.pollTimer !== undefined,
      graceScheduled: this.graceTimer !== undefined,
    };
  }

  start(): Promise<void> {
    if (this.lifecycle !== "idle") return Promise.resolve();
    this.lifecycle = "running";
    this.resultsFenced = false;
    this.generation += 1;
    void this.runCycle();
    return Promise.resolve();
  }

  stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    if (this.lifecycle === "idle") {
      this.clearRetainedState();
      this.resultsFenced = true;
      this.fenceTelemetry();
      this.lifecycle = "stopped";
      this.stopPromise = Promise.resolve();
      return this.stopPromise;
    }

    this.lifecycle = "stopping";
    this.clearPollTimer();
    this.stopPromise = new Promise<void>((resolve) => {
      this.finishStop = resolve;
    });
    const cycle = this.cyclePromise;
    if (!cycle) {
      this.completeStop();
      return this.stopPromise;
    }

    this.graceTimer = this.scheduler.setTimeout(() => {
      this.graceTimer = undefined;
      this.resultsFenced = true;
      this.completeStop();
    }, this.configuration.shutdownGraceMs);
    void cycle.then(
      () => this.completeStop(),
      () => this.completeStop(),
    );
    return this.stopPromise;
  }

  runCycle(): Promise<void> {
    if (this.lifecycle !== "running") return Promise.resolve();
    if (this.cyclePromise) return this.cyclePromise;
    const generation = this.generation;
    const cycle = this.executeCycle(generation).catch(() => undefined);
    this.cyclePromise = cycle;
    void cycle.finally(() => {
      if (this.cyclePromise === cycle) this.cyclePromise = undefined;
      if (this.lifecycle === "running" && this.generation === generation) this.scheduleNextPoll();
    });
    return cycle;
  }

  private async executeCycle(generation: number): Promise<void> {
    const currentTime = this.currentTime();
    const result = await this.repository.discover({
      cursor: this.cursor,
      scanLimit: this.configuration.candidateScanLimit,
      candidateLimit: this.configuration.candidateLimit,
      operationThreshold: this.configuration.operationThreshold,
      changedAgeMs: this.configuration.changedAgeMs,
      operationBytesThreshold: this.configuration.operationBytesThreshold,
      currentTime: new Date(currentTime),
    });
    if (!this.canStartWork(generation)) return;
    this.cursor = result.nextCursor;
    const candidates = result.candidates.slice(0, this.configuration.candidateLimit);
    let nextCandidate = 0;
    const captureWorker = async (): Promise<void> => {
      while (this.canStartWork(generation)) {
        const candidate = candidates[nextCandidate];
        nextCandidate += 1;
        if (!candidate) return;
        if (this.shouldSkip(candidate, currentTime)) continue;
        if (!this.canStartWork(generation)) return;
        const task = this.captureCandidate(candidate, generation);
        this.activeCaptures.add(task);
        try {
          await task;
        } finally {
          this.activeCaptures.delete(task);
        }
      }
    };
    await Promise.all(
      Array.from(
        { length: Math.min(this.configuration.maximumConcurrency, candidates.length) },
        captureWorker,
      ),
    );
  }

  private async captureCandidate(candidate: SnapshotCandidate, generation: number): Promise<void> {
    let startedAt: number | undefined;
    let telemetryActive = false;
    try {
      let outcome: SnapshotCaptureOutcome;
      try {
        const options = {
          operationThreshold: this.configuration.operationThreshold,
          changedAgeMs: this.configuration.changedAgeMs,
          operationBytesThreshold: this.configuration.operationBytesThreshold,
          maximumPayloadBytes: this.configuration.maximumPayloadBytes,
          currentTime: new Date(this.currentTime()),
        };
        startedAt = telemetryNow(this.telemetryClock);
        this.beginTelemetryWork();
        telemetryActive = true;
        outcome = await this.repository.capture(candidate.boardId, options);
      } catch {
        if (this.canAcceptResult(generation)) {
          this.recordTransientFailure(candidate, this.currentTime());
          if (telemetryActive) this.recordTelemetryOutcome("transient_failure", startedAt);
        }
        return;
      }
      if (!this.canAcceptResult(generation)) return;

      switch (outcome.status) {
        case "captured":
          this.clearBoardState(candidate.boardId);
          this.recordTelemetryOutcome("captured", startedAt);
          await this.invokeHook(() =>
            this.hooks.captured?.({
              boardId: candidate.boardId,
              snapshotId: outcome.snapshotId,
              canvasHead: outcome.canvasHead,
              deliveryHead: outcome.deliveryHead,
            }),
          );
          return;
        case "busy":
          this.recordBusy(candidate, this.currentTime());
          this.recordTelemetryOutcome("busy", startedAt);
          return;
        case "no_longer_eligible":
          this.clearBoardState(candidate.boardId);
          this.recordTelemetryOutcome("no_progress", startedAt);
          return;
        case "deterministic_failure":
          this.retries.delete(candidate.boardId);
          this.cooldowns.delete(candidate.boardId);
          this.recordSuppression(candidate);
          this.recordTelemetryOutcome("deterministic_failure", startedAt);
          await this.invokeHook(() =>
            this.hooks.deterministicFailure?.({
              boardId: candidate.boardId,
              canvasHead: candidate.canvasHead,
              deliveryHead: candidate.deliveryHead,
              code: outcome.code,
            }),
          );
      }
    } finally {
      if (telemetryActive) this.finishTelemetryWork();
    }
  }

  private beginTelemetryWork(): void {
    this.activeTelemetryWork += 1;
    if (!this.telemetryFenced)
      this.telemetry.setGauge("converge_snapshot_active_work", {}, this.activeTelemetryWork);
  }

  private finishTelemetryWork(): void {
    this.activeTelemetryWork = Math.max(0, this.activeTelemetryWork - 1);
    if (!this.telemetryFenced)
      this.telemetry.setGauge("converge_snapshot_active_work", {}, this.activeTelemetryWork);
  }

  private recordTelemetryOutcome(
    outcome: "captured" | "busy" | "no_progress" | "deterministic_failure" | "transient_failure",
    startedAt: number | undefined,
  ): void {
    if (this.telemetryFenced) return;
    this.telemetry.increment("converge_snapshot_runs_total", { outcome });
    this.telemetry.observe(
      "converge_snapshot_duration_seconds",
      {},
      telemetryDurationSeconds(this.telemetryClock, startedAt),
    );
    this.telemetry.emit({
      schemaVersion: 1,
      eventName: "snapshot.capture.result",
      severity: outcome === "captured" || outcome === "no_progress" ? "info" : "warn",
      component: "snapshot",
      timestamp: new Date().toISOString(),
      code: outcome.toUpperCase(),
    });
  }

  private shouldSkip(candidate: SnapshotCandidate, currentTime: number): boolean {
    const value = fingerprint(candidate);
    const suppression = this.suppressions.get(candidate.boardId);
    if (suppression?.fingerprint === value) {
      this.touch(this.suppressions, candidate.boardId, suppression);
      return true;
    }
    const cooldown = this.cooldowns.get(candidate.boardId);
    if (cooldown?.fingerprint === value && currentTime < cooldown.notBefore) {
      this.touch(this.cooldowns, candidate.boardId, cooldown);
      return true;
    }
    const retry = this.retries.get(candidate.boardId);
    if (retry?.fingerprint === value && currentTime < retry.notBefore) {
      this.touch(this.retries, candidate.boardId, retry);
      return true;
    }
    return false;
  }

  private recordBusy(candidate: SnapshotCandidate, currentTime: number): void {
    const delay = this.symmetricJitter(this.configuration.busyRetryMs);
    this.touch(this.cooldowns, candidate.boardId, {
      fingerprint: fingerprint(candidate),
      notBefore: currentTime + delay,
      ordinal: 0,
    });
  }

  private recordTransientFailure(candidate: SnapshotCandidate, currentTime: number): void {
    const value = fingerprint(candidate);
    const previous = this.retries.get(candidate.boardId);
    const maximumExponent = Math.ceil(
      Math.log2(this.configuration.retryCapMs / this.configuration.retryBaseMs),
    );
    const attempt = Math.min(
      previous?.fingerprint === value ? previous.attempt + 1 : 1,
      maximumExponent + 1,
    );
    const exponent = attempt - 1;
    const limit = Math.min(
      this.configuration.retryCapMs,
      this.configuration.retryBaseMs * 2 ** exponent,
    );
    const delay = Math.floor(this.randomUnit() * (limit + 1));
    this.cooldowns.delete(candidate.boardId);
    this.touch(this.retries, candidate.boardId, {
      fingerprint: value,
      attempt,
      notBefore: currentTime + delay,
      ordinal: 0,
    });
  }

  private recordSuppression(candidate: SnapshotCandidate): void {
    this.touch(this.suppressions, candidate.boardId, {
      fingerprint: fingerprint(candidate),
      ordinal: 0,
    });
  }

  private touch<T extends { ordinal: number }>(map: Map<string, T>, key: string, value: T): void {
    value.ordinal = ++this.ordinal;
    map.delete(key);
    map.set(key, value);
    if (map.size <= this.configuration.failureFingerprintLimit) return;
    let oldestKey: string | undefined;
    let oldestOrdinal = Number.POSITIVE_INFINITY;
    for (const [candidateKey, candidateValue] of map)
      if (candidateValue.ordinal < oldestOrdinal) {
        oldestKey = candidateKey;
        oldestOrdinal = candidateValue.ordinal;
      }
    if (oldestKey !== undefined) map.delete(oldestKey);
  }

  private clearBoardState(boardId: string): void {
    this.retries.delete(boardId);
    this.cooldowns.delete(boardId);
    this.suppressions.delete(boardId);
  }

  private scheduleNextPoll(): void {
    if (this.pollTimer !== undefined || this.lifecycle !== "running") return;
    this.pollTimer = this.scheduler.setTimeout(() => {
      this.pollTimer = undefined;
      void this.runCycle();
    }, this.symmetricJitter(this.configuration.pollIntervalMs));
  }

  private symmetricJitter(baseMs: number): number {
    const offset = (this.randomUnit() * 2 - 1) * (this.configuration.pollJitterPercent / 100);
    return Math.max(1, Math.round(baseMs * (1 + offset)));
  }

  private randomUnit(): number {
    const value = this.random.next();
    if (!Number.isFinite(value) || value < 0 || value >= 1)
      throw new SnapshotCoordinatorConfigurationError();
    return value;
  }

  private currentTime(): number {
    const value = this.clock.now();
    if (!Number.isSafeInteger(value) || value < 0)
      throw new SnapshotCoordinatorConfigurationError();
    return value;
  }

  private canStartWork(generation: number): boolean {
    return this.lifecycle === "running" && this.generation === generation;
  }

  private canAcceptResult(generation: number): boolean {
    return this.generation === generation && !this.resultsFenced;
  }

  private async invokeHook(callback: () => Promise<void> | void | undefined): Promise<void> {
    try {
      await callback();
    } catch {
      // Hooks are bounded observations and cannot stop unrelated snapshot candidates.
    }
  }

  private clearPollTimer(): void {
    if (this.pollTimer === undefined) return;
    this.scheduler.clearTimeout(this.pollTimer);
    this.pollTimer = undefined;
  }

  private completeStop(): void {
    if (this.lifecycle !== "stopping") return;
    if (this.graceTimer !== undefined) {
      this.scheduler.clearTimeout(this.graceTimer);
      this.graceTimer = undefined;
    }
    this.resultsFenced = true;
    this.fenceTelemetry();
    this.lifecycle = "stopped";
    this.generation += 1;
    this.cyclePromise = undefined;
    this.clearRetainedState();
    const finish = this.finishStop;
    this.finishStop = undefined;
    finish?.();
  }

  private fenceTelemetry(): void {
    if (this.telemetryFenced) return;
    this.activeTelemetryWork = 0;
    this.telemetry.setGauge("converge_snapshot_active_work", {}, 0);
    this.telemetryFenced = true;
  }

  private clearRetainedState(): void {
    this.clearPollTimer();
    this.cursor = null;
    this.retries.clear();
    this.cooldowns.clear();
    this.suppressions.clear();
    this.ordinal = 0;
  }
}
