import {
  COMPACTION_CANDIDATE_RESULT_LIMIT_MAXIMUM,
  COMPACTION_CANDIDATE_SCAN_LIMIT_MAXIMUM,
  type BoardCompactionCandidate,
  type BoardCompactionCandidateDiscoveryOptions,
  type BoardCompactionCandidateDiscoveryResult,
  type BoardCompactionResult,
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

export const COMPACTION_POLL_INTERVAL_MS_DEFAULT = 300_000 as const;
export const COMPACTION_POLL_JITTER_PERCENT_DEFAULT = 20 as const;
export const COMPACTION_CANDIDATE_SCAN_LIMIT_DEFAULT = 100 as const;
export const COMPACTION_CANDIDATE_BATCH_SIZE_DEFAULT = 16 as const;
export const COMPACTION_MAX_CONCURRENCY_DEFAULT = 2 as const;
export const COMPACTION_RETRY_BASE_MS_DEFAULT = 5_000 as const;
export const COMPACTION_RETRY_CAP_MS_DEFAULT = 300_000 as const;
export const COMPACTION_RETAINED_STATE_LIMIT_DEFAULT = 1_000 as const;
export const COMPACTION_SHUTDOWN_GRACE_MS_DEFAULT = 10_000 as const;

export interface CompactionCandidateDiscoveryRepository {
  discover(
    options: BoardCompactionCandidateDiscoveryOptions,
  ): Promise<BoardCompactionCandidateDiscoveryResult>;
}

export interface CompactionExecutionRepository {
  compact(boardId: string): Promise<BoardCompactionResult>;
}

export interface CompactionCoordinatorClock {
  now(): number;
}

export interface CompactionCoordinatorRandomSource {
  next(): number;
}

export interface CompactionCoordinatorScheduler {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

export const systemCompactionCoordinatorClock: CompactionCoordinatorClock = { now: Date.now };
export const systemCompactionCoordinatorRandomSource: CompactionCoordinatorRandomSource = {
  next: Math.random,
};
export const systemCompactionCoordinatorScheduler: CompactionCoordinatorScheduler = {
  setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
};

export interface CompactionCoordinatorConfiguration {
  pollIntervalMs: number;
  pollJitterPercent: number;
  candidateScanLimit: number;
  candidateResultLimit: number;
  maximumConcurrency: number;
  retryBaseMs: number;
  retryCapMs: number;
  retainedStateLimit: number;
  shutdownGraceMs: number;
}

export const defaultCompactionCoordinatorConfiguration: Readonly<CompactionCoordinatorConfiguration> =
  {
    pollIntervalMs: COMPACTION_POLL_INTERVAL_MS_DEFAULT,
    pollJitterPercent: COMPACTION_POLL_JITTER_PERCENT_DEFAULT,
    candidateScanLimit: COMPACTION_CANDIDATE_SCAN_LIMIT_DEFAULT,
    candidateResultLimit: COMPACTION_CANDIDATE_BATCH_SIZE_DEFAULT,
    maximumConcurrency: COMPACTION_MAX_CONCURRENCY_DEFAULT,
    retryBaseMs: COMPACTION_RETRY_BASE_MS_DEFAULT,
    retryCapMs: COMPACTION_RETRY_CAP_MS_DEFAULT,
    retainedStateLimit: COMPACTION_RETAINED_STATE_LIMIT_DEFAULT,
    shutdownGraceMs: COMPACTION_SHUTDOWN_GRACE_MS_DEFAULT,
  };

export interface CompactionSuccessNotification {
  boardId: string;
  snapshotId: string;
  snapshotCanvasSeq: number;
  snapshotDeliverySeq: number;
  previousOperationFloor: number;
  newOperationFloor: number;
  previousDeliveryFloor: number;
  newDeliveryFloor: number;
  deletedOperationCount: number;
  deletedOutboxCount: number;
}

export interface CompactionBlockedNotification {
  boardId: string;
  snapshotId: string;
  snapshotCanvasSeq: number;
  snapshotDeliverySeq: number;
  operationRecoveryFloor: number;
  deliveryRecoveryFloor: number;
  code: Extract<BoardCompactionResult, { outcome: "blocked" }>["code"];
}

export interface CompactionCoordinatorHooks {
  compacted?(notification: CompactionSuccessNotification): Promise<void> | void;
  blocked?(notification: CompactionBlockedNotification): Promise<void> | void;
}

export interface CompactionCoordinatorDependencies {
  candidates: CompactionCandidateDiscoveryRepository;
  compaction: CompactionExecutionRepository;
  configuration?: CompactionCoordinatorConfiguration;
  clock?: CompactionCoordinatorClock;
  random?: CompactionCoordinatorRandomSource;
  scheduler?: CompactionCoordinatorScheduler;
  hooks?: CompactionCoordinatorHooks;
  telemetry?: TelemetryRecorder;
  telemetryClock?: WorkerTelemetryClock;
}

export class CompactionCoordinatorConfigurationError extends Error {
  constructor() {
    super("Compaction coordinator configuration is invalid");
  }
}

interface RetryState {
  fingerprint: string;
  attempt: number;
  notBefore: number;
  ordinal: number;
}

interface BlockedState {
  fingerprint: string;
  ordinal: number;
}

function positiveSafeInteger(value: number, timerSafe = false): boolean {
  return Number.isSafeInteger(value) && value > 0 && (!timerSafe || value <= TIMER_MAXIMUM_MS);
}

function validateConfiguration(configuration: CompactionCoordinatorConfiguration): void {
  if (
    !positiveSafeInteger(configuration.pollIntervalMs, true) ||
    !Number.isSafeInteger(configuration.pollJitterPercent) ||
    configuration.pollJitterPercent < 0 ||
    configuration.pollJitterPercent > 100 ||
    !positiveSafeInteger(configuration.candidateScanLimit) ||
    configuration.candidateScanLimit > COMPACTION_CANDIDATE_SCAN_LIMIT_MAXIMUM ||
    !positiveSafeInteger(configuration.candidateResultLimit) ||
    configuration.candidateResultLimit > COMPACTION_CANDIDATE_RESULT_LIMIT_MAXIMUM ||
    configuration.candidateResultLimit > configuration.candidateScanLimit ||
    !positiveSafeInteger(configuration.maximumConcurrency) ||
    configuration.maximumConcurrency > COMPACTION_MAX_CONCURRENCY_DEFAULT ||
    configuration.maximumConcurrency > configuration.candidateResultLimit ||
    !positiveSafeInteger(configuration.retryBaseMs, true) ||
    !positiveSafeInteger(configuration.retryCapMs, true) ||
    configuration.retryCapMs > COMPACTION_RETRY_CAP_MS_DEFAULT ||
    configuration.retryBaseMs > configuration.retryCapMs ||
    !positiveSafeInteger(configuration.retainedStateLimit) ||
    configuration.retainedStateLimit > COMPACTION_RETAINED_STATE_LIMIT_DEFAULT ||
    !positiveSafeInteger(configuration.shutdownGraceMs, true) ||
    configuration.pollIntervalMs * (1 + configuration.pollJitterPercent / 100) > TIMER_MAXIMUM_MS
  )
    throw new CompactionCoordinatorConfigurationError();
}

function candidateFingerprint(candidate: BoardCompactionCandidate): string {
  return [
    candidate.boardId,
    candidate.snapshotId,
    candidate.snapshotCanvasSeq,
    candidate.snapshotDeliverySeq,
    candidate.operationRecoveryFloor,
    candidate.deliveryRecoveryFloor,
  ].join(":");
}

export class CompactionCoordinator {
  private readonly candidates: CompactionCandidateDiscoveryRepository;
  private readonly compaction: CompactionExecutionRepository;
  private readonly configuration: CompactionCoordinatorConfiguration;
  private readonly clock: CompactionCoordinatorClock;
  private readonly random: CompactionCoordinatorRandomSource;
  private readonly scheduler: CompactionCoordinatorScheduler;
  private readonly hooks: CompactionCoordinatorHooks;
  private readonly telemetry: TelemetryRecorder;
  private readonly telemetryClock: WorkerTelemetryClock;
  private readonly retries = new Map<string, RetryState>();
  private readonly blocked = new Map<string, BlockedState>();
  private readonly activeCompactions = new Set<Promise<void>>();
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

  constructor(dependencies: CompactionCoordinatorDependencies) {
    this.candidates = dependencies.candidates;
    this.compaction = dependencies.compaction;
    this.configuration =
      dependencies.configuration ?? ({ ...defaultCompactionCoordinatorConfiguration } as const);
    this.clock = dependencies.clock ?? systemCompactionCoordinatorClock;
    this.random = dependencies.random ?? systemCompactionCoordinatorRandomSource;
    this.scheduler = dependencies.scheduler ?? systemCompactionCoordinatorScheduler;
    this.hooks = dependencies.hooks ?? {};
    this.telemetry = safeTelemetryRecorder(dependencies.telemetry ?? noOpTelemetryRecorder);
    this.telemetryClock = dependencies.telemetryClock ?? systemWorkerTelemetryClock;
    validateConfiguration(this.configuration);
    this.telemetry.setGauge("converge_compaction_active_work", {}, 0);
  }

  get diagnostics(): Readonly<{
    lifecycle: "idle" | "running" | "stopping" | "stopped";
    cursor: string | null;
    cycleInFlight: boolean;
    activeCompactions: number;
    retryBoards: number;
    blockedFingerprints: number;
    pollScheduled: boolean;
    graceScheduled: boolean;
  }> {
    return {
      lifecycle: this.lifecycle,
      cursor: this.cursor,
      cycleInFlight: this.cyclePromise !== undefined,
      activeCompactions: this.activeCompactions.size,
      retryBoards: this.retries.size,
      blockedFingerprints: this.blocked.size,
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
      this.resultsFenced = true;
      this.fenceTelemetry();
      this.lifecycle = "stopped";
      this.clearRetainedState();
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
    const result = await this.candidates.discover({
      cursor: this.cursor,
      scanLimit: this.configuration.candidateScanLimit,
      resultLimit: this.configuration.candidateResultLimit,
    });
    if (!this.canStartWork(generation)) return;
    this.cursor = result.nextCursor;
    const seen = new Set<string>();
    const candidates = result.candidates
      .slice(0, this.configuration.candidateResultLimit)
      .filter((candidate) => {
        const value = candidateFingerprint(candidate);
        if (seen.has(value)) return false;
        seen.add(value);
        return true;
      });
    let nextCandidate = 0;
    const worker = async (): Promise<void> => {
      while (this.canStartWork(generation)) {
        const candidate = candidates[nextCandidate];
        nextCandidate += 1;
        if (!candidate) return;
        if (this.shouldSkip(candidate, currentTime)) continue;
        if (!this.canStartWork(generation)) return;
        const task = this.compactCandidate(candidate, generation);
        this.activeCompactions.add(task);
        try {
          await task;
        } finally {
          this.activeCompactions.delete(task);
        }
      }
    };
    await Promise.all(
      Array.from(
        { length: Math.min(this.configuration.maximumConcurrency, candidates.length) },
        worker,
      ),
    );
  }

  private async compactCandidate(
    candidate: BoardCompactionCandidate,
    generation: number,
  ): Promise<void> {
    const startedAt = telemetryNow(this.telemetryClock);
    this.beginTelemetryWork();
    try {
      let result: BoardCompactionResult;
      try {
        result = await this.compaction.compact(candidate.boardId);
      } catch {
        if (this.canAcceptResult(generation)) {
          this.recordTransientFailure(candidate, this.currentTime());
          this.recordTelemetryOutcome("transient_failure", startedAt);
        }
        return;
      }
      if (!this.canAcceptResult(generation)) return;
      switch (result.outcome) {
        case "compacted":
          this.clearBoardState(candidate.boardId);
          this.recordTelemetryOutcome("compacted", startedAt);
          await this.invokeHook(() =>
            this.hooks.compacted?.({
              boardId: result.boardId,
              snapshotId: result.snapshotId,
              snapshotCanvasSeq: result.snapshotCanvasSeq,
              snapshotDeliverySeq: result.snapshotDeliverySeq,
              previousOperationFloor: result.previousOperationFloor,
              newOperationFloor: result.newOperationFloor,
              previousDeliveryFloor: result.previousDeliveryFloor,
              newDeliveryFloor: result.newDeliveryFloor,
              deletedOperationCount: result.deletedOperationCount,
              deletedOutboxCount: result.deletedOutboxCount,
            }),
          );
          return;
        case "no_progress":
          this.clearBoardState(candidate.boardId);
          this.recordTelemetryOutcome("no_progress", startedAt);
          return;
        case "no_verified_boundary":
          this.clearBoardState(candidate.boardId);
          this.recordTelemetryOutcome("no_boundary", startedAt);
          return;
        case "blocked":
          this.retries.delete(candidate.boardId);
          this.recordBlocked(candidate);
          this.recordTelemetryOutcome("blocked", startedAt);
          await this.invokeHook(() =>
            this.hooks.blocked?.({
              boardId: candidate.boardId,
              snapshotId: candidate.snapshotId,
              snapshotCanvasSeq: candidate.snapshotCanvasSeq,
              snapshotDeliverySeq: candidate.snapshotDeliverySeq,
              operationRecoveryFloor: candidate.operationRecoveryFloor,
              deliveryRecoveryFloor: candidate.deliveryRecoveryFloor,
              code: result.code,
            }),
          );
      }
    } finally {
      this.finishTelemetryWork();
    }
  }

  private beginTelemetryWork(): void {
    this.activeTelemetryWork += 1;
    if (!this.telemetryFenced)
      this.telemetry.setGauge("converge_compaction_active_work", {}, this.activeTelemetryWork);
  }

  private finishTelemetryWork(): void {
    this.activeTelemetryWork = Math.max(0, this.activeTelemetryWork - 1);
    if (!this.telemetryFenced)
      this.telemetry.setGauge("converge_compaction_active_work", {}, this.activeTelemetryWork);
  }

  private recordTelemetryOutcome(
    outcome: "compacted" | "no_progress" | "no_boundary" | "blocked" | "transient_failure",
    startedAt: number | undefined,
  ): void {
    if (this.telemetryFenced) return;
    this.telemetry.increment("converge_compaction_runs_total", { outcome });
    this.telemetry.observe(
      "converge_compaction_duration_seconds",
      {},
      telemetryDurationSeconds(this.telemetryClock, startedAt),
    );
    this.telemetry.emit({
      schemaVersion: 1,
      eventName: "compaction.result",
      severity: outcome === "compacted" || outcome === "no_progress" ? "info" : "warn",
      component: "compaction",
      timestamp: new Date().toISOString(),
      code: outcome.toUpperCase(),
    });
  }

  private shouldSkip(candidate: BoardCompactionCandidate, currentTime: number): boolean {
    const value = candidateFingerprint(candidate);
    const suppression = this.blocked.get(candidate.boardId);
    if (suppression?.fingerprint === value) {
      this.touch(this.blocked, candidate.boardId, suppression);
      return true;
    }
    const retry = this.retries.get(candidate.boardId);
    if (retry?.fingerprint === value && currentTime < retry.notBefore) {
      this.touch(this.retries, candidate.boardId, retry);
      return true;
    }
    return false;
  }

  private recordTransientFailure(candidate: BoardCompactionCandidate, currentTime: number): void {
    const value = candidateFingerprint(candidate);
    const previous = this.retries.get(candidate.boardId);
    const maximumExponent = Math.ceil(
      Math.log2(this.configuration.retryCapMs / this.configuration.retryBaseMs),
    );
    const attempt = Math.min(
      previous?.fingerprint === value ? previous.attempt + 1 : 1,
      maximumExponent + 1,
    );
    const limit = Math.min(
      this.configuration.retryCapMs,
      this.configuration.retryBaseMs * 2 ** (attempt - 1),
    );
    const delay = Math.floor(this.randomUnit() * (limit + 1));
    this.blocked.delete(candidate.boardId);
    this.touch(this.retries, candidate.boardId, {
      fingerprint: value,
      attempt,
      notBefore: currentTime + delay,
      ordinal: 0,
    });
  }

  private recordBlocked(candidate: BoardCompactionCandidate): void {
    this.touch(this.blocked, candidate.boardId, {
      fingerprint: candidateFingerprint(candidate),
      ordinal: 0,
    });
  }

  private touch<T extends { ordinal: number }>(map: Map<string, T>, key: string, value: T): void {
    value.ordinal = ++this.ordinal;
    map.delete(key);
    map.set(key, value);
    if (map.size <= this.configuration.retainedStateLimit) return;
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
    this.blocked.delete(boardId);
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
      throw new CompactionCoordinatorConfigurationError();
    return value;
  }

  private currentTime(): number {
    const value = this.clock.now();
    if (!Number.isSafeInteger(value) || value < 0)
      throw new CompactionCoordinatorConfigurationError();
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
      // Observation hooks cannot stop unrelated compaction candidates.
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
    this.telemetry.setGauge("converge_compaction_active_work", {}, 0);
    this.telemetryFenced = true;
  }

  private clearRetainedState(): void {
    this.clearPollTimer();
    this.cursor = null;
    this.retries.clear();
    this.blocked.clear();
    this.ordinal = 0;
  }
}
