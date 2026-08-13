import { describe, expect, it, vi } from "vitest";
import type {
  SnapshotCandidate,
  SnapshotCandidateDiscoveryOptions,
  SnapshotCandidateDiscoveryResult,
  SnapshotCaptureOptions,
  SnapshotCaptureOutcome,
} from "@converge/database";
import { InMemoryTelemetryRecorder } from "@converge/observability";
import {
  SnapshotCoordinator,
  SnapshotCoordinatorConfigurationError,
  defaultSnapshotCoordinatorConfiguration,
  type SnapshotCoordinatorConfiguration,
  type SnapshotCoordinatorRepository,
  type SnapshotCoordinatorScheduler,
} from "./snapshot-coordinator.js";

const boardIds = [
  "10000000-0000-4000-8000-000000000001",
  "10000000-0000-4000-8000-000000000002",
  "10000000-0000-4000-8000-000000000003",
  "10000000-0000-4000-8000-000000000004",
] as const;

function candidate(index: number, canvasHead = 10, deliveryHead = canvasHead): SnapshotCandidate {
  return {
    boardId: boardIds[index]!,
    canvasHead,
    deliveryHead,
    verifiedSnapshotCanvasHead: 0,
    verifiedSnapshotDeliveryHead: 0,
    reason: "operation_count",
  };
}

function discovery(
  candidates: SnapshotCandidate[] = [],
  nextCursor: string | null = boardIds[0],
): SnapshotCandidateDiscoveryResult {
  return { candidates, nextCursor, inspectedCount: candidates.length };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

class FakeScheduler implements SnapshotCoordinatorScheduler {
  currentTime = 0;
  readonly delays: number[] = [];
  private nextId = 1;
  private readonly tasks = new Map<number, { deadline: number; callback: () => void }>();

  setTimeout(callback: () => void, delayMs: number): unknown {
    const id = this.nextId++;
    this.delays.push(delayMs);
    this.tasks.set(id, { deadline: this.currentTime + delayMs, callback });
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.tasks.delete(handle as number);
  }

  async runNext(): Promise<void> {
    const next = [...this.tasks.entries()].sort(
      ([leftId, left], [rightId, right]) => left.deadline - right.deadline || leftId - rightId,
    )[0];
    if (!next) throw new Error("No scheduled task");
    const [id, task] = next;
    this.tasks.delete(id);
    this.currentTime = task.deadline;
    task.callback();
    await flush();
  }

  async advanceBy(delayMs: number): Promise<void> {
    this.currentTime += delayMs;
    const due = [...this.tasks.entries()]
      .filter(([, task]) => task.deadline <= this.currentTime)
      .sort(
        ([leftId, left], [rightId, right]) => left.deadline - right.deadline || leftId - rightId,
      );
    for (const [id, task] of due) {
      this.tasks.delete(id);
      task.callback();
    }
    await flush();
  }

  get pendingTasks(): number {
    return this.tasks.size;
  }
}

class FakeRepository implements SnapshotCoordinatorRepository {
  readonly discoverCalls: SnapshotCandidateDiscoveryOptions[] = [];
  readonly captureCalls: Array<{ boardId: string; options: SnapshotCaptureOptions }> = [];
  discoverImplementation: (
    options: SnapshotCandidateDiscoveryOptions,
  ) => Promise<SnapshotCandidateDiscoveryResult> = () => Promise.resolve(discovery());
  captureImplementation: (
    boardId: string,
    options: SnapshotCaptureOptions,
  ) => Promise<SnapshotCaptureOutcome> = () => Promise.resolve({ status: "no_longer_eligible" });

  discover(options: SnapshotCandidateDiscoveryOptions): Promise<SnapshotCandidateDiscoveryResult> {
    this.discoverCalls.push(options);
    return this.discoverImplementation(options);
  }

  capture(boardId: string, options: SnapshotCaptureOptions): Promise<SnapshotCaptureOutcome> {
    this.captureCalls.push({ boardId, options });
    return this.captureImplementation(boardId, options);
  }
}

function configuration(
  overrides: Partial<SnapshotCoordinatorConfiguration> = {},
): SnapshotCoordinatorConfiguration {
  return { ...defaultSnapshotCoordinatorConfiguration, ...overrides };
}

function harness(
  overrides: Partial<SnapshotCoordinatorConfiguration> = {},
  randomValues: number[] = [],
  telemetry = new InMemoryTelemetryRecorder(),
) {
  const repository = new FakeRepository();
  const scheduler = new FakeScheduler();
  const captured = vi.fn();
  const deterministicFailure = vi.fn();
  let randomIndex = 0;
  const coordinator = new SnapshotCoordinator({
    repository,
    configuration: configuration(overrides),
    clock: { now: () => scheduler.currentTime },
    random: { next: () => randomValues[randomIndex++] ?? 0.5 },
    scheduler,
    telemetry,
    telemetryClock: { now: () => scheduler.currentTime },
    hooks: { captured, deterministicFailure },
  });
  return { coordinator, repository, scheduler, captured, deterministicFailure, telemetry };
}

async function startAndFinishFirstCycle(state: ReturnType<typeof harness>): Promise<void> {
  await state.coordinator.start();
  await state.coordinator.runCycle();
  await flush();
}

describe("snapshot coordinator", () => {
  it("polls immediately, schedules symmetric jitter, and advances the repository cursor", async () => {
    const state = harness({}, [0, 0.999]);
    state.repository.discoverImplementation = (options) =>
      Promise.resolve(discovery([], options.cursor === null ? boardIds[1] : boardIds[2]));

    await startAndFinishFirstCycle(state);
    expect(state.repository.discoverCalls[0]).toEqual({
      cursor: null,
      scanLimit: 100,
      candidateLimit: 16,
      operationThreshold: 1_000,
      changedAgeMs: 86_400_000,
      operationBytesThreshold: 8_388_608,
      currentTime: new Date(0),
    });
    expect(state.scheduler.delays).toEqual([24_000]);
    expect(state.coordinator.diagnostics.cursor).toBe(boardIds[1]);

    await state.scheduler.runNext();
    expect(state.repository.discoverCalls[1]?.cursor).toBe(boardIds[1]);
    expect(state.coordinator.diagnostics.cursor).toBe(boardIds[2]);
    expect(state.scheduler.delays).toEqual([24_000, 35_988]);
    await state.coordinator.stop();
  });

  it("coalesces overlapping cycles", async () => {
    const state = harness();
    const pending = deferred<SnapshotCandidateDiscoveryResult>();
    state.repository.discoverImplementation = () => pending.promise;
    await state.coordinator.start();
    const first = state.coordinator.runCycle();
    const second = state.coordinator.runCycle();

    expect(first).toBe(second);
    expect(state.repository.discoverCalls).toHaveLength(1);
    expect(
      state.telemetry.snapshot().gauges.find(({ name }) => name === "converge_snapshot_active_work")
        ?.value,
    ).toBe(0);
    pending.resolve(discovery());
    await first;
    await flush();
    expect(state.scheduler.pendingTasks).toBe(1);
    await state.coordinator.stop();
  });

  it("runs at most two captures and lets unrelated candidates continue after failure", async () => {
    const state = harness({ candidateLimit: 4, maximumConcurrency: 2 });
    state.repository.discoverImplementation = () =>
      Promise.resolve(discovery([candidate(0), candidate(1), candidate(2), candidate(3)]));
    const pending = boardIds.map(() => deferred<SnapshotCaptureOutcome>());
    state.repository.captureImplementation = (boardId) => {
      const index = boardIds.findIndex((candidateBoardId) => candidateBoardId === boardId);
      return pending[index]!.promise;
    };

    await state.coordinator.start();
    await flush();
    expect(state.repository.captureCalls.map((call) => call.boardId)).toEqual([
      boardIds[0],
      boardIds[1],
    ]);
    expect(state.coordinator.diagnostics.activeCaptures).toBe(2);
    expect(
      state.telemetry.snapshot().gauges.find(({ name }) => name === "converge_snapshot_active_work")
        ?.value,
    ).toBe(2);

    pending[0]!.reject(new Error("database unavailable"));
    await flush();
    expect(state.repository.captureCalls[2]?.boardId).toBe(boardIds[2]);
    pending[1]!.resolve({ status: "no_longer_eligible" });
    await flush();
    expect(state.repository.captureCalls[3]?.boardId).toBe(boardIds[3]);
    pending[2]!.resolve({ status: "no_longer_eligible" });
    pending[3]!.resolve({ status: "no_longer_eligible" });
    await state.coordinator.runCycle();
    expect(state.coordinator.diagnostics.activeCaptures).toBe(0);
    expect(
      state.telemetry.snapshot().gauges.find(({ name }) => name === "converge_snapshot_active_work")
        ?.value,
    ).toBe(0);
    expect(state.coordinator.diagnostics.retryBoards).toBe(1);
    await state.coordinator.stop();
  });

  it("applies busy cooldown without a failure attempt and clears state when no longer eligible", async () => {
    const state = harness();
    state.repository.discoverImplementation = () => Promise.resolve(discovery([candidate(0)]));
    const outcomes: SnapshotCaptureOutcome[] = [
      { status: "busy" },
      { status: "no_longer_eligible" },
    ];
    state.repository.captureImplementation = () => Promise.resolve(outcomes.shift()!);

    await startAndFinishFirstCycle(state);
    expect(state.coordinator.diagnostics).toMatchObject({ cooldownBoards: 1, retryBoards: 0 });
    state.scheduler.currentTime = 4_999;
    await state.coordinator.runCycle();
    expect(state.repository.captureCalls).toHaveLength(1);
    state.scheduler.currentTime = 5_000;
    await state.coordinator.runCycle();
    expect(state.repository.captureCalls).toHaveLength(2);
    expect(state.coordinator.diagnostics).toMatchObject({ cooldownBoards: 0, retryBoards: 0 });
    await state.coordinator.stop();
  });

  it("uses uncapped then capped full-jitter transient backoff and clears it after capture", async () => {
    const state = harness({ retryBaseMs: 100, retryCapMs: 200 });
    state.repository.discoverImplementation = () => Promise.resolve(discovery([candidate(0)]));
    let attempt = 0;
    state.repository.captureImplementation = () => {
      attempt += 1;
      if (attempt < 3) return Promise.reject(new Error("transient"));
      return Promise.resolve({
        status: "captured",
        snapshotId: crypto.randomUUID(),
        canvasHead: 10,
        deliveryHead: 10,
      });
    };

    await startAndFinishFirstCycle(state);
    state.scheduler.currentTime = 49;
    await state.coordinator.runCycle();
    expect(state.repository.captureCalls).toHaveLength(1);
    state.scheduler.currentTime = 50;
    await state.coordinator.runCycle();
    expect(state.repository.captureCalls).toHaveLength(2);
    state.scheduler.currentTime = 149;
    await state.coordinator.runCycle();
    expect(state.repository.captureCalls).toHaveLength(2);
    state.scheduler.currentTime = 150;
    await state.coordinator.runCycle();
    expect(state.repository.captureCalls).toHaveLength(3);
    expect(state.coordinator.diagnostics.retryBoards).toBe(0);
    expect(state.captured).toHaveBeenCalledOnce();
    await state.coordinator.stop();
  });

  it("suppresses deterministic failure once per head and permits a changed head", async () => {
    const state = harness();
    let head = 10;
    state.repository.discoverImplementation = () =>
      Promise.resolve(discovery([candidate(0, head, head + 1)]));
    state.repository.captureImplementation = () =>
      Promise.resolve({ status: "deterministic_failure", code: "SNAPSHOT_TOO_LARGE" });

    await startAndFinishFirstCycle(state);
    await state.coordinator.runCycle();
    expect(state.repository.captureCalls).toHaveLength(1);
    expect(state.deterministicFailure).toHaveBeenCalledOnce();
    expect(state.deterministicFailure.mock.calls[0]?.[0]).toEqual({
      boardId: boardIds[0],
      canvasHead: 10,
      deliveryHead: 11,
      code: "SNAPSHOT_TOO_LARGE",
    });

    head = 11;
    await state.coordinator.runCycle();
    expect(state.repository.captureCalls).toHaveLength(2);
    expect(state.deterministicFailure).toHaveBeenCalledTimes(2);
    await state.coordinator.stop();
  });

  it("maps all five capture outcomes once with duration and no private fields", async () => {
    const telemetry = new InMemoryTelemetryRecorder();
    const outcomes: Array<SnapshotCaptureOutcome | Error> = [
      {
        status: "captured",
        snapshotId: "50000000-0000-4000-8000-000000000099",
        canvasHead: 10,
        deliveryHead: 10,
      },
      { status: "busy" },
      { status: "no_longer_eligible" },
      { status: "deterministic_failure", code: "SNAPSHOT_TOO_LARGE" },
      new Error("SELECT secret_snapshot_payload FROM private_table"),
    ];

    for (const outcome of outcomes) {
      const state = harness({}, [], telemetry);
      state.repository.discoverImplementation = () => Promise.resolve(discovery([candidate(0)]));
      state.repository.captureImplementation = () => {
        state.scheduler.currentTime += 250;
        return outcome instanceof Error ? Promise.reject(outcome) : Promise.resolve(outcome);
      };
      await startAndFinishFirstCycle(state);
      await state.coordinator.stop();
    }

    const snapshot = telemetry.snapshot();
    expect(
      snapshot.counters
        .filter(({ name }) => name === "converge_snapshot_runs_total")
        .map(({ labels, value }) => ({ ...labels, value })),
    ).toEqual([
      { outcome: "busy", value: 1 },
      { outcome: "captured", value: 1 },
      { outcome: "deterministic_failure", value: 1 },
      { outcome: "no_progress", value: 1 },
      { outcome: "transient_failure", value: 1 },
    ]);
    expect(
      snapshot.histograms.find(({ name }) => name === "converge_snapshot_duration_seconds"),
    ).toMatchObject({ count: 5, sum: 1.25 });
    expect(snapshot.events.map(({ code }) => code)).toEqual([
      "CAPTURED",
      "BUSY",
      "NO_PROGRESS",
      "DETERMINISTIC_FAILURE",
      "TRANSIENT_FAILURE",
    ]);
    expect(
      snapshot.gauges.find(({ name }) => name === "converge_snapshot_active_work")?.value,
    ).toBe(0);
    expect(JSON.stringify(snapshot)).not.toMatch(
      /10000000-0000-4000|50000000-0000-4000|SELECT|private|secret|payload/i,
    );
  });

  it("bounds suppression, retry, and cooldown maps with deterministic LRU eviction", async () => {
    const state = harness({
      candidateScanLimit: 3,
      candidateLimit: 3,
      maximumConcurrency: 1,
      failureFingerprintLimit: 2,
    });
    let candidates = [candidate(0), candidate(1), candidate(2)];
    let mode: "deterministic" | "transient" | "busy" = "deterministic";
    state.repository.discoverImplementation = () => Promise.resolve(discovery(candidates));
    state.repository.captureImplementation = () => {
      if (mode === "transient") return Promise.reject(new Error("transient"));
      if (mode === "busy") return Promise.resolve({ status: "busy" });
      return Promise.resolve({ status: "deterministic_failure", code: "SNAPSHOT_TOO_LARGE" });
    };

    await startAndFinishFirstCycle(state);
    expect(state.coordinator.diagnostics.suppressedFingerprints).toBe(2);
    candidates = [candidate(0)];
    await state.coordinator.runCycle();
    expect(
      state.repository.captureCalls.filter((call) => call.boardId === boardIds[0]),
    ).toHaveLength(2);

    mode = "transient";
    candidates = [candidate(0, 11), candidate(1, 11), candidate(2, 11)];
    await state.coordinator.runCycle();
    expect(state.coordinator.diagnostics.retryBoards).toBe(2);
    mode = "busy";
    candidates = [candidate(0, 12), candidate(1, 12), candidate(2, 12)];
    await state.coordinator.runCycle();
    expect(state.coordinator.diagnostics.cooldownBoards).toBe(2);
    expect(state.coordinator.diagnostics).toMatchObject({
      retryBoards: 2,
      cooldownBoards: 2,
      suppressedFingerprints: 2,
    });
    await state.coordinator.stop();
  });

  it("stops during discovery without beginning capture and shares repeated stop", async () => {
    const state = harness();
    const pending = deferred<SnapshotCandidateDiscoveryResult>();
    state.repository.discoverImplementation = () => pending.promise;
    await state.coordinator.start();
    const firstStop = state.coordinator.stop();
    expect(state.coordinator.stop()).toBe(firstStop);
    pending.resolve(discovery([candidate(0)]));
    await firstStop;

    expect(state.repository.captureCalls).toEqual([]);
    expect(state.scheduler.pendingTasks).toBe(0);
    expect(state.coordinator.diagnostics).toMatchObject({
      lifecycle: "stopped",
      cursor: null,
      retryBoards: 0,
      cooldownBoards: 0,
      suppressedFingerprints: 0,
    });
  });

  it("drains active capture inside grace and invokes its bounded hook", async () => {
    const state = harness({ shutdownGraceMs: 100 });
    const pending = deferred<SnapshotCaptureOutcome>();
    state.repository.discoverImplementation = () => Promise.resolve(discovery([candidate(0)]));
    state.repository.captureImplementation = () => pending.promise;
    await state.coordinator.start();
    await flush();
    const stopping = state.coordinator.stop();
    pending.resolve({
      status: "captured",
      snapshotId: crypto.randomUUID(),
      canvasHead: 10,
      deliveryHead: 10,
    });
    await stopping;

    expect(state.captured).toHaveBeenCalledOnce();
    expect(state.scheduler.pendingTasks).toBe(0);
    expect(state.coordinator.diagnostics.lifecycle).toBe("stopped");
  });

  it("fences capture completion after grace expiry", async () => {
    const state = harness({ shutdownGraceMs: 100 });
    const pending = deferred<SnapshotCaptureOutcome>();
    state.repository.discoverImplementation = () => Promise.resolve(discovery([candidate(0)]));
    state.repository.captureImplementation = () => pending.promise;
    await state.coordinator.start();
    await flush();
    const stopping = state.coordinator.stop();
    await state.scheduler.advanceBy(100);
    await stopping;
    const fenced = state.telemetry.snapshot();
    pending.resolve({
      status: "captured",
      snapshotId: crypto.randomUUID(),
      canvasHead: 10,
      deliveryHead: 10,
    });
    await flush();

    expect(state.captured).not.toHaveBeenCalled();
    expect(state.telemetry.snapshot()).toEqual(fenced);
    expect(state.scheduler.pendingTasks).toBe(0);
    expect(state.coordinator.diagnostics).toMatchObject({
      lifecycle: "stopped",
      cursor: null,
      pollScheduled: false,
      graceScheduled: false,
    });
  });

  it("supports harmless repeated start and stop", async () => {
    const state = harness();
    await state.coordinator.start();
    await state.coordinator.start();
    await state.coordinator.runCycle();
    expect(state.repository.discoverCalls).toHaveLength(1);
    const first = state.coordinator.stop();
    expect(state.coordinator.stop()).toBe(first);
    await first;

    await state.coordinator.start();
    await state.coordinator.runCycle();
    expect(state.repository.discoverCalls).toHaveLength(1);
    expect(state.coordinator.stop()).toBe(first);
  });

  it("rejects invalid timing, capacity, jitter, and retry configuration", () => {
    const repository = new FakeRepository();
    const invalid: Array<Partial<SnapshotCoordinatorConfiguration>> = [
      { pollIntervalMs: 0 },
      { pollIntervalMs: 2_147_483_647, pollJitterPercent: 20 },
      { pollJitterPercent: 0 },
      { pollJitterPercent: 21 },
      { candidateScanLimit: 0 },
      { candidateScanLimit: 101 },
      { candidateLimit: 17 },
      { candidateScanLimit: 1, candidateLimit: 2 },
      { candidateLimit: 1, maximumConcurrency: 2 },
      { maximumConcurrency: 3 },
      { operationThreshold: Number.MAX_SAFE_INTEGER + 1 },
      { changedAgeMs: 2_147_483_648 },
      { operationBytesThreshold: 0 },
      { maximumPayloadBytes: 0 },
      { maximumPayloadBytes: 16_777_217 },
      { retryBaseMs: 101, retryCapMs: 100 },
      { retryCapMs: 300_001 },
      { busyRetryMs: 0 },
      { failureFingerprintLimit: 0 },
      { failureFingerprintLimit: 1_001 },
      { shutdownGraceMs: 0 },
    ];
    for (const override of invalid)
      expect(
        () =>
          new SnapshotCoordinator({
            repository,
            configuration: configuration(override),
          }),
      ).toThrow(SnapshotCoordinatorConfigurationError);
  });

  it("keeps one timer and no accumulated lifecycle resources across one hundred idle cycles", async () => {
    const state = harness();
    await startAndFinishFirstCycle(state);
    for (let cycle = 0; cycle < 100; cycle += 1) {
      expect(state.scheduler.pendingTasks).toBe(1);
      await state.scheduler.runNext();
    }
    expect(state.repository.discoverCalls).toHaveLength(101);
    expect(state.scheduler.pendingTasks).toBe(1);
    expect(state.coordinator.diagnostics).toMatchObject({
      cycleInFlight: false,
      activeCaptures: 0,
      retryBoards: 0,
      cooldownBoards: 0,
      suppressedFingerprints: 0,
    });
    await state.coordinator.stop();
    expect(state.scheduler.pendingTasks).toBe(0);
  });
});
