import { describe, expect, it, vi } from "vitest";
import type {
  BoardCompactionCandidate,
  BoardCompactionCandidateDiscoveryOptions,
  BoardCompactionCandidateDiscoveryResult,
  BoardCompactionResult,
} from "@converge/database";
import {
  CompactionCoordinator,
  CompactionCoordinatorConfigurationError,
  defaultCompactionCoordinatorConfiguration,
  type CompactionCandidateDiscoveryRepository,
  type CompactionCoordinatorConfiguration,
  type CompactionCoordinatorScheduler,
  type CompactionExecutionRepository,
} from "./compaction-coordinator.js";

const boardIds = Array.from(
  { length: 5 },
  (_, index) => `10000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
);

function candidate(
  index: number,
  snapshotCanvasSeq = 10,
  snapshotDeliverySeq = snapshotCanvasSeq,
  operationRecoveryFloor = 0,
  deliveryRecoveryFloor = 0,
): BoardCompactionCandidate {
  return {
    boardId: boardIds[index]!,
    operationRecoveryFloor,
    deliveryRecoveryFloor,
    snapshotId: `50000000-0000-4000-8000-${String(index + snapshotCanvasSeq).padStart(12, "0")}`,
    snapshotCanvasSeq,
    snapshotDeliverySeq,
    canvasHead: snapshotCanvasSeq + 1,
    deliveryHead: snapshotDeliverySeq + 1,
  };
}

function discovery(
  candidates: BoardCompactionCandidate[] = [],
  nextCursor: string | null = boardIds[0]!,
): BoardCompactionCandidateDiscoveryResult {
  return { candidates, nextCursor, inspectedCount: candidates.length };
}

function compacted(
  value: BoardCompactionCandidate,
): Extract<BoardCompactionResult, { outcome: "compacted" | "no_progress" }> {
  return {
    outcome: "compacted",
    boardId: value.boardId,
    previousOperationFloor: value.operationRecoveryFloor,
    newOperationFloor: value.snapshotCanvasSeq,
    previousDeliveryFloor: value.deliveryRecoveryFloor,
    newDeliveryFloor: value.snapshotDeliverySeq,
    deletedOperationCount: value.snapshotCanvasSeq - value.operationRecoveryFloor,
    deletedOutboxCount: value.snapshotDeliverySeq - value.deliveryRecoveryFloor,
    snapshotId: value.snapshotId,
    snapshotCanvasSeq: value.snapshotCanvasSeq,
    snapshotDeliverySeq: value.snapshotDeliverySeq,
  };
}

function noProgress(
  value: BoardCompactionCandidate,
): Extract<BoardCompactionResult, { outcome: "compacted" | "no_progress" }> {
  return {
    ...compacted(value),
    outcome: "no_progress",
    deletedOperationCount: 0,
    deletedOutboxCount: 0,
  };
}

function blocked(value: BoardCompactionCandidate): BoardCompactionResult {
  return {
    outcome: "blocked",
    boardId: value.boardId,
    code: "OUTBOX_PUBLICATION_EVIDENCE_INVALID",
    snapshotId: value.snapshotId,
    snapshotCanvasSeq: value.snapshotCanvasSeq,
    snapshotDeliverySeq: value.snapshotDeliverySeq,
  };
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

class FakeScheduler implements CompactionCoordinatorScheduler {
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

class FakeCandidates implements CompactionCandidateDiscoveryRepository {
  readonly calls: BoardCompactionCandidateDiscoveryOptions[] = [];
  implementation: (
    options: BoardCompactionCandidateDiscoveryOptions,
  ) => Promise<BoardCompactionCandidateDiscoveryResult> = () => Promise.resolve(discovery());

  discover(
    options: BoardCompactionCandidateDiscoveryOptions,
  ): Promise<BoardCompactionCandidateDiscoveryResult> {
    this.calls.push(options);
    return this.implementation(options);
  }
}

class FakeCompaction implements CompactionExecutionRepository {
  readonly calls: string[] = [];
  implementation: (boardId: string) => Promise<BoardCompactionResult> = (boardId) =>
    Promise.resolve({ outcome: "no_verified_boundary", boardId });

  compact(boardId: string): Promise<BoardCompactionResult> {
    this.calls.push(boardId);
    return this.implementation(boardId);
  }
}

function configuration(
  overrides: Partial<CompactionCoordinatorConfiguration> = {},
): CompactionCoordinatorConfiguration {
  return { ...defaultCompactionCoordinatorConfiguration, ...overrides };
}

function harness(
  overrides: Partial<CompactionCoordinatorConfiguration> = {},
  randomValues: number[] = [],
) {
  const candidates = new FakeCandidates();
  const compaction = new FakeCompaction();
  const scheduler = new FakeScheduler();
  const success = vi.fn();
  const blockedHook = vi.fn();
  let randomIndex = 0;
  const coordinator = new CompactionCoordinator({
    candidates,
    compaction,
    configuration: configuration(overrides),
    clock: { now: () => scheduler.currentTime },
    random: { next: () => randomValues[randomIndex++] ?? 0.5 },
    scheduler,
    hooks: { compacted: success, blocked: blockedHook },
  });
  return { coordinator, candidates, compaction, scheduler, success, blockedHook };
}

async function startAndFinish(state: ReturnType<typeof harness>): Promise<void> {
  await state.coordinator.start();
  await state.coordinator.runCycle();
  await flush();
}

describe("standalone compaction coordinator", () => {
  it("scans immediately, advances its cursor, and schedules symmetric jitter", async () => {
    const state = harness({}, [0, 0.999]);
    state.candidates.implementation = (input) =>
      Promise.resolve(discovery([], input.cursor === null ? boardIds[1] : boardIds[2]));

    await startAndFinish(state);
    expect(state.candidates.calls[0]).toEqual({ cursor: null, scanLimit: 100, resultLimit: 16 });
    expect(state.coordinator.diagnostics.cursor).toBe(boardIds[1]);
    expect(state.scheduler.delays).toEqual([240_000]);
    await state.scheduler.runNext();
    expect(state.candidates.calls[1]?.cursor).toBe(boardIds[1]);
    expect(state.coordinator.diagnostics.cursor).toBe(boardIds[2]);
    expect(state.scheduler.delays).toEqual([240_000, 359_880]);
    await state.coordinator.stop();
  });

  it("keeps poll cycles single-flight", async () => {
    const state = harness();
    const pending = deferred<BoardCompactionCandidateDiscoveryResult>();
    state.candidates.implementation = () => pending.promise;
    await state.coordinator.start();
    const first = state.coordinator.runCycle();
    expect(state.coordinator.runCycle()).toBe(first);
    expect(state.candidates.calls).toHaveLength(1);
    pending.resolve(discovery());
    await first;
    await flush();
    expect(state.scheduler.pendingTasks).toBe(1);
    await state.coordinator.stop();
  });

  it("runs at most two compactions and isolates one board failure", async () => {
    const state = harness({ candidateResultLimit: 4, maximumConcurrency: 2 });
    const values = [candidate(0), candidate(1), candidate(2), candidate(3)];
    state.candidates.implementation = () => Promise.resolve(discovery(values));
    const pending = values.map(() => deferred<BoardCompactionResult>());
    state.compaction.implementation = (boardId) => pending[boardIds.indexOf(boardId)]!.promise;

    await state.coordinator.start();
    await flush();
    expect(state.compaction.calls).toEqual(boardIds.slice(0, 2));
    expect(state.coordinator.diagnostics.activeCompactions).toBe(2);
    pending[0]!.reject(new Error("database unavailable"));
    await flush();
    expect(state.compaction.calls[2]).toBe(boardIds[2]);
    pending[1]!.resolve({ outcome: "no_verified_boundary", boardId: boardIds[1]! });
    await flush();
    expect(state.compaction.calls[3]).toBe(boardIds[3]);
    pending[2]!.resolve(noProgress(values[2]!));
    pending[3]!.resolve(noProgress(values[3]!));
    await state.coordinator.runCycle();
    expect(state.coordinator.diagnostics).toMatchObject({ activeCompactions: 0, retryBoards: 1 });
    await state.coordinator.stop();
  });

  it("clears state and emits one bounded success notification for compacted", async () => {
    const state = harness();
    const value = candidate(0);
    state.candidates.implementation = () => Promise.resolve(discovery([value, value]));
    state.compaction.implementation = () => Promise.resolve(compacted(value));

    await startAndFinish(state);
    expect(state.compaction.calls).toEqual([value.boardId]);
    expect(state.success).toHaveBeenCalledExactlyOnceWith({
      boardId: value.boardId,
      snapshotId: value.snapshotId,
      snapshotCanvasSeq: value.snapshotCanvasSeq,
      snapshotDeliverySeq: value.snapshotDeliverySeq,
      previousOperationFloor: value.operationRecoveryFloor,
      newOperationFloor: value.snapshotCanvasSeq,
      previousDeliveryFloor: value.deliveryRecoveryFloor,
      newDeliveryFloor: value.snapshotDeliverySeq,
      deletedOperationCount: 10,
      deletedOutboxCount: 10,
    });
    expect(state.coordinator.diagnostics).toMatchObject({ retryBoards: 0, blockedFingerprints: 0 });
    await state.coordinator.stop();
  });

  it("treats no-progress and no-boundary as harmless cleanup", async () => {
    const state = harness({ retryBaseMs: 100, retryCapMs: 100 });
    const values = [candidate(0), candidate(1)];
    state.candidates.implementation = () => Promise.resolve(discovery(values));
    let failure = true;
    state.compaction.implementation = (boardId) => {
      if (failure) return Promise.reject(new Error("transient"));
      return Promise.resolve(
        boardId === values[0]!.boardId
          ? noProgress(values[0]!)
          : { outcome: "no_verified_boundary", boardId },
      );
    };
    await startAndFinish(state);
    expect(state.coordinator.diagnostics.retryBoards).toBe(2);
    failure = false;
    state.scheduler.currentTime = 100;
    await state.coordinator.runCycle();
    expect(state.coordinator.diagnostics.retryBoards).toBe(0);
    expect(state.success).not.toHaveBeenCalled();
    expect(state.blockedHook).not.toHaveBeenCalled();
    await state.coordinator.stop();
  });

  it("emits blocked once per fingerprint and permits a changed boundary", async () => {
    const state = harness();
    let value = candidate(0);
    state.candidates.implementation = () => Promise.resolve(discovery([value]));
    state.compaction.implementation = () => Promise.resolve(blocked(value));

    await startAndFinish(state);
    await state.coordinator.runCycle();
    expect(state.compaction.calls).toHaveLength(1);
    expect(state.blockedHook).toHaveBeenCalledOnce();
    expect(state.blockedHook.mock.calls[0]?.[0]).toEqual({
      boardId: value.boardId,
      snapshotId: value.snapshotId,
      snapshotCanvasSeq: 10,
      snapshotDeliverySeq: 10,
      operationRecoveryFloor: 0,
      deliveryRecoveryFloor: 0,
      code: "OUTBOX_PUBLICATION_EVIDENCE_INVALID",
    });

    value = candidate(0, 11, 12, 1, 1);
    await state.coordinator.runCycle();
    expect(state.compaction.calls).toHaveLength(2);
    expect(state.blockedHook).toHaveBeenCalledTimes(2);
    await state.coordinator.stop();
  });

  it("applies full-jitter exponential retry through the cap", async () => {
    const state = harness({ retryBaseMs: 100, retryCapMs: 200 }, [0.5, 0.5, 0.5]);
    const value = candidate(0);
    state.candidates.implementation = () => Promise.resolve(discovery([value]));
    state.compaction.implementation = () => Promise.reject(new Error("transient"));

    await startAndFinish(state);
    state.scheduler.currentTime = 49;
    await state.coordinator.runCycle();
    expect(state.compaction.calls).toHaveLength(1);
    state.scheduler.currentTime = 50;
    await state.coordinator.runCycle();
    expect(state.compaction.calls).toHaveLength(2);
    state.scheduler.currentTime = 150;
    await state.coordinator.runCycle();
    expect(state.compaction.calls).toHaveLength(3);
    state.scheduler.currentTime = 250;
    await state.coordinator.runCycle();
    expect(state.compaction.calls).toHaveLength(4);
    expect(state.coordinator.diagnostics.retryBoards).toBe(1);
    await state.coordinator.stop();
  });

  it("bounds retry and blocked state with deterministic LRU eviction", async () => {
    const state = harness({
      candidateScanLimit: 3,
      candidateResultLimit: 3,
      maximumConcurrency: 1,
      retainedStateLimit: 2,
    });
    let values = [candidate(0), candidate(1), candidate(2)];
    let mode: "blocked" | "transient" = "blocked";
    state.candidates.implementation = () => Promise.resolve(discovery(values));
    state.compaction.implementation = () =>
      mode === "blocked" ? Promise.resolve(blocked(values[0]!)) : Promise.reject(new Error("db"));

    await startAndFinish(state);
    expect(state.coordinator.diagnostics.blockedFingerprints).toBe(2);
    values = [candidate(0)];
    await state.coordinator.runCycle();
    expect(state.compaction.calls.filter((boardId) => boardId === boardIds[0])).toHaveLength(2);

    mode = "transient";
    values = [candidate(0, 11), candidate(1, 11), candidate(2, 11)];
    await state.coordinator.runCycle();
    expect(state.coordinator.diagnostics).toMatchObject({
      retryBoards: 2,
      blockedFingerprints: 0,
    });
    await state.coordinator.stop();
  });

  it("accepts duplicate multi-worker style discovery as harmless no-progress", async () => {
    const state = harness();
    const value = candidate(0);
    state.candidates.implementation = () => Promise.resolve(discovery([value]));
    state.compaction.implementation = () => Promise.resolve(noProgress(value));
    await startAndFinish(state);
    await state.coordinator.runCycle();
    expect(state.compaction.calls).toHaveLength(2);
    expect(state.success).not.toHaveBeenCalled();
    expect(state.blockedHook).not.toHaveBeenCalled();
    await state.coordinator.stop();
  });

  it("stops during discovery without starting compaction and shares stop", async () => {
    const state = harness();
    const pending = deferred<BoardCompactionCandidateDiscoveryResult>();
    state.candidates.implementation = () => pending.promise;
    await state.coordinator.start();
    const stopping = state.coordinator.stop();
    expect(state.coordinator.stop()).toBe(stopping);
    pending.resolve(discovery([candidate(0)]));
    await stopping;
    expect(state.compaction.calls).toEqual([]);
    expect(state.scheduler.pendingTasks).toBe(0);
    expect(state.coordinator.diagnostics).toMatchObject({ lifecycle: "stopped", cursor: null });
  });

  it("drains active compaction within grace and invokes its hook", async () => {
    const state = harness({ shutdownGraceMs: 100 });
    const value = candidate(0);
    const pending = deferred<BoardCompactionResult>();
    state.candidates.implementation = () => Promise.resolve(discovery([value]));
    state.compaction.implementation = () => pending.promise;
    await state.coordinator.start();
    await flush();
    const stopping = state.coordinator.stop();
    pending.resolve(compacted(value));
    await stopping;
    expect(state.success).toHaveBeenCalledOnce();
    expect(state.scheduler.pendingTasks).toBe(0);
  });

  it("fences late results after shutdown grace expires", async () => {
    const state = harness({ shutdownGraceMs: 100 });
    const value = candidate(0);
    const pending = deferred<BoardCompactionResult>();
    state.candidates.implementation = () => Promise.resolve(discovery([value]));
    state.compaction.implementation = () => pending.promise;
    await state.coordinator.start();
    await flush();
    const stopping = state.coordinator.stop();
    await state.scheduler.advanceBy(100);
    await stopping;
    pending.resolve(blocked(value));
    await flush();
    expect(state.blockedHook).not.toHaveBeenCalled();
    expect(state.scheduler.pendingTasks).toBe(0);
    expect(state.coordinator.diagnostics).toMatchObject({
      lifecycle: "stopped",
      cursor: null,
      retryBoards: 0,
      blockedFingerprints: 0,
    });
  });

  it("makes repeated start and stop harmless", async () => {
    const state = harness();
    await state.coordinator.start();
    await state.coordinator.start();
    await state.coordinator.runCycle();
    expect(state.candidates.calls).toHaveLength(1);
    const stopping = state.coordinator.stop();
    expect(state.coordinator.stop()).toBe(stopping);
    await stopping;
    await state.coordinator.start();
    expect(state.candidates.calls).toHaveLength(1);
  });

  it("rejects invalid timing, jitter, capacity, retry, and retained-state configuration", () => {
    const invalid: Array<Partial<CompactionCoordinatorConfiguration>> = [
      { pollIntervalMs: 0 },
      { pollIntervalMs: 2_147_483_647, pollJitterPercent: 20 },
      { pollJitterPercent: -1 },
      { pollJitterPercent: 101 },
      { candidateScanLimit: 0 },
      { candidateScanLimit: 101 },
      { candidateResultLimit: 17 },
      { candidateScanLimit: 1, candidateResultLimit: 2 },
      { candidateResultLimit: 1, maximumConcurrency: 2 },
      { maximumConcurrency: 3 },
      { retryBaseMs: 101, retryCapMs: 100 },
      { retryCapMs: 300_001 },
      { retainedStateLimit: 0 },
      { retainedStateLimit: 1_001 },
      { shutdownGraceMs: 0 },
    ];
    for (const override of invalid)
      expect(
        () =>
          new CompactionCoordinator({
            candidates: new FakeCandidates(),
            compaction: new FakeCompaction(),
            configuration: configuration(override),
          }),
      ).toThrow(CompactionCoordinatorConfigurationError);
  });

  it("retains one timer and no lifecycle resources across one hundred idle cycles", async () => {
    const state = harness();
    await startAndFinish(state);
    for (let cycle = 0; cycle < 100; cycle += 1) {
      expect(state.scheduler.pendingTasks).toBe(1);
      await state.scheduler.runNext();
    }
    expect(state.candidates.calls).toHaveLength(101);
    expect(state.scheduler.pendingTasks).toBe(1);
    expect(state.coordinator.diagnostics).toMatchObject({
      cycleInFlight: false,
      activeCompactions: 0,
      retryBoards: 0,
      blockedFingerprints: 0,
    });
    await state.coordinator.stop();
    expect(state.scheduler.pendingTasks).toBe(0);
  });
});
