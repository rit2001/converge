import { describe, expect, it, vi } from "vitest";
import {
  BoardDeliveryHeadReadError,
  BoardRepository,
  type BoardDeliveryHead,
  type DatabasePool,
} from "@converge/database";
import {
  BoardDeliveryHeadWatchdog,
  BoardDeliveryHeadWatchdogError,
  defaultBoardDeliveryHeadWatchdogConfiguration,
  type BoardDeliveryHeadRepository,
  type BoardDeliveryHeadWatchdogConfiguration,
  type BoardDeliveryHeadWatchdogLifecycleEvent,
  type BoardDeliveryHeadWatchdogScheduler,
} from "./board-delivery-head-watchdog.js";

const ids = {
  a: "10000000-0000-4000-8000-000000000001",
  b: "10000000-0000-4000-8000-000000000002",
  c: "10000000-0000-4000-8000-000000000003",
} as const;

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
}

class ControlledScheduler implements BoardDeliveryHeadWatchdogScheduler {
  currentTime = 0;
  randomValue = 0.5;
  private nextTaskId = 1;
  private readonly tasks = new Map<
    number,
    { deadline: number; resolve: () => void; signal: AbortSignal; abort: () => void }
  >();

  now = (): number => this.currentTime;
  random = (): number => this.randomValue;

  wait(delayMs: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      const taskId = this.nextTaskId++;
      const settle = (): void => {
        const task = this.tasks.get(taskId);
        if (!task) return;
        this.tasks.delete(taskId);
        signal.removeEventListener("abort", task.abort);
        resolve();
      };
      const task = {
        deadline: this.currentTime + delayMs,
        resolve: settle,
        signal,
        abort: settle,
      };
      this.tasks.set(taskId, task);
      signal.addEventListener("abort", settle, { once: true });
    });
  }

  async advanceBy(milliseconds: number): Promise<void> {
    this.currentTime += milliseconds;
    for (const task of [...this.tasks.values()])
      if (task.deadline <= this.currentTime) task.resolve();
    await flush();
  }

  get pendingTasks(): number {
    return this.tasks.size;
  }
}

function configuration(
  overrides: Partial<BoardDeliveryHeadWatchdogConfiguration> = {},
): BoardDeliveryHeadWatchdogConfiguration {
  return { ...defaultBoardDeliveryHeadWatchdogConfiguration, ...overrides };
}

function harness(
  overrides: Partial<BoardDeliveryHeadWatchdogConfiguration> = {},
  repositoryImplementation?: (boardIds: readonly string[]) => Promise<readonly BoardDeliveryHead[]>,
) {
  const active = new Set<string>();
  const databaseHeads = new Map<string, number>();
  const localHeads = new Map<string, number>();
  const lifecycleEvents: BoardDeliveryHeadWatchdogLifecycleEvent[] = [];
  const getBoardDeliveryHeads = vi.fn(
    repositoryImplementation ??
      ((boardIds: readonly string[]) =>
        Promise.resolve(
          boardIds.map((boardId) => ({
            boardId,
            lastDeliverySeq: databaseHeads.get(boardId) ?? 0,
          })),
        )),
  );
  const scheduler = new ControlledScheduler();
  const watchdog = new BoardDeliveryHeadWatchdog(
    { getBoardDeliveryHeads },
    { activeBoardIds: () => active.values() },
    { handledDeliverySequence: (boardId) => localHeads.get(boardId) ?? 0 },
    {
      lifecycle: (event) => {
        lifecycleEvents.push(event);
      },
    },
    configuration(overrides),
    scheduler,
  );
  return {
    active,
    databaseHeads,
    localHeads,
    lifecycleEvents,
    getBoardDeliveryHeads,
    scheduler,
    watchdog,
  };
}

describe("PostgreSQL board-delivery-head repository read", () => {
  it("uses one bounded parameterized query and returns heads in requested order", async () => {
    const query = vi.fn((sql: string, values: readonly unknown[]) => {
      void sql;
      void values;
      return Promise.resolve({
        rowCount: 2,
        rows: [
          { id: ids.b, last_delivery_seq: "12" },
          { id: ids.a, last_delivery_seq: "9" },
        ],
      });
    });
    const repository = new BoardRepository({ query } as unknown as DatabasePool);

    await expect(repository.getBoardDeliveryHeads([ids.a, ids.b])).resolves.toEqual([
      { boardId: ids.a, lastDeliverySeq: 9 },
      { boardId: ids.b, lastDeliverySeq: 12 },
    ]);
    expect(query).toHaveBeenCalledOnce();
    expect(query.mock.calls[0]?.[0]).toContain("WHERE id = ANY($1::uuid[])");
    expect(query.mock.calls[0]?.[1]).toEqual([[ids.a, ids.b]]);
  });

  it("rejects missing, malformed, duplicate, empty, and oversized head batches", async () => {
    const missing = new BoardRepository({
      query: vi.fn(() => Promise.resolve({ rowCount: 0, rows: [] })),
    } as unknown as DatabasePool);
    await expect(missing.getBoardDeliveryHeads([ids.a])).rejects.toEqual(
      new BoardDeliveryHeadReadError("MISSING_BOARD_HEAD"),
    );

    const malformed = new BoardRepository({
      query: vi.fn(() =>
        Promise.resolve({ rowCount: 1, rows: [{ id: ids.a, last_delivery_seq: "unsafe" }] }),
      ),
    } as unknown as DatabasePool);
    await expect(malformed.getBoardDeliveryHeads([ids.a])).rejects.toEqual(
      new BoardDeliveryHeadReadError("INVALID_DELIVERY_HEAD"),
    );
    await expect(malformed.getBoardDeliveryHeads([])).rejects.toEqual(
      new BoardDeliveryHeadReadError("INVALID_BOARD_BATCH"),
    );
    await expect(malformed.getBoardDeliveryHeads([ids.a, ids.a])).rejects.toEqual(
      new BoardDeliveryHeadReadError("INVALID_BOARD_BATCH"),
    );
    await expect(
      malformed.getBoardDeliveryHeads(
        Array.from({ length: 101 }, (_, index) => `board-${String(index)}`),
      ),
    ).rejects.toEqual(new BoardDeliveryHeadReadError("INVALID_BOARD_BATCH"));
  });
});

describe("board-delivery-head watchdog core", () => {
  it("does not query or retain state without active boards", async () => {
    const state = harness();
    await state.watchdog.runCheck();
    expect(state.getBoardDeliveryHeads).not.toHaveBeenCalled();
    expect(state.lifecycleEvents).toEqual([]);
    expect(state.watchdog.diagnostics).toEqual({
      activeBoards: 0,
      divergentBoards: 0,
      notifiedBoards: 0,
      recoveryPendingBoards: 0,
      queryInFlight: false,
    });
  });

  it("queries multiple active boards in bounded round-robin batches", async () => {
    const state = harness({ batchSize: 2, maximumActiveBoards: 3 });
    state.active.add(ids.a).add(ids.b).add(ids.c);

    await state.watchdog.runCheck();
    await state.watchdog.runCheck();

    expect(state.getBoardDeliveryHeads).toHaveBeenCalledTimes(2);
    expect(state.getBoardDeliveryHeads).toHaveBeenNthCalledWith(1, [ids.a, ids.b]);
    expect(state.getBoardDeliveryHeads).toHaveBeenNthCalledWith(2, [ids.c, ids.a]);
  });

  it("clears divergence when local delivery catches up inside the default grace", async () => {
    const state = harness();
    state.active.add(ids.a);
    state.databaseHeads.set(ids.a, 1);

    await state.watchdog.runCheck();
    await state.scheduler.advanceBy(4_999);
    state.localHeads.set(ids.a, 1);
    await state.watchdog.runCheck();

    expect(state.lifecycleEvents).toEqual([]);
    expect(state.watchdog.diagnostics.divergentBoards).toBe(0);
  });

  it("preserves first observation as database heads increase and emits unavailable once", async () => {
    const state = harness();
    state.active.add(ids.a).add(ids.b);
    state.databaseHeads.set(ids.a, 1);

    await state.watchdog.runCheck();
    await state.scheduler.advanceBy(4_999);
    state.databaseHeads.set(ids.a, 2);
    await state.watchdog.runCheck();
    expect(state.lifecycleEvents).toEqual([]);

    await state.scheduler.advanceBy(1);
    state.databaseHeads.set(ids.a, 3);
    await state.watchdog.runCheck();
    await state.scheduler.advanceBy(5_000);
    state.databaseHeads.set(ids.a, 4);
    await state.watchdog.runCheck();

    expect(state.lifecycleEvents).toEqual([
      {
        state: "unavailable",
        code: "DELIVERY_HEAD_DIVERGED",
        boardIds: [ids.a],
      },
    ]);
  });

  it("does not let a healthy board hide lag and recovers once after all-board parity", async () => {
    const state = harness();
    state.active.add(ids.a).add(ids.b);
    state.databaseHeads.set(ids.a, 2);
    state.databaseHeads.set(ids.b, 7);
    state.localHeads.set(ids.b, 7);

    await state.watchdog.runCheck();
    await state.scheduler.advanceBy(5_000);
    await state.watchdog.runCheck();
    state.localHeads.set(ids.a, 2);
    await state.watchdog.runCheck();
    await state.watchdog.runCheck();

    expect(state.lifecycleEvents).toEqual([
      {
        state: "unavailable",
        code: "DELIVERY_HEAD_DIVERGED",
        boardIds: [ids.a],
      },
      { state: "recovered" },
    ]);
  });

  it("releases all divergence state when a board leaves the active set", async () => {
    const state = harness();
    state.active.add(ids.a);
    state.databaseHeads.set(ids.a, 1);
    await state.watchdog.runCheck();
    expect(state.watchdog.diagnostics.divergentBoards).toBe(1);

    state.active.delete(ids.a);
    await state.watchdog.runCheck();
    expect(state.getBoardDeliveryHeads).toHaveBeenCalledOnce();
    expect(state.watchdog.diagnostics).toMatchObject({
      activeBoards: 0,
      divergentBoards: 0,
      notifiedBoards: 0,
      recoveryPendingBoards: 0,
    });
  });

  it("rejects over-capacity active snapshots before querying or retaining them", async () => {
    const state = harness({ batchSize: 2, maximumActiveBoards: 2 });
    state.active.add(ids.a).add(ids.b).add(ids.c);

    await expect(state.watchdog.runCheck()).rejects.toEqual(
      new BoardDeliveryHeadWatchdogError("ACTIVE_BOARD_CAPACITY_EXCEEDED"),
    );
    expect(state.getBoardDeliveryHeads).not.toHaveBeenCalled();
    expect(state.watchdog.diagnostics.activeBoards).toBe(0);
    expect(state.lifecycleEvents).toEqual([
      {
        state: "unavailable",
        code: "ACTIVE_BOARD_CAPACITY_EXCEEDED",
        boardIds: [],
      },
    ]);
  });

  it("never treats rejection, missing rows, malformed heads, or timeout as healthy", async () => {
    const failures: Array<() => Promise<readonly BoardDeliveryHead[]>> = [
      () => Promise.reject(new Error("secret SQL failure")),
      () => Promise.resolve([]),
      () => Promise.resolve([{ boardId: ids.a, lastDeliverySeq: Number.NaN }]),
    ];
    for (const failure of failures) {
      const state = harness({}, failure);
      state.active.add(ids.a);
      await expect(state.watchdog.runCheck()).rejects.toEqual(
        new BoardDeliveryHeadWatchdogError("DATABASE_CHECK_FAILED"),
      );
      expect(state.lifecycleEvents).toEqual([
        { state: "unavailable", code: "DATABASE_CHECK_FAILED", boardIds: [] },
      ]);
      expect(JSON.stringify(state.lifecycleEvents)).not.toMatch(/secret|sql/i);
    }

    const pending = deferred<readonly BoardDeliveryHead[]>();
    const timedOut = harness({ queryTimeoutMs: 25 }, () => pending.promise);
    timedOut.active.add(ids.a);
    const check = timedOut.watchdog.runCheck();
    await flush();
    await timedOut.scheduler.advanceBy(25);
    await expect(check).rejects.toEqual(
      new BoardDeliveryHeadWatchdogError("DATABASE_CHECK_TIMEOUT"),
    );
    expect(timedOut.lifecycleEvents).toEqual([
      { state: "unavailable", code: "DATABASE_CHECK_TIMEOUT", boardIds: [] },
    ]);
    expect(timedOut.watchdog.diagnostics.queryInFlight).toBe(true);
    pending.resolve([{ boardId: ids.a, lastDeliverySeq: 0 }]);
    await flush();
    expect(timedOut.watchdog.diagnostics.queryInFlight).toBe(false);
    expect(timedOut.lifecycleEvents).not.toContainEqual({ state: "recovered" });
  });

  it("coalesces overlapping checks and fences a late result after stop", async () => {
    const pending = deferred<readonly BoardDeliveryHead[]>();
    const state = harness({}, () => pending.promise);
    state.active.add(ids.a);
    const first = state.watchdog.runCheck();
    const second = state.watchdog.runCheck();
    expect(second).toBe(first);
    await flush();
    expect(state.getBoardDeliveryHeads).toHaveBeenCalledOnce();

    const firstStop = state.watchdog.stop();
    const secondStop = state.watchdog.stop();
    expect(secondStop).toBe(firstStop);
    await firstStop;
    await expect(first).resolves.toBeUndefined();
    pending.resolve([{ boardId: ids.a, lastDeliverySeq: 10 }]);
    await flush();
    expect(state.lifecycleEvents).toEqual([]);
    expect(state.watchdog.diagnostics).toMatchObject({
      activeBoards: 0,
      divergentBoards: 0,
      notifiedBoards: 0,
      recoveryPendingBoards: 0,
    });
  });

  it("starts and stops idempotently without retaining scheduled work", async () => {
    const state = harness();
    const firstStart = state.watchdog.start();
    const secondStart = state.watchdog.start();
    expect(secondStart).toBe(firstStart);
    await firstStart;
    expect(state.scheduler.pendingTasks).toBe(1);

    const firstStop = state.watchdog.stop();
    const secondStop = state.watchdog.stop();
    expect(secondStop).toBe(firstStop);
    await Promise.all([firstStop, secondStop]);
    await state.watchdog.start();
    expect(state.scheduler.pendingTasks).toBe(0);
    expect(state.getBoardDeliveryHeads).not.toHaveBeenCalled();
  });

  it("rejects invalid timing, capacity, batch, and jitter configuration", () => {
    const invalid: Array<Partial<BoardDeliveryHeadWatchdogConfiguration>> = [
      { intervalMs: 0 },
      { gracePeriodMs: 0 },
      { queryTimeoutMs: 0 },
      { batchSize: 0 },
      { batchSize: 101 },
      { maximumActiveBoards: 0 },
      { batchSize: 2, maximumActiveBoards: 1 },
      { jitterRatio: -0.01 },
      { jitterRatio: 0.21 },
      { intervalMs: 2_147_483_647, jitterRatio: 0.2 },
      { gracePeriodMs: Number.MAX_SAFE_INTEGER },
      { maximumActiveBoards: Number.MAX_SAFE_INTEGER },
    ];
    const repository: BoardDeliveryHeadRepository = {
      getBoardDeliveryHeads: vi.fn(() => Promise.resolve([])),
    };
    for (const override of invalid)
      expect(
        () =>
          new BoardDeliveryHeadWatchdog(
            repository,
            { activeBoardIds: () => [] },
            { handledDeliverySequence: () => 0 },
            { lifecycle: vi.fn() },
            configuration(override),
            new ControlledScheduler(),
          ),
      ).toThrow(new BoardDeliveryHeadWatchdogError("INVALID_CONFIGURATION"));
  });
});
