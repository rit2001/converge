import { BOARD_DELIVERY_HEAD_QUERY_MAXIMUM, type BoardDeliveryHead } from "@converge/database";

const TIMER_MAXIMUM_MS = 2_147_483_647;
const ACTIVE_BOARD_MAXIMUM = 100_000;

export interface BoardDeliveryHeadRepository {
  getBoardDeliveryHeads(boardIds: readonly string[]): Promise<readonly BoardDeliveryHead[]>;
}

export interface ActiveBoardProvider {
  activeBoardIds(): Iterable<string>;
}

export interface DeliveryProgressProvider {
  handledDeliverySequence(boardId: string): number;
}

export type BoardDeliveryHeadWatchdogLifecycleEvent =
  | {
      state: "unavailable";
      code:
        | "DELIVERY_HEAD_DIVERGED"
        | "DATABASE_CHECK_FAILED"
        | "DATABASE_CHECK_TIMEOUT"
        | "ACTIVE_BOARD_CAPACITY_EXCEEDED";
      boardIds: readonly string[];
    }
  | { state: "recovered" };

export interface BoardDeliveryHeadWatchdogObserver {
  lifecycle(event: BoardDeliveryHeadWatchdogLifecycleEvent): Promise<void> | void;
}

export interface BoardDeliveryHeadWatchdogOwner {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export interface BoardDeliveryHeadWatchdogFactoryInput {
  repository: BoardDeliveryHeadRepository;
  activeBoards: ActiveBoardProvider;
  deliveryProgress: DeliveryProgressProvider;
  observer: BoardDeliveryHeadWatchdogObserver;
}

export type BoardDeliveryHeadWatchdogFactory = (
  input: BoardDeliveryHeadWatchdogFactoryInput,
) => BoardDeliveryHeadWatchdogOwner;

export interface BoardDeliveryHeadWatchdogScheduler {
  now(): number;
  random(): number;
  wait(delayMs: number, signal: AbortSignal): Promise<void>;
}

export const systemBoardDeliveryHeadWatchdogScheduler: BoardDeliveryHeadWatchdogScheduler = {
  now: Date.now,
  random: Math.random,
  wait: (delayMs, signal) =>
    new Promise((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      let settled = false;
      const settle = (): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        signal.removeEventListener("abort", settle);
        resolve();
      };
      const timeout = setTimeout(settle, delayMs);
      signal.addEventListener("abort", settle, { once: true });
      if (signal.aborted) settle();
    }),
};

export interface BoardDeliveryHeadWatchdogConfiguration {
  intervalMs: number;
  gracePeriodMs: number;
  queryTimeoutMs: number;
  batchSize: number;
  maximumActiveBoards: number;
  jitterRatio: number;
}

export const defaultBoardDeliveryHeadWatchdogConfiguration: Readonly<BoardDeliveryHeadWatchdogConfiguration> =
  {
    intervalMs: 5_000,
    gracePeriodMs: 5_000,
    queryTimeoutMs: 5_000,
    batchSize: BOARD_DELIVERY_HEAD_QUERY_MAXIMUM,
    maximumActiveBoards: 1_000,
    jitterRatio: 0.2,
  };

export class BoardDeliveryHeadWatchdogError extends Error {
  constructor(
    public readonly code:
      | "INVALID_CONFIGURATION"
      | "ACTIVE_BOARD_CAPACITY_EXCEEDED"
      | "DATABASE_CHECK_FAILED"
      | "DATABASE_CHECK_TIMEOUT",
  ) {
    super(`Board delivery-head watchdog failed: ${code}`);
  }
}

interface DivergenceState {
  firstObservedAt: number;
}

function isPositiveSafeInteger(value: number, maximum = Number.MAX_SAFE_INTEGER): boolean {
  return Number.isSafeInteger(value) && value > 0 && value <= maximum;
}

function validateConfiguration(configuration: BoardDeliveryHeadWatchdogConfiguration): void {
  if (
    !isPositiveSafeInteger(configuration.intervalMs, TIMER_MAXIMUM_MS) ||
    !isPositiveSafeInteger(configuration.gracePeriodMs, TIMER_MAXIMUM_MS) ||
    !isPositiveSafeInteger(configuration.queryTimeoutMs, TIMER_MAXIMUM_MS) ||
    !isPositiveSafeInteger(configuration.batchSize, BOARD_DELIVERY_HEAD_QUERY_MAXIMUM) ||
    !isPositiveSafeInteger(configuration.maximumActiveBoards, ACTIVE_BOARD_MAXIMUM) ||
    configuration.maximumActiveBoards < configuration.batchSize ||
    !Number.isFinite(configuration.jitterRatio) ||
    configuration.jitterRatio < 0 ||
    configuration.jitterRatio > 0.2 ||
    configuration.intervalMs * (1 + configuration.jitterRatio) > TIMER_MAXIMUM_MS
  )
    throw new BoardDeliveryHeadWatchdogError("INVALID_CONFIGURATION");
}

function validSequence(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

export class BoardDeliveryHeadWatchdog {
  private readonly divergences = new Map<string, DivergenceState>();
  private readonly notifiedDivergences = new Set<string>();
  private readonly knownActiveBoards = new Set<string>();
  private readonly recoveryPendingBoards = new Set<string>();
  private readonly loopController = new AbortController();
  private activeCheckController: AbortController | undefined;
  private repositoryInFlight: Promise<unknown> | undefined;
  private checkPromise: Promise<void> | undefined;
  private loopPromise: Promise<void> | undefined;
  private startPromise: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private roundRobinOffset = 0;
  private generation = 0;
  private unavailable = false;
  private databaseFailureNotified = false;
  private stopped = false;

  constructor(
    private readonly repository: BoardDeliveryHeadRepository,
    private readonly activeBoards: ActiveBoardProvider,
    private readonly deliveryProgress: DeliveryProgressProvider,
    private readonly observer: BoardDeliveryHeadWatchdogObserver,
    private readonly configuration: BoardDeliveryHeadWatchdogConfiguration = {
      ...defaultBoardDeliveryHeadWatchdogConfiguration,
    },
    private readonly scheduler: BoardDeliveryHeadWatchdogScheduler = systemBoardDeliveryHeadWatchdogScheduler,
  ) {
    validateConfiguration(configuration);
  }

  get diagnostics(): Readonly<{
    activeBoards: number;
    divergentBoards: number;
    notifiedBoards: number;
    recoveryPendingBoards: number;
    queryInFlight: boolean;
  }> {
    return {
      activeBoards: this.knownActiveBoards.size,
      divergentBoards: this.divergences.size,
      notifiedBoards: this.notifiedDivergences.size,
      recoveryPendingBoards: this.recoveryPendingBoards.size,
      queryInFlight: this.repositoryInFlight !== undefined,
    };
  }

  start(): Promise<void> {
    this.startPromise ??= Promise.resolve().then(() => {
      if (this.stopped || this.loopPromise) return;
      const generation = ++this.generation;
      this.loopPromise = this.runLoop(generation, this.loopController.signal);
    });
    return this.startPromise;
  }

  stop(): Promise<void> {
    this.stopPromise ??= this.stopOnce();
    return this.stopPromise;
  }

  runCheck(): Promise<void> {
    if (this.stopped) return Promise.resolve();
    if (this.checkPromise) return this.checkPromise;
    const generation = this.generation;
    const check = this.executeCheck(generation);
    this.checkPromise = check;
    void check.then(
      () => {
        if (this.checkPromise === check) this.checkPromise = undefined;
      },
      () => {
        if (this.checkPromise === check) this.checkPromise = undefined;
      },
    );
    return check;
  }

  private async runLoop(generation: number, signal: AbortSignal): Promise<void> {
    while (this.isCurrent(generation) && !signal.aborted) {
      await this.scheduler.wait(this.nextInterval(), signal);
      if (!this.isCurrent(generation) || signal.aborted) return;
      await this.runCheck().catch(() => undefined);
    }
  }

  private nextInterval(): number {
    const random = this.scheduler.random();
    if (!Number.isFinite(random) || random < 0 || random >= 1)
      throw new BoardDeliveryHeadWatchdogError("INVALID_CONFIGURATION");
    const factor = 1 + (random * 2 - 1) * this.configuration.jitterRatio;
    return Math.max(1, Math.round(this.configuration.intervalMs * factor));
  }

  private async executeCheck(generation: number): Promise<void> {
    if (!this.isCurrent(generation) || this.repositoryInFlight) return;
    let active: string[];
    try {
      active = this.collectActiveBoards();
    } catch (error) {
      if (
        error instanceof BoardDeliveryHeadWatchdogError &&
        error.code === "ACTIVE_BOARD_CAPACITY_EXCEEDED"
      ) {
        await this.notifyDatabaseFailure(
          "ACTIVE_BOARD_CAPACITY_EXCEEDED",
          this.knownActiveBoards,
          generation,
        );
      }
      throw error;
    }
    if (!this.isCurrent(generation)) return;
    this.reconcileActiveBoards(active);
    if (active.length === 0) {
      if (this.unavailable && !this.databaseFailureNotified) {
        await this.notify({ state: "recovered" }, generation);
        if (this.isCurrent(generation)) {
          this.unavailable = false;
          this.databaseFailureNotified = false;
        }
      }
      return;
    }

    const batch = this.selectBatch(active);
    const checkController = new AbortController();
    this.activeCheckController = checkController;
    const query = Promise.resolve()
      .then(() => this.repository.getBoardDeliveryHeads(batch))
      .then(
        (heads) => ({ state: "fulfilled" as const, heads }),
        () => ({ state: "rejected" as const }),
      );
    this.repositoryInFlight = query;
    void query.then(() => {
      if (this.repositoryInFlight === query) this.repositoryInFlight = undefined;
    });
    const timeout = this.scheduler
      .wait(this.configuration.queryTimeoutMs, checkController.signal)
      .then((): "cancelled" | "timeout" =>
        checkController.signal.aborted ? "cancelled" : "timeout",
      );
    try {
      const result = await Promise.race([query, timeout]);
      if (!this.isCurrent(generation) || result === "cancelled") return;
      if (result === "timeout") {
        await this.notifyDatabaseFailure("DATABASE_CHECK_TIMEOUT", active, generation);
        throw new BoardDeliveryHeadWatchdogError("DATABASE_CHECK_TIMEOUT");
      }
      if (result.state === "rejected") {
        await this.notifyDatabaseFailure("DATABASE_CHECK_FAILED", active, generation);
        throw new BoardDeliveryHeadWatchdogError("DATABASE_CHECK_FAILED");
      }
      await this.applySuccessfulCheck(active, batch, result.heads, generation);
    } finally {
      checkController.abort();
      if (this.activeCheckController === checkController) this.activeCheckController = undefined;
    }
  }

  private collectActiveBoards(): string[] {
    const active: string[] = [];
    const seen = new Set<string>();
    for (const boardId of this.activeBoards.activeBoardIds()) {
      if (typeof boardId !== "string" || boardId.length === 0)
        throw new BoardDeliveryHeadWatchdogError("DATABASE_CHECK_FAILED");
      if (seen.has(boardId)) continue;
      if (seen.size >= this.configuration.maximumActiveBoards)
        throw new BoardDeliveryHeadWatchdogError("ACTIVE_BOARD_CAPACITY_EXCEEDED");
      seen.add(boardId);
      active.push(boardId);
    }
    return active;
  }

  private reconcileActiveBoards(active: readonly string[]): void {
    const current = new Set(active);
    for (const boardId of this.knownActiveBoards) {
      if (current.has(boardId)) continue;
      this.divergences.delete(boardId);
      this.notifiedDivergences.delete(boardId);
      this.recoveryPendingBoards.delete(boardId);
    }
    if (this.unavailable)
      for (const boardId of current)
        if (!this.knownActiveBoards.has(boardId)) this.recoveryPendingBoards.add(boardId);
    this.knownActiveBoards.clear();
    for (const boardId of current) this.knownActiveBoards.add(boardId);
    if (active.length === 0) this.roundRobinOffset = 0;
    else this.roundRobinOffset %= active.length;
  }

  private selectBatch(active: readonly string[]): string[] {
    const count = Math.min(active.length, this.configuration.batchSize);
    const selected = Array.from(
      { length: count },
      (_, index) => active[(this.roundRobinOffset + index) % active.length]!,
    );
    this.roundRobinOffset = (this.roundRobinOffset + count) % active.length;
    return selected;
  }

  private async applySuccessfulCheck(
    active: readonly string[],
    batch: readonly string[],
    heads: readonly BoardDeliveryHead[],
    generation: number,
  ): Promise<void> {
    const expected = new Set(batch);
    const observed = new Map<string, { database: number; local: number }>();
    for (const head of heads) {
      if (
        !expected.has(head.boardId) ||
        observed.has(head.boardId) ||
        !validSequence(head.lastDeliverySeq)
      )
        return this.failMalformedCheck(active, generation);
      const local = this.deliveryProgress.handledDeliverySequence(head.boardId);
      if (!validSequence(local)) return this.failMalformedCheck(active, generation);
      observed.set(head.boardId, { database: head.lastDeliverySeq, local });
    }
    if (observed.size !== expected.size) return this.failMalformedCheck(active, generation);

    const now = this.scheduler.now();
    if (!Number.isSafeInteger(now) || now < 0)
      throw new BoardDeliveryHeadWatchdogError("INVALID_CONFIGURATION");
    const newlyUnavailable: string[] = [];
    for (const boardId of batch) {
      const head = observed.get(boardId)!;
      if (head.database > head.local) {
        const divergence = this.divergences.get(boardId) ?? { firstObservedAt: now };
        this.divergences.set(boardId, divergence);
        this.recoveryPendingBoards.add(boardId);
        if (
          now - divergence.firstObservedAt >= this.configuration.gracePeriodMs &&
          !this.notifiedDivergences.has(boardId)
        ) {
          this.notifiedDivergences.add(boardId);
          newlyUnavailable.push(boardId);
        }
      } else {
        this.divergences.delete(boardId);
        this.notifiedDivergences.delete(boardId);
        this.recoveryPendingBoards.delete(boardId);
      }
    }
    if (newlyUnavailable.length > 0) {
      this.markRecoveryRequired(active);
      for (const boardId of batch) {
        const head = observed.get(boardId)!;
        if (head.database <= head.local) this.recoveryPendingBoards.delete(boardId);
      }
      this.unavailable = true;
      await this.notify(
        {
          state: "unavailable",
          code: "DELIVERY_HEAD_DIVERGED",
          boardIds: Object.freeze([...newlyUnavailable]),
        },
        generation,
      );
    }
    if (this.unavailable && this.divergences.size === 0 && this.recoveryPendingBoards.size === 0) {
      await this.notify({ state: "recovered" }, generation);
      if (!this.isCurrent(generation)) return;
      this.unavailable = false;
      this.databaseFailureNotified = false;
    }
  }

  private async failMalformedCheck(active: readonly string[], generation: number): Promise<void> {
    await this.notifyDatabaseFailure("DATABASE_CHECK_FAILED", active, generation);
    throw new BoardDeliveryHeadWatchdogError("DATABASE_CHECK_FAILED");
  }

  private async notifyDatabaseFailure(
    code: "DATABASE_CHECK_FAILED" | "DATABASE_CHECK_TIMEOUT" | "ACTIVE_BOARD_CAPACITY_EXCEEDED",
    active: Iterable<string>,
    generation: number,
  ): Promise<void> {
    this.markRecoveryRequired(active);
    this.unavailable = true;
    if (this.databaseFailureNotified) return;
    this.databaseFailureNotified = true;
    await this.notify({ state: "unavailable", code, boardIds: Object.freeze([]) }, generation);
  }

  private markRecoveryRequired(active: Iterable<string>): void {
    this.recoveryPendingBoards.clear();
    for (const boardId of active) this.recoveryPendingBoards.add(boardId);
  }

  private async notify(
    event: BoardDeliveryHeadWatchdogLifecycleEvent,
    generation: number,
  ): Promise<void> {
    if (!this.isCurrent(generation)) return;
    await this.observer.lifecycle(event);
  }

  private isCurrent(generation: number): boolean {
    return !this.stopped && generation === this.generation;
  }

  private async stopOnce(): Promise<void> {
    this.stopped = true;
    this.generation += 1;
    this.loopController.abort();
    this.activeCheckController?.abort();
    await this.loopPromise?.catch(() => undefined);
    this.divergences.clear();
    this.notifiedDivergences.clear();
    this.knownActiveBoards.clear();
    this.recoveryPendingBoards.clear();
    this.repositoryInFlight = undefined;
    this.unavailable = false;
    this.databaseFailureNotified = false;
    this.roundRobinOffset = 0;
  }
}
