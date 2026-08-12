import { describe, expect, it, vi } from "vitest";
import type { StructuredLogger } from "@converge/observability";
import type { DeliveryStreamFields } from "@converge/protocol";
import {
  createWorkerApplication,
  type CompactionCoordinatorComponent,
  type OutboxPublisherComponent,
  type SnapshotCoordinatorComponent,
  type WorkerApplicationDatabase,
  type WorkerApplicationFactories,
} from "./application.js";
import type {
  CompactionCandidateDiscoveryRepository,
  CompactionExecutionRepository,
} from "./compaction-coordinator.js";
import { parseWorkerEnvironment } from "./env.js";
import type { DeliveryStream } from "./redis-stream.js";

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

const logger: StructuredLogger = {
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
};

class FakeDatabase implements WorkerApplicationDatabase {
  queryCalls: string[] = [];
  endCalls = 0;
  queryFailure: Error | undefined;

  query(statement: string): Promise<unknown> {
    this.queryCalls.push(statement);
    return this.queryFailure ? Promise.reject(this.queryFailure) : Promise.resolve({ rows: [] });
  }

  end(): Promise<void> {
    this.endCalls += 1;
    return Promise.resolve();
  }
}

class FakeSnapshots implements SnapshotCoordinatorComponent {
  startCalls = 0;
  stopCalls = 0;
  captures = 0;
  failures = 0;
  running = false;

  start(): Promise<void> {
    this.startCalls += 1;
    this.running = true;
    return Promise.resolve();
  }

  stop(): Promise<void> {
    this.stopCalls += 1;
    this.running = false;
    return Promise.resolve();
  }

  capture(): void {
    if (!this.running) throw new Error("snapshot coordinator is stopped");
    this.captures += 1;
  }

  failCapture(): void {
    if (!this.running) throw new Error("snapshot coordinator is stopped");
    this.failures += 1;
  }
}

class FakeCompaction implements CompactionCoordinatorComponent {
  startCalls = 0;
  stopCalls = 0;
  cycles = 0;
  failures = 0;
  running = false;
  startFailure: Error | undefined;

  start(): Promise<void> {
    this.startCalls += 1;
    if (this.startFailure) return Promise.reject(this.startFailure);
    this.running = true;
    return Promise.resolve();
  }

  stop(): Promise<void> {
    this.stopCalls += 1;
    this.running = false;
    return Promise.resolve();
  }

  runCycle(): void {
    if (!this.running) throw new Error("compaction coordinator is stopped");
    this.cycles += 1;
  }

  failCycle(): void {
    if (!this.running) throw new Error("compaction coordinator is stopped");
    this.failures += 1;
  }
}

class FakeStream implements DeliveryStream {
  ready = false;
  connectCalls = 0;
  closeCalls: boolean[] = [];
  connectOutcomes: Array<"ready" | Error> = [];

  connect(): Promise<void> {
    this.connectCalls += 1;
    const outcome = this.connectOutcomes.shift() ?? "ready";
    if (outcome instanceof Error) return Promise.reject(outcome);
    this.ready = true;
    return Promise.resolve();
  }

  isReady(): boolean {
    return this.ready;
  }

  append(_fields: DeliveryStreamFields, _signal: AbortSignal): Promise<unknown> {
    void _fields;
    void _signal;
    return Promise.resolve("1-0");
  }

  trimByAge(_signal: AbortSignal): Promise<void> {
    void _signal;
    return Promise.resolve();
  }

  resetAfterCommandTimeout(): Promise<void> {
    return Promise.resolve();
  }

  close(force = false): Promise<void> {
    this.closeCalls.push(force);
    this.ready = false;
    return Promise.resolve();
  }
}

class FakeOutbox implements OutboxPublisherComponent {
  runCalls = 0;
  stopCalls = 0;
  drainCalls: number[] = [];
  abandonCalls = 0;
  claims = 0;
  drainResult = true;
  runFailure: Error | undefined;
  private stream: FakeStream | undefined;

  attach(stream: FakeStream): void {
    this.stream = stream;
  }

  run(signal: AbortSignal): Promise<void> {
    this.runCalls += 1;
    if (this.runFailure) return Promise.reject(this.runFailure);
    return new Promise((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      signal.addEventListener("abort", () => resolve(), { once: true });
    });
  }

  claimCycle(): void {
    if (this.stream?.isReady()) this.claims += 1;
  }

  stopTakingClaims(): void {
    this.stopCalls += 1;
  }

  abandonActiveLeases(): void {
    this.abandonCalls += 1;
  }

  drain(gracePeriodMs: number): Promise<boolean> {
    this.drainCalls.push(gracePeriodMs);
    return Promise.resolve(this.drainResult);
  }
}

class ControlledReconnectScheduler {
  delays: number[] = [];
  private waits: Array<() => void> = [];

  sleep(delayMs: number, signal?: AbortSignal): Promise<void> {
    this.delays.push(delayMs);
    return new Promise((resolve) => {
      if (signal?.aborted) {
        resolve();
        return;
      }
      const settle = (): void => {
        signal?.removeEventListener("abort", settle);
        resolve();
      };
      this.waits.push(settle);
      signal?.addEventListener("abort", settle, { once: true });
    });
  }

  async continue(): Promise<void> {
    this.waits.shift()?.();
    await flush();
  }
}

function harness(stream = new FakeStream(), compactionEnabled = false) {
  const database = new FakeDatabase();
  const snapshots = new FakeSnapshots();
  const compaction = new FakeCompaction();
  const outbox = new FakeOutbox();
  const scheduler = new ControlledReconnectScheduler();
  outbox.attach(stream);
  const snapshotConfigurations: unknown[] = [];
  const compactionConfigurations: unknown[] = [];
  const compactionCandidateRepository = {} as CompactionCandidateDiscoveryRepository;
  const boardCompactionRepository = {} as CompactionExecutionRepository;
  let candidateRepositoryConstructions = 0;
  let boardCompactionRepositoryConstructions = 0;
  let compactionCoordinatorConstructions = 0;
  const factories: Partial<WorkerApplicationFactories> = {
    createLogger: () => logger,
    createDatabase: () => database,
    createSnapshotCoordinator: ({ configuration }) => {
      snapshotConfigurations.push(configuration);
      return snapshots;
    },
    createCompactionCandidateRepository: () => {
      candidateRepositoryConstructions += 1;
      return compactionCandidateRepository;
    },
    createBoardCompactionRepository: () => {
      boardCompactionRepositoryConstructions += 1;
      return boardCompactionRepository;
    },
    createCompactionCoordinator: ({ candidates, compaction: repository, configuration }) => {
      expect(candidates).toBe(compactionCandidateRepository);
      expect(repository).toBe(boardCompactionRepository);
      compactionCoordinatorConstructions += 1;
      compactionConfigurations.push(configuration);
      return compaction;
    },
    createStream: () => stream,
    createOutboxPublisher: () => outbox,
    reconnectScheduler: scheduler,
  };
  const environment = parseWorkerEnvironment({
    DATABASE_URL: "postgresql://converge:secret@localhost:55432/converge",
    COMPACTION_ENABLED: String(compactionEnabled),
  });
  return {
    database,
    snapshots,
    compaction,
    stream,
    outbox,
    scheduler,
    factories,
    environment,
    snapshotConfigurations,
    compactionConfigurations,
    constructions: () => ({
      candidates: candidateRepositoryConstructions,
      repository: boardCompactionRepositoryConstructions,
      coordinator: compactionCoordinatorConstructions,
    }),
  };
}

describe("worker application supervision", () => {
  it("starts snapshots before Redis and runs both components when Redis is ready", async () => {
    const state = harness();
    state.stream.connectOutcomes.push("ready");
    const application = await createWorkerApplication(state.environment, state.factories);

    await application.start();
    await flush();

    expect(state.database.queryCalls).toEqual(["SELECT 1"]);
    expect(state.snapshots.startCalls).toBe(1);
    expect(state.stream.connectCalls).toBe(1);
    expect(state.outbox.runCalls).toBe(1);
    state.outbox.claimCycle();
    expect(state.outbox.claims).toBe(1);
    expect(state.snapshotConfigurations[0]).toMatchObject({
      pollIntervalMs: 30_000,
      pollJitterPercent: 20,
      candidateScanLimit: 100,
      candidateLimit: 16,
      maximumConcurrency: 2,
      operationThreshold: 1_000,
      maximumPayloadBytes: 16_777_216,
      shutdownGraceMs: 10_000,
    });
    await application.shutdown("PROCESS_END");
  });

  it("leaves compaction entirely unconstructed and unstarted by default", async () => {
    const state = harness();
    const application = await createWorkerApplication(state.environment, state.factories);
    await application.start();
    expect(state.constructions()).toEqual({ candidates: 0, repository: 0, coordinator: 0 });
    expect(state.compaction.startCalls).toBe(0);
    await application.shutdown("PROCESS_END");
    expect(state.compaction.stopCalls).toBe(0);
  });

  it("constructs and starts each compaction component once with validated configuration", async () => {
    const state = harness(new FakeStream(), true);
    const application = await createWorkerApplication(state.environment, state.factories);
    expect(state.constructions()).toEqual({ candidates: 1, repository: 1, coordinator: 1 });
    await application.start();
    expect(state.compaction.startCalls).toBe(1);
    expect(state.compactionConfigurations).toEqual([
      {
        pollIntervalMs: 300_000,
        pollJitterPercent: 20,
        candidateScanLimit: 100,
        candidateResultLimit: 16,
        maximumConcurrency: 2,
        retryBaseMs: 5_000,
        retryCapMs: 300_000,
        retainedStateLimit: 1_000,
        shutdownGraceMs: 10_000,
      },
    ]);
    await application.shutdown("PROCESS_END");
  });

  it("keeps snapshots running and outbox claims at zero across Redis startup failure", async () => {
    const state = harness(new FakeStream(), true);
    state.stream.connectOutcomes.push(new Error("redis unavailable"));
    const application = await createWorkerApplication(state.environment, state.factories);

    await application.start();
    await flush();
    state.outbox.claimCycle();
    state.snapshots.capture();
    state.compaction.runCycle();

    expect(state.snapshots.running).toBe(true);
    expect(state.snapshots.captures).toBe(1);
    expect(state.compaction.cycles).toBe(1);
    expect(state.outbox.claims).toBe(0);
    expect(state.scheduler.delays).toEqual([250]);
    expect(JSON.stringify((logger.warn as ReturnType<typeof vi.fn>).mock.calls)).not.toContain(
      "redis unavailable",
    );
    await application.shutdown("PROCESS_END");
  });

  it("reconnects Redis without restarting snapshots and then permits outbox work", async () => {
    const state = harness();
    state.stream.connectOutcomes.push(new Error("first failure"), "ready");
    const application = await createWorkerApplication(state.environment, state.factories);
    await application.start();
    await flush();

    expect(state.stream.connectCalls).toBe(1);
    await state.scheduler.continue();
    expect(state.stream.connectCalls).toBe(2);
    expect(state.snapshots.startCalls).toBe(1);
    state.outbox.claimCycle();
    expect(state.outbox.claims).toBe(1);
    await application.shutdown("PROCESS_END");
  });

  it("isolates snapshot and outbox runtime failures", async () => {
    const snapshotFailure = harness(new FakeStream(), true);
    const firstApplication = await createWorkerApplication(
      snapshotFailure.environment,
      snapshotFailure.factories,
    );
    await firstApplication.start();
    snapshotFailure.snapshots.failCapture();
    expect(snapshotFailure.compaction.running).toBe(true);
    expect(snapshotFailure.outbox.runCalls).toBe(1);
    expect(snapshotFailure.outbox.stopCalls).toBe(0);
    await firstApplication.shutdown("PROCESS_END");

    const outboxFailure = harness(new FakeStream(), true);
    outboxFailure.outbox.runFailure = new Error("publisher failed");
    const secondApplication = await createWorkerApplication(
      outboxFailure.environment,
      outboxFailure.factories,
    );
    await secondApplication.start();
    await flush();
    expect(outboxFailure.snapshots.running).toBe(true);
    expect(outboxFailure.compaction.running).toBe(true);
    expect(outboxFailure.snapshots.stopCalls).toBe(0);
    await secondApplication.shutdown("PROCESS_END");
  });

  it("isolates compaction runtime failure from snapshots and outbox", async () => {
    const state = harness(new FakeStream(), true);
    const application = await createWorkerApplication(state.environment, state.factories);
    await application.start();
    state.compaction.failCycle();
    expect(state.snapshots.running).toBe(true);
    expect(state.outbox.runCalls).toBe(1);
    expect(state.outbox.stopCalls).toBe(0);
    await application.shutdown("PROCESS_END");
  });

  it("treats PostgreSQL startup failure as fatal and cleans every resource once", async () => {
    const state = harness(new FakeStream(), true);
    state.database.queryFailure = new Error("database unavailable");
    const application = await createWorkerApplication(state.environment, state.factories);

    await expect(application.start()).rejects.toThrow("database unavailable");
    expect(state.snapshots.startCalls).toBe(0);
    expect(state.compaction.startCalls).toBe(0);
    expect(state.snapshots.stopCalls).toBe(1);
    expect(state.compaction.stopCalls).toBe(1);
    expect(state.outbox.stopCalls).toBe(1);
    expect(state.stream.closeCalls).toEqual([false]);
    expect(state.database.endCalls).toBe(1);
    await application.shutdown("SIGTERM");
    expect(state.stream.closeCalls).toHaveLength(1);
    expect(state.database.endCalls).toBe(1);
  });

  it("stops all components within one grace and shares repeated shutdown", async () => {
    const state = harness(new FakeStream(), true);
    state.outbox.drainResult = false;
    const application = await createWorkerApplication(state.environment, state.factories);
    await application.start();
    await flush();

    const first = application.shutdown("SIGINT");
    expect(application.shutdown("SIGTERM")).toBe(first);
    await first;

    expect(state.snapshots.stopCalls).toBe(1);
    expect(state.compaction.stopCalls).toBe(1);
    expect(state.outbox.stopCalls).toBe(1);
    expect(state.outbox.drainCalls).toEqual([10_000]);
    expect(state.outbox.abandonCalls).toBe(1);
    expect(state.stream.closeCalls).toEqual([true]);
    expect(state.database.endCalls).toBe(1);
    expect(state.scheduler.delays).toEqual([250]);
  });

  it("cleans previously started components after compaction startup failure", async () => {
    const state = harness(new FakeStream(), true);
    state.compaction.startFailure = new Error("compaction startup failed");
    const application = await createWorkerApplication(state.environment, state.factories);
    await expect(application.start()).rejects.toThrow("compaction startup failed");
    expect(state.snapshots.startCalls).toBe(1);
    expect(state.snapshots.stopCalls).toBe(1);
    expect(state.compaction.startCalls).toBe(1);
    expect(state.compaction.stopCalls).toBe(1);
    expect(state.outbox.runCalls).toBe(0);
    expect(state.stream.closeCalls).toEqual([false]);
    expect(state.database.endCalls).toBe(1);
  });

  it("cleans partially constructed resources when a later factory fails", async () => {
    const state = harness(new FakeStream(), true);
    state.factories.createOutboxPublisher = () => {
      throw new Error("publisher construction failed");
    };

    await expect(createWorkerApplication(state.environment, state.factories)).rejects.toThrow(
      "publisher construction failed",
    );
    expect(state.snapshots.stopCalls).toBe(1);
    expect(state.compaction.stopCalls).toBe(1);
    expect(state.stream.closeCalls).toEqual([true]);
    expect(state.database.endCalls).toBe(1);
  });

  it("does not silently disable requested compaction when its construction fails", async () => {
    const state = harness(new FakeStream(), true);
    state.factories.createCompactionCoordinator = () => {
      throw new Error("compaction construction failed");
    };

    await expect(createWorkerApplication(state.environment, state.factories)).rejects.toThrow(
      "compaction construction failed",
    );
    expect(state.constructions()).toEqual({ candidates: 1, repository: 1, coordinator: 0 });
    expect(state.snapshots.startCalls).toBe(0);
    expect(state.snapshots.stopCalls).toBe(1);
    expect(state.compaction.startCalls).toBe(0);
    expect(state.compaction.stopCalls).toBe(0);
    expect(state.stream.closeCalls).toEqual([]);
    expect(state.database.endCalls).toBe(1);
  });
});
