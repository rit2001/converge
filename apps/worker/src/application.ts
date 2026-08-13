import {
  BoardCompactionCandidateRepository,
  BoardCompactionRepository,
  BoardSnapshotCandidateRepository,
  OutboxRepository,
  createPool,
  type DatabasePool,
} from "@converge/database";
import {
  noOpTelemetryRecorder,
  safeTelemetryRecorder,
  type StructuredLogger,
  type TelemetryRecorder,
} from "@converge/observability";
import type { WorkerEnvironment } from "./env.js";
import {
  CompactionCoordinator,
  type CompactionCandidateDiscoveryRepository,
  type CompactionCoordinatorConfiguration,
  type CompactionExecutionRepository,
} from "./compaction-coordinator.js";
import { createWorkerLogger } from "./logger.js";
import { RedisDeliveryStream, type DeliveryStream } from "./redis-stream.js";
import {
  SnapshotCoordinator,
  type SnapshotCoordinatorConfiguration,
} from "./snapshot-coordinator.js";
import { systemWorkerTelemetryClock, type WorkerTelemetryClock } from "./telemetry.js";
import {
  OutboxWorker,
  systemWorkerScheduler,
  type OutboxWorkerConfiguration,
  type WorkerScheduler,
} from "./worker.js";

export interface WorkerApplicationDatabase {
  query(statement: string): Promise<unknown>;
  end(): Promise<void>;
}

export interface SnapshotCoordinatorComponent {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export interface CompactionCoordinatorComponent {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export interface OutboxPublisherComponent {
  run(signal: AbortSignal): Promise<void>;
  stopTakingClaims(): void;
  abandonActiveLeases(): void;
  drain(gracePeriodMs: number): Promise<boolean>;
}

export interface WorkerApplicationFactories {
  createLogger(level: WorkerEnvironment["LOG_LEVEL"]): StructuredLogger;
  createDatabase(databaseUrl: string): WorkerApplicationDatabase;
  createSnapshotCoordinator(input: {
    database: WorkerApplicationDatabase;
    configuration: SnapshotCoordinatorConfiguration;
    logger: StructuredLogger;
    telemetry: TelemetryRecorder;
    telemetryClock: WorkerTelemetryClock;
  }): SnapshotCoordinatorComponent;
  createCompactionCandidateRepository(
    database: WorkerApplicationDatabase,
  ): CompactionCandidateDiscoveryRepository;
  createBoardCompactionRepository(
    database: WorkerApplicationDatabase,
  ): CompactionExecutionRepository;
  createCompactionCoordinator(input: {
    candidates: CompactionCandidateDiscoveryRepository;
    compaction: CompactionExecutionRepository;
    configuration: CompactionCoordinatorConfiguration;
    logger: StructuredLogger;
    telemetry: TelemetryRecorder;
    telemetryClock: WorkerTelemetryClock;
  }): CompactionCoordinatorComponent;
  createStream(input: { environment: WorkerEnvironment; logger: StructuredLogger }): DeliveryStream;
  createOutboxPublisher(input: {
    database: WorkerApplicationDatabase;
    stream: DeliveryStream;
    configuration: OutboxWorkerConfiguration;
    logger: StructuredLogger;
    telemetry: TelemetryRecorder;
    telemetryClock: WorkerTelemetryClock;
  }): OutboxPublisherComponent;
  reconnectScheduler: Pick<WorkerScheduler, "sleep">;
}

const productionWorkerApplicationFactories: WorkerApplicationFactories = {
  createLogger: createWorkerLogger,
  createDatabase: createPool,
  createSnapshotCoordinator: ({ database, configuration, logger, telemetry, telemetryClock }) =>
    new SnapshotCoordinator({
      repository: new BoardSnapshotCandidateRepository(database as DatabasePool),
      configuration,
      telemetry,
      telemetryClock,
      hooks: {
        captured: (snapshot) =>
          logger.info(
            { component: "snapshot", ...snapshot, outcome: "captured" },
            "Board snapshot captured",
          ),
        deterministicFailure: (failure) =>
          logger.error(
            { component: "snapshot", ...failure, outcome: "deterministic_failure" },
            "Board snapshot capture was rejected",
          ),
      },
    }),
  createCompactionCandidateRepository: (database) =>
    new BoardCompactionCandidateRepository(database as DatabasePool),
  createBoardCompactionRepository: (database) =>
    new BoardCompactionRepository(database as DatabasePool),
  createCompactionCoordinator: ({
    candidates,
    compaction,
    configuration,
    logger,
    telemetry,
    telemetryClock,
  }) =>
    new CompactionCoordinator({
      candidates,
      compaction,
      configuration,
      telemetry,
      telemetryClock,
      hooks: {
        compacted: (result) =>
          logger.info(
            { component: "compaction", ...result, outcome: "compacted" },
            "Board history compacted",
          ),
        blocked: (result) =>
          logger.error(
            { component: "compaction", ...result, outcome: "blocked" },
            "Board compaction was blocked",
          ),
      },
    }),
  createStream: ({ environment, logger }) =>
    new RedisDeliveryStream(
      environment.REDIS_URL,
      environment.REDIS_STREAM_KEY,
      environment.REDIS_STREAM_MAXLEN,
      environment.REDIS_STREAM_MAX_AGE_MS,
      logger,
    ),
  createOutboxPublisher: ({ database, stream, configuration, logger, telemetry, telemetryClock }) =>
    new OutboxWorker(
      new OutboxRepository(database as DatabasePool),
      stream,
      configuration,
      logger,
      undefined,
      undefined,
      telemetry,
      telemetryClock,
    ),
  reconnectScheduler: systemWorkerScheduler,
};

export type WorkerShutdownSignal = "SIGINT" | "SIGTERM" | "PROCESS_END";

export class WorkerApplication {
  private readonly controller = new AbortController();
  private startPromise: Promise<void> | undefined;
  private shutdownPromise: Promise<void> | undefined;
  private outboxRun: Promise<void> | undefined;
  private redisRun: Promise<void> | undefined;
  private stopping = false;
  private readonly telemetry: TelemetryRecorder;
  private lifecycle: "idle" | "starting" | "ready" | "startup_failed" | "stopping" | "stopped" =
    "idle";

  constructor(
    private readonly database: WorkerApplicationDatabase,
    private readonly snapshots: SnapshotCoordinatorComponent,
    private readonly compaction: CompactionCoordinatorComponent | undefined,
    private readonly stream: DeliveryStream,
    private readonly outbox: OutboxPublisherComponent,
    private readonly reconnectDelayMs: number,
    private readonly shutdownGraceMs: number,
    private readonly logger: StructuredLogger,
    private readonly reconnectScheduler: Pick<WorkerScheduler, "sleep">,
    telemetry: TelemetryRecorder = noOpTelemetryRecorder,
  ) {
    this.telemetry = safeTelemetryRecorder(telemetry);
  }

  start(): Promise<void> {
    this.startPromise ??= this.startOnce();
    return this.startPromise;
  }

  async wait(): Promise<void> {
    await this.start();
    await Promise.all([this.outboxRun, this.redisRun]);
  }

  shutdown(signal: WorkerShutdownSignal): Promise<void> {
    this.shutdownPromise ??= this.shutdownOnce(signal);
    return this.shutdownPromise;
  }

  private async startOnce(): Promise<void> {
    this.transitionLifecycle("starting");
    try {
      await this.database.query("SELECT 1");
      if (this.stopping) return;
      await this.snapshots.start();
      if (this.stopping) return;
      await this.compaction?.start();
      if (this.stopping) return;
      this.outboxRun = this.runOutbox();
      this.redisRun = this.maintainRedisConnection();
      this.transitionLifecycle("ready");
    } catch (error) {
      this.transitionLifecycle("startup_failed");
      await this.shutdown("PROCESS_END");
      throw error;
    }
  }

  private async runOutbox(): Promise<void> {
    try {
      await this.outbox.run(this.controller.signal);
    } catch {
      if (!this.controller.signal.aborted)
        this.logger.error(
          { component: "outbox", code: "OUTBOX_PUBLISHER_STOPPED" },
          "Outbox publisher stopped unexpectedly",
        );
    }
  }

  private async maintainRedisConnection(): Promise<void> {
    while (!this.controller.signal.aborted) {
      if (!this.stream.isReady()) {
        try {
          await this.stream.connect();
          if (!this.controller.signal.aborted && this.stream.isReady())
            this.logger.info(
              { component: "worker", outcome: "ready" },
              "Worker connected to Redis",
            );
        } catch {
          if (!this.controller.signal.aborted)
            this.logger.warn(
              { component: "redis", code: "REDIS_CONNECT_FAILED" },
              "Redis connection attempt failed",
            );
        }
      }
      if (this.controller.signal.aborted) return;
      await this.reconnectScheduler.sleep(this.reconnectDelayMs, this.controller.signal);
    }
  }

  private async shutdownOnce(signal: WorkerShutdownSignal): Promise<void> {
    this.stopping = true;
    this.transitionLifecycle("stopping");
    this.outbox.stopTakingClaims();
    this.controller.abort();
    const [snapshotResult, compactionResult, outboxResult] = await Promise.allSettled([
      this.snapshots.stop(),
      this.compaction?.stop(),
      this.outbox.drain(this.shutdownGraceMs),
    ]);
    const outboxDrained = outboxResult.status === "fulfilled" && outboxResult.value;
    if (!outboxDrained) this.outbox.abandonActiveLeases();
    const compactionDrained = compactionResult.status === "fulfilled";
    const drained = snapshotResult.status === "fulfilled" && compactionDrained && outboxDrained;
    this.logger.info(
      { component: "worker", signal, outcome: drained ? "drained" : "grace_expired" },
      "Worker shutdown",
    );
    try {
      await this.stream.close(!drained);
    } finally {
      try {
        await this.database.end();
      } finally {
        this.transitionLifecycle("stopped");
      }
    }
  }

  private transitionLifecycle(
    next: "starting" | "ready" | "startup_failed" | "stopping" | "stopped",
  ): void {
    if (this.lifecycle === next || this.lifecycle === "stopped") return;
    this.lifecycle = next;
    this.telemetry.emit({
      schemaVersion: 1,
      eventName: "worker.lifecycle",
      severity: next === "startup_failed" ? "error" : "info",
      component: "worker",
      timestamp: new Date().toISOString(),
      code: next.toUpperCase(),
    });
  }
}

function snapshotConfiguration(environment: WorkerEnvironment): SnapshotCoordinatorConfiguration {
  return {
    pollIntervalMs: environment.SNAPSHOT_POLL_INTERVAL_MS,
    pollJitterPercent: environment.SNAPSHOT_POLL_JITTER_PERCENT,
    candidateScanLimit: environment.SNAPSHOT_CANDIDATE_SCAN_LIMIT,
    candidateLimit: environment.SNAPSHOT_CANDIDATE_BATCH_SIZE,
    maximumConcurrency: environment.SNAPSHOT_MAX_CONCURRENCY,
    operationThreshold: environment.SNAPSHOT_OPERATION_THRESHOLD,
    changedAgeMs: environment.SNAPSHOT_CHANGED_AGE_MS,
    operationBytesThreshold: environment.SNAPSHOT_OPERATION_BYTES_THRESHOLD,
    maximumPayloadBytes: environment.SNAPSHOT_MAX_PAYLOAD_BYTES,
    retryBaseMs: environment.SNAPSHOT_RETRY_BASE_MS,
    retryCapMs: environment.SNAPSHOT_RETRY_CAP_MS,
    busyRetryMs: environment.SNAPSHOT_BUSY_RETRY_MS,
    failureFingerprintLimit: environment.SNAPSHOT_FAILURE_FINGERPRINT_LIMIT,
    shutdownGraceMs: environment.WORKER_SHUTDOWN_GRACE_MS,
  };
}

function compactionConfiguration(
  environment: WorkerEnvironment,
): CompactionCoordinatorConfiguration {
  return {
    pollIntervalMs: environment.COMPACTION_POLL_INTERVAL_MS,
    pollJitterPercent: environment.COMPACTION_POLL_JITTER_PERCENT,
    candidateScanLimit: environment.COMPACTION_CANDIDATE_SCAN_LIMIT,
    candidateResultLimit: environment.COMPACTION_CANDIDATE_BATCH_SIZE,
    maximumConcurrency: environment.COMPACTION_MAX_CONCURRENCY,
    retryBaseMs: environment.COMPACTION_RETRY_BASE_MS,
    retryCapMs: environment.COMPACTION_RETRY_CAP_MS,
    retainedStateLimit: environment.COMPACTION_RETAINED_STATE_LIMIT,
    shutdownGraceMs: environment.WORKER_SHUTDOWN_GRACE_MS,
  };
}

function outboxConfiguration(environment: WorkerEnvironment): OutboxWorkerConfiguration {
  return {
    owner: environment.WORKER_ID ?? `worker-${crypto.randomUUID()}`,
    claimBatchSize: environment.OUTBOX_CLAIM_BATCH_SIZE,
    publishConcurrency: environment.OUTBOX_PUBLISH_CONCURRENCY,
    leaseDurationMs: environment.OUTBOX_LEASE_MS,
    idlePollMs: environment.OUTBOX_IDLE_POLL_MS,
    pollJitterRatio: environment.OUTBOX_POLL_JITTER_RATIO,
    publicationTimeoutMs: environment.REDIS_PUBLISH_TIMEOUT_MS,
    maximumEnvelopeBytes: environment.DELIVERY_ENVELOPE_MAX_BYTES,
  };
}

export async function createWorkerApplication(
  environment: WorkerEnvironment,
  overrides: Partial<WorkerApplicationFactories> = {},
  options: {
    telemetry?: TelemetryRecorder;
    telemetryClock?: WorkerTelemetryClock;
  } = {},
): Promise<WorkerApplication> {
  const factories = { ...productionWorkerApplicationFactories, ...overrides };
  const telemetry = options.telemetry ?? noOpTelemetryRecorder;
  const telemetryClock = options.telemetryClock ?? systemWorkerTelemetryClock;
  const logger = factories.createLogger(environment.LOG_LEVEL);
  let database: WorkerApplicationDatabase | undefined;
  let snapshots: SnapshotCoordinatorComponent | undefined;
  let compaction: CompactionCoordinatorComponent | undefined;
  let stream: DeliveryStream | undefined;
  try {
    database = factories.createDatabase(environment.DATABASE_URL);
    snapshots = factories.createSnapshotCoordinator({
      database,
      configuration: snapshotConfiguration(environment),
      logger,
      telemetry,
      telemetryClock,
    });
    if (environment.COMPACTION_ENABLED) {
      const candidates = factories.createCompactionCandidateRepository(database);
      const boardCompaction = factories.createBoardCompactionRepository(database);
      compaction = factories.createCompactionCoordinator({
        candidates,
        compaction: boardCompaction,
        configuration: compactionConfiguration(environment),
        logger,
        telemetry,
        telemetryClock,
      });
    }
    stream = factories.createStream({ environment, logger });
    const outbox = factories.createOutboxPublisher({
      database,
      stream,
      configuration: outboxConfiguration(environment),
      logger,
      telemetry,
      telemetryClock,
    });
    return new WorkerApplication(
      database,
      snapshots,
      compaction,
      stream,
      outbox,
      environment.OUTBOX_IDLE_POLL_MS,
      environment.WORKER_SHUTDOWN_GRACE_MS,
      logger,
      factories.reconnectScheduler,
      telemetry,
    );
  } catch (error) {
    await Promise.allSettled([
      compaction?.stop(),
      snapshots?.stop(),
      stream?.close(true),
      database?.end(),
    ]);
    throw error;
  }
}
