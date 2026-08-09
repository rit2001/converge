import { createPool, OutboxRepository } from "@converge/database";
import { parseWorkerEnvironment, type WorkerEnvironment } from "./env.js";
import { WorkerProcessLifecycle } from "./lifecycle.js";
import { createWorkerLogger } from "./logger.js";
import { RedisDeliveryStream } from "./redis-stream.js";
import { OutboxWorker } from "./worker.js";

export async function runWorkerServer(environment: WorkerEnvironment): Promise<void> {
  const logger = createWorkerLogger(environment.LOG_LEVEL);
  const pool = createPool(environment.DATABASE_URL);
  const repository = new OutboxRepository(pool);
  const stream = new RedisDeliveryStream(
    environment.REDIS_URL,
    environment.REDIS_STREAM_KEY,
    environment.REDIS_STREAM_MAXLEN,
    environment.REDIS_STREAM_MAX_AGE_MS,
    logger,
  );
  const worker = new OutboxWorker(
    repository,
    stream,
    {
      owner: environment.WORKER_ID ?? `worker-${crypto.randomUUID()}`,
      claimBatchSize: environment.OUTBOX_CLAIM_BATCH_SIZE,
      publishConcurrency: environment.OUTBOX_PUBLISH_CONCURRENCY,
      leaseDurationMs: environment.OUTBOX_LEASE_MS,
      idlePollMs: environment.OUTBOX_IDLE_POLL_MS,
      pollJitterRatio: environment.OUTBOX_POLL_JITTER_RATIO,
      publicationTimeoutMs: environment.REDIS_PUBLISH_TIMEOUT_MS,
      maximumEnvelopeBytes: environment.DELIVERY_ENVELOPE_MAX_BYTES,
    },
    logger,
  );
  const lifecycle = new WorkerProcessLifecycle(
    worker,
    stream,
    pool,
    environment.WORKER_SHUTDOWN_GRACE_MS,
    logger,
  );
  let signalReceived = false;
  const onInterrupt = (): void => {
    signalReceived = true;
    void lifecycle.shutdown("SIGINT");
  };
  const onTerminate = (): void => {
    signalReceived = true;
    void lifecycle.shutdown("SIGTERM");
  };
  process.once("SIGINT", onInterrupt);
  process.once("SIGTERM", onTerminate);

  try {
    await stream.connect();
    logger.info({ component: "worker", outcome: "ready" }, "Worker connected to Redis");
    await lifecycle.run();
    await lifecycle.shutdown(signalReceived ? "SIGTERM" : "PROCESS_END");
  } finally {
    process.off("SIGINT", onInterrupt);
    process.off("SIGTERM", onTerminate);
    if (!signalReceived) await lifecycle.shutdown("PROCESS_END");
  }
}

let environment: WorkerEnvironment | undefined;
try {
  environment = parseWorkerEnvironment(process.env);
  await runWorkerServer(environment);
} catch {
  process.stderr.write(
    `${JSON.stringify({
      level: "error",
      code: environment === undefined ? "WORKER_CONFIGURATION_INVALID" : "WORKER_FATAL",
      message: "Worker failed before completing its lifecycle.",
    })}\n`,
  );
  process.exitCode = 1;
}
