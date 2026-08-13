import type { StructuredLogger } from "@converge/observability";
import type { DeliveryStream } from "./redis-stream.js";

export interface WorkerLoop {
  run(signal: AbortSignal): Promise<void>;
  stopTakingClaims(): void;
  abandonActiveLeases(): void;
  drain(gracePeriodMs: number): Promise<boolean>;
}

export interface DatabaseCloser {
  end(): Promise<void>;
}

export class WorkerProcessLifecycle {
  private readonly controller = new AbortController();
  private shutdownPromise: Promise<void> | undefined;

  constructor(
    private readonly worker: WorkerLoop,
    private readonly stream: DeliveryStream,
    private readonly database: DatabaseCloser,
    private readonly gracePeriodMs: number,
    private readonly logger: StructuredLogger,
  ) {}

  run(): Promise<void> {
    return this.worker.run(this.controller.signal);
  }

  shutdown(signal: "SIGINT" | "SIGTERM" | "PROCESS_END"): Promise<void> {
    this.shutdownPromise ??= (async () => {
      this.worker.stopTakingClaims();
      this.controller.abort();
      const drained = await this.worker.drain(this.gracePeriodMs);
      if (!drained) this.worker.abandonActiveLeases();
      this.logger.info(
        { component: "worker", signal, outcome: drained ? "drained" : "grace_expired" },
        "Worker shutdown",
      );
      try {
        await this.stream.close(!drained);
      } finally {
        await this.database.end();
      }
    })();
    return this.shutdownPromise;
  }
}
