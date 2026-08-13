import { InMemoryTelemetryRecorder } from "@converge/observability";
import { createWorkerApplication } from "./application.js";
import { parseWorkerEnvironment, type WorkerEnvironment } from "./env.js";

export async function runWorkerServer(environment: WorkerEnvironment): Promise<void> {
  const telemetry = new InMemoryTelemetryRecorder(1_000);
  const application = await createWorkerApplication(environment, {}, { telemetry });
  let signalReceived = false;
  const onInterrupt = (): void => {
    signalReceived = true;
    void application.shutdown("SIGINT").catch(() => undefined);
  };
  const onTerminate = (): void => {
    signalReceived = true;
    void application.shutdown("SIGTERM").catch(() => undefined);
  };
  process.once("SIGINT", onInterrupt);
  process.once("SIGTERM", onTerminate);

  try {
    await application.start();
    await application.wait();
    await application.shutdown(signalReceived ? "SIGTERM" : "PROCESS_END");
  } finally {
    process.off("SIGINT", onInterrupt);
    process.off("SIGTERM", onTerminate);
    if (!signalReceived) await application.shutdown("PROCESS_END");
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
