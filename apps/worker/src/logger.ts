import type { StructuredLogger } from "@converge/observability";

export type WorkerLogLevel = "error" | "warn" | "info" | "silent";

export function createWorkerLogger(
  level: WorkerLogLevel,
  destination: Pick<NodeJS.WritableStream, "write"> = process.stdout,
): StructuredLogger {
  const ranks = { error: 0, warn: 1, info: 2, silent: -1 } as const;
  const enabled = (candidate: Exclude<WorkerLogLevel, "silent">): boolean =>
    level !== "silent" && ranks[candidate] <= ranks[level];
  const write = (
    candidate: Exclude<WorkerLogLevel, "silent">,
    fields: Record<string, unknown>,
    message: string,
  ): void => {
    if (!enabled(candidate)) return;
    destination.write(`${JSON.stringify({ level: candidate, message, ...fields })}\n`);
  };
  return {
    info: (fields, message) => write("info", fields, message),
    warn: (fields, message) => write("warn", fields, message),
    error: (fields, message) => write("error", fields, message),
  };
}
