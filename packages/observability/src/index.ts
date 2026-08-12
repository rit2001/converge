export interface StructuredLogger {
  info(fields: Record<string, unknown>, message: string): void;
  warn(fields: Record<string, unknown>, message: string): void;
  error(fields: Record<string, unknown>, message: string): void;
}

export interface Span {
  setAttribute(name: string, value: string | number | boolean): void;
  recordException(error: Error): void;
  end(): void;
}

export interface Tracer {
  startSpan(name: string, attributes?: Record<string, string | number | boolean>): Span;
}

export const noOpTracer: Tracer = {
  startSpan: () => ({
    setAttribute: () => undefined,
    recordException: () => undefined,
    end: () => undefined,
  }),
};

export * from "./telemetry.js";
