const METRIC_PREFIX = "converge_";
const MAX_RETAINED_EVENTS = 10_000;
const MAX_IDENTIFIER_LENGTH = 128;
const MACHINE_CODE_PATTERN = /^[A-Z][A-Z0-9_]{0,63}$/;

export const TELEMETRY_SCHEMA_VERSION = 1 as const;

export const DEFAULT_DURATION_BUCKETS_SECONDS = Object.freeze([
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300,
] as const);

const metricCatalog = {
  converge_delivery_events_total: {
    type: "counter",
    labels: {
      event_type: ["operation", "membership_revoked"],
      outcome: ["handled", "duplicate", "quarantined", "failed"],
    },
  },
  converge_delivery_state_transitions_total: {
    type: "counter",
    labels: {
      source: ["consumer", "watchdog", "socket_readiness"],
      state: ["established", "unavailable", "recovering", "recovered", "terminal"],
    },
  },
  converge_outbox_publications_total: {
    type: "counter",
    labels: { outcome: ["published", "retry", "blocked", "stale"] },
  },
  converge_snapshot_runs_total: {
    type: "counter",
    labels: {
      outcome: ["captured", "busy", "no_progress", "deterministic_failure", "transient_failure"],
    },
  },
  converge_compaction_runs_total: {
    type: "counter",
    labels: {
      outcome: ["compacted", "no_progress", "no_boundary", "blocked", "transient_failure"],
    },
  },
  converge_recovery_requests_total: {
    type: "counter",
    labels: {
      outcome: [
        "snapshot_tail",
        "refreshed",
        "recovery_blocked",
        "retryable_failure",
        "authorization_failure",
      ],
    },
  },
  converge_outbox_publication_duration_seconds: {
    type: "histogram",
    labels: {},
    buckets: DEFAULT_DURATION_BUCKETS_SECONDS,
  },
  converge_snapshot_duration_seconds: {
    type: "histogram",
    labels: {},
    buckets: DEFAULT_DURATION_BUCKETS_SECONDS,
  },
  converge_compaction_duration_seconds: {
    type: "histogram",
    labels: {},
    buckets: DEFAULT_DURATION_BUCKETS_SECONDS,
  },
  converge_recovery_duration_seconds: {
    type: "histogram",
    labels: {},
    buckets: DEFAULT_DURATION_BUCKETS_SECONDS,
  },
  converge_socket_ready: { type: "gauge", labels: {}, value: "binary" },
  converge_delivery_consumer_ready: { type: "gauge", labels: {}, value: "binary" },
  converge_outbox_active_work: { type: "gauge", labels: {}, value: "active_work" },
  converge_snapshot_active_work: { type: "gauge", labels: {}, value: "active_work" },
  converge_compaction_active_work: { type: "gauge", labels: {}, value: "active_work" },
} as const;

function deepFreeze<T>(value: T): Readonly<T> {
  if (typeof value !== "object" || value === null) return value;
  for (const nested of Object.values(value)) deepFreeze(nested);
  return Object.freeze(value);
}

export const METRIC_CATALOG = deepFreeze(metricCatalog);

type MetricDefinition = (typeof METRIC_CATALOG)[keyof typeof METRIC_CATALOG];
export type MetricName = keyof typeof METRIC_CATALOG;
export type CounterMetricName = {
  [Name in MetricName]: (typeof METRIC_CATALOG)[Name]["type"] extends "counter" ? Name : never;
}[MetricName];
export type HistogramMetricName = {
  [Name in MetricName]: (typeof METRIC_CATALOG)[Name]["type"] extends "histogram" ? Name : never;
}[MetricName];
export type GaugeMetricName = {
  [Name in MetricName]: (typeof METRIC_CATALOG)[Name]["type"] extends "gauge" ? Name : never;
}[MetricName];

type LabelDefinition<Name extends MetricName> = (typeof METRIC_CATALOG)[Name]["labels"];
export type MetricLabels<Name extends MetricName> = {
  [Key in keyof LabelDefinition<Name>]: LabelDefinition<Name>[Key] extends readonly string[]
    ? LabelDefinition<Name>[Key][number]
    : never;
};

export const STRUCTURED_EVENT_NAMES = Object.freeze([
  "delivery.consumer.lifecycle",
  "delivery.cursor_lost",
  "delivery.board_quarantined",
  "delivery.watchdog.divergence",
  "socket.readiness.changed",
  "outbox.publication.result",
  "snapshot.capture.result",
  "compaction.result",
  "recovery.request.result",
  "worker.lifecycle",
  "api.lifecycle",
] as const);

export type StructuredEventName = (typeof STRUCTURED_EVENT_NAMES)[number];
export type TelemetrySeverity = "info" | "warn" | "error";
export type TelemetryComponent =
  | "api"
  | "worker"
  | "delivery_consumer"
  | "delivery_watchdog"
  | "socket_readiness"
  | "outbox"
  | "snapshot"
  | "compaction"
  | "recovery";

export const CORRELATION_IDENTIFIER_KEYS = Object.freeze([
  "boardId",
  "eventId",
  "operationId",
  "snapshotId",
  "socketId",
  "redisEntryId",
  "correlationId",
] as const);
export type CorrelationIdentifierKey = (typeof CORRELATION_IDENTIFIER_KEYS)[number];
export type CorrelationIdentifiers = Partial<Record<CorrelationIdentifierKey, string>>;

export interface StructuredTelemetryEvent {
  schemaVersion: typeof TELEMETRY_SCHEMA_VERSION;
  eventName: StructuredEventName;
  severity: TelemetrySeverity;
  component: TelemetryComponent;
  timestamp: string;
  code: string;
  correlation?: CorrelationIdentifiers;
}

const eventComponents: Readonly<Record<StructuredEventName, readonly TelemetryComponent[]>> =
  deepFreeze({
    "delivery.consumer.lifecycle": ["delivery_consumer"],
    "delivery.cursor_lost": ["delivery_consumer"],
    "delivery.board_quarantined": ["delivery_consumer"],
    "delivery.watchdog.divergence": ["delivery_watchdog"],
    "socket.readiness.changed": ["socket_readiness"],
    "outbox.publication.result": ["outbox"],
    "snapshot.capture.result": ["snapshot"],
    "compaction.result": ["compaction"],
    "recovery.request.result": ["recovery", "api"],
    "worker.lifecycle": ["worker"],
    "api.lifecycle": ["api"],
  });

const eventCorrelationKeys: Readonly<
  Record<StructuredEventName, readonly CorrelationIdentifierKey[]>
> = deepFreeze({
  "delivery.consumer.lifecycle": ["redisEntryId", "correlationId"],
  "delivery.cursor_lost": ["redisEntryId", "correlationId"],
  "delivery.board_quarantined": ["boardId", "eventId", "correlationId"],
  "delivery.watchdog.divergence": ["boardId", "correlationId"],
  "socket.readiness.changed": ["socketId", "correlationId"],
  "outbox.publication.result": ["boardId", "eventId", "correlationId"],
  "snapshot.capture.result": ["boardId", "snapshotId", "correlationId"],
  "compaction.result": ["boardId", "snapshotId", "correlationId"],
  "recovery.request.result": ["boardId", "correlationId"],
  "worker.lifecycle": ["correlationId"],
  "api.lifecycle": ["correlationId"],
});

export interface DiagnosticClassification {
  category: "EXPECTED_ERROR" | "UNEXPECTED_ERROR";
  code: string;
  internalCorrelationId: string;
}

export class ClassifiedDiagnosticError extends Error {
  constructor(public readonly diagnosticCode: string) {
    super("A classified diagnostic failure occurred");
  }
}

export class TelemetryValidationError extends Error {
  constructor(public readonly code: string) {
    super(`Telemetry validation failed: ${code}`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sanitizeBoundedString(value: string, maximum = MAX_IDENTIFIER_LENGTH): string {
  return [...value]
    .map((character) => {
      const code = character.charCodeAt(0);
      return code <= 31 || code === 127 ? " " : character;
    })
    .join("")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maximum);
}

function requireMachineCode(value: unknown): string {
  if (typeof value !== "string") throw new TelemetryValidationError("INVALID_EVENT_CODE");
  if ([...value].length > 64) throw new TelemetryValidationError("INVALID_EVENT_CODE");
  const sanitized = sanitizeBoundedString(value, 64)
    .replace(/[^A-Za-z0-9_]/g, "_")
    .toUpperCase();
  if (!MACHINE_CODE_PATTERN.test(sanitized))
    throw new TelemetryValidationError("INVALID_EVENT_CODE");
  return sanitized;
}

function requireCorrelationId(value: unknown): string {
  if (typeof value !== "string")
    throw new TelemetryValidationError("INVALID_CORRELATION_IDENTIFIER");
  if ([...value].length > MAX_IDENTIFIER_LENGTH)
    throw new TelemetryValidationError("INVALID_CORRELATION_IDENTIFIER");
  const sanitized = sanitizeBoundedString(value);
  if (sanitized.length === 0) throw new TelemetryValidationError("INVALID_CORRELATION_IDENTIFIER");
  return sanitized;
}

function exactKeys(
  record: Record<string, unknown>,
  allowed: readonly string[],
  code: string,
): void {
  if (Object.keys(record).some((key) => !allowed.includes(key)))
    throw new TelemetryValidationError(code);
}

export function validateStructuredEvent(value: unknown): StructuredTelemetryEvent {
  if (!isRecord(value)) throw new TelemetryValidationError("INVALID_EVENT");
  exactKeys(
    value,
    ["schemaVersion", "eventName", "severity", "component", "timestamp", "code", "correlation"],
    "UNKNOWN_EVENT_FIELD",
  );
  if (value.schemaVersion !== TELEMETRY_SCHEMA_VERSION)
    throw new TelemetryValidationError("INVALID_EVENT_SCHEMA_VERSION");
  if (
    typeof value.eventName !== "string" ||
    !STRUCTURED_EVENT_NAMES.includes(value.eventName as StructuredEventName)
  )
    throw new TelemetryValidationError("UNKNOWN_EVENT_NAME");
  const eventName = value.eventName as StructuredEventName;
  if (value.severity !== "info" && value.severity !== "warn" && value.severity !== "error")
    throw new TelemetryValidationError("INVALID_EVENT_SEVERITY");
  if (
    typeof value.component !== "string" ||
    !eventComponents[eventName].includes(value.component as TelemetryComponent)
  )
    throw new TelemetryValidationError("INVALID_EVENT_COMPONENT");
  if (typeof value.timestamp !== "string")
    throw new TelemetryValidationError("INVALID_EVENT_TIMESTAMP");
  const date = new Date(value.timestamp);
  if (!Number.isFinite(date.getTime()) || date.toISOString() !== value.timestamp)
    throw new TelemetryValidationError("INVALID_EVENT_TIMESTAMP");

  let correlation: CorrelationIdentifiers | undefined;
  if (value.correlation !== undefined) {
    if (!isRecord(value.correlation))
      throw new TelemetryValidationError("INVALID_EVENT_CORRELATION");
    exactKeys(
      value.correlation,
      eventCorrelationKeys[eventName],
      "UNKNOWN_EVENT_CORRELATION_FIELD",
    );
    if (Object.keys(value.correlation).length > eventCorrelationKeys[eventName].length)
      throw new TelemetryValidationError("INVALID_EVENT_CORRELATION");
    correlation = Object.fromEntries(
      Object.entries(value.correlation).map(([key, identifier]) => [
        key,
        requireCorrelationId(identifier),
      ]),
    );
  }
  return Object.freeze({
    schemaVersion: TELEMETRY_SCHEMA_VERSION,
    eventName,
    severity: value.severity,
    component: value.component as TelemetryComponent,
    timestamp: value.timestamp,
    code: requireMachineCode(value.code),
    ...(correlation === undefined ? {} : { correlation: Object.freeze(correlation) }),
  });
}

export function classifyDiagnosticError(
  error: unknown,
  internalCorrelationId: string,
): DiagnosticClassification {
  const correlation = requireCorrelationId(internalCorrelationId);
  if (error instanceof ClassifiedDiagnosticError)
    return Object.freeze({
      category: "EXPECTED_ERROR",
      code: requireMachineCode(error.diagnosticCode),
      internalCorrelationId: correlation,
    });
  return Object.freeze({
    category: "UNEXPECTED_ERROR",
    code: "UNEXPECTED_ERROR",
    internalCorrelationId: correlation,
  });
}

export interface TelemetryRecorder {
  increment<Name extends CounterMetricName>(
    metric: Name,
    labels: MetricLabels<Name>,
    amount?: number,
  ): void;
  observe<Name extends HistogramMetricName>(
    metric: Name,
    labels: MetricLabels<Name>,
    value: number,
  ): void;
  setGauge<Name extends GaugeMetricName>(
    metric: Name,
    labels: MetricLabels<Name>,
    value: number,
  ): void;
  emit(event: StructuredTelemetryEvent): void;
}

export interface CounterSnapshot {
  name: CounterMetricName;
  labels: Readonly<Record<string, string>>;
  value: number;
}

export interface HistogramSnapshot {
  name: HistogramMetricName;
  labels: Readonly<Record<string, string>>;
  count: number;
  sum: number;
  buckets: readonly Readonly<{ upperBound: number; count: number }>[];
}

export interface GaugeSnapshot {
  name: GaugeMetricName;
  labels: Readonly<Record<string, string>>;
  value: number;
}

export interface TelemetrySnapshot {
  counters: readonly CounterSnapshot[];
  histograms: readonly HistogramSnapshot[];
  gauges: readonly GaugeSnapshot[];
  events: readonly StructuredTelemetryEvent[];
}

export interface TelemetrySink {
  export(snapshot: TelemetrySnapshot): void | Promise<void>;
}

function metricDefinition<Type extends MetricDefinition["type"]>(
  metric: unknown,
  expected: Type,
): Extract<MetricDefinition, { type: Type }> {
  if (typeof metric !== "string" || !Object.hasOwn(METRIC_CATALOG, metric))
    throw new TelemetryValidationError("UNKNOWN_METRIC");
  const definition = METRIC_CATALOG[metric as MetricName];
  if (definition.type !== expected) throw new TelemetryValidationError("METRIC_TYPE_MISMATCH");
  return definition as Extract<MetricDefinition, { type: Type }>;
}

function validatedLabels(
  definition: MetricDefinition,
  labels: unknown,
): Readonly<Record<string, string>> {
  if (!isRecord(labels)) throw new TelemetryValidationError("INVALID_METRIC_LABELS");
  const allowed = definition.labels as Readonly<Record<string, readonly string[]>>;
  const expectedKeys = Object.keys(allowed).sort();
  const actualKeys = Object.keys(labels).sort();
  if (
    expectedKeys.length !== actualKeys.length ||
    expectedKeys.some((key, index) => key !== actualKeys[index])
  )
    throw new TelemetryValidationError("INVALID_METRIC_LABELS");
  const normalized: Record<string, string> = {};
  for (const key of expectedKeys) {
    const value = labels[key];
    if (typeof value !== "string" || !allowed[key]?.includes(value))
      throw new TelemetryValidationError("INVALID_METRIC_LABEL_VALUE");
    normalized[key] = value;
  }
  return Object.freeze(normalized);
}

function seriesKey(metric: string, labels: Readonly<Record<string, string>>): string {
  return `${metric}|${Object.entries(labels)
    .map(([key, value]) => `${key}=${value}`)
    .join(",")}`;
}

function cloneEvent(event: StructuredTelemetryEvent): StructuredTelemetryEvent {
  return Object.freeze({
    ...event,
    ...(event.correlation === undefined
      ? {}
      : { correlation: Object.freeze({ ...event.correlation }) }),
  });
}

export class InMemoryTelemetryRecorder implements TelemetryRecorder {
  private readonly counters = new Map<string, CounterSnapshot>();
  private readonly histograms = new Map<
    string,
    {
      name: HistogramMetricName;
      labels: Readonly<Record<string, string>>;
      count: number;
      sum: number;
      bucketCounts: number[];
    }
  >();
  private readonly gauges = new Map<string, GaugeSnapshot>();
  private readonly events: StructuredTelemetryEvent[] = [];

  constructor(private readonly retainedEventLimit = 1_000) {
    if (
      !Number.isSafeInteger(retainedEventLimit) ||
      retainedEventLimit <= 0 ||
      retainedEventLimit > MAX_RETAINED_EVENTS
    )
      throw new TelemetryValidationError("INVALID_EVENT_RETENTION_LIMIT");
  }

  increment<Name extends CounterMetricName>(
    metric: Name,
    labels: MetricLabels<Name>,
    amount = 1,
  ): void {
    const definition = metricDefinition(metric, "counter");
    const normalized = validatedLabels(definition, labels);
    if (!Number.isFinite(amount) || amount <= 0)
      throw new TelemetryValidationError("INVALID_COUNTER_AMOUNT");
    const key = seriesKey(metric, normalized);
    const previous = this.counters.get(key)?.value ?? 0;
    const next = previous + amount;
    if (!Number.isFinite(next)) throw new TelemetryValidationError("INVALID_COUNTER_AMOUNT");
    this.counters.set(key, Object.freeze({ name: metric, labels: normalized, value: next }));
  }

  observe<Name extends HistogramMetricName>(
    metric: Name,
    labels: MetricLabels<Name>,
    value: number,
  ): void {
    const definition = metricDefinition(metric, "histogram");
    const normalized = validatedLabels(definition, labels);
    if (!Number.isFinite(value) || value < 0)
      throw new TelemetryValidationError("INVALID_HISTOGRAM_VALUE");
    const key = seriesKey(metric, normalized);
    const existing = this.histograms.get(key) ?? {
      name: metric,
      labels: normalized,
      count: 0,
      sum: 0,
      bucketCounts: definition.buckets.map(() => 0),
    };
    existing.count += 1;
    existing.sum += value;
    definition.buckets.forEach((upperBound, index) => {
      if (value <= upperBound)
        existing.bucketCounts[index] = (existing.bucketCounts[index] ?? 0) + 1;
    });
    this.histograms.set(key, existing);
  }

  setGauge<Name extends GaugeMetricName>(
    metric: Name,
    labels: MetricLabels<Name>,
    value: number,
  ): void {
    const definition = metricDefinition(metric, "gauge");
    const normalized = validatedLabels(definition, labels);
    if (!Number.isSafeInteger(value) || value < 0)
      throw new TelemetryValidationError("INVALID_GAUGE_VALUE");
    if ("value" in definition && definition.value === "binary" && value !== 0 && value !== 1)
      throw new TelemetryValidationError("INVALID_GAUGE_VALUE");
    const key = seriesKey(metric, normalized);
    this.gauges.set(key, Object.freeze({ name: metric, labels: normalized, value }));
  }

  emit(event: StructuredTelemetryEvent): void {
    this.events.push(cloneEvent(validateStructuredEvent(event)));
    if (this.events.length > this.retainedEventLimit) this.events.shift();
  }

  snapshot(): TelemetrySnapshot {
    const counters = [...this.counters.values()]
      .sort((left, right) =>
        seriesKey(left.name, left.labels).localeCompare(seriesKey(right.name, right.labels)),
      )
      .map((entry) => Object.freeze({ ...entry, labels: Object.freeze({ ...entry.labels }) }));
    const gauges = [...this.gauges.values()]
      .sort((left, right) =>
        seriesKey(left.name, left.labels).localeCompare(seriesKey(right.name, right.labels)),
      )
      .map((entry) => Object.freeze({ ...entry, labels: Object.freeze({ ...entry.labels }) }));
    const histograms = [...this.histograms.values()]
      .sort((left, right) =>
        seriesKey(left.name, left.labels).localeCompare(seriesKey(right.name, right.labels)),
      )
      .map((entry) => {
        const definition = METRIC_CATALOG[entry.name];
        const buckets = definition.buckets.map((upperBound, index) =>
          Object.freeze({
            upperBound,
            count: entry.bucketCounts[index] ?? 0,
          }),
        );
        return Object.freeze({
          name: entry.name,
          labels: Object.freeze({ ...entry.labels }),
          count: entry.count,
          sum: entry.sum,
          buckets: Object.freeze(buckets),
        });
      });
    return Object.freeze({
      counters: Object.freeze(counters),
      histograms: Object.freeze(histograms),
      gauges: Object.freeze(gauges),
      events: Object.freeze(this.events.map(cloneEvent)),
    });
  }
}

export const noOpTelemetryRecorder: TelemetryRecorder = Object.freeze({
  increment: () => undefined,
  observe: () => undefined,
  setGauge: () => undefined,
  emit: () => undefined,
});

export const noOpTelemetrySink: TelemetrySink = Object.freeze({
  export: () => undefined,
});

export interface TelemetryFailureDiagnostic {
  category: "TELEMETRY_RECORDER_FAILURE";
  operation: "increment" | "observe" | "setGauge" | "emit";
  internalCorrelationId: string;
}

export interface SafeTelemetryOptions {
  fallback?: (diagnostic: TelemetryFailureDiagnostic) => void | Promise<void>;
  correlationId?: () => string;
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return isRecord(value) && typeof value.then === "function";
}

export function safeTelemetryRecorder(
  recorder: TelemetryRecorder,
  options: SafeTelemetryOptions = {},
): TelemetryRecorder {
  const report = (operation: TelemetryFailureDiagnostic["operation"]): void => {
    let correlation = "telemetry-failure";
    try {
      correlation = requireCorrelationId(options.correlationId?.() ?? crypto.randomUUID());
    } catch {
      // A fallback identifier factory cannot escape the telemetry boundary.
    }
    try {
      const result = options.fallback?.({
        category: "TELEMETRY_RECORDER_FAILURE",
        operation,
        internalCorrelationId: correlation,
      });
      if (isPromiseLike(result)) void Promise.resolve(result).catch(() => undefined);
    } catch {
      // Recursive fallback failures are deliberately suppressed.
    }
  };
  const invoke = (operation: TelemetryFailureDiagnostic["operation"], callback: () => unknown) => {
    try {
      const result = callback();
      if (isPromiseLike(result)) void Promise.resolve(result).catch(() => report(operation));
    } catch {
      report(operation);
    }
  };
  return Object.freeze({
    increment: <Name extends CounterMetricName>(
      metric: Name,
      labels: MetricLabels<Name>,
      amount?: number,
    ) => invoke("increment", () => recorder.increment(metric, labels, amount)),
    observe: <Name extends HistogramMetricName>(
      metric: Name,
      labels: MetricLabels<Name>,
      value: number,
    ) => invoke("observe", () => recorder.observe(metric, labels, value)),
    setGauge: <Name extends GaugeMetricName>(
      metric: Name,
      labels: MetricLabels<Name>,
      value: number,
    ) => invoke("setGauge", () => recorder.setGauge(metric, labels, value)),
    emit: (event: StructuredTelemetryEvent) => invoke("emit", () => recorder.emit(event)),
  });
}

export const FORBIDDEN_METRIC_LABEL_KEYS = Object.freeze([
  "board_id",
  "boardId",
  "user_id",
  "userId",
  "principal_id",
  "principalId",
  "operation_id",
  "operationId",
  "event_id",
  "eventId",
  "snapshot_id",
  "snapshotId",
  "socket_id",
  "socketId",
  "redis_entry_id",
  "redisEntryId",
  "error_message",
  "errorMessage",
  "url",
  "sql",
  "instance_id",
  "instanceId",
] as const);

if (
  Object.keys(METRIC_CATALOG).some((name) => !name.startsWith(METRIC_PREFIX)) ||
  Object.values(METRIC_CATALOG).some((definition) =>
    Object.keys(definition.labels).some((label) =>
      (FORBIDDEN_METRIC_LABEL_KEYS as readonly string[]).includes(label),
    ),
  )
)
  throw new Error("Telemetry metric catalog violates its fixed naming or cardinality contract");
