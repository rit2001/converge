import {
  METRIC_CATALOG,
  type HistogramMetricName,
  type MetricName,
  type TelemetrySnapshot,
  validateStructuredEvent,
} from "./telemetry.js";

export const PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

export interface PrometheusRenderResult {
  contentType: typeof PROMETHEUS_CONTENT_TYPE;
  body: string;
}

export class PrometheusRenderError extends Error {
  constructor(public readonly code: string) {
    super(`Prometheus telemetry rendering failed: ${code}`);
  }
}

export interface SafePrometheusRenderOptions {
  fallback?: (error: PrometheusRenderError) => void | Promise<void>;
}

export type SafePrometheusRenderResult =
  | { ok: true; value: PrometheusRenderResult }
  | { ok: false; error: PrometheusRenderError };

type CatalogDefinition = (typeof METRIC_CATALOG)[MetricName];
type HistogramDefinition = Extract<CatalogDefinition, { type: "histogram" }>;
type SnapshotSeries = TelemetrySnapshot["counters"][number] | TelemetrySnapshot["gauges"][number];

function fail(code: string): never {
  throw new PrometheusRenderError(code);
}

function record(value: unknown, code: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) fail(code);
  return value as Record<string, unknown>;
}

function frozen(value: unknown, code: string): void {
  if (
    (typeof value !== "object" && typeof value !== "function") ||
    value === null ||
    !Object.isFrozen(value)
  )
    fail(code);
}

function exactKeys(
  value: Record<string, unknown>,
  expected: readonly string[],
  code: string,
): void {
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  if (
    actual.length !== sortedExpected.length ||
    actual.some((key, index) => key !== sortedExpected[index])
  )
    fail(code);
}

function definition<Type extends CatalogDefinition["type"]>(
  name: unknown,
  type: Type,
): Extract<CatalogDefinition, { type: Type }> {
  if (typeof name !== "string" || !Object.hasOwn(METRIC_CATALOG, name)) fail("UNKNOWN_METRIC");
  const value = METRIC_CATALOG[name as MetricName];
  if (value.type !== type) fail("METRIC_TYPE_MISMATCH");
  return value as Extract<CatalogDefinition, { type: Type }>;
}

function validateLabels(
  labels: unknown,
  metric: CatalogDefinition,
): Readonly<Record<string, string>> {
  const value = record(labels, "INVALID_LABELS");
  const allowed = metric.labels as Readonly<Record<string, readonly string[]>>;
  const expectedKeys = Object.keys(allowed).sort();
  const actualKeys = Object.keys(value).sort();
  if (
    expectedKeys.length !== actualKeys.length ||
    expectedKeys.some((key, index) => key !== actualKeys[index])
  )
    fail("INVALID_LABELS");
  const validated: Record<string, string> = {};
  for (const key of expectedKeys) {
    const label = value[key];
    if (typeof label !== "string" || !allowed[key]?.includes(label)) fail("INVALID_LABEL_VALUE");
    validated[key] = label;
  }
  return validated;
}

function seriesKey(name: string, labels: Readonly<Record<string, string>>): string {
  return `${name}|${Object.entries(labels)
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .map(([key, value]) => `${key}=${value}`)
    .join(",")}`;
}

function finiteNumber(value: unknown, code: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) fail(code);
  return value;
}

function safeInteger(value: unknown, code: string): number {
  const numeric = finiteNumber(value, code);
  if (!Number.isSafeInteger(numeric)) fail(code);
  return numeric;
}

function number(value: number): string {
  if (!Number.isFinite(value) || value < 0) fail("INVALID_NUMERIC_VALUE");
  return String(value);
}

export function escapePrometheusLabelValue(value: string): string {
  let escaped = "";
  for (const character of value) {
    if (character === "\\") escaped += "\\\\";
    else if (character === '"') escaped += '\\"';
    else if (character === "\n") escaped += "\\n";
    else {
      const code = character.charCodeAt(0);
      if (code < 32 || code === 127) fail("INVALID_LABEL_CONTROL_CHARACTER");
      escaped += character;
    }
  }
  return escaped;
}

function renderLabels(
  labels: Readonly<Record<string, string>>,
  extra?: readonly [string, string],
): string {
  const entries: [string, string][] = Object.entries(labels).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  );
  if (extra) entries.push([extra[0], extra[1]]);
  if (entries.length === 0) return "";
  return `{${entries.map(([key, value]) => `${key}="${escapePrometheusLabelValue(value)}"`).join(",")}}`;
}

function renderSimpleSeries(
  lines: string[],
  entries: readonly SnapshotSeries[],
  type: "counter" | "gauge",
): void {
  const seen = new Set<string>();
  const families = new Map<
    string,
    Array<{ labels: Readonly<Record<string, string>>; value: number }>
  >();
  for (const rawEntry of entries as readonly unknown[]) {
    frozen(rawEntry, "MUTABLE_SNAPSHOT");
    const entry = record(rawEntry, "INVALID_SERIES");
    exactKeys(entry, ["name", "labels", "value"], "INVALID_SERIES");
    const metric = definition(entry.name, type);
    frozen(entry.labels, "MUTABLE_SNAPSHOT");
    const labels = validateLabels(entry.labels, metric);
    const value =
      type === "counter"
        ? finiteNumber(entry.value, "INVALID_COUNTER_VALUE")
        : safeInteger(entry.value, "INVALID_GAUGE_VALUE");
    if (type === "counter" && value === 0) fail("INVALID_COUNTER_VALUE");
    if (
      type === "gauge" &&
      "value" in metric &&
      metric.value === "binary" &&
      value !== 0 &&
      value !== 1
    )
      fail("INVALID_GAUGE_VALUE");
    const key = seriesKey(entry.name as string, labels);
    if (seen.has(key)) fail("DUPLICATE_SERIES");
    seen.add(key);
    const family = families.get(entry.name as string) ?? [];
    family.push({ labels, value });
    families.set(entry.name as string, family);
  }
  for (const name of [...families.keys()].sort()) {
    const metric = METRIC_CATALOG[name as MetricName];
    lines.push(`# HELP ${name} ${metric.description}`, `# TYPE ${name} ${type}`);
    for (const entry of families
      .get(name)!
      .sort((left, right) =>
        seriesKey(name, left.labels) < seriesKey(name, right.labels)
          ? -1
          : seriesKey(name, left.labels) > seriesKey(name, right.labels)
            ? 1
            : 0,
      ))
      lines.push(`${name}${renderLabels(entry.labels)} ${number(entry.value)}`);
  }
}

function renderHistograms(lines: string[], entries: TelemetrySnapshot["histograms"]): void {
  const seen = new Set<string>();
  const families = new Map<
    string,
    Array<{
      labels: Readonly<Record<string, string>>;
      count: number;
      sum: number;
      buckets: readonly { upperBound: number; count: number }[];
    }>
  >();
  for (const rawEntry of entries as readonly unknown[]) {
    frozen(rawEntry, "MUTABLE_SNAPSHOT");
    const entry = record(rawEntry, "INVALID_HISTOGRAM");
    exactKeys(entry, ["name", "labels", "count", "sum", "buckets"], "INVALID_HISTOGRAM");
    const metric: HistogramDefinition = definition(entry.name, "histogram");
    frozen(entry.labels, "MUTABLE_SNAPSHOT");
    const labels = validateLabels(entry.labels, metric);
    const count = safeInteger(entry.count, "INVALID_HISTOGRAM_COUNT");
    if (count === 0) fail("INVALID_HISTOGRAM_COUNT");
    const sum = finiteNumber(entry.sum, "INVALID_HISTOGRAM_SUM");
    if (!Array.isArray(entry.buckets)) fail("INVALID_HISTOGRAM_BUCKETS");
    frozen(entry.buckets, "MUTABLE_SNAPSHOT");
    const expected = metric.buckets;
    if (entry.buckets.length !== expected.length) fail("INVALID_HISTOGRAM_BUCKETS");
    let previous = 0;
    const buckets = entry.buckets.map((rawBucket, index) => {
      frozen(rawBucket, "MUTABLE_SNAPSHOT");
      const bucket = record(rawBucket, "INVALID_HISTOGRAM_BUCKETS");
      exactKeys(bucket, ["upperBound", "count"], "INVALID_HISTOGRAM_BUCKETS");
      if (bucket.upperBound !== expected[index]) fail("INVALID_HISTOGRAM_BUCKETS");
      const bucketCount = safeInteger(bucket.count, "INVALID_HISTOGRAM_BUCKETS");
      if (bucketCount < previous || bucketCount > count) fail("INVALID_HISTOGRAM_BUCKETS");
      previous = bucketCount;
      return { upperBound: bucket.upperBound as number, count: bucketCount };
    });
    const key = seriesKey(entry.name as string, labels);
    if (seen.has(key)) fail("DUPLICATE_SERIES");
    seen.add(key);
    const family = families.get(entry.name as string) ?? [];
    family.push({ labels, count, sum, buckets });
    families.set(entry.name as string, family);
  }
  for (const name of [...families.keys()].sort()) {
    const metric = METRIC_CATALOG[name as HistogramMetricName];
    lines.push(`# HELP ${name} ${metric.description}`, `# TYPE ${name} histogram`);
    for (const entry of families
      .get(name)!
      .sort((left, right) =>
        seriesKey(name, left.labels) < seriesKey(name, right.labels)
          ? -1
          : seriesKey(name, left.labels) > seriesKey(name, right.labels)
            ? 1
            : 0,
      )) {
      for (const bucket of entry.buckets)
        lines.push(
          `${name}_bucket${renderLabels(entry.labels, ["le", number(bucket.upperBound)])} ${number(bucket.count)}`,
        );
      lines.push(
        `${name}_bucket${renderLabels(entry.labels, ["le", "+Inf"])} ${number(entry.count)}`,
      );
      lines.push(`${name}_sum${renderLabels(entry.labels)} ${number(entry.sum)}`);
      lines.push(`${name}_count${renderLabels(entry.labels)} ${number(entry.count)}`);
    }
  }
}

function validateEventEvidence(event: unknown): void {
  frozen(event, "MUTABLE_SNAPSHOT");
  const value = record(event, "INVALID_EVENT_EVIDENCE");
  let correlation: Record<string, unknown> | undefined;
  if (value.correlation !== undefined) {
    frozen(value.correlation, "MUTABLE_SNAPSHOT");
    correlation = record(value.correlation, "INVALID_EVENT_EVIDENCE");
  }
  try {
    const validated = validateStructuredEvent(event);
    if (validated.code !== value.code) fail("INVALID_EVENT_EVIDENCE");
    if (
      validated.correlation !== undefined &&
      Object.entries(validated.correlation).some(
        ([key, identifier]) => correlation?.[key] !== identifier,
      )
    )
      fail("INVALID_EVENT_EVIDENCE");
  } catch {
    fail("INVALID_EVENT_EVIDENCE");
  }
}

export function renderPrometheus(snapshot: TelemetrySnapshot): PrometheusRenderResult {
  frozen(snapshot, "MUTABLE_SNAPSHOT");
  const value = record(snapshot, "INVALID_SNAPSHOT");
  exactKeys(value, ["counters", "histograms", "gauges", "events"], "INVALID_SNAPSHOT");
  if (
    !Array.isArray(value.counters) ||
    !Array.isArray(value.histograms) ||
    !Array.isArray(value.gauges) ||
    !Array.isArray(value.events)
  )
    fail("INVALID_SNAPSHOT");
  frozen(value.counters, "MUTABLE_SNAPSHOT");
  frozen(value.histograms, "MUTABLE_SNAPSHOT");
  frozen(value.gauges, "MUTABLE_SNAPSHOT");
  frozen(value.events, "MUTABLE_SNAPSHOT");
  for (const event of value.events) validateEventEvidence(event);
  const lines: string[] = [];
  renderSimpleSeries(lines, value.counters as TelemetrySnapshot["counters"], "counter");
  renderSimpleSeries(lines, value.gauges as TelemetrySnapshot["gauges"], "gauge");
  renderHistograms(lines, value.histograms as TelemetrySnapshot["histograms"]);
  return Object.freeze({ contentType: PROMETHEUS_CONTENT_TYPE, body: `${lines.join("\n")}\n` });
}

export function safeRenderPrometheus(
  snapshot: TelemetrySnapshot,
  options: SafePrometheusRenderOptions = {},
): SafePrometheusRenderResult {
  try {
    return Object.freeze({ ok: true, value: renderPrometheus(snapshot) });
  } catch {
    const error = new PrometheusRenderError("RENDER_FAILED");
    try {
      const fallback = options.fallback?.(error);
      if (fallback && typeof (fallback as PromiseLike<void>).then === "function")
        void Promise.resolve(fallback).catch(() => undefined);
    } catch {
      // Fallback observation cannot escape the exporter boundary.
    }
    return Object.freeze({ ok: false, error });
  }
}
