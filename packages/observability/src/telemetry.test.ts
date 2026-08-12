import { describe, expect, it, vi } from "vitest";
import {
  classifyDiagnosticError,
  ClassifiedDiagnosticError,
  CORRELATION_IDENTIFIER_KEYS,
  DEFAULT_DURATION_BUCKETS_SECONDS,
  FORBIDDEN_METRIC_LABEL_KEYS,
  InMemoryTelemetryRecorder,
  METRIC_CATALOG,
  noOpTelemetryRecorder,
  noOpTelemetrySink,
  safeTelemetryRecorder,
  STRUCTURED_EVENT_NAMES,
  TelemetryValidationError,
  validateStructuredEvent,
  type StructuredTelemetryEvent,
  type TelemetryRecorder,
} from "./telemetry.js";

const timestamp = "2026-08-12T10:00:00.000Z";

function event(overrides: Partial<StructuredTelemetryEvent> = {}): StructuredTelemetryEvent {
  return {
    schemaVersion: 1,
    eventName: "compaction.result",
    severity: "info",
    component: "compaction",
    timestamp,
    code: "COMPACTED",
    correlation: { boardId: "board-1", snapshotId: "snapshot-1" },
    ...overrides,
  };
}

function expectValidationCode(callback: () => void, code: string): void {
  try {
    callback();
  } catch (error) {
    expect(error).toBeInstanceOf(TelemetryValidationError);
    if (!(error instanceof TelemetryValidationError)) throw error;
    expect(error.code).toBe(code);
    return;
  }
  throw new Error(`Expected telemetry validation failure ${code}`);
}

describe("fixed metric catalog", () => {
  it("accepts every metric with its complete valid label set", () => {
    const recorder = new InMemoryTelemetryRecorder();
    recorder.increment(
      "converge_delivery_events_total",
      { event_type: "operation", outcome: "handled" },
      2,
    );
    recorder.increment("converge_delivery_state_transitions_total", {
      source: "consumer",
      state: "established",
    });
    recorder.increment("converge_outbox_publications_total", { outcome: "published" });
    recorder.increment("converge_snapshot_runs_total", { outcome: "captured" });
    recorder.increment("converge_compaction_runs_total", { outcome: "compacted" });
    recorder.increment("converge_recovery_requests_total", { outcome: "snapshot_tail" });
    recorder.observe("converge_outbox_publication_duration_seconds", {}, 0.01);
    recorder.observe("converge_snapshot_duration_seconds", {}, 0.1);
    recorder.observe("converge_compaction_duration_seconds", {}, 1);
    recorder.observe("converge_recovery_duration_seconds", {}, 5);
    recorder.setGauge("converge_socket_ready", {}, 1);
    recorder.setGauge("converge_delivery_consumer_ready", {}, 0);
    recorder.setGauge("converge_outbox_active_work", {}, 1);
    recorder.setGauge("converge_snapshot_active_work", {}, 2);
    recorder.setGauge("converge_compaction_active_work", {}, 3);

    const snapshot = recorder.snapshot();
    expect(
      snapshot.counters.find(({ name }) => name === "converge_delivery_events_total")?.value,
    ).toBe(2);
    expect(
      snapshot.histograms.find(({ name }) => name === "converge_compaction_duration_seconds")
        ?.count,
    ).toBe(1);
    expect(
      snapshot.gauges.find(({ name }) => name === "converge_compaction_active_work")?.value,
    ).toBe(3);
  });

  it("rejects missing, extra, unknown, and high-cardinality labels", () => {
    const recorder = new InMemoryTelemetryRecorder() as unknown as {
      increment(metric: string, labels: Record<string, unknown>): void;
    };
    for (const labels of [
      { event_type: "operation" },
      { event_type: "operation", outcome: "handled", board_id: "private" },
      { event_type: "operation", outcome: "invented" },
      { event_type: "operation", outcome: "handled", arbitrary: "value" },
    ])
      expect(() => recorder.increment("converge_delivery_events_total", labels)).toThrow(
        TelemetryValidationError,
      );
  });

  it("rejects unknown names and metric type mismatches", () => {
    const recorder = new InMemoryTelemetryRecorder() as unknown as {
      increment(metric: string, labels: Record<string, unknown>): void;
      observe(metric: string, labels: Record<string, unknown>, value: number): void;
    };
    expectValidationCode(() => recorder.increment("converge_dynamic_total", {}), "UNKNOWN_METRIC");
    expectValidationCode(
      () => recorder.observe("converge_socket_ready", {}, 1),
      "METRIC_TYPE_MISMATCH",
    );
  });

  it("validates counter, histogram, and gauge values", () => {
    const recorder = new InMemoryTelemetryRecorder();
    for (const amount of [0, -1, Number.NaN, Number.POSITIVE_INFINITY])
      expectValidationCode(
        () =>
          recorder.increment(
            "converge_outbox_publications_total",
            { outcome: "published" },
            amount,
          ),
        "INVALID_COUNTER_AMOUNT",
      );
    for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY])
      expectValidationCode(
        () => recorder.observe("converge_snapshot_duration_seconds", {}, value),
        "INVALID_HISTOGRAM_VALUE",
      );
    for (const value of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1])
      expectValidationCode(
        () => recorder.setGauge("converge_outbox_active_work", {}, value),
        "INVALID_GAUGE_VALUE",
      );
    for (const value of [-1, 2])
      expectValidationCode(
        () => recorder.setGauge("converge_socket_ready", {}, value),
        "INVALID_GAUGE_VALUE",
      );
  });

  it("accounts for immutable cumulative histogram buckets", () => {
    const recorder = new InMemoryTelemetryRecorder();
    recorder.observe("converge_compaction_duration_seconds", {}, 0.005);
    recorder.observe("converge_compaction_duration_seconds", {}, 0.02);
    recorder.observe("converge_compaction_duration_seconds", {}, 301);
    const histogram = recorder.snapshot().histograms[0];
    expect(histogram).toMatchObject({ count: 3, sum: 301.025 });
    expect(histogram?.buckets[0]).toEqual({ upperBound: 0.005, count: 1 });
    expect(histogram?.buckets.at(-1)).toEqual({ upperBound: 300, count: 2 });
    expect(Object.isFrozen(DEFAULT_DURATION_BUCKETS_SECONDS)).toBe(true);
    expect(Object.isFrozen(histogram?.buckets)).toBe(true);
    expect(Object.isFrozen(METRIC_CATALOG.converge_delivery_events_total.labels.outcome)).toBe(
      true,
    );
  });

  it("keeps names, keys, and forbidden-label exclusions stable", () => {
    expect(new Set(Object.keys(METRIC_CATALOG)).size).toBe(15);
    expect(new Set(STRUCTURED_EVENT_NAMES).size).toBe(STRUCTURED_EVENT_NAMES.length);
    expect(new Set(CORRELATION_IDENTIFIER_KEYS).size).toBe(CORRELATION_IDENTIFIER_KEYS.length);
    const labelKeys = Object.values(METRIC_CATALOG).flatMap((definition) =>
      Object.keys(definition.labels),
    );
    expect(labelKeys.every((key) => !FORBIDDEN_METRIC_LABEL_KEYS.includes(key as never))).toBe(
      true,
    );
    expect(Object.keys(METRIC_CATALOG)).toEqual([
      "converge_delivery_events_total",
      "converge_delivery_state_transitions_total",
      "converge_outbox_publications_total",
      "converge_snapshot_runs_total",
      "converge_compaction_runs_total",
      "converge_recovery_requests_total",
      "converge_outbox_publication_duration_seconds",
      "converge_snapshot_duration_seconds",
      "converge_compaction_duration_seconds",
      "converge_recovery_duration_seconds",
      "converge_socket_ready",
      "converge_delivery_consumer_ready",
      "converge_outbox_active_work",
      "converge_snapshot_active_work",
      "converge_compaction_active_work",
    ]);
  });
});

describe("bounded structured events", () => {
  it("accepts every catalog event with its fixed component", () => {
    const recorder = new InMemoryTelemetryRecorder(STRUCTURED_EVENT_NAMES.length);
    const components = {
      "delivery.consumer.lifecycle": "delivery_consumer",
      "delivery.cursor_lost": "delivery_consumer",
      "delivery.board_quarantined": "delivery_consumer",
      "delivery.watchdog.divergence": "delivery_watchdog",
      "socket.readiness.changed": "socket_readiness",
      "outbox.publication.result": "outbox",
      "snapshot.capture.result": "snapshot",
      "compaction.result": "compaction",
      "recovery.request.result": "recovery",
      "worker.lifecycle": "worker",
      "api.lifecycle": "api",
    } as const;
    for (const eventName of STRUCTURED_EVENT_NAMES)
      recorder.emit({
        schemaVersion: 1,
        eventName,
        severity: "info",
        component: components[eventName],
        timestamp,
        code: "ACCEPTED",
      });
    expect(recorder.snapshot().events.map(({ eventName }) => eventName)).toEqual(
      STRUCTURED_EVENT_NAMES,
    );
  });

  it("accepts catalog events and sanitizes control characters", () => {
    expect(
      validateStructuredEvent(
        event({ code: "compacted\ncleanly", correlation: { boardId: "board\r\n1" } }),
      ),
    ).toMatchObject({ code: "COMPACTED_CLEANLY", correlation: { boardId: "board 1" } });
  });

  it("rejects unknown, unbounded, sensitive, and event-inappropriate fields", () => {
    for (const invalid of [
      { ...event(), token: "secret" },
      { ...event(), error: new Error("private stack") },
      { ...event(), correlation: { boardId: "x".repeat(129) } },
      { ...event(), correlation: { userId: "principal" } },
      { ...event(), correlation: { redisEntryId: "12-0" } },
      { ...event(), code: "X".repeat(65) },
      { ...event(), component: "worker" },
    ])
      expect(() => validateStructuredEvent(invalid)).toThrow(TelemetryValidationError);
  });

  it("classifies errors without exporting messages or stacks", () => {
    expect(classifyDiagnosticError(new ClassifiedDiagnosticError("LOCK_BUSY"), "corr-1")).toEqual({
      category: "EXPECTED_ERROR",
      code: "LOCK_BUSY",
      internalCorrelationId: "corr-1",
    });
    const unexpected = classifyDiagnosticError(new Error("credential=private"), "corr-2");
    expect(unexpected).toEqual({
      category: "UNEXPECTED_ERROR",
      code: "UNEXPECTED_ERROR",
      internalCorrelationId: "corr-2",
    });
    expect(JSON.stringify(unexpected)).not.toMatch(/credential|stack|private/);
  });

  it("bounds event retention with deterministic oldest-first eviction", () => {
    const recorder = new InMemoryTelemetryRecorder(2);
    recorder.emit(event({ code: "FIRST" }));
    recorder.emit(event({ code: "SECOND" }));
    recorder.emit(event({ code: "THIRD" }));
    expect(recorder.snapshot().events.map(({ code }) => code)).toEqual(["SECOND", "THIRD"]);
    for (const limit of [0, -1, 1.5, 10_001])
      expect(() => new InMemoryTelemetryRecorder(limit)).toThrow(TelemetryValidationError);
  });

  it("returns immutable copies that cannot mutate recorder state", () => {
    const recorder = new InMemoryTelemetryRecorder();
    recorder.increment("converge_compaction_runs_total", { outcome: "compacted" });
    recorder.emit(event());
    const snapshot = recorder.snapshot();
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(() => (snapshot.counters as unknown as Array<{ value: number }>).splice(0, 1)).toThrow();
    expect(
      () => ((snapshot.events[0]?.correlation as Record<string, string>).boardId = "changed"),
    ).toThrow();
    expect(recorder.snapshot()).toMatchObject({
      counters: [{ value: 1 }],
      events: [{ correlation: { boardId: "board-1" } }],
    });
  });
});

describe("telemetry failure isolation", () => {
  it("keeps the no-op recorder non-throwing even for invalid runtime inputs", () => {
    const unchecked = noOpTelemetryRecorder as unknown as Record<
      "increment" | "observe" | "setGauge" | "emit",
      (...arguments_: unknown[]) => void
    >;
    expect(() => unchecked.increment("unknown", null, -1)).not.toThrow();
    expect(() => unchecked.observe("unknown", null, Number.NaN)).not.toThrow();
    expect(() => unchecked.setGauge("unknown", null, -1)).not.toThrow();
    expect(() => unchecked.emit(new Error("private"))).not.toThrow();
    expect(() => noOpTelemetrySink.export({} as never)).not.toThrow();
  });

  it("contains recorder failures without changing a domain result", () => {
    const failing = {
      increment: () => {
        throw new Error("telemetry unavailable");
      },
      observe: () => undefined,
      setGauge: () => undefined,
      emit: () => undefined,
    } as TelemetryRecorder;
    const fallback = vi.fn();
    const safe = safeTelemetryRecorder(failing, {
      fallback,
      correlationId: () => "internal-1",
    });
    const domain = () => {
      safe.increment("converge_compaction_runs_total", { outcome: "compacted" });
      return { outcome: "compacted" } as const;
    };
    expect(domain()).toEqual({ outcome: "compacted" });
    expect(fallback).toHaveBeenCalledOnce();
    expect(fallback).toHaveBeenCalledWith({
      category: "TELEMETRY_RECORDER_FAILURE",
      operation: "increment",
      internalCorrelationId: "internal-1",
    });
  });

  it("suppresses recursive and asynchronous fallback failures", async () => {
    const failing = {
      increment: () => Promise.reject(new Error("async failure")),
      observe: () => undefined,
      setGauge: () => undefined,
      emit: () => undefined,
    } as unknown as TelemetryRecorder;
    const fallback = vi.fn(() => Promise.reject(new Error("fallback failure")));
    const safe = safeTelemetryRecorder(failing, { fallback, correlationId: () => "internal-2" });
    expect(() =>
      safe.increment("converge_compaction_runs_total", { outcome: "transient_failure" }),
    ).not.toThrow();
    await vi.waitFor(() => expect(fallback).toHaveBeenCalledOnce());
  });
});
