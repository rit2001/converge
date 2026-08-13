import { describe, expect, it, vi } from "vitest";
import {
  PROMETHEUS_CONTENT_TYPE,
  PrometheusRenderError,
  escapePrometheusLabelValue,
  renderPrometheus,
  safeRenderPrometheus,
} from "./prometheus.js";
import {
  DEFAULT_DURATION_BUCKETS_SECONDS,
  InMemoryTelemetryRecorder,
  type TelemetrySnapshot,
} from "./telemetry.js";

function deepFreeze<T>(value: T): T {
  if (typeof value !== "object" || value === null) return value;
  for (const nested of Object.values(value)) deepFreeze(nested);
  return Object.freeze(value);
}

function replaceSnapshot(
  snapshot: TelemetrySnapshot,
  replacement: Partial<TelemetrySnapshot>,
): TelemetrySnapshot {
  return deepFreeze({ ...snapshot, ...replacement }) as TelemetrySnapshot;
}

function populatedSnapshot(): TelemetrySnapshot {
  const recorder = new InMemoryTelemetryRecorder();
  recorder.increment("converge_delivery_events_total", {
    event_type: "operation",
    outcome: "handled",
  });
  recorder.increment("converge_delivery_events_total", {
    event_type: "membership_revoked",
    outcome: "failed",
  });
  recorder.setGauge("converge_socket_ready", {}, 0);
  recorder.setGauge("converge_outbox_active_work", {}, 3);
  recorder.observe("converge_compaction_duration_seconds", {}, 0.005);
  recorder.observe("converge_compaction_duration_seconds", {}, 0.02);
  recorder.observe("converge_compaction_duration_seconds", {}, 301);
  recorder.emit({
    schemaVersion: 1,
    eventName: "compaction.result",
    severity: "error",
    component: "compaction",
    timestamp: "2026-08-13T10:00:00.000Z",
    code: "PRIVATE_SQL_FAILURE",
    correlation: {
      boardId: "10000000-0000-4000-8000-000000000001",
      snapshotId: "50000000-0000-4000-8000-000000000001",
    },
  });
  return recorder.snapshot();
}

describe("Prometheus text rendering", () => {
  it("renders deterministic counter metadata, labels, values, and ordering", () => {
    const snapshot = populatedSnapshot();
    const first = renderPrometheus(snapshot);
    const second = renderPrometheus(snapshot);

    expect(first).toEqual(second);
    expect(first.contentType).toBe(PROMETHEUS_CONTENT_TYPE);
    expect(first.body).toContain(
      "# HELP converge_delivery_events_total Final distributed delivery event outcomes.\n" +
        "# TYPE converge_delivery_events_total counter\n" +
        'converge_delivery_events_total{event_type="membership_revoked",outcome="failed"} 1\n' +
        'converge_delivery_events_total{event_type="operation",outcome="handled"} 1\n',
    );
    expect(first.body.endsWith("\n")).toBe(true);
    expect(first.body.endsWith("\n\n")).toBe(false);
  });

  it("renders zero readiness and nonnegative active-work gauges", () => {
    const body = renderPrometheus(populatedSnapshot()).body;
    expect(body).toContain(
      "# HELP converge_outbox_active_work Currently active transactional outbox publication attempts.\n" +
        "# TYPE converge_outbox_active_work gauge\n" +
        "converge_outbox_active_work 3\n",
    );
    expect(body).toContain(
      "# HELP converge_socket_ready Whether distributed socket delivery is ready.\n" +
        "# TYPE converge_socket_ready gauge\n" +
        "converge_socket_ready 0\n",
    );
  });

  it("renders exact cumulative histogram buckets, +Inf, sum, and count", () => {
    const body = renderPrometheus(populatedSnapshot()).body;
    const histogramLines = body
      .split("\n")
      .filter((line) => line.startsWith("converge_compaction_duration_seconds"));
    expect(histogramLines).toHaveLength(DEFAULT_DURATION_BUCKETS_SECONDS.length + 3);
    expect(
      histogramLines
        .slice(0, DEFAULT_DURATION_BUCKETS_SECONDS.length)
        .map((line) => line.match(/le="([^"]+)"/)?.[1]),
    ).toEqual(DEFAULT_DURATION_BUCKETS_SECONDS.map(String));
    expect(histogramLines[0]).toBe('converge_compaction_duration_seconds_bucket{le="0.005"} 1');
    expect(histogramLines[2]).toBe('converge_compaction_duration_seconds_bucket{le="0.025"} 2');
    expect(histogramLines[14]).toBe('converge_compaction_duration_seconds_bucket{le="300"} 2');
    expect(histogramLines[15]).toBe('converge_compaction_duration_seconds_bucket{le="+Inf"} 3');
    expect(histogramLines[16]).toBe("converge_compaction_duration_seconds_sum 301.025");
    expect(histogramLines[17]).toBe("converge_compaction_duration_seconds_count 3");
  });

  it("uses locale-independent numbers and escapes Prometheus label values", () => {
    expect(escapePrometheusLabelValue('path\\segment"line\nnext')).toBe(
      'path\\\\segment\\"line\\nnext',
    );
    expect(escapePrometheusLabelValue('path\\segment"line\nnext')).not.toMatch(/[\n\r]/);
    expect(() => escapePrometheusLabelValue("unsafe\rvalue")).toThrow(PrometheusRenderError);
    const body = renderPrometheus(populatedSnapshot()).body;
    expect(body).toContain("301.025");
    expect(body).not.toContain("301,025");
    expect(
      [...body].some((character) => {
        const code = character.charCodeAt(0);
        return (code < 32 && character !== "\n") || code === 127;
      }),
    ).toBe(false);
  });

  it("renders an empty snapshot as exactly one newline and omits empty histograms", () => {
    const empty = new InMemoryTelemetryRecorder().snapshot();
    expect(renderPrometheus(empty)).toEqual({
      contentType: PROMETHEUS_CONTENT_TYPE,
      body: "\n",
    });
  });

  it("does not mutate or retain input evidence and never exports events", () => {
    const snapshot = populatedSnapshot();
    const before = JSON.stringify(snapshot);
    const body = renderPrometheus(snapshot).body;
    expect(JSON.stringify(snapshot)).toBe(before);
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(body).not.toMatch(
      /PRIVATE_SQL_FAILURE|10000000-0000-4000|50000000-0000-4000|boardId|snapshotId/i,
    );
  });
});

describe("Prometheus snapshot validation", () => {
  it("rejects unknown metrics, unexpected labels, type mismatches, and duplicates", () => {
    const snapshot = populatedSnapshot();
    const counter = snapshot.counters[0]!;
    const invalidEntries = [
      { ...counter, name: "converge_dynamic_total" },
      { ...counter, labels: { ...counter.labels, boardId: "private" } },
      { ...counter, name: "converge_socket_ready", labels: {} },
    ];
    for (const entry of invalidEntries)
      expect(() =>
        renderPrometheus(
          replaceSnapshot(snapshot, {
            counters: deepFreeze([deepFreeze(entry)]) as TelemetrySnapshot["counters"],
          }),
        ),
      ).toThrow(PrometheusRenderError);
    expect(() =>
      renderPrometheus(replaceSnapshot(snapshot, { counters: deepFreeze([counter, counter]) })),
    ).toThrow(PrometheusRenderError);
  });

  it("rejects mutable snapshots and invalid numeric evidence", () => {
    const snapshot = populatedSnapshot();
    expect(() => renderPrometheus({ ...snapshot })).toThrow(PrometheusRenderError);
    for (const value of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      const counter = deepFreeze({ ...snapshot.counters[0]!, value });
      expect(() =>
        renderPrometheus(replaceSnapshot(snapshot, { counters: deepFreeze([counter]) })),
      ).toThrow(PrometheusRenderError);
    }
    for (const value of [-1, 1.5, Number.POSITIVE_INFINITY]) {
      const gauge = deepFreeze({ ...snapshot.gauges[0]!, value });
      expect(() =>
        renderPrometheus(replaceSnapshot(snapshot, { gauges: deepFreeze([gauge]) })),
      ).toThrow(PrometheusRenderError);
    }
  });

  it("rejects invalid bucket layouts and nonmonotonic cumulative counts", () => {
    const snapshot = populatedSnapshot();
    const histogram = snapshot.histograms[0]!;
    const wrongBoundary = histogram.buckets.map((bucket, index) =>
      deepFreeze(index === 1 ? { ...bucket, upperBound: 0.02 } : { ...bucket }),
    );
    const nonmonotonic = histogram.buckets.map((bucket, index) =>
      deepFreeze(index === 1 ? { ...bucket, count: 0 } : { ...bucket }),
    );
    for (const buckets of [wrongBoundary, nonmonotonic, histogram.buckets.slice(1)])
      expect(() =>
        renderPrometheus(
          replaceSnapshot(snapshot, {
            histograms: deepFreeze([deepFreeze({ ...histogram, buckets: deepFreeze(buckets) })]),
          }),
        ),
      ).toThrow(PrometheusRenderError);
  });

  it("rejects nonfinite histogram sums and counts", () => {
    const snapshot = populatedSnapshot();
    const histogram = snapshot.histograms[0]!;
    for (const replacement of [
      { ...histogram, sum: Number.NaN },
      { ...histogram, sum: Number.NEGATIVE_INFINITY },
      { ...histogram, count: 0 },
      { ...histogram, count: 1.5 },
    ])
      expect(() =>
        renderPrometheus(
          replaceSnapshot(snapshot, { histograms: deepFreeze([deepFreeze(replacement)]) }),
        ),
      ).toThrow(PrometheusRenderError);
  });
});

describe("safe Prometheus rendering", () => {
  it("contains exporter and synchronous fallback failures once without leaking evidence", () => {
    const snapshot = populatedSnapshot();
    const fallback = vi.fn(() => {
      throw new Error("fallback leaked private snapshot");
    });
    const result = safeRenderPrometheus({ ...snapshot } as TelemetrySnapshot, { fallback });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("Expected safe render failure");
    expect(result.error).toEqual(new PrometheusRenderError("RENDER_FAILED"));
    expect(result.error.message).not.toMatch(/PRIVATE|10000000|snapshot/i);
    expect(fallback).toHaveBeenCalledOnce();
  });

  it("suppresses rejected fallback promises without an unhandled rejection", async () => {
    const fallback = vi.fn(() => Promise.reject(new Error("private fallback rejection")));
    const snapshot = populatedSnapshot();
    const result = safeRenderPrometheus({ ...snapshot } as TelemetrySnapshot, { fallback });
    expect(result.ok).toBe(false);
    await vi.waitFor(() => expect(fallback).toHaveBeenCalledOnce());
  });
});
