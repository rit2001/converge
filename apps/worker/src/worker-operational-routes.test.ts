import type { IncomingMessage, ServerResponse } from "node:http";
import { describe, expect, it, vi } from "vitest";
import {
  InMemoryTelemetryRecorder,
  PROMETHEUS_CONTENT_TYPE,
  renderPrometheus,
  type TelemetrySnapshot,
} from "@converge/observability";
import {
  InstanceWorkerOperationalState,
  NodeWorkerOperationalListener,
  handleWorkerOperationalRequest,
  type WorkerOperationalListenerInput,
} from "./operational.js";

const token = "worker-operational-token-value-32";

function setup(input: { metricsEnabled?: boolean; snapshotTelemetry?: () => TelemetrySnapshot }) {
  const state = new InstanceWorkerOperationalState();
  const configuration = {
    host: "127.0.0.1",
    port: 9_091,
    metricsEnabled: input.metricsEnabled ?? false,
    metricsBearerToken: token,
  };
  return {
    state,
    input: {
      configuration,
      state,
      ...(input.snapshotTelemetry === undefined
        ? {}
        : { snapshotTelemetry: input.snapshotTelemetry }),
    } satisfies WorkerOperationalListenerInput,
  };
}

function request(
  input: WorkerOperationalListenerInput,
  url: string,
  options: { method?: string; authorization?: string } = {},
) {
  let statusCode = 0;
  let headers: Record<string, string | number> = {};
  let body = "";
  const fakeResponse = {
    headersSent: false,
    writeHead(status: number, nextHeaders: Record<string, string | number>) {
      statusCode = status;
      headers = nextHeaders;
      this.headersSent = true;
      return this;
    },
    end(value?: string) {
      body = value ?? "";
      return this;
    },
  };
  handleWorkerOperationalRequest(
    input,
    {
      method: options.method ?? "GET",
      url,
      headers: { authorization: options.authorization },
    } as IncomingMessage,
    fakeResponse as unknown as ServerResponse,
  );
  return { statusCode, headers, body };
}

describe("worker operational HTTP listener", () => {
  it("keeps process, core, and delivery health meanings distinct", () => {
    const test = setup({});
    expect(request(test.input, "/health/live").statusCode).toBe(200);
    expect(request(test.input, "/health/ready").statusCode).toBe(503);
    expect(request(test.input, "/health/delivery-ready").statusCode).toBe(503);

    test.state.setCoreReady(true);
    test.state.setOutboxAccepting(true);
    test.state.transition("ready");
    expect(request(test.input, "/health/live").statusCode).toBe(200);
    expect(request(test.input, "/health/ready").statusCode).toBe(200);
    expect(request(test.input, "/health/delivery-ready").statusCode).toBe(503);
    test.state.setRedisReady(true);
    expect(request(test.input, "/health/delivery-ready").statusCode).toBe(200);
    test.state.setRedisReady(false);
    expect(request(test.input, "/health/ready").statusCode).toBe(200);
    expect(request(test.input, "/health/delivery-ready").statusCode).toBe(503);
    test.state.transition("stopping");
    for (const path of ["live", "ready", "delivery-ready"])
      expect(request(test.input, `/health/${path}`).statusCode).toBe(503);
    test.state.setCoreReady(true);
    test.state.setRedisReady(true);
    test.state.setOutboxAccepting(true);
    expect(test.state.isCoreReady()).toBe(false);
    expect(test.state.isDeliveryReady()).toBe(false);
  });

  it("returns fixed bounded 404 and 405 responses without request work", () => {
    const test = setup({});
    expect(request(test.input, "/unknown")).toMatchObject({
      statusCode: 404,
      body: JSON.stringify({ ok: false, status: "not_found" }),
    });
    expect(request(test.input, "/health/live", { method: "POST" })).toMatchObject({
      statusCode: 405,
      body: JSON.stringify({ ok: false, status: "method_not_allowed" }),
    });
  });

  it("does not expose metrics when disabled", () => {
    const snapshotTelemetry = vi.fn();
    const test = setup({ snapshotTelemetry });
    const response = request(test.input, "/metrics", {
      authorization: `Bearer ${token}`,
    });
    expect(response.statusCode).toBe(404);
    expect(response.body).not.toContain("converge_");
    expect(snapshotTelemetry).not.toHaveBeenCalled();
  });

  it("protects deterministic Prometheus output with timing-safe bounded credentials", () => {
    const recorder = new InMemoryTelemetryRecorder();
    recorder.increment("converge_outbox_publications_total", { outcome: "published" });
    const snapshotTelemetry = vi.fn(() => recorder.snapshot());
    const test = setup({ metricsEnabled: true, snapshotTelemetry });
    const missing = request(test.input, "/metrics");
    const incorrect = request(test.input, "/metrics", {
      authorization: `Bearer ${"x".repeat(40)}`,
    });
    const differentLength = request(test.input, "/metrics", {
      authorization: `Bearer ${"y".repeat(200)}`,
    });
    expect(missing.statusCode).toBe(401);
    expect(incorrect.statusCode).toBe(401);
    expect(differentLength.statusCode).toBe(401);
    expect(missing.body).toBe(incorrect.body);
    expect(missing.body).not.toContain(token);
    expect(JSON.stringify(recorder.snapshot())).not.toContain(token);

    const before = recorder.snapshot();
    const expected = renderPrometheus(before).body;
    const authorized = { authorization: `Bearer ${token}` };
    const first = request(test.input, "/metrics", authorized);
    const second = request(test.input, "/metrics", authorized);
    expect(first.statusCode).toBe(200);
    expect(first.headers["content-type"]).toBe(PROMETHEUS_CONTENT_TYPE);
    expect(first.body).toBe(expected);
    expect(second.body).toBe(first.body);
    expect(recorder.snapshot()).toEqual(before);
    expect(snapshotTelemetry).toHaveBeenCalledTimes(2);
  });

  it("contains snapshot, renderer, and request failures without leaking recorder evidence", () => {
    const recorder = new InMemoryTelemetryRecorder();
    recorder.increment("converge_snapshot_runs_total", { outcome: "captured" });
    const before = recorder.snapshot();
    for (const snapshotTelemetry of [
      () => {
        throw new Error("private token URL SQL payload");
      },
      () => ({ ...recorder.snapshot() }) as TelemetrySnapshot,
    ]) {
      const test = setup({ metricsEnabled: true, snapshotTelemetry });
      const response = request(test.input, "/metrics", {
        authorization: `Bearer ${token}`,
      });
      expect(response.statusCode).toBe(503);
      expect(response.body).toBe("Metrics unavailable\n");
      expect(recorder.snapshot()).toEqual(before);
    }

    const broken = setup({});
    broken.input.state.isLive = () => {
      throw new Error("private dependency failure");
    };
    expect(request(broken.input, "/health/live")).toMatchObject({
      statusCode: 500,
      body: JSON.stringify({ ok: false, status: "internal_error" }),
    });
  });

  it("closes an unstarted Node listener idempotently", async () => {
    const test = setup({});
    const listener = new NodeWorkerOperationalListener(test.input);
    await listener.close();
    await listener.close();
  });
});
