import { Writable } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import type { DatabasePool } from "@converge/database";
import {
  InMemoryTelemetryRecorder,
  PROMETHEUS_CONTENT_TYPE,
  renderPrometheus,
  type TelemetrySnapshot,
} from "@converge/observability";
import { buildApp, type BuildAppOptions } from "./app.js";
import { InstanceApiOperationalState, type ApiOperationalState } from "./api-operational-state.js";
import type { AuthAdapter } from "./auth.js";
import type {
  BoardDeliveryHeadWatchdogLifecycleEvent,
  BoardDeliveryHeadWatchdogObserver,
} from "./board-delivery-head-watchdog.js";
import type { DeliveryRuntimeLifecycleEvent, DeliveryRuntimeObserver } from "./delivery-runtime.js";
import { parseEnvironment, type Environment } from "./env.js";

const metricsToken = "bounded-api-metrics-token";

function environment(metricsEnabled = false): Environment {
  return parseEnvironment({
    NODE_ENV: "test",
    HOST: "127.0.0.1",
    API_PORT: "4000",
    WEB_ORIGIN: "http://127.0.0.1:3000",
    DATABASE_URL: "postgresql://unused",
    LOG_LEVEL: "silent",
    API_METRICS_ENABLED: metricsEnabled ? "true" : "false",
    API_METRICS_BEARER_TOKEN: metricsEnabled ? metricsToken : "",
  });
}

const pool = {} as DatabasePool;
const authenticateHttp = vi.fn(() => Promise.resolve(null));
const authenticateSocket = vi.fn(() => Promise.resolve(null));
const auth: AuthAdapter = {
  authenticateHttp,
  authenticateSocket,
};

function distributedHarness() {
  let runtimeObserver: DeliveryRuntimeObserver | undefined;
  let watchdogObserver: BoardDeliveryHeadWatchdogObserver | undefined;
  const startRuntime = vi.fn(() => Promise.resolve());
  const stopRuntime = vi.fn(() => Promise.resolve());
  const startWatchdog = vi.fn(() => Promise.resolve());
  const stopWatchdog = vi.fn(() => Promise.resolve());
  const options: BuildAppOptions = {
    deliveryMode: {
      mode: "distributed",
      createRuntime: (_handlers, observer) => {
        runtimeObserver = observer;
        return { start: startRuntime, stop: stopRuntime };
      },
    },
    createBoardDeliveryHeadWatchdog: (input) => {
      watchdogObserver = input.observer;
      return { start: startWatchdog, stop: stopWatchdog };
    },
  };
  return {
    options,
    consumer: (event: DeliveryRuntimeLifecycleEvent) =>
      Promise.resolve(runtimeObserver?.lifecycle(event)),
    watchdog: (event: BoardDeliveryHeadWatchdogLifecycleEvent) =>
      Promise.resolve(watchdogObserver?.lifecycle(event)),
  };
}

function captureLogs(): { output: () => string; stream: Writable } {
  let output = "";
  return {
    output: () => output,
    stream: new Writable({
      write(chunk, _encoding, callback) {
        output += String(chunk);
        callback();
      },
    }),
  };
}

describe("API operational health routes", () => {
  it("keeps liveness distinct from startup and stopping readiness", async () => {
    const startingState: ApiOperationalState = {
      transition: vi.fn(),
      setSocketReady: vi.fn(),
      isLive: () => true,
      isHttpReady: () => false,
      isSocketReady: () => false,
    };
    const starting = await buildApp(environment(), pool, auth, {
      operationalState: startingState,
      healthProbe: { check: vi.fn() },
    });
    try {
      expect((await starting.app.inject({ method: "GET", url: "/health/live" })).json()).toEqual({
        ok: true,
        status: "live",
      });
      expect((await starting.app.inject({ method: "GET", url: "/health/ready" })).statusCode).toBe(
        503,
      );
      expect(
        (await starting.app.inject({ method: "GET", url: "/health/socket-ready" })).statusCode,
      ).toBe(503);
    } finally {
      await starting.app.close();
    }

    const stoppingState = new InstanceApiOperationalState(true);
    const stopping = await buildApp(environment(), pool, auth, {
      operationalState: stoppingState,
      healthProbe: { check: vi.fn() },
    });
    try {
      await stopping.app.ready();
      expect((await stopping.app.inject({ method: "GET", url: "/health/live" })).statusCode).toBe(
        200,
      );
      stoppingState.transition("stopping");
      for (const url of ["/health/live", "/health/ready", "/health/socket-ready"])
        expect((await stopping.app.inject({ method: "GET", url })).statusCode).toBe(503);
    } finally {
      await stopping.app.close();
    }
  });

  it("probes PostgreSQL once and contains rejection without affecting ordinary HTTP", async () => {
    const healthyProbe = vi.fn(() => Promise.resolve());
    const healthy = await buildApp(environment(), pool, auth, {
      healthProbe: { check: healthyProbe },
    });
    try {
      const response = await healthy.app.inject({ method: "GET", url: "/health/ready" });
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ ok: true, status: "ready" });
      expect(healthyProbe).toHaveBeenCalledOnce();
    } finally {
      await healthy.app.close();
    }

    const rejectedProbe = vi.fn(() => Promise.reject(new Error("private database URL")));
    const rejected = await buildApp(environment(), pool, auth, {
      healthProbe: { check: rejectedProbe },
    });
    try {
      const disconnectSockets = vi.spyOn(rejected.io.local, "disconnectSockets");
      const response = await rejected.app.inject({ method: "GET", url: "/health/ready" });
      expect(response.statusCode).toBe(503);
      expect(response.json()).toEqual({ ok: false, status: "unavailable" });
      expect(rejectedProbe).toHaveBeenCalledOnce();
      expect(disconnectSockets).not.toHaveBeenCalled();
      expect((await rejected.app.inject({ method: "GET", url: "/health" })).statusCode).toBe(200);
    } finally {
      await rejected.app.close();
    }
  });

  it("bounds a hanging PostgreSQL probe with a deterministic timeout", async () => {
    vi.useFakeTimers();
    const probe = vi.fn(() => new Promise<void>(() => undefined));
    const context = await buildApp(environment(), pool, auth, {
      healthProbe: { check: probe },
      healthProbeTimeoutMs: 25,
    });
    try {
      await context.app.ready();
      const pending = context.app.inject({ method: "GET", url: "/health/ready" });
      await vi.advanceTimersByTimeAsync(25);
      const response = await pending;
      expect(response.statusCode).toBe(503);
      expect(response.json()).toEqual({ ok: false, status: "unavailable" });
      expect(probe).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
      await context.app.close();
    }
  });

  it("keeps HTTP readiness independent while consumer and watchdog compose socket readiness", async () => {
    const lifecycle = distributedHarness();
    const operationalState = new InstanceApiOperationalState(false);
    const healthProbe = vi.fn(() => Promise.resolve());
    const context = await buildApp(environment(), pool, auth, {
      ...lifecycle.options,
      operationalState,
      healthProbe: { check: healthProbe },
    });
    try {
      await context.app.ready();
      expect(
        (await context.app.inject({ method: "GET", url: "/health/socket-ready" })).statusCode,
      ).toBe(503);
      expect((await context.app.inject({ method: "GET", url: "/health/ready" })).statusCode).toBe(
        200,
      );

      await lifecycle.consumer({ state: "established" });
      expect(
        (await context.app.inject({ method: "GET", url: "/health/socket-ready" })).statusCode,
      ).toBe(200);
      await lifecycle.watchdog({
        state: "unavailable",
        code: "DATABASE_CHECK_FAILED",
        boardIds: [],
      });
      expect(
        (await context.app.inject({ method: "GET", url: "/health/socket-ready" })).statusCode,
      ).toBe(503);
      expect((await context.app.inject({ method: "GET", url: "/health/ready" })).statusCode).toBe(
        200,
      );

      await lifecycle.consumer({ state: "unavailable", code: "REDIS_UNAVAILABLE" });
      await lifecycle.watchdog({ state: "recovered" });
      expect(
        (await context.app.inject({ method: "GET", url: "/health/socket-ready" })).statusCode,
      ).toBe(503);
      await lifecycle.consumer({ state: "recovered" });
      expect(
        (await context.app.inject({ method: "GET", url: "/health/socket-ready" })).statusCode,
      ).toBe(200);

      operationalState.transition("stopping");
      await lifecycle.consumer({ state: "recovered" });
      await lifecycle.watchdog({ state: "recovered" });
      expect(
        (await context.app.inject({ method: "GET", url: "/health/socket-ready" })).statusCode,
      ).toBe(503);
      await context.app.close();
      await lifecycle.consumer({ state: "recovered" });
      await lifecycle.watchdog({ state: "recovered" });
      expect(operationalState.isSocketReady()).toBe(false);
    } finally {
      await context.app.close();
    }
  });

  it("reports local socket readiness without PostgreSQL or Redis work", async () => {
    const probe = vi.fn();
    const context = await buildApp(environment(), pool, auth, { healthProbe: { check: probe } });
    try {
      const response = await context.app.inject({ method: "GET", url: "/health/socket-ready" });
      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({ ok: true, status: "ready" });
      expect(probe).not.toHaveBeenCalled();
      expect(authenticateHttp).not.toHaveBeenCalled();
    } finally {
      await context.app.close();
    }
  });
});

describe("protected API metrics route", () => {
  it("leaves metrics absent unless explicitly enabled", async () => {
    const recorder = new InMemoryTelemetryRecorder();
    const context = await buildApp(environment(), pool, auth, { telemetry: recorder });
    try {
      const response = await context.app.inject({ method: "GET", url: "/metrics" });
      expect(response.statusCode).toBe(404);
      expect(response.body).not.toContain("converge_");
    } finally {
      await context.app.close();
    }
  });

  it("makes missing and unequal-length incorrect tokens indistinguishable", async () => {
    const recorder = new InMemoryTelemetryRecorder();
    const context = await buildApp(environment(true), pool, auth, { telemetry: recorder });
    try {
      const missing = await context.app.inject({ method: "GET", url: "/metrics" });
      const incorrect = await context.app.inject({
        method: "GET",
        url: "/metrics",
        headers: { authorization: "Bearer wrong" },
      });
      const oversized = await context.app.inject({
        method: "GET",
        url: "/metrics",
        headers: { authorization: `Bearer ${"x".repeat(300)}` },
      });
      expect(missing.statusCode).toBe(401);
      expect(incorrect.statusCode).toBe(401);
      expect(oversized.statusCode).toBe(401);
      expect(missing.body).toBe(incorrect.body);
      expect(missing.body).toBe(oversized.body);
      expect(missing.json()).toEqual({ ok: false, status: "unauthorized" });
    } finally {
      await context.app.close();
    }
  });

  it("returns exact deterministic Prometheus output without mutating telemetry", async () => {
    const recorder = new InMemoryTelemetryRecorder();
    recorder.increment("converge_delivery_events_total", {
      event_type: "operation",
      outcome: "handled",
    });
    const context = await buildApp(environment(true), pool, auth, { telemetry: recorder });
    try {
      await context.app.ready();
      const before = recorder.snapshot();
      const expected = renderPrometheus(before).body;
      const request = {
        method: "GET" as const,
        url: "/metrics",
        headers: { authorization: `Bearer ${metricsToken}` },
      };
      const first = await context.app.inject(request);
      const second = await context.app.inject(request);
      expect(first.statusCode).toBe(200);
      expect(first.headers["content-type"]).toBe(PROMETHEUS_CONTENT_TYPE);
      expect(first.body).toBe(expected);
      expect(second.body).toBe(first.body);
      expect(recorder.snapshot()).toEqual(before);
      expect(authenticateHttp).not.toHaveBeenCalled();
    } finally {
      await context.app.close();
    }
  });

  it("never logs or records bearer tokens on authorization failure", async () => {
    const secret = "incorrect-private-operational-token";
    const recorder = new InMemoryTelemetryRecorder();
    const logs = captureLogs();
    const configured = parseEnvironment({
      NODE_ENV: "test",
      DATABASE_URL: "postgresql://unused",
      API_METRICS_ENABLED: "true",
      API_METRICS_BEARER_TOKEN: metricsToken,
      LOG_LEVEL: "info",
    });
    const context = await buildApp(configured, pool, auth, {
      telemetry: recorder,
      loggerStream: logs.stream,
    });
    try {
      const response = await context.app.inject({
        method: "GET",
        url: "/metrics",
        headers: { authorization: `Bearer ${secret}` },
      });
      expect(response.statusCode).toBe(401);
      expect(response.body).not.toContain(secret);
      expect(logs.output()).not.toContain(secret);
      expect(JSON.stringify(recorder.snapshot())).not.toContain(secret);
    } finally {
      await context.app.close();
    }
  });

  it.each([
    [
      "snapshot",
      () => {
        throw new Error("private snapshot failure");
      },
    ],
    [
      "renderer",
      (recorder: InMemoryTelemetryRecorder) => ({ ...recorder.snapshot() }) as TelemetrySnapshot,
    ],
  ])(
    "contains %s failure and preserves recorder and ordinary HTTP state",
    async (_name, source) => {
      const recorder = new InMemoryTelemetryRecorder();
      const context = await buildApp(environment(true), pool, auth, {
        telemetry: recorder,
        telemetrySnapshot: () => source(recorder),
      });
      try {
        await context.app.ready();
        const before = recorder.snapshot();
        const response = await context.app.inject({
          method: "GET",
          url: "/metrics",
          headers: { authorization: `Bearer ${metricsToken}` },
        });
        expect(response.statusCode).toBe(503);
        expect(response.body).toBe("Metrics unavailable\n");
        expect(response.body).not.toMatch(/private|snapshot|renderer/i);
        expect(recorder.snapshot()).toEqual(before);
        expect((await context.app.inject({ method: "GET", url: "/health" })).statusCode).toBe(200);
        expect(
          (
            await context.app.inject({
              method: "POST",
              url: "/v1/boards",
              payload: { name: "ordinary-route" },
            })
          ).statusCode,
        ).toBe(401);
      } finally {
        await context.app.close();
      }
    },
  );
});
