import { createHash, timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { safeRenderPrometheus, type TelemetrySnapshot } from "@converge/observability";

export type WorkerOperationalLifecycle =
  | "starting"
  | "ready"
  | "startup_failed"
  | "stopping"
  | "stopped";

export interface WorkerOperationalState {
  transition(next: WorkerOperationalLifecycle): void;
  setCoreReady(ready: boolean): void;
  setRedisReady(ready: boolean): void;
  setOutboxAccepting(accepting: boolean): void;
  isLive(): boolean;
  isCoreReady(): boolean;
  isDeliveryReady(): boolean;
}

export class InstanceWorkerOperationalState implements WorkerOperationalState {
  private lifecycle: WorkerOperationalLifecycle = "starting";
  private coreReady = false;
  private redisReady = false;
  private outboxAccepting = false;

  transition(next: WorkerOperationalLifecycle): void {
    if (this.lifecycle === next || this.lifecycle === "stopped") return;
    const allowed: Readonly<
      Record<WorkerOperationalLifecycle, readonly WorkerOperationalLifecycle[]>
    > = {
      starting: ["ready", "startup_failed", "stopping"],
      ready: ["stopping"],
      startup_failed: ["stopping", "stopped"],
      stopping: ["stopped"],
      stopped: [],
    };
    if (!allowed[this.lifecycle].includes(next)) return;
    this.lifecycle = next;
    if (next === "startup_failed" || next === "stopping" || next === "stopped") {
      this.coreReady = false;
      this.redisReady = false;
      this.outboxAccepting = false;
    }
  }

  setCoreReady(ready: boolean): void {
    if (this.acceptsUpdates()) this.coreReady = ready;
  }

  setRedisReady(ready: boolean): void {
    if (this.acceptsUpdates()) this.redisReady = ready;
  }

  setOutboxAccepting(accepting: boolean): void {
    if (this.acceptsUpdates()) this.outboxAccepting = accepting;
  }

  isLive(): boolean {
    return this.lifecycle === "starting" || this.lifecycle === "ready";
  }

  isCoreReady(): boolean {
    return this.lifecycle === "ready" && this.coreReady;
  }

  isDeliveryReady(): boolean {
    return this.isCoreReady() && this.redisReady && this.outboxAccepting;
  }

  private acceptsUpdates(): boolean {
    return this.lifecycle === "starting" || this.lifecycle === "ready";
  }
}

export interface WorkerOperationalListener {
  start(): Promise<void>;
  close(): Promise<void>;
}

export interface WorkerOperationalListenerConfiguration {
  host: string;
  port: number;
  metricsEnabled: boolean;
  metricsBearerToken: string;
}

export interface WorkerOperationalListenerInput {
  configuration: WorkerOperationalListenerConfiguration;
  state: WorkerOperationalState;
  snapshotTelemetry?: () => TelemetrySnapshot;
}

export type WorkerOperationalListenerFactory = (
  input: WorkerOperationalListenerInput,
) => WorkerOperationalListener;

export class WorkerOperationalListenerStartupError extends Error {
  constructor() {
    super("Worker operational listener failed to start");
  }
}

const liveBody = JSON.stringify({ ok: true, status: "live" });
const readyBody = JSON.stringify({ ok: true, status: "ready" });
const unavailableBody = JSON.stringify({ ok: false, status: "unavailable" });
const unauthorizedBody = JSON.stringify({ ok: false, status: "unauthorized" });
const notFoundBody = JSON.stringify({ ok: false, status: "not_found" });
const methodNotAllowedBody = JSON.stringify({ ok: false, status: "method_not_allowed" });
const internalFailureBody = JSON.stringify({ ok: false, status: "internal_error" });
const metricsUnavailableBody = "Metrics unavailable\n";
const TOKEN_MAXIMUM_BYTES = 256;

function send(
  response: ServerResponse,
  statusCode: number,
  contentType: string,
  body: string,
): void {
  response.writeHead(statusCode, {
    "content-type": contentType,
    "content-length": Buffer.byteLength(body),
    "cache-control": "no-store",
  });
  response.end(body);
}

function sendJson(response: ServerResponse, statusCode: number, body: string): void {
  send(response, statusCode, "application/json; charset=utf-8", body);
}

function tokenIsBounded(value: string): boolean {
  const bytes = Buffer.byteLength(value, "utf8");
  return bytes >= 32 && bytes <= TOKEN_MAXIMUM_BYTES;
}

function authorized(request: IncomingMessage, expected: string): boolean {
  const header = request.headers.authorization;
  if (typeof header !== "string" || !header.startsWith("Bearer ")) return false;
  const provided = header.slice("Bearer ".length);
  if (!tokenIsBounded(provided) || !tokenIsBounded(expected)) return false;
  const providedDigest = createHash("sha256").update(provided, "utf8").digest();
  const expectedDigest = createHash("sha256").update(expected, "utf8").digest();
  return timingSafeEqual(providedDigest, expectedDigest);
}

function requestPath(request: IncomingMessage): string | undefined {
  try {
    return new URL(request.url ?? "", "http://worker.invalid").pathname;
  } catch {
    return undefined;
  }
}

export class NodeWorkerOperationalListener implements WorkerOperationalListener {
  private readonly server: Server;
  private startPromise: Promise<void> | undefined;
  private closePromise: Promise<void> | undefined;

  constructor(private readonly input: WorkerOperationalListenerInput) {
    this.server = createServer((request, response) =>
      handleWorkerOperationalRequest(this.input, request, response),
    );
  }

  start(): Promise<void> {
    this.startPromise ??= new Promise<void>((resolve, reject) => {
      const failed = (): void => {
        this.server.off("listening", listening);
        reject(new WorkerOperationalListenerStartupError());
      };
      const listening = (): void => {
        this.server.off("error", failed);
        resolve();
      };
      this.server.once("error", failed);
      this.server.once("listening", listening);
      this.server.listen(this.input.configuration.port, this.input.configuration.host);
    });
    return this.startPromise;
  }

  close(): Promise<void> {
    this.closePromise ??= new Promise<void>((resolve) => {
      if (!this.server.listening) {
        resolve();
        return;
      }
      this.server.close(() => resolve());
      this.server.closeAllConnections();
    });
    return this.closePromise;
  }

  address(): ReturnType<Server["address"]> {
    return this.server.address();
  }
}

function renderMetrics(
  input: WorkerOperationalListenerInput,
  request: IncomingMessage,
  response: ServerResponse,
): void {
  if (!authorized(request, input.configuration.metricsBearerToken)) {
    sendJson(response, 401, unauthorizedBody);
    return;
  }
  if (input.snapshotTelemetry === undefined) {
    send(response, 503, "text/plain; charset=utf-8", metricsUnavailableBody);
    return;
  }
  let snapshot: TelemetrySnapshot;
  try {
    snapshot = input.snapshotTelemetry();
  } catch {
    send(response, 503, "text/plain; charset=utf-8", metricsUnavailableBody);
    return;
  }
  const rendered = safeRenderPrometheus(snapshot);
  if (!rendered.ok) {
    send(response, 503, "text/plain; charset=utf-8", metricsUnavailableBody);
    return;
  }
  send(response, 200, rendered.value.contentType, rendered.value.body);
}

export function handleWorkerOperationalRequest(
  input: WorkerOperationalListenerInput,
  request: IncomingMessage,
  response: ServerResponse,
): void {
  try {
    if (request.method !== "GET") {
      sendJson(response, 405, methodNotAllowedBody);
      return;
    }
    const path = requestPath(request);
    if (path === "/health/live") {
      const live = input.state.isLive();
      sendJson(response, live ? 200 : 503, live ? liveBody : unavailableBody);
      return;
    }
    if (path === "/health/ready") {
      const ready = input.state.isCoreReady();
      sendJson(response, ready ? 200 : 503, ready ? readyBody : unavailableBody);
      return;
    }
    if (path === "/health/delivery-ready") {
      const ready = input.state.isDeliveryReady();
      sendJson(response, ready ? 200 : 503, ready ? readyBody : unavailableBody);
      return;
    }
    if (path === "/metrics" && input.configuration.metricsEnabled) {
      renderMetrics(input, request, response);
      return;
    }
    sendJson(response, 404, notFoundBody);
  } catch {
    if (!response.headersSent) sendJson(response, 500, internalFailureBody);
    else response.end();
  }
}

export const createNodeWorkerOperationalListener: WorkerOperationalListenerFactory = (input) =>
  new NodeWorkerOperationalListener(input);
