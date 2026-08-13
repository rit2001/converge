import { describe, expect, it, vi } from "vitest";
import { InMemoryTelemetryRecorder, type TelemetryRecorder } from "@converge/observability";
import {
  BoardRecoveryError,
  type DatabasePool,
  type VerifiedBoardRecoveryMaterial,
} from "@converge/database";
import {
  boardRecoveryMaterialSchema,
  httpInternalErrorResponseSchema,
  protocolErrorSchema,
} from "@converge/protocol";
import { buildApp } from "./app.js";
import type { AuthAdapter, AuthenticatedPrincipal } from "./auth.js";
import { parseEnvironment } from "./env.js";

const boardId = "20000000-0000-4000-8000-000000000071";
const principal: AuthenticatedPrincipal = {
  id: "00000000-0000-4000-8000-000000000071",
  displayName: "Recovery tester",
};
const authenticated: AuthAdapter = {
  authenticateHttp: () => Promise.resolve(principal),
  authenticateSocket: () => Promise.resolve(principal),
};
const unauthenticated: AuthAdapter = {
  authenticateHttp: () => Promise.resolve(null),
  authenticateSocket: () => Promise.resolve(null),
};
const environment = parseEnvironment({
  NODE_ENV: "test",
  HOST: "127.0.0.1",
  API_PORT: "4000",
  WEB_ORIGIN: "http://127.0.0.1:3000",
  DATABASE_URL: "postgresql://localhost/converge",
  REDIS_URL: "redis://localhost:6379",
  LOG_LEVEL: "silent",
  DEV_AUTH_USER_NAME: "Unused",
});

function pool(authorized = true): DatabasePool {
  return {
    query: (sql: string) =>
      Promise.resolve({
        rowCount: authorized && sql.includes("FROM board_members") ? 1 : 0,
        rows: authorized && sql.includes("FROM board_members") ? [{ role: "viewer" }] : [],
      }),
  } as unknown as DatabasePool;
}

const material: VerifiedBoardRecoveryMaterial = {
  boardId,
  snapshotId: "50000000-0000-4000-8000-000000000071",
  snapshotSchemaVersion: 1,
  snapshotCanvasSeq: 0,
  snapshotDeliverySeq: 0,
  capturedCanvasSeq: 0,
  capturedDeliverySeq: 0,
  snapshot: {
    id: "50000000-0000-4000-8000-000000000071",
    boardId,
    snapshotSeq: 0,
    snapshotDeliverySeq: 0,
    schemaVersion: 1,
    projection: {
      schemaVersion: 1,
      boardId,
      boardName: "Recovery board",
      lastSeq: 0,
      lastDeliverySeq: 0,
      objects: [],
    },
    canonicalHash: "a".repeat(64),
    objectCount: 0,
    byteSize: 128,
    createdAt: "2026-08-11T10:00:00.000Z",
    verifiedAt: "2026-08-11T10:00:00.001Z",
  },
  snapshotHash: "a".repeat(64),
  operations: [],
  reconstructedState: { lastSeq: 0, objects: {}, order: [] },
  reconstructedHash: "b".repeat(64),
  reconstructedProjectionHash: "c".repeat(64),
};

describe("verified recovery HTTP route", () => {
  it("returns only strict repository-provided recovery material", async () => {
    const load = vi.fn(() => Promise.resolve(material));
    const context = await buildApp(environment, pool(), authenticated, {
      recoveryMaterialRepository: { load },
    });
    try {
      const response = await context.app.inject({
        method: "GET",
        url: `/v1/boards/${boardId}/recovery`,
      });
      expect(response.statusCode).toBe(200);
      expect(boardRecoveryMaterialSchema.parse(response.json())).toMatchObject({
        boardId,
        snapshotId: material.snapshotId,
        operationTail: [],
      });
      expect(load).toHaveBeenCalledExactlyOnceWith(boardId);
    } finally {
      await context.app.close();
    }
  });

  it("preserves authentication, validation, and enumeration boundaries", async () => {
    const load = vi.fn(() => Promise.resolve(material));
    const noAuth = await buildApp(environment, pool(), unauthenticated, {
      recoveryMaterialRepository: { load },
    });
    const noAccess = await buildApp(environment, pool(false), authenticated, {
      recoveryMaterialRepository: { load },
    });
    try {
      const authentication = await noAuth.app.inject({
        method: "GET",
        url: `/v1/boards/${boardId}/recovery`,
      });
      expect(authentication.statusCode).toBe(401);
      expect(protocolErrorSchema.parse(authentication.json()).code).toBe("AUTHENTICATION_REQUIRED");
      for (const id of [boardId, "20000000-0000-4000-8000-000000000099"]) {
        const response = await noAccess.app.inject({
          method: "GET",
          url: `/v1/boards/${id}/recovery`,
        });
        expect(response.statusCode).toBe(404);
        expect(protocolErrorSchema.parse(response.json())).toMatchObject({
          code: "BOARD_NOT_FOUND",
          retryable: false,
        });
      }
      for (const url of [
        "/v1/boards/not-a-uuid/recovery",
        `/v1/boards/${boardId}/recovery?unknown=true`,
      ]) {
        const response = await noAccess.app.inject({ method: "GET", url });
        expect(response.statusCode).toBe(400);
        expect(protocolErrorSchema.parse(response.json())).toMatchObject({
          code: "INVALID_COMMAND",
          retryable: false,
        });
      }
      expect(load).not.toHaveBeenCalled();
    } finally {
      await Promise.all([noAuth.app.close(), noAccess.app.close()]);
    }
  });

  it.each([
    "MISSING_REQUIRED_SNAPSHOT",
    "SNAPSHOT_BELOW_RECOVERY_FLOOR",
    "SNAPSHOT_CORRUPT",
    "UNSUPPORTED_SNAPSHOT_VERSION",
    "TAIL_GAP",
    "TAIL_ORDER_CONFLICT",
    "TAIL_LIMIT_EXCEEDED",
    "REDUCER_FAILURE",
    "PROJECTION_MISMATCH",
    "CANONICAL_HASH_MISMATCH",
  ] as const)("maps durable %s evidence to the fixed 409 contract", async (code) => {
    const context = await buildApp(environment, pool(), authenticated, {
      recoveryMaterialRepository: {
        load: () => Promise.reject(new BoardRecoveryError(code)),
      },
    });
    try {
      const response = await context.app.inject({
        method: "GET",
        url: `/v1/boards/${boardId}/recovery`,
      });
      expect(response.statusCode).toBe(409);
      expect(protocolErrorSchema.parse(response.json())).toEqual({
        ok: false,
        code: "RECOVERY_BLOCKED",
        message: "Authoritative board recovery is unavailable",
        retryable: false,
      });
      expect(response.body).not.toContain(code);
    } finally {
      await context.app.close();
    }
  });

  it("keeps infrastructure failures on the existing sanitized retryable 500 path", async () => {
    const privateFailure = new Error("connection password at SELECT private_snapshot");
    const context = await buildApp(environment, pool(), authenticated, {
      recoveryMaterialRepository: { load: () => Promise.reject(privateFailure) },
    });
    try {
      const response = await context.app.inject({
        method: "GET",
        url: `/v1/boards/${boardId}/recovery`,
      });
      expect(response.statusCode).toBe(500);
      expect(httpInternalErrorResponseSchema.parse(response.json())).toMatchObject({
        code: "INTERNAL_ERROR",
        retryable: true,
      });
      expect(response.body).not.toContain(privateFailure.message);
    } finally {
      await context.app.close();
    }
  });

  it("classifies every recovery result once and observes complete monotonic duration", async () => {
    const secretFailure = "connection password at SELECT private_snapshot";
    const cases = [
      {
        outcome: "snapshot_tail",
        auth: authenticated,
        database: pool(),
        repository: { load: () => Promise.resolve(material) },
        status: 200,
      },
      {
        outcome: "refreshed",
        auth: authenticated,
        database: pool(),
        repository: {
          load: () => Promise.resolve(material),
          loadWithOutcome: () => Promise.resolve({ material, outcome: "refreshed" as const }),
        },
        status: 200,
      },
      {
        outcome: "recovery_blocked",
        auth: authenticated,
        database: pool(),
        repository: {
          load: () => Promise.reject(new BoardRecoveryError("SNAPSHOT_CORRUPT")),
        },
        status: 409,
      },
      {
        outcome: "retryable_failure",
        auth: authenticated,
        database: pool(),
        repository: { load: () => Promise.reject(new Error(secretFailure)) },
        status: 500,
      },
      {
        outcome: "authorization_failure",
        auth: unauthenticated,
        database: pool(),
        repository: { load: () => Promise.resolve(material) },
        status: 401,
      },
    ] as const;

    for (const testCase of cases) {
      const telemetry = new InMemoryTelemetryRecorder();
      const times = [1_000, 1_250];
      const context = await buildApp(environment, testCase.database, testCase.auth, {
        recoveryMaterialRepository: testCase.repository,
        telemetry,
        telemetryClock: { now: () => times.shift() ?? 1_250 },
      });
      const response = await context.app.inject({
        method: "GET",
        url: `/v1/boards/${boardId}/recovery`,
      });
      expect(response.statusCode).toBe(testCase.status);
      await context.app.close();

      const snapshot = telemetry.snapshot();
      expect(
        snapshot.counters.filter(({ name }) => name === "converge_recovery_requests_total"),
      ).toEqual([
        {
          name: "converge_recovery_requests_total",
          labels: { outcome: testCase.outcome },
          value: 1,
        },
      ]);
      expect(
        snapshot.histograms.find(({ name }) => name === "converge_recovery_duration_seconds"),
      ).toMatchObject({ count: 1, sum: 0.25, labels: {} });
      expect(
        snapshot.events.filter(({ eventName }) => eventName === "recovery.request.result"),
      ).toMatchObject([{ code: testCase.outcome.toUpperCase(), component: "recovery" }]);
      const exported = JSON.stringify(snapshot);
      for (const forbidden of [
        boardId,
        material.snapshotId,
        material.snapshotHash,
        material.reconstructedHash,
        secretFailure,
      ])
        expect(exported).not.toContain(forbidden);
    }
  });

  it("keeps the recovery response unchanged when telemetry dependencies throw or reject", async () => {
    const reject = () => Promise.reject(new Error("private telemetry failure"));
    const failingTelemetry = {
      increment: () => {
        throw new Error("private counter failure");
      },
      observe: reject,
      setGauge: () => {
        throw new Error("private gauge failure");
      },
      emit: reject,
    } as unknown as TelemetryRecorder;
    const context = await buildApp(environment, pool(), authenticated, {
      recoveryMaterialRepository: { load: () => Promise.resolve(material) },
      telemetry: failingTelemetry,
      telemetryClock: {
        now: () => {
          throw new Error("private clock failure");
        },
      },
    });
    try {
      const response = await context.app.inject({
        method: "GET",
        url: `/v1/boards/${boardId}/recovery`,
      });
      expect(response.statusCode).toBe(200);
      expect(boardRecoveryMaterialSchema.parse(response.json()).boardId).toBe(boardId);
      await Promise.resolve();
    } finally {
      await context.app.close();
    }
  });
});
