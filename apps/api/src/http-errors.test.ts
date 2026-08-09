import { Writable } from "node:stream";
import { describe, expect, it } from "vitest";
import { RepositoryError, type DatabasePool } from "@converge/database";
import { httpInternalErrorResponseSchema, protocolErrorSchema } from "@converge/protocol";
import { buildApp } from "./app.js";
import type { AuthAdapter, AuthenticatedPrincipal } from "./auth.js";
import type { Environment } from "./env.js";

const principal: AuthenticatedPrincipal = {
  id: "00000000-0000-4000-8000-000000000091",
  displayName: "HTTP Error Tester",
};

const authenticated: AuthAdapter = {
  authenticateHttp: () => Promise.resolve(principal),
  authenticateSocket: () => Promise.resolve(principal),
};

const unauthenticated: AuthAdapter = {
  authenticateHttp: () => Promise.resolve(null),
  authenticateSocket: () => Promise.resolve(null),
};

function environment(
  nodeEnvironment: Environment["NODE_ENV"],
  logLevel: Environment["LOG_LEVEL"] = "silent",
): Environment {
  return {
    NODE_ENV: nodeEnvironment,
    HOST: "127.0.0.1",
    API_PORT: 4000,
    WEB_ORIGIN: "http://127.0.0.1:3000",
    DATABASE_URL: "postgresql://localhost/converge",
    REDIS_URL: "redis://localhost:6379",
    LOG_LEVEL: logLevel,
    DEV_AUTH_USER_NAME: "Unused",
  };
}

function failingPool(error: Error): DatabasePool {
  return { connect: () => Promise.reject(error) } as unknown as DatabasePool;
}

function queryPool(rows: unknown[] = []): DatabasePool {
  return {
    query: () => Promise.resolve({ rowCount: rows.length, rows }),
  } as unknown as DatabasePool;
}

function captureLogs(): { entries: Record<string, unknown>[]; stream: Writable } {
  const entries: Record<string, unknown>[] = [];
  let pending = "";
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      pending += String(chunk);
      const lines = pending.split("\n");
      pending = lines.pop() ?? "";
      for (const line of lines) if (line) entries.push(JSON.parse(line) as Record<string, unknown>);
      callback();
    },
  });
  return { entries, stream };
}

describe("HTTP error sanitization", () => {
  it("returns a strict sanitized client error for malformed JSON", async () => {
    const context = await buildApp(environment("test"), queryPool(), authenticated);
    try {
      const malformed = await context.app.inject({
        method: "POST",
        url: "/v1/boards",
        headers: { "content-type": "application/json" },
        payload: '{"name":"private-parser-fragment"',
      });
      expect(malformed.statusCode).toBe(400);
      expect(protocolErrorSchema.parse(malformed.json())).toEqual({
        ok: false,
        code: "INVALID_COMMAND",
        message: "Request body is invalid",
        retryable: false,
      });
      for (const forbidden of [
        "FST_ERR",
        "SyntaxError",
        "Unexpected",
        "private-parser-fragment",
        "/Users/",
        "node_modules",
      ])
        expect(malformed.body).not.toContain(forbidden);

      const health = await context.app.inject({ method: "GET", url: "/health" });
      expect(health.statusCode).toBe(200);
      expect(health.json()).toEqual({ ok: true });
    } finally {
      await context.io.close();
      await context.app.close();
    }
  });

  it("returns a strict sanitized client error for an oversized JSON body", async () => {
    const context = await buildApp(environment("test"), queryPool(), authenticated);
    const oversizedMarker = "private-oversized-payload";
    try {
      const oversized = await context.app.inject({
        method: "POST",
        url: "/v1/boards",
        headers: { "content-type": "application/json" },
        payload: JSON.stringify({ name: `${oversizedMarker}${"x".repeat(70 * 1024)}` }),
      });
      expect(oversized.statusCode).toBe(413);
      expect(protocolErrorSchema.parse(oversized.json())).toEqual({
        ok: false,
        code: "PAYLOAD_TOO_LARGE",
        message: "Request body exceeds the maximum allowed size",
        retryable: false,
      });
      for (const forbidden of ["FST_ERR", "RangeError", oversizedMarker, "/Users/", "node_modules"])
        expect(oversized.body).not.toContain(forbidden);
    } finally {
      await context.io.close();
      await context.app.close();
    }
  });

  it("preserves rate-limit handling and rejects forged client-error identity", async () => {
    const rateLimitContext = await buildApp(environment("test"), queryPool(), authenticated);
    const forged = Object.assign(
      new Error("private forged parser error at /srv/converge/parser.ts"),
      {
        statusCode: 413,
        code: "FST_ERR_CTP_BODY_TOO_LARGE",
      },
    );
    const forgedContext = await buildApp(environment("test"), failingPool(forged), authenticated);
    try {
      let limited;
      for (let request = 0; request <= 120; request += 1)
        limited = await rateLimitContext.app.inject({ method: "GET", url: "/health" });
      expect(limited?.statusCode).toBe(429);
      expect(protocolErrorSchema.parse(limited?.json())).toEqual({
        ok: false,
        code: "RATE_LIMITED",
        message: "Request rate exceeded",
        retryable: true,
      });

      const response = await forgedContext.app.inject({
        method: "POST",
        url: "/v1/boards",
        payload: { name: "Valid board" },
      });
      expect(response.statusCode).toBe(500);
      expect(httpInternalErrorResponseSchema.parse(response.json())).toMatchObject({
        ok: false,
        code: "INTERNAL_ERROR",
        retryable: true,
      });
      expect(response.body).not.toContain(forged.message);
      expect(response.body).not.toContain(forged.code);
    } finally {
      await Promise.all([rateLimitContext.io.close(), forgedContext.io.close()]);
      await Promise.all([rateLimitContext.app.close(), forgedContext.app.close()]);
    }
  });

  it.each(["development", "production"] as const)(
    "returns the same sanitized 500 contract in %s and keeps bounded server evidence",
    async (nodeEnvironment) => {
      const distinctive =
        "SELECT password FROM private_accounts at /srv/converge/internal/repository.ts";
      const logs = captureLogs();
      const context = await buildApp(
        environment(nodeEnvironment, "info"),
        failingPool(new Error(distinctive)),
        authenticated,
        { loggerStream: logs.stream },
      );
      try {
        const response = await context.app.inject({
          method: "POST",
          url: "/v1/boards",
          headers: { authorization: "Bearer caller-secret" },
          payload: { name: "sensitive-request-body" },
        });
        expect(response.statusCode).toBe(500);
        const body = httpInternalErrorResponseSchema.parse(response.json());
        expect(body).toMatchObject({
          ok: false,
          code: "INTERNAL_ERROR",
          message: "An internal server error occurred.",
          retryable: true,
        });
        const publicResponse = response.body;
        for (const forbidden of [
          distinctive,
          "SELECT",
          "private_accounts",
          "/srv/converge",
          "password",
          "caller-secret",
          "sensitive-request-body",
        ])
          expect(publicResponse).not.toContain(forbidden);

        await new Promise<void>((resolve) => setImmediate(resolve));
        const logged = logs.entries.find(
          (entry) => entry.msg === "unexpected HTTP request failure",
        );
        expect(logged).toMatchObject({ requestId: body.requestId });
        expect(JSON.stringify(logged)).toContain(distinctive);
        const allLogs = JSON.stringify(logs.entries);
        expect(allLogs).not.toContain("caller-secret");
        expect(allLogs).not.toContain("sensitive-request-body");
      } finally {
        await context.io.close();
        await context.app.close();
      }
    },
  );

  it("preserves known authentication, validation, domain, and enumeration responses", async () => {
    const authenticationContext = await buildApp(environment("test"), queryPool(), unauthenticated);
    const validationContext = await buildApp(environment("test"), queryPool(), authenticated);
    const domainContext = await buildApp(
      environment("test"),
      failingPool(new RepositoryError("CONFLICT", "Known board conflict")),
      authenticated,
    );
    try {
      const authentication = await authenticationContext.app.inject({
        method: "POST",
        url: "/v1/boards",
        payload: { name: "Board" },
      });
      expect(authentication.statusCode).toBe(401);
      expect(protocolErrorSchema.parse(authentication.json())).toEqual({
        ok: false,
        code: "AUTHENTICATION_REQUIRED",
        message: "Authentication required",
        retryable: false,
      });

      const validation = await validationContext.app.inject({
        method: "POST",
        url: "/v1/boards",
        payload: { name: "", unknown: true },
      });
      expect(validation.statusCode).toBe(400);
      expect(protocolErrorSchema.parse(validation.json())).toEqual({
        ok: false,
        code: "INVALID_COMMAND",
        message: "Request validation failed",
        retryable: false,
      });

      const domain = await domainContext.app.inject({
        method: "POST",
        url: "/v1/boards",
        payload: { name: "Board" },
      });
      expect(domain.statusCode).toBe(409);
      expect(domain.json()).toEqual({
        ok: false,
        code: "CONFLICT",
        message: "Known board conflict",
        retryable: false,
      });

      const enumeration = await validationContext.app.inject({
        method: "GET",
        url: "/v1/boards/00000000-0000-4000-8000-000000000099",
      });
      expect(enumeration.statusCode).toBe(404);
      expect(enumeration.json()).toEqual({
        ok: false,
        code: "BOARD_NOT_FOUND",
        message: "Board not found",
        retryable: false,
      });
    } finally {
      await Promise.all([
        authenticationContext.io.close(),
        validationContext.io.close(),
        domainContext.io.close(),
      ]);
      await Promise.all([
        authenticationContext.app.close(),
        validationContext.app.close(),
        domainContext.app.close(),
      ]);
    }
  });
});
