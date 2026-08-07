import { describe, expect, it } from "vitest";
import type { Environment } from "./env.js";
import { AuthenticationError, DevelopmentAuthAdapter, type AuthAdapter } from "./auth.js";

const configuredUserId = "00000000-0000-4000-8000-000000000011";

function environment(overrides: Partial<Environment> = {}): Environment {
  return {
    NODE_ENV: "development",
    HOST: "127.0.0.1",
    API_PORT: 4000,
    WEB_ORIGIN: "http://127.0.0.1:3000",
    DATABASE_URL: "postgresql://localhost/converge",
    REDIS_URL: "redis://localhost:6379",
    LOG_LEVEL: "silent",
    DEV_AUTH_USER_ID: configuredUserId,
    DEV_AUTH_USER_NAME: "Configured Developer",
    ...overrides,
  };
}

type HttpRequest = Parameters<AuthAdapter["authenticateHttp"]>[0];
type SocketRequest = Parameters<AuthAdapter["authenticateSocket"]>[0];

function httpRequest(headers: Record<string, string> = {}): HttpRequest {
  return { headers } as HttpRequest;
}

function socketRequest(auth: Record<string, unknown> = {}): SocketRequest {
  return { handshake: { auth } } as SocketRequest;
}

function captureAuthenticationError(action: () => unknown): AuthenticationError {
  try {
    action();
  } catch (error) {
    if (error instanceof AuthenticationError) return error;
    throw error;
  }
  throw new Error("Expected authentication to fail");
}

describe("development authentication", () => {
  it("uses the server-configured principal consistently for HTTP and Socket.IO", async () => {
    const adapter = new DevelopmentAuthAdapter(environment());
    const expected = { id: configuredUserId, displayName: "Configured Developer" };
    await expect(adapter.authenticateHttp(httpRequest())).resolves.toEqual(expected);
    await expect(adapter.authenticateSocket(socketRequest())).resolves.toEqual(expected);
  });

  it("rejects caller-selected HTTP development identity", () => {
    const adapter = new DevelopmentAuthAdapter(environment());
    const error = captureAuthenticationError(() =>
      adapter.authenticateHttp(
        httpRequest({ "x-dev-user-id": "00000000-0000-4000-8000-000000000099" }),
      ),
    );
    expect(error.code).toBe("INVALID_AUTH_INPUT");
  });

  it("rejects caller-selected Socket.IO development identity", () => {
    const adapter = new DevelopmentAuthAdapter(environment());
    const error = captureAuthenticationError(() =>
      adapter.authenticateSocket(socketRequest({ userId: "00000000-0000-4000-8000-000000000099" })),
    );
    expect(error.code).toBe("INVALID_AUTH_INPUT");
  });

  it("refuses to initialize in production", () => {
    expect(() => new DevelopmentAuthAdapter(environment({ NODE_ENV: "production" }))).toThrow(
      "Development authentication cannot run in production",
    );
  });

  it("fails clearly when the configured development identity is missing", () => {
    const missingIdentity = environment();
    delete missingIdentity.DEV_AUTH_USER_ID;
    expect(() => new DevelopmentAuthAdapter(missingIdentity)).toThrow(
      "DEV_AUTH_USER_ID is required for development authentication",
    );
  });
});
