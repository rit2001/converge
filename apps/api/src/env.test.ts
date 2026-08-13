import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { EnvironmentConfigurationError, parseEnvironment } from "./env.js";

const required = {
  DATABASE_URL: "postgresql://converge:local-password@localhost:55432/converge",
};

describe("environment validation", () => {
  it("accepts injected production configuration without an environment file", () => {
    expect(
      parseEnvironment({
        ...required,
        NODE_ENV: "production",
        WEB_ORIGIN: "https://example.test",
      }),
    ).toMatchObject({
      NODE_ENV: "production",
      DATABASE_URL: required.DATABASE_URL,
      WEB_ORIGIN: "https://example.test",
      API_METRICS_ENABLED: false,
      API_METRICS_BEARER_TOKEN: "",
      API_DELIVERY_MODE: "local",
      REDIS_URL: "redis://localhost:6379",
      REDIS_STREAM_KEY: "converge:delivery:v1",
    });
  });

  it("accepts explicit distributed delivery with the required Redis identity", () => {
    expect(
      parseEnvironment({
        ...required,
        API_DELIVERY_MODE: "distributed",
        REDIS_URL: "rediss://delivery-user:secret@example.test:6380",
        REDIS_STREAM_KEY: "converge:delivery:production:v1",
      }),
    ).toMatchObject({
      API_DELIVERY_MODE: "distributed",
      REDIS_STREAM_KEY: "converge:delivery:production:v1",
      REDIS_DELIVERY_MAX_BOARD_STATES: 1_000,
      DELIVERY_WATCHDOG_INTERVAL_MS: 5_000,
    });
  });

  it("rejects incomplete distributed configuration instead of falling back to local mode", () => {
    expect(() => parseEnvironment({ ...required, API_DELIVERY_MODE: "distributed" })).toThrowError(
      new EnvironmentConfigurationError(["REDIS_STREAM_KEY", "REDIS_URL"]),
    );
  });

  it("does not apply distributed-only Redis constraints in local mode", () => {
    expect(
      parseEnvironment({
        ...required,
        API_DELIVERY_MODE: "local",
        REDIS_URL: "https://unused.example.test",
        REDIS_STREAM_KEY: "unused local stream value",
      }),
    ).toMatchObject({
      API_DELIVERY_MODE: "local",
      REDIS_URL: "https://unused.example.test",
      REDIS_STREAM_KEY: "unused local stream value",
    });
  });

  it("strictly validates the delivery mode and bounded distributed settings", () => {
    expect(() => parseEnvironment({ ...required, API_DELIVERY_MODE: "automatic" })).toThrowError(
      new EnvironmentConfigurationError(["API_DELIVERY_MODE"]),
    );
    expect(() =>
      parseEnvironment({
        ...required,
        API_DELIVERY_MODE: "distributed",
        REDIS_URL: "redis://localhost:6379",
        REDIS_STREAM_KEY: "converge:delivery:v1",
        DELIVERY_WATCHDOG_INTERVAL_MS: "0",
      }),
    ).toThrowError(new EnvironmentConfigurationError(["DELIVERY_WATCHDOG_INTERVAL_MS"]));
  });

  it("fails clearly before startup when required configuration is absent", () => {
    expect(() => parseEnvironment({ NODE_ENV: "production" })).toThrowError(
      new EnvironmentConfigurationError(["DATABASE_URL"]),
    );
  });

  it("enables API metrics only with exact true and a bounded printable bearer token", () => {
    expect(
      parseEnvironment({
        ...required,
        API_METRICS_ENABLED: "true",
        API_METRICS_BEARER_TOKEN: "bounded-metrics-token",
      }),
    ).toMatchObject({
      API_METRICS_ENABLED: true,
      API_METRICS_BEARER_TOKEN: "bounded-metrics-token",
    });
    expect(() =>
      parseEnvironment({
        ...required,
        API_METRICS_ENABLED: "TRUE",
        API_METRICS_BEARER_TOKEN: "bounded-metrics-token",
      }),
    ).toThrowError(new EnvironmentConfigurationError(["API_METRICS_ENABLED"]));
    expect(() => parseEnvironment({ ...required, API_METRICS_ENABLED: "true" })).toThrowError(
      new EnvironmentConfigurationError(["API_METRICS_BEARER_TOKEN"]),
    );
  });

  it("rejects unsafe metrics tokens without exposing them and documents both variables", () => {
    const unsafe = `private-token\n${"x".repeat(257)}`;
    let message = "";
    try {
      parseEnvironment({
        ...required,
        API_METRICS_ENABLED: "true",
        API_METRICS_BEARER_TOKEN: unsafe,
      });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toBe("Invalid environment configuration: API_METRICS_BEARER_TOKEN");
    expect(message).not.toContain("private-token");
    const example = readFileSync(new URL("../../../.env.example", import.meta.url), "utf8");
    expect(example).toContain("API_METRICS_ENABLED=false\n");
    expect(example).toContain("API_METRICS_BEARER_TOKEN=\n");
  });

  it("does not include credentials or complete connection strings in configuration errors", () => {
    const secret = "do-not-expose-this-password";
    let message = "";
    try {
      parseEnvironment({ DATABASE_URL: `not-a-url-${secret}` });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toBe("Invalid environment configuration: DATABASE_URL");
    expect(message).not.toContain(secret);
    expect(message).not.toContain("not-a-url");
  });

  it("does not expose Redis credentials in distributed configuration errors", () => {
    const secret = "redis-password-must-stay-private";
    let message = "";
    try {
      parseEnvironment({
        ...required,
        API_DELIVERY_MODE: "distributed",
        REDIS_URL: `https://delivery:${secret}@example.test`,
        REDIS_STREAM_KEY: "invalid stream key",
      });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toBe("Invalid environment configuration: REDIS_STREAM_KEY, REDIS_URL");
    expect(message).not.toContain(secret);
    expect(message).not.toContain("example.test");
  });
});
