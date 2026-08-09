import { describe, expect, it } from "vitest";
import { parseWorkerEnvironment, WorkerEnvironmentConfigurationError } from "./env.js";

const required = {
  DATABASE_URL: "postgresql://converge:password@localhost:55432/converge",
};

describe("worker environment", () => {
  it("applies the approved bounded defaults", () => {
    expect(parseWorkerEnvironment(required)).toMatchObject({
      OUTBOX_CLAIM_BATCH_SIZE: 32,
      OUTBOX_PUBLISH_CONCURRENCY: 8,
      OUTBOX_LEASE_MS: 60_000,
      OUTBOX_IDLE_POLL_MS: 250,
      OUTBOX_POLL_JITTER_RATIO: 0.2,
      REDIS_PUBLISH_TIMEOUT_MS: 5_000,
      DELIVERY_ENVELOPE_MAX_BYTES: 128 * 1024,
      REDIS_STREAM_KEY: "converge:delivery:v1",
      REDIS_STREAM_MAXLEN: 100_000,
      REDIS_STREAM_MAX_AGE_MS: 24 * 60 * 60 * 1000,
    });
  });

  it("rejects unsafe concurrency, timing, size, and Redis URL values", () => {
    const invalidValues: Array<[string, string]> = [
      ["OUTBOX_CLAIM_BATCH_SIZE", "33"],
      ["OUTBOX_PUBLISH_CONCURRENCY", "9"],
      ["OUTBOX_POLL_JITTER_RATIO", "0.21"],
      ["DELIVERY_ENVELOPE_MAX_BYTES", String(128 * 1024 + 1)],
      ["REDIS_STREAM_MAXLEN", "0"],
      ["REDIS_URL", "https://example.test"],
    ];
    for (const [field, value] of invalidValues)
      expect(() => parseWorkerEnvironment({ ...required, [field]: value })).toThrowError(
        new WorkerEnvironmentConfigurationError([field]),
      );
    expect(() =>
      parseWorkerEnvironment({
        ...required,
        OUTBOX_LEASE_MS: "5000",
        REDIS_PUBLISH_TIMEOUT_MS: "5000",
      }),
    ).toThrowError(new WorkerEnvironmentConfigurationError(["REDIS_PUBLISH_TIMEOUT_MS"]));
  });

  it("reports only field names for secret-bearing configuration errors", () => {
    const secret = "never-log-this-secret";
    let message = "";
    try {
      parseWorkerEnvironment({ DATABASE_URL: `invalid-${secret}`, REDIS_URL: `invalid-${secret}` });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toBe("Invalid worker environment configuration: DATABASE_URL, REDIS_URL");
    expect(message).not.toContain(secret);
  });
});
