import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import {
  parseWorkerEnvironment,
  workerEnvironmentVariableNames,
  WorkerEnvironmentConfigurationError,
} from "./env.js";

const required = {
  DATABASE_URL: "postgresql://converge:password@localhost:55432/converge",
};

describe("worker environment", () => {
  it("applies the approved bounded defaults", () => {
    expect(parseWorkerEnvironment(required)).toMatchObject({
      OUTBOX_CLAIM_BATCH_SIZE: 32,
      OUTBOX_PUBLISH_CONCURRENCY: 8,
      OUTBOX_LEASE_MS: 60_000,
      OUTBOX_FINALIZATION_MARGIN_MS: 5_000,
      OUTBOX_IDLE_POLL_MS: 250,
      OUTBOX_POLL_JITTER_RATIO: 0.2,
      REDIS_PUBLISH_TIMEOUT_MS: 5_000,
      DELIVERY_ENVELOPE_MAX_BYTES: 128 * 1024,
      REDIS_STREAM_KEY: "converge:delivery:v1",
      REDIS_STREAM_MAXLEN: 100_000,
      REDIS_STREAM_MAX_AGE_MS: 24 * 60 * 60 * 1000,
      SNAPSHOT_POLL_INTERVAL_MS: 30_000,
      SNAPSHOT_POLL_JITTER_PERCENT: 20,
      SNAPSHOT_CANDIDATE_SCAN_LIMIT: 100,
      SNAPSHOT_CANDIDATE_BATCH_SIZE: 16,
      SNAPSHOT_MAX_CONCURRENCY: 2,
      SNAPSHOT_OPERATION_THRESHOLD: 1_000,
      SNAPSHOT_CHANGED_AGE_MS: 86_400_000,
      SNAPSHOT_OPERATION_BYTES_THRESHOLD: 8_388_608,
      SNAPSHOT_MAX_PAYLOAD_BYTES: 16_777_216,
      SNAPSHOT_RETRY_BASE_MS: 5_000,
      SNAPSHOT_RETRY_CAP_MS: 300_000,
      SNAPSHOT_BUSY_RETRY_MS: 5_000,
      SNAPSHOT_FAILURE_FINGERPRINT_LIMIT: 1_000,
      COMPACTION_ENABLED: false,
      COMPACTION_POLL_INTERVAL_MS: 300_000,
      COMPACTION_POLL_JITTER_PERCENT: 20,
      COMPACTION_CANDIDATE_SCAN_LIMIT: 100,
      COMPACTION_CANDIDATE_BATCH_SIZE: 16,
      COMPACTION_MAX_CONCURRENCY: 2,
      COMPACTION_RETRY_BASE_MS: 5_000,
      COMPACTION_RETRY_CAP_MS: 300_000,
      COMPACTION_RETAINED_STATE_LIMIT: 1_000,
    });
  });

  it("enables compaction only for the exact true string", () => {
    expect(parseWorkerEnvironment({ ...required, COMPACTION_ENABLED: "true" })).toMatchObject({
      COMPACTION_ENABLED: true,
    });
    for (const value of ["TRUE", "1", "yes", "on", "", "false "])
      expect(() => parseWorkerEnvironment({ ...required, COMPACTION_ENABLED: value })).toThrowError(
        new WorkerEnvironmentConfigurationError(["COMPACTION_ENABLED"]),
      );
  });

  it("rejects unsafe concurrency, timing, size, and Redis URL values", () => {
    const invalidValues: Array<[string, string]> = [
      ["OUTBOX_CLAIM_BATCH_SIZE", "33"],
      ["OUTBOX_PUBLISH_CONCURRENCY", "9"],
      ["OUTBOX_POLL_JITTER_RATIO", "0.21"],
      ["DELIVERY_ENVELOPE_MAX_BYTES", String(128 * 1024 + 1)],
      ["REDIS_STREAM_MAXLEN", "0"],
      ["REDIS_URL", "https://example.test"],
      ["SNAPSHOT_POLL_INTERVAL_MS", "0"],
      ["SNAPSHOT_POLL_JITTER_PERCENT", "21"],
      ["SNAPSHOT_CANDIDATE_SCAN_LIMIT", "101"],
      ["SNAPSHOT_CANDIDATE_BATCH_SIZE", "17"],
      ["SNAPSHOT_MAX_CONCURRENCY", "3"],
      ["SNAPSHOT_MAX_PAYLOAD_BYTES", "16777217"],
      ["SNAPSHOT_RETRY_CAP_MS", "300001"],
      ["SNAPSHOT_FAILURE_FINGERPRINT_LIMIT", "1001"],
      ["COMPACTION_POLL_INTERVAL_MS", "0"],
      ["COMPACTION_POLL_JITTER_PERCENT", "101"],
      ["COMPACTION_CANDIDATE_SCAN_LIMIT", "101"],
      ["COMPACTION_CANDIDATE_BATCH_SIZE", "17"],
      ["COMPACTION_MAX_CONCURRENCY", "3"],
      ["COMPACTION_RETRY_CAP_MS", "300001"],
      ["COMPACTION_RETAINED_STATE_LIMIT", "1001"],
    ];
    for (const [field, value] of invalidValues)
      expect(() => parseWorkerEnvironment({ ...required, [field]: value })).toThrowError(
        new WorkerEnvironmentConfigurationError([field]),
      );
    expect(() =>
      parseWorkerEnvironment({
        ...required,
        OUTBOX_LEASE_MS: "8000",
        REDIS_PUBLISH_TIMEOUT_MS: "5000",
      }),
    ).toThrowError(new WorkerEnvironmentConfigurationError(["OUTBOX_LEASE_MS"]));
    expect(() =>
      parseWorkerEnvironment({
        ...required,
        SNAPSHOT_CANDIDATE_SCAN_LIMIT: "1",
        SNAPSHOT_CANDIDATE_BATCH_SIZE: "2",
      }),
    ).toThrowError(new WorkerEnvironmentConfigurationError(["SNAPSHOT_CANDIDATE_BATCH_SIZE"]));
    expect(() =>
      parseWorkerEnvironment({
        ...required,
        SNAPSHOT_CANDIDATE_BATCH_SIZE: "1",
        SNAPSHOT_MAX_CONCURRENCY: "2",
      }),
    ).toThrowError(new WorkerEnvironmentConfigurationError(["SNAPSHOT_MAX_CONCURRENCY"]));
    expect(() =>
      parseWorkerEnvironment({
        ...required,
        SNAPSHOT_RETRY_BASE_MS: "5001",
        SNAPSHOT_RETRY_CAP_MS: "5000",
      }),
    ).toThrowError(new WorkerEnvironmentConfigurationError(["SNAPSHOT_RETRY_BASE_MS"]));
    expect(() =>
      parseWorkerEnvironment({
        ...required,
        COMPACTION_CANDIDATE_SCAN_LIMIT: "1",
        COMPACTION_CANDIDATE_BATCH_SIZE: "2",
      }),
    ).toThrowError(new WorkerEnvironmentConfigurationError(["COMPACTION_CANDIDATE_BATCH_SIZE"]));
    expect(() =>
      parseWorkerEnvironment({
        ...required,
        COMPACTION_CANDIDATE_BATCH_SIZE: "1",
        COMPACTION_MAX_CONCURRENCY: "2",
      }),
    ).toThrowError(new WorkerEnvironmentConfigurationError(["COMPACTION_MAX_CONCURRENCY"]));
    expect(() =>
      parseWorkerEnvironment({
        ...required,
        COMPACTION_RETRY_BASE_MS: "5001",
        COMPACTION_RETRY_CAP_MS: "5000",
      }),
    ).toThrowError(new WorkerEnvironmentConfigurationError(["COMPACTION_RETRY_BASE_MS"]));
    expect(() =>
      parseWorkerEnvironment({
        ...required,
        COMPACTION_POLL_INTERVAL_MS: "2147483647",
        COMPACTION_POLL_JITTER_PERCENT: "20",
      }),
    ).toThrowError(new WorkerEnvironmentConfigurationError(["COMPACTION_POLL_INTERVAL_MS"]));
  });

  it("reports only field names for secret-bearing configuration errors", () => {
    const secret = "never-log-this-secret";
    let message = "";
    try {
      parseWorkerEnvironment({
        DATABASE_URL: `invalid-${secret}`,
        REDIS_URL: `invalid-${secret}`,
        COMPACTION_ENABLED: secret,
      });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toBe(
      "Invalid worker environment configuration: COMPACTION_ENABLED, DATABASE_URL, REDIS_URL",
    );
    expect(message).not.toContain(secret);
  });

  it("registers every parsed worker variable in Turbo pass-through configuration", async () => {
    const turbo = JSON.parse(
      await readFile(new URL("../../../turbo.json", import.meta.url), "utf8"),
    ) as { globalPassThroughEnv?: unknown };
    expect(turbo.globalPassThroughEnv).toEqual(expect.any(Array));
    const passThrough = new Set(turbo.globalPassThroughEnv as string[]);

    expect(workerEnvironmentVariableNames.filter((name) => !passThrough.has(name))).toEqual([]);
  });

  it("documents every snapshot variable with its parsed default", async () => {
    const example = await readFile(new URL("../../../.env.example", import.meta.url), "utf8");
    const entries = new Map(
      example
        .split("\n")
        .filter((line) => line && !line.startsWith("#"))
        .map((line) => {
          const separator = line.indexOf("=");
          return [line.slice(0, separator), line.slice(separator + 1)] as const;
        }),
    );
    const parsed = parseWorkerEnvironment(required);
    const snapshotNames = workerEnvironmentVariableNames.filter((name) =>
      name.startsWith("SNAPSHOT_"),
    ) as Array<keyof typeof parsed>;

    expect(snapshotNames).toHaveLength(13);
    for (const name of snapshotNames) expect(entries.get(name)).toBe(String(parsed[name]));
  });

  it("documents and forwards every compaction variable with its parsed default", async () => {
    const [example, turboSource] = await Promise.all([
      readFile(new URL("../../../.env.example", import.meta.url), "utf8"),
      readFile(new URL("../../../turbo.json", import.meta.url), "utf8"),
    ]);
    const entries = new Map(
      example
        .split("\n")
        .filter((line) => line && !line.startsWith("#"))
        .map((line) => {
          const separator = line.indexOf("=");
          return [line.slice(0, separator), line.slice(separator + 1)] as const;
        }),
    );
    const passThrough = new Set(
      (JSON.parse(turboSource) as { globalPassThroughEnv: string[] }).globalPassThroughEnv,
    );
    const parsed = parseWorkerEnvironment(required);
    const compactionNames = workerEnvironmentVariableNames.filter((name) =>
      name.startsWith("COMPACTION_"),
    ) as Array<keyof typeof parsed>;

    expect(compactionNames).toHaveLength(9);
    for (const name of compactionNames) {
      expect(entries.get(name)).toBe(String(parsed[name]));
      expect(passThrough.has(name)).toBe(true);
    }
  });
});
