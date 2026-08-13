import assert from "node:assert/strict";
import test from "node:test";
import {
  CleanupStack,
  assertSanitizedArtifact,
  durableEvidenceSql,
  runWithOwnedCleanup,
  validateDurableEvidence,
  validateEnvironmentArtifact,
  validateK6Summary,
} from "./k6-smoke-harness-lib.mjs";

function passingSummary() {
  const threshold = { thresholds: { gate: false } };
  return {
    metrics: {
      converge_iteration_failures: { ...threshold, value: 0 },
      converge_protocol_failures: { ...threshold, value: 0 },
      converge_sequence_gaps: { ...threshold, count: 0 },
      converge_socket_connect_duration: threshold,
      converge_board_join_ack_duration: threshold,
      converge_command_ack_duration: threshold,
      converge_commands_acknowledged: { count: 4 },
      converge_live_events_received: { count: 8 },
      converge_duplicate_events: { count: 0 },
    },
  };
}

test("accepts only the required sanitized environment metadata", () => {
  validateEnvironmentArtifact({
    timestampUtc: "2026-08-13T00:00:00.000Z",
    gitCommitSha: "a".repeat(40),
    k6Version: "0.57.0",
    nodeVersion: "22.18.0",
    pnpmVersion: "10.15.0",
    os: "darwin",
    architecture: "arm64",
    profile: "smoke",
    virtualUsers: 2,
    duration: "30s",
    apiReplicas: 1,
    workerReplicas: 1,
    configuration: { delivery: "distributed", transport: "socket.io-v4" },
  });
  assert.throws(() => assertSanitizedArtifact({ board_id: "secret" }));
  assert.throws(() => assertSanitizedArtifact("opaque-token", ["opaque-token"]));
});

test("cleanup owns resources once and is idempotent", async () => {
  const cleanup = new CleanupStack();
  const calls = [];
  cleanup.own("database", () => calls.push("database"));
  cleanup.own("redis", () => calls.push("redis"));
  await cleanup.close();
  await cleanup.close();
  assert.deepEqual(calls, ["redis", "database"]);
});

for (const failurePoint of ["before", "after"]) {
  test(`cleans owned resources after failure ${failurePoint} k6 execution`, async () => {
    const calls = [];
    await assert.rejects(() =>
      runWithOwnedCleanup({
        prepare: async (cleanup) => {
          cleanup.own("fixture", () => calls.push("cleanup"));
          if (failurePoint === "before") throw new Error("prepare failed");
        },
        runK6: async () => 0,
        readSummary: async () => passingSummary(),
        verify: async () => {
          if (failurePoint === "after") throw new Error("verification failed");
        },
      }),
    );
    assert.deepEqual(calls, ["cleanup"]);
  });
}

test("propagates nonzero k6 status and threshold failures", async () => {
  const steps = {
    prepare: async () => undefined,
    runK6: async () => 17,
    readSummary: async () => passingSummary(),
    verify: async () => undefined,
  };
  await assert.rejects(() => runWithOwnedCleanup(steps), /k6 exited nonzero: 17/);
  const failed = passingSummary();
  failed.metrics.converge_command_ack_duration.thresholds.gate = true;
  assert.throws(() => validateK6Summary(failed), /k6 threshold failed/);
});

test("accepts observed and suppressed transport duplicates without logical reapplication", () => {
  const summary = passingSummary();
  summary.metrics.converge_duplicate_events.count = 7;
  const result = validateK6Summary(summary);
  assert.equal(result.transportDuplicatesObserved, 7);
  assert.equal(result.transportDuplicatesSuppressed, 7);
  assert.deepEqual(
    validateDurableEvidence(
      {
        lastSequence: 4,
        deliveryHead: 4,
        operations: 4,
        distinctOperations: 4,
        objects: 4,
        outbox: 4,
        distinctOutboxEvents: 4,
        published: 4,
        handled: 4,
        identityMismatches: 0,
        invalidPublicationIds: 0,
        invalidPublishedState: 0,
      },
      4,
    ),
    {
      lastSequence: 4,
      deliveryHead: 4,
      operations: 4,
      distinctOperations: 4,
      objects: 4,
      outbox: 4,
      distinctOutboxEvents: 4,
      published: 4,
      handled: 4,
      identityMismatches: 0,
      invalidPublicationIds: 0,
      invalidPublishedState: 0,
      distinctCommands: 4,
      logicalReapplications: 0,
      logicalReducerApplications: 4,
    },
  );
});

test("rejects durable operation or outbox evidence that exceeds distinct commands", () => {
  const evidence = {
    lastSequence: 4,
    deliveryHead: 4,
    operations: 5,
    distinctOperations: 4,
    objects: 4,
    outbox: 5,
    distinctOutboxEvents: 4,
    published: 5,
    handled: 5,
    identityMismatches: 0,
    invalidPublicationIds: 0,
    invalidPublishedState: 0,
  };
  assert.throws(() => validateDurableEvidence(evidence, 4), /logical reapplication/);
  assert.throws(
    () =>
      validateDurableEvidence({ ...evidence, operations: 4, distinctOperations: 4, outbox: 5 }, 4),
    /logical reapplication/,
  );
});

test("durable evidence SQL uses only current relational and envelope columns", () => {
  const sql = durableEvidenceSql("00000000-0000-4000-8000-000000000001");
  assert.match(sql, /DISTINCT id/);
  assert.match(sql, /payload->>'eventId' <> id::text/);
  assert.match(sql, /redis_entry_id/);
  assert.match(sql, /next_attempt_at <> 'infinity'/);
  assert.match(sql, /lease_token IS NOT NULL/);
  assert.doesNotMatch(sql, /DISTINCT event_id.*outbox_events/s);
  assert.throws(() => durableEvidenceSql("private-value"), /board identity/);
});
