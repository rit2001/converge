import assert from "node:assert/strict";
import test from "node:test";
import {
  K6_ALLOWED_TAGS,
  K6_METRIC_NAMES,
  K6_SUMMARY_TREND_STATS,
  parseWorkloadConfig,
  workloadOptions,
} from "./config.js";
import { deterministicUuid } from "./identity.js";
import {
  classifyTimeout,
  createDeliveryTracker,
  encodeConnect,
  encodeEvent,
  parseCommandAcknowledgement,
  parseBoardSnapshot,
  parseHttpJsonBody,
  parseJoinAcknowledgement,
  parseOpenPacket,
  parseOperationRange,
  parseSocketPacket,
  pongFor,
} from "./protocol.js";

const ids = {
  board: "00000000-0000-4000-8000-000000000001",
  client: "10000000-0000-4000-8000-000000000001",
  object: "20000000-0000-4000-8000-000000000001",
  operation1: "30000000-0000-4000-8000-000000000001",
  operation2: "30000000-0000-4000-8000-000000000002",
  operation3: "30000000-0000-4000-8000-000000000003",
};

function operation(sequence, opId = ids.operation1) {
  return {
    schemaVersion: 1,
    opId,
    boardId: ids.board,
    clientId: ids.client,
    baseSeq: sequence - 1,
    targetId: ids.object,
    clientTimestamp: "2026-08-13T10:00:00.000Z",
    type: "object.create",
    payload: {
      id: ids.object,
      kind: "rectangle",
      x: 40,
      y: 40,
      width: 160,
      height: 100,
      rotation: 0,
      fill: "#818cf8",
      text: "",
    },
    seq: sequence,
    committedAt: "2026-08-13T10:00:00.001Z",
  };
}

test("strictly parses Engine.IO open and Socket.IO namespace connection packets", () => {
  const open =
    '0{"sid":"engine-session","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}';
  assert.deepEqual(parseOpenPacket(open, 2_048), {
    sid: "engine-session",
    upgrades: [],
    pingInterval: 25_000,
    pingTimeout: 20_000,
    maxPayload: 1_000_000,
  });
  assert.deepEqual(parseSocketPacket('40{"sid":"socket-session"}', 2_048), {
    kind: "connected",
    sid: "socket-session",
  });
  assert.equal(encodeConnect("opaque-token"), '40{"token":"opaque-token"}');
});

test("implements Engine.IO ping and pong without accepting other packets", () => {
  assert.deepEqual(parseSocketPacket("2", 10), { kind: "ping" });
  assert.equal(pongFor("2"), "3");
  assert.throws(() => pongFor("3"), { name: "ProtocolFailure" });
});

test("encodes events with bounded acknowledgement IDs and correlates acknowledgement frames", () => {
  assert.equal(
    encodeEvent("board:join", { schemaVersion: 1 }, 17),
    '4217["board:join",{"schemaVersion":1}]',
  );
  assert.deepEqual(parseSocketPacket('4317[{"ok":true}]', 2_048), {
    kind: "ack",
    acknowledgementId: 17,
    value: { ok: true },
  });
  assert.throws(() => encodeEvent("board:join", {}, -1), { name: "ProtocolFailure" });
});

test("strictly validates join and command acknowledgements", () => {
  assert.equal(
    parseJoinAcknowledgement({ ok: true, boardId: ids.board, joinWatermark: 0 }, ids.board).kind,
    "success",
  );
  const acknowledgement = parseCommandAcknowledgement(
    { ok: true, duplicate: false, operation: operation(1) },
    { boardId: ids.board, opId: ids.operation1 },
  );
  assert.equal(acknowledgement.kind, "success");
  assert.equal(acknowledgement.operation.seq, 1);
  assert.throws(
    () =>
      parseJoinAcknowledgement(
        { ok: true, boardId: ids.board, joinWatermark: 0, extra: true },
        ids.board,
      ),
    { name: "ProtocolFailure" },
  );
  assert.throws(
    () =>
      parseCommandAcknowledgement(
        { ok: true, duplicate: false, operation: operation(1) },
        { boardId: ids.board, opId: ids.operation2 },
      ),
    { name: "ProtocolFailure" },
  );
});

test("establishes a strict authoritative snapshot boundary before catch-up replay", () => {
  const snapshot = parseBoardSnapshot(
    {
      id: ids.board,
      name: "Benchmark board",
      lastSeq: 1,
      objects: [operation(1).payload],
    },
    ids.board,
  );
  const tracker = createDeliveryTracker(snapshot.lastSeq, 256);
  const range = parseOperationRange(
    {
      boardId: ids.board,
      afterSeq: 1,
      watermark: 2,
      operations: [operation(2, ids.operation2)],
      nextSeq: 2,
      hasMore: false,
    },
    { boardId: ids.board, after: tracker.snapshot().lastSequence, watermark: 2 },
  );
  assert.equal(tracker.observe(range.operations[0]), "next_contiguous_operation");
  assert.equal(tracker.snapshot().lastSequence, 2);
  assert.throws(
    () =>
      parseBoardSnapshot(
        { id: ids.client, name: "Wrong board", lastSeq: 1, objects: [operation(1).payload] },
        ids.board,
      ),
    { code: "SNAPSHOT_IDENTITY" },
  );
});

test("parses the direct API snapshot body into fresh iteration-owned state", () => {
  const source = JSON.stringify({
    id: ids.board,
    name: "Benchmark board",
    lastSeq: 1,
    objects: [operation(1).payload],
  });
  const first = parseBoardSnapshot(parseHttpJsonBody(source, 8_192, "SNAPSHOT_JSON"), ids.board);
  const second = parseBoardSnapshot(parseHttpJsonBody(source, 8_192, "SNAPSHOT_JSON"), ids.board);
  assert.notEqual(first, second);
  assert.notEqual(first.objects, second.objects);
  assert.equal(first.lastSeq, 1);
  assert.throws(() => parseHttpJsonBody("{", 8_192, "SNAPSHOT_JSON"), {
    code: "SNAPSHOT_JSON",
  });
  assert.throws(
    () =>
      parseBoardSnapshot(
        parseHttpJsonBody(JSON.stringify({ snapshot: JSON.parse(source) }), 8_192),
        ids.board,
      ),
    { code: "SNAPSHOT_SHAPE" },
  );
});

test("create-only growth crosses the configured snapshot response-byte boundary", () => {
  const limit = 131_072;
  const objects = [];
  let below;
  let oversized;
  for (let index = 1; index <= 1_024; index += 1) {
    const id = deterministicUuid("object", 1, index, 1);
    objects.push({
      id,
      kind: "rectangle",
      x: 40,
      y: 40,
      width: 160,
      height: 100,
      rotation: 0,
      fill: "#818cf8",
      text: "",
    });
    const body = JSON.stringify({ id: ids.board, name: "bounded", lastSeq: index, objects });
    if (body.length <= limit) below = body;
    else {
      oversized = body;
      break;
    }
  }
  assert.ok(below);
  assert.ok(oversized);
  assert.doesNotThrow(() => parseHttpJsonBody(below, limit, "SNAPSHOT_JSON"));
  assert.throws(() => parseHttpJsonBody(oversized, limit, "SNAPSHOT_JSON"), {
    code: "SNAPSHOT_JSON",
  });
  const boundedCode = "RECOVERY_JSON";
  const sensitiveOversizedInput = `private-token${"x".repeat(limit)}`;
  assert.throws(
    () => parseHttpJsonBody(sensitiveOversizedInput, limit, boundedCode),
    (error) => {
      assert.equal(error.code, boundedCode);
      assert.doesNotMatch(error.message, /private-token/);
      return true;
    },
  );
});

test("parses live operations and detects duplicates without double application", () => {
  const parsed = parseSocketPacket(
    `42${JSON.stringify(["operation:committed", operation(1)])}`,
    8_192,
  );
  assert.equal(parsed.kind, "operation");
  const tracker = createDeliveryTracker(0, 2);
  assert.equal(tracker.observe(parsed.value), "next_contiguous_operation");
  assert.equal(tracker.observe(parsed.value), "catchup_live_overlap");
  assert.throws(
    () => tracker.observe({ ...parsed.value, committedAt: "2026-08-13T10:00:00.002Z" }),
    { code: "OPERATION_IDENTITY_CONFLICT" },
  );
  assert.deepEqual(tracker.snapshot(), {
    lastSequence: 1,
    retainedOperations: 1,
    bufferedOperations: 0,
  });
});

test("strictly validates bounded contiguous authoritative catch-up evidence", () => {
  const range = parseOperationRange(
    {
      boardId: ids.board,
      afterSeq: 0,
      watermark: 2,
      operations: [operation(1, ids.operation1), operation(2, ids.operation2)],
      nextSeq: 2,
      hasMore: false,
    },
    { boardId: ids.board, after: 0, watermark: 2 },
  );
  assert.equal(range.operations.length, 2);
  assert.throws(
    () =>
      parseOperationRange(
        {
          boardId: ids.board,
          afterSeq: 0,
          watermark: 2,
          operations: [operation(2, ids.operation2)],
          nextSeq: 2,
          hasMore: false,
        },
        { boardId: ids.board, after: 0, watermark: 2 },
      ),
    { name: "ProtocolFailure" },
  );
});

test("reconciles catch-up and buffered-live overlap without replaying snapshot-covered history", () => {
  const tracker = createDeliveryTracker(1, 256);
  const overlapping = operation(2, ids.operation2);
  assert.equal(tracker.observe(overlapping), "next_contiguous_operation");
  assert.equal(tracker.observe({ ...overlapping }), "catchup_live_overlap");
  assert.equal(tracker.observe(operation(1, ids.operation1)), "covered_by_snapshot");
  assert.equal(tracker.snapshot().lastSequence, 2);
});

test("strictly parses revocation and requires the caller to terminate participation", () => {
  const parsed = parseSocketPacket(
    `42${JSON.stringify([
      "board:access-revoked",
      { schemaVersion: 1, boardId: ids.board, code: "ACCESS_REVOKED", message: "Access revoked" },
    ])}`,
    8_192,
  );
  assert.equal(parsed.kind, "revocation");
  assert.equal(parsed.value.boardId, ids.board);
});

test("rejects malformed oversized unknown and out-of-order evidence", () => {
  for (const packet of ["garbage", "42[]", '42["unknown:event",{}]', "43x[{}]"])
    assert.throws(() => parseSocketPacket(packet, 8_192), { name: "ProtocolFailure" });
  assert.throws(() => parseSocketPacket(`42["x","${"a".repeat(100)}"]`, 32), {
    name: "ProtocolFailure",
  });
  const tracker = createDeliveryTracker(0, 4);
  assert.equal(tracker.observe(operation(2, ids.operation2)), "future_operation_buffered");
  assert.equal(tracker.snapshot().lastSequence, 0);
  assert.equal(tracker.observe(operation(1)), "next_contiguous_operation");
  assert.deepEqual(
    tracker.drainApplied().map(({ seq }) => seq),
    [1, 2],
  );
  assert.throws(
    () => tracker.observe(operation(1, ids.operation2)),
    (error) => {
      assert.equal(error.code, "OPERATION_IDENTITY_CONFLICT");
      return true;
    },
  );
  assert.throws(() => tracker.observe(operation(1, ids.operation3)), {
    code: "CANVAS_SEQUENCE_CONFLICT",
  });
});

test("bounds retained dedupe state", () => {
  const tracker = createDeliveryTracker(0, 2);
  tracker.observe(operation(1, ids.operation1));
  tracker.observe(operation(2, ids.operation2));
  tracker.observe(operation(3, ids.operation3));
  assert.deepEqual(tracker.snapshot(), {
    lastSequence: 3,
    retainedOperations: 2,
    bufferedOperations: 0,
  });
  assert.equal(tracker.has(ids.operation1), false);
  assert.equal(tracker.has(ids.operation3), true);
});

test("classifies timeouts with fixed diagnostics that never contain tokens", () => {
  const secret = "never-print-this-token";
  assert.deepEqual(classifyTimeout("command_ack"), {
    code: "TIMEOUT_COMMAND_ACK",
    retryable: true,
  });
  let error;
  try {
    parseWorkloadConfig({ CONVERGE_AUTH_TOKEN: secret });
  } catch (caught) {
    error = caught;
  }
  assert.ok(error instanceof Error);
  assert.doesNotMatch(error.message, new RegExp(secret));
  assert.throws(() => classifyTimeout("database"), { name: "ProtocolFailure" });
});

test("validates safe profiles explicit stateful configuration and remote opt-in", () => {
  const base = {
    CONVERGE_BASE_URL: "http://127.0.0.1:4000",
    CONVERGE_SOCKET_URL: "ws://127.0.0.1:4000",
    CONVERGE_BOARD_ID: ids.board,
    CONVERGE_AUTH_TOKEN: "test-owned-opaque-token",
  };
  const smoke = parseWorkloadConfig(base);
  assert.equal(smoke.profile, "smoke");
  assert.equal(smoke.vus, 2);
  assert.equal(smoke.duration, "30s");
  assert.equal(smoke.commandsPerClient, 2);
  assert.deepEqual(workloadOptions(smoke).systemTags, []);
  assert.deepEqual(workloadOptions(smoke).summaryTrendStats, [
    "avg",
    "min",
    "max",
    "p(50)",
    "p(95)",
    "p(99)",
  ]);
  assert.deepEqual(
    workloadOptions(parseWorkloadConfig({ ...base, CONVERGE_DEBUG_PHASES: "true" })),
    {
      vus: 1,
      iterations: 1,
      systemTags: [],
      summaryTrendStats: K6_SUMMARY_TREND_STATS,
      thresholds: workloadOptions(smoke).thresholds,
    },
  );
  assert.deepEqual(
    workloadOptions(
      parseWorkloadConfig({
        ...base,
        CONVERGE_DEBUG_PHASES: "true",
        CONVERGE_DEBUG_CONCURRENT: "true",
      }),
    ).scenarios,
    {
      debug_concurrent: {
        executor: "per-vu-iterations",
        vus: 2,
        iterations: 1,
        maxDuration: "15s",
      },
    },
  );
  assert.deepEqual(
    workloadOptions(
      parseWorkloadConfig({
        ...base,
        CONVERGE_DEBUG_PHASES: "true",
        CONVERGE_DEBUG_SEQUENTIAL: "true",
      }),
    ).scenarios,
    {
      debug_sequential: {
        executor: "per-vu-iterations",
        vus: 1,
        iterations: 2,
        maxDuration: "15s",
      },
    },
  );
  assert.deepEqual(
    workloadOptions(
      parseWorkloadConfig({
        ...base,
        CONVERGE_DEBUG_PHASES: "true",
        CONVERGE_DEBUG_REPEAT: "true",
      }),
    ).scenarios,
    {
      debug_repeat: {
        executor: "per-vu-iterations",
        vus: 2,
        iterations: 4,
        maxDuration: "30s",
      },
    },
  );
  assert.deepEqual(workloadOptions(smoke).thresholds, {
    converge_iteration_failures: ["rate<0.01"],
    converge_protocol_failures: ["rate==0"],
    converge_sequence_gaps: ["count==0"],
    converge_command_ack_duration: ["p(99)<2000"],
    converge_socket_connect_duration: ["p(99)<5000"],
    converge_board_join_ack_duration: ["p(99)<5000"],
  });
  const baseline = parseWorkloadConfig({ ...base, CONVERGE_PROFILE: "baseline" });
  assert.equal(baseline.vus, 10);
  assert.equal(baseline.duration, "2m");
  assert.equal(baseline.workloadModel, "bounded");
  assert.equal(smoke.workloadModel, "create-only");
  const scaleStep = parseWorkloadConfig({ ...base, CONVERGE_PROFILE: "scale-step" });
  const debug = parseWorkloadConfig({ ...base, CONVERGE_DEBUG_PHASES: "true" });
  for (const config of [smoke, baseline, scaleStep, debug])
    assert.strictEqual(workloadOptions(config).summaryTrendStats, K6_SUMMARY_TREND_STATS);
  assert.equal(Object.isFrozen(K6_SUMMARY_TREND_STATS), true);
  assert.throws(() => K6_SUMMARY_TREND_STATS.push("p(100)"), TypeError);
  assert.deepEqual(workloadOptions(baseline).summaryTrendStats, [
    "avg",
    "min",
    "max",
    "p(50)",
    "p(95)",
    "p(99)",
  ]);
  assert.equal(
    parseWorkloadConfig({ ...base, CONVERGE_PROFILE: "scale-step" }).stages.at(-1).target,
    0,
  );
  assert.throws(
    () =>
      parseWorkloadConfig({
        ...base,
        CONVERGE_BASE_URL: "https://benchmark.invalid",
        CONVERGE_SOCKET_URL: "wss://benchmark.invalid",
      }),
    /CONVERGE_ALLOW_REMOTE_TARGET_REQUIRED/,
  );
  assert.equal(
    parseWorkloadConfig({
      ...base,
      CONVERGE_BASE_URL: "https://benchmark.invalid",
      CONVERGE_SOCKET_URL: "wss://benchmark.invalid",
      CONVERGE_ALLOW_REMOTE_TARGET: "true",
    }).baseUrl,
    "https://benchmark.invalid",
  );
  for (const override of [
    { CONVERGE_VUS: "0" },
    { CONVERGE_VUS: "101" },
    { CONVERGE_DURATION: "0s" },
    { CONVERGE_DURATION: "11m" },
    { CONVERGE_COMMANDS_PER_CLIENT: "-1" },
    { CONVERGE_COMMAND_INTERVAL_MS: "60001" },
    { CONVERGE_MAX_PACKET_BYTES: "0" },
    { CONVERGE_CONNECT_TIMEOUT_MS: "-1" },
    { CONVERGE_ACK_TIMEOUT_MS: "60001" },
    { CONVERGE_AUTH_TOKEN: "unsafe\ntoken" },
    { CONVERGE_BASE_URL: "http://user:secret@127.0.0.1:4000" },
    { CONVERGE_SOCKET_URL: "ws://127.0.0.1:4000/?token=secret" },
  ])
    assert.throws(
      () => parseWorkloadConfig({ ...base, ...override }),
      /Invalid k6 workload configuration/,
    );
  assert.equal(
    Object.hasOwn(
      workloadOptions(parseWorkloadConfig({ ...base, CONVERGE_PROFILE: "scale-step" })).thresholds,
      "converge_command_ack_duration",
    ),
    false,
  );
});

test("keeps the custom metric and tag catalogs fixed and low-cardinality", () => {
  assert.equal(K6_METRIC_NAMES.length, 10);
  assert.deepEqual(K6_ALLOWED_TAGS, ["profile", "operation_type", "outcome", "reason"]);
  assert.doesNotMatch(
    JSON.stringify(K6_ALLOWED_TAGS),
    /board|principal|user|operation_id|event_id|socket|redis|sequence|url|error_message/i,
  );
});
