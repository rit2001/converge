import { spawn } from "node:child_process";
import { ITERATION_FAILURE_REASONS, ITERATION_PHASES } from "../tests/k6/iteration-lifecycle.js";

const FORBIDDEN_ARTIFACT_PATTERNS = [
  /authorization/i,
  /bearer/i,
  /database_url/i,
  /redis_url/i,
  /board_id/i,
  /user_id/i,
  /stream_key/i,
  /password/i,
  /credential/i,
  /payload/i,
  /\/Users\//,
  /\/home\//,
];

export class CleanupStack {
  #entries = [];
  #closed = false;

  own(label, cleanup) {
    if (this.#closed) throw new Error("Cleanup ownership is closed");
    this.#entries.push({ label, cleanup });
  }

  async close() {
    if (this.#closed) return;
    this.#closed = true;
    const failures = [];
    for (const entry of this.#entries.reverse()) {
      try {
        await entry.cleanup();
      } catch {
        failures.push(entry.label);
      }
    }
    if (failures.length > 0) throw new Error(`Owned cleanup failed: ${failures.sort().join(",")}`);
  }
}

export function assertSanitizedArtifact(value, secrets = []) {
  const rendered = typeof value === "string" ? value : JSON.stringify(value);
  for (const pattern of FORBIDDEN_ARTIFACT_PATTERNS) {
    if (pattern.test(rendered)) throw new Error("Benchmark artifact contains forbidden evidence");
  }
  for (const secret of secrets) {
    if (secret && rendered.includes(secret))
      throw new Error("Benchmark artifact contains test-owned sensitive evidence");
  }
}

export function validateEnvironmentArtifact(value) {
  const required = [
    "timestampUtc",
    "gitCommitSha",
    "k6Version",
    "nodeVersion",
    "pnpmVersion",
    "os",
    "architecture",
    "profile",
    "virtualUsers",
    "duration",
    "apiReplicas",
    "workerReplicas",
    "configuration",
  ];
  if (Object.keys(value).sort().join("|") !== required.sort().join("|"))
    throw new Error("Benchmark environment metadata has an unexpected shape");
  const preset =
    value.profile === "smoke" && value.virtualUsers === 2 && value.duration === "30s"
      ? "smoke"
      : value.profile === "baseline" && value.virtualUsers === 10 && value.duration === "2m"
        ? "baseline"
        : value.profile === "scale-step" && value.virtualUsers === 100 && value.duration === "3m45s"
          ? "scale-step"
          : undefined;
  if (!preset) throw new Error("Benchmark environment metadata is not an approved preset");
  assertSanitizedArtifact(value);
}

export function validateK6Summary(
  summary,
  {
    requireCommandAckThreshold = true,
    maximumSnapshots,
    expectedScaleSessions,
    requirePersistentScaleEvidence = false,
  } = {},
) {
  const metrics = summary?.metrics;
  if (!metrics || typeof metrics !== "object") throw new Error("k6 summary metrics are missing");
  const requiredThresholds = [
    "converge_iteration_failures",
    "converge_protocol_failures",
    "converge_sequence_gaps",
    "converge_socket_connect_duration",
    "converge_board_join_ack_duration",
  ];
  if (requireCommandAckThreshold) requiredThresholds.push("converge_command_ack_duration");
  if (requirePersistentScaleEvidence)
    requiredThresholds.push(
      "converge_scale_second_invocations",
      "converge_scale_unexpected_disconnects",
      "converge_scale_session_failures",
    );
  for (const name of requiredThresholds) {
    const thresholds = metrics[name]?.thresholds;
    if (!thresholds || Object.values(thresholds).some((failed) => failed !== false))
      throw new Error(`k6 threshold failed: ${name}`);
  }
  const value = (name, field) => Number(metrics[name]?.[field] ?? 0);
  if (value("converge_iteration_failures", "value") !== 0)
    throw new Error("k6 iterations reported failures");
  if (value("converge_protocol_failures", "value") !== 0)
    throw new Error("k6 protocol reported failures");
  if (value("converge_sequence_gaps", "count") !== 0)
    throw new Error("k6 sequence validation reported a gap");
  const acknowledgements = value("converge_commands_acknowledged", "count");
  const liveEvents = value("converge_live_events_received", "count");
  if (acknowledgements <= 0 || liveEvents < acknowledgements)
    throw new Error("k6 did not observe committed commands and corresponding live delivery");
  const snapshotRequestCount = value("converge_snapshot_requests", "count");
  const scaleSessionCount = value("converge_scale_sessions_started", "count");
  if (
    maximumSnapshots !== undefined &&
    (!Number.isSafeInteger(snapshotRequestCount) || snapshotRequestCount > maximumSnapshots)
  )
    throw new Error("Persistent scale snapshot request budget exceeded");
  if (expectedScaleSessions !== undefined && scaleSessionCount !== expectedScaleSessions)
    throw new Error("Persistent scale session ownership did not converge");
  const scaleSessionsInitialized = value("converge_scale_sessions_initialized", "count");
  const scaleSessionsCompleted = value("converge_scale_sessions_completed", "count");
  const scaleSecondInvocations = value("converge_scale_second_invocations", "count");
  const scaleUnexpectedDisconnects = value("converge_scale_unexpected_disconnects", "count");
  const scaleSessionFailures = value("converge_scale_session_failures", "count");
  if (
    requirePersistentScaleEvidence &&
    (scaleSessionsInitialized !== scaleSessionCount ||
      scaleSessionsCompleted !== scaleSessionCount ||
      scaleSecondInvocations !== 0 ||
      scaleUnexpectedDisconnects !== 0 ||
      scaleSessionFailures !== 0 ||
      snapshotRequestCount > scaleSessionCount)
  )
    throw new Error("Persistent scale lifecycle evidence did not converge");
  return {
    acknowledgements,
    liveEvents,
    transportDuplicatesObserved: value("converge_duplicate_events", "count"),
    transportDuplicatesSuppressed: value("converge_duplicate_events", "count"),
    snapshotRequests: snapshotRequestCount,
    scaleSessions: scaleSessionCount,
    rangeRequests: value("converge_range_requests", "count"),
    scaleSessionsInitialized,
    scaleSessionsCompleted,
    scaleSecondInvocations,
    scaleUnexpectedDisconnects,
    scaleSessionFailures,
  };
}

function appendBoundedTail(current, chunk, maximumBytes) {
  const combined = Buffer.concat([current, Buffer.from(chunk)]);
  return combined.length <= maximumBytes
    ? combined
    : combined.subarray(combined.length - maximumBytes);
}

export function runStreamingCommand(command, args, { cwd, env, tailBytes = 16_384 } = {}) {
  if (!Number.isInteger(tailBytes) || tailBytes < 1 || tailBytes > 65_536)
    throw new Error("Invalid subprocess tail limit");
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
    let stdoutTail = Buffer.alloc(0);
    let stderrTail = Buffer.alloc(0);
    let stdoutBytes = 0;
    let stderrBytes = 0;
    const diagnostics = createFailureDiagnosticAggregator();
    child.stdout.on("data", (chunk) => {
      stdoutBytes += chunk.length;
      stdoutTail = appendBoundedTail(stdoutTail, chunk, tailBytes);
      diagnostics.push("stdout", chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderrBytes += chunk.length;
      stderrTail = appendBoundedTail(stderrTail, chunk, tailBytes);
      diagnostics.push("stderr", chunk);
    });
    child.once("error", reject);
    child.once("close", (status) => {
      diagnostics.finish();
      resolvePromise({
        status: status ?? 1,
        stdoutTail: stdoutTail.toString("utf8"),
        stderrTail: stderrTail.toString("utf8"),
        stdoutBytes,
        stderrBytes,
        failureDiagnostics: diagnostics.snapshot(),
      });
    });
  });
}

const FAILURE_EVIDENCE =
  /^(?: status=[0-9]+| last_sequence=[0-9]+| objects=[0-9]+| response_bytes=[0-9]+| configured_limit=[0-9]+| snapshot_canvas_sequence=[0-9]+| current_canvas_sequence=[0-9]+| buffered_canvas_sequence=[0-9]+| sameOperationIdentity=(?:true|false)| sameCanonicalContent=(?:true|false))*$/;

export function createFailureDiagnosticAggregator() {
  const counts = new Map();
  const partial = { stdout: "", stderr: "" };
  let invalid = 0;
  const accept = (line) => {
    let payload = line;
    if (!line.startsWith("converge_failure ")) {
      const wrapped =
        /^time="[0-9T:.+Z-]{20,40}" level=info msg="(converge_failure [^"\\]{1,512})" source=console$/.exec(
          line,
        );
      if (wrapped) payload = wrapped[1];
      else {
        if (line.includes("converge_failure")) invalid += 1;
        return;
      }
    }
    const match =
      /^converge_failure vu=[0-9]+ iteration=[0-9]+ phase=([a-z_]+) reason=([a-z_]+)(.*)$/.exec(
        payload,
      );
    if (
      !match ||
      !ITERATION_PHASES.includes(match[1]) ||
      !ITERATION_FAILURE_REASONS.includes(match[2]) ||
      !FAILURE_EVIDENCE.test(match[3])
    ) {
      invalid += 1;
      return;
    }
    const key = `${match[1]}:${match[2]}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  };
  return Object.freeze({
    push(stream, chunk) {
      if (!(stream in partial)) throw new Error("Unknown diagnostic stream");
      const joined = partial[stream] + Buffer.from(chunk).toString("utf8");
      const lines = joined.split("\n");
      partial[stream] = lines.pop() ?? "";
      for (const line of lines) accept(line.trim());
    },
    finish() {
      for (const stream of Object.keys(partial)) {
        if (partial[stream]) accept(partial[stream].trim());
        partial[stream] = "";
      }
    },
    snapshot() {
      return Object.freeze({
        invalid,
        counts: Object.freeze(
          [...counts.entries()]
            .sort(([left], [right]) => left.localeCompare(right))
            .map(([key, count]) => {
              const [phase, reason] = key.split(":");
              return Object.freeze({ phase, reason, count });
            }),
        ),
      });
    },
  });
}

export function aggregateFailureDiagnostics(output) {
  const aggregator = createFailureDiagnosticAggregator();
  aggregator.push("stdout", `${String(output)}\n`);
  aggregator.finish();
  return aggregator.snapshot();
}

export function readK6FailureDiagnostic(summary) {
  const metrics = summary?.metrics ?? {};
  const value = (name, field) => Number(metrics[name]?.[field] ?? 0);
  return Object.freeze({
    iterations: value("iterations", "count"),
    iterationFailures: value("converge_iteration_failures", "passes"),
    protocolFailures: value("converge_protocol_failures", "passes"),
    sequenceGaps: value("converge_sequence_gaps", "count"),
    acknowledgements: value("converge_commands_acknowledged", "count"),
    liveEvents: value("converge_live_events_received", "count"),
    transportDuplicates: value("converge_duplicate_events", "count"),
  });
}

export async function collectPostFailureEvidence(originalFailure, collector) {
  try {
    return Object.freeze({ originalFailure, evidence: await collector(), collectionFailed: false });
  } catch {
    return Object.freeze({ originalFailure, evidence: undefined, collectionFailed: true });
  }
}

export function validateDurableEvidence(
  evidence,
  distinctCommands,
  expectedObjects = distinctCommands,
) {
  if (!Number.isSafeInteger(distinctCommands) || distinctCommands < 0)
    throw new Error("Distinct command evidence is invalid");
  const required = [
    "lastSequence",
    "deliveryHead",
    "operations",
    "distinctOperations",
    "objects",
    "outbox",
    "distinctOutboxEvents",
    "published",
    "handled",
    "identityMismatches",
    "invalidPublicationIds",
  ];
  if (required.some((name) => !Number.isSafeInteger(evidence?.[name])))
    throw new Error("Durable acceptance evidence is incomplete");
  const converged = [
    "lastSequence",
    "deliveryHead",
    "operations",
    "distinctOperations",
    "outbox",
    "distinctOutboxEvents",
    "published",
    "handled",
  ];
  if (
    converged.some((name) => evidence[name] !== distinctCommands) ||
    evidence.objects !== expectedObjects ||
    evidence.operations !== evidence.distinctOperations ||
    evidence.outbox !== evidence.distinctOutboxEvents ||
    evidence.identityMismatches !== 0 ||
    evidence.invalidPublicationIds !== 0 ||
    evidence.invalidPublishedState !== 0
  )
    throw new Error("Durable state indicates logical reapplication");
  return Object.freeze({
    ...evidence,
    distinctCommands,
    logicalReapplications: 0,
    logicalReducerApplications: evidence.operations,
  });
}

export function durableEvidenceSql(boardId) {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(boardId))
    throw new Error("Durable evidence board identity is invalid");
  return `SELECT b.last_seq, b.last_delivery_seq,
    (SELECT count(*) FROM board_operations WHERE board_id = b.id),
    (SELECT count(DISTINCT op_id) FROM board_operations WHERE board_id = b.id),
    (SELECT count(*) FROM board_objects WHERE board_id = b.id AND deleted_seq IS NULL),
    (SELECT count(*) FROM outbox_events WHERE board_id = b.id AND event_type = 'operation.committed'),
    (SELECT count(DISTINCT id) FROM outbox_events WHERE board_id = b.id AND event_type = 'operation.committed'),
    (SELECT count(*) FROM outbox_events WHERE board_id = b.id AND event_type = 'operation.committed' AND status = 'published'),
    (SELECT count(*) FROM outbox_events WHERE board_id = b.id AND event_type = 'operation.committed' AND payload->>'eventId' <> id::text),
    (SELECT count(*) FROM outbox_events WHERE board_id = b.id AND event_type = 'operation.committed' AND status = 'published' AND (redis_entry_id IS NULL OR redis_entry_id !~ '^[1-9][0-9]*-[0-9]+$')),
    (SELECT count(*) FROM outbox_events WHERE board_id = b.id AND event_type = 'operation.committed' AND status = 'published' AND (next_attempt_at <> 'infinity'::timestamptz OR published_at IS NULL OR lease_owner IS NOT NULL OR lease_token IS NOT NULL OR leased_until IS NOT NULL OR last_error_code IS NOT NULL OR last_error_message IS NOT NULL OR last_error_at IS NOT NULL))
   FROM boards b WHERE b.id = '${boardId}'`;
}

export async function runWithOwnedCleanup(steps) {
  const cleanup = new CleanupStack();
  try {
    await steps.prepare(cleanup);
    const status = await steps.runK6();
    if (status !== 0) throw new Error(`k6 exited nonzero: ${status}`);
    const summary = await steps.readSummary();
    validateK6Summary(summary);
    await steps.verify(summary);
    return summary;
  } finally {
    await cleanup.close();
  }
}
