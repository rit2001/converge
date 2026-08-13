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
  if (value.profile !== "smoke" || value.virtualUsers !== 2 || value.duration !== "30s")
    throw new Error("Benchmark environment metadata is not the smoke preset");
  assertSanitizedArtifact(value);
}

export function validateK6Summary(summary) {
  const metrics = summary?.metrics;
  if (!metrics || typeof metrics !== "object") throw new Error("k6 summary metrics are missing");
  const requiredThresholds = [
    "converge_iteration_failures",
    "converge_protocol_failures",
    "converge_sequence_gaps",
    "converge_socket_connect_duration",
    "converge_board_join_ack_duration",
    "converge_command_ack_duration",
  ];
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
  return {
    acknowledgements,
    liveEvents,
    transportDuplicatesObserved: value("converge_duplicate_events", "count"),
    transportDuplicatesSuppressed: value("converge_duplicate_events", "count"),
  };
}

export function validateDurableEvidence(evidence, distinctCommands) {
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
    "objects",
    "outbox",
    "distinctOutboxEvents",
    "published",
    "handled",
  ];
  if (
    converged.some((name) => evidence[name] !== distinctCommands) ||
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
