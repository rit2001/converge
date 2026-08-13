const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const ISO_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/;
const ERROR_CODES = new Set([
  "AUTHENTICATION_REQUIRED",
  "INVALID_AUTH_INPUT",
  "BOARD_NOT_FOUND",
  "FORBIDDEN",
  "INVALID_COMMAND",
  "PAYLOAD_TOO_LARGE",
  "RATE_LIMITED",
  "TARGET_NOT_FOUND",
  "TARGET_DELETED",
  "CONFLICT",
  "IDEMPOTENCY_CONFLICT",
  "RESYNC_REQUIRED",
  "CANNOT_REMOVE_OWNER",
  "ACCESS_REVOKED",
  "RECOVERY_BLOCKED",
  "INTERNAL_ERROR",
]);

export class ProtocolFailure extends Error {
  constructor(code) {
    super(`Socket protocol failure: ${code}`);
    this.name = "ProtocolFailure";
    this.code = code;
  }
}

export const OVERLAP_CLASSIFICATIONS = Object.freeze([
  "covered_by_snapshot",
  "exact_event_duplicate",
  "exact_operation_duplicate",
  "catchup_live_overlap",
  "next_contiguous_operation",
  "future_operation_buffered",
  "canvas_gap",
  "canvas_sequence_conflict",
  "event_identity_conflict",
  "operation_identity_conflict",
  "delivery_sequence_regression",
  "reconciliation_unknown",
]);

function failure(code) {
  throw new ProtocolFailure(code);
}

function object(value, keys, code) {
  if (!value || typeof value !== "object" || Array.isArray(value)) failure(code);
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  if (actual.length !== expected.length || actual.some((key, index) => key !== expected[index]))
    failure(code);
  return value;
}

function uuid(value, code) {
  if (typeof value !== "string" || !UUID_PATTERN.test(value)) failure(code);
  return value;
}

function sequence(value, positive, code) {
  if (!Number.isSafeInteger(value) || value < (positive ? 1 : 0)) failure(code);
  return value;
}

function finite(value, code) {
  if (typeof value !== "number" || !Number.isFinite(value) || Math.abs(value) > 1_000_000)
    failure(code);
  return value;
}

function utf8Length(value) {
  let bytes = 0;
  for (const character of value) {
    const codePoint = character.codePointAt(0);
    if (codePoint <= 0x7f) bytes += 1;
    else if (codePoint <= 0x7ff) bytes += 2;
    else if (codePoint <= 0xffff) bytes += 3;
    else bytes += 4;
  }
  return bytes;
}

function json(source, code) {
  try {
    return JSON.parse(source);
  } catch {
    return failure(code);
  }
}

export function parseHttpJsonBody(source, maximumBytes, code = "HTTP_JSON_INVALID") {
  if (
    typeof source !== "string" ||
    !Number.isSafeInteger(maximumBytes) ||
    maximumBytes < 1 ||
    utf8Length(source) > maximumBytes
  )
    failure(code);
  return json(source, code);
}

export function parseOpenPacket(packet, maximumBytes) {
  if (typeof packet !== "string" || utf8Length(packet) > maximumBytes || !packet.startsWith("0"))
    failure("ENGINE_OPEN_INVALID");
  const value = object(
    json(packet.slice(1), "ENGINE_OPEN_JSON"),
    ["sid", "upgrades", "pingInterval", "pingTimeout", "maxPayload"],
    "ENGINE_OPEN_SHAPE",
  );
  if (typeof value.sid !== "string" || value.sid.length < 1 || value.sid.length > 128)
    failure("ENGINE_OPEN_SID");
  if (!Array.isArray(value.upgrades) || value.upgrades.some((entry) => typeof entry !== "string"))
    failure("ENGINE_OPEN_UPGRADES");
  for (const key of ["pingInterval", "pingTimeout", "maxPayload"])
    if (!Number.isSafeInteger(value[key]) || value[key] < 1) failure("ENGINE_OPEN_LIMIT");
  return Object.freeze(value);
}

export function encodeConnect(authToken) {
  if (typeof authToken !== "string" || authToken.length < 1 || authToken.length > 4_096)
    failure("AUTH_TOKEN_INVALID");
  return `40${JSON.stringify({ token: authToken })}`;
}

export function encodeEvent(name, payload, acknowledgementId) {
  if (!/^[a-z]+(?::[a-z-]+)+$/.test(name)) failure("EVENT_NAME_INVALID");
  if (
    !Number.isSafeInteger(acknowledgementId) ||
    acknowledgementId < 0 ||
    acknowledgementId > 999999
  )
    failure("ACK_ID_INVALID");
  return `42${acknowledgementId}${JSON.stringify([name, payload])}`;
}

export function pongFor(packet) {
  if (packet !== "2") failure("ENGINE_PING_INVALID");
  return "3";
}

export function parseSocketPacket(packet, maximumBytes) {
  if (typeof packet !== "string" || utf8Length(packet) > maximumBytes) failure("PACKET_TOO_LARGE");
  if (packet === "2") return Object.freeze({ kind: "ping" });
  if (packet === "1") return Object.freeze({ kind: "engine_disconnect" });
  if (packet === "41") return Object.freeze({ kind: "socket_disconnect" });
  if (packet.startsWith("0"))
    return Object.freeze({ kind: "open", value: parseOpenPacket(packet, maximumBytes) });
  if (packet.startsWith("40")) {
    const value = object(json(packet.slice(2), "CONNECT_JSON"), ["sid"], "CONNECT_SHAPE");
    if (typeof value.sid !== "string" || value.sid.length < 1 || value.sid.length > 128)
      failure("CONNECT_SID");
    return Object.freeze({ kind: "connected", sid: value.sid });
  }
  if (packet.startsWith("43")) {
    const match = /^43([0-9]+)([\s\S]+)$/.exec(packet);
    if (!match) failure("ACK_FRAME_INVALID");
    const acknowledgementId = Number(match[1]);
    if (!Number.isSafeInteger(acknowledgementId) || acknowledgementId > 999999)
      failure("ACK_ID_INVALID");
    const values = json(match[2], "ACK_JSON");
    if (!Array.isArray(values) || values.length !== 1) failure("ACK_PAYLOAD_INVALID");
    return Object.freeze({ kind: "ack", acknowledgementId, value: values[0] });
  }
  if (packet.startsWith("42")) {
    const values = json(packet.slice(2), "EVENT_JSON");
    if (!Array.isArray(values) || values.length !== 2 || typeof values[0] !== "string")
      failure("EVENT_FRAME_INVALID");
    if (values[0] === "operation:committed")
      return Object.freeze({ kind: "operation", value: parseCommittedOperation(values[1]) });
    if (values[0] === "board:access-revoked")
      return Object.freeze({ kind: "revocation", value: parseRevocation(values[1]) });
    failure("EVENT_UNKNOWN");
  }
  if (packet.startsWith("44"))
    return Object.freeze({
      kind: "connect_error",
      value: parseConnectError(json(packet.slice(2), "CONNECT_ERROR_JSON")),
    });
  failure("PACKET_UNKNOWN");
}

export function parseProtocolError(value) {
  const parsed = object(value, ["ok", "code", "message", "retryable"], "ERROR_SHAPE");
  if (parsed.ok !== false || !ERROR_CODES.has(parsed.code)) failure("ERROR_CODE");
  if (typeof parsed.message !== "string" || parsed.message.length > 500) failure("ERROR_MESSAGE");
  if (typeof parsed.retryable !== "boolean") failure("ERROR_RETRYABLE");
  return Object.freeze(parsed);
}

function parseConnectError(value) {
  const parsed = object(value, ["message", "data"], "CONNECT_ERROR_SHAPE");
  if (typeof parsed.message !== "string" || parsed.message.length > 500)
    failure("CONNECT_ERROR_MESSAGE");
  return Object.freeze({
    message: "Socket connection rejected",
    data: parseProtocolError(parsed.data),
  });
}

export function parseJoinAcknowledgement(value, expectedBoardId) {
  if (value?.ok === false)
    return Object.freeze({ kind: "error", error: parseProtocolError(value) });
  const parsed = object(value, ["ok", "boardId", "joinWatermark"], "JOIN_ACK_SHAPE");
  if (parsed.ok !== true || uuid(parsed.boardId, "JOIN_BOARD_ID") !== expectedBoardId)
    failure("JOIN_ACK_IDENTITY");
  sequence(parsed.joinWatermark, false, "JOIN_WATERMARK");
  return Object.freeze({ kind: "success", value: parsed });
}

export function parseBoardSnapshot(value, expectedBoardId, maximumObjects = 1_024) {
  const parsed = object(value, ["id", "name", "lastSeq", "objects"], "SNAPSHOT_SHAPE");
  if (uuid(parsed.id, "SNAPSHOT_BOARD") !== expectedBoardId) failure("SNAPSHOT_IDENTITY");
  if (typeof parsed.name !== "string" || parsed.name.length < 1 || parsed.name.length > 120)
    failure("SNAPSHOT_NAME");
  sequence(parsed.lastSeq, false, "SNAPSHOT_SEQUENCE");
  if (!Array.isArray(parsed.objects) || parsed.objects.length > maximumObjects)
    failure("SNAPSHOT_OBJECTS");
  const identities = new Set();
  const objects = parsed.objects.map((entry) => {
    const validated = parseCanvasPayload("object.create", entry);
    if (identities.has(validated.id)) failure("SNAPSHOT_DUPLICATE_OBJECT");
    identities.add(validated.id);
    return Object.freeze(validated);
  });
  return Object.freeze({ ...parsed, objects: Object.freeze(objects) });
}

function parseCanvasPayload(type, value) {
  if (type === "object.delete") return object(value, [], "DELETE_PAYLOAD");
  if (type === "object.update") {
    if (!value || typeof value !== "object" || Array.isArray(value)) failure("UPDATE_PAYLOAD");
    const keys = Object.keys(value);
    if (keys.length < 1 || keys.some((key) => !["fill", "text"].includes(key)))
      failure("UPDATE_PAYLOAD");
    if (
      value.fill !== undefined &&
      (typeof value.fill !== "string" || !/^#[0-9a-f]{6}$/i.test(value.fill))
    )
      failure("UPDATE_FILL");
    if (value.text !== undefined && (typeof value.text !== "string" || value.text.length > 10_000))
      failure("UPDATE_TEXT");
    return value;
  }
  if (type === "object.transform") {
    if (!value || typeof value !== "object" || Array.isArray(value)) failure("TRANSFORM_PAYLOAD");
    const keys = Object.keys(value);
    if (
      keys.length < 1 ||
      keys.some((key) => !["x", "y", "width", "height", "rotation"].includes(key))
    )
      failure("TRANSFORM_PAYLOAD");
    for (const [key, entry] of Object.entries(value)) {
      finite(entry, "TRANSFORM_VALUE");
      if ((key === "width" || key === "height") && (entry < 8 || entry > 100_000))
        failure("TRANSFORM_SIZE");
    }
    return value;
  }
  const parsed = object(
    value,
    ["id", "kind", "x", "y", "width", "height", "rotation", "fill", "text"],
    "CREATE_PAYLOAD",
  );
  uuid(parsed.id, "OBJECT_ID");
  if (parsed.kind !== "rectangle" && parsed.kind !== "sticky") failure("OBJECT_KIND");
  for (const key of ["x", "y", "width", "height", "rotation"]) finite(parsed[key], "OBJECT_NUMBER");
  if (parsed.width < 8 || parsed.width > 100_000 || parsed.height < 8 || parsed.height > 100_000)
    failure("OBJECT_SIZE");
  if (typeof parsed.fill !== "string" || !/^#[0-9a-f]{6}$/i.test(parsed.fill))
    failure("OBJECT_FILL");
  if (typeof parsed.text !== "string" || parsed.text.length > 10_000) failure("OBJECT_TEXT");
  if (parsed.kind === "rectangle" && parsed.text !== "") failure("RECTANGLE_TEXT");
  return parsed;
}

export function parseCommittedOperation(value) {
  const parsed = object(
    value,
    [
      "schemaVersion",
      "opId",
      "boardId",
      "clientId",
      "baseSeq",
      "targetId",
      "clientTimestamp",
      "type",
      "payload",
      "seq",
      "committedAt",
    ],
    "OPERATION_SHAPE",
  );
  if (parsed.schemaVersion !== 1) failure("OPERATION_SCHEMA");
  for (const key of ["opId", "boardId", "clientId", "targetId"]) uuid(parsed[key], "OPERATION_ID");
  sequence(parsed.baseSeq, false, "OPERATION_BASE_SEQUENCE");
  sequence(parsed.seq, true, "OPERATION_SEQUENCE");
  if (!ISO_PATTERN.test(parsed.clientTimestamp) || !ISO_PATTERN.test(parsed.committedAt))
    failure("OPERATION_TIMESTAMP");
  if (
    !["object.create", "object.update", "object.transform", "object.delete"].includes(parsed.type)
  )
    failure("OPERATION_TYPE");
  parseCanvasPayload(parsed.type, parsed.payload);
  return Object.freeze(parsed);
}

export function parseCommandAcknowledgement(value, expected) {
  if (value?.ok === false)
    return Object.freeze({ kind: "error", error: parseProtocolError(value) });
  const parsed = object(value, ["ok", "duplicate", "operation"], "COMMAND_ACK_SHAPE");
  if (parsed.ok !== true || typeof parsed.duplicate !== "boolean") failure("COMMAND_ACK_STATUS");
  const operation = parseCommittedOperation(parsed.operation);
  if (operation.boardId !== expected.boardId || operation.opId !== expected.opId)
    failure("COMMAND_ACK_IDENTITY");
  return Object.freeze({ kind: "success", duplicate: parsed.duplicate, operation });
}

export function parseRevocation(value) {
  const parsed = object(value, ["schemaVersion", "boardId", "code", "message"], "REVOCATION_SHAPE");
  if (parsed.schemaVersion !== 1 || parsed.code !== "ACCESS_REVOKED") failure("REVOCATION_CODE");
  uuid(parsed.boardId, "REVOCATION_BOARD");
  if (
    typeof parsed.message !== "string" ||
    parsed.message.length < 1 ||
    parsed.message.length > 200
  )
    failure("REVOCATION_MESSAGE");
  return Object.freeze(parsed);
}

export function parseOperationRange(value, expected) {
  const parsed = object(
    value,
    ["boardId", "afterSeq", "watermark", "operations", "nextSeq", "hasMore"],
    "RANGE_SHAPE",
  );
  if (uuid(parsed.boardId, "RANGE_BOARD") !== expected.boardId) failure("RANGE_IDENTITY");
  sequence(parsed.afterSeq, false, "RANGE_AFTER");
  sequence(parsed.watermark, false, "RANGE_WATERMARK");
  sequence(parsed.nextSeq, false, "RANGE_NEXT");
  if (parsed.afterSeq !== expected.after || parsed.watermark !== expected.watermark)
    failure("RANGE_BOUNDARY");
  if (!Array.isArray(parsed.operations) || parsed.operations.length > 100)
    failure("RANGE_OPERATIONS");
  if (typeof parsed.hasMore !== "boolean") failure("RANGE_HAS_MORE");
  let next = parsed.afterSeq + 1;
  const operations = parsed.operations.map((operation) => {
    const validated = parseCommittedOperation(operation);
    if (
      validated.boardId !== parsed.boardId ||
      validated.seq !== next ||
      validated.seq > parsed.watermark
    )
      failure("RANGE_SEQUENCE");
    next += 1;
    return validated;
  });
  if (parsed.nextSeq !== (operations.at(-1)?.seq ?? parsed.afterSeq)) failure("RANGE_NEXT");
  if (parsed.hasMore !== parsed.nextSeq < parsed.watermark) failure("RANGE_PROGRESS");
  return Object.freeze({ ...parsed, operations: Object.freeze(operations) });
}

export function createDeliveryTracker(initialSequence, maximumEntries) {
  sequence(initialSequence, false, "TRACKER_SEQUENCE");
  if (!Number.isSafeInteger(maximumEntries) || maximumEntries < 1 || maximumEntries > 1_024)
    failure("TRACKER_LIMIT");
  const snapshotSequence = initialSequence;
  let lastSequence = initialSequence;
  const operationSequences = new Map();
  const sequenceOperations = new Map();
  const future = new Map();
  const order = [];
  const newlyApplied = [];

  const canonical = (value) => {
    if (Array.isArray(value)) return value.map(canonical);
    if (value && typeof value === "object")
      return Object.fromEntries(
        Object.keys(value)
          .sort()
          .map((key) => [key, canonical(value[key])]),
      );
    return typeof value === "number" && Object.is(value, -0) ? 0 : value;
  };
  const evidenceFor = (operation) => JSON.stringify(canonical(operation));
  const conflict = (code, evidence = {}) => {
    const error = new ProtocolFailure(code);
    error.reconciliationEvidence = Object.freeze({
      snapshotCanvasSequence: snapshotSequence,
      currentCanvasSequence: lastSequence,
      bufferedCanvasSequence: Number.isSafeInteger(evidence.bufferedCanvasSequence)
        ? evidence.bufferedCanvasSequence
        : 0,
      sameOperationIdentity: evidence.sameOperationIdentity === true,
      sameCanonicalContent: evidence.sameCanonicalContent === true,
    });
    throw error;
  };
  const retain = (parsed, evidence) => {
    operationSequences.set(parsed.opId, { seq: parsed.seq, evidence });
    sequenceOperations.set(parsed.seq, { opId: parsed.opId, evidence });
    order.push(parsed.opId);
    if (order.length > maximumEntries) {
      const expired = order.shift();
      const record = operationSequences.get(expired);
      operationSequences.delete(expired);
      if (record) sequenceOperations.delete(record.seq);
    }
  };

  return Object.freeze({
    observe(operation) {
      const parsed = parseCommittedOperation(operation);
      const evidence = evidenceFor(parsed);
      const known = operationSequences.get(parsed.opId);
      if (known !== undefined) {
        if (known.seq !== parsed.seq || known.evidence !== evidence)
          conflict("OPERATION_IDENTITY_CONFLICT", {
            bufferedCanvasSequence: parsed.seq,
            sameOperationIdentity: true,
            sameCanonicalContent: known.evidence === evidence,
          });
        return "catchup_live_overlap";
      }
      const bySequence = sequenceOperations.get(parsed.seq);
      if (bySequence !== undefined) {
        if (bySequence.opId !== parsed.opId || bySequence.evidence !== evidence)
          conflict("CANVAS_SEQUENCE_CONFLICT", {
            bufferedCanvasSequence: parsed.seq,
            sameOperationIdentity: bySequence.opId === parsed.opId,
            sameCanonicalContent: bySequence.evidence === evidence,
          });
        return "exact_operation_duplicate";
      }
      if (parsed.seq <= snapshotSequence) return "covered_by_snapshot";
      if (parsed.seq <= lastSequence)
        conflict("CANVAS_SEQUENCE_CONFLICT", { bufferedCanvasSequence: parsed.seq });

      const buffered = future.get(parsed.seq);
      if (buffered !== undefined) {
        if (buffered.opId !== parsed.opId)
          conflict("CANVAS_SEQUENCE_CONFLICT", {
            bufferedCanvasSequence: parsed.seq,
            sameCanonicalContent: buffered.evidence === evidence,
          });
        if (buffered.evidence !== evidence)
          conflict("OPERATION_IDENTITY_CONFLICT", {
            bufferedCanvasSequence: parsed.seq,
            sameOperationIdentity: true,
          });
        return "exact_operation_duplicate";
      }
      if (parsed.seq > lastSequence + 1) {
        if (future.size >= maximumEntries) conflict("FUTURE_OPERATION_BUFFER_OVERFLOW");
        future.set(parsed.seq, { parsed, opId: parsed.opId, evidence });
        return "future_operation_buffered";
      }

      lastSequence = parsed.seq;
      retain(parsed, evidence);
      newlyApplied.push(parsed);
      while (future.has(lastSequence + 1)) {
        const next = future.get(lastSequence + 1);
        future.delete(lastSequence + 1);
        lastSequence = next.parsed.seq;
        retain(next.parsed, next.evidence);
        newlyApplied.push(next.parsed);
      }
      return "next_contiguous_operation";
    },
    drainApplied() {
      return newlyApplied.splice(0, newlyApplied.length);
    },
    has(operationId) {
      return operationSequences.has(operationId);
    },
    snapshot() {
      return Object.freeze({
        lastSequence,
        retainedOperations: operationSequences.size,
        bufferedOperations: future.size,
      });
    },
  });
}

export function classifyTimeout(kind) {
  if (!["connect", "join_ack", "command_ack", "live_delivery"].includes(kind))
    failure("TIMEOUT_KIND_UNKNOWN");
  return Object.freeze({ code: `TIMEOUT_${kind.toUpperCase()}`, retryable: true });
}
