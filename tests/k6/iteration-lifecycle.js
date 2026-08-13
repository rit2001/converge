export const ITERATION_PHASES = Object.freeze([
  "connected",
  "authenticated",
  "snapshot_requested",
  "snapshot_response_received",
  "snapshot_body_parsed",
  "snapshot_schema_validated",
  "snapshot_identity_validated",
  "snapshot_hash_verified",
  "snapshot_projection_applied",
  "snapshot_applied",
  "join_emitted",
  "prejoin_delivery_buffered",
  "join_ack_received",
  "catchup_applied",
  "prejoin_buffer_reconciled",
  "joined",
  "command_scheduled",
  "command_sent",
  "command_acknowledged",
  "delivery_observed",
  "closed",
  "failed",
]);

export const ITERATION_FAILURE_REASONS = Object.freeze([
  "join_rejected",
  "join_timeout",
  "command_not_scheduled",
  "command_rejected",
  "command_ack_timeout",
  "command_ack_mismatch",
  "delivery_timeout",
  "delivery_mismatch",
  "invalid_event",
  "sequence_conflict",
  "unexpected_close",
  "canvas_gap",
  "canvas_sequence_conflict",
  "event_identity_conflict",
  "operation_identity_conflict",
  "delivery_sequence_regression",
  "reconciliation_unknown",
  "snapshot_request_failed",
  "snapshot_timeout",
  "snapshot_http_status",
  "snapshot_json_invalid",
  "snapshot_schema_invalid",
  "snapshot_board_mismatch",
  "snapshot_sequence_invalid",
  "snapshot_projection_invalid",
  "snapshot_duplicate_object",
  "snapshot_hash_mismatch",
  "snapshot_apply_failed",
  "snapshot_stale_generation",
  "snapshot_unknown",
  "join_not_emitted",
  "join_ack_invalid",
  "join_ack_after_terminal",
  "catchup_schema_invalid",
  "catchup_board_mismatch",
  "catchup_snapshot_invalid",
  "catchup_start_mismatch",
  "catchup_noncontiguous",
  "catchup_duplicate_conflict",
  "catchup_reducer_rejected",
  "catchup_head_mismatch",
  "catchup_hash_mismatch",
  "catchup_cursor_regression",
  "catchup_reconciliation_failed",
  "catchup_unknown",
  "prejoin_reconcile_failed",
  "unrelated_delivery_rejected",
  "active_command_missing",
  "command_schedule_failed",
  "premature_terminal",
  "unknown_internal_state",
]);

export function classifyIterationFailure(error, context = {}) {
  const code = typeof error?.code === "string" ? error.code : "";
  if (code.startsWith("CATCHUP_")) return code.toLowerCase();
  if (code.startsWith("SNAPSHOT_")) {
    const reason = code.toLowerCase();
    return ITERATION_FAILURE_REASONS.includes(reason) ? reason : "snapshot_unknown";
  }
  if (code === "CANVAS_SEQUENCE_CONFLICT") return "canvas_sequence_conflict";
  if (code === "OPERATION_IDENTITY_CONFLICT") return "operation_identity_conflict";
  if (code === "EVENT_IDENTITY_CONFLICT") return "event_identity_conflict";
  if (code === "DELIVERY_SEQUENCE_REGRESSION") return "delivery_sequence_regression";
  if (code === "FUTURE_OPERATION_BUFFER_OVERFLOW") return "reconciliation_unknown";
  if (code === "JOIN_ACK_TIMEOUT") return "join_timeout";
  if (code.startsWith("JOIN_") || code.startsWith("CONNECT_")) return "join_rejected";
  if (code === "COMMAND_ACK_TIMEOUT") return "command_ack_timeout";
  if (code.startsWith("COMMAND_ACK_") || code === "ACK_UNCORRELATED") return "command_ack_mismatch";
  if (code.startsWith("COMMAND_")) return "command_rejected";
  if (code === "LIVE_DELIVERY_TIMEOUT") return "delivery_timeout";
  if (
    code.startsWith("CATCH_UP_") ||
    code.startsWith("RANGE_") ||
    code === "LIVE_OPERATION_CONTEXT" ||
    code === "LIVE_OPERATION_BUFFER_OVERFLOW"
  )
    return "delivery_mismatch";
  if (code === "SEQUENCE_GAP" || code === "SEQUENCE_REGRESSION") return "sequence_conflict";
  if (code === "UNEXPECTED_DISCONNECT" || code === "WEBSOCKET_CLOSED") return "unexpected_close";
  if (code.startsWith("PACKET_") || code.startsWith("EVENT_") || code.includes("OPERATION"))
    return "invalid_event";
  if (context.terminal)
    return context.phase === "join_emitted" ? "join_ack_after_terminal" : "premature_terminal";
  if (context.phase === "snapshot_applied") return "join_not_emitted";
  if (context.phase.startsWith("snapshot_")) return "snapshot_unknown";
  if (context.phase === "authenticated") return "snapshot_request_failed";
  if (context.phase === "join_emitted" || context.phase === "prejoin_delivery_buffered")
    return "join_ack_invalid";
  if (context.phase === "join_ack_received") return "catchup_unknown";
  if (context.phase === "catchup_applied") return "prejoin_reconcile_failed";
  if (context.phase === "prejoin_buffer_reconciled" || context.phase === "joined")
    return "command_schedule_failed";
  if (context.phase === "command_scheduled")
    return context.activeCommand ? "command_schedule_failed" : "active_command_missing";
  return "unknown_internal_state";
}

export function createPreJoinDeliveryBuffer(maximumEntries) {
  if (!Number.isInteger(maximumEntries) || maximumEntries < 1 || maximumEntries > 1_024)
    throw new Error("Invalid pre-join delivery buffer limit");
  const entries = [];
  return Object.freeze({
    add(operation) {
      if (entries.length >= maximumEntries) {
        const error = new Error("Pre-join delivery buffer exceeded its fixed limit");
        error.code = "LIVE_OPERATION_BUFFER_OVERFLOW";
        throw error;
      }
      entries.push(operation);
    },
    drain(visitor) {
      const retained = entries.splice(0, entries.length);
      for (const operation of retained) visitor(operation);
    },
    clear() {
      entries.length = 0;
    },
    get size() {
      return entries.length;
    },
  });
}

export function matchesActiveCommand(activeOperationId, deliveredOperationId) {
  return (
    typeof activeOperationId === "string" &&
    activeOperationId.length > 0 &&
    activeOperationId === deliveredOperationId
  );
}

export function createIterationLifecycle({ requiredCommands, onPhase = () => undefined, cleanup }) {
  if (!Number.isInteger(requiredCommands) || requiredCommands < 1)
    throw new Error("Invalid collaboration lifecycle command count");
  let terminal = false;
  let scheduled = 0;
  let attempted = 0;
  let acknowledged = 0;
  let delivered = 0;

  const phase = (name) => {
    if (!ITERATION_PHASES.includes(name)) throw new Error("Invalid collaboration lifecycle phase");
    onPhase(name);
  };

  const finish = (name) => {
    if (terminal) return;
    terminal = true;
    cleanup();
    phase(name);
  };

  return Object.freeze({
    connected() {
      if (!terminal) phase("connected");
    },
    authenticated() {
      if (!terminal) phase("authenticated");
    },
    note(name) {
      if (!terminal) phase(name);
    },
    joined() {
      if (!terminal) phase("joined");
    },
    schedule(socket, callback, delayMs) {
      if (terminal) return;
      if (!socket || typeof socket.setTimeout !== "function")
        throw new Error("Unsupported k6 WebSocket timer boundary");
      if (!Number.isInteger(delayMs) || delayMs < 1)
        throw new Error("Invalid k6 WebSocket timer delay");
      scheduled += 1;
      phase("command_scheduled");
      socket.setTimeout(() => {
        if (!terminal) callback();
      }, delayMs);
    },
    commandSent() {
      if (terminal || scheduled <= attempted)
        throw new Error("Command sent without a scheduled attempt");
      attempted += 1;
      phase("command_sent");
    },
    commandAcknowledged() {
      if (terminal || attempted <= acknowledged)
        throw new Error("Command acknowledgement without an attempt");
      acknowledged += 1;
      phase("command_acknowledged");
    },
    deliveryObserved() {
      if (terminal || attempted <= delivered) throw new Error("Live delivery without an attempt");
      delivered += 1;
      phase("delivery_observed");
    },
    complete() {
      if (
        attempted < requiredCommands ||
        acknowledged < requiredCommands ||
        delivered < requiredCommands
      )
        throw new Error("Collaboration lifecycle completed before required evidence");
      finish("closed");
    },
    fail() {
      finish("failed");
    },
    snapshot() {
      return Object.freeze({ terminal, scheduled, attempted, acknowledged, delivered });
    },
  });
}
