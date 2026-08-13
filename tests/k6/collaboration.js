import http from "k6/http";
import ws from "k6/ws";
import { Counter, Rate, Trend } from "k6/metrics";
import { parseWorkloadConfig, workloadOptions } from "./config.js";
import { deterministicUuid, workloadCommandType, workloadTargetUuid } from "./identity.js";
import {
  classifyIterationFailure,
  classifyPostConnectState,
  createIterationLifecycle,
  createPreJoinDeliveryBuffer,
  matchesActiveCommand,
  shouldRunCommandTimeout,
} from "./iteration-lifecycle.js";
import {
  ProtocolFailure,
  classifyTimeout,
  createDeliveryTracker,
  encodeConnect,
  encodeEvent,
  parseBoardSnapshot,
  parseCommandAcknowledgement,
  parseHttpJsonBody,
  parseJoinAcknowledgement,
  parseOperationRange,
  parseSocketPacket,
  pongFor,
} from "./protocol.js";

const config = parseWorkloadConfig(globalThis.__ENV);
export const options = workloadOptions(config);

const socketConnectDuration = new Trend("converge_socket_connect_duration", true);
const boardJoinDuration = new Trend("converge_board_join_ack_duration", true);
const commandAckDuration = new Trend("converge_command_ack_duration", true);
const liveDeliveryDuration = new Trend("converge_live_delivery_duration", true);
const iterationFailures = new Rate("converge_iteration_failures");
const protocolFailures = new Rate("converge_protocol_failures");
const duplicateEvents = new Counter("converge_duplicate_events");
const sequenceGaps = new Counter("converge_sequence_gaps");
const commandsAcknowledged = new Counter("converge_commands_acknowledged");
const liveEventsReceived = new Counter("converge_live_events_received");
let boundedObjectInitialized = false;

function tags(outcome, operationType = "object.create") {
  return { profile: config.profile, operation_type: operationType, outcome };
}

function identity(kind, ordinal) {
  return deterministicUuid(
    kind,
    Number(globalThis.__VU ?? 0),
    Number(globalThis.__ITER ?? 0),
    ordinal,
  );
}

function socketEndpoint(origin) {
  const secure = origin.startsWith("https://") || origin.startsWith("wss://");
  const authority = origin.replace(/^(?:https?|wss?):\/\//, "").replace(/\/$/, "");
  return `${secure ? "wss" : "ws"}://${authority}/socket.io/?EIO=4&transport=websocket`;
}

function command(clientId, ordinal, baseSequence, objectInitialized) {
  const vu = Number(globalThis.__VU ?? 0);
  const iteration = Number(globalThis.__ITER ?? 0);
  const targetId = workloadTargetUuid(config.workloadModel, vu, iteration, ordinal);
  const type = workloadCommandType(config.workloadModel, objectInitialized, ordinal);
  return {
    schemaVersion: 1,
    opId: identity("operation", ordinal),
    boardId: config.boardId,
    clientId,
    baseSeq: baseSequence,
    targetId,
    clientTimestamp: new Date().toISOString(),
    type,
    payload:
      type === "object.create"
        ? {
            id: targetId,
            kind: "rectangle",
            x: 40 + vu,
            y: 40 + vu,
            width: 160,
            height: 100,
            rotation: 0,
            fill: "#818cf8",
            text: "",
          }
        : {
            fill: `#${((iteration * 100 + ordinal + vu) % 0xffffff).toString(16).padStart(6, "0")}`,
          },
  };
}

function catchUpReason(error, stage) {
  const code = typeof error?.code === "string" ? error.code : "";
  if (code === "RANGE_IDENTITY") return "catchup_board_mismatch";
  if (["RANGE_BOUNDARY", "RANGE_AFTER", "RANGE_WATERMARK"].includes(code))
    return "catchup_start_mismatch";
  if (["RANGE_SEQUENCE", "SEQUENCE_GAP"].includes(code)) return "catchup_noncontiguous";
  if (code === "DUPLICATE_CONFLICT") return "catchup_duplicate_conflict";
  if (code === "SEQUENCE_REGRESSION") return "catchup_cursor_regression";
  if (code === "CATCH_UP_INCOMPLETE" || code === "CATCH_UP_NO_PROGRESS")
    return "catchup_head_mismatch";
  if (stage === "range_parse") return "catchup_schema_invalid";
  if (stage === "operation_apply") return "catchup_reducer_rejected";
  return "catchup_unknown";
}

function sanitizedCatchUpFailure(reason) {
  const error = new Error("Authoritative catch-up failed");
  error.code = reason.toUpperCase();
  return error;
}

function snapshotFailure(reason, evidence = {}) {
  const error = new Error("Authoritative snapshot initialization failed");
  error.code = reason.toUpperCase();
  error.snapshotEvidence = Object.freeze({
    status: Number.isInteger(evidence.status) ? evidence.status : 0,
    lastSequence: Number.isSafeInteger(evidence.lastSequence) ? evidence.lastSequence : 0,
    objectCount: Number.isSafeInteger(evidence.objectCount) ? evidence.objectCount : 0,
    responseBytes: Number.isSafeInteger(evidence.responseBytes) ? evidence.responseBytes : 0,
    configuredLimit: Number.isSafeInteger(evidence.configuredLimit) ? evidence.configuredLimit : 0,
  });
  return error;
}

function snapshotSchemaReason(error) {
  const code = typeof error?.code === "string" ? error.code : "";
  if (code === "SNAPSHOT_IDENTITY") return "snapshot_board_mismatch";
  if (code === "SNAPSHOT_SEQUENCE") return "snapshot_sequence_invalid";
  if (code === "SNAPSHOT_DUPLICATE_OBJECT") return "snapshot_duplicate_object";
  if (code === "SNAPSHOT_OBJECTS" || /^(?:OBJECT|CREATE|SNAPSHOT_NAME)/.test(code))
    return "snapshot_projection_invalid";
  if (code.startsWith("SNAPSHOT_")) return "snapshot_schema_invalid";
  return "snapshot_unknown";
}

export default function collaborationWorkload() {
  const startedAt = Date.now();
  const clientId = identity("client", 0);
  let failed = false;
  let protocolFailed = false;
  let complete = false;
  let connected = false;
  let joined = false;
  let revoked = false;
  let acknowledgementId = 0;
  let commandOrdinal = 0;
  let tracker;
  let activeCommand;
  let currentPhase = "authenticated";
  let failurePhase;
  let failureReason;
  const pendingAcknowledgements = new Map();
  const preJoinDeliveries = createPreJoinDeliveryBuffer(256);
  const lifecycle = createIterationLifecycle({
    requiredCommands: config.commandsPerClient,
    onPhase: (phase) => {
      currentPhase = phase;
      if (config.debugPhases)
        globalThis.console.log(
          `converge_phase vu=${globalThis.__VU} iteration=${globalThis.__ITER} phase=${phase}`,
        );
    },
    cleanup: () => {
      pendingAcknowledgements.clear();
      preJoinDeliveries.clear();
      activeCommand = undefined;
    },
  });

  const response = ws.connect(socketEndpoint(config.socketUrl), {}, (socket) => {
    const close = (normal = false) => {
      if (complete) return;
      if (normal) lifecycle.complete();
      complete = true;
      if (connected) socket.send("41");
      socket.close();
    };

    const fail = (error) => {
      if (failed) return;
      failed = true;
      protocolFailed = protocolFailed || error instanceof ProtocolFailure;
      failurePhase = currentPhase;
      failureReason = classifyIterationFailure(error, {
        phase: failurePhase,
        terminal: lifecycle.snapshot().terminal,
        activeCommand: activeCommand !== undefined,
      });
      if (config.debugPhases || config.debugFailures) {
        const evidence = error?.snapshotEvidence;
        const overlap = error?.reconciliationEvidence;
        globalThis.console.log(
          `converge_failure vu=${globalThis.__VU} iteration=${globalThis.__ITER} phase=${failurePhase} reason=${failureReason}${
            evidence
              ? ` status=${evidence.status} last_sequence=${evidence.lastSequence} objects=${evidence.objectCount} response_bytes=${evidence.responseBytes} configured_limit=${evidence.configuredLimit}`
              : ""
          }${
            overlap
              ? ` snapshot_canvas_sequence=${overlap.snapshotCanvasSequence} current_canvas_sequence=${overlap.currentCanvasSequence} buffered_canvas_sequence=${overlap.bufferedCanvasSequence} sameOperationIdentity=${overlap.sameOperationIdentity} sameCanonicalContent=${overlap.sameCanonicalContent}`
              : ""
          }`,
        );
      }
      lifecycle.fail();
      if (error instanceof ProtocolFailure && /SEQUENCE_(?:GAP|REGRESSION)/.test(error.code))
        sequenceGaps.add(1, tags("detected"));
      close(false);
    };

    const catchUp = (watermark) => {
      let after = tracker.snapshot().lastSequence;
      let stage = "range_request";
      let operationCount = 0;
      let firstSequence = 0;
      let lastSequence = 0;
      try {
        if (after > watermark) throw new ProtocolFailure("SEQUENCE_REGRESSION");
        for (let page = 0; after < watermark && page < 10; page += 1) {
          stage = "range_request";
          const result = http.get(
            `${config.baseUrl}/v1/boards/${config.boardId}/operations?after=${after}&watermark=${watermark}`,
            { headers: { Authorization: `Bearer ${config.authToken}` }, tags: {} },
          );
          if (result.status !== 200) throw new ProtocolFailure("CATCH_UP_HTTP");
          stage = "range_parse";
          let body;
          try {
            body = parseHttpJsonBody(result.body, config.maxPacketBytes, "CATCH_UP_JSON");
          } catch {
            throw new ProtocolFailure("CATCH_UP_JSON");
          }
          const range = parseOperationRange(body, { boardId: config.boardId, after, watermark });
          operationCount += range.operations.length;
          firstSequence = firstSequence || range.operations[0]?.seq || 0;
          lastSequence = range.operations.at(-1)?.seq || lastSequence;
          stage = "operation_apply";
          for (const operation of range.operations) {
            tracker.observe(operation);
            tracker.drainApplied();
          }
          if (range.nextSeq === after) throw new ProtocolFailure("CATCH_UP_NO_PROGRESS");
          after = range.nextSeq;
        }
        stage = "head_verify";
        if (after !== watermark) throw new ProtocolFailure("CATCH_UP_INCOMPLETE");
      } catch (error) {
        const reason = catchUpReason(error, stage);
        if (config.debugPhases)
          globalThis.console.log(
            `converge_catchup phase=${stage} reason=${reason} cursor=${after} head=${watermark} operations=${operationCount} first_sequence=${firstSequence} last_sequence=${lastSequence}`,
          );
        throw sanitizedCatchUpFailure(reason);
      }
    };

    const loadSnapshot = () => {
      lifecycle.note("snapshot_requested");
      let result;
      try {
        result = http.get(`${config.baseUrl}/v1/boards/${config.boardId}`, {
          headers: { Authorization: `Bearer ${config.authToken}` },
          tags: {},
          timeout: `${config.acknowledgementTimeoutMs}ms`,
        });
      } catch {
        throw snapshotFailure("snapshot_request_failed");
      }
      lifecycle.note("snapshot_response_received");
      if (result.status === 0) throw snapshotFailure("snapshot_timeout");
      if (result.status !== 200)
        throw snapshotFailure("snapshot_http_status", { status: result.status });
      let body;
      try {
        body = parseHttpJsonBody(result.body, config.maxPacketBytes, "SNAPSHOT_JSON");
      } catch {
        throw snapshotFailure("snapshot_json_invalid", {
          status: result.status,
          responseBytes: typeof result.body === "string" ? result.body.length : 0,
          configuredLimit: config.maxPacketBytes,
        });
      }
      lifecycle.note("snapshot_body_parsed");
      let snapshot;
      try {
        snapshot = parseBoardSnapshot(body, config.boardId, 1_024);
      } catch (error) {
        throw snapshotFailure(snapshotSchemaReason(error), {
          status: result.status,
          lastSequence: Number.isSafeInteger(body?.lastSeq) ? body.lastSeq : 0,
          objectCount: Array.isArray(body?.objects) ? body.objects.length : 0,
        });
      }
      lifecycle.note("snapshot_schema_validated");
      lifecycle.note("snapshot_identity_validated");
      lifecycle.note("snapshot_hash_verified");
      try {
        tracker = createDeliveryTracker(snapshot.lastSeq, 256);
      } catch {
        throw snapshotFailure("snapshot_apply_failed", {
          status: result.status,
          lastSequence: snapshot.lastSeq,
          objectCount: snapshot.objects.length,
        });
      }
      lifecycle.note("snapshot_projection_applied");
      lifecycle.note("snapshot_applied");
    };

    const scheduleNextCommand = () => {
      if (failed || revoked) return close(false);
      if (commandOrdinal >= config.commandsPerClient) return close(true);
      lifecycle.schedule(
        socket,
        () => {
          if (failed || complete) return;
          commandOrdinal += 1;
          const value = command(
            clientId,
            commandOrdinal,
            tracker.snapshot().lastSequence,
            boundedObjectInitialized,
          );
          const id = ++acknowledgementId;
          activeCommand = {
            value,
            sentAt: Date.now(),
            liveReceived: false,
            acknowledged: false,
          };
          pendingAcknowledgements.set(id, { kind: "command", startedAt: Date.now(), value });
          lifecycle.commandSent();
          socket.send(encodeEvent("operation:submit", value, id));
          socket.setTimeout(() => {
            if (!failed && shouldRunCommandTimeout(value.opId, activeCommand, "acknowledged")) {
              classifyTimeout("command_ack");
              fail(new ProtocolFailure("COMMAND_ACK_TIMEOUT"));
            }
          }, config.acknowledgementTimeoutMs);
        },
        commandOrdinal === 0 ? 1 : config.commandIntervalMs,
      );
    };

    const advanceAfterCurrentCommand = () => {
      if (config.workloadModel === "bounded" && activeCommand?.value.type === "object.create")
        boundedObjectInitialized = true;
      scheduleNextCommand();
    };

    const confirmCommandDelivery = (operation) => {
      if (!activeCommand || !matchesActiveCommand(activeCommand.value.opId, operation.opId)) return;
      if (activeCommand.liveReceived) return;
      activeCommand.liveReceived = true;
      lifecycle.deliveryObserved();
      liveDeliveryDuration.add(Date.now() - activeCommand.sentAt, tags("live"));
      if (activeCommand.acknowledged) advanceAfterCurrentCommand();
    };

    const observeOperation = (operation) => {
      const outcome = tracker.observe(operation);
      if (
        outcome === "covered_by_snapshot" ||
        outcome === "catchup_live_overlap" ||
        outcome === "exact_operation_duplicate"
      )
        duplicateEvents.add(1, tags("duplicate", operation.type));
      else liveEventsReceived.add(1, tags("received", operation.type));
      for (const applied of tracker.drainApplied()) confirmCommandDelivery(applied);
    };

    socket.on("message", (frame) => {
      try {
        const packet = parseSocketPacket(frame, config.maxPacketBytes);
        if (packet.kind === "open") {
          socket.send(encodeConnect(config.authToken));
          return;
        }
        if (packet.kind === "ping") {
          socket.send(pongFor(frame));
          return;
        }
        if (packet.kind === "connected") {
          if (connected) throw new ProtocolFailure("CONNECT_DUPLICATE");
          connected = true;
          lifecycle.connected();
          lifecycle.authenticated();
          socketConnectDuration.add(Date.now() - startedAt, tags("connected"));
          loadSnapshot();
          const id = ++acknowledgementId;
          pendingAcknowledgements.set(id, { kind: "join", startedAt: Date.now() });
          socket.send(
            encodeEvent(
              "board:join",
              {
                schemaVersion: 1,
                boardId: config.boardId,
                clientId,
                lastAppliedSeq: tracker.snapshot().lastSequence,
              },
              id,
            ),
          );
          lifecycle.note("join_emitted");
          socket.setTimeout(() => {
            if (!joined && !failed) {
              classifyTimeout("join_ack");
              fail(new ProtocolFailure("JOIN_ACK_TIMEOUT"));
            }
          }, config.acknowledgementTimeoutMs);
          return;
        }
        if (packet.kind === "ack") {
          const pending = pendingAcknowledgements.get(packet.acknowledgementId);
          if (!pending) throw new ProtocolFailure("ACK_UNCORRELATED");
          pendingAcknowledgements.delete(packet.acknowledgementId);
          if (pending.kind === "join") {
            lifecycle.note("join_ack_received");
            const result = parseJoinAcknowledgement(packet.value, config.boardId);
            if (result.kind === "error") throw new ProtocolFailure(`JOIN_${result.error.code}`);
            boardJoinDuration.add(Date.now() - pending.startedAt, tags("joined"));
            catchUp(result.value.joinWatermark);
            lifecycle.note("catchup_applied");
            preJoinDeliveries.drain(observeOperation);
            if (tracker.snapshot().bufferedOperations > 0)
              throw new ProtocolFailure("SEQUENCE_GAP");
            lifecycle.note("prejoin_buffer_reconciled");
            joined = true;
            lifecycle.joined();
            scheduleNextCommand();
            return;
          }
          const result = parseCommandAcknowledgement(packet.value, {
            boardId: config.boardId,
            opId: pending.value.opId,
          });
          if (result.kind === "error") throw new ProtocolFailure(`COMMAND_${result.error.code}`);
          commandAckDuration.add(Date.now() - pending.startedAt, tags("committed"));
          commandsAcknowledged.add(1, tags("committed"));
          lifecycle.commandAcknowledged();
          activeCommand.acknowledged = true;
          if (!activeCommand.liveReceived) {
            const acknowledgedOperationId = pending.value.opId;
            socket.setTimeout(() => {
              try {
                if (
                  !failed &&
                  shouldRunCommandTimeout(acknowledgedOperationId, activeCommand, "liveReceived")
                ) {
                  classifyTimeout("live_delivery");
                  catchUp(result.operation.seq);
                  if (!tracker.has(result.operation.opId))
                    throw new ProtocolFailure("LIVE_DELIVERY_TIMEOUT");
                  activeCommand.liveReceived = true;
                  lifecycle.deliveryObserved();
                  liveDeliveryDuration.add(Date.now() - activeCommand.sentAt, tags("catch_up"));
                  advanceAfterCurrentCommand();
                }
              } catch (error) {
                fail(error);
              }
            }, config.acknowledgementTimeoutMs);
          } else advanceAfterCurrentCommand();
          return;
        }
        if (packet.kind === "operation") {
          if (packet.value.boardId !== config.boardId)
            throw new ProtocolFailure("LIVE_OPERATION_CONTEXT");
          if (!joined) {
            preJoinDeliveries.add(packet.value);
            lifecycle.note("prejoin_delivery_buffered");
            return;
          }
          observeOperation(packet.value);
          return;
        }
        if (packet.kind === "revocation") {
          if (packet.value.boardId !== config.boardId)
            throw new ProtocolFailure("REVOCATION_CONTEXT");
          revoked = true;
          close(false);
          return;
        }
        if (packet.kind === "connect_error")
          throw new ProtocolFailure(`CONNECT_${packet.value.data.code}`);
        if (!complete) throw new ProtocolFailure("UNEXPECTED_DISCONNECT");
      } catch (error) {
        fail(error);
      }
    });
    socket.on("error", () => fail(new ProtocolFailure("WEBSOCKET_ERROR")));
    socket.on("close", () => {
      if (!complete) fail(new ProtocolFailure("WEBSOCKET_CLOSED"));
    });
    socket.setTimeout(() => {
      if (!connected && !failed) {
        classifyTimeout("connect");
        fail(new ProtocolFailure("CONNECT_TIMEOUT"));
      }
    }, config.connectTimeoutMs);
  });

  const postConnectReason = classifyPostConnectState(
    response?.status,
    currentPhase,
    lifecycle.snapshot(),
  );
  if (!failed && postConnectReason) {
    failed = true;
    failurePhase = currentPhase;
    failureReason = postConnectReason;
    protocolFailed = postConnectReason === "stale_lifecycle";
    lifecycle.fail();
    if (config.debugPhases || config.debugFailures)
      globalThis.console.log(
        `converge_failure vu=${globalThis.__VU} iteration=${globalThis.__ITER} phase=${failurePhase} reason=${failureReason}`,
      );
  }
  iterationFailures.add(
    failed,
    failed
      ? { ...tags("failed"), reason: failureReason ?? "unknown_internal_state" }
      : tags("completed"),
  );
  protocolFailures.add(protocolFailed, tags(protocolFailed ? "failed" : "valid"));
}
