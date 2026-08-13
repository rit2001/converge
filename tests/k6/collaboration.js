import http from "k6/http";
import ws from "k6/ws";
import { Counter, Rate, Trend } from "k6/metrics";
import { parseWorkloadConfig, workloadOptions } from "./config.js";
import {
  ProtocolFailure,
  classifyTimeout,
  createDeliveryTracker,
  encodeConnect,
  encodeEvent,
  parseCommandAcknowledgement,
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

function tags(outcome, operationType = "object.create") {
  return { profile: config.profile, operation_type: operationType, outcome };
}

function deterministicUuid(kind, ordinal) {
  const vu = Number(globalThis.__VU ?? 0);
  const iteration = Number(globalThis.__ITER ?? 0);
  const value = vu * 1_000_000 + iteration * 1_000 + ordinal;
  const tail = value.toString(16).padStart(12, "0").slice(-12);
  const prefix = kind === "client" ? "10000000" : kind === "object" ? "20000000" : "30000000";
  return `${prefix}-0000-4000-8000-${tail}`;
}

function socketEndpoint(origin) {
  const parsed = new globalThis.URL(origin);
  parsed.protocol = parsed.protocol === "https:" || parsed.protocol === "wss:" ? "wss:" : "ws:";
  parsed.pathname = "/socket.io/";
  parsed.search = "EIO=4&transport=websocket";
  return parsed.toString();
}

function command(clientId, ordinal, baseSequence) {
  const targetId = deterministicUuid("object", ordinal);
  return {
    schemaVersion: 1,
    opId: deterministicUuid("operation", ordinal),
    boardId: config.boardId,
    clientId,
    baseSeq: baseSequence,
    targetId,
    clientTimestamp: new Date().toISOString(),
    type: "object.create",
    payload: {
      id: targetId,
      kind: "rectangle",
      x: 40 + (ordinal % 20),
      y: 40 + (ordinal % 20),
      width: 160,
      height: 100,
      rotation: 0,
      fill: "#818cf8",
      text: "",
    },
  };
}

export default function collaborationWorkload() {
  const startedAt = Date.now();
  const clientId = deterministicUuid("client", 0);
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
  const pendingAcknowledgements = new Map();

  const response = ws.connect(socketEndpoint(config.socketUrl), {}, (socket) => {
    const close = () => {
      if (complete) return;
      complete = true;
      if (connected) socket.send("41");
      socket.close();
    };

    const fail = (error) => {
      if (failed) return;
      failed = true;
      protocolFailed ||= error instanceof ProtocolFailure;
      if (error instanceof ProtocolFailure && /SEQUENCE_(?:GAP|REGRESSION)/.test(error.code))
        sequenceGaps.add(1, tags("detected"));
      close();
    };

    const catchUp = (watermark) => {
      let after = tracker.snapshot().lastSequence;
      for (let page = 0; after < watermark && page < 10; page += 1) {
        const result = http.get(
          `${config.baseUrl}/v1/boards/${config.boardId}/operations?after=${after}&watermark=${watermark}`,
          { headers: { Authorization: `Bearer ${config.authToken}` }, tags: {} },
        );
        if (result.status !== 200) throw new ProtocolFailure("CATCH_UP_HTTP");
        let body;
        try {
          body = result.json();
        } catch {
          throw new ProtocolFailure("CATCH_UP_JSON");
        }
        const range = parseOperationRange(body, { boardId: config.boardId, after, watermark });
        for (const operation of range.operations) tracker.observe(operation);
        if (range.nextSeq === after) throw new ProtocolFailure("CATCH_UP_NO_PROGRESS");
        after = range.nextSeq;
      }
      if (after !== watermark) throw new ProtocolFailure("CATCH_UP_INCOMPLETE");
    };

    const scheduleNextCommand = () => {
      if (failed || revoked) return close();
      if (commandOrdinal >= config.commandsPerClient) return close();
      socket.setTimeout(
        () => {
          if (failed || complete) return;
          commandOrdinal += 1;
          const value = command(clientId, commandOrdinal, tracker.snapshot().lastSequence);
          const id = ++acknowledgementId;
          activeCommand = {
            value,
            sentAt: Date.now(),
            liveReceived: false,
            acknowledged: false,
          };
          pendingAcknowledgements.set(id, { kind: "command", startedAt: Date.now(), value });
          socket.send(encodeEvent("operation:submit", value, id));
          socket.setTimeout(() => {
            if (!activeCommand?.acknowledged && !failed) {
              classifyTimeout("command_ack");
              fail(new ProtocolFailure("COMMAND_ACK_TIMEOUT"));
            }
          }, config.acknowledgementTimeoutMs);
        },
        commandOrdinal === 0 ? 0 : config.commandIntervalMs,
      );
    };

    const confirmCommandDelivery = (operation) => {
      if (!activeCommand || operation.opId !== activeCommand.value.opId) return;
      activeCommand.liveReceived = true;
      liveDeliveryDuration.add(Date.now() - activeCommand.sentAt, tags("live"));
      if (activeCommand.acknowledged) scheduleNextCommand();
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
          socketConnectDuration.add(Date.now() - startedAt, tags("connected"));
          const id = ++acknowledgementId;
          pendingAcknowledgements.set(id, { kind: "join", startedAt: Date.now() });
          socket.send(
            encodeEvent(
              "board:join",
              { schemaVersion: 1, boardId: config.boardId, clientId, lastAppliedSeq: 0 },
              id,
            ),
          );
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
            const result = parseJoinAcknowledgement(packet.value, config.boardId);
            if (result.kind === "error") throw new ProtocolFailure(`JOIN_${result.error.code}`);
            joined = true;
            tracker = createDeliveryTracker(0, 256);
            boardJoinDuration.add(Date.now() - pending.startedAt, tags("joined"));
            catchUp(result.value.joinWatermark);
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
          activeCommand.acknowledged = true;
          if (!activeCommand.liveReceived) {
            socket.setTimeout(() => {
              try {
                if (!activeCommand?.liveReceived && !failed) {
                  classifyTimeout("live_delivery");
                  catchUp(result.operation.seq);
                  if (!tracker.has(result.operation.opId))
                    throw new ProtocolFailure("CATCH_UP_OPERATION_MISSING");
                  activeCommand.liveReceived = true;
                  liveDeliveryDuration.add(Date.now() - activeCommand.sentAt, tags("catch_up"));
                  scheduleNextCommand();
                }
              } catch (error) {
                fail(error);
              }
            }, config.acknowledgementTimeoutMs);
          } else scheduleNextCommand();
          return;
        }
        if (packet.kind === "operation") {
          if (!joined || packet.value.boardId !== config.boardId)
            throw new ProtocolFailure("LIVE_OPERATION_CONTEXT");
          const outcome = tracker.observe(packet.value);
          if (outcome === "duplicate") duplicateEvents.add(1, tags("duplicate", packet.value.type));
          else liveEventsReceived.add(1, tags("received", packet.value.type));
          confirmCommandDelivery(packet.value);
          return;
        }
        if (packet.kind === "revocation") {
          if (packet.value.boardId !== config.boardId)
            throw new ProtocolFailure("REVOCATION_CONTEXT");
          revoked = true;
          close();
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

  if (!response || response.status !== 101) failed = true;
  iterationFailures.add(failed, tags(failed ? "failed" : "completed"));
  protocolFailures.add(protocolFailed, tags(protocolFailed ? "failed" : "valid"));
}
