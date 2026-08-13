import assert from "node:assert/strict";
import test from "node:test";
import { deterministicUuid } from "./identity.js";
import {
  ITERATION_FAILURE_REASONS,
  ITERATION_PHASES,
  classifyIterationFailure,
  createIterationLifecycle,
  createPreJoinDeliveryBuffer,
  matchesActiveCommand,
} from "./iteration-lifecycle.js";

function fixture(requiredCommands = 1) {
  const phases = [];
  const timers = [];
  const pending = new Map([[1, "join"]]);
  let cleanups = 0;
  const lifecycle = createIterationLifecycle({
    requiredCommands,
    onPhase: (phase) => phases.push(phase),
    cleanup: () => {
      cleanups += 1;
      pending.clear();
    },
  });
  const socket = {
    setTimeout(callback, delay) {
      timers.push({ callback, delay });
    },
  };
  return { lifecycle, socket, timers, phases, pending, cleanups: () => cleanups };
}

test("a successful join necessarily schedules a supported positive-delay command timer", () => {
  const value = fixture();
  value.lifecycle.joined();
  value.lifecycle.schedule(value.socket, () => value.lifecycle.commandSent(), 1);
  assert.equal(value.timers.length, 1);
  assert.equal(value.timers[0].delay, 1);
  value.timers[0].callback();
  assert.equal(value.lifecycle.snapshot().attempted, 1);
  assert.throws(() => value.lifecycle.schedule(value.socket, () => undefined, 0), /timer delay/);
  assert.throws(() => value.lifecycle.schedule({}, () => undefined, 1), /WebSocket timer boundary/);
});

test("normal completion requires a command attempt acknowledgement and delivery", () => {
  const value = fixture();
  assert.throws(() => value.lifecycle.complete(), /required evidence/);
  value.lifecycle.schedule(value.socket, () => value.lifecycle.commandSent(), 1);
  value.timers[0].callback();
  assert.throws(() => value.lifecycle.complete(), /required evidence/);
  value.lifecycle.commandAcknowledged();
  assert.throws(() => value.lifecycle.complete(), /required evidence/);
  value.lifecycle.deliveryObserved();
  value.lifecycle.complete();
  assert.equal(value.lifecycle.snapshot().terminal, true);
  assert.deepEqual(value.phases.slice(-3), ["command_acknowledged", "delivery_observed", "closed"]);
});

test("bounded missing acknowledgement and delivery failures clean pending work once", () => {
  for (const code of ["COMMAND_ACK_TIMEOUT", "LIVE_DELIVERY_TIMEOUT"]) {
    const value = fixture();
    value.lifecycle.fail(code);
    value.lifecycle.fail(code);
    assert.equal(value.cleanups(), 1);
    assert.equal(value.pending.size, 0);
    assert.equal(value.phases.at(-1), "failed");
  }
});

test("terminal timers cannot leak a command attempt", () => {
  const value = fixture();
  value.lifecycle.schedule(value.socket, () => value.lifecycle.commandSent(), 1);
  value.lifecycle.fail();
  value.timers[0].callback();
  assert.equal(value.lifecycle.snapshot().attempted, 0);
});

test("distinct VUs and iterations own unique retry-stable command and object identities", () => {
  const identities = new Set();
  for (const vu of [1, 2])
    for (const iteration of [0, 1, 2, 3])
      for (const kind of ["client", "object", "operation"]) {
        const first = deterministicUuid(kind, vu, iteration, kind === "client" ? 0 : 1);
        const retry = deterministicUuid(kind, vu, iteration, kind === "client" ? 0 : 1);
        assert.equal(first, retry);
        assert.match(first, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-8[0-9a-f]{3}-[0-9a-f]{12}$/);
        assert.equal(identities.has(first), false);
        identities.add(first);
      }
});

test("each iteration receives fresh mutable lifecycle state", () => {
  const previous = fixture();
  previous.lifecycle.schedule(previous.socket, () => previous.lifecycle.commandSent(), 1);
  previous.timers[0].callback();
  previous.lifecycle.fail();
  const next = fixture();
  assert.deepEqual(next.lifecycle.snapshot(), {
    terminal: false,
    scheduled: 0,
    attempted: 0,
    acknowledged: 0,
    delivered: 0,
  });
});

test("reconciles bounded pre-join delivery once without leaking into another iteration", () => {
  const buffer = createPreJoinDeliveryBuffer(2);
  const observed = [];
  buffer.add({ seq: 1 });
  buffer.add({ seq: 2 });
  buffer.drain((operation) => observed.push(operation.seq));
  buffer.drain(() => assert.fail("drained evidence twice"));
  assert.deepEqual(observed, [1, 2]);
  assert.equal(buffer.size, 0);
  buffer.add({ seq: 3 });
  buffer.add({ seq: 4 });
  assert.throws(() => buffer.add({ seq: 5 }), { code: "LIVE_OPERATION_BUFFER_OVERFLOW" });
  buffer.clear();
  assert.equal(buffer.size, 0);
});

test("an unrelated VU delivery cannot complete the current command", () => {
  assert.equal(matchesActiveCommand("current-operation", "other-operation"), false);
  assert.equal(matchesActiveCommand("current-operation", "current-operation"), true);
  assert.equal(matchesActiveCommand(undefined, "current-operation"), false);
});

test("classifies only fixed bounded sanitized failure reasons", () => {
  assert.equal(classifyIterationFailure({ code: "JOIN_ACK_TIMEOUT" }), "join_timeout");
  assert.equal(classifyIterationFailure({ code: "COMMAND_CONFLICT" }), "command_rejected");
  assert.equal(classifyIterationFailure({ code: "LIVE_OPERATION_CONTEXT" }), "delivery_mismatch");
  assert.equal(classifyIterationFailure({ code: "SEQUENCE_GAP" }), "sequence_conflict");
  assert.equal(classifyIterationFailure({ code: "WEBSOCKET_CLOSED" }), "unexpected_close");
  assert.equal(
    classifyIterationFailure(new Error("opaque"), { phase: "join_ack_received" }),
    "catchup_unknown",
  );
  assert.equal(
    classifyIterationFailure({ code: "CATCHUP_NONCONTIGUOUS" }),
    "catchup_noncontiguous",
  );
  assert.equal(
    classifyIterationFailure({ code: "SNAPSHOT_DUPLICATE_OBJECT" }, { phase: "authenticated" }),
    "snapshot_duplicate_object",
  );
  assert.equal(
    classifyIterationFailure(new Error("opaque"), { phase: "snapshot_body_parsed" }),
    "snapshot_unknown",
  );
  assert.equal(
    classifyIterationFailure(new Error("opaque"), { phase: "snapshot_applied" }),
    "join_not_emitted",
  );
  assert.equal(
    classifyIterationFailure(new Error("opaque"), { phase: "catchup_applied" }),
    "prejoin_reconcile_failed",
  );
  assert.equal(
    classifyIterationFailure(new Error("opaque"), { phase: "command_scheduled" }),
    "active_command_missing",
  );
  assert.equal(ITERATION_FAILURE_REASONS.includes("internal_lifecycle"), false);
  assert.equal(
    ITERATION_FAILURE_REASONS.every((reason) => /^[a-z_]+$/.test(reason)),
    true,
  );
});

test("debug phases are fixed sanitized low-cardinality values", () => {
  assert.deepEqual(ITERATION_PHASES, [
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
  assert.equal(
    ITERATION_PHASES.every((phase) => /^[a-z_]+$/.test(phase)),
    true,
  );
});
