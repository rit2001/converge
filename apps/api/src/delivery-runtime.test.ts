import { describe, expect, it, vi } from "vitest";
import {
  membershipRevokedDeliveryEnvelopeSchema,
  operationCommittedDeliveryEnvelopeSchema,
  type DeliveryEnvelope,
} from "@converge/protocol";
import {
  ApiDeliveryRuntime,
  type DeliveryConsumerFactory,
  type DeliveryRuntimeEventHandlers,
  type DeliveryRuntimeObserver,
} from "./delivery-runtime.js";
import type { BoardQuarantineEvent, DeliveryConsumerCallbacks } from "./delivery-consumer.js";

const ids = {
  board: "10000000-0000-4000-8000-000000000001",
  client: "20000000-0000-4000-8000-000000000001",
  object: "30000000-0000-4000-8000-000000000001",
  operation: "40000000-0000-4000-8000-000000000001",
  operationEvent: "50000000-0000-4000-8000-000000000001",
  membershipEvent: "50000000-0000-4000-8000-000000000002",
  revoked: "60000000-0000-4000-8000-000000000001",
  actor: "70000000-0000-4000-8000-000000000001",
} as const;

const operationEnvelope = operationCommittedDeliveryEnvelopeSchema.parse({
  schemaVersion: 1,
  eventId: ids.operationEvent,
  boardId: ids.board,
  deliverySeq: 1,
  eventType: "operation.committed",
  occurredAt: "2026-08-11T10:00:01.000Z",
  payload: {
    operation: {
      schemaVersion: 1,
      opId: ids.operation,
      boardId: ids.board,
      clientId: ids.client,
      baseSeq: 0,
      targetId: ids.object,
      clientTimestamp: "2026-08-11T10:00:00.000Z",
      type: "object.create",
      payload: {
        id: ids.object,
        kind: "rectangle",
        x: 0,
        y: 0,
        width: 100,
        height: 80,
        rotation: 0,
        fill: "#818cf8",
        text: "",
      },
      seq: 1,
      committedAt: "2026-08-11T10:00:01.000Z",
    },
  },
});

const membershipEnvelope = membershipRevokedDeliveryEnvelopeSchema.parse({
  schemaVersion: 1,
  eventId: ids.membershipEvent,
  boardId: ids.board,
  deliverySeq: 2,
  eventType: "board.membership.revoked",
  occurredAt: "2026-08-11T10:00:02.000Z",
  payload: { revokedUserId: ids.revoked, initiatedByUserId: ids.actor },
});

const quarantine: BoardQuarantineEvent = {
  redisEntryId: "12-0",
  boardId: ids.board,
  eventId: ids.operationEvent,
  deliverySeq: 1,
  reason: "DELIVERY_SEQUENCE_GAP",
  retainedEvents: 1,
  retainedBytes: 128,
  overflowed: false,
};

function harness(options: { startError?: Error } = {}) {
  let callbacks: DeliveryConsumerCallbacks | undefined;
  const startConsumer = vi.fn(() =>
    options.startError === undefined ? Promise.resolve() : Promise.reject(options.startError),
  );
  const stopConsumer = vi.fn(() => Promise.resolve());
  const consumer = {
    start: startConsumer,
    stop: stopConsumer,
  };
  const createConsumer: DeliveryConsumerFactory = vi.fn(
    (createdCallbacks: DeliveryConsumerCallbacks) => {
      callbacks = createdCallbacks;
      return consumer;
    },
  );
  const operationCommitted = vi.fn(() => Promise.resolve());
  const membershipRevoked = vi.fn(() => Promise.resolve());
  const handlers: DeliveryRuntimeEventHandlers = {
    operationCommitted,
    membershipRevoked,
  };
  const observeLifecycle = vi.fn();
  const observeQuarantine = vi.fn(() => Promise.resolve());
  const observer: DeliveryRuntimeObserver = {
    lifecycle: observeLifecycle,
    quarantine: observeQuarantine,
  };
  const runtime = new ApiDeliveryRuntime({ createConsumer, handlers, observer });
  return {
    runtime,
    startConsumer,
    stopConsumer,
    createConsumer,
    operationCommitted,
    membershipRevoked,
    observeLifecycle,
    observeQuarantine,
    get callbacks(): DeliveryConsumerCallbacks {
      if (!callbacks) throw new Error("Expected the consumer callbacks to be constructed");
      return callbacks;
    },
  };
}

describe("API delivery runtime", () => {
  it("requires both delivery handlers and both observers", () => {
    const createConsumer = vi.fn();
    const observer: DeliveryRuntimeObserver = {
      lifecycle: vi.fn(),
      quarantine: vi.fn(() => Promise.resolve()),
    };
    const incompleteHandlers = {
      operationCommitted: vi.fn(() => Promise.resolve()),
    } as unknown as DeliveryRuntimeEventHandlers;

    expect(
      () => new ApiDeliveryRuntime({ createConsumer, handlers: incompleteHandlers, observer }),
    ).toThrow("board.membership.revoked handler");
    expect(
      () =>
        new ApiDeliveryRuntime({
          createConsumer,
          handlers: {
            operationCommitted: vi.fn(() => Promise.resolve()),
            membershipRevoked: vi.fn(() => Promise.resolve()),
          },
          observer: { lifecycle: vi.fn() } as unknown as DeliveryRuntimeObserver,
        }),
    ).toThrow("quarantine observer");
  });

  it("constructs and starts one consumer, then routes every envelope explicitly", async () => {
    const state = harness();
    expect(state.createConsumer).not.toHaveBeenCalled();

    const firstStart = state.runtime.start();
    const secondStart = state.runtime.start();
    expect(secondStart).toBe(firstStart);
    await Promise.all([firstStart, secondStart]);
    expect(state.createConsumer).toHaveBeenCalledOnce();
    expect(state.startConsumer).toHaveBeenCalledOnce();

    await state.callbacks.deliver({ redisEntryId: "10-0", envelope: operationEnvelope });
    await state.callbacks.deliver({ redisEntryId: "11-0", envelope: membershipEnvelope });
    expect(state.operationCommitted).toHaveBeenCalledWith(operationEnvelope);
    expect(state.membershipRevoked).toHaveBeenCalledWith(membershipEnvelope);
  });

  it("forwards quarantine and maps availability evidence into the runtime contract", async () => {
    const state = harness();
    await state.runtime.start();

    await state.callbacks.quarantine(quarantine);
    await state.callbacks.lifecycle({ state: "starting" });
    await state.callbacks.lifecycle({ state: "established", cursor: "10-0", initialTail: "10-0" });
    await state.callbacks.lifecycle({
      state: "unavailable",
      cursor: "10-0",
      code: "REDIS_UNAVAILABLE",
    });
    await state.callbacks.lifecycle({ state: "recovering", cursor: "10-0" });
    await state.callbacks.lifecycle({ state: "recovered", cursor: "11-0", recoveryTail: "11-0" });
    await state.callbacks.lifecycle({
      state: "cursor_lost",
      cursor: "11-0",
      reason: "STREAM_BEHIND_CURSOR",
    });
    await state.callbacks.lifecycle({
      state: "error",
      cursor: "11-0",
      code: "INVALID_STREAM_ENTRY",
    });
    await state.callbacks.lifecycle({ state: "stopping", cursor: "11-0" });

    expect(state.observeQuarantine).toHaveBeenCalledWith(quarantine);
    expect(state.observeLifecycle).toHaveBeenCalledTimes(6);
    expect(state.observeLifecycle).toHaveBeenNthCalledWith(1, { state: "established" });
    expect(state.observeLifecycle).toHaveBeenNthCalledWith(2, {
      state: "unavailable",
      code: "REDIS_UNAVAILABLE",
    });
    expect(state.observeLifecycle).toHaveBeenNthCalledWith(3, { state: "recovering" });
    expect(state.observeLifecycle).toHaveBeenNthCalledWith(4, { state: "recovered" });
    expect(state.observeLifecycle).toHaveBeenNthCalledWith(5, {
      state: "terminal",
      source: "cursor",
      code: "STREAM_BEHIND_CURSOR",
    });
    expect(state.observeLifecycle).toHaveBeenNthCalledWith(6, {
      state: "terminal",
      source: "consumer",
      code: "INVALID_STREAM_ENTRY",
    });
  });

  it("stops its consumer exactly once and fences every late callback", async () => {
    const state = harness();
    await state.runtime.start();
    const callbacks = state.callbacks;

    const firstStop = state.runtime.stop();
    const secondStop = state.runtime.stop();
    expect(secondStop).toBe(firstStop);
    await Promise.all([firstStop, secondStop]);
    expect(state.stopConsumer).toHaveBeenCalledOnce();
    expect(state.observeLifecycle).toHaveBeenCalledOnce();
    expect(state.observeLifecycle).toHaveBeenCalledWith({ state: "stopped" });

    await callbacks.deliver({ redisEntryId: "20-0", envelope: operationEnvelope });
    await callbacks.quarantine(quarantine);
    await callbacks.lifecycle({
      state: "unavailable",
      cursor: "20-0",
      code: "REDIS_UNAVAILABLE",
    });
    await callbacks.lifecycle({ state: "stopped", cursor: "20-0" });
    expect(state.operationCommitted).not.toHaveBeenCalled();
    expect(state.membershipRevoked).not.toHaveBeenCalled();
    expect(state.observeQuarantine).not.toHaveBeenCalled();
    expect(state.observeLifecycle).toHaveBeenCalledOnce();
    await expect(state.runtime.start()).rejects.toThrow("cannot start after shutdown");
  });

  it("cleans up one failed startup and reports stopped once", async () => {
    const startError = new Error("consumer startup failed");
    const state = harness({ startError });

    await expect(state.runtime.start()).rejects.toBe(startError);
    expect(state.startConsumer).toHaveBeenCalledOnce();
    expect(state.stopConsumer).toHaveBeenCalledOnce();
    expect(state.observeLifecycle).toHaveBeenCalledOnce();
    expect(state.observeLifecycle).toHaveBeenCalledWith({ state: "stopped" });
    await state.runtime.stop();
    expect(state.stopConsumer).toHaveBeenCalledOnce();
    expect(state.observeLifecycle).toHaveBeenCalledOnce();
  });

  it("rejects an invalid consumed envelope before invoking an event handler", async () => {
    const state = harness();
    await state.runtime.start();
    const invalidEnvelope = {
      ...operationEnvelope,
      eventType: "operation.unknown",
    } as unknown as DeliveryEnvelope;

    await expect(
      state.callbacks.deliver({ redisEntryId: "30-0", envelope: invalidEnvelope }),
    ).rejects.toThrow();
    expect(state.operationCommitted).not.toHaveBeenCalled();
    expect(state.membershipRevoked).not.toHaveBeenCalled();
  });
});
