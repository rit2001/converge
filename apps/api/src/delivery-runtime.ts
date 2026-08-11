import {
  deliveryEnvelopeSchema,
  type MembershipRevokedDeliveryEnvelope,
  type OperationCommittedDeliveryEnvelope,
} from "@converge/protocol";
import type {
  BoardQuarantineEvent,
  CursorLossReason,
  DeliveryConsumerCallbacks,
  DeliveryConsumerErrorCode,
  DeliveryConsumerLifecycleEvent,
} from "./delivery-consumer.js";

export interface DeliveryConsumerInstance {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export type DeliveryConsumerFactory = (
  callbacks: DeliveryConsumerCallbacks,
) => DeliveryConsumerInstance;

export interface DeliveryRuntimeEventHandlers {
  operationCommitted(envelope: OperationCommittedDeliveryEnvelope): Promise<void>;
  membershipRevoked(envelope: MembershipRevokedDeliveryEnvelope): Promise<void>;
}

export type DeliveryRuntimeLifecycleEvent =
  | { state: "established" }
  | {
      state: "unavailable";
      code: "REDIS_UNAVAILABLE" | "BOARD_STATE_CAPACITY_EXCEEDED";
    }
  | { state: "recovering" }
  | { state: "recovered" }
  | {
      state: "terminal";
      source: "consumer";
      code: DeliveryConsumerErrorCode;
    }
  | {
      state: "terminal";
      source: "cursor";
      code: CursorLossReason;
    }
  | { state: "stopped" };

export interface DeliveryRuntimeObserver {
  lifecycle(event: DeliveryRuntimeLifecycleEvent): Promise<void> | void;
  quarantine(event: BoardQuarantineEvent): Promise<void>;
}

export interface DeliveryRuntimeOwner {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export type DeliveryRuntimeFactory = (
  handlers: DeliveryRuntimeEventHandlers,
  observer: DeliveryRuntimeObserver,
) => DeliveryRuntimeOwner;

export interface ApiDeliveryRuntimeOptions {
  createConsumer: DeliveryConsumerFactory;
  handlers: DeliveryRuntimeEventHandlers;
  observer: DeliveryRuntimeObserver;
}

function assertFunction(
  value: unknown,
  name: string,
): asserts value is (...args: never[]) => unknown {
  if (typeof value !== "function") throw new TypeError(`Delivery runtime requires ${name}`);
}

function property(value: unknown, name: string): unknown {
  if (typeof value !== "object" || value === null) return undefined;
  return (value as Record<string, unknown>)[name];
}

function assertHandlers(handlers: DeliveryRuntimeEventHandlers): void {
  assertFunction(property(handlers, "operationCommitted"), "an operation.committed handler");
  assertFunction(property(handlers, "membershipRevoked"), "a board.membership.revoked handler");
}

function assertObserver(observer: DeliveryRuntimeObserver): void {
  assertFunction(property(observer, "lifecycle"), "a lifecycle observer");
  assertFunction(property(observer, "quarantine"), "a quarantine observer");
}

function unreachable(value: never): never {
  throw new Error(`Unhandled delivery event type: ${String(value)}`);
}

/**
 * Owns the one delivery consumer for an API application instance. Consumer construction is delayed
 * until start so application construction cannot allocate Redis clients.
 */
export class ApiDeliveryRuntime implements DeliveryRuntimeOwner {
  private consumer: DeliveryConsumerInstance | undefined;
  private consumerStopPromise: Promise<void> | undefined;
  private startPromise: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private generation = 0;
  private acceptsDeliveries = false;
  private acceptsLifecycle = false;
  private stoppedObserved = false;

  constructor(private readonly options: ApiDeliveryRuntimeOptions) {
    assertFunction(options?.createConsumer, "a consumer factory");
    assertHandlers(options.handlers);
    assertObserver(options.observer);
  }

  start(): Promise<void> {
    if (this.stopPromise)
      return Promise.reject(new Error("Delivery runtime cannot start after shutdown"));
    this.startPromise ??= this.startOnce();
    return this.startPromise;
  }

  stop(): Promise<void> {
    this.acceptsDeliveries = false;
    this.stopPromise ??= this.stopOnce();
    return this.stopPromise;
  }

  private async startOnce(): Promise<void> {
    const generation = ++this.generation;
    this.acceptsDeliveries = true;
    this.acceptsLifecycle = true;
    const callbacks: DeliveryConsumerCallbacks = {
      deliver: (context) => {
        if (!this.isDeliveryOwner(generation)) return Promise.resolve();
        return this.route(context.envelope);
      },
      quarantine: (event) => {
        if (!this.isDeliveryOwner(generation)) return Promise.resolve();
        return this.options.observer.quarantine(event);
      },
      lifecycle: (event) => {
        if (!this.isLifecycleOwner(generation)) return;
        return this.observeLifecycle(event);
      },
    };
    try {
      const consumer = this.options.createConsumer(callbacks);
      this.consumer = consumer;
      await consumer.start();
    } catch (error) {
      this.acceptsDeliveries = false;
      await this.stopConsumerOnce().catch(() => undefined);
      this.acceptsLifecycle = false;
      await this.observeStoppedOnce();
      throw error;
    }
  }

  private async stopOnce(): Promise<void> {
    let stopError: unknown;
    try {
      await this.stopConsumerOnce();
      await this.startPromise?.catch(() => undefined);
    } catch (error) {
      stopError = error;
    } finally {
      this.acceptsLifecycle = false;
      this.generation += 1;
      await this.observeStoppedOnce();
    }
    if (stopError !== undefined)
      throw stopError instanceof Error
        ? stopError
        : new Error("Delivery consumer shutdown failed", { cause: stopError });
  }

  private stopConsumerOnce(): Promise<void> {
    if (!this.consumer) return Promise.resolve();
    this.consumerStopPromise ??= this.consumer.stop();
    return this.consumerStopPromise;
  }

  private isDeliveryOwner(generation: number): boolean {
    return this.acceptsDeliveries && this.generation === generation;
  }

  private isLifecycleOwner(generation: number): boolean {
    return this.acceptsLifecycle && this.generation === generation;
  }

  private async route(value: unknown): Promise<void> {
    const envelope = deliveryEnvelopeSchema.parse(value);
    switch (envelope.eventType) {
      case "operation.committed":
        await this.options.handlers.operationCommitted(envelope);
        return;
      case "board.membership.revoked":
        await this.options.handlers.membershipRevoked(envelope);
        return;
      default:
        return unreachable(envelope);
    }
  }

  private observeLifecycle(event: DeliveryConsumerLifecycleEvent): Promise<void> | void {
    switch (event.state) {
      case "starting":
      case "stopping":
        return;
      case "established":
        return this.options.observer.lifecycle({ state: "established" });
      case "unavailable":
        return this.options.observer.lifecycle({ state: "unavailable", code: event.code });
      case "recovering":
        return this.options.observer.lifecycle({ state: "recovering" });
      case "recovered":
        return this.options.observer.lifecycle({ state: "recovered" });
      case "cursor_lost":
        return this.options.observer.lifecycle({
          state: "terminal",
          source: "cursor",
          code: event.reason,
        });
      case "error":
        return this.options.observer.lifecycle({
          state: "terminal",
          source: "consumer",
          code: event.code,
        });
      case "stopped":
        return this.observeStoppedOnce();
      default:
        return unreachable(event);
    }
  }

  private async observeStoppedOnce(): Promise<void> {
    if (this.stoppedObserved) return;
    this.stoppedObserved = true;
    await this.options.observer.lifecycle({ state: "stopped" });
  }
}
