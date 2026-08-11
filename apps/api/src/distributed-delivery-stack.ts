import type { BuildAppOptions } from "./app.js";
import {
  BoardDeliveryHeadWatchdog,
  type BoardDeliveryHeadWatchdogConfiguration,
  type BoardDeliveryHeadWatchdogFactoryInput,
  type BoardDeliveryHeadWatchdogOwner,
} from "./board-delivery-head-watchdog.js";
import {
  RedisDeliveryConsumer,
  defaultDeliveryConsumerConfiguration,
  type DeliveryConsumerCallbacks,
  type DeliveryConsumerConfiguration,
  type DeliveryConsumerTransport,
} from "./delivery-consumer.js";
import {
  ApiDeliveryRuntime,
  type ApiDeliveryRuntimeOptions,
  type DeliveryConsumerInstance,
  type DeliveryRuntimeOwner,
} from "./delivery-runtime.js";
import type { Environment } from "./env.js";
import { RedisDeliveryConsumerTransport } from "./redis-delivery-transport.js";

export interface DistributedDeliveryStackFactories {
  createTransport(redisUrl: string, streamKey: string): DeliveryConsumerTransport;
  createConsumer(
    transport: DeliveryConsumerTransport,
    callbacks: DeliveryConsumerCallbacks,
    configuration: DeliveryConsumerConfiguration,
  ): DeliveryConsumerInstance;
  createRuntime(options: ApiDeliveryRuntimeOptions): DeliveryRuntimeOwner;
  createWatchdog(
    input: BoardDeliveryHeadWatchdogFactoryInput,
    configuration: BoardDeliveryHeadWatchdogConfiguration,
  ): BoardDeliveryHeadWatchdogOwner;
}

export const productionDistributedDeliveryStackFactories: DistributedDeliveryStackFactories = {
  createTransport: (redisUrl, streamKey) => new RedisDeliveryConsumerTransport(redisUrl, streamKey),
  createConsumer: (transport, callbacks, configuration) =>
    new RedisDeliveryConsumer(transport, callbacks, configuration),
  createRuntime: (options) => new ApiDeliveryRuntime(options),
  createWatchdog: (input, configuration) =>
    new BoardDeliveryHeadWatchdog(
      input.repository,
      input.activeBoards,
      input.deliveryProgress,
      input.observer,
      configuration,
    ),
};

class ConfiguredDeliveryConsumer implements DeliveryConsumerInstance {
  private transport: DeliveryConsumerTransport | undefined;
  private consumer: DeliveryConsumerInstance | undefined;
  private startPromise: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;

  constructor(
    private readonly environment: Environment,
    private readonly callbacks: DeliveryConsumerCallbacks,
    private readonly configuration: DeliveryConsumerConfiguration,
    private readonly factories: DistributedDeliveryStackFactories,
  ) {}

  start(): Promise<void> {
    this.startPromise ??= this.startOnce();
    return this.startPromise;
  }

  stop(): Promise<void> {
    this.stopPromise ??= this.stopOnce();
    return this.stopPromise;
  }

  private async startOnce(): Promise<void> {
    if (this.stopPromise) throw new Error("Delivery consumer cannot start after shutdown");
    const transport = this.factories.createTransport(
      this.environment.REDIS_URL,
      this.environment.REDIS_STREAM_KEY,
    );
    this.transport = transport;
    try {
      const consumer = this.factories.createConsumer(transport, this.callbacks, this.configuration);
      this.consumer = consumer;
      await consumer.start();
    } catch (error) {
      await this.stop().catch(() => undefined);
      throw error;
    }
  }

  private async stopOnce(): Promise<void> {
    if (this.consumer) {
      await this.consumer.stop();
      return;
    }
    await this.transport?.close();
  }
}

function consumerConfiguration(environment: Environment): DeliveryConsumerConfiguration {
  return {
    ...defaultDeliveryConsumerConfiguration,
    globalQueueMaximumEvents: environment.REDIS_API_QUEUE_MAX_EVENTS,
    globalQueueMaximumBytes: environment.REDIS_API_QUEUE_MAX_BYTES,
    boardQuarantineMaximumEvents: environment.DELIVERY_BOARD_BUFFER_MAX_EVENTS,
    boardQuarantineMaximumBytes: environment.DELIVERY_BOARD_BUFFER_MAX_BYTES,
    boardDedupeMaximumEvents: environment.DELIVERY_DEDUPE_WINDOW_EVENTS,
    maximumBoardStates: environment.REDIS_DELIVERY_MAX_BOARD_STATES,
    reconnectDelayMs: environment.REDIS_DELIVERY_RECONNECT_DELAY_MS,
  };
}

function watchdogConfiguration(environment: Environment): BoardDeliveryHeadWatchdogConfiguration {
  return {
    intervalMs: environment.DELIVERY_WATCHDOG_INTERVAL_MS,
    gracePeriodMs: environment.DELIVERY_WATCHDOG_GRACE_MS,
    queryTimeoutMs: environment.DELIVERY_WATCHDOG_QUERY_TIMEOUT_MS,
    batchSize: environment.DELIVERY_WATCHDOG_BATCH_SIZE,
    maximumActiveBoards: environment.REDIS_DELIVERY_MAX_BOARD_STATES,
    jitterRatio: environment.DELIVERY_WATCHDOG_JITTER_RATIO,
  };
}

export function configuredDeliveryBuildOptions(
  environment: Environment,
  factories: DistributedDeliveryStackFactories = productionDistributedDeliveryStackFactories,
): Pick<BuildAppOptions, "deliveryMode" | "createBoardDeliveryHeadWatchdog"> {
  if (environment.API_DELIVERY_MODE === "local") return { deliveryMode: { mode: "local" } };

  const deliveryConsumerConfiguration = consumerConfiguration(environment);
  const deliveryWatchdogConfiguration = watchdogConfiguration(environment);
  return {
    deliveryMode: {
      mode: "distributed",
      createRuntime: (handlers, observer) =>
        factories.createRuntime({
          handlers,
          observer,
          createConsumer: (callbacks) =>
            new ConfiguredDeliveryConsumer(
              environment,
              callbacks,
              deliveryConsumerConfiguration,
              factories,
            ),
        }),
    },
    createBoardDeliveryHeadWatchdog: (input) =>
      factories.createWatchdog(input, deliveryWatchdogConfiguration),
  };
}
