import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import type { DatabasePool } from "@converge/database";
import type { AuthAdapter } from "./auth.js";
import { buildApp, type AppContext, type BuildAppOptions } from "./app.js";
import type { BoardDeliveryHeadWatchdogOwner } from "./board-delivery-head-watchdog.js";
import {
  configuredDeliveryBuildOptions,
  type DistributedDeliveryStackFactories,
} from "./distributed-delivery-stack.js";
import type { DeliveryConsumerCallbacks, DeliveryConsumerTransport } from "./delivery-consumer.js";
import { ApiDeliveryRuntime, type ApiDeliveryRuntimeOptions } from "./delivery-runtime.js";
import { apiEnvironmentVariableNames, parseEnvironment } from "./env.js";
import {
  ApiServerStartupError,
  createApiServer,
  type ApiServerDependencies,
} from "./server-runtime.js";

const databaseUrl = "postgresql://converge:local-password@localhost:55432/converge";
const localEnvironment = parseEnvironment({
  NODE_ENV: "test",
  DATABASE_URL: databaseUrl,
  LOG_LEVEL: "silent",
});
const distributedEnvironment = parseEnvironment({
  NODE_ENV: "test",
  DATABASE_URL: databaseUrl,
  API_DELIVERY_MODE: "distributed",
  REDIS_URL: "redis://delivery-user:private-password@redis.example.test:6379",
  REDIS_STREAM_KEY: "converge:delivery:test:v1",
  LOG_LEVEL: "silent",
});

const authentication: AuthAdapter = {
  authenticateHttp: vi.fn(() => Promise.resolve(null)),
  authenticateSocket: vi.fn(() => Promise.resolve(null)),
};
const unusedPool = {} as DatabasePool;

function stackHarness(options: { consumerFactoryError?: Error } = {}) {
  const closeTransport = vi.fn(() => Promise.resolve());
  const transport = {
    close: closeTransport,
  } as unknown as DeliveryConsumerTransport;
  let consumerCallbacks: DeliveryConsumerCallbacks | undefined;
  const startConsumer = vi.fn(async () => {
    await consumerCallbacks?.lifecycle({
      state: "established",
      cursor: "0-0",
      initialTail: "0-0",
    });
  });
  const stopConsumer = vi.fn(async () => closeTransport());
  const startWatchdog = vi.fn(() => Promise.resolve());
  const stopWatchdog = vi.fn(() => Promise.resolve());
  const watchdog: BoardDeliveryHeadWatchdogOwner = {
    start: startWatchdog,
    stop: stopWatchdog,
  };
  const createTransport = vi.fn(() => transport);
  const createConsumer = vi.fn(
    (_transport: DeliveryConsumerTransport, callbacks: DeliveryConsumerCallbacks) => {
      if (options.consumerFactoryError) throw options.consumerFactoryError;
      consumerCallbacks = callbacks;
      return { start: startConsumer, stop: stopConsumer };
    },
  );
  const createRuntime = vi.fn(
    (runtimeOptions: ApiDeliveryRuntimeOptions) => new ApiDeliveryRuntime(runtimeOptions),
  );
  const createWatchdog = vi.fn(() => watchdog);
  const factories = {
    createTransport,
    createConsumer,
    createRuntime,
    createWatchdog,
  } satisfies DistributedDeliveryStackFactories;
  return {
    factories,
    createTransport,
    createConsumer,
    createRuntime,
    createWatchdog,
    startConsumer,
    stopConsumer,
    closeTransport,
    startWatchdog,
    stopWatchdog,
  };
}

describe("configured API delivery composition", () => {
  it("keeps local mode Redis-free and immediately usable", async () => {
    const stack = stackHarness();
    const options = configuredDeliveryBuildOptions(localEnvironment, stack.factories);
    expect(options.deliveryMode).toEqual({ mode: "local" });

    const context = await buildApp(localEnvironment, unusedPool, authentication, options);
    await context.app.ready();
    await context.app.close();

    expect(stack.createTransport).not.toHaveBeenCalled();
    expect(stack.createConsumer).not.toHaveBeenCalled();
    expect(stack.createRuntime).not.toHaveBeenCalled();
    expect(stack.createWatchdog).not.toHaveBeenCalled();
  });

  it("constructs and app-owns one complete distributed stack", async () => {
    const stack = stackHarness();
    const options = configuredDeliveryBuildOptions(distributedEnvironment, stack.factories);
    expect(options.deliveryMode?.mode).toBe("distributed");

    const context = await buildApp(distributedEnvironment, unusedPool, authentication, options);
    await context.app.ready();
    await context.app.ready();
    await context.app.close();
    await context.app.close();

    expect(stack.createRuntime).toHaveBeenCalledOnce();
    expect(stack.createWatchdog).toHaveBeenCalledOnce();
    expect(stack.createTransport).toHaveBeenCalledOnce();
    expect(stack.createTransport).toHaveBeenCalledWith(
      distributedEnvironment.REDIS_URL,
      distributedEnvironment.REDIS_STREAM_KEY,
    );
    expect(stack.createConsumer).toHaveBeenCalledOnce();
    expect(stack.startConsumer).toHaveBeenCalledOnce();
    expect(stack.stopConsumer).toHaveBeenCalledOnce();
    expect(stack.startWatchdog).toHaveBeenCalledOnce();
    expect(stack.stopWatchdog).toHaveBeenCalledOnce();
    expect(stack.closeTransport).toHaveBeenCalledOnce();
  });

  it("cleans up transport, runtime, and watchdog after partial startup failure", async () => {
    const stack = stackHarness({ consumerFactoryError: new Error("consumer construction failed") });
    const context = await buildApp(
      distributedEnvironment,
      unusedPool,
      authentication,
      configuredDeliveryBuildOptions(distributedEnvironment, stack.factories),
    );

    await expect(context.app.ready()).rejects.toThrow("consumer construction failed");
    await context.app.close();
    await context.app.close();

    expect(stack.createTransport).toHaveBeenCalledOnce();
    expect(stack.createConsumer).toHaveBeenCalledOnce();
    expect(stack.closeTransport).toHaveBeenCalledOnce();
    expect(stack.stopWatchdog).toHaveBeenCalledOnce();
  });

  it("forwards every API environment variable through Turbo", () => {
    const turbo = JSON.parse(
      readFileSync(new URL("../../../turbo.json", import.meta.url), "utf8"),
    ) as { globalPassThroughEnv: string[] };
    expect(turbo.globalPassThroughEnv).toEqual(
      expect.arrayContaining([...apiEnvironmentVariableNames]),
    );
  });
});

function serverDependencies(input: {
  app: AppContext["app"];
  pool: DatabasePool;
}): ApiServerDependencies {
  return {
    createDatabasePool: vi.fn(() => input.pool),
    createAuthentication: vi.fn(() => authentication),
    createDeliveryOptions: vi.fn((): BuildAppOptions => ({ deliveryMode: { mode: "local" } })),
    buildApplication: vi.fn(() => Promise.resolve({ app: input.app, io: {} as AppContext["io"] })),
  };
}

describe("API server resource ownership", () => {
  it("uses app.close as an idempotent shutdown boundary before closing PostgreSQL", async () => {
    const calls: string[] = [];
    const closeApp = vi.fn(() => {
      calls.push("app");
      return Promise.resolve();
    });
    const app = {
      close: closeApp,
      listen: vi.fn(() => Promise.resolve("http://127.0.0.1:4000")),
    } as unknown as AppContext["app"];
    const endPool = vi.fn(() => {
      calls.push("pool");
      return Promise.resolve();
    });
    const pool = {
      end: endPool,
    } as unknown as DatabasePool;
    const server = await createApiServer(localEnvironment, serverDependencies({ app, pool }));

    await server.close();
    await server.close();

    expect(closeApp).toHaveBeenCalledOnce();
    expect(endPool).toHaveBeenCalledOnce();
    expect(calls).toEqual(["app", "pool"]);
  });

  it("sanitizes listen failures and closes every constructed owner", async () => {
    const secret = "redis-password-must-stay-private";
    const closeApp = vi.fn(() => Promise.resolve());
    const app = {
      close: closeApp,
      listen: vi.fn(() => Promise.reject(new Error(`unable to connect with ${secret}`))),
    } as unknown as AppContext["app"];
    const endPool = vi.fn(() => Promise.resolve());
    const pool = { end: endPool } as unknown as DatabasePool;
    const server = await createApiServer(distributedEnvironment, serverDependencies({ app, pool }));

    const error = await server.listen().catch((caught: unknown) => caught);
    expect(error).toEqual(new ApiServerStartupError("listen"));
    expect(String(error)).not.toContain(secret);
    expect(closeApp).toHaveBeenCalledOnce();
    expect(endPool).toHaveBeenCalledOnce();
  });

  it("sanitizes construction failures and releases PostgreSQL ownership", async () => {
    const secret = "database-password-must-stay-private";
    const endPool = vi.fn(() => Promise.resolve());
    const pool = { end: endPool } as unknown as DatabasePool;
    const dependencies = serverDependencies({
      app: {} as AppContext["app"],
      pool,
    });
    dependencies.buildApplication = vi.fn(() =>
      Promise.reject(new Error(`failed for postgresql://user:${secret}@database`)),
    );

    const error = await createApiServer(localEnvironment, dependencies).catch(
      (caught: unknown) => caught,
    );
    expect(error).toEqual(new ApiServerStartupError("construction"));
    expect(String(error)).not.toContain(secret);
    expect(endPool).toHaveBeenCalledOnce();
  });
});
