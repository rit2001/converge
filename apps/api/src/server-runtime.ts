import { createPool, type DatabasePool } from "@converge/database";
import { buildApp, type AppContext, type BuildAppOptions } from "./app.js";
import { DevelopmentAuthAdapter, type AuthAdapter } from "./auth.js";
import { configuredDeliveryBuildOptions } from "./distributed-delivery-stack.js";
import type { Environment } from "./env.js";

export interface ApiServerDependencies {
  createDatabasePool(databaseUrl: string): DatabasePool;
  createAuthentication(environment: Environment): AuthAdapter;
  createDeliveryOptions(environment: Environment): BuildAppOptions;
  buildApplication(
    environment: Environment,
    pool: DatabasePool,
    authentication: AuthAdapter,
    options: BuildAppOptions,
  ): Promise<AppContext>;
}

const productionApiServerDependencies: ApiServerDependencies = {
  createDatabasePool: createPool,
  createAuthentication: (environment) => new DevelopmentAuthAdapter(environment),
  createDeliveryOptions: configuredDeliveryBuildOptions,
  buildApplication: buildApp,
};

export class ApiServerStartupError extends Error {
  constructor(public readonly stage: "construction" | "listen") {
    super(`API startup failed during ${stage}`);
  }
}

export interface ApiServerOwner {
  readonly app: AppContext["app"];
  listen(): Promise<void>;
  close(): Promise<void>;
}

export async function createApiServer(
  environment: Environment,
  dependencies: ApiServerDependencies = productionApiServerDependencies,
): Promise<ApiServerOwner> {
  const pool = dependencies.createDatabasePool(environment.DATABASE_URL);
  let context: AppContext;
  try {
    const options = dependencies.createDeliveryOptions(environment);
    context = await dependencies.buildApplication(
      environment,
      pool,
      dependencies.createAuthentication(environment),
      options,
    );
  } catch {
    await pool.end().catch(() => undefined);
    throw new ApiServerStartupError("construction");
  }

  let closePromise: Promise<void> | undefined;
  const close = (): Promise<void> => {
    closePromise ??= (async () => {
      let applicationError: unknown;
      try {
        await context.app.close();
      } catch (error) {
        applicationError = error;
      }
      await pool.end();
      if (applicationError !== undefined)
        throw applicationError instanceof Error
          ? applicationError
          : new Error("API application shutdown failed");
    })();
    return closePromise;
  };

  return {
    app: context.app,
    close,
    listen: async () => {
      try {
        await context.app.listen({ host: environment.HOST, port: environment.API_PORT });
      } catch {
        await close().catch(() => undefined);
        throw new ApiServerStartupError("listen");
      }
    },
  };
}
