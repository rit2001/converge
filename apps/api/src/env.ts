import { z } from "zod";

const environmentSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  HOST: z.string().default("0.0.0.0"),
  API_PORT: z.coerce.number().int().min(1).max(65_535).default(4000),
  WEB_ORIGIN: z.string().url().default("http://localhost:3000"),
  DATABASE_URL: z.string().url(),
  REDIS_URL: z.string().url().default("redis://localhost:6379"),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
  DEV_AUTH_USER_ID: z.string().uuid().optional(),
  DEV_AUTH_USER_NAME: z.string().min(1).max(100).default("Local Developer"),
});

export type Environment = z.infer<typeof environmentSchema>;

export class EnvironmentConfigurationError extends Error {
  constructor(public readonly fields: string[]) {
    super(`Invalid environment configuration: ${fields.join(", ")}`);
  }
}

export function parseEnvironment(source: NodeJS.ProcessEnv): Environment {
  const result = environmentSchema.safeParse(source);
  if (result.success) return result.data;
  const fields = [
    ...new Set(
      result.error.issues.map((issue) =>
        typeof issue.path[0] === "string" ? issue.path[0] : "environment",
      ),
    ),
  ].sort();
  throw new EnvironmentConfigurationError(fields);
}
