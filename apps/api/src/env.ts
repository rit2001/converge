import { BOARD_DELIVERY_HEAD_QUERY_MAXIMUM } from "@converge/database";
import {
  DELIVERY_ENVELOPE_MAX_BYTES,
  DELIVERY_STREAM_METADATA_MAX_BYTES,
  REDIS_STREAM_ENTRY_ID_MAX_BYTES,
} from "@converge/protocol";
import { z } from "zod";

const TIMER_MAXIMUM_MS = 2_147_483_647;
const REDIS_DELIVERY_MAXIMUM_BOARD_STATES = 100_000;
const REDIS_DELIVERY_READ_COUNT = 100;
const REDIS_DELIVERY_MINIMUM_QUEUE_BYTES =
  REDIS_DELIVERY_READ_COUNT *
  (DELIVERY_ENVELOPE_MAX_BYTES +
    DELIVERY_STREAM_METADATA_MAX_BYTES +
    REDIS_STREAM_ENTRY_ID_MAX_BYTES);
const LOCAL_REDIS_URL = "redis://localhost:6379";
const LOCAL_REDIS_STREAM_KEY = "converge:delivery:v1";

const redisStreamKeyPattern = /^[A-Za-z0-9:._-]{1,128}$/;

const environmentShape = {
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  HOST: z.string().default("0.0.0.0"),
  API_PORT: z.coerce.number().int().min(1).max(65_535).default(4000),
  WEB_ORIGIN: z.string().url().default("http://localhost:3000"),
  DATABASE_URL: z.string().url(),
  API_DELIVERY_MODE: z.enum(["local", "distributed"]).default("local"),
  REDIS_URL: z.string().url().optional(),
  REDIS_STREAM_KEY: z.string().optional(),
  REDIS_API_QUEUE_MAX_EVENTS: z.coerce
    .number()
    .int()
    .min(REDIS_DELIVERY_READ_COUNT)
    .max(1_000_000)
    .default(1_000),
  REDIS_API_QUEUE_MAX_BYTES: z.coerce
    .number()
    .int()
    .min(REDIS_DELIVERY_MINIMUM_QUEUE_BYTES)
    .max(1_073_741_824)
    .default(16 * 1024 * 1024),
  REDIS_DELIVERY_MAX_BOARD_STATES: z.coerce
    .number()
    .int()
    .min(1)
    .max(REDIS_DELIVERY_MAXIMUM_BOARD_STATES)
    .default(1_000),
  REDIS_DELIVERY_RECONNECT_DELAY_MS: z.coerce
    .number()
    .int()
    .min(1)
    .max(TIMER_MAXIMUM_MS)
    .default(250),
  DELIVERY_BOARD_BUFFER_MAX_EVENTS: z.coerce.number().int().min(1).max(1_000_000).default(100),
  DELIVERY_BOARD_BUFFER_MAX_BYTES: z.coerce
    .number()
    .int()
    .min(1)
    .max(1_073_741_824)
    .default(2 * 1024 * 1024),
  DELIVERY_DEDUPE_WINDOW_EVENTS: z.coerce.number().int().min(1).max(1_000_000).default(256),
  DELIVERY_WATCHDOG_INTERVAL_MS: z.coerce
    .number()
    .int()
    .min(1)
    .max(TIMER_MAXIMUM_MS)
    .default(5_000),
  DELIVERY_WATCHDOG_GRACE_MS: z.coerce.number().int().min(1).max(TIMER_MAXIMUM_MS).default(5_000),
  DELIVERY_WATCHDOG_QUERY_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .min(1)
    .max(TIMER_MAXIMUM_MS)
    .default(5_000),
  DELIVERY_WATCHDOG_BATCH_SIZE: z.coerce
    .number()
    .int()
    .min(1)
    .max(BOARD_DELIVERY_HEAD_QUERY_MAXIMUM)
    .default(BOARD_DELIVERY_HEAD_QUERY_MAXIMUM),
  DELIVERY_WATCHDOG_JITTER_RATIO: z.coerce.number().min(0).max(0.2).default(0.2),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
  DEV_AUTH_USER_ID: z.string().uuid().optional(),
  DEV_AUTH_USER_NAME: z.string().min(1).max(100).default("Local Developer"),
} satisfies z.ZodRawShape;

export const apiEnvironmentVariableNames = Object.freeze(Object.keys(environmentShape));

const environmentSchema = z
  .object(environmentShape)
  .superRefine((environment, context) => {
    if (environment.API_DELIVERY_MODE === "distributed") {
      if (environment.REDIS_URL === undefined)
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["REDIS_URL"],
          message: "Redis URL is required for distributed API delivery",
        });
      else if (
        !environment.REDIS_URL.startsWith("redis://") &&
        !environment.REDIS_URL.startsWith("rediss://")
      )
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["REDIS_URL"],
          message: "Redis URL must use a Redis protocol",
        });
      if (environment.REDIS_STREAM_KEY === undefined)
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["REDIS_STREAM_KEY"],
          message: "Redis stream key is required for distributed API delivery",
        });
      else if (!redisStreamKeyPattern.test(environment.REDIS_STREAM_KEY))
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["REDIS_STREAM_KEY"],
          message: "Redis stream key is invalid",
        });
    }
    if (environment.DELIVERY_WATCHDOG_BATCH_SIZE > environment.REDIS_DELIVERY_MAX_BOARD_STATES)
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["DELIVERY_WATCHDOG_BATCH_SIZE"],
        message: "Watchdog batch size exceeds retained board-state capacity",
      });
    if (
      environment.DELIVERY_WATCHDOG_INTERVAL_MS * (1 + environment.DELIVERY_WATCHDOG_JITTER_RATIO) >
      TIMER_MAXIMUM_MS
    )
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["DELIVERY_WATCHDOG_INTERVAL_MS"],
        message: "Watchdog jitter exceeds the timer-safe interval",
      });
  })
  .transform((environment) => ({
    ...environment,
    REDIS_URL: environment.REDIS_URL ?? LOCAL_REDIS_URL,
    REDIS_STREAM_KEY: environment.REDIS_STREAM_KEY ?? LOCAL_REDIS_STREAM_KEY,
  }));

export type Environment = z.output<typeof environmentSchema>;

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
