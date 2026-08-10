import { DELIVERY_ENVELOPE_MAX_BYTES } from "@converge/protocol";
import { z } from "zod";

const redisUrlSchema = z
  .string()
  .url()
  .refine((value) => value.startsWith("redis://") || value.startsWith("rediss://"));

const environmentShape = {
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  DATABASE_URL: z.string().url(),
  REDIS_URL: redisUrlSchema.default("redis://localhost:6379"),
  LOG_LEVEL: z.enum(["error", "warn", "info", "silent"]).default("info"),
  WORKER_ID: z
    .string()
    .regex(/^[A-Za-z0-9._:-]{1,128}$/)
    .optional(),
  OUTBOX_CLAIM_BATCH_SIZE: z.coerce.number().int().min(1).max(32).default(32),
  OUTBOX_PUBLISH_CONCURRENCY: z.coerce.number().int().min(1).max(8).default(8),
  OUTBOX_LEASE_MS: z.coerce.number().int().min(1).max(300_000).default(60_000),
  OUTBOX_FINALIZATION_MARGIN_MS: z.coerce.number().int().min(1_000).max(30_000).default(5_000),
  OUTBOX_IDLE_POLL_MS: z.coerce.number().int().min(1).max(60_000).default(250),
  OUTBOX_POLL_JITTER_RATIO: z.coerce.number().min(0).max(0.2).default(0.2),
  REDIS_PUBLISH_TIMEOUT_MS: z.coerce.number().int().min(1).max(60_000).default(5_000),
  DELIVERY_ENVELOPE_MAX_BYTES: z.coerce
    .number()
    .int()
    .min(1)
    .max(DELIVERY_ENVELOPE_MAX_BYTES)
    .default(DELIVERY_ENVELOPE_MAX_BYTES),
  REDIS_STREAM_KEY: z
    .string()
    .regex(/^[A-Za-z0-9:._-]{1,128}$/)
    .default("converge:delivery:v1"),
  REDIS_STREAM_MAXLEN: z.coerce.number().int().min(1).max(1_000_000).default(100_000),
  REDIS_STREAM_MAX_AGE_MS: z.coerce
    .number()
    .int()
    .min(1)
    .max(30 * 24 * 60 * 60 * 1000)
    .default(24 * 60 * 60 * 1000),
  WORKER_SHUTDOWN_GRACE_MS: z.coerce.number().int().min(0).max(60_000).default(10_000),
} satisfies z.ZodRawShape;

export const workerEnvironmentVariableNames = Object.freeze(Object.keys(environmentShape));

const environmentSchema = z.object(environmentShape).superRefine((environment, context) => {
  if (
    environment.OUTBOX_LEASE_MS <=
    environment.REDIS_PUBLISH_TIMEOUT_MS + environment.OUTBOX_FINALIZATION_MARGIN_MS
  )
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["OUTBOX_LEASE_MS"],
      message: "The outbox lease must reserve the database finalization safety margin",
    });
});

export type WorkerEnvironment = z.infer<typeof environmentSchema>;

export class WorkerEnvironmentConfigurationError extends Error {
  constructor(public readonly fields: string[]) {
    super(`Invalid worker environment configuration: ${fields.join(", ")}`);
  }
}

export function parseWorkerEnvironment(source: NodeJS.ProcessEnv): WorkerEnvironment {
  const result = environmentSchema.safeParse(source);
  if (result.success) return result.data;
  const fields = [
    ...new Set(
      result.error.issues.map((issue) =>
        typeof issue.path[0] === "string" ? issue.path[0] : "environment",
      ),
    ),
  ].sort();
  throw new WorkerEnvironmentConfigurationError(fields);
}
