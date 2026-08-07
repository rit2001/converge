import { z } from "zod";

export const SCHEMA_VERSION = 1 as const;

export const idSchema = z.string().uuid();
export const sequenceSchema = z.number().int().nonnegative().safe();
const boundedText = z.string().max(10_000);
const finiteNumber = z.number().finite().min(-1_000_000).max(1_000_000);
const positiveSize = z.number().finite().min(8).max(100_000);

export const rectangleObjectSchema = z.object({
  id: idSchema,
  kind: z.literal("rectangle"),
  x: finiteNumber,
  y: finiteNumber,
  width: positiveSize,
  height: positiveSize,
  rotation: finiteNumber,
  fill: z.string().regex(/^#[0-9a-fA-F]{6}$/),
  text: z.literal(""),
});

export const stickyObjectSchema = z.object({
  id: idSchema,
  kind: z.literal("sticky"),
  x: finiteNumber,
  y: finiteNumber,
  width: positiveSize,
  height: positiveSize,
  rotation: finiteNumber,
  fill: z.string().regex(/^#[0-9a-fA-F]{6}$/),
  text: boundedText,
});

export const canvasObjectSchema = z.discriminatedUnion("kind", [
  rectangleObjectSchema,
  stickyObjectSchema,
]);

export const objectPatchSchema = z
  .object({
    fill: z
      .string()
      .regex(/^#[0-9a-fA-F]{6}$/)
      .optional(),
    text: boundedText.optional(),
  })
  .strict()
  .refine((patch) => Object.keys(patch).length > 0, "Patch must contain a field");

export const transformPatchSchema = z
  .object({
    x: finiteNumber.optional(),
    y: finiteNumber.optional(),
    width: positiveSize.optional(),
    height: positiveSize.optional(),
    rotation: finiteNumber.optional(),
  })
  .strict()
  .refine((patch) => Object.keys(patch).length > 0, "Transform must contain a field");

const commandBaseSchema = z.object({
  schemaVersion: z.literal(SCHEMA_VERSION),
  opId: idSchema,
  boardId: idSchema,
  clientId: idSchema,
  baseSeq: sequenceSchema,
  targetId: idSchema,
  clientTimestamp: z.string().datetime({ offset: true }),
});

export const objectCreateCommandSchema = commandBaseSchema.extend({
  type: z.literal("object.create"),
  payload: canvasObjectSchema,
});
export const objectUpdateCommandSchema = commandBaseSchema.extend({
  type: z.literal("object.update"),
  payload: objectPatchSchema,
});
export const objectTransformCommandSchema = commandBaseSchema.extend({
  type: z.literal("object.transform"),
  payload: transformPatchSchema,
});
export const objectDeleteCommandSchema = commandBaseSchema.extend({
  type: z.literal("object.delete"),
  payload: z.object({}).strict(),
});

export const durableCommandSchema = z.discriminatedUnion("type", [
  objectCreateCommandSchema,
  objectUpdateCommandSchema,
  objectTransformCommandSchema,
  objectDeleteCommandSchema,
]);

export const durableEventTypeSchema = z.enum([
  "object.create",
  "object.update",
  "object.transform",
  "object.delete",
  "object.restore",
  "object.reorder",
  "group.create",
  "group.ungroup",
  "board.clear",
  "version.restore",
]);

export const ephemeralEventTypeSchema = z.enum([
  "presence.join",
  "presence.leave",
  "cursor.move",
  "selection.preview",
  "transform.preview",
  "stroke.preview",
  "text.preview",
]);

export const committedOperationSchema = durableCommandSchema.and(
  z.object({ seq: sequenceSchema.positive(), committedAt: z.string().datetime({ offset: true }) }),
);

export const errorCodeSchema = z.enum([
  "AUTHENTICATION_REQUIRED",
  "BOARD_NOT_FOUND",
  "FORBIDDEN",
  "INVALID_COMMAND",
  "PAYLOAD_TOO_LARGE",
  "RATE_LIMITED",
  "TARGET_NOT_FOUND",
  "TARGET_DELETED",
  "CONFLICT",
  "INTERNAL_ERROR",
]);

export const operationAckSchema = z.discriminatedUnion("ok", [
  z.object({ ok: z.literal(true), duplicate: z.boolean(), operation: committedOperationSchema }),
  z.object({
    ok: z.literal(false),
    code: errorCodeSchema,
    message: z.string().max(500),
    retryable: z.boolean(),
  }),
]);

export const boardSnapshotSchema = z.object({
  id: idSchema,
  name: z.string().min(1).max(120),
  lastSeq: sequenceSchema,
  objects: z.array(canvasObjectSchema),
});
export const createBoardRequestSchema = z
  .object({ name: z.string().trim().min(1).max(120) })
  .strict();
export const joinBoardRequestSchema = z.object({
  schemaVersion: z.literal(SCHEMA_VERSION),
  boardId: idSchema,
  clientId: idSchema,
  lastAppliedSeq: sequenceSchema,
  pendingOpIds: z.array(idSchema).max(1_000),
});
export const operationRangeQuerySchema = z
  .object({
    from: z.coerce.number().int().positive(),
    to: z.coerce.number().int().positive(),
  })
  .refine(
    ({ from, to }) => to >= from && to - from < 1_000,
    "Range must contain at most 1,000 operations",
  );

export type CanvasObject = z.infer<typeof canvasObjectSchema>;
export type ObjectPatch = z.infer<typeof objectPatchSchema>;
export type TransformPatch = z.infer<typeof transformPatchSchema>;
export type DurableCommand = z.infer<typeof durableCommandSchema>;
export type CommittedOperation = z.infer<typeof committedOperationSchema>;
export type OperationAck = z.infer<typeof operationAckSchema>;
export type BoardSnapshot = z.infer<typeof boardSnapshotSchema>;
export type JoinBoardRequest = z.infer<typeof joinBoardRequestSchema>;

export interface ClientToServerEvents {
  "board:join": (
    request: JoinBoardRequest,
    acknowledge: (ack: OperationAck | { ok: true }) => void,
  ) => void;
  "operation:submit": (command: DurableCommand, acknowledge: (ack: OperationAck) => void) => void;
}
export interface ServerToClientEvents {
  "operation:committed": (operation: CommittedOperation) => void;
}
