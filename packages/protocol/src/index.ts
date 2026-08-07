import { z } from "zod";

export const SCHEMA_VERSION = 1 as const;
export const MAX_SYNC_BATCH_SIZE = 100 as const;

export const idSchema = z.string().uuid();
export const sequenceSchema = z.number().int().nonnegative().safe();
const boundedText = z.string().max(10_000);
const finiteNumber = z.number().finite().min(-1_000_000).max(1_000_000);
const positiveSize = z.number().finite().min(8).max(100_000);

export const rectangleObjectSchema = z
  .object({
    id: idSchema,
    kind: z.literal("rectangle"),
    x: finiteNumber,
    y: finiteNumber,
    width: positiveSize,
    height: positiveSize,
    rotation: finiteNumber,
    fill: z.string().regex(/^#[0-9a-fA-F]{6}$/),
    text: z.literal(""),
  })
  .strict();

export const stickyObjectSchema = z
  .object({
    id: idSchema,
    kind: z.literal("sticky"),
    x: finiteNumber,
    y: finiteNumber,
    width: positiveSize,
    height: positiveSize,
    rotation: finiteNumber,
    fill: z.string().regex(/^#[0-9a-fA-F]{6}$/),
    text: boundedText,
  })
  .strict();

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

const commandBaseSchema = z
  .object({
    schemaVersion: z.literal(SCHEMA_VERSION),
    opId: idSchema,
    boardId: idSchema,
    clientId: idSchema,
    baseSeq: sequenceSchema,
    targetId: idSchema,
    clientTimestamp: z.string().datetime({ offset: true }),
  })
  .strict();

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

const committedFields = {
  seq: sequenceSchema.positive(),
  committedAt: z.string().datetime({ offset: true }),
};

export const committedOperationSchema = z.discriminatedUnion("type", [
  objectCreateCommandSchema.extend(committedFields),
  objectUpdateCommandSchema.extend(committedFields),
  objectTransformCommandSchema.extend(committedFields),
  objectDeleteCommandSchema.extend(committedFields),
]);

export const errorCodeSchema = z.enum([
  "AUTHENTICATION_REQUIRED",
  "INVALID_AUTH_INPUT",
  "BOARD_NOT_FOUND",
  "FORBIDDEN",
  "INVALID_COMMAND",
  "PAYLOAD_TOO_LARGE",
  "RATE_LIMITED",
  "TARGET_NOT_FOUND",
  "TARGET_DELETED",
  "CONFLICT",
  "IDEMPOTENCY_CONFLICT",
  "RESYNC_REQUIRED",
  "CANNOT_REMOVE_OWNER",
  "ACCESS_REVOKED",
  "INTERNAL_ERROR",
]);

export const protocolErrorSchema = z
  .object({
    ok: z.literal(false),
    code: errorCodeSchema,
    message: z.string().max(500),
    retryable: z.boolean(),
  })
  .strict();

export const httpInternalErrorResponseSchema = z
  .object({
    ok: z.literal(false),
    code: z.literal("INTERNAL_ERROR"),
    message: z.literal("An internal server error occurred."),
    retryable: z.literal(true),
    requestId: z.string().min(1).max(128),
  })
  .strict();

export const operationAckSchema = z.discriminatedUnion("ok", [
  z
    .object({ ok: z.literal(true), duplicate: z.boolean(), operation: committedOperationSchema })
    .strict(),
  protocolErrorSchema,
]);

export const boardSnapshotSchema = z
  .object({
    id: idSchema,
    name: z.string().min(1).max(120),
    lastSeq: sequenceSchema,
    objects: z.array(canvasObjectSchema),
  })
  .strict();
export const createBoardRequestSchema = z
  .object({ name: z.string().trim().min(1).max(120) })
  .strict();

export const removeBoardMemberParamsSchema = z
  .object({
    boardId: idSchema,
    userId: idSchema,
  })
  .strict();

export const removeBoardMemberRequestSchema = z.object({}).strict();

export const removeBoardMemberResponseSchema = z
  .object({
    ok: z.literal(true),
    boardId: idSchema,
    userId: idSchema,
    removed: z.boolean(),
    eventId: idSchema.nullable(),
  })
  .strict();

export const boardAccessRevokedEventSchema = z
  .object({
    schemaVersion: z.literal(SCHEMA_VERSION),
    boardId: idSchema,
    code: z.literal("ACCESS_REVOKED"),
    message: z.string().min(1).max(200),
  })
  .strict();

export const membershipRevocationOutboxPayloadSchema = z
  .object({
    schemaVersion: z.literal(SCHEMA_VERSION),
    eventId: idSchema,
    kind: z.literal("board.membership.revoked"),
    boardId: idSchema,
    revokedUserId: idSchema,
    initiatedByUserId: idSchema,
    committedAt: z.string().datetime({ offset: true }),
  })
  .strict();
export const joinBoardRequestSchema = z
  .object({
    schemaVersion: z.literal(SCHEMA_VERSION),
    boardId: idSchema,
    clientId: idSchema,
    lastAppliedSeq: sequenceSchema,
  })
  .strict();

export const joinBoardSuccessSchema = z
  .object({
    ok: z.literal(true),
    boardId: idSchema,
    joinWatermark: sequenceSchema,
  })
  .strict();

export const joinBoardAckSchema = z.discriminatedUnion("ok", [
  joinBoardSuccessSchema,
  protocolErrorSchema,
]);

export const operationRangeQuerySchema = z
  .object({
    after: z.coerce.number().int().nonnegative().safe(),
    watermark: z.coerce.number().int().nonnegative().safe(),
  })
  .strict()
  .refine(({ after, watermark }) => watermark >= after, "Watermark must not precede cursor");

export const operationRangeResponseSchema = z
  .object({
    boardId: idSchema,
    afterSeq: sequenceSchema,
    watermark: sequenceSchema,
    operations: z.array(committedOperationSchema).max(MAX_SYNC_BATCH_SIZE),
    nextSeq: sequenceSchema,
    hasMore: z.boolean(),
  })
  .strict()
  .superRefine((response, context) => {
    if (response.watermark < response.afterSeq) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Invalid response range" });
      return;
    }
    let expected = response.afterSeq + 1;
    for (const operation of response.operations) {
      if (operation.boardId !== response.boardId || operation.seq !== expected) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: "Operations must be contiguous and belong to the requested board",
        });
        return;
      }
      if (operation.seq > response.watermark) {
        context.addIssue({ code: z.ZodIssueCode.custom, message: "Operation exceeds watermark" });
        return;
      }
      expected += 1;
    }
    const expectedNext = response.operations.at(-1)?.seq ?? response.afterSeq;
    if (response.nextSeq !== expectedNext || response.hasMore !== expectedNext < response.watermark)
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Invalid pagination metadata" });
    if (response.hasMore && response.operations.length === 0)
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Pagination made no progress" });
  });

export type CanvasObject = z.infer<typeof canvasObjectSchema>;
export type ObjectPatch = z.infer<typeof objectPatchSchema>;
export type TransformPatch = z.infer<typeof transformPatchSchema>;
export type DurableCommand = z.infer<typeof durableCommandSchema>;
export type CommittedOperation = z.infer<typeof committedOperationSchema>;
export type OperationAck = z.infer<typeof operationAckSchema>;
export type ProtocolError = z.infer<typeof protocolErrorSchema>;
export type HttpInternalErrorResponse = z.infer<typeof httpInternalErrorResponseSchema>;
export type BoardSnapshot = z.infer<typeof boardSnapshotSchema>;
export type JoinBoardRequest = z.infer<typeof joinBoardRequestSchema>;
export type JoinBoardAck = z.infer<typeof joinBoardAckSchema>;
export type OperationRangeQuery = z.infer<typeof operationRangeQuerySchema>;
export type OperationRangeResponse = z.infer<typeof operationRangeResponseSchema>;
export type RemoveBoardMemberParams = z.infer<typeof removeBoardMemberParamsSchema>;
export type RemoveBoardMemberResponse = z.infer<typeof removeBoardMemberResponseSchema>;
export type BoardAccessRevokedEvent = z.infer<typeof boardAccessRevokedEventSchema>;
export type MembershipRevocationOutboxPayload = z.infer<
  typeof membershipRevocationOutboxPayloadSchema
>;

export interface ClientToServerEvents {
  "board:join": (request: JoinBoardRequest, acknowledge: (ack: JoinBoardAck) => void) => void;
  "operation:submit": (command: DurableCommand, acknowledge: (ack: OperationAck) => void) => void;
}
export interface ServerToClientEvents {
  "operation:committed": (operation: CommittedOperation) => void;
  "board:access-revoked": (event: BoardAccessRevokedEvent) => void;
}
