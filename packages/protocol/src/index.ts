import { z } from "zod";

export const SCHEMA_VERSION = 1 as const;
export const MAX_SYNC_BATCH_SIZE = 100 as const;
export const DELIVERY_ENVELOPE_MAX_BYTES = 128 * 1024;

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
  "RECOVERY_BLOCKED",
  "INTERNAL_ERROR",
]);

export const protocolErrorSchema = z
  .object({
    ok: z.literal(false),
    code: errorCodeSchema,
    message: z.string().max(500),
    retryable: z.boolean(),
  })
  .strict()
  .superRefine((error, context) => {
    if (
      error.code === "RECOVERY_BLOCKED" &&
      (error.message !== "Authoritative board recovery is unavailable" || error.retryable)
    )
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Recovery-blocked errors use the fixed non-retryable public contract",
      });
  });

export const httpInternalErrorResponseSchema = z
  .object({
    ok: z.literal(false),
    code: z.literal("INTERNAL_ERROR"),
    message: z.literal("An internal server error occurred."),
    retryable: z.literal(true),
    requestId: z.string().min(1).max(128),
  })
  .strict();

export const operationAckSchema = z.union([
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

const recoveryFieldSeqSchema = z
  .object({
    id: sequenceSchema.positive(),
    kind: sequenceSchema.positive(),
    x: sequenceSchema.positive(),
    y: sequenceSchema.positive(),
    width: sequenceSchema.positive(),
    height: sequenceSchema.positive(),
    rotation: sequenceSchema.positive(),
    fill: sequenceSchema.positive(),
    text: sequenceSchema.positive(),
  })
  .strict();

export const boardRecoverySnapshotStateSchema = z
  .object({
    schemaVersion: z.literal(SCHEMA_VERSION),
    boardId: idSchema,
    boardName: z.string().min(1).max(120),
    lastSeq: sequenceSchema,
    lastDeliverySeq: sequenceSchema,
    objects: z.array(
      z
        .object({
          objectId: idSchema,
          stackOrder: sequenceSchema.positive(),
          value: canvasObjectSchema,
          fieldSeq: recoveryFieldSeqSchema,
          createdSeq: sequenceSchema.positive(),
          updatedSeq: sequenceSchema.positive(),
          deletedSeq: sequenceSchema.positive().nullable(),
        })
        .strict(),
    ),
  })
  .strict()
  .superRefine((state, context) => {
    if (state.lastDeliverySeq < state.lastSeq) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Delivery head precedes canvas head",
      });
      return;
    }
    let priorStackOrder = 0;
    for (const object of state.objects) {
      if (
        object.objectId !== object.value.id ||
        object.stackOrder <= priorStackOrder ||
        object.createdSeq > object.updatedSeq ||
        object.updatedSeq > state.lastSeq ||
        (object.deletedSeq !== null &&
          (object.deletedSeq !== object.updatedSeq || object.deletedSeq > state.lastSeq)) ||
        Object.values(object.fieldSeq).some((sequence) => sequence > state.lastSeq)
      ) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: "Invalid authoritative snapshot projection",
        });
        return;
      }
      priorStackOrder = object.stackOrder;
    }
  });

export const boardRecoveryRequestQuerySchema = z.object({}).strict();

export const boardRecoveryMaterialSchema = z
  .object({
    boardId: idSchema,
    snapshotId: idSchema,
    snapshotSchemaVersion: z.literal(SCHEMA_VERSION),
    snapshotCanvasSeq: sequenceSchema,
    snapshotDeliverySeq: sequenceSchema,
    capturedCanvasSeq: sequenceSchema,
    capturedDeliverySeq: sequenceSchema,
    snapshotState: boardRecoverySnapshotStateSchema,
    snapshotCanonicalHash: z.string().regex(/^[0-9a-f]{64}$/),
    operationTail: z.array(committedOperationSchema).max(MAX_SYNC_BATCH_SIZE),
    reconstructedCanonicalHash: z.string().regex(/^[0-9a-f]{64}$/),
  })
  .strict()
  .superRefine((material, context) => {
    if (
      material.snapshotState.boardId !== material.boardId ||
      material.snapshotState.schemaVersion !== material.snapshotSchemaVersion ||
      material.snapshotState.lastSeq !== material.snapshotCanvasSeq ||
      material.snapshotState.lastDeliverySeq !== material.snapshotDeliverySeq ||
      material.snapshotCanvasSeq > material.capturedCanvasSeq ||
      material.snapshotDeliverySeq > material.capturedDeliverySeq
    ) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Recovery metadata mismatch" });
      return;
    }
    let expected = material.snapshotCanvasSeq + 1;
    for (const operation of material.operationTail) {
      if (
        operation.boardId !== material.boardId ||
        operation.seq !== expected ||
        operation.seq > material.capturedCanvasSeq
      ) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: "Recovery operation tail must be contiguous",
        });
        return;
      }
      expected += 1;
    }
    if (expected - 1 !== material.capturedCanvasSeq)
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Recovery operation tail does not reach the captured head",
      });
  });
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

export const deliveryEventTypeSchema = z.enum(["operation.committed", "board.membership.revoked"]);

// Reserved by ADR 004, but deliberately not accepted by deliveryEnvelopeSchema until restore exists.
export const reservedDeliveryEventTypeSchema = z.literal("version.restored");

const deliveryEnvelopeMetadata = {
  schemaVersion: z.literal(SCHEMA_VERSION),
  eventId: idSchema,
  boardId: idSchema,
  deliverySeq: sequenceSchema.positive(),
  occurredAt: z.string().datetime({ offset: true }),
};

const operationCommittedDeliveryEnvelopeObjectSchema = z
  .object({
    ...deliveryEnvelopeMetadata,
    eventType: z.literal("operation.committed"),
    payload: z.object({ operation: committedOperationSchema }).strict(),
  })
  .strict();

const membershipRevokedDeliveryEnvelopeObjectSchema = z
  .object({
    ...deliveryEnvelopeMetadata,
    eventType: z.literal("board.membership.revoked"),
    payload: z
      .object({
        revokedUserId: idSchema,
        initiatedByUserId: idSchema,
      })
      .strict(),
  })
  .strict();

function validateDeliveryEnvelopeIdentity(
  envelope:
    | z.infer<typeof operationCommittedDeliveryEnvelopeObjectSchema>
    | z.infer<typeof membershipRevokedDeliveryEnvelopeObjectSchema>,
  context: z.RefinementCtx,
): void {
  if (
    envelope.eventType === "operation.committed" &&
    envelope.payload.operation.boardId !== envelope.boardId
  ) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["payload", "operation", "boardId"],
      message: "Operation board must match delivery envelope board",
    });
  }
}

export const operationCommittedDeliveryEnvelopeSchema =
  operationCommittedDeliveryEnvelopeObjectSchema.superRefine(validateDeliveryEnvelopeIdentity);

export const membershipRevokedDeliveryEnvelopeSchema =
  membershipRevokedDeliveryEnvelopeObjectSchema.superRefine(validateDeliveryEnvelopeIdentity);

export const deliveryEnvelopeSchema = z
  .discriminatedUnion("eventType", [
    operationCommittedDeliveryEnvelopeObjectSchema,
    membershipRevokedDeliveryEnvelopeObjectSchema,
  ])
  .superRefine(validateDeliveryEnvelopeIdentity);

export const redisStreamEntryIdSchema = z
  .string()
  .regex(/^(0|[1-9]\d*)-(0|[1-9]\d*)$/)
  .superRefine((value, context) => {
    const match = /^(0|[1-9]\d*)-(0|[1-9]\d*)$/.exec(value);
    if (!match) return;
    const milliseconds = match[1];
    const sequence = match[2];
    const maximum = 18_446_744_073_709_551_615n;
    if (
      value === "0-0" ||
      milliseconds === undefined ||
      sequence === undefined ||
      BigInt(milliseconds) > maximum ||
      BigInt(sequence) > maximum
    )
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Redis stream entry ID must be a positive uint64 pair",
      });
  });

export const deliveryStreamFieldsSchema = z
  .object({
    schemaVersion: z.literal(String(SCHEMA_VERSION)),
    eventId: idSchema,
    boardId: idSchema,
    deliverySeq: z.string().regex(/^[1-9]\d*$/),
    eventType: deliveryEventTypeSchema,
    event: z.string().min(2),
  })
  .strict()
  .superRefine((fields, context) => {
    const deliverySeq = Number(fields.deliverySeq);
    if (!Number.isSafeInteger(deliverySeq)) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["deliverySeq"],
        message: "Delivery sequence must be a safe integer",
      });
      return;
    }
    let decoded: unknown;
    try {
      decoded = JSON.parse(fields.event);
    } catch {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["event"],
        message: "Serialized delivery envelope must be JSON",
      });
      return;
    }
    const envelope = deliveryEnvelopeSchema.safeParse(decoded);
    if (!envelope.success) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["event"],
        message: "Serialized delivery envelope is invalid",
      });
      return;
    }
    if (
      fields.schemaVersion !== String(envelope.data.schemaVersion) ||
      fields.eventId !== envelope.data.eventId ||
      fields.boardId !== envelope.data.boardId ||
      deliverySeq !== envelope.data.deliverySeq ||
      fields.eventType !== envelope.data.eventType
    )
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Redis stream fields must match the serialized delivery envelope",
      });
  });

export function encodeDeliveryStreamFields(input: DeliveryEnvelope): DeliveryStreamFields {
  const envelope = deliveryEnvelopeSchema.parse(input);
  return deliveryStreamFieldsSchema.parse({
    schemaVersion: String(envelope.schemaVersion),
    eventId: envelope.eventId,
    boardId: envelope.boardId,
    deliverySeq: String(envelope.deliverySeq),
    eventType: envelope.eventType,
    event: JSON.stringify(envelope),
  });
}

export function decodeDeliveryStreamFields(input: unknown): DeliveryEnvelope {
  const fields = deliveryStreamFieldsSchema.parse(input);
  return deliveryEnvelopeSchema.parse(JSON.parse(fields.event));
}

export const DELIVERY_STREAM_FIELD_NAMES = Object.freeze([
  "schemaVersion",
  "eventId",
  "boardId",
  "deliverySeq",
  "eventType",
  "event",
] as const);

const DELIVERY_STREAM_FIELD_VALUE_MAX_BYTES = Object.freeze({
  schemaVersion: String(SCHEMA_VERSION).length,
  eventId: 36,
  boardId: 36,
  deliverySeq: String(Number.MAX_SAFE_INTEGER).length,
  eventType: "board.membership.revoked".length,
} as const);

export const DELIVERY_STREAM_METADATA_MAX_BYTES =
  DELIVERY_STREAM_FIELD_NAMES.reduce((total, name) => total + name.length, 0) +
  Object.values(DELIVERY_STREAM_FIELD_VALUE_MAX_BYTES).reduce(
    (total, maximum) => total + maximum,
    0,
  );

export const DELIVERY_STREAM_ENTRY_MAX_BYTES =
  DELIVERY_ENVELOPE_MAX_BYTES + DELIVERY_STREAM_METADATA_MAX_BYTES;
export const REDIS_STREAM_ENTRY_ID_MAX_BYTES = 20 + 1 + 20;
export const DELIVERY_STREAM_DECODED_ENTRY_MAX_BYTES =
  DELIVERY_STREAM_ENTRY_MAX_BYTES + REDIS_STREAM_ENTRY_ID_MAX_BYTES;

export type DeliveryStreamFieldPair = readonly [name: string, value: string];

export type DeliveryStreamEntrySizeValidation =
  | { valid: true; entryBytes: number }
  | { valid: false; reason: "FIELD_TOO_LARGE" | "ENTRY_TOO_LARGE" };

const deliveryTextEncoder = new TextEncoder();

function utf8Bytes(value: string): number {
  return deliveryTextEncoder.encode(value).byteLength;
}

/**
 * Measures the complete producer entry before XADD. The configured envelope limit applies to the
 * serialized `event` value; all other values have tighter protocol-schema maxima. Field names are
 * included because Redis stores and returns them with every stream entry.
 */
export function validateDeliveryStreamEntrySize(
  fields: DeliveryStreamFields,
  maximumEnvelopeBytes = DELIVERY_ENVELOPE_MAX_BYTES,
): DeliveryStreamEntrySizeValidation {
  if (!Number.isSafeInteger(maximumEnvelopeBytes) || maximumEnvelopeBytes < 1)
    return { valid: false, reason: "FIELD_TOO_LARGE" };

  let entryBytes = 0;
  for (const name of DELIVERY_STREAM_FIELD_NAMES) {
    const nameBytes = utf8Bytes(name);
    const value = fields[name];
    const valueBytes = utf8Bytes(value);
    const maximumValueBytes =
      name === "event" ? maximumEnvelopeBytes : DELIVERY_STREAM_FIELD_VALUE_MAX_BYTES[name];
    if (nameBytes !== name.length || valueBytes > maximumValueBytes)
      return { valid: false, reason: "FIELD_TOO_LARGE" };
    entryBytes += nameBytes + valueBytes;
  }

  if (entryBytes > maximumEnvelopeBytes + DELIVERY_STREAM_METADATA_MAX_BYTES)
    return { valid: false, reason: "ENTRY_TOO_LARGE" };
  return { valid: true, entryBytes };
}

/**
 * Decodes Redis field pairs without first collapsing them into an object. This is the consumer-side
 * trust boundary: duplicate field names must remain observable and fail validation.
 */
export function decodeDeliveryStreamFieldPairs(
  pairs: readonly DeliveryStreamFieldPair[],
  maximumEnvelopeBytes: number,
): DeliveryEnvelope {
  if (!Number.isSafeInteger(maximumEnvelopeBytes) || maximumEnvelopeBytes < 1)
    throw new Error("Maximum delivery envelope bytes must be a positive safe integer");
  const fields: Record<string, string> = {};
  const seen = new Set<string>();
  for (const [name, value] of pairs) {
    if (seen.has(name)) throw new Error("Redis stream entry has duplicate fields");
    if (!(DELIVERY_STREAM_FIELD_NAMES as readonly string[]).includes(name))
      throw new Error("Redis stream entry has unknown fields");
    seen.add(name);
    fields[name] = value;
  }
  if (pairs.length !== DELIVERY_STREAM_FIELD_NAMES.length)
    throw new Error("Redis stream entry has an invalid field count");
  for (const name of DELIVERY_STREAM_FIELD_NAMES)
    if (!seen.has(name)) throw new Error("Redis stream entry is missing required fields");

  const event = fields.event;
  if (event === undefined || new TextEncoder().encode(event).byteLength > maximumEnvelopeBytes)
    throw new Error("Redis stream delivery envelope exceeds the configured byte limit");
  return decodeDeliveryStreamFields(fields);
}
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

export const joinBoardAckSchema = z.union([joinBoardSuccessSchema, protocolErrorSchema]);

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
export type BoardRecoverySnapshotState = z.infer<typeof boardRecoverySnapshotStateSchema>;
export type BoardRecoveryMaterial = z.infer<typeof boardRecoveryMaterialSchema>;
export type JoinBoardRequest = z.infer<typeof joinBoardRequestSchema>;
export type JoinBoardAck = z.infer<typeof joinBoardAckSchema>;
export type OperationRangeQuery = z.infer<typeof operationRangeQuerySchema>;
export type OperationRangeResponse = z.infer<typeof operationRangeResponseSchema>;
export type RemoveBoardMemberParams = z.infer<typeof removeBoardMemberParamsSchema>;
export type RemoveBoardMemberResponse = z.infer<typeof removeBoardMemberResponseSchema>;
export type BoardAccessRevokedEvent = z.infer<typeof boardAccessRevokedEventSchema>;
export type DeliveryEventType = z.infer<typeof deliveryEventTypeSchema>;
export type OperationCommittedDeliveryEnvelope = z.infer<
  typeof operationCommittedDeliveryEnvelopeSchema
>;
export type MembershipRevokedDeliveryEnvelope = z.infer<
  typeof membershipRevokedDeliveryEnvelopeSchema
>;
export type DeliveryEnvelope = z.infer<typeof deliveryEnvelopeSchema>;
export type DeliveryStreamFields = z.infer<typeof deliveryStreamFieldsSchema>;

export interface ClientToServerEvents {
  "board:join": (request: JoinBoardRequest, acknowledge: (ack: JoinBoardAck) => void) => void;
  "operation:submit": (command: DurableCommand, acknowledge: (ack: OperationAck) => void) => void;
}
export interface ServerToClientEvents {
  "operation:committed": (operation: CommittedOperation) => void;
  "board:access-revoked": (event: BoardAccessRevokedEvent) => void;
}
