import { describe, expect, it } from "vitest";
import {
  DELIVERY_ENVELOPE_MAX_BYTES,
  DELIVERY_STREAM_DECODED_ENTRY_MAX_BYTES,
  DELIVERY_STREAM_ENTRY_MAX_BYTES,
  DELIVERY_STREAM_METADATA_MAX_BYTES,
  REDIS_STREAM_ENTRY_ID_MAX_BYTES,
  boardAccessRevokedEventSchema,
  boardSnapshotSchema,
  createBoardRequestSchema,
  decodeDeliveryStreamFieldPairs,
  decodeDeliveryStreamFields,
  deliveryEnvelopeSchema,
  deliveryStreamFieldsSchema,
  durableCommandSchema,
  encodeDeliveryStreamFields,
  ephemeralEventTypeSchema,
  httpInternalErrorResponseSchema,
  joinBoardAckSchema,
  joinBoardRequestSchema,
  operationRangeQuerySchema,
  operationRangeResponseSchema,
  protocolErrorSchema,
  removeBoardMemberParamsSchema,
  removeBoardMemberRequestSchema,
  removeBoardMemberResponseSchema,
  redisStreamEntryIdSchema,
  SCHEMA_VERSION,
  validateDeliveryStreamEntrySize,
} from "../src/index.js";

const command = {
  schemaVersion: SCHEMA_VERSION,
  opId: "10000000-0000-4000-8000-000000000001",
  boardId: "20000000-0000-4000-8000-000000000001",
  clientId: "30000000-0000-4000-8000-000000000001",
  baseSeq: 0,
  type: "object.delete",
  targetId: "40000000-0000-4000-8000-000000000001",
  payload: {},
  clientTimestamp: "2026-08-06T12:00:00.000Z",
};

const rectangle = {
  ...command,
  type: "object.create",
  payload: {
    id: command.targetId,
    kind: "rectangle",
    x: 10,
    y: 20,
    width: 160,
    height: 100,
    rotation: 0,
    fill: "#818cf8",
    text: "",
  },
};

describe("protocol schemas", () => {
  it("keeps the complete delivery-stream producer byte contract calculable", () => {
    expect(DELIVERY_STREAM_METADATA_MAX_BYTES).toBe(165);
    expect(DELIVERY_STREAM_ENTRY_MAX_BYTES).toBe(
      DELIVERY_ENVELOPE_MAX_BYTES + DELIVERY_STREAM_METADATA_MAX_BYTES,
    );
    expect(DELIVERY_STREAM_DECODED_ENTRY_MAX_BYTES).toBe(
      DELIVERY_STREAM_ENTRY_MAX_BYTES + REDIS_STREAM_ENTRY_ID_MAX_BYTES,
    );
  });
  it("accepts a bounded versioned durable command", () => {
    expect(durableCommandSchema.parse(command)).toEqual(command);
  });

  it("rejects unknown versions", () => {
    expect(durableCommandSchema.safeParse({ ...command, schemaVersion: 2 }).success).toBe(false);
  });

  it("rejects unknown command-envelope fields", () => {
    expect(durableCommandSchema.safeParse({ ...command, surprise: true }).success).toBe(false);
  });

  it("rejects unknown create-payload fields", () => {
    expect(
      durableCommandSchema.safeParse({
        ...rectangle,
        payload: { ...rectangle.payload, surprise: true },
      }).success,
    ).toBe(false);
  });

  it("rejects unknown update-patch fields", () => {
    expect(
      durableCommandSchema.safeParse({
        ...command,
        type: "object.update",
        payload: { fill: "#ffffff", surprise: true },
      }).success,
    ).toBe(false);
  });

  it("rejects unknown board-join fields", () => {
    expect(
      joinBoardRequestSchema.safeParse({
        schemaVersion: SCHEMA_VERSION,
        boardId: command.boardId,
        clientId: command.clientId,
        lastAppliedSeq: 0,
        surprise: true,
      }).success,
    ).toBe(false);
  });

  it("rejects the obsolete pending-operation list on board join", () => {
    expect(
      joinBoardRequestSchema.parse({
        schemaVersion: SCHEMA_VERSION,
        boardId: command.boardId,
        clientId: command.clientId,
        lastAppliedSeq: 0,
      }),
    ).toMatchObject({ boardId: command.boardId, lastAppliedSeq: 0 });
    expect(
      joinBoardRequestSchema.safeParse({
        schemaVersion: SCHEMA_VERSION,
        boardId: command.boardId,
        clientId: command.clientId,
        lastAppliedSeq: 0,
        pendingOpIds: [],
      }).success,
    ).toBe(false);
  });

  it("keeps HTTP request schemas strict", () => {
    expect(createBoardRequestSchema.safeParse({ name: "Board", surprise: true }).success).toBe(
      false,
    );
    expect(
      operationRangeQuerySchema.safeParse({ after: "0", watermark: "2", surprise: true }).success,
    ).toBe(false);
  });

  it("keeps authoritative snapshot responses strict", () => {
    const snapshot = { id: command.boardId, name: "Board", lastSeq: 0, objects: [] };
    expect(boardSnapshotSchema.parse(snapshot)).toEqual(snapshot);
    expect(boardSnapshotSchema.safeParse({ ...snapshot, surprise: true }).success).toBe(false);
  });

  it("validates strict membership-removal and revocation-control schemas", () => {
    const params = { boardId: command.boardId, userId: command.clientId };
    expect(removeBoardMemberParamsSchema.parse(params)).toEqual(params);
    expect(removeBoardMemberParamsSchema.safeParse({ ...params, surprise: true }).success).toBe(
      false,
    );
    expect(removeBoardMemberRequestSchema.safeParse({ surprise: true }).success).toBe(false);
    expect(
      removeBoardMemberResponseSchema.parse({
        ok: true,
        ...params,
        removed: true,
        eventId: command.opId,
      }),
    ).toMatchObject({ removed: true, eventId: command.opId });
    const revoked = {
      schemaVersion: SCHEMA_VERSION,
      boardId: command.boardId,
      code: "ACCESS_REVOKED",
      message: "Board access was revoked",
    };
    expect(boardAccessRevokedEventSchema.parse(revoked)).toEqual(revoked);
    expect(boardAccessRevokedEventSchema.safeParse({ ...revoked, surprise: true }).success).toBe(
      false,
    );
  });

  it("validates strict current delivery-envelope variants", () => {
    const operation = {
      ...command,
      seq: 1,
      committedAt: "2026-08-07T12:00:01.000Z",
    };
    const operationEnvelope = {
      schemaVersion: SCHEMA_VERSION,
      eventId: command.opId,
      boardId: command.boardId,
      deliverySeq: 1,
      eventType: "operation.committed",
      occurredAt: operation.committedAt,
      payload: { operation },
    };
    expect(deliveryEnvelopeSchema.parse(operationEnvelope)).toEqual(operationEnvelope);

    const revocationEnvelope = {
      schemaVersion: SCHEMA_VERSION,
      eventId: command.clientId,
      boardId: command.boardId,
      deliverySeq: 2,
      eventType: "board.membership.revoked",
      occurredAt: "2026-08-07T12:00:02.000Z",
      payload: {
        revokedUserId: command.clientId,
        initiatedByUserId: command.targetId,
      },
    };
    expect(deliveryEnvelopeSchema.parse(revocationEnvelope)).toEqual(revocationEnvelope);
    expect(
      deliveryEnvelopeSchema.safeParse({ ...revocationEnvelope, boardContent: {} }).success,
    ).toBe(false);
    expect(
      deliveryEnvelopeSchema.safeParse({
        ...revocationEnvelope,
        payload: { ...revocationEnvelope.payload, boardContent: {} },
      }).success,
    ).toBe(false);
  });

  it("rejects malformed, mismatched, and reserved delivery envelopes", () => {
    const operation = {
      ...command,
      seq: 1,
      committedAt: "2026-08-07T12:00:01.000Z",
    };
    const envelope = {
      schemaVersion: SCHEMA_VERSION,
      eventId: command.opId,
      boardId: command.boardId,
      deliverySeq: 1,
      eventType: "operation.committed",
      occurredAt: operation.committedAt,
      payload: { operation },
    };
    expect(deliveryEnvelopeSchema.safeParse({ ...envelope, deliverySeq: 0 }).success).toBe(false);
    expect(
      deliveryEnvelopeSchema.safeParse({
        ...envelope,
        payload: {
          operation: {
            ...operation,
            boardId: "50000000-0000-4000-8000-000000000001",
          },
        },
      }).success,
    ).toBe(false);
    expect(
      deliveryEnvelopeSchema.safeParse({
        ...envelope,
        eventType: "version.restored",
        payload: { sourceVersionId: command.targetId, newCanvasSeq: 2 },
      }).success,
    ).toBe(false);
    expect(deliveryEnvelopeSchema.safeParse({ ...envelope, schemaVersion: 2 }).success).toBe(false);
  });

  it("round-trips strict explicit Redis stream fields without trusting redundant metadata", () => {
    const operation = {
      ...command,
      seq: 1,
      committedAt: "2026-08-07T12:00:01.000Z",
    };
    const envelope = deliveryEnvelopeSchema.parse({
      schemaVersion: SCHEMA_VERSION,
      eventId: command.opId,
      boardId: command.boardId,
      deliverySeq: 1,
      eventType: "operation.committed",
      occurredAt: operation.committedAt,
      payload: { operation },
    });
    const fields = encodeDeliveryStreamFields(envelope);
    const reorderedEnvelope = {
      payload: envelope.payload,
      occurredAt: envelope.occurredAt,
      eventType: envelope.eventType,
      deliverySeq: envelope.deliverySeq,
      boardId: envelope.boardId,
      eventId: envelope.eventId,
      schemaVersion: envelope.schemaVersion,
    };
    expect(deliveryStreamFieldsSchema.parse(fields)).toEqual(fields);
    expect(validateDeliveryStreamEntrySize(fields)).toMatchObject({ valid: true });
    expect(decodeDeliveryStreamFields(fields)).toEqual(envelope);
    expect(encodeDeliveryStreamFields(deliveryEnvelopeSchema.parse(reorderedEnvelope)).event).toBe(
      fields.event,
    );
    expect(
      deliveryStreamFieldsSchema.safeParse({ ...fields, boardId: command.targetId }).success,
    ).toBe(false);
    expect(deliveryStreamFieldsSchema.safeParse({ ...fields, surprise: true }).success).toBe(false);
    expect(redisStreamEntryIdSchema.safeParse("1730000000000-7").success).toBe(true);
    expect(redisStreamEntryIdSchema.safeParse("0-0").success).toBe(false);
    expect(redisStreamEntryIdSchema.safeParse("not-an-id").success).toBe(false);
    expect(redisStreamEntryIdSchema.safeParse("18446744073709551616-0").success).toBe(false);

    const pairs = Object.entries(fields);
    expect(decodeDeliveryStreamFieldPairs(pairs, 128 * 1024)).toEqual(envelope);
    expect(() => decodeDeliveryStreamFieldPairs([...pairs, pairs[0]!], 128 * 1024)).toThrow(
      /field count|duplicate/i,
    );
    expect(() =>
      decodeDeliveryStreamFieldPairs(
        pairs.map(([name, value]) => [name === "eventType" ? "surprise" : name, value]),
        128 * 1024,
      ),
    ).toThrow(/unknown/i);
    expect(() => decodeDeliveryStreamFieldPairs(pairs, 1)).toThrow(/byte limit/i);
  });

  it("prevents callers from supplying server-owned delivery metadata", () => {
    for (const metadata of [
      { deliverySeq: 1 },
      { eventId: "50000000-0000-4000-8000-000000000001" },
      { committedAt: "2026-08-07T12:00:01.000Z" },
      { occurredAt: "2026-08-07T12:00:01.000Z" },
      { eventType: "operation.committed" },
    ]) {
      expect(durableCommandSchema.safeParse({ ...command, ...metadata }).success).toBe(false);
    }
  });

  it("validates strict join acknowledgements and synchronization errors", () => {
    expect(
      joinBoardAckSchema.parse({
        ok: true,
        boardId: command.boardId,
        joinWatermark: 2,
      }),
    ).toEqual({ ok: true, boardId: command.boardId, joinWatermark: 2 });
    expect(
      joinBoardAckSchema.safeParse({
        ok: true,
        boardId: command.boardId,
        joinWatermark: 2,
        surprise: true,
      }).success,
    ).toBe(false);
    expect(
      protocolErrorSchema.safeParse({
        ok: false,
        code: "RESYNC_REQUIRED",
        message: "Resynchronize",
        retryable: true,
        surprise: true,
      }).success,
    ).toBe(false);
    expect(
      protocolErrorSchema.parse({
        ok: false,
        code: "IDEMPOTENCY_CONFLICT",
        message: "Operation id was already used",
        retryable: false,
      }),
    ).toMatchObject({ code: "IDEMPOTENCY_CONFLICT", retryable: false });
  });

  it("validates the strict generic HTTP internal-error envelope", () => {
    const response = {
      ok: false,
      code: "INTERNAL_ERROR",
      message: "An internal server error occurred.",
      retryable: true,
      requestId: "req-1",
    } as const;
    expect(httpInternalErrorResponseSchema.parse(response)).toEqual(response);
    expect(
      httpInternalErrorResponseSchema.safeParse({ ...response, detail: "SELECT secret" }).success,
    ).toBe(false);
  });

  it("rejects malformed synchronization ranges", () => {
    expect(operationRangeQuerySchema.parse({ after: "0", watermark: "2" })).toEqual({
      after: 0,
      watermark: 2,
    });
    expect(operationRangeQuerySchema.safeParse({ after: -1, watermark: 2 }).success).toBe(false);
    expect(operationRangeQuerySchema.safeParse({ after: 3, watermark: 2 }).success).toBe(false);
    expect(operationRangeQuerySchema.safeParse({ after: 0.5, watermark: 2 }).success).toBe(false);
  });

  it("validates bounded, contiguous operation-range responses", () => {
    const operation = {
      ...command,
      seq: 1,
      committedAt: "2026-08-06T12:00:01.000Z",
    };
    const response = {
      boardId: command.boardId,
      afterSeq: 0,
      watermark: 1,
      operations: [operation],
      nextSeq: 1,
      hasMore: false,
    };
    expect(operationRangeResponseSchema.parse(response)).toEqual(response);
    expect(operationRangeResponseSchema.safeParse({ ...response, nextSeq: 0 }).success).toBe(false);
    expect(operationRangeResponseSchema.safeParse({ ...response, surprise: true }).success).toBe(
      false,
    );
  });

  it("classifies previews as ephemeral", () => {
    expect(ephemeralEventTypeSchema.parse("transform.preview")).toBe("transform.preview");
    expect(ephemeralEventTypeSchema.safeParse("object.update").success).toBe(false);
  });
});
