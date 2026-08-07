import { describe, expect, it } from "vitest";
import {
  createBoardRequestSchema,
  durableCommandSchema,
  ephemeralEventTypeSchema,
  joinBoardAckSchema,
  joinBoardRequestSchema,
  operationRangeQuerySchema,
  operationRangeResponseSchema,
  protocolErrorSchema,
  SCHEMA_VERSION,
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
