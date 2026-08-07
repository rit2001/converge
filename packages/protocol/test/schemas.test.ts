import { describe, expect, it } from "vitest";
import {
  createBoardRequestSchema,
  durableCommandSchema,
  ephemeralEventTypeSchema,
  joinBoardRequestSchema,
  operationRangeQuerySchema,
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
        pendingOpIds: [],
        surprise: true,
      }).success,
    ).toBe(false);
  });

  it("keeps HTTP request schemas strict", () => {
    expect(createBoardRequestSchema.safeParse({ name: "Board", surprise: true }).success).toBe(
      false,
    );
    expect(
      operationRangeQuerySchema.safeParse({ from: "1", to: "2", surprise: true }).success,
    ).toBe(false);
  });

  it("classifies previews as ephemeral", () => {
    expect(ephemeralEventTypeSchema.parse("transform.preview")).toBe("transform.preview");
    expect(ephemeralEventTypeSchema.safeParse("object.update").success).toBe(false);
  });
});
