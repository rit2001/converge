import { describe, expect, it } from "vitest";
import { durableCommandSchema, ephemeralEventTypeSchema, SCHEMA_VERSION } from "../src/index.js";

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

describe("protocol schemas", () => {
  it("accepts a bounded versioned durable command", () => {
    expect(durableCommandSchema.parse(command)).toEqual(command);
  });

  it("rejects unknown versions and unbounded payload fields", () => {
    expect(durableCommandSchema.safeParse({ ...command, schemaVersion: 2 }).success).toBe(false);
    expect(
      durableCommandSchema.safeParse({ ...command, payload: { surprise: true } }).success,
    ).toBe(false);
  });

  it("classifies previews as ephemeral", () => {
    expect(ephemeralEventTypeSchema.parse("transform.preview")).toBe("transform.preview");
    expect(ephemeralEventTypeSchema.safeParse("object.update").success).toBe(false);
  });
});
