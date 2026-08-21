import { describe, expect, it } from "vitest";
import {
  PRESENCE_SNAPSHOT_MAX_SESSIONS,
  boardPresenceSnapshotSchema,
  presenceAvailabilitySchema,
  presenceParticipantLeaveSchema,
  presenceParticipantUpsertSchema,
  presenceUpdateSchema,
} from "../src/index.js";

const ids = {
  board: "10000000-0000-4000-8000-000000000001",
  user: "20000000-0000-4000-8000-000000000002",
  session: "30000000-0000-4000-8000-000000000003",
  secondSession: "40000000-0000-4000-8000-000000000004",
};
const observedAt = "2026-08-20T12:00:00.000Z";
const expiresAt = "2026-08-20T12:00:45.000Z";
const participant = (presenceSessionId = ids.session) => ({
  presenceSessionId,
  userId: ids.user,
  displayName: "Ada",
  revision: 4,
  activity: "active" as const,
  cursor: { x: 12, y: -5 },
  observedAt,
  expiresAt,
});

describe("ephemeral presence protocol", () => {
  it("strictly accepts bounded client updates and each server message", () => {
    expect(
      presenceUpdateSchema.parse({
        schemaVersion: 1,
        boardId: ids.board,
        cursor: null,
        activity: "idle",
      }),
    ).toMatchObject({ boardId: ids.board });
    expect(
      boardPresenceSnapshotSchema.parse({
        schemaVersion: 1,
        boardId: ids.board,
        observedAt,
        selfPresenceSessionId: ids.session,
        participants: [participant()],
      }).participants,
    ).toHaveLength(1);
    expect(
      presenceParticipantUpsertSchema.parse({
        schemaVersion: 1,
        boardId: ids.board,
        participant: participant(),
      }).participant.revision,
    ).toBe(4);
    expect(
      presenceParticipantLeaveSchema.parse({
        schemaVersion: 1,
        boardId: ids.board,
        presenceSessionId: ids.session,
        revision: 5,
        reason: "expired",
        observedAt,
      }).reason,
    ).toBe("expired");
    expect(
      presenceAvailabilitySchema.parse({
        schemaVersion: 1,
        boardId: ids.board,
        status: "unavailable",
      }).status,
    ).toBe("unavailable");
  });

  it("rejects client-injected identity, server evidence, invalid coordinates, and unknown fields", () => {
    const update = {
      schemaVersion: 1,
      boardId: ids.board,
      cursor: { x: 1, y: 2 },
      activity: "active",
    };
    for (const invalid of [
      { ...update, userId: ids.user },
      { ...update, displayName: "Ada" },
      { ...update, color: "red" },
      { ...update, revision: 1 },
      { ...update, observedAt },
      { ...update, cursor: { x: Infinity, y: 2 } },
      { ...update, cursor: { x: 1_000_001, y: 2 } },
      { ...update, activity: "busy" },
      { ...update, boardId: "not-a-uuid" },
    ])
      expect(presenceUpdateSchema.safeParse(invalid).success).toBe(false);
  });

  it("enforces revision, timestamps, and snapshot bounds while allowing multiple sessions for one user", () => {
    expect(
      presenceParticipantUpsertSchema.safeParse({
        schemaVersion: 1,
        boardId: ids.board,
        participant: { ...participant(), revision: -1 },
      }).success,
    ).toBe(false);
    expect(
      presenceParticipantUpsertSchema.safeParse({
        schemaVersion: 1,
        boardId: ids.board,
        participant: { ...participant(), observedAt: "tomorrow" },
      }).success,
    ).toBe(false);
    const sessions = Array.from({ length: PRESENCE_SNAPSHOT_MAX_SESSIONS }, (_, index) =>
      participant(
        index === 1
          ? ids.secondSession
          : `${index}`.padStart(8, "0") + "-0000-4000-8000-000000000003",
      ),
    );
    sessions[0] = participant();
    expect(
      boardPresenceSnapshotSchema.safeParse({
        schemaVersion: 1,
        boardId: ids.board,
        observedAt,
        selfPresenceSessionId: ids.session,
        participants: sessions,
      }).success,
    ).toBe(true);
    expect(
      boardPresenceSnapshotSchema.safeParse({
        schemaVersion: 1,
        boardId: ids.board,
        observedAt,
        selfPresenceSessionId: ids.session,
        participants: [...sessions, participant(ids.secondSession)],
      }).success,
    ).toBe(false);
    expect(
      boardPresenceSnapshotSchema
        .parse({
          schemaVersion: 1,
          boardId: ids.board,
          observedAt,
          selfPresenceSessionId: ids.session,
          participants: [participant(), participant(ids.secondSession)],
        })
        .participants.map(({ userId }) => userId),
    ).toEqual([ids.user, ids.user]);
  });

  it("rejects unknown server fields and overlong participant text", () => {
    expect(
      presenceParticipantUpsertSchema.safeParse({
        schemaVersion: 1,
        boardId: ids.board,
        participant: { ...participant(), displayName: "x".repeat(101) },
      }).success,
    ).toBe(false);
    expect(
      boardPresenceSnapshotSchema.safeParse({
        schemaVersion: 1,
        boardId: ids.board,
        observedAt,
        selfPresenceSessionId: ids.session,
        participants: [],
        redisChannel: "internal",
      }).success,
    ).toBe(false);
    expect(
      presenceParticipantLeaveSchema.safeParse({
        schemaVersion: 1,
        boardId: ids.board,
        presenceSessionId: ids.session,
        revision: 1,
        reason: "left",
        observedAt,
        socketId: "internal",
      }).success,
    ).toBe(false);
    expect(
      presenceAvailabilitySchema.safeParse({
        schemaVersion: 1,
        boardId: ids.board,
        status: "available",
        error: "redis failure",
      }).success,
    ).toBe(false);
  });

  it("requires exactly one admitted self session while allowing other tabs for that user", () => {
    const base = {
      schemaVersion: 1,
      boardId: ids.board,
      observedAt,
      selfPresenceSessionId: ids.session,
      participants: [participant(), participant(ids.secondSession)],
    };
    expect(boardPresenceSnapshotSchema.safeParse(base).success).toBe(true);
    expect(
      boardPresenceSnapshotSchema.safeParse({ ...base, selfPresenceSessionId: ids.secondSession })
        .success,
    ).toBe(true);
    expect(
      boardPresenceSnapshotSchema.safeParse({
        ...base,
        selfPresenceSessionId: ids.secondSession.replace("4", "5"),
      }).success,
    ).toBe(false);
    expect(
      boardPresenceSnapshotSchema.safeParse({
        ...base,
        participants: [participant(ids.secondSession)],
      }).success,
    ).toBe(false);
    expect(
      boardPresenceSnapshotSchema.safeParse({
        ...base,
        participants: [participant(), participant()],
      }).success,
    ).toBe(false);
    expect(
      boardPresenceSnapshotSchema.safeParse({ ...base, selfPresenceSessionId: "invalid" }).success,
    ).toBe(false);
  });
});
