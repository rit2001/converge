import {
  PRESENCE_CURSOR_UPDATES_PER_SECOND,
  PRESENCE_SESSION_TTL_MS,
  PRESENCE_SNAPSHOT_MAX_SESSIONS,
  boardPresenceSnapshotSchema,
  presenceAvailabilitySchema,
  presenceParticipantLeaveSchema,
  presenceParticipantUpsertSchema,
  presenceUpdateSchema,
  type PresenceActivity,
  type PresenceParticipant,
  type PresenceUpdate,
} from "@converge/protocol";
import type { BoardSessionToken } from "./board-session";

export const PRESENCE_TOMBSTONE_LIMIT = 200;
export const PRESENCE_TOMBSTONE_RETENTION_MS = PRESENCE_SESSION_TTL_MS * 2;
export const COLLABORATOR_PALETTE = [
  "collaborator-1",
  "collaborator-2",
  "collaborator-3",
  "collaborator-4",
  "collaborator-5",
  "collaborator-6",
  "collaborator-7",
  "collaborator-8",
] as const;
export interface PresenceScheduler {
  now(): number;
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}
const defaultScheduler: PresenceScheduler = {
  now: () => performance.now(),
  setTimeout: (callback, delay) => setTimeout(callback, delay),
  clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};
type Session = PresenceParticipant & { expiresAtReceipt: number; observedAtMs: number };
type Tombstone = { revision: number; expiresAtReceipt: number; observedAtMs: number };
export type PresencePresentation = Readonly<{
  key: string;
  label: string;
  displayName: string;
  self: boolean;
  activity: PresenceActivity;
  cursor: PresenceParticipant["cursor"];
  paletteToken: (typeof COLLABORATOR_PALETTE)[number];
  current: boolean;
}>;
export type PresenceSnapshot = Readonly<{
  availability: "available" | "unavailable";
  current: boolean;
  selfUserId: string | null;
  selfPresenceSessionId: string | null;
  collaborators: readonly PresencePresentation[];
}>;

function time(value: string): number | null {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}
function palette(userId: string): (typeof COLLABORATOR_PALETTE)[number] {
  let hash = 0;
  for (const char of userId) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return COLLABORATOR_PALETTE[hash % COLLABORATOR_PALETTE.length]!;
}
function compareSession(left: Session, right: Session): number {
  return (
    right.observedAtMs - left.observedAtMs ||
    right.revision - left.revision ||
    left.presenceSessionId.localeCompare(right.presenceSessionId)
  );
}

/** Ephemeral, session-fenced authority. It has no BoardStore or persistence dependency. */
export class PresenceStore {
  private readonly sessions = new Map<string, Session>();
  private readonly tombstones = new Map<string, Tombstone>();
  private availability: "available" | "unavailable" = "unavailable";
  private selfSessionId: string | null = null;
  private selfUserId: string | null = null;
  private selfObservedAt = -Infinity;
  private closed = false;
  private expiryTimer: unknown;
  private publicationTimer: unknown;
  private lastPublished: PresenceUpdate | null = null;
  private queued: PresenceUpdate | null = null;
  private lastPublicationAt = -Infinity;
  private publisher: ((update: PresenceUpdate) => void) | null = null;

  constructor(
    readonly boardId: string,
    readonly token: BoardSessionToken,
    private readonly scheduler: PresenceScheduler = defaultScheduler,
  ) {}

  setPublisher(publisher: ((update: PresenceUpdate) => void) | null): void {
    this.publisher = publisher;
  }
  receiveSnapshot(raw: unknown): void {
    if (this.closed) return;
    const parsed = boardPresenceSnapshotSchema.safeParse(raw);
    if (!parsed.success || parsed.data.boardId !== this.boardId) return this.failClosed();
    const snapshot = parsed.data;
    const observed = time(snapshot.observedAt);
    const self = snapshot.participants.find(
      (item) => item.presenceSessionId === snapshot.selfPresenceSessionId,
    );
    if (
      !self ||
      observed === null ||
      (this.selfSessionId !== null &&
        snapshot.selfPresenceSessionId !== this.selfSessionId &&
        observed < this.selfObservedAt)
    )
      return this.failClosed();
    for (const participant of snapshot.participants) this.upsert(participant);
    if (!this.sessions.has(snapshot.selfPresenceSessionId)) return this.failClosed();
    this.selfSessionId = snapshot.selfPresenceSessionId;
    this.selfUserId = self.userId;
    this.selfObservedAt = observed;
    this.availability = "available";
    this.scheduleExpiry();
  }
  receiveUpsert(raw: unknown): void {
    if (this.closed) return;
    const parsed = presenceParticipantUpsertSchema.safeParse(raw);
    if (!parsed.success || parsed.data.boardId !== this.boardId) return;
    this.upsert(parsed.data.participant);
    this.scheduleExpiry();
  }
  receiveLeave(raw: unknown): void {
    if (this.closed) return;
    const parsed = presenceParticipantLeaveSchema.safeParse(raw);
    if (!parsed.success || parsed.data.boardId !== this.boardId) return;
    const leave = parsed.data;
    const current = this.sessions.get(leave.presenceSessionId);
    if (current && leave.revision >= current.revision)
      this.sessions.delete(leave.presenceSessionId);
    const previous = this.tombstones.get(leave.presenceSessionId);
    if (!previous || leave.revision > previous.revision)
      this.tombstones.set(leave.presenceSessionId, {
        revision: leave.revision,
        observedAtMs: time(leave.observedAt) ?? 0,
        expiresAtReceipt: this.scheduler.now() + PRESENCE_TOMBSTONE_RETENTION_MS,
      });
    this.boundTombstones();
    this.scheduleExpiry();
  }
  receiveAvailability(raw: unknown): void {
    if (this.closed) return;
    const parsed = presenceAvailabilitySchema.safeParse(raw);
    if (!parsed.success || parsed.data.boardId !== this.boardId) return;
    if (parsed.data.status === "unavailable") {
      this.availability = "unavailable";
      this.clearPublication();
    } else if (this.selfSessionId !== null) this.availability = "available";
  }
  publish(cursor: PresenceUpdate["cursor"], activity: PresenceActivity): void {
    if (this.closed || this.availability !== "available" || this.selfSessionId === null) return;
    const parsed = presenceUpdateSchema.safeParse({
      schemaVersion: 1,
      boardId: this.boardId,
      cursor,
      activity,
    });
    if (
      !parsed.success ||
      this.same(parsed.data, this.lastPublished) ||
      this.same(parsed.data, this.queued)
    )
      return;
    const interval = 1000 / PRESENCE_CURSOR_UPDATES_PER_SECOND;
    if (this.scheduler.now() - this.lastPublicationAt >= interval) this.emit(parsed.data);
    else {
      this.queued = parsed.data;
      if (this.publicationTimer === undefined)
        this.publicationTimer = this.scheduler.setTimeout(
          () => {
            this.publicationTimer = undefined;
            const queued = this.queued;
            this.queued = null;
            if (queued) this.emit(queued);
          },
          Math.max(0, interval - (this.scheduler.now() - this.lastPublicationAt)),
        );
    }
  }
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.sessions.clear();
    this.tombstones.clear();
    this.selfSessionId = this.selfUserId = null;
    this.availability = "unavailable";
    this.clearPublication();
    if (this.expiryTimer !== undefined) this.scheduler.clearTimeout(this.expiryTimer);
    this.expiryTimer = undefined;
  }
  snapshot(): PresenceSnapshot {
    const current = this.availability === "available" && this.selfSessionId !== null;
    const groups = new Map<string, Session[]>();
    for (const session of this.sessions.values()) {
      const list = groups.get(session.userId) ?? [];
      list.push(session);
      groups.set(session.userId, list);
    }
    const collaborators = [...groups.entries()]
      .map(([userId, sessions]) => {
        const sorted = [...sessions].sort(compareSession);
        const active = sorted.filter((session) => session.activity === "active");
        const displayed = (active.length > 0 ? active : sorted)[0]!;
        const self = userId === this.selfUserId;
        return Object.freeze({
          key: userId,
          label: self ? "You" : displayed.displayName,
          displayName: displayed.displayName,
          self,
          activity: active.length > 0 ? ("active" as const) : ("idle" as const),
          cursor: displayed.cursor,
          paletteToken: palette(userId),
          current,
        });
      })
      .sort(
        (left, right) => left.label.localeCompare(right.label) || left.key.localeCompare(right.key),
      );
    return Object.freeze({
      availability: this.availability,
      current,
      selfUserId: this.selfUserId,
      selfPresenceSessionId: this.selfSessionId,
      collaborators,
    });
  }
  private upsert(participant: PresenceParticipant): void {
    const observed = time(participant.observedAt),
      expires = time(participant.expiresAt);
    if (observed === null || expires === null || expires <= observed) return;
    const receiptExpiry =
      this.scheduler.now() + Math.min(PRESENCE_SESSION_TTL_MS, expires - observed);
    if (receiptExpiry <= this.scheduler.now()) return;
    const tombstone = this.tombstones.get(participant.presenceSessionId);
    const current = this.sessions.get(participant.presenceSessionId);
    if (
      (tombstone && tombstone.revision >= participant.revision) ||
      (current && current.revision >= participant.revision)
    )
      return;
    if (!current && this.sessions.size >= PRESENCE_SNAPSHOT_MAX_SESSIONS) return this.failClosed();
    this.sessions.set(participant.presenceSessionId, {
      ...participant,
      observedAtMs: observed,
      expiresAtReceipt: receiptExpiry,
    });
  }
  private emit(update: PresenceUpdate): void {
    if (this.closed || this.availability !== "available" || !this.publisher) return;
    try {
      this.publisher(update);
      this.lastPublished = update;
      this.lastPublicationAt = this.scheduler.now();
    } catch {
      /* presence is isolated */
    }
  }
  private same(left: PresenceUpdate, right: PresenceUpdate | null): boolean {
    return (
      right !== null &&
      left.activity === right.activity &&
      left.cursor?.x === right.cursor?.x &&
      left.cursor?.y === right.cursor?.y &&
      (left.cursor === null) === (right.cursor === null)
    );
  }
  private clearPublication(): void {
    this.queued = null;
    if (this.publicationTimer !== undefined) this.scheduler.clearTimeout(this.publicationTimer);
    this.publicationTimer = undefined;
  }
  private failClosed(): void {
    this.availability = "unavailable";
    this.clearPublication();
  }
  private boundTombstones(): void {
    if (this.tombstones.size <= PRESENCE_TOMBSTONE_LIMIT) return;
    const oldest = [...this.tombstones.entries()].sort(
      ([leftId, left], [rightId, right]) =>
        left.observedAtMs - right.observedAtMs || leftId.localeCompare(rightId),
    )[0];
    if (oldest) this.tombstones.delete(oldest[0]);
  }
  private scheduleExpiry(): void {
    if (this.expiryTimer !== undefined) this.scheduler.clearTimeout(this.expiryTimer);
    const now = this.scheduler.now();
    for (const [id, session] of this.sessions)
      if (session.expiresAtReceipt <= now) this.sessions.delete(id);
    for (const [id, tombstone] of this.tombstones)
      if (tombstone.expiresAtReceipt <= now) this.tombstones.delete(id);
    const next = Math.min(
      ...[...this.sessions.values(), ...this.tombstones.values()].map(
        (item) => item.expiresAtReceipt,
      ),
    );
    this.expiryTimer = Number.isFinite(next)
      ? this.scheduler.setTimeout(
          () => {
            this.expiryTimer = undefined;
            this.scheduleExpiry();
          },
          Math.max(0, next - now),
        )
      : undefined;
  }
}
