import { randomUUID } from "node:crypto";
import {
  PRESENCE_CURSOR_UPDATES_PER_SECOND,
  PRESENCE_HEARTBEAT_INTERVAL_MS,
  PRESENCE_IDLE_AFTER_MS,
  boardPresenceSnapshotSchema,
  presenceAvailabilitySchema,
  presenceUpdateSchema,
  type PresenceParticipant,
  type PresenceParticipantLeave,
  type PresenceUpdate,
} from "@converge/protocol";
import type {
  PresenceOutcome,
  PresencePrincipalEvidence,
  PresenceRedisTransport,
  PresenceStorageSnapshot,
} from "./presence-redis-transport.js";

export interface PresenceRuntimeSocket {
  id: string;
  emit(event: string, value: unknown): void;
  on(event: string, listener: (value?: unknown) => void): void;
}
export interface PresenceRuntimeIo {
  local: { to(room: string): { emit(event: string, value: unknown): void } };
}
export interface PresenceRuntimeScheduler {
  now(): number;
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}
const scheduler: PresenceRuntimeScheduler = {
  now: () => Date.now(),
  setTimeout: (callback, delay) => setTimeout(callback, delay),
  clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};
type Binding = {
  token: symbol;
  socket: PresenceRuntimeSocket;
  boardId: string;
  sessionId: string;
  principal: PresencePrincipalEvidence;
  latest: PresenceUpdate | null;
  cursor: PresenceUpdate["cursor"];
  activity: PresenceUpdate["activity"];
  generation: number;
  busy: boolean;
  lastSent: number;
  heartbeat?: unknown;
  idle?: unknown;
  flush?: unknown;
};

/** A disabled deployment advertises a bounded capability without allocating Redis clients. */
export class UnavailablePresenceTransport implements PresenceRedisTransport {
  private readonly unavailable = {
    kind: "unavailable" as const,
    code: "PRESENCE_REDIS_UNAVAILABLE" as const,
  };
  start(): Promise<PresenceOutcome<void>> {
    return Promise.resolve(this.unavailable);
  }
  admit(): Promise<PresenceOutcome<PresenceParticipant>> {
    return Promise.resolve(this.unavailable);
  }
  refresh(): Promise<PresenceOutcome<PresenceParticipant>> {
    return Promise.resolve(this.unavailable);
  }
  snapshot(): Promise<PresenceOutcome<PresenceStorageSnapshot>> {
    return Promise.resolve(this.unavailable);
  }
  leave(): Promise<PresenceOutcome<PresenceParticipantLeave | null>> {
    return Promise.resolve(this.unavailable);
  }
  onDelta(): () => void {
    return () => undefined;
  }
  onAvailability(): () => void {
    return () => undefined;
  }
  stop(): Promise<void> {
    return Promise.resolve();
  }
}

export class PresenceRuntime {
  private readonly bindings = new Map<string, Binding>();
  private started = false;
  private stopped = false;
  private stopPromise?: Promise<void>;
  constructor(
    private readonly transport: PresenceRedisTransport,
    private readonly io: PresenceRuntimeIo,
    private readonly timing: PresenceRuntimeScheduler = scheduler,
  ) {
    transport.onDelta((event) => {
      if (!this.stopped)
        this.io.local
          .to(`board:${event.boardId}`)
          .emit(
            "participant" in event ? "presence:participant-upsert" : "presence:participant-leave",
            event,
          );
    });
    transport.onAvailability((event, code) => {
      if (event.status === "available") this.recover();
      else if (code) this.notifyUnavailable();
    });
  }
  start(): void {
    if (this.started || this.stopped) return;
    this.started = true;
    void this.transport
      .start()
      .then((outcome) => {
        if (outcome.kind !== "ok") this.notifyUnavailable();
      })
      .catch(() => this.notifyUnavailable());
  }
  bind(socket: PresenceRuntimeSocket, boardId: string, principal: PresencePrincipalEvidence): void {
    this.unbind(socket.id);
    if (this.stopped) return this.emitAvailability(socket, boardId, "unavailable");
    const binding: Binding = {
      token: Symbol("presence"),
      socket,
      boardId,
      sessionId: randomUUID(),
      principal,
      latest: null,
      cursor: null,
      activity: "active",
      generation: 0,
      busy: false,
      lastSent: -Infinity,
    };
    this.bindings.set(socket.id, binding);
    socket.on("presence:update", (raw) => this.update(binding, raw));
    void this.admit(binding);
  }
  private async admit(binding: Binding, generation = binding.generation): Promise<void> {
    const outcome = await this.transport
      .admit({
        boardId: binding.boardId,
        presenceSessionId: binding.sessionId,
        principal: binding.principal,
        cursor: binding.cursor,
        activity: binding.activity,
      })
      .catch(() => ({ kind: "unavailable" as const, code: "PRESENCE_REDIS_UNAVAILABLE" as const }));
    if (!this.current(binding) || binding.generation !== generation) return;
    if (outcome.kind !== "ok")
      return this.emitAvailability(binding.socket, binding.boardId, "unavailable");
    const snapshot = await this.transport
      .snapshot(binding.boardId)
      .catch(() => ({ kind: "unavailable" as const, code: "PRESENCE_REDIS_UNAVAILABLE" as const }));
    if (!this.current(binding) || binding.generation !== generation) return;
    const self =
      snapshot.kind === "ok"
        ? snapshot.value.participants.filter(
            (participant) => participant.presenceSessionId === outcome.value.presenceSessionId,
          )
        : [];
    const selfMatchesPrincipal =
      self.length === 1 &&
      self[0]?.userId === binding.principal.userId &&
      self[0].displayName === binding.principal.displayName;
    if (snapshot.kind === "ok" && selfMatchesPrincipal)
      binding.socket.emit(
        "board:presence-snapshot",
        boardPresenceSnapshotSchema.parse({
          ...snapshot.value,
          selfPresenceSessionId: outcome.value.presenceSessionId,
        }),
      );
    this.emitAvailability(
      binding.socket,
      binding.boardId,
      snapshot.kind === "ok" && selfMatchesPrincipal ? "available" : "unavailable",
    );
    if (snapshot.kind === "ok" && selfMatchesPrincipal) this.schedule(binding);
  }
  private update(binding: Binding, raw: unknown): void {
    if (!this.current(binding)) return;
    const parsed = presenceUpdateSchema.safeParse(raw);
    if (!parsed.success || parsed.data.boardId !== binding.boardId) return;
    binding.latest = parsed.data;
    binding.cursor = parsed.data.cursor;
    binding.activity = parsed.data.activity;
    if (parsed.data.activity === "active") this.resetIdle(binding);
    this.scheduleHeartbeat(binding);
    if (this.timing.now() - binding.lastSent >= 1000 / PRESENCE_CURSOR_UPDATES_PER_SECOND)
      void this.flush(binding);
    else this.scheduleFlush(binding);
  }
  private schedule(binding: Binding): void {
    this.scheduleHeartbeat(binding);
    this.resetIdle(binding);
  }
  private scheduleHeartbeat(binding: Binding): void {
    if (!this.current(binding)) return;
    if (binding.heartbeat === undefined)
      binding.heartbeat = this.timing.setTimeout(() => {
        binding.heartbeat = undefined;
        binding.latest ??= {
          schemaVersion: 1,
          boardId: binding.boardId,
          cursor: binding.cursor,
          activity: binding.activity,
        };
        void this.flush(binding);
        this.scheduleHeartbeat(binding);
      }, PRESENCE_HEARTBEAT_INTERVAL_MS);
  }
  private resetIdle(binding: Binding): void {
    if (!this.current(binding)) return;
    if (binding.idle !== undefined) this.timing.clearTimeout(binding.idle);
    binding.idle = this.timing.setTimeout(() => {
      binding.idle = undefined;
      binding.activity = "idle";
      binding.latest = {
        schemaVersion: 1,
        boardId: binding.boardId,
        cursor: binding.cursor,
        activity: "idle",
      };
      void this.flush(binding);
    }, PRESENCE_IDLE_AFTER_MS);
  }
  private scheduleFlush(binding: Binding): void {
    if (!this.current(binding) || binding.flush !== undefined) return;
    const wait = Math.max(
      0,
      1000 / PRESENCE_CURSOR_UPDATES_PER_SECOND - (this.timing.now() - binding.lastSent),
    );
    binding.flush = this.timing.setTimeout(() => {
      binding.flush = undefined;
      void this.flush(binding);
    }, wait);
  }
  private async flush(binding: Binding): Promise<void> {
    if (!this.current(binding) || binding.busy || !binding.latest) return;
    const generation = binding.generation;
    const update = binding.latest;
    binding.latest = null;
    binding.busy = true;
    binding.lastSent = this.timing.now();
    const outcome = await this.transport
      .refresh({
        boardId: binding.boardId,
        presenceSessionId: binding.sessionId,
        principal: binding.principal,
        cursor: update.cursor,
        activity: update.activity,
      })
      .catch(() => ({ kind: "unavailable" as const, code: "PRESENCE_REDIS_UNAVAILABLE" as const }));
    binding.busy = false;
    if (!this.current(binding) || binding.generation !== generation) return;
    if (outcome.kind !== "ok")
      this.emitAvailability(binding.socket, binding.boardId, "unavailable");
    if (binding.latest) {
      this.scheduleFlush(binding);
    }
  }
  unbind(socketId: string): void {
    const binding = this.bindings.get(socketId);
    if (!binding) return;
    this.bindings.delete(socketId);
    this.clear(binding);
    void this.transport
      .leave(binding.boardId, binding.sessionId, binding.principal)
      .catch(() => undefined);
  }
  private clear(binding: Binding): void {
    for (const timer of [binding.heartbeat, binding.idle, binding.flush])
      if (timer !== undefined) this.timing.clearTimeout(timer);
    binding.heartbeat = binding.idle = binding.flush = undefined;
  }
  private current(binding: Binding): boolean {
    return !this.stopped && this.bindings.get(binding.socket.id)?.token === binding.token;
  }
  private emitAvailability(
    socket: PresenceRuntimeSocket,
    boardId: string,
    status: "available" | "unavailable",
  ): void {
    socket.emit(
      "presence:availability",
      presenceAvailabilitySchema.parse({ schemaVersion: 1, boardId, status }),
    );
  }
  private notifyUnavailable(): void {
    for (const binding of this.bindings.values())
      this.emitAvailability(binding.socket, binding.boardId, "unavailable");
  }
  private recover(): void {
    for (const binding of this.bindings.values()) {
      if (!this.current(binding)) continue;
      this.clear(binding);
      const previousSessionId = binding.sessionId;
      binding.generation += 1;
      binding.busy = false;
      binding.latest = null;
      binding.sessionId = randomUUID();
      void this.transport
        .leave(binding.boardId, previousSessionId, binding.principal)
        .catch(() => undefined);
      void this.admit(binding);
    }
  }
  stop(): Promise<void> {
    this.stopPromise ??= this.stopOnce();
    return this.stopPromise;
  }
  private async stopOnce(): Promise<void> {
    this.stopped = true;
    for (const id of [...this.bindings.keys()]) this.unbind(id);
    await this.transport.stop().catch(() => undefined);
  }
}
