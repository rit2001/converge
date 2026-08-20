import type { SynchronizationStatus } from "./board-store";
import type { PendingRecoveryStatus } from "./pending-command-queue";
import type { BadgeTone } from "./components/ui/primitives";

export type SynchronizationPresentationState =
  | "connecting"
  | "restoring"
  | "synced"
  | "saving"
  | "reconnecting"
  | "locally_preserved"
  | "access_removed"
  | "recovery_blocked"
  | "unavailable";

export interface SynchronizationPresentationInput {
  hasCurrentSession: boolean;
  hasBoard: boolean;
  connection: SynchronizationStatus;
  pendingCount: number;
  pendingStatus: PendingRecoveryStatus;
}

export interface SynchronizationPresentation {
  state: SynchronizationPresentationState;
  label: string;
  tone: BadgeTone;
  readyForEditing: boolean;
  pendingPreservedLocally: boolean;
  terminal: boolean;
  next: string;
}

const presentations: Record<
  SynchronizationPresentationState,
  Omit<SynchronizationPresentation, "state">
> = {
  connecting: {
    label: "Connecting…",
    tone: "reconnecting",
    readyForEditing: false,
    pendingPreservedLocally: false,
    terminal: false,
    next: "Connecting to the board.",
  },
  restoring: {
    label: "Restoring board…",
    tone: "recovering",
    readyForEditing: false,
    pendingPreservedLocally: false,
    terminal: false,
    next: "Verifying the board before editing resumes.",
  },
  synced: {
    label: "Synced",
    tone: "success",
    readyForEditing: true,
    pendingPreservedLocally: false,
    terminal: false,
    next: "New edits can be sent to the board.",
  },
  saving: {
    label: "Saving…",
    tone: "information",
    readyForEditing: true,
    pendingPreservedLocally: false,
    terminal: false,
    next: "Waiting for your pending changes to be acknowledged.",
  },
  reconnecting: {
    label: "Reconnecting…",
    tone: "reconnecting",
    readyForEditing: false,
    pendingPreservedLocally: false,
    terminal: false,
    next: "Waiting to safely reconnect to the board.",
  },
  locally_preserved: {
    label: "Changes kept on this device",
    tone: "warning",
    readyForEditing: false,
    pendingPreservedLocally: true,
    terminal: false,
    next: "Your pending changes are preserved here while recovery continues.",
  },
  access_removed: {
    label: "Access removed",
    tone: "revoked",
    readyForEditing: false,
    pendingPreservedLocally: false,
    terminal: true,
    next: "Return home to leave this board safely.",
  },
  recovery_blocked: {
    label: "Recovery needs attention",
    tone: "danger",
    readyForEditing: false,
    pendingPreservedLocally: false,
    terminal: true,
    next: "Editing stays disabled because this board could not be safely recovered.",
  },
  unavailable: {
    label: "Temporarily unavailable",
    tone: "unavailable",
    readyForEditing: false,
    pendingPreservedLocally: false,
    terminal: false,
    next: "Waiting for a current board session.",
  },
};

/**
 * The ordered fail-closed synchronization language contract. Inputs are current BoardStore evidence
 * only; a socket connection is intentionally not enough to produce "Synced".
 */
export function deriveSynchronizationPresentation(
  input: SynchronizationPresentationInput,
): SynchronizationPresentation {
  let state: SynchronizationPresentationState;
  if (input.connection === "authorization-failed") state = "access_removed";
  else if (input.connection === "error" || input.pendingStatus === "persistence-error")
    state = "recovery_blocked";
  else if (!input.hasCurrentSession || !input.hasBoard) state = "unavailable";
  else if (
    input.pendingCount > 0 &&
    (input.connection === "disconnected" ||
      input.connection === "retry-wait" ||
      input.pendingStatus === "pending-retry")
  )
    state = "locally_preserved";
  else if (input.connection === "retry-wait" || input.connection === "disconnected")
    state = "reconnecting";
  else if (input.connection === "connecting") state = "connecting";
  else if (input.connection === "joining" || input.connection === "catching-up")
    state = "restoring";
  else if (input.connection === "ready" && input.pendingCount > 0) state = "saving";
  else if (
    input.connection === "ready" &&
    input.pendingCount === 0 &&
    input.pendingStatus === "idle"
  )
    state = "synced";
  else state = "unavailable";
  return { state, ...presentations[state] };
}

export function synchronizationAnnouncement(presentation: SynchronizationPresentation): string {
  return presentation.terminal ? `${presentation.label}. ${presentation.next}` : presentation.label;
}
