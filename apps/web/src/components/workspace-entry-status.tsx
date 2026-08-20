import Link from "next/link";
import * as React from "react";
import type { SynchronizationStatus } from "../board-store";
import { StatusPill, Surface, type BadgeTone } from "./ui/primitives";

export interface WorkspaceStatePresentation {
  label: string;
  title: string;
  description: string;
  tone: BadgeTone;
  terminal: boolean;
}

export function toneForSynchronization(status: SynchronizationStatus): BadgeTone {
  if (status === "ready") return "success";
  if (status === "catching-up") return "recovering";
  if (status === "connecting" || status === "joining" || status === "retry-wait")
    return "reconnecting";
  if (status === "authorization-failed" || status === "error") return "danger";
  return "unavailable";
}

export function workspaceStatePresentation(
  status: SynchronizationStatus,
  hasBoard: boolean,
): WorkspaceStatePresentation | null {
  switch (status) {
    case "ready":
      return null;
    case "connecting":
      return {
        label: "Connecting",
        title: "Connecting to the studio",
        description: "Establishing a secure collaboration session.",
        tone: "reconnecting",
        terminal: false,
      };
    case "joining":
      return {
        label: "Synchronizing",
        title: "Joining the shared board",
        description: "Confirming the authoritative board boundary before editing begins.",
        tone: "reconnecting",
        terminal: false,
      };
    case "catching-up":
      return {
        label: "Catching up",
        title: "Catching up with the board",
        description: "Applying verified board history before editing resumes.",
        tone: "recovering",
        terminal: false,
      };
    case "retry-wait":
      return {
        label: "Recovering",
        title: "Connection interrupted",
        description: "Queued edits remain on this device while Converge waits to retry safely.",
        tone: "reconnecting",
        terminal: false,
      };
    case "authorization-failed":
      return {
        label: "Access revoked",
        title: "You no longer have access to this board",
        description: "Editing is disabled. Return home to leave this session safely.",
        tone: "revoked",
        terminal: true,
      };
    case "error":
      return {
        label: "Unavailable",
        title: "This board cannot be opened safely",
        description: "Editing remains disabled because the current state could not be verified.",
        tone: "danger",
        terminal: true,
      };
    case "disconnected":
      return hasBoard
        ? {
            label: "Unavailable",
            title: "The collaboration connection is unavailable",
            description: "Editing pauses while the existing session reconnects.",
            tone: "unavailable",
            terminal: false,
          }
        : {
            label: "Preparing",
            title: "Preparing your studio",
            description: "No editing session is ready yet.",
            tone: "information",
            terminal: false,
          };
  }
}

export function WorkspaceEntryStatus({
  status,
  hasBoard,
}: {
  status: SynchronizationStatus;
  hasBoard: boolean;
}): React.JSX.Element | null {
  const presentation = workspaceStatePresentation(status, hasBoard);
  if (!presentation) return null;

  return (
    <section
      className={`workspace-entry-state${presentation.terminal ? " workspace-entry-state--terminal" : ""}`}
      aria-live={presentation.terminal ? "assertive" : "polite"}
      aria-atomic="true"
    >
      <Surface
        className="workspace-entry-state__panel"
        role={presentation.terminal ? "alert" : "status"}
        aria-labelledby="workspace-entry-title"
      >
        <StatusPill label={presentation.label} tone={presentation.tone} />
        <h1 id="workspace-entry-title">{presentation.title}</h1>
        <p>{presentation.description}</p>
        {presentation.terminal && (
          <Link className="ui-button ui-button--secondary ui-button--default" href="/">
            Return home
          </Link>
        )}
      </Surface>
    </section>
  );
}
