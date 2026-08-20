"use client";

import { createPortal } from "react-dom";
import { useEffect, useRef, useState } from "react";
import * as React from "react";
import type { PresenceSnapshot } from "../presence-store";

function initials(name: string): string {
  return (
    name
      .trim()
      .split(/\s+/)
      .slice(0, 2)
      .map((part) => part.slice(0, 1).toUpperCase())
      .join("") || "?"
  );
}

export function CollaboratorPresence({
  presence,
  terminal = false,
}: {
  presence: PresenceSnapshot | null;
  terminal?: boolean;
}): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLElement>(null);
  const announcement = useRef("");
  const [live, setLive] = useState("");
  const snapshot = presence ?? {
    availability: "unavailable" as const,
    current: false,
    collaborators: [],
  };
  const collaborators = [...snapshot.collaborators].sort(
    (left, right) =>
      Number(right.self) - Number(left.self) ||
      Number(right.activity === "active") - Number(left.activity === "active") ||
      left.label.localeCompare(right.label),
  );
  const remoteCount = collaborators.filter((person) => !person.self).length;
  const available = snapshot.availability === "available" && snapshot.current;
  const message = available
    ? remoteCount === 0
      ? "Only you here"
      : `${remoteCount} live collaborator${remoteCount === 1 ? "" : "s"}`
    : "Presence temporarily unavailable";
  useEffect(() => {
    if (!open) return;
    const close = (event: KeyboardEvent): void => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      setOpen(false);
      trigger.current?.focus();
    };
    const outside = (event: MouseEvent): void => {
      if (
        !panel.current?.contains(event.target as Node) &&
        !trigger.current?.contains(event.target as Node)
      ) {
        setOpen(false);
        trigger.current?.focus();
      }
    };
    window.addEventListener("keydown", close);
    window.addEventListener("mousedown", outside);
    return () => {
      window.removeEventListener("keydown", close);
      window.removeEventListener("mousedown", outside);
    };
  }, [open]);
  useEffect(() => {
    if (!available || terminal) setOpen(false);
  }, [available, terminal]);
  useEffect(() => {
    if (terminal || announcement.current === message) return;
    announcement.current = message;
    setLive(message);
  }, [message, terminal]);
  const visible = collaborators.slice(0, 4);
  const details =
    open && typeof document !== "undefined"
      ? createPortal(
          <section
            ref={panel}
            id="collaborator-details"
            className="collaborator-details"
            aria-label="Live collaborators"
          >
            <h2>Live collaborators</h2>
            {!available && (
              <p className="collaborator-unavailable">Presence temporarily unavailable</p>
            )}
            {available && remoteCount === 0 && <p>Only you here.</p>}
            <ul>
              {collaborators.map((person) => (
                <li key={person.key}>
                  <span
                    className={`collaborator-avatar collaborator-avatar--${person.paletteToken}`}
                    aria-hidden="true"
                  >
                    {initials(person.displayName)}
                  </span>
                  <span>
                    <strong>{person.self ? "You" : person.displayName}</strong>
                    {!person.current && <em>Last known</em>}
                  </span>
                  <span>{person.activity === "active" ? "Active" : "Idle"}</span>
                  {person.sessionCount > 1 && (
                    <small>
                      {person.activeSessionCount} active session
                      {person.activeSessionCount === 1 ? "" : "s"}
                    </small>
                  )}
                </li>
              ))}
            </ul>
          </section>,
          document.getElementById("overlay-popovers")!,
        )
      : null;
  return (
    <>
      <button
        ref={trigger}
        type="button"
        className="collaborator-trigger"
        aria-label={`Collaborators: ${terminal ? "Presence temporarily unavailable" : message}`}
        aria-expanded={open}
        aria-controls="collaborator-details"
        onClick={() => setOpen((value) => !value)}
      >
        <span className="collaborator-stack" aria-hidden="true">
          {visible.map((person) => (
            <span
              key={person.key}
              className={`collaborator-avatar collaborator-avatar--${person.paletteToken}`}
            >
              {initials(person.displayName)}
            </span>
          ))}
        </span>
        <span className="collaborator-trigger__count">{available ? remoteCount : "—"}</span>
        {collaborators.length > 4 && (
          <span
            className="collaborator-overflow"
            aria-label={`${collaborators.length - 4} more collaborators`}
          >
            +{collaborators.length - 4}
          </span>
        )}
      </button>
      <output className="ui-visually-hidden" aria-live="polite" aria-atomic="true">
        {live}
      </output>
      {details}
    </>
  );
}
