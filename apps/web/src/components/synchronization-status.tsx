"use client";

import { createPortal } from "react-dom";
import { useEffect, useRef, useState } from "react";
import * as React from "react";
import type { SynchronizationPresentation } from "../synchronization-presentation";

export function SynchronizationStatus({
  presentation,
  pendingCount,
}: {
  presentation: SynchronizationPresentation;
  pendingCount: number;
}): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLElement>(null);
  const announced = useRef<string | null>(null);
  const announcement = presentation.terminal
    ? `${presentation.label}. ${presentation.next}`
    : presentation.label;
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
      )
        setOpen(false);
    };
    window.addEventListener("keydown", close);
    window.addEventListener("mousedown", outside);
    return () => {
      window.removeEventListener("keydown", close);
      window.removeEventListener("mousedown", outside);
    };
  }, [open]);
  const live = announced.current === announcement ? "" : announcement;
  announced.current = announcement;
  const details =
    open && typeof document !== "undefined"
      ? createPortal(
          <section
            ref={panel}
            id="synchronization-details"
            className="synchronization-details"
            aria-label="Synchronization details"
          >
            <strong>{presentation.label}</strong>
            <dl>
              <div>
                <dt>Editing</dt>
                <dd>{presentation.readyForEditing ? "Ready" : "Temporarily paused"}</dd>
              </div>
              <div>
                <dt>Pending changes</dt>
                <dd>{pendingCount}</dd>
              </div>
              <div>
                <dt>Stored locally</dt>
                <dd>{presentation.pendingPreservedLocally ? "Yes" : "No"}</dd>
              </div>
            </dl>
            <p>{presentation.next}</p>
          </section>,
          document.getElementById("overlay-popovers")!,
        )
      : null;
  return (
    <>
      <button
        ref={trigger}
        type="button"
        className={`synchronization-trigger synchronization-trigger--${presentation.tone}`}
        aria-label={`Synchronization status: ${presentation.label}`}
        aria-expanded={open}
        aria-controls="synchronization-details"
        onClick={() => setOpen((value) => !value)}
      >
        <span aria-hidden="true" className="synchronization-trigger__icon">
          ●
        </span>
        <span>{presentation.label}</span>
      </button>
      <output
        className="ui-visually-hidden"
        aria-live={presentation.terminal ? "assertive" : "polite"}
        aria-atomic="true"
      >
        {live}
      </output>
      {details}
    </>
  );
}
