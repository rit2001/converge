"use client";
import { createPortal } from "react-dom";
import { useEffect, useRef } from "react";
import * as React from "react";
export function StudioHelp({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}): React.JSX.Element | null {
  const close = useRef<HTMLButtonElement>(null);
  const previous = useRef<HTMLElement | null>(null);
  useEffect(() => {
    if (open) {
      previous.current = document.activeElement as HTMLElement;
      queueMicrotask(() => close.current?.focus());
    } else previous.current?.focus();
  }, [open]);
  if (!open || typeof document === "undefined") return null;
  return createPortal(
    <div className="command-palette-backdrop">
      <section
        className="command-palette studio-help"
        role="dialog"
        aria-modal="true"
        aria-labelledby="studio-help-title"
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            event.stopPropagation();
            onClose();
          }
          if (event.key === "Tab") {
            event.preventDefault();
            close.current?.focus();
          }
        }}
      >
        <button ref={close} className="ui-button ui-button--ghost" onClick={onClose}>
          Close help
        </button>
        <h2 id="studio-help-title">Studio help</h2>
        <p>Use the canvas tools and header status to work confidently.</p>
        <h3>Create</h3>
        <p>
          <kbd>R</kbd> Rectangle · <kbd>N</kbd> Sticky note
        </p>
        <h3>Navigate</h3>
        <p>
          <kbd>V</kbd> Select · <kbd>H</kbd> Hand/pan · <kbd>0</kbd> Reset zoom
        </p>
        <h3>Work efficiently</h3>
        <p>
          <kbd>Ctrl/⌘ K</kbd> Command palette · <kbd>Delete</kbd> eligible selection · Shift rotates
          by 15° · Alt/Option bypasses snapping.
        </p>
        <h3>Organize and collaborate</h3>
        <p>
          Layers and “This view” visibility/locking are local. Rotation is shared. Synchronization
          shows saving/recovery; collaborator cursors are ephemeral, and unavailable presence does
          not mean changes are unsaved.
        </p>
      </section>
    </div>,
    document.getElementById("overlay-modals")!,
  );
}
