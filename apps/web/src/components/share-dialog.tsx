"use client";
import { createPortal } from "react-dom";
import { useEffect, useRef, useState } from "react";
import * as React from "react";
export function ShareDialog({
  boardId,
  open,
  onClose,
}: {
  boardId: string | null;
  open: boolean;
  onClose: () => void;
}): React.JSX.Element | null {
  const copy = useRef<HTMLButtonElement>(null);
  const previous = useRef<HTMLElement | null>(null);
  const [message, setMessage] = useState("");
  useEffect(() => {
    if (open) {
      previous.current = document.activeElement as HTMLElement;
      queueMicrotask(() => copy.current?.focus());
      setMessage("");
    } else previous.current?.focus();
  }, [open]);
  if (!open || !boardId || typeof document === "undefined") return null;
  const url = new URL(`/studio/${boardId}`, window.location.origin).toString();
  return createPortal(
    <div className="command-palette-backdrop">
      <section
        className="command-palette"
        role="dialog"
        aria-modal="true"
        aria-labelledby="share-title"
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            event.stopPropagation();
            onClose();
          }
          if (event.key === "Tab") {
            event.preventDefault();
            copy.current?.focus();
          }
        }}
      >
        <h2 id="share-title">Share this board</h2>
        <p>
          This link does not grant access. Only people who already have access to this board can
          open it.
        </p>
        <label>
          Board link
          <input aria-label="Board link" readOnly value={url} />
        </label>
        <button
          ref={copy}
          onClick={() => {
            if (!navigator.clipboard) {
              setMessage("Copy failed. Select the link manually.");
              return;
            }
            void navigator.clipboard
              .writeText(url)
              .then(() => setMessage("Link copied."))
              .catch(() => setMessage("Copy failed. Select the link manually."));
          }}
        >
          Copy link
        </button>
        <button onClick={onClose}>Close</button>
        <output aria-live="polite">{message}</output>
      </section>
    </div>,
    document.getElementById("overlay-modals")!,
  );
}
