"use client";

import { createPortal } from "react-dom";
import { useEffect, useId, useRef, useState } from "react";
import * as React from "react";
import { MAX_COMMAND_QUERY_LENGTH, searchCommands, type WorkspaceCommand } from "../commands";

export function CommandPalette({
  open,
  commands,
  onClose,
}: {
  open: boolean;
  commands: readonly WorkspaceCommand[];
  onClose: () => void;
}): React.JSX.Element | null {
  const input = useRef<HTMLInputElement>(null);
  const dialog = useRef<HTMLElement>(null);
  const previous = useRef<HTMLElement | null>(null);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const listId = useId();
  const results = searchCommands(commands, query);
  useEffect(() => {
    if (!open) return;
    previous.current =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    setQuery("");
    setActive(0);
    queueMicrotask(() => input.current?.focus());
    return () => undefined;
  }, [open]);
  useEffect(() => {
    if (!open) previous.current?.focus();
  }, [open]);
  useEffect(
    () => setActive((value) => Math.min(value, Math.max(0, results.length - 1))),
    [results.length],
  );
  if (!open || typeof document === "undefined") return null;
  const execute = (command: WorkspaceCommand): void => {
    if (!command.available) return;
    try {
      command.execute();
      onClose();
    } catch {
      // Presentation callbacks cannot alter command/pending state if they fail.
    }
  };
  const keys = (event: React.KeyboardEvent): void => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      onClose();
    } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (results.length)
        setActive(
          (value) =>
            (value + (event.key === "ArrowDown" ? 1 : -1) + results.length) % results.length,
        );
    } else if (event.key === "Home") {
      event.preventDefault();
      setActive(0);
    } else if (event.key === "End") {
      event.preventDefault();
      setActive(Math.max(0, results.length - 1));
    } else if (event.key === "Enter" && results[active]) {
      event.preventDefault();
      execute(results[active]);
    } else if (event.key === "Tab") {
      event.preventDefault();
      input.current?.focus();
    }
  };
  return createPortal(
    <div
      className="command-palette-backdrop"
      onMouseDown={(event) => event.target === event.currentTarget && onClose()}
    >
      <section
        ref={dialog}
        className="command-palette"
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
        onKeyDown={keys}
      >
        <label className="ui-visually-hidden" htmlFor="command-palette-search">
          Search commands
        </label>
        <input
          ref={input}
          id="command-palette-search"
          role="combobox"
          aria-controls={listId}
          aria-expanded="true"
          aria-activedescendant={results[active] ? `command-${results[active].id}` : undefined}
          value={query}
          maxLength={MAX_COMMAND_QUERY_LENGTH}
          placeholder="Search commands"
          onChange={(event) => {
            setQuery(event.target.value);
            setActive(0);
          }}
        />
        <output className="ui-visually-hidden" aria-live="polite">
          {results.length} commands
        </output>
        {results.length ? (
          <ul id={listId} role="listbox">
            {results.map((command, index) => (
              <li
                key={command.id}
                id={`command-${command.id}`}
                role="option"
                aria-selected={index === active}
                aria-disabled={!command.available}
                className={index === active ? "active" : undefined}
                onMouseEnter={() => setActive(index)}
              >
                <button
                  type="button"
                  disabled={!command.available}
                  onClick={() => execute(command)}
                >
                  <span>
                    <strong>{command.label}</strong>
                    <small>{command.description}</small>
                    {command.disabledReason && <em>{command.disabledReason}</em>}
                  </span>
                  {command.shortcut && <kbd>{command.shortcut}</kbd>}
                </button>
              </li>
            ))}
          </ul>
        ) : (
          <p>No commands found.</p>
        )}
      </section>
    </div>,
    document.getElementById("overlay-modals")!,
  );
}
