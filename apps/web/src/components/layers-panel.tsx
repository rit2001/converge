"use client";

import * as React from "react";
import type { CanvasObject } from "@converge/protocol";
import { IconButton, Tooltip } from "./ui/primitives";

export interface LayerEntry {
  object: CanvasObject;
  label: string;
  position: number;
  total: number;
}

/**
 * Board order is bottom-to-top for Konva rendering. The Layers surface presents
 * that same durable order in the conventional top-to-bottom reading direction.
 */
export function layerEntries(objects: CanvasObject[]): LayerEntry[] {
  const topToBottom = [...objects].reverse();
  const counts = new Map<CanvasObject["kind"], number>();
  for (const object of topToBottom) counts.set(object.kind, (counts.get(object.kind) ?? 0) + 1);

  const seen = new Map<CanvasObject["kind"], number>();
  return topToBottom.map((object, index) => {
    const type = object.kind === "sticky" ? "Sticky note" : "Rectangle";
    const duplicateCount = counts.get(object.kind) ?? 0;
    const occurrence = (seen.get(object.kind) ?? 0) + 1;
    seen.set(object.kind, occurrence);
    return {
      object,
      label: duplicateCount > 1 ? `${type} ${occurrence}` : type,
      position: index + 1,
      total: topToBottom.length,
    };
  });
}

export function nextLayerFocus(
  currentIndex: number,
  key: "ArrowUp" | "ArrowDown" | "Home" | "End",
  count: number,
): number {
  if (count === 0) return -1;
  if (key === "Home") return 0;
  if (key === "End") return count - 1;
  if (key === "ArrowUp") return Math.max(0, currentIndex - 1);
  return Math.min(count - 1, currentIndex + 1);
}

function ObjectTypeIcon({ kind }: { kind: CanvasObject["kind"] }): React.JSX.Element {
  const shared = { fill: "none", stroke: "currentColor", strokeWidth: 1.75 };
  return kind === "sticky" ? (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <path {...shared} d="M6 4h12v12l-4 4H6V4Z" />
      <path {...shared} d="M14 20v-4h4M9 9h6M9 13h4" />
    </svg>
  ) : (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <rect {...shared} x="4" y="6" width="16" height="12" rx="2" />
    </svg>
  );
}

function ViewControlIcon({ kind }: { kind: "visibility" | "lock" }): React.JSX.Element {
  const shared = { fill: "none", stroke: "currentColor", strokeWidth: 1.75 };
  return kind === "visibility" ? (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <path {...shared} d="M3 12s3.5-5 9-5 9 5 9 5-3.5 5-9 5-9-5-9-5Z" />
      <circle {...shared} cx="12" cy="12" r="2.5" />
    </svg>
  ) : (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <rect {...shared} x="5" y="10" width="14" height="10" rx="2" />
      <path {...shared} d="M8 10V7a4 4 0 0 1 8 0v3" />
    </svg>
  );
}

export function LayersPanel({
  objects,
  selectedId,
  hiddenObjectIds,
  lockedObjectIds,
  onSelect,
  onToggleHidden,
  onToggleLocked,
  onClose,
}: {
  objects: CanvasObject[];
  selectedId: string | null;
  hiddenObjectIds: ReadonlySet<string>;
  lockedObjectIds: ReadonlySet<string>;
  onSelect: (id: string) => void;
  onToggleHidden: (id: string) => void;
  onToggleLocked: (id: string) => void;
  onClose: () => void;
}): React.JSX.Element {
  const entries = layerEntries(objects);
  const rowRefs = React.useRef<Array<HTMLButtonElement | null>>([]);

  const moveFocus = (event: React.KeyboardEvent<HTMLButtonElement>, index: number): void => {
    if (!(["ArrowUp", "ArrowDown", "Home", "End"] as const).includes(event.key as never)) return;
    event.preventDefault();
    rowRefs.current[
      nextLayerFocus(index, event.key as "ArrowUp" | "ArrowDown" | "Home" | "End", entries.length)
    ]?.focus();
  };

  return (
    <aside id="layers-panel" className="layers-panel" aria-labelledby="layers-panel-title">
      <header className="layers-panel__header">
        <div>
          <h2 id="layers-panel-title">Layers</h2>
          <p aria-label={`${entries.length} objects`}>{entries.length} objects</p>
          <p className="layers-panel__view-note">
            <strong>This view</strong> only. Visibility and locks are local and aren’t shared with
            collaborators.
          </p>
        </div>
        <button
          className="ui-button ui-button--ghost ui-button--icon"
          type="button"
          aria-label="Close layers panel"
          onClick={onClose}
        >
          <svg aria-hidden="true" viewBox="0 0 24 24">
            <path fill="none" stroke="currentColor" strokeWidth="1.75" d="m7 7 10 10M17 7 7 17" />
          </svg>
        </button>
      </header>
      {entries.length === 0 ? (
        <div className="layers-panel__empty">
          <span className="layers-panel__empty-icon" aria-hidden="true">
            <ObjectTypeIcon kind="rectangle" />
          </span>
          <p>Your board is ready for its first idea.</p>
          <span>Add a rectangle or sticky note from the tool dock.</span>
        </div>
      ) : (
        <ul className="layers-panel__list" aria-label="Board layers, top to bottom">
          {entries.map((entry, index) => {
            const selected = selectedId === entry.object.id;
            const hidden = hiddenObjectIds.has(entry.object.id);
            const locked = lockedObjectIds.has(entry.object.id);
            const accessibleLabel = `${entry.label}, ${entry.position === 1 ? "top" : entry.position === entry.total ? "bottom" : "layer"} layer, ${entry.position} of ${entry.total}`;
            return (
              <li key={entry.object.id}>
                <button
                  ref={(element) => {
                    rowRefs.current[index] = element;
                  }}
                  className="layers-panel__row ui-focus-ring"
                  type="button"
                  aria-label={accessibleLabel}
                  aria-pressed={selected}
                  data-hidden={hidden || undefined}
                  data-locked={locked || undefined}
                  onClick={() => onSelect(entry.object.id)}
                  onKeyDown={(event) => moveFocus(event, index)}
                >
                  <span className="layers-panel__icon" aria-hidden="true">
                    <ObjectTypeIcon kind={entry.object.kind} />
                  </span>
                  <span className="layers-panel__label">{entry.label}</span>
                  <span className="layers-panel__state">
                    {hidden && <span>Hidden</span>}
                    {locked && <span>Locked</span>}
                    {selected && <span className="layers-panel__selected">Selected</span>}
                  </span>
                </button>
                <div
                  className="layers-panel__controls"
                  aria-label={`${entry.label} local controls`}
                >
                  <Tooltip label={`${hidden ? "Show" : "Hide"} ${entry.label}`}>
                    <IconButton
                      className="layers-panel__control"
                      variant="ghost"
                      aria-label={`${hidden ? "Show" : "Hide"} ${entry.label}`}
                      aria-pressed={!hidden}
                      onClick={(event) => {
                        event.stopPropagation();
                        onToggleHidden(entry.object.id);
                      }}
                    >
                      <ViewControlIcon kind="visibility" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip label={`${locked ? "Unlock" : "Lock"} ${entry.label}`}>
                    <IconButton
                      className="layers-panel__control"
                      variant="ghost"
                      aria-label={`${locked ? "Unlock" : "Lock"} ${entry.label}`}
                      aria-pressed={locked}
                      onClick={(event) => {
                        event.stopPropagation();
                        onToggleLocked(entry.object.id);
                      }}
                    >
                      <ViewControlIcon kind="lock" />
                    </IconButton>
                  </Tooltip>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </aside>
  );
}
