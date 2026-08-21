"use client";

import * as React from "react";
import type { CanvasObject } from "@converge/protocol";
import { ROTATION_STEP_DEGREES } from "../canvas/rotation";
import { IconButton, Tooltip } from "./ui/primitives";

function objectTypeLabel(object: CanvasObject): string {
  return object.kind === "sticky" ? "Sticky note" : "Rectangle";
}

function RotationIcon({ direction }: { direction: "clockwise" | "counterclockwise" | "reset" }) {
  const shared = { fill: "none", stroke: "currentColor", strokeWidth: 1.75 };
  if (direction === "reset")
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24">
        <path {...shared} d="M5 8V4m0 0h4M5 4l3 3a8 8 0 1 1-1.3 8.8" />
      </svg>
    );
  const clockwise = direction === "clockwise";
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <path
        {...shared}
        d={
          clockwise
            ? "M19 8V4m0 0h-4m4 0-3 3a8 8 0 1 0 1.3 8.8"
            : "M5 8V4m0 0h4M5 4l3 3a8 8 0 1 1-1.3 8.8"
        }
      />
    </svg>
  );
}

export function RotationControls({
  object,
  onRotate,
  onFocusWithinChange,
}: {
  object: CanvasObject;
  onRotate: (rotation: number) => void;
  onFocusWithinChange?: (focused: boolean) => void;
}): React.JSX.Element {
  const label = objectTypeLabel(object);
  const counterclockwise = `Rotate ${label} ${ROTATION_STEP_DEGREES}° counterclockwise`;
  const clockwise = `Rotate ${label} ${ROTATION_STEP_DEGREES}° clockwise`;
  const reset = `Reset ${label} rotation to 0°`;
  return (
    <div
      className="rotation-controls"
      aria-label={`Shared rotation controls for ${label}`}
      onFocusCapture={() => onFocusWithinChange?.(true)}
      onBlurCapture={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) onFocusWithinChange?.(false);
      }}
    >
      <span className="rotation-controls__shared-note">Shared rotation</span>
      <span className="ui-visually-hidden">Rotation changes are shared with collaborators.</span>
      <Tooltip label={counterclockwise}>
        <IconButton
          className="workspace-tool-button"
          variant="ghost"
          aria-label={counterclockwise}
          onClick={() => onRotate(object.rotation - ROTATION_STEP_DEGREES)}
        >
          <RotationIcon direction="counterclockwise" />
        </IconButton>
      </Tooltip>
      <Tooltip label={clockwise}>
        <IconButton
          className="workspace-tool-button"
          variant="ghost"
          aria-label={clockwise}
          onClick={() => onRotate(object.rotation + ROTATION_STEP_DEGREES)}
        >
          <RotationIcon direction="clockwise" />
        </IconButton>
      </Tooltip>
      <Tooltip label={reset}>
        <IconButton
          className="workspace-tool-button"
          variant="ghost"
          aria-label={reset}
          onClick={() => onRotate(0)}
        >
          <RotationIcon direction="reset" />
        </IconButton>
      </Tooltip>
    </div>
  );
}
