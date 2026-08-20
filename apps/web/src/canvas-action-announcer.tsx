"use client";
import * as React from "react";
export type CanvasAnnouncement = Readonly<{ generation: number; message: string }>;
export function nextCanvasAnnouncement(
  current: CanvasAnnouncement,
  message: string,
): CanvasAnnouncement {
  return { generation: current.generation + 1, message: message.slice(0, 160) };
}
export function CanvasActionAnnouncer({
  announcement,
}: {
  announcement: CanvasAnnouncement;
}): React.JSX.Element {
  return (
    <output
      className="ui-visually-hidden"
      aria-live="polite"
      aria-atomic="true"
      data-generation={announcement.generation}
    >
      {announcement.message}
    </output>
  );
}
